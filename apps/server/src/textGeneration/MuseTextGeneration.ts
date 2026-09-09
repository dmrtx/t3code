/**
 * MuseTextGeneration — Text generation layer using Muse Code over persistent MSP.
 *
 * Implements the TextGeneration service contract for commit messages, change
 * request titles/descriptions, branch names, and thread titles via persistent MSP stdio.
 *
 * @module MuseTextGeneration
 */
import type { ModelSelection, MuseSettings } from "@t3tools/contracts";
import { TextGenerationError } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { makeMspHost, newMspCommandId } from "../provider/msp/MspClient.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

const MUSE_TIMEOUT_MS = 180_000;

const isTextGenerationError = Schema.is(TextGenerationError);

export const makeMuseTextGeneration = Effect.fn("makeMuseTextGeneration")(function* (
  museSettings: MuseSettings,
  environment?: NodeJS.ProcessEnv,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const resolvedEnvironment = environment ?? process.env;

  const runMuseJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const outputRef = yield* Ref.make("");
      const completionDeferred = yield* Deferred.make<void, TextGenerationError>();

      const host = yield* makeMspHost({
        command: museSettings.binaryPath || "muse",
        cwd,
        env: resolvedEnvironment,
      }).pipe(
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, commandSpawner),
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: `Failed to initialize Muse host for text generation: ${cause.detail ?? cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      // Listen for view events from host
      yield* host.notifications.pipe(
        Stream.runForEach((notification) =>
          Effect.gen(function* () {
            if (notification.method === "item/delta") {
              const delta =
                typeof notification.params.delta === "string" ? notification.params.delta : "";
              const field =
                typeof notification.params.field === "string" ? notification.params.field : "";
              if (!field.startsWith("summary")) {
                yield* Ref.update(outputRef, (curr) => curr + delta);
              }
            } else if (notification.method === "item/completed") {
              const item = notification.params.item;
              if (
                typeof item === "object" &&
                item !== null &&
                "text" in item &&
                typeof item.text === "string" &&
                item.text.length > 0
              ) {
                const text = item.text;
                yield* Ref.update(outputRef, (curr) => (curr.length === 0 ? text : curr));
              }
            } else if (
              notification.method === "turn/completed" ||
              notification.method === "turn/finished"
            ) {
              yield* Deferred.succeed(completionDeferred, void 0);
            } else if (notification.method === "turn/failed") {
              const error =
                typeof notification.params.error === "string"
                  ? notification.params.error
                  : String(notification.params.error ?? "Turn execution failed");
              yield* Deferred.fail(
                completionDeferred,
                new TextGenerationError({
                  operation,
                  detail: `Muse turn failed during text generation: ${error}`,
                }),
              );
            }
          }),
        ),
        Effect.forkScoped,
      );

      yield* Effect.gen(function* () {
        const sessionCommandId = newMspCommandId();
        const startResult = yield* host
          .startSession({
            commandId: sessionCommandId,
            workspaceRoot: cwd,
            modelId: modelSelection.model,
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation,
                  detail: `Muse failed to start session: ${cause.detail ?? cause.message ?? String(cause)}`,
                  cause,
                }),
            ),
          );

        const turnCommandId = newMspCommandId();
        const reasoningEffort = getModelSelectionStringOptionValue(
          modelSelection,
          "reasoningEffort",
        );

        yield* host
          .startTurn({
            commandId: turnCommandId,
            sessionId: startResult.session.sessionId,
            input: [
              {
                type: "text",
                text: [
                  "Use only the input below. Return only the requested JSON object without markdown fences or additional commentary.",
                  "",
                  prompt,
                ].join("\n"),
              },
            ],
            ...(reasoningEffort ? { reasoningEffort } : {}),
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation,
                  detail: `Muse turn/start failed: ${cause.detail ?? cause.message ?? String(cause)}`,
                  cause,
                }),
            ),
          );

        yield* Deferred.await(completionDeferred).pipe(
          Effect.timeoutOption(MUSE_TIMEOUT_MS),
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new TextGenerationError({
                    operation,
                    detail: "Muse text generation timed out.",
                  }),
                ),
              onSome: () => Effect.void,
            }),
          ),
        );
      }).pipe(Effect.ensuring(host.close.pipe(Effect.ignore)));

      const rawResult = (yield* Ref.get(outputRef)).trim();
      if (!rawResult) {
        return yield* new TextGenerationError({
          operation,
          detail: "Muse returned empty output.",
        });
      }

      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));
      return yield* decodeOutput(extractJsonObject(rawResult)).pipe(
        Effect.catchTags({
          SchemaError: (cause) =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: "Muse returned invalid structured output.",
                cause,
              }),
            ),
        }),
      );
    }).pipe(
      Effect.mapError((cause) =>
        isTextGenerationError(cause)
          ? cause
          : new TextGenerationError({
              operation,
              detail: "Muse text generation failed.",
              cause,
            }),
      ),
      Effect.scoped,
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("MuseTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runMuseJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("MuseTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runMuseJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("MuseTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runMuseJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("MuseTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const generated = yield* runMuseJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
