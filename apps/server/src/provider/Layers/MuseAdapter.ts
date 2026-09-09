/**
 * MuseAdapter — implements ProviderAdapterShape over persistent MSP host (`muse serve`).
 *
 * Maps T3 session, turn, approval, user input, and event semantics to native
 * MSP operations over stdio.
 *
 * @module provider/Layers/MuseAdapter
 */
import {
  ApprovalRequestId,
  EventId,
  type IsoDateTime,
  type ModelSelection,
  type MuseSettings,
  type ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcessSpawner } from "effect/unstable/process";

import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  type ProviderAdapterError,
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type {
  ProviderAdapterCapabilities,
  ProviderCompaction,
} from "../Services/ProviderAdapter.ts";
import type { MuseAdapterShape } from "../Services/MuseAdapter.ts";
import {
  makeMspHost,
  type MspError,
  type MspHost,
  type MspHostOptions,
  newMspCommandId,
} from "../msp/MspClient.ts";
import {
  type MspApprovalRequest,
  type MspNotification,
  type MspTurnInputPart,
  type MspUserInputAnswer,
  type MspUserInputRequest,
} from "../msp/MspTypes.ts";
import {
  type MspEventStamp,
  makeMspAssistantItemCompletedEvent,
  makeMspAssistantItemStartedEvent,
  makeMspContentDeltaEvent,
  makeMspRequestOpenedEvent,
  makeMspRequestResolvedEvent,
  makeMspToolCallItemEvent,
  makeMspTurnCompletedEvent,
  makeMspTurnStartedEvent,
  makeMspUserInputRequestedEvent,
  makeMspUserInputResolvedEvent,
  mapMuseApprovalDecision,
} from "../msp/MspEvents.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("muse");

export interface MuseAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
  readonly nativeEventLogger?: EventNdjsonLogger;
  /** Injected host factory for focused tests. */
  readonly makeHost?: (
    options: MspHostOptions,
  ) => Effect.Effect<MspHost, MspError, Scope.Scope | ChildProcessSpawner.ChildProcessSpawner>;
}

interface PendingApproval {
  readonly request: MspApprovalRequest;
}

interface PendingUserInput {
  readonly request: MspUserInputRequest;
}

interface MuseSessionContext {
  readonly threadId: ThreadId;
  readonly mspSessionId: string;
  session: ProviderSession;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly pendingUserInputs: Map<ApprovalRequestId, PendingUserInput>;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  stopped: boolean;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const asRecord = (value: unknown): Record<string, unknown> => (isRecord(value) ? value : {});

function appendItemToTurn(ctx: MuseSessionContext, turnId: TurnId, item: unknown): void {
  const existing = ctx.turns.find((t) => t.id === turnId);
  if (existing) {
    existing.items.push(item);
  } else {
    ctx.turns.push({ id: turnId, items: [item] });
  }
}

export function selectMuseApprovalChoiceId(
  request: MspApprovalRequest,
  decision: ProviderApprovalDecision,
): string {
  const choices = request.availableChoices;
  if (choices.length === 0) {
    return "abort";
  }
  if (decision === "acceptAlways") {
    const alwaysChoice = choices.find(
      (c) => c.decision === "approvedPolicyAmendment" || c.scope === "localPersistent",
    );
    if (alwaysChoice) return alwaysChoice.choiceId;
  }
  if (decision === "acceptForSession" || decision === "acceptAlways") {
    const sessionChoice = choices.find(
      (c) => c.decision === "approvedForSession" || c.scope === "session",
    );
    if (sessionChoice) return sessionChoice.choiceId;
  }
  if (decision === "accept" || decision === "acceptForSession" || decision === "acceptAlways") {
    const approvedChoice = choices.find(
      (c) => c.decision === "approved" || c.decision === "approvedPolicyAmendment",
    );
    if (approvedChoice) return approvedChoice.choiceId;
  }
  if (decision === "decline") {
    const deniedChoice = choices.find((c) => c.decision === "denied");
    if (deniedChoice) return deniedChoice.choiceId;
  }
  // cancel or fallback
  const abortChoice = choices.find((c) => c.decision === "abort" || c.decision === "denied");
  return abortChoice ? abortChoice.choiceId : choices[0]!.choiceId;
}

export function makeMuseAdapter(
  museSettings: MuseSettings,
  options?: MuseAdapterLiveOptions,
): Effect.Effect<
  MuseAdapterShape,
  never,
  | ChildProcessSpawner.ChildProcessSpawner
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ServerConfig
  | Scope.Scope
> {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("muse");
    const crypto = yield* Crypto.Crypto;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const serverConfig = yield* ServerConfig;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const adapterScope = yield* Scope.Scope;

    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, MuseSessionContext>();
    const hostRef = yield* SynchronizedRef.make<Option.Option<MspHost>>(Option.none());

    const nowIso: Effect.Effect<IsoDateTime> = Effect.map(
      DateTime.now,
      DateTime.formatIso,
    ) as Effect.Effect<IsoDateTime>;

    const nextEventStamp = Effect.gen(function* () {
      const id = yield* crypto.randomUUIDv4;
      const createdAt = yield* nowIso;
      return { eventId: EventId.make(id), createdAt } satisfies MspEventStamp;
    });

    const offerEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const handleNotification = (notif: MspNotification) =>
      Effect.gen(function* () {
        const params = notif.params;
        const sessionId = typeof params.sessionId === "string" ? params.sessionId : undefined;
        if (!sessionId) return;

        let ctx: MuseSessionContext | undefined;
        for (const c of sessions.values()) {
          if (c.mspSessionId === sessionId) {
            ctx = c;
            break;
          }
        }
        if (!ctx || ctx.stopped) return;

        const threadId = ctx.threadId;
        const stamp = yield* nextEventStamp;

        switch (notif.method) {
          case "turn/started": {
            const turnId = TurnId.make(String(params.turnId ?? ""));
            ctx.activeTurnId = turnId;
            ctx.session = {
              ...ctx.session,
              status: "running",
              activeTurnId: turnId,
              updatedAt: stamp.createdAt,
            };
            yield* offerEvent(
              makeMspTurnStartedEvent({
                stamp,
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId,
                turnId,
                rawPayload: params,
              }),
            );
            break;
          }

          case "turn/completed": {
            const turnId = TurnId.make(String(params.turnId ?? ctx.activeTurnId ?? ""));
            const terminal = String(params.terminal ?? "completed");
            const state: "completed" | "failed" | "interrupted" | "cancelled" =
              terminal === "failed"
                ? "failed"
                : terminal === "interrupted"
                  ? "interrupted"
                  : "completed";
            const err = isRecord(params.error) ? (params.error as { message?: string }) : undefined;
            ctx.activeTurnId = undefined;
            const { activeTurnId: _activeTurnId, ...readySession } = ctx.session;
            ctx.session = {
              ...readySession,
              status: "ready",
              updatedAt: stamp.createdAt,
            };
            yield* offerEvent(
              makeMspTurnCompletedEvent({
                stamp,
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId,
                turnId,
                state,
                errorMessage:
                  err?.message ?? (typeof params.reason === "string" ? params.reason : undefined),
                rawPayload: params,
              }),
            );
            break;
          }

          case "item/started": {
            const item = isRecord(params.item) ? params.item : undefined;
            if (!item) break;
            const kind = String(item.kind ?? "");
            const itemId = String(item.itemId ?? "");
            const turnId = ctx.activeTurnId ?? TurnId.make(String(item.turnId ?? ""));
            if (kind === "agentMessage") {
              yield* offerEvent(
                makeMspAssistantItemStartedEvent({
                  stamp,
                  provider: PROVIDER,
                  providerInstanceId: boundInstanceId,
                  threadId,
                  turnId,
                  itemId,
                  rawPayload: params,
                }),
              );
            } else if (kind === "toolCall") {
              yield* offerEvent(
                makeMspToolCallItemEvent({
                  stamp,
                  provider: PROVIDER,
                  providerInstanceId: boundInstanceId,
                  threadId,
                  turnId,
                  itemId,
                  status: "inProgress",
                  toolName: typeof item.tool === "string" ? item.tool : undefined,
                  rawPayload: params,
                }),
              );
            }
            break;
          }

          case "item/delta": {
            const itemId = String(params.itemId ?? "");
            const delta = String(params.delta ?? "");
            const field = typeof params.field === "string" ? params.field : undefined;
            const turnId = ctx.activeTurnId ?? TurnId.make("");
            const streamKind = field?.startsWith("summary")
              ? ("reasoning_text" as const)
              : field === "output"
                ? ("command_output" as const)
                : ("assistant_text" as const);
            yield* offerEvent(
              makeMspContentDeltaEvent({
                stamp,
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId,
                turnId,
                itemId,
                streamKind,
                delta,
                rawPayload: params,
              }),
            );
            break;
          }

          case "item/completed": {
            const item = isRecord(params.item) ? params.item : undefined;
            if (!item) break;
            const kind = String(item.kind ?? "");
            const itemId = String(item.itemId ?? "");
            const turnId = ctx.activeTurnId ?? TurnId.make(String(item.turnId ?? ""));
            if (kind === "agentMessage") {
              yield* offerEvent(
                makeMspAssistantItemCompletedEvent({
                  stamp,
                  provider: PROVIDER,
                  providerInstanceId: boundInstanceId,
                  threadId,
                  turnId,
                  itemId,
                  rawPayload: params,
                }),
              );
            } else if (kind === "toolCall") {
              yield* offerEvent(
                makeMspToolCallItemEvent({
                  stamp,
                  provider: PROVIDER,
                  providerInstanceId: boundInstanceId,
                  threadId,
                  turnId,
                  itemId,
                  status: item.status === "completed" ? "completed" : "failed",
                  toolName: typeof item.tool === "string" ? item.tool : undefined,
                  visibleOutput:
                    typeof item.visibleOutput === "string" ? item.visibleOutput : undefined,
                  rawPayload: params,
                }),
              );
            }
            appendItemToTurn(ctx, turnId, item);
            break;
          }

          case "approval/requested":
          case "approval/request": {
            const req = params as unknown as MspApprovalRequest;
            const reqId = ApprovalRequestId.make(req.approvalId);
            ctx.pendingApprovals.set(reqId, { request: req });
            yield* offerEvent(
              makeMspRequestOpenedEvent({
                stamp,
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId,
                turnId: ctx.activeTurnId,
                approvalRequest: req,
                rawPayload: params,
              }),
            );
            break;
          }

          case "approval/resolved": {
            const reqId = ApprovalRequestId.make(String(params.approvalId ?? ""));
            ctx.pendingApprovals.delete(reqId);
            const decisionStr = String(params.decision ?? "");
            yield* offerEvent(
              makeMspRequestResolvedEvent({
                stamp,
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId,
                turnId: ctx.activeTurnId,
                approvalId: reqId,
                decision: mapMuseApprovalDecision(decisionStr),
              }),
            );
            break;
          }

          case "userInput/requested":
          case "userInput/request": {
            const req = params as unknown as MspUserInputRequest;
            const reqId = ApprovalRequestId.make(req.userInputId);
            ctx.pendingUserInputs.set(reqId, { request: req });
            yield* offerEvent(
              makeMspUserInputRequestedEvent({
                stamp,
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId,
                turnId: ctx.activeTurnId,
                userInputRequest: req,
                rawPayload: params,
              }),
            );
            break;
          }

          case "userInput/settled": {
            const reqId = ApprovalRequestId.make(String(params.userInputId ?? ""));
            ctx.pendingUserInputs.delete(reqId);
            const answers = asRecord(params.answers);
            yield* offerEvent(
              makeMspUserInputResolvedEvent({
                stamp,
                provider: PROVIDER,
                providerInstanceId: boundInstanceId,
                threadId,
                turnId: ctx.activeTurnId,
                userInputId: reqId,
                answers,
              }),
            );
            break;
          }
        }
      });

    const getHost: Effect.Effect<MspHost, ProviderAdapterProcessError> =
      SynchronizedRef.modifyEffect(hostRef, (current) => {
        if (Option.isSome(current)) {
          return Effect.succeed([current.value, current] as const);
        }
        const hostFactory = options?.makeHost ?? makeMspHost;
        return hostFactory({
          command: museSettings.binaryPath ?? "muse",
          ...(options?.environment ? { env: options.environment } : {}),
        }).pipe(
          Effect.provideService(Scope.Scope, adapterScope),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.tap((newHost) =>
            newHost.notifications.pipe(
              Stream.runForEach((notif) => handleNotification(notif)),
              Effect.ignore,
              Effect.forkIn(adapterScope),
            ),
          ),
          Effect.map((newHost) => [newHost, Option.some(newHost)] as const),
          Effect.mapError(
            (cause) =>
              new ProviderAdapterProcessError({
                provider: PROVIDER,
                threadId: "",
                detail: `Failed to spawn or initialize Muse MSP host: ${cause.detail ?? cause.message ?? String(cause)}`,
                cause,
              }),
          ),
        );
      });

    const requireSession = (threadId: ThreadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        if (!ctx || ctx.stopped) {
          throw new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId,
          });
        }
        return ctx;
      });

    const startSession: MuseAdapterShape["startSession"] = (input) =>
      Effect.gen(function* () {
        const host = yield* getHost;
        const commandId = newMspCommandId();
        const modelId = input.modelSelection?.model;
        const startResult = yield* host
          .startSession({
            commandId,
            ...(input.cwd ? { workspaceRoot: input.cwd } : {}),
            ...(modelId ? { modelId } : {}),
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/start",
                  detail: `Muse failed to start session for thread ${input.threadId}: ${cause.detail ?? cause.message ?? String(cause)}`,
                  cause,
                }),
            ),
          );

        const createdAt = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: boundInstanceId,
          status: "ready",
          runtimeMode: input.runtimeMode,
          cwd: input.cwd,
          model: modelId,
          threadId: input.threadId,
          createdAt,
          updatedAt: createdAt,
        };

        const ctx: MuseSessionContext = {
          threadId: input.threadId,
          mspSessionId: startResult.session.sessionId,
          session,
          pendingApprovals: new Map(),
          pendingUserInputs: new Map(),
          turns: [],
          activeTurnId: undefined,
          stopped: false,
        };

        sessions.set(input.threadId, ctx);
        return session;
      });

    const sendTurn: MuseAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(input.threadId);
        const host = yield* getHost;

        const turnParts: MspTurnInputPart[] = [
          {
            type: "text",
            ...(input.input !== undefined ? { text: input.input } : {}),
          },
        ];

        // Handle image attachments if present
        if (input.attachments && input.attachments.length > 0) {
          for (const att of input.attachments) {
            if (att.type === "image") {
              const fullPath = resolveAttachmentPath({
                attachmentsDir: serverConfig.attachmentsDir,
                attachment: att,
              });
              if (fullPath) {
                const exists = yield* fileSystem
                  .exists(fullPath)
                  .pipe(Effect.orElseSucceed(() => false));
                if (exists) {
                  const bytes = yield* fileSystem
                    .readFile(fullPath)
                    .pipe(Effect.orElseSucceed(() => null));
                  if (bytes) {
                    const base64Data = Buffer.from(bytes).toString("base64");
                    turnParts.push({
                      type: "image",
                      base64Data,
                      mediaType: att.mimeType,
                    });
                  }
                }
              }
            }
          }
        }

        const commandId = newMspCommandId();
        const ifBusy = ctx.activeTurnId ? ("steer" as const) : undefined;
        const reasoningEffort = input.modelSelection
          ? (getModelSelectionStringOptionValue(input.modelSelection, "reasoningEffort") ??
            undefined)
          : undefined;
        const result = yield* host
          .startTurn({
            commandId,
            sessionId: ctx.mspSessionId,
            input: turnParts,
            ...(ifBusy ? { ifBusy } : {}),
            ...(reasoningEffort ? { reasoningEffort } : {}),
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "turn/start",
                  detail: `Muse turn/start failed for thread ${input.threadId}: ${cause.detail ?? cause.message ?? String(cause)}`,
                  cause,
                }),
            ),
          );

        const turnId = TurnId.make(result.turnId);
        ctx.activeTurnId = turnId;
        ctx.session = {
          ...ctx.session,
          status: "running",
          activeTurnId: turnId,
          updatedAt: yield* nowIso,
        };

        return { threadId: input.threadId, turnId } satisfies ProviderTurnStartResult;
      });

    const interruptTurn: MuseAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const host = yield* getHost;
        const commandId = newMspCommandId();
        yield* host
          .interruptTurn({
            commandId,
            sessionId: ctx.mspSessionId,
            ...(turnId ? { turnId: String(turnId) } : {}),
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "turn/interrupt",
                  detail: `Muse interruptTurn failed for thread ${threadId}: ${cause.detail ?? cause.message ?? String(cause)}`,
                  cause,
                }),
            ),
          );
      });

    const respondToRequest: MuseAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "approval/decide",
            detail: `No pending approval found for request id ${requestId}`,
          });
        }
        const host = yield* getHost;
        const choiceId = selectMuseApprovalChoiceId(pending.request, decision);
        const commandId = newMspCommandId();
        yield* host
          .decideApproval({
            commandId,
            sessionId: ctx.mspSessionId,
            approvalId: pending.request.approvalId,
            choiceId,
            requirementId: pending.request.currentRequirementId,
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "approval/decide",
                  detail: `Muse decideApproval failed for thread ${threadId}: ${cause.detail ?? cause.message ?? String(cause)}`,
                  cause,
                }),
            ),
          );
        ctx.pendingApprovals.delete(requestId);
      });

    const respondToUserInput: MuseAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      answers,
    ) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        const pending = ctx.pendingUserInputs.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "userInput/answer",
            detail: `No pending user input found for request id ${requestId}`,
          });
        }
        const host = yield* getHost;
        const commandId = newMspCommandId();

        const answerList: MspUserInputAnswer[] = [];
        for (const [qId, ans] of Object.entries(answers)) {
          if (Array.isArray(ans)) {
            answerList.push({ questionId: qId, selectedLabels: ans.map(String) });
          } else if (typeof ans === "string") {
            answerList.push({ questionId: qId, selectedLabel: ans });
          } else if (isRecord(ans)) {
            const rec = ans as Record<string, unknown>;
            answerList.push({
              questionId: qId,
              ...(typeof rec.selectedLabel === "string"
                ? { selectedLabel: rec.selectedLabel }
                : {}),
              ...(typeof rec.freeText === "string" ? { freeText: rec.freeText } : {}),
              ...(typeof rec.note === "string" ? { note: rec.note } : {}),
            });
          }
        }

        if (answerList.length === 0) {
          // Cancelled user input
          yield* host
            .cancelUserInput({
              commandId,
              sessionId: ctx.mspSessionId,
              userInputId: pending.request.userInputId,
              reason: "Cancelled by user",
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "userInput/cancel",
                    detail: `Muse cancelUserInput failed for thread ${threadId}: ${cause.detail ?? cause.message ?? String(cause)}`,
                    cause,
                  }),
              ),
            );
        } else {
          yield* host
            .answerUserInput({
              commandId,
              sessionId: ctx.mspSessionId,
              userInputId: pending.request.userInputId,
              answers: answerList,
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "userInput/answer",
                    detail: `Muse answerUserInput failed for thread ${threadId}: ${cause.detail ?? cause.message ?? String(cause)}`,
                    cause,
                  }),
              ),
            );
        }
        ctx.pendingUserInputs.delete(requestId);
      });

    const stopSession: MuseAdapterShape["stopSession"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = sessions.get(threadId);
        if (!ctx) return;
        ctx.stopped = true;
        const hostOpt = yield* SynchronizedRef.get(hostRef);
        if (Option.isSome(hostOpt)) {
          yield* hostOpt.value.unsubscribeView({ sessionId: ctx.mspSessionId }).pipe(Effect.ignore);
        }
        sessions.delete(threadId);
      });

    const stopAll: MuseAdapterShape["stopAll"] = () =>
      Effect.gen(function* () {
        for (const threadId of [...sessions.keys()]) {
          yield* stopSession(threadId);
        }
        const currentHost = yield* SynchronizedRef.get(hostRef);
        if (Option.isSome(currentHost)) {
          yield* currentHost.value.close.pipe(Effect.ignore);
          yield* SynchronizedRef.set(hostRef, Option.none());
        }
      });

    const listSessions: MuseAdapterShape["listSessions"] = () =>
      Effect.sync(() => [...sessions.values()].filter((c) => !c.stopped).map((c) => c.session));

    const hasSession: MuseAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => sessions.has(threadId) && !sessions.get(threadId)?.stopped);

    const readThread: MuseAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* requireSession(threadId);
        return {
          threadId,
          turns: ctx.turns,
        };
      });

    const rollbackThread: MuseAdapterShape["rollbackThread"] = (_threadId, _numTurns) =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "rollbackThread",
          issue: "Muse does not support conversation rewind. Start a new thread instead.",
        }),
      );

    const compaction: ProviderCompaction<ProviderAdapterError> = {
      type: "native",
      start: (threadId: ThreadId, _modelSelection?: ProviderSendTurnInput["modelSelection"]) =>
        Effect.gen(function* () {
          const ctx = yield* requireSession(threadId);
          const host = yield* getHost;
          const commandId = newMspCommandId();
          yield* host.compact({ commandId, sessionId: ctx.mspSessionId }).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/compact",
                  detail: `Muse compaction failed for thread ${threadId}: ${cause.detail ?? cause.message ?? String(cause)}`,
                  cause,
                }),
            ),
          );
        }),
    };

    const capabilities: ProviderAdapterCapabilities = {
      sessionModelSwitch: "in-session",
      supportsConversationRollback: false,
      promptlessTurnContinuation: false,
    };

    return {
      provider: PROVIDER,
      capabilities,
      startSession,
      sendTurn,
      compaction,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies MuseAdapterShape;
  });
}
