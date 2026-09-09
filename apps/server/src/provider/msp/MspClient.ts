/**
 * MspClient — minimal typed JSON-RPC 2.0 client for `muse serve` (MSP).
 *
 * The host speaks newline-delimited JSON-RPC over stdio: requests carry a
 * numeric `id`, commands additionally carry a UUIDv7 `commandId` for
 * idempotent admission, and view events arrive as server-to-client
 * notifications on the same stream. `session/start` and `session/resume`
 * subscribe this connection to the session's view; `view/unsubscribe`
 * removes it.
 *
 * This file intentionally models only the MSP surface T3 needs
 * (lifecycle, turns, approvals, user input, models, paging). It is not a
 * full MSP SDK: consult `muse schema generate-ts` (kept out of the repo)
 * before extending it.
 *
 * @module provider/msp/MspClient
 */
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { resolveSpawnCommand } from "@t3tools/shared/shell";

const decodeJsonOption = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));
const encodeJsonString = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

/** Transport failure: the host died, or a frame could not be decoded. */
export class MspTransportError extends Schema.TaggedError<MspTransportError>()(
  "MspTransportError",
  {
    operation: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/** The host answered a request with a JSON-RPC error. */
export class MspRequestError extends Schema.TaggedError<MspRequestError>()("MspRequestError", {
  method: Schema.String,
  code: Schema.Number,
  detail: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

export type MspError = MspTransportError | MspRequestError;

export type {
  MspApprovalChoice,
  MspApprovalRequest,
  MspApprovalRequirementRef,
  MspApprovalSubject,
  MspItem,
  MspModelInfo,
  MspNotification,
  MspSessionStartParams,
  MspSessionStartResult,
  MspSessionSummary,
  MspTurnInputPart,
  MspTurnStartParams,
  MspTurnStartResult,
  MspUserInputAnswer,
  MspUserInputQuestion,
  MspUserInputQuestionOption,
  MspUserInputRequest,
} from "./MspTypes.ts";

import type {
  MspApprovalChoice,
  MspApprovalRequest,
  MspApprovalRequirementRef,
  MspApprovalSubject,
  MspModelInfo,
  MspNotification,
  MspSessionStartParams,
  MspSessionStartResult,
  MspSessionSummary,
  MspTurnInputPart,
  MspTurnStartParams,
  MspTurnStartResult,
  MspUserInputAnswer,
  MspUserInputRequest,
} from "./MspTypes.ts";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const asRecord = (value: unknown): Record<string, unknown> => (isRecord(value) ? value : {});

/** UUIDv7 for MSP `commandId` idempotency handles (ms timestamp + random). */
export const newMspCommandId = (): string => {
  const now = DateTime.toEpochMillis(DateTime.nowUnsafe());
  const rand = new Uint8Array(10);
  globalThis.crypto.getRandomValues(rand);
  const hex = (bytes: Uint8Array): string =>
    Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  const timeHex = now.toString(16).padStart(12, "0");
  const randHex = hex(rand);
  const part1 = timeHex.slice(0, 8);
  const part2 = timeHex.slice(8, 12);
  const part3 = "7" + randHex.slice(0, 3);
  const varByte = ((rand[2]! & 0x3f) | 0x80).toString(16).padStart(2, "0");
  const part4 = varByte + randHex.slice(3, 5);
  const part5 = randHex.slice(5, 17);
  return `${part1}-${part2}-${part3}-${part4}-${part5}`;
};

export interface MspHostOptions {
  readonly command: string;
  readonly args?: ReadonlyArray<string> | undefined;
  readonly cwd?: string | undefined;
  readonly env?: NodeJS.ProcessEnv | undefined;
  readonly clientName?: string | undefined;
  readonly clientVersion?: string | undefined;
}

export interface MspHost {
  readonly serverInfo: Record<string, unknown>;
  readonly notifications: Stream.Stream<MspNotification, MspError>;
  readonly request: (
    method: string,
    params: Record<string, unknown>,
  ) => Effect.Effect<unknown, MspError>;
  readonly notify: (
    method: string,
    params?: Record<string, unknown> | undefined,
  ) => Effect.Effect<void, MspError>;
  readonly startSession: (
    params: MspSessionStartParams,
  ) => Effect.Effect<MspSessionStartResult, MspError>;
  readonly startTurn: (params: MspTurnStartParams) => Effect.Effect<MspTurnStartResult, MspError>;
  readonly interruptTurn: (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly turnId?: string | undefined;
    readonly retract?: boolean | undefined;
  }) => Effect.Effect<
    { readonly commandId: string; readonly status: string; readonly turnId: string },
    MspError
  >;
  readonly setModel: (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly modelId: string;
  }) => Effect.Effect<{ readonly commandId: string; readonly status: string }, MspError>;
  readonly compact: (params: {
    readonly commandId: string;
    readonly sessionId: string;
  }) => Effect.Effect<{ readonly commandId: string; readonly status: string }, MspError>;
  readonly decideApproval: (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly approvalId: string;
    readonly choiceId: string;
    readonly requirementId: MspApprovalRequirementRef;
    readonly feedback?: string | null | undefined;
  }) => Effect.Effect<
    { readonly commandId: string; readonly status: string; readonly terminal: boolean },
    MspError
  >;
  readonly answerUserInput: (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly userInputId: string;
    readonly answers: ReadonlyArray<MspUserInputAnswer>;
  }) => Effect.Effect<{ readonly commandId: string; readonly status: string }, MspError>;
  readonly cancelUserInput: (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly userInputId: string;
    readonly reason?: string | undefined;
  }) => Effect.Effect<{ readonly commandId: string; readonly status: string }, MspError>;
  readonly listModels: () => Effect.Effect<ReadonlyArray<MspModelInfo>, MspError>;
  readonly unsubscribeView: (params: {
    readonly sessionId: string;
  }) => Effect.Effect<void, MspError>;
  readonly close: Effect.Effect<void>;
}

export const makeMspHost = Effect.fn("makeMspHost")(function* (options: MspHostOptions) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runtimeScope = yield* Scope.Scope;

  const spawnCommand = yield* resolveSpawnCommand(
    options.command,
    options.args ? [...options.args] : ["serve"],
    {
      ...(options.env ? { env: options.env } : {}),
      extendEnv: true,
    },
  ).pipe(
    Effect.mapError(
      (cause) =>
        new MspTransportError({
          operation: "resolveSpawnCommand",
          detail: `Failed to resolve Muse command '${options.command}'`,
          cause,
        }),
    ),
  );

  const child = yield* spawner
    .spawn(
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        ...(options.cwd ? { cwd: options.cwd } : {}),
        ...(options.env ? { env: options.env } : {}),
        extendEnv: true,
        shell: spawnCommand.shell,
      }),
    )
    .pipe(
      Effect.provideService(Scope.Scope, runtimeScope),
      Effect.mapError(
        (cause) =>
          new MspTransportError({
            operation: "spawn",
            detail: `Failed to spawn Muse process '${spawnCommand.command}'`,
            cause,
          }),
      ),
    );

  const writeQueue = yield* Queue.unbounded<string>();
  const pendingRequests = new Map<
    number,
    { method: string; deferred: Deferred.Deferred<unknown, MspError> }
  >();
  const notificationPubSub = yield* PubSub.unbounded<MspNotification>();
  const isClosedRef = yield* Ref.make(false);
  let nextRequestId = 1;

  // Background writer: consumes lines from writeQueue and sends to child.stdin
  yield* Stream.fromQueue(writeQueue).pipe(
    Stream.encodeText,
    Stream.run(child.stdin),
    Effect.ignore,
    Effect.forkScoped,
  );

  const failAllPending = (detail: string, defect?: unknown) =>
    Effect.gen(function* () {
      const error = new MspTransportError({
        operation: "process",
        detail,
        ...(defect !== undefined ? { cause: defect } : {}),
      });
      for (const [id, pending] of pendingRequests.entries()) {
        pendingRequests.delete(id);
        yield* Deferred.fail(pending.deferred, error).pipe(Effect.ignore);
      }
    });

  // Background reader for stdout: newline-delimited JSON-RPC
  yield* child.stdout.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.runForEach((rawLine) =>
      Effect.gen(function* () {
        const line = rawLine.trim();
        if (!line) return;

        const parsedOption = decodeJsonOption(line);
        if (Option.isNone(parsedOption)) return;
        const parsed = parsedOption.value;

        if (!isRecord(parsed)) return;

        const id = parsed.id;
        if (typeof id === "number") {
          const pending = pendingRequests.get(id);
          if (pending) {
            pendingRequests.delete(id);
            if (isRecord(parsed.error)) {
              const code = typeof parsed.error.code === "number" ? parsed.error.code : -32603;
              const detail =
                typeof parsed.error.message === "string"
                  ? parsed.error.message
                  : encodeJsonString(parsed.error);
              yield* Deferred.fail(
                pending.deferred,
                new MspRequestError({
                  method: pending.method,
                  code,
                  detail,
                }),
              );
            } else {
              yield* Deferred.succeed(pending.deferred, parsed.result);
            }
          }
          return;
        }

        // Notification frame from server
        if (typeof parsed.method === "string") {
          const params = asRecord(parsed.params);
          yield* PubSub.publish(notificationPubSub, {
            method: parsed.method,
            params,
          });
        }
      }),
    ),
    Effect.catch((cause: unknown) => failAllPending("MSP stdout stream ended unexpectedly", cause)),
    Effect.forkScoped,
  );

  // Background reader for stderr (drain silently)
  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.runDrain,
    Effect.ignore,
    Effect.forkScoped,
  );

  // Watch exitCode
  yield* child.exitCode.pipe(
    Effect.flatMap((code) =>
      Effect.gen(function* () {
        yield* Ref.set(isClosedRef, true);
        yield* failAllPending(`Muse process exited with code ${code}`);
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );

  const request = (
    method: string,
    params: Record<string, unknown>,
  ): Effect.Effect<unknown, MspError> =>
    Effect.gen(function* () {
      if (yield* Ref.get(isClosedRef)) {
        return yield* new MspTransportError({
          operation: method,
          detail: "MSP host is closed",
        });
      }
      const id = nextRequestId++;
      const deferred = yield* Deferred.make<unknown, MspError>();
      pendingRequests.set(id, { method, deferred });
      const frame = encodeJsonString({ jsonrpc: "2.0", id, method, params }) + "\n";
      yield* Queue.offer(writeQueue, frame).pipe(
        Effect.mapError(
          (cause) =>
            new MspTransportError({
              operation: method,
              detail: `Failed to queue request ${method}`,
              cause,
            }),
        ),
      );
      return yield* Deferred.await(deferred);
    });

  const notify = (
    method: string,
    params?: Record<string, unknown>,
  ): Effect.Effect<void, MspError> =>
    Effect.gen(function* () {
      if (yield* Ref.get(isClosedRef)) {
        return;
      }
      const frame = encodeJsonString({ jsonrpc: "2.0", method, params: params ?? {} }) + "\n";
      yield* Queue.offer(writeQueue, frame).pipe(
        Effect.mapError(
          (cause) =>
            new MspTransportError({
              operation: method,
              detail: `Failed to send notification ${method}`,
              cause,
            }),
        ),
      );
    });

  // Perform handshake: initialize request followed by initialized notification
  const initResultRaw = yield* request("initialize", {
    clientInfo: {
      name: options.clientName ?? "t3code",
      version: options.clientVersion ?? "0.0.1",
    },
  });
  const initResult = asRecord(initResultRaw);
  const serverInfo = asRecord(initResult.serverInfo);

  // Conform to MSP protocol: client acknowledges initialize with `initialized` notification
  yield* notify("initialized");

  const close = Effect.gen(function* () {
    const wasClosed = yield* Ref.getAndSet(isClosedRef, true);
    if (wasClosed) return;
    yield* Queue.shutdown(writeQueue);
    yield* failAllPending("MSP client closed by user");
  });

  yield* Effect.addFinalizer(() => close);

  const startSession = (params: MspSessionStartParams) =>
    request("session/start", { ...params }).pipe(
      Effect.map((res): MspSessionStartResult => {
        const r = asRecord(res);
        const s = asRecord(r.session);
        return {
          session: {
            sessionId: String(s.sessionId ?? ""),
            status: String(s.status ?? "idle"),
            activeTurnId: s.activeTurnId ? String(s.activeTurnId) : null,
            modelId: s.modelId ? String(s.modelId) : undefined,
            workspaceRoot: s.workspaceRoot ? String(s.workspaceRoot) : undefined,
            providerId: s.providerId ? String(s.providerId) : undefined,
          },
          viewCursor: String(r.viewCursor ?? ""),
        };
      }),
    );

  const startTurn = (params: MspTurnStartParams) =>
    request("turn/start", { ...params }).pipe(
      Effect.map((res): MspTurnStartResult => {
        const r = asRecord(res);
        return {
          commandId: String(r.commandId ?? params.commandId),
          disposition: String(r.disposition ?? "started"),
          startedNewTurn: Boolean(r.startedNewTurn ?? true),
          status: String(r.status ?? "accepted"),
          turnId: String(r.turnId ?? params.commandId),
        };
      }),
    );

  const interruptTurn = (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly turnId?: string | undefined;
    readonly retract?: boolean | undefined;
  }) =>
    request("turn/interrupt", { ...params }).pipe(
      Effect.map((res) => {
        const r = asRecord(res);
        return {
          commandId: String(r.commandId ?? params.commandId),
          status: String(r.status ?? "accepted"),
          turnId: String(r.turnId ?? params.turnId ?? ""),
        };
      }),
    );

  const setModel = (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly modelId: string;
  }) =>
    request("session/setModel", { ...params }).pipe(
      Effect.map((res) => {
        const r = asRecord(res);
        return {
          commandId: String(r.commandId ?? params.commandId),
          status: String(r.status ?? "accepted"),
        };
      }),
    );

  const compact = (params: { readonly commandId: string; readonly sessionId: string }) =>
    request("session/compact", { ...params }).pipe(
      Effect.map((res) => {
        const r = asRecord(res);
        return {
          commandId: String(r.commandId ?? params.commandId),
          status: String(r.status ?? "accepted"),
        };
      }),
    );

  const decideApproval = (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly approvalId: string;
    readonly choiceId: string;
    readonly requirementId: MspApprovalRequirementRef;
    readonly feedback?: string | null | undefined;
  }) =>
    request("approval/decide", { ...params }).pipe(
      Effect.map((res) => {
        const r = asRecord(res);
        return {
          commandId: String(r.commandId ?? params.commandId),
          status: String(r.status ?? "accepted"),
          terminal: Boolean(r.terminal ?? true),
        };
      }),
    );

  const answerUserInput = (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly userInputId: string;
    readonly answers: ReadonlyArray<MspUserInputAnswer>;
  }) =>
    request("userInput/answer", { ...params }).pipe(
      Effect.map((res) => {
        const r = asRecord(res);
        return {
          commandId: String(r.commandId ?? params.commandId),
          status: String(r.status ?? "accepted"),
        };
      }),
    );

  const cancelUserInput = (params: {
    readonly commandId: string;
    readonly sessionId: string;
    readonly userInputId: string;
    readonly reason?: string | undefined;
  }) =>
    request("userInput/cancel", { ...params }).pipe(
      Effect.map((res) => {
        const r = asRecord(res);
        return {
          commandId: String(r.commandId ?? params.commandId),
          status: String(r.status ?? "accepted"),
        };
      }),
    );

  const listModels = () =>
    request("model/list", {}).pipe(
      Effect.map((res): ReadonlyArray<MspModelInfo> => {
        const r = asRecord(res);
        const models = Array.isArray(r.models) ? r.models : [];
        return models.map((m: unknown): MspModelInfo => {
          const rec = asRecord(m);
          return {
            modelId: String(rec.modelId ?? ""),
            displayLabel: String(rec.displayLabel ?? rec.modelId ?? ""),
            providerId: rec.providerId ? String(rec.providerId) : undefined,
            profileId: rec.profileId ? String(rec.profileId) : undefined,
            contextLimit: typeof rec.contextLimit === "number" ? rec.contextLimit : undefined,
            outputLimit: typeof rec.outputLimit === "number" ? rec.outputLimit : undefined,
            isDefault: Boolean(rec.isDefault),
          };
        });
      }),
    );

  const unsubscribeView = (params: { readonly sessionId: string }) =>
    request("view/unsubscribe", { ...params }).pipe(Effect.asVoid);

  return {
    serverInfo,
    notifications: Stream.fromPubSub(notificationPubSub),
    request,
    notify,
    startSession,
    startTurn,
    interruptTurn,
    setModel,
    compact,
    decideApproval,
    answerUserInput,
    cancelUserInput,
    listModels,
    unsubscribeView,
    close,
  } as MspHost;
});
