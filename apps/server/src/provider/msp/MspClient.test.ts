import { describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { makeMspHost, newMspCommandId } from "./MspClient.ts";

const encodeUnknownJsonString = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeUnknownJsonString = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Unknown));

function makeMockMspEnvironment(options?: {
  readonly onMessage?: (
    rec: Record<string, unknown>,
    sendLine: (obj: Record<string, unknown>) => Effect.Effect<void>,
    exitDeferred: Deferred.Deferred<ChildProcessSpawner.ExitCode>,
  ) => Effect.Effect<void>;
  readonly requestTimeout?: Duration.Input | undefined;
  readonly handshakeTimeout?: Duration.Input | undefined;
}) {
  return Effect.gen(function* () {
    const stdoutQueue = yield* Queue.unbounded<Uint8Array>();
    const exitDeferred = yield* Deferred.make<ChildProcessSpawner.ExitCode>();
    const killDeferred = yield* Deferred.make<void>();

    const sendLine = (obj: Record<string, unknown>): Effect.Effect<void> =>
      Queue.offer(stdoutQueue, Buffer.from(encodeUnknownJsonString(obj) + "\n", "utf8")).pipe(
        Effect.asVoid,
      );

    const defaultHandler = (
      rec: Record<string, unknown>,
      send: (obj: Record<string, unknown>) => Effect.Effect<void>,
    ) =>
      Effect.gen(function* () {
        const id = rec.id;
        const method = rec.method;
        if (method === "initialize" && typeof id === "number") {
          yield* send({
            jsonrpc: "2.0",
            id,
            result: {
              serverInfo: { name: "muse-code", version: "1.0.3" },
              capabilities: {},
            },
          });
        } else if (method === "session/start" && typeof id === "number") {
          yield* send({
            jsonrpc: "2.0",
            id,
            result: {
              session: {
                sessionId: "test-sess-1",
                status: "ready",
                activeTurnId: null,
              },
              viewCursor: "cur-1",
            },
          });
        } else if (method === "turn/start" && typeof id === "number") {
          yield* send({
            jsonrpc: "2.0",
            id,
            result: {
              commandId: String(
                rec.params ? (rec.params as Record<string, unknown>).commandId : "cmd",
              ),
              disposition: "started",
              startedNewTurn: true,
              status: "running",
              turnId: "turn-1",
            },
          });
          yield* send({
            jsonrpc: "2.0",
            method: "turn/started",
            params: {
              sessionId: "test-sess-1",
              turnId: "turn-1",
            },
          });
        } else if (method === "model/list" && typeof id === "number") {
          yield* send({
            jsonrpc: "2.0",
            id,
            result: {
              models: [
                { modelId: "muse-spark-1.3", displayLabel: "Muse Spark 1.3", isDefault: true },
                { modelId: "echo", displayLabel: "Offline Echo", isDefault: false },
              ],
            },
          });
        }
      });

    const handler = options?.onMessage ?? defaultHandler;

    const stdinSink: ChildProcessSpawner.ChildProcessHandle["stdin"] = Sink.forEach(
      (chunk: Uint8Array): Effect.Effect<void> =>
        Effect.gen(function* () {
          const raw = Buffer.from(chunk).toString("utf8");
          for (const line of raw.split("\n")) {
            const trimmed = line.trim();
            if (!trimmed) continue;
            const msg = decodeUnknownJsonString(trimmed);
            if (msg._tag === "None") continue;
            const rec = msg.value as Record<string, unknown>;
            yield* handler(rec, sendLine, exitDeferred);
          }
        }),
    );

    const mockHandle = ChildProcessSpawner.makeHandle({
      pid: ChildProcessSpawner.ProcessId(12345),
      exitCode: Deferred.await(exitDeferred),
      isRunning: Effect.succeed(true),
      kill: () =>
        Deferred.succeed(killDeferred, undefined).pipe(
          Effect.flatMap(() => Deferred.succeed(exitDeferred, ChildProcessSpawner.ExitCode(0))),
          Effect.asVoid,
        ),
      unref: Effect.succeed(Effect.void),
      stdin: stdinSink,
      stdout: Stream.fromQueue(stdoutQueue),
      stderr: Stream.empty,
      all: Stream.empty,
      getInputFd: () => Sink.drain,
      getOutputFd: () => Stream.empty,
    });

    const mockSpawner = ChildProcessSpawner.make(() => Effect.succeed(mockHandle));

    const host = yield* makeMspHost({
      command: "muse",
      ...(options?.requestTimeout !== undefined ? { requestTimeout: options.requestTimeout } : {}),
      ...(options?.handshakeTimeout !== undefined
        ? { handshakeTimeout: options.handshakeTimeout }
        : {}),
    }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, mockSpawner));

    return {
      host,
      sendLine,
      exitDeferred,
      killDeferred,
    };
  });
}

describe("MspClient", () => {
  describe("newMspCommandId", () => {
    it("generates valid UUIDv7 format strings", () => {
      const id1 = newMspCommandId();
      const id2 = newMspCommandId();
      expect(id1).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
      expect(id2).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
      expect(id1).not.toBe(id2);
    });
  });

  describe("makeMspHost", () => {
    it.effect("completes initialize handshake and executes commands over stdio", () =>
      Effect.gen(function* () {
        const env = yield* makeMockMspEnvironment();

        expect(env.host.serverInfo).toEqual({
          name: "muse-code",
          version: "1.0.3",
        });

        // Test startSession
        const sessionResult = yield* env.host.startSession({
          commandId: newMspCommandId(),
          workspaceRoot: "/workspace",
        });
        expect(sessionResult.session.sessionId).toBe("test-sess-1");

        // Test startTurn
        const turnResult = yield* env.host.startTurn({
          commandId: newMspCommandId(),
          sessionId: sessionResult.session.sessionId,
          input: [{ type: "text", text: "Hello Muse" }],
        });
        expect(turnResult.turnId).toBe("turn-1");

        // Test notification streaming
        const notif = yield* Stream.take(env.host.notifications, 1).pipe(Stream.runHead);
        expect(notif._tag).toBe("Some");
        if (notif._tag === "Some") {
          expect(notif.value.method).toBe("turn/started");
          expect(notif.value.params.sessionId).toBe("test-sess-1");
        }

        // Test model list
        const models = yield* env.host.listModels();
        expect(models.length).toBe(2);
        expect(models[0]?.modelId).toBe("muse-spark-1.3");
        expect(models[1]?.modelId).toBe("echo");

        // Test close
        yield* env.host.close;
      }).pipe(Effect.scoped),
    );

    it.live("times out when request takes longer than requestTimeout", () =>
      Effect.gen(function* () {
        const env = yield* makeMockMspEnvironment({
          requestTimeout: "40 millis",
          onMessage: (rec, sendLine) =>
            Effect.gen(function* () {
              if (rec.method === "initialize" && typeof rec.id === "number") {
                yield* sendLine({
                  jsonrpc: "2.0",
                  id: rec.id,
                  result: { serverInfo: { name: "muse-code", version: "1.0.3" } },
                });
              }
              // Intentionally do not answer session/start to trigger timeout
            }),
        });

        const error = yield* Effect.flip(env.host.startSession({ commandId: newMspCommandId() }));
        expect(error._tag).toBe("MspTransportError");
        if (error._tag === "MspTransportError") {
          expect(error.detail).toContain("timed out after 40ms");
        }
      }).pipe(Effect.scoped),
    );

    it.effect("fails pending requests when child process exits unexpectedly", () =>
      Effect.gen(function* () {
        const env = yield* makeMockMspEnvironment({
          onMessage: (rec, sendLine, exitDeferred) =>
            Effect.gen(function* () {
              if (rec.method === "initialize" && typeof rec.id === "number") {
                yield* sendLine({
                  jsonrpc: "2.0",
                  id: rec.id,
                  result: { serverInfo: { name: "muse-code", version: "1.0.3" } },
                });
              }
              if (rec.method === "session/start") {
                yield* Deferred.succeed(exitDeferred, ChildProcessSpawner.ExitCode(1));
              }
            }),
        });

        const error = yield* Effect.flip(env.host.startSession({ commandId: newMspCommandId() }));
        expect(error._tag).toBe("MspTransportError");
        if (error._tag === "MspTransportError") {
          expect(error.detail).toContain("exited with code 1");
        }
      }).pipe(Effect.scoped),
    );

    it.effect("validates responses and fails when critical field is missing or empty", () =>
      Effect.gen(function* () {
        const env = yield* makeMockMspEnvironment({
          onMessage: (rec, sendLine) =>
            Effect.gen(function* () {
              if (rec.method === "initialize" && typeof rec.id === "number") {
                yield* sendLine({
                  jsonrpc: "2.0",
                  id: rec.id,
                  result: { serverInfo: { name: "muse-code", version: "1.0.3" } },
                });
              } else if (rec.method === "session/start" && typeof rec.id === "number") {
                // Return malformed response with empty sessionId
                yield* sendLine({
                  jsonrpc: "2.0",
                  id: rec.id,
                  result: {
                    session: {
                      sessionId: "",
                      status: "ready",
                    },
                    viewCursor: "cur-1",
                  },
                });
              }
            }),
        });

        const error = yield* Effect.flip(env.host.startSession({ commandId: newMspCommandId() }));
        expect(error._tag).toBe("MspRequestError");
        if (error._tag === "MspRequestError") {
          expect(error.detail).toContain("missing or empty required field 'session.sessionId'");
        }
      }).pipe(Effect.scoped),
    );

    it.effect("terminates child process when host.close is called", () =>
      Effect.gen(function* () {
        const env = yield* makeMockMspEnvironment();
        expect(yield* Deferred.isDone(env.killDeferred)).toBe(false);
        yield* env.host.close;
        expect(yield* Deferred.isDone(env.killDeferred)).toBe(true);
      }).pipe(Effect.scoped),
    );
  });
});
