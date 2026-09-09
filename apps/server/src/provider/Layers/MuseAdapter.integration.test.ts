// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { makeMuseAdapter } from "./MuseAdapter.ts";

function resolveMuseBinary(): string | undefined {
  const envBinary = process.env.MUSE_BINARY;
  if (envBinary && envBinary.trim().length > 0) {
    return envBinary.trim();
  }
  try {
    const result = NodeChildProcess.spawnSync("muse", ["--version"], {
      stdio: "ignore",
      encoding: "utf8",
    });
    if (result.status === 0) {
      return "muse";
    }
  } catch {
    // ignore
  }
  return undefined;
}

const resolvedMuseBinary = resolveMuseBinary();

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-muse-integration-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

describe.skipIf(!resolvedMuseBinary)("MuseAdapter live integration with muse serve", () => {
  it.layer(testLayer)(
    "spawns live muse serve, creates session and executes turn with echo model",
    (it) => {
      it.effect("communicates with real muse serve", () =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const tempDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-muse-live-" });

          const adapter = yield* makeMuseAdapter(
            {
              enabled: true,
              binaryPath: resolvedMuseBinary!,
              customModels: [],
            },
            {
              instanceId: ProviderInstanceId.make("muse-live-test"),
            },
          );

          expect(adapter.provider).toBe(ProviderDriverKind.make("muse"));

          const eventsQueue = yield* Queue.unbounded<any>();
          yield* adapter.streamEvents.pipe(
            Stream.runForEach((event) => Queue.offer(eventsQueue, event)),
            Effect.forkScoped,
          );
          yield* Effect.yieldNow;

          const threadId = ThreadId.make("live-smoke-thread");
          const session = yield* adapter.startSession({
            threadId,
            cwd: tempDir,
            runtimeMode: "approval-required",
            modelSelection: {
              instanceId: ProviderInstanceId.make("muse-live-test"),
              model: "echo",
            },
          });

          expect(session.threadId).toBe(threadId);
          expect(session.status).toBe("ready");

          // Send a turn with the echo model
          const turnResult = yield* adapter.sendTurn({
            threadId,
            input: "Hello from T3 live test",
          });

          expect(turnResult.threadId).toBe(threadId);
          expect(turnResult.turnId).toBeDefined();

          // Collect events until turn completes
          const receivedEvents: Array<{ type: string; payload?: unknown }> = [];
          let completed = false;
          const startTime = yield* Clock.currentTimeMillis;
          const deadline = startTime + 15000;

          while (!completed) {
            const now = yield* Clock.currentTimeMillis;
            if (now >= deadline) {
              break;
            }
            const event = yield* Queue.take(eventsQueue);
            receivedEvents.push({ type: event.type, payload: event.payload });
            if (event.type === "turn.completed") {
              completed = true;
            }
          }

          expect(completed).toBe(true);
          expect(receivedEvents.some((e) => e.type === "turn.started")).toBe(true);
          expect(receivedEvents.some((e) => e.type === "turn.completed")).toBe(true);

          // Verify session returns to ready
          const sessions = yield* adapter.listSessions();
          const active = sessions.find((s) => s.threadId === threadId);
          expect(active?.status).toBe("ready");

          // Clean up
          yield* adapter.stopAll();
        }).pipe(Effect.scoped),
      );
    },
  );
});
