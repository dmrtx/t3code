// @effect-diagnostics nodeBuiltinImport:off
import * as NodeChildProcess from "node:child_process";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { ProviderDriverKind, ProviderInstanceId, ThreadId, type TurnId } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
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

function waitForTurnCompleted(
  eventsQueue: Queue.Queue<any>,
  targetTurnId: TurnId,
  timeoutDuration: Duration.Input = "15 seconds",
) {
  return Effect.gen(function* () {
    const receivedEvents: Array<any> = [];
    while (true) {
      const event = yield* Queue.take(eventsQueue);
      receivedEvents.push(event);
      if (event.type === "turn.completed" && String(event.turnId) === String(targetTurnId)) {
        return { completedEvent: event, receivedEvents };
      }
    }
  }).pipe(
    Effect.timeoutOrElse({
      duration: timeoutDuration,
      orElse: () =>
        Effect.die(
          `Timed out waiting for turn.completed for turn ${targetTurnId} after ${Duration.toMillis(Duration.fromInputUnsafe(timeoutDuration))}ms`,
        ),
    }),
  );
}

describe.skipIf(!resolvedMuseBinary)("MuseAdapter live integration with muse serve", () => {
  it.layer(testLayer, { excludeTestServices: true })(
    "spawns live muse serve, creates session and executes multiple turns with echo model",
    (it) => {
      it.effect(
        "communicates with real muse serve and maintains same-session continuity across turns",
        () =>
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

            const threadId = ThreadId.make("live-continuity-thread");

            // 1. Start session
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

            // 2. Send turn #1 using echo
            const turn1 = yield* adapter.sendTurn({
              threadId,
              input: "Hello from turn 1",
            });

            expect(turn1.threadId).toBe(threadId);
            expect(turn1.turnId).toBeDefined();

            // Wait for turn #1 turn.completed event
            const turn1Result = yield* waitForTurnCompleted(
              eventsQueue,
              turn1.turnId,
              "15 seconds",
            );
            expect(turn1Result.completedEvent.turnId).toBe(turn1.turnId);
            expect(turn1Result.receivedEvents.some((e) => e.type === "turn.started")).toBe(true);
            expect(turn1Result.receivedEvents.some((e) => e.type === "turn.completed")).toBe(true);

            // Verify session returns to ready after turn #1
            const sessionsAfterTurn1 = yield* adapter.listSessions();
            expect(sessionsAfterTurn1.length).toBe(1);
            expect(sessionsAfterTurn1[0]?.threadId).toBe(threadId);
            expect(sessionsAfterTurn1[0]?.status).toBe("ready");

            // 3. Send turn #2 through the SAME T3 thread/session
            const turn2 = yield* adapter.sendTurn({
              threadId,
              input: "Hello from turn 2 on the same session",
            });

            expect(turn2.threadId).toBe(threadId);
            expect(turn2.turnId).toBeDefined();

            // Verify two distinct turn IDs
            expect(String(turn1.turnId)).not.toBe(String(turn2.turnId));

            // Wait for turn #2 turn.completed event
            const turn2Result = yield* waitForTurnCompleted(
              eventsQueue,
              turn2.turnId,
              "15 seconds",
            );
            expect(turn2Result.completedEvent.turnId).toBe(turn2.turnId);
            expect(turn2Result.receivedEvents.some((e) => e.type === "turn.started")).toBe(true);
            expect(turn2Result.receivedEvents.some((e) => e.type === "turn.completed")).toBe(true);

            // 4. Verify adapter still owns exactly the same session/thread
            const sessionsAfterTurn2 = yield* adapter.listSessions();
            expect(sessionsAfterTurn2.length).toBe(1);
            expect(sessionsAfterTurn2[0]?.threadId).toBe(threadId);
            expect(sessionsAfterTurn2[0]?.status).toBe("ready");

            // 5. Clean stop
            yield* adapter.stopSession(threadId);
            expect(yield* adapter.hasSession(threadId)).toBe(false);

            yield* adapter.stopAll();
          }).pipe(Effect.scoped),
      );
    },
  );
});
