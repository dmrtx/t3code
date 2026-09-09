// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { ServerConfig } from "../../config.ts";
import { type MspHost, type MspNotification, MspTransportError } from "../msp/MspClient.ts";
import type {
  MspModelInfo,
  MspSessionStartResult,
  MspTurnStartResult,
} from "../msp/MspTypes.ts";
import { makeMuseAdapter } from "./MuseAdapter.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-muse-adapter-race-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeRaceMockHost = Effect.gen(function* () {
  const notifications = yield* PubSub.unbounded<MspNotification>();
  let turnCounter = 0;
  const calls = {
    interruptTurn: 0,
    failInterrupt: false,
    unsubscribeView: 0,
    close: 0,
  };

  const host: MspHost = {
    serverInfo: { name: "mock-muse", version: "1.0.3" },
    notifications: Stream.fromPubSub(notifications),
    request: () => Effect.succeed({}),
    notify: () => Effect.void,
    startSession: (params) =>
      Effect.succeed({
        session: {
          sessionId: "msp-race-session",
          status: "ready",
          activeTurnId: null,
          workspaceRoot: params.workspaceRoot,
        },
        viewCursor: "cursor-race",
      } satisfies MspSessionStartResult),
    resumeSession: (params) =>
      Effect.succeed({
        session: {
          sessionId: params.sessionId,
          status: "ready",
          activeTurnId: null,
        },
        viewCursor: "cursor-race",
      }),
    startTurn: (params) =>
      Effect.sync(() => {
        turnCounter += 1;
        return {
          commandId: params.commandId,
          disposition: "started",
          startedNewTurn: true,
          status: "running",
          turnId: `msp-race-turn-${turnCounter}`,
        } satisfies MspTurnStartResult;
      }),
    interruptTurn: (params) =>
      Effect.gen(function* () {
        calls.interruptTurn += 1;
        if (calls.failInterrupt) {
          return yield* Effect.fail(
            new MspTransportError({
              operation: "turn/interrupt",
              detail: "Simulated stopAll interrupt failure",
            }),
          );
        }
        return {
          commandId: params.commandId,
          status: "interrupted",
          turnId: params.turnId ?? `msp-race-turn-${turnCounter}`,
        };
      }),
    cancelTurn: (params) =>
      Effect.succeed({
        commandId: params.commandId,
        status: "cancelled",
        turnId: params.turnId ?? `msp-race-turn-${turnCounter}`,
      }),
    setModel: (params) => Effect.succeed({ commandId: params.commandId, status: "accepted" }),
    compact: (params) => Effect.succeed({ commandId: params.commandId, status: "accepted" }),
    decideApproval: (params) =>
      Effect.succeed({ commandId: params.commandId, status: "accepted", terminal: true }),
    answerUserInput: (params) =>
      Effect.succeed({ commandId: params.commandId, status: "accepted" }),
    cancelUserInput: (params) =>
      Effect.succeed({ commandId: params.commandId, status: "cancelled" }),
    listModels: () =>
      Effect.succeed([
        { modelId: "muse-spark-1.3", displayLabel: "Muse Spark 1.3", isDefault: true },
      ] satisfies ReadonlyArray<MspModelInfo>),
    unsubscribeView: () =>
      Effect.sync(() => {
        calls.unsubscribeView += 1;
      }),
    close: Effect.sync(() => {
      calls.close += 1;
    }),
  };

  return {
    host,
    calls,
    emit: (notification: MspNotification) => PubSub.publish(notifications, notification),
  };
});

it.layer(testLayer)("MuseAdapter race hardening", (it) => {
  it.effect("late completion from an older turn cannot clear a newer active turn", () =>
    Effect.gen(function* () {
      const mock = yield* makeRaceMockHost;
      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-race"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );
      const threadId = ThreadId.make("race-late-completion");
      const events = yield* Queue.unbounded<any>();
      yield* adapter.streamEvents.pipe(
        Stream.runForEach((event) => Queue.offer(events, event)),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;

      yield* adapter.startSession({ threadId, cwd: "/test", runtimeMode: "approval-required" });
      const turn1 = yield* adapter.sendTurn({ threadId, input: "first" });
      const turn2 = yield* adapter.sendTurn({ threadId, input: "steer/newer" });
      expect(String(turn1.turnId)).not.toBe(String(turn2.turnId));

      yield* mock.emit({
        method: "turn/completed",
        params: {
          sessionId: "msp-race-session",
          turnId: String(turn1.turnId),
          terminal: "completed",
        },
      });
      const completed = yield* Queue.take(events);
      expect(completed.type).toBe("turn.completed");
      expect(completed.turnId).toBe(turn1.turnId);

      const sessions = yield* adapter.listSessions();
      expect(sessions[0]?.status).toBe("running");
      expect(sessions[0]?.activeTurnId).toBe(turn2.turnId);

      yield* mock.emit({
        method: "item/delta",
        params: {
          sessionId: "msp-race-session",
          turnId: String(turn1.turnId),
          itemId: "late-item",
          delta: "late output",
        },
      });
      const lateDelta = yield* Queue.take(events);
      expect(lateDelta.type).toBe("content.delta");
      expect(lateDelta.turnId).toBe(turn1.turnId);

      yield* mock.emit({
        method: "turn/completed",
        params: {
          sessionId: "msp-race-session",
          turnId: String(turn2.turnId),
          terminal: "completed",
        },
      });
      yield* Queue.take(events);
      yield* adapter.stopSession(threadId);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );

  it.effect("sendTurn is rejected while stopSession owns the session shutdown", () =>
    Effect.gen(function* () {
      const mock = yield* makeRaceMockHost;
      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-race"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );
      const threadId = ThreadId.make("race-send-during-stop");

      yield* adapter.startSession({ threadId, cwd: "/test", runtimeMode: "approval-required" });
      const turn = yield* adapter.sendTurn({ threadId, input: "long running" });
      const stopFiber = yield* adapter.stopSession(threadId).pipe(Effect.forkScoped);
      yield* Effect.yieldNow;
      expect(mock.calls.interruptTurn).toBe(1);

      const sendExit = yield* adapter
        .sendTurn({ threadId, input: "must not start while stopping" })
        .pipe(Effect.exit);
      expect(sendExit._tag).toBe("Failure");

      yield* mock.emit({
        method: "turn/completed",
        params: {
          sessionId: "msp-race-session",
          turnId: String(turn.turnId),
          terminal: "cancelled",
        },
      });
      yield* Fiber.join(stopFiber);
      expect(yield* adapter.hasSession(threadId)).toBe(false);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );

  it.effect("stopAll force-closes the host and removes sessions after per-session stop failure", () =>
    Effect.gen(function* () {
      const mock = yield* makeRaceMockHost;
      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-race"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );
      const threadId = ThreadId.make("race-stop-all");

      yield* adapter.startSession({ threadId, cwd: "/test", runtimeMode: "approval-required" });
      yield* adapter.sendTurn({ threadId, input: "interrupt fails" });
      mock.calls.failInterrupt = true;

      // stopAll deliberately treats individual session stop as best-effort. Even
      // when turn/interrupt fails, closing the persistent host guarantees no Muse
      // execution remains alive; only then may residual T3 session state be cleared.
      yield* adapter.stopAll();

      expect(mock.calls.interruptTurn).toBe(1);
      expect(mock.calls.close).toBe(1);
      expect(yield* adapter.hasSession(threadId)).toBe(false);
      expect((yield* adapter.listSessions()).length).toBe(0);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );

  it.effect("startSession refuses to overwrite an already-owned thread", () =>
    Effect.gen(function* () {
      const mock = yield* makeRaceMockHost;
      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-race"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );
      const threadId = ThreadId.make("race-duplicate-start");

      yield* adapter.startSession({ threadId, cwd: "/test", runtimeMode: "approval-required" });
      const duplicate = yield* adapter
        .startSession({ threadId, cwd: "/test", runtimeMode: "approval-required" })
        .pipe(Effect.exit);
      expect(duplicate._tag).toBe("Failure");

      yield* adapter.stopSession(threadId);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );
});
