// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { ServerConfig } from "../../config.ts";
import type { MspHost, MspNotification } from "../msp/MspClient.ts";
import type {
  MspApprovalRequest,
  MspModelInfo,
  MspSessionStartParams,
  MspSessionStartResult,
  MspTurnStartParams,
  MspTurnStartResult,
  MspUserInputRequest,
} from "../msp/MspTypes.ts";
import { makeMuseAdapter } from "./MuseAdapter.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-muse-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeMockHost = Effect.gen(function* () {
  const notifPubSub = yield* PubSub.unbounded<MspNotification>();
  const calls = {
    startSession: [] as MspSessionStartParams[],
    startTurn: [] as MspTurnStartParams[],
    interruptTurn: [] as Array<{
      commandId: string;
      sessionId: string;
      turnId?: string | undefined;
    }>,
    decideApproval: [] as Array<{
      commandId: string;
      sessionId: string;
      approvalId: string;
      choiceId: string;
    }>,
    answerUserInput: [] as Array<{ commandId: string; sessionId: string; userInputId: string }>,
    compact: [] as Array<{ commandId: string; sessionId: string }>,
  };

  const host: MspHost = {
    serverInfo: { name: "mock-muse", version: "1.0.3" },
    notifications: Stream.fromPubSub(notifPubSub),
    request: () => Effect.succeed({}),
    notify: () => Effect.void,
    startSession: (params) =>
      Effect.sync(() => {
        calls.startSession.push(params);
        return {
          session: {
            sessionId: "msp-sess-42",
            status: "ready",
            activeTurnId: null,
            workspaceRoot: params.workspaceRoot,
          },
          viewCursor: "cursor-1",
        } satisfies MspSessionStartResult;
      }),
    startTurn: (params) =>
      Effect.sync(() => {
        calls.startTurn.push(params);
        return {
          commandId: params.commandId,
          disposition: "started",
          startedNewTurn: true,
          status: "running",
          turnId: "msp-turn-101",
        } satisfies MspTurnStartResult;
      }),
    interruptTurn: (params) =>
      Effect.sync(() => {
        calls.interruptTurn.push(params);
        return {
          commandId: params.commandId,
          status: "interrupted",
          turnId: params.turnId ?? "msp-turn-101",
        };
      }),
    setModel: (params) => Effect.succeed({ commandId: params.commandId, status: "accepted" }),
    compact: (params) =>
      Effect.sync(() => {
        calls.compact.push(params);
        return { commandId: params.commandId, status: "accepted" };
      }),
    decideApproval: (params) =>
      Effect.sync(() => {
        calls.decideApproval.push({
          commandId: params.commandId,
          sessionId: params.sessionId,
          approvalId: params.approvalId,
          choiceId: params.choiceId,
        });
        return { commandId: params.commandId, status: "accepted", terminal: true };
      }),
    answerUserInput: (params) =>
      Effect.sync(() => {
        calls.answerUserInput.push({
          commandId: params.commandId,
          sessionId: params.sessionId,
          userInputId: params.userInputId,
        });
        return { commandId: params.commandId, status: "accepted" };
      }),
    cancelUserInput: (params) =>
      Effect.succeed({ commandId: params.commandId, status: "cancelled" }),
    listModels: () =>
      Effect.succeed([
        { modelId: "muse-spark-1.3", displayLabel: "Muse Spark 1.3", isDefault: true },
      ] satisfies ReadonlyArray<MspModelInfo>),
    unsubscribeView: () => Effect.void,
    close: Effect.void,
  };

  return {
    host,
    calls,
    emitNotification: (notif: MspNotification) => PubSub.publish(notifPubSub, notif),
  };
});

it.layer(testLayer)("MuseAdapter", (it) => {
  it.effect("manages session lifecycle and turns", () =>
    Effect.gen(function* () {
      const mock = yield* makeMockHost;
      const threadId = ThreadId.make("thread-1");

      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-primary"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );

      expect(adapter.provider).toBe(ProviderDriverKind.make("muse"));
      expect(adapter.capabilities.supportsConversationRollback).toBe(false);

      // Start session
      const session = yield* adapter.startSession({
        threadId,
        cwd: "/workspace/project",
        runtimeMode: "approval-required",
      });

      expect(session.threadId).toBe(threadId);
      expect(session.status).toBe("ready");
      expect(session.cwd).toBe("/workspace/project");
      expect(mock.calls.startSession.length).toBe(1);
      expect(mock.calls.startSession[0]?.workspaceRoot).toBe("/workspace/project");

      // Verify session queries
      expect(yield* adapter.hasSession(threadId)).toBe(true);
      const sessions = yield* adapter.listSessions();
      expect(sessions.length).toBe(1);
      expect(sessions[0]?.threadId).toBe(threadId);

      // Send turn
      const turnResult = yield* adapter.sendTurn({
        threadId,
        input: "Write a hello world program",
      });

      expect(turnResult.threadId).toBe(threadId);
      expect(turnResult.turnId).toBe(TurnId.make("msp-turn-101"));
      expect(mock.calls.startTurn.length).toBe(1);
      expect(mock.calls.startTurn[0]?.sessionId).toBe("msp-sess-42");
      expect(mock.calls.startTurn[0]?.input[0]?.text).toBe("Write a hello world program");

      // Interrupt turn
      yield* adapter.interruptTurn(threadId, TurnId.make("msp-turn-101"));
      expect(mock.calls.interruptTurn.length).toBe(1);

      // Native compaction
      if (adapter.compaction?.type === "native") {
        yield* adapter.compaction.start(threadId);
        expect(mock.calls.compact.length).toBe(1);
      }

      // Rollback should fail
      const rollbackResult = yield* adapter.rollbackThread(threadId, 1).pipe(Effect.exit);
      expect(rollbackResult._tag).toBe("Failure");

      // Stop session
      yield* adapter.stopSession(threadId);
      expect(yield* adapter.hasSession(threadId)).toBe(false);
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );

  it.effect("streams mapped canonical runtime events from MSP notifications", () =>
    Effect.gen(function* () {
      const mock = yield* makeMockHost;
      const threadId = ThreadId.make("thread-events");

      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-primary"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );

      const eventsQueue = yield* Queue.unbounded<any>();
      yield* adapter.streamEvents.pipe(
        Stream.runForEach((event) => Queue.offer(eventsQueue, event)),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;

      // Start session
      yield* adapter.startSession({
        threadId,
        cwd: "/test",
        runtimeMode: "approval-required",
      });
      yield* Effect.yieldNow;

      // Emit turn/started
      yield* mock.emitNotification({
        method: "turn/started",
        params: {
          sessionId: "msp-sess-42",
          turnId: "msp-turn-200",
        },
      });

      const event1 = yield* Queue.take(eventsQueue);
      expect(event1.type).toBe("turn.started");
      expect(event1.turnId).toBe(TurnId.make("msp-turn-200"));

      // Emit item/started
      yield* mock.emitNotification({
        method: "item/started",
        params: {
          sessionId: "msp-sess-42",
          item: {
            itemId: "item-1",
            kind: "agentMessage",
            status: "inProgress",
            turnId: "msp-turn-200",
          },
        },
      });

      const event2 = yield* Queue.take(eventsQueue);
      expect(event2.type).toBe("item.started");
      expect(event2.payload.itemType).toBe("assistant_message");

      // Emit item/delta
      yield* mock.emitNotification({
        method: "item/delta",
        params: {
          sessionId: "msp-sess-42",
          itemId: "item-1",
          turnId: "msp-turn-200",
          delta: "Hello there!",
        },
      });

      const event3 = yield* Queue.take(eventsQueue);
      expect(event3.type).toBe("content.delta");
      expect(event3.payload.delta).toBe("Hello there!");

      // Emit item/completed
      yield* mock.emitNotification({
        method: "item/completed",
        params: {
          sessionId: "msp-sess-42",
          item: {
            itemId: "item-1",
            kind: "agentMessage",
            status: "completed",
            turnId: "msp-turn-200",
          },
        },
      });

      const event4 = yield* Queue.take(eventsQueue);
      expect(event4.type).toBe("item.completed");
      expect(event4.payload.status).toBe("completed");

      // Emit turn/completed
      yield* mock.emitNotification({
        method: "turn/completed",
        params: {
          sessionId: "msp-sess-42",
          turnId: "msp-turn-200",
          terminal: "completed",
        },
      });

      const event5 = yield* Queue.take(eventsQueue);
      expect(event5.type).toBe("turn.completed");
      expect(event5.payload.state).toBe("completed");
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );

  it.effect("handles approval requests and user inputs", () =>
    Effect.gen(function* () {
      const mock = yield* makeMockHost;
      const threadId = ThreadId.make("thread-approvals");

      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-primary"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );

      const eventsQueue = yield* Queue.unbounded<any>();
      yield* adapter.streamEvents.pipe(
        Stream.runForEach((event) => Queue.offer(eventsQueue, event)),
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;

      yield* adapter.startSession({
        threadId,
        cwd: "/test",
        runtimeMode: "approval-required",
      });
      yield* Effect.yieldNow;

      // Emit approval/requested
      const approvalReq: MspApprovalRequest = {
        approvalId: "app-999",
        sessionId: "msp-sess-42",
        turnId: "turn-1",
        itemId: "item-1",
        toolCallId: "tc-1",
        toolName: "Bash",
        currentRequirementId: { approvalId: "app-999", sourceIndex: 0 },
        availableChoices: [
          { choiceId: "c-yes", decision: "approved", label: "Allow", scope: "turn" },
          { choiceId: "c-no", decision: "denied", label: "Deny", scope: "turn" },
        ],
        subject: { kind: "command", command: "npm test" },
        rawArgs: '{"command":"npm test"}',
        viewCursor: "cur-1",
      };

      yield* mock.emitNotification({
        method: "approval/requested",
        params: approvalReq as unknown as Record<string, unknown>,
      });

      const appEvent = yield* Queue.take(eventsQueue);
      expect(appEvent.type).toBe("request.opened");

      // Respond to approval
      yield* adapter.respondToRequest(
        threadId,
        ApprovalRequestId.make("app-999"),
        "accept" as ProviderApprovalDecision,
      );

      expect(mock.calls.decideApproval.length).toBe(1);
      expect(mock.calls.decideApproval[0]?.approvalId).toBe("app-999");
      expect(mock.calls.decideApproval[0]?.choiceId).toBe("c-yes");

      // Emit userInput/requested
      const userInputReq: MspUserInputRequest = {
        userInputId: "ui-888",
        sessionId: "msp-sess-42",
        turnId: "turn-1",
        itemId: "item-2",
        toolCallId: "tc-2",
        toolName: "ask_question",
        questions: [
          {
            id: "q-1",
            header: "Target framework",
            question: "Which framework?",
            options: [{ label: "React" }, { label: "Vue" }],
          },
        ],
        viewCursor: "cur-2",
      };

      yield* mock.emitNotification({
        method: "userInput/requested",
        params: userInputReq as unknown as Record<string, unknown>,
      });

      const uiEvent = yield* Queue.take(eventsQueue);
      expect(uiEvent.type).toBe("user-input.requested");

      // Respond to user input
      yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make("ui-888"), {
        "q-1": "React",
      });

      expect(mock.calls.answerUserInput.length).toBe(1);
      expect(mock.calls.answerUserInput[0]?.userInputId).toBe("ui-888");
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );
});
