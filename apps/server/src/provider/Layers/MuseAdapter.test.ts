// @effect-diagnostics nodeBuiltinImport:off
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  ProviderApprovalDecision,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

import { ServerConfig } from "../../config.ts";
import { type MspHost, type MspNotification, MspTransportError } from "../msp/MspClient.ts";
import type {
  MspApprovalRequest,
  MspModelInfo,
  MspSessionStartParams,
  MspSessionStartResult,
  MspTurnStartParams,
  MspTurnStartResult,
  MspUserInputRequest,
} from "../msp/MspTypes.ts";
import { makeMuseAdapter, selectMuseApprovalChoiceId } from "./MuseAdapter.ts";
import {
  mapMuseApprovalDecision,
  mapMuseTurnTerminalToState,
  mapRuntimeModeToMspApprovalMode,
} from "../msp/MspEvents.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-muse-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

const makeMockHost = Effect.gen(function* () {
  const notifPubSub = yield* PubSub.unbounded<MspNotification>();
  const calls = {
    shouldFailInterrupt: false,
    startSession: [] as MspSessionStartParams[],
    startTurn: [] as MspTurnStartParams[],
    interruptTurn: [] as Array<{
      commandId: string;
      sessionId: string;
      turnId?: string | undefined;
    }>,
    setModel: [] as Array<{
      commandId: string;
      sessionId: string;
      modelId: string;
    }>,
    decideApproval: [] as Array<{
      commandId: string;
      sessionId: string;
      approvalId: string;
      choiceId: string;
    }>,
    answerUserInput: [] as Array<{ commandId: string; sessionId: string; userInputId: string }>,
    compact: [] as Array<{ commandId: string; sessionId: string }>,
    unsubscribeView: [] as Array<{ sessionId: string }>,
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
      Effect.gen(function* () {
        calls.interruptTurn.push(params);
        if (calls.shouldFailInterrupt) {
          return yield* Effect.fail(
            new MspTransportError({
              operation: "turn/interrupt",
              detail: "Simulated interrupt failure",
            }),
          );
        }
        return {
          commandId: params.commandId,
          status: "interrupted",
          turnId: params.turnId ?? "msp-turn-101",
        };
      }),
    setModel: (params) =>
      Effect.sync(() => {
        calls.setModel.push(params);
        return { commandId: params.commandId, status: "accepted" };
      }),
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
    unsubscribeView: (params) =>
      Effect.sync(() => {
        calls.unsubscribeView.push(params);
      }),
    resumeSession: (params) =>
      Effect.succeed({
        session: {
          sessionId: params.sessionId,
          status: "ready",
          activeTurnId: null,
          workspaceRoot: "/workspace/project",
        },
        viewCursor: "cursor-1",
      }),
    cancelTurn: (params) =>
      Effect.succeed({
        commandId: params.commandId,
        status: "cancelled",
        turnId: params.turnId ?? "msp-turn-101",
      }),
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

  it.effect(
    "stopSession interrupts active turn and unsubscribes view before removing local state",
    () =>
      Effect.gen(function* () {
        const mock = yield* makeMockHost;
        const threadId = ThreadId.make("thread-stop-active");

        const adapter = yield* makeMuseAdapter(
          { enabled: true, binaryPath: "", customModels: [] },
          {
            instanceId: ProviderInstanceId.make("muse-primary"),
            makeHost: () => Effect.succeed(mock.host),
          },
        );

        yield* adapter.startSession({
          threadId,
          cwd: "/test",
          runtimeMode: "approval-required",
        });

        // Start a turn so that activeTurnId is set and status is 'running'
        yield* adapter.sendTurn({
          threadId,
          input: "Run a command",
        });

        expect(mock.calls.interruptTurn.length).toBe(0);
        expect(mock.calls.unsubscribeView.length).toBe(0);

        // Stop session while turn is active
        yield* adapter.stopSession(threadId);

        // Verify that turn was interrupted
        expect(mock.calls.interruptTurn.length).toBe(1);
        expect(mock.calls.interruptTurn[0]?.sessionId).toBe("msp-sess-42");
        expect(mock.calls.interruptTurn[0]?.turnId).toBe("msp-turn-101");

        // Verify view was unsubscribed
        expect(mock.calls.unsubscribeView.length).toBe(1);
        expect(mock.calls.unsubscribeView[0]?.sessionId).toBe("msp-sess-42");

        // Verify local state was cleaned up
        expect(yield* adapter.hasSession(threadId)).toBe(false);
        const list = yield* adapter.listSessions();
        expect(list.find((s) => s.threadId === threadId)).toBeUndefined();
      }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
        ),
        Effect.scoped,
      ),
  );

  it.effect(
    "stopSession: active turn + interrupt failure -> stopSession fails and session remains owned by T3",
    () =>
      Effect.gen(function* () {
        const mock = yield* makeMockHost;
        const threadId = ThreadId.make("thread-stop-failure");

        const adapter = yield* makeMuseAdapter(
          { enabled: true, binaryPath: "", customModels: [] },
          {
            instanceId: ProviderInstanceId.make("muse-primary"),
            makeHost: () => Effect.succeed(mock.host),
          },
        );

        yield* adapter.startSession({
          threadId,
          cwd: "/test",
          runtimeMode: "approval-required",
        });

        // Start a turn so that activeTurnId is set and status is 'running'
        yield* adapter.sendTurn({
          threadId,
          input: "Run long running command",
        });

        // Simulate interrupt failure
        mock.calls.shouldFailInterrupt = true;

        // stopSession must fail
        const stopExit = yield* adapter.stopSession(threadId).pipe(Effect.exit);
        expect(stopExit._tag).toBe("Failure");

        // Interrupt was attempted
        expect(mock.calls.interruptTurn.length).toBe(1);

        // View was NOT unsubscribed because turn was not stopped
        expect(mock.calls.unsubscribeView.length).toBe(0);

        // CRITICAL INVARIANT: Session MUST remain owned by T3 (not deleted)
        expect(yield* adapter.hasSession(threadId)).toBe(true);
        const sessions = yield* adapter.listSessions();
        const active = sessions.find((s) => s.threadId === threadId);
        expect(active).toBeDefined();
        expect(active?.status).toBe("running");

        // Retry stopSession when interrupt succeeds
        mock.calls.shouldFailInterrupt = false;
        yield* adapter.stopSession(threadId);

        expect(mock.calls.interruptTurn.length).toBe(2);
        expect(mock.calls.unsubscribeView.length).toBe(1);
        expect(yield* adapter.hasSession(threadId)).toBe(false);
      }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
        ),
        Effect.scoped,
      ),
  );

  it.effect("maps turn/completed terminal cancellation to canonical cancelled state", () =>
    Effect.gen(function* () {
      // 1. Direct unit test of mapMuseTurnTerminalToState
      expect(mapMuseTurnTerminalToState("completed")).toBe("completed");
      expect(mapMuseTurnTerminalToState("failed")).toBe("failed");
      expect(mapMuseTurnTerminalToState("interrupted")).toBe("interrupted");
      expect(mapMuseTurnTerminalToState("cancelled")).toBe("cancelled");
      expect(mapMuseTurnTerminalToState("canceled")).toBe("cancelled");
      expect(mapMuseTurnTerminalToState(undefined)).toBe("completed");

      // 2. Integration with event stream: emit turn/completed with terminal: "cancelled"
      const mock = yield* makeMockHost;
      const threadId = ThreadId.make("thread-turn-cancelled");

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

      yield* mock.emitNotification({
        method: "turn/completed",
        params: {
          sessionId: "msp-sess-42",
          turnId: "msp-turn-cancelled",
          terminal: "cancelled",
        },
      });

      const event1 = yield* Queue.take(eventsQueue);
      expect(event1.type).toBe("turn.completed");
      expect(event1.payload.state).toBe("cancelled");

      // Also verify American spelling "canceled"
      yield* mock.emitNotification({
        method: "turn/completed",
        params: {
          sessionId: "msp-sess-42",
          turnId: "msp-turn-canceled-2",
          terminal: "canceled",
        },
      });

      const event2 = yield* Queue.take(eventsQueue);
      expect(event2.type).toBe("turn.completed");
      expect(event2.payload.state).toBe("cancelled");
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );

  it.effect(
    "sessionModelSwitch calls session/setModel when model changes and updates session",
    () =>
      Effect.gen(function* () {
        const mock = yield* makeMockHost;
        const threadId = ThreadId.make("thread-model-switch");

        const adapter = yield* makeMuseAdapter(
          { enabled: true, binaryPath: "", customModels: [] },
          {
            instanceId: ProviderInstanceId.make("muse-primary"),
            makeHost: () => Effect.succeed(mock.host),
          },
        );

        yield* adapter.startSession({
          threadId,
          cwd: "/test",
          runtimeMode: "approval-required",
        });

        // Send turn with a model selection
        yield* adapter.sendTurn({
          threadId,
          input: "Hello model switch",
          modelSelection: {
            instanceId: ProviderInstanceId.make("muse-primary"),
            model: "muse-spark-1.3",
          },
        });

        expect(mock.calls.setModel.length).toBe(1);
        expect(mock.calls.setModel[0]?.sessionId).toBe("msp-sess-42");
        expect(mock.calls.setModel[0]?.modelId).toBe("muse-spark-1.3");

        const sessions1 = yield* adapter.listSessions();
        expect(sessions1.find((s) => s.threadId === threadId)?.model).toBe("muse-spark-1.3");

        // Send another turn with the same model: setModel should NOT be called again
        yield* adapter.sendTurn({
          threadId,
          input: "Second turn same model",
          modelSelection: {
            instanceId: ProviderInstanceId.make("muse-primary"),
            model: "muse-spark-1.3",
          },
        });

        expect(mock.calls.setModel.length).toBe(1);

        // Send another turn with a DIFFERENT model: setModel SHOULD be called
        yield* adapter.sendTurn({
          threadId,
          input: "Third turn different model",
          modelSelection: {
            instanceId: ProviderInstanceId.make("muse-primary"),
            model: "muse-spark-pro",
          },
        });

        expect(mock.calls.setModel.length).toBe(2);
        expect(mock.calls.setModel[1]?.modelId).toBe("muse-spark-pro");

        const sessions2 = yield* adapter.listSessions();
        expect(sessions2.find((s) => s.threadId === threadId)?.model).toBe("muse-spark-pro");
      }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
        ),
        Effect.scoped,
      ),
  );

  it.effect(
    "fails closed on unknown approval decisions and never selects approved for decline/cancel",
    () =>
      Effect.gen(function* () {
        // 1. Test mapMuseApprovalDecision directly
        expect(mapMuseApprovalDecision("approved")).toBe("accept");
        expect(mapMuseApprovalDecision("approvedForSession")).toBe("acceptForSession");
        expect(mapMuseApprovalDecision("approvedPolicyAmendment")).toBe("acceptAlways");
        expect(mapMuseApprovalDecision("denied")).toBe("decline");
        expect(mapMuseApprovalDecision("abort")).toBe("cancel");
        expect(mapMuseApprovalDecision("timedOut")).toBe("cancel");
        // Fail closed for unknown decisions
        expect(mapMuseApprovalDecision("unknown")).toBe("cancel");
        expect(mapMuseApprovalDecision("something_else")).toBe("cancel");
        expect(mapMuseApprovalDecision("")).toBe("cancel");

        // 2. Test selectMuseApprovalChoiceId
        const allowChoice = {
          choiceId: "c-allow",
          decision: "approved",
          label: "Allow",
          scope: "turn" as const,
        };
        const denyChoice = {
          choiceId: "c-deny",
          decision: "denied",
          label: "Deny",
          scope: "turn" as const,
        };
        const abortChoice = {
          choiceId: "c-abort",
          decision: "abort",
          label: "Abort",
          scope: "turn" as const,
        };

        const baseReq: MspApprovalRequest = {
          approvalId: "app-1",
          sessionId: "s-1",
          turnId: "turn-1",
          itemId: "item-1",
          toolCallId: "tc-1",
          toolName: "Bash",
          subject: { kind: "command", command: "test" },
          rawArgs: "{}",
          currentRequirementId: { approvalId: "app-1", sourceIndex: 0 },
          availableChoices: [allowChoice, denyChoice],
          viewCursor: "c-1",
        };

        // Both choices available:
        expect(selectMuseApprovalChoiceId(baseReq, "accept")).toEqual(Option.some("c-allow"));
        expect(selectMuseApprovalChoiceId(baseReq, "decline")).toEqual(Option.some("c-deny"));
        expect(selectMuseApprovalChoiceId(baseReq, "cancel")).toEqual(Option.some("c-deny"));

        // Only allow choice available: decline and cancel MUST fail closed (Option.none()), NEVER pick approved
        const allowOnlyReq: MspApprovalRequest = {
          ...baseReq,
          availableChoices: [allowChoice],
        };
        expect(selectMuseApprovalChoiceId(allowOnlyReq, "accept")).toEqual(Option.some("c-allow"));
        expect(selectMuseApprovalChoiceId(allowOnlyReq, "decline")).toEqual(Option.none());
        expect(selectMuseApprovalChoiceId(allowOnlyReq, "cancel")).toEqual(Option.none());

        // Only deny choice available: accept MUST fail closed (Option.none()), NEVER pick denied
        const denyOnlyReq: MspApprovalRequest = {
          ...baseReq,
          availableChoices: [denyChoice],
        };
        expect(selectMuseApprovalChoiceId(denyOnlyReq, "accept")).toEqual(Option.none());
        expect(selectMuseApprovalChoiceId(denyOnlyReq, "decline")).toEqual(Option.some("c-deny"));
        expect(selectMuseApprovalChoiceId(denyOnlyReq, "cancel")).toEqual(Option.some("c-deny"));

        // With abort choice: cancel prefers abort
        const abortReq: MspApprovalRequest = {
          ...baseReq,
          availableChoices: [allowChoice, denyChoice, abortChoice],
        };
        expect(selectMuseApprovalChoiceId(abortReq, "cancel")).toEqual(Option.some("c-abort"));

        // 3. Test respondToRequest fails with ProviderAdapterRequestError when Option.none() is returned
        const mock = yield* makeMockHost;
        const threadId = ThreadId.make("thread-fail-closed");

        const adapter = yield* makeMuseAdapter(
          { enabled: true, binaryPath: "", customModels: [] },
          {
            instanceId: ProviderInstanceId.make("muse-primary"),
            makeHost: () => Effect.succeed(mock.host),
          },
        );

        yield* adapter.startSession({
          threadId,
          cwd: "/test",
          runtimeMode: "approval-required",
        });

        // Emit approval with ONLY approved choice
        yield* mock.emitNotification({
          method: "approval/requested",
          params: allowOnlyReq as unknown as Record<string, unknown>,
        });

        // User tries to decline, but only allow is available -> should fail with typed error
        const declineResult = yield* adapter
          .respondToRequest(threadId, ApprovalRequestId.make("app-1"), "decline")
          .pipe(Effect.exit);

        expect(declineResult._tag).toBe("Failure");
      }).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
        ),
        Effect.scoped,
      ),
  );

  it.effect("requireSession fails via typed ProviderAdapterSessionNotFoundError channel", () =>
    Effect.gen(function* () {
      const mock = yield* makeMockHost;
      const threadId = ThreadId.make("thread-nonexistent");

      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-primary"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );

      // sendTurn on nonexistent session
      const sendTurnError = yield* adapter.sendTurn({ threadId, input: "hello" }).pipe(Effect.flip);
      expect(sendTurnError._tag).toBe("ProviderAdapterSessionNotFoundError");
      expect(sendTurnError.provider).toBe("muse");

      // readThread on nonexistent session
      const readThreadError = yield* adapter.readThread(threadId).pipe(Effect.flip);
      expect(readThreadError._tag).toBe("ProviderAdapterSessionNotFoundError");

      // interruptTurn on nonexistent session
      const interruptError = yield* adapter.interruptTurn(threadId).pipe(Effect.flip);
      expect(interruptError._tag).toBe("ProviderAdapterSessionNotFoundError");

      // respondToRequest on nonexistent session
      const respondError = yield* adapter
        .respondToRequest(threadId, ApprovalRequestId.make("req-1"), "accept")
        .pipe(Effect.flip);
      expect(respondError._tag).toBe("ProviderAdapterSessionNotFoundError");
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );

  it.effect("startSession maps runtimeMode to MSP approvalMode", () =>
    Effect.gen(function* () {
      const mock = yield* makeMockHost;

      const adapter = yield* makeMuseAdapter(
        { enabled: true, binaryPath: "", customModels: [] },
        {
          instanceId: ProviderInstanceId.make("muse-primary"),
          makeHost: () => Effect.succeed(mock.host),
        },
      );

      expect(mapRuntimeModeToMspApprovalMode("full-access")).toBe("allowAll");
      expect(mapRuntimeModeToMspApprovalMode("approval-required")).toBe("onRequest");
      expect(mapRuntimeModeToMspApprovalMode("auto")).toBe("promptUnmatched");
      expect(mapRuntimeModeToMspApprovalMode("auto-accept-edits")).toBe("promptUnmatched");

      // full-access -> allowAll
      yield* adapter.startSession({
        threadId: ThreadId.make("thread-fa"),
        cwd: "/test",
        runtimeMode: "full-access",
      });
      expect(mock.calls.startSession[0]?.approvalMode).toBe("allowAll");

      // approval-required -> onRequest
      yield* adapter.startSession({
        threadId: ThreadId.make("thread-ar"),
        cwd: "/test",
        runtimeMode: "approval-required",
      });
      expect(mock.calls.startSession[1]?.approvalMode).toBe("onRequest");

      // auto -> promptUnmatched
      yield* adapter.startSession({
        threadId: ThreadId.make("thread-auto"),
        cwd: "/test",
        runtimeMode: "auto",
      });
      expect(mock.calls.startSession[2]?.approvalMode).toBe("promptUnmatched");

      // auto-accept-edits -> promptUnmatched
      yield* adapter.startSession({
        threadId: ThreadId.make("thread-aae"),
        cwd: "/test",
        runtimeMode: "auto-accept-edits",
      });
      expect(mock.calls.startSession[3]?.approvalMode).toBe("promptUnmatched");
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Mock host does not spawn")),
      ),
      Effect.scoped,
    ),
  );
});
