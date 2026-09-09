/**
 * MspEvents — translates Muse MSP view events into canonical ProviderRuntimeEvents.
 *
 * Preserves canonical T3 typing without leaking internal MSP structures.
 *
 * @module provider/msp/MspEvents
 */
import {
  type CanonicalRequestType,
  type EventId,
  type IsoDateTime,
  type ProviderApprovalDecision,
  type ProviderApprovalOption,
  ProviderDriverKind,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  RuntimeItemId,
  RuntimeRequestId,
  type RuntimeMode,
  type ThreadId,
  type ToolLifecycleItemType,
  TurnId,
  type UserInputQuestion,
} from "@t3tools/contracts";
import * as Option from "effect/Option";

import type {
  MspApprovalChoice,
  MspApprovalMode,
  MspApprovalRequest,
  MspApprovalSubject,
  MspUserInputQuestion,
  MspUserInputRequest,
} from "./MspTypes.ts";

export interface MspEventStamp {
  readonly eventId: EventId;
  readonly createdAt: IsoDateTime;
}

export function canonicalItemTypeFromMuseTool(
  toolName?: string,
  itemKind?: string,
): ToolLifecycleItemType {
  const name = toolName?.toLowerCase() ?? "";
  if (
    name.includes("shell") ||
    name.includes("bash") ||
    name === "exec" ||
    itemKind === "userShell"
  ) {
    return "command_execution";
  }
  if (
    name.includes("file") ||
    name.includes("write") ||
    name.includes("edit") ||
    name.includes("patch")
  ) {
    return "file_change";
  }
  if (name.includes("search") || name.includes("fetch") || name.includes("browse")) {
    return "web_search";
  }
  return "dynamic_tool_call";
}

export function canonicalRequestTypeFromMuseSubject(
  subject?: MspApprovalSubject,
): CanonicalRequestType {
  const kind = subject?.kind?.toLowerCase() ?? "";
  if (kind === "command" || subject?.command) {
    return "exec_command_approval";
  }
  if (kind === "file_read" || kind === "read") {
    return "file_read_approval";
  }
  if (kind === "file_change" || kind === "write" || kind === "edit" || subject?.path) {
    return "file_change_approval";
  }
  return "dynamic_tool_call";
}

/**
 * Maps T3 RuntimeMode to Muse MSP ApprovalMode.
 *
 * - "full-access" -> "allowAll" (bypasses permission checks)
 * - "approval-required" -> "onRequest" (interactive approval on every sensitive request)
 * - "auto-accept-edits" -> "promptUnmatched" (MSP lacks edit-only auto-approval; promptUnmatched is safest)
 * - "auto" -> "promptUnmatched" (default interactive mode for matching preconfigured policies)
 */
export function mapRuntimeModeToMspApprovalMode(runtimeMode: RuntimeMode): MspApprovalMode {
  switch (runtimeMode) {
    case "full-access":
      return "allowAll";
    case "approval-required":
      return "onRequest";
    case "auto-accept-edits":
    case "auto":
      return "promptUnmatched";
  }
}

export function parseMuseApprovalDecision(
  decision: string,
): Option.Option<ProviderApprovalDecision> {
  switch (decision) {
    case "approved":
      return Option.some("accept");
    case "approvedForSession":
      return Option.some("acceptForSession");
    case "approvedPolicyAmendment":
      return Option.some("acceptAlways");
    case "denied":
    case "deniedPolicyAmendment":
      return Option.some("decline");
    case "abort":
    case "timedOut":
      return Option.some("cancel");
    default:
      return Option.none();
  }
}

export function mapMuseApprovalDecision(decision: string): ProviderApprovalDecision {
  const parsed = parseMuseApprovalDecision(decision);
  if (Option.isNone(parsed)) {
    // Fail closed: an unknown/unrecognized decision must never become "accept"
    return "cancel";
  }
  return parsed.value;
}

export function mapMuseApprovalChoices(
  choices: ReadonlyArray<MspApprovalChoice>,
): ReadonlyArray<ProviderApprovalOption> {
  return choices.map((c) => ({
    decision: mapMuseApprovalDecision(c.decision),
    label: c.label || c.decision,
  }));
}

export function mapMuseUserInputQuestions(
  questions: ReadonlyArray<MspUserInputQuestion>,
): ReadonlyArray<UserInputQuestion> {
  return questions.map((q) => ({
    id: q.id,
    header: q.header || "Input requested",
    question: q.question,
    options: q.options.map((opt) => ({
      label: opt.label,
      description: opt.description ?? opt.label,
      value: opt.label,
    })),
    multiSelect: q.selection?.mode === "multiple",
    allowCustomAnswer: true,
  }));
}

export function makeMspTurnStartedEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
  readonly rawPayload: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "turn.started",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    payload: {},
    raw: {
      source: "msp.jsonrpc",
      method: "turn/started",
      payload: input.rawPayload,
    },
  };
}

export function makeMspTurnCompletedEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
  readonly state: "completed" | "failed" | "interrupted" | "cancelled";
  readonly errorMessage?: string | undefined;
  readonly rawPayload: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "turn.completed",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    payload: {
      state: input.state,
      ...(input.errorMessage ? { errorMessage: input.errorMessage } : {}),
    },
    raw: {
      source: "msp.jsonrpc",
      method: "turn/completed",
      payload: input.rawPayload,
    },
  };
}

export function makeMspAssistantItemStartedEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
  readonly itemId: string;
  readonly rawPayload: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "item.started",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    itemId: RuntimeItemId.make(input.itemId),
    payload: {
      itemType: "assistant_message",
      status: "inProgress",
    },
    raw: {
      source: "msp.jsonrpc",
      method: "item/started",
      payload: input.rawPayload,
    },
  };
}

export function makeMspAssistantItemCompletedEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
  readonly itemId: string;
  readonly rawPayload: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "item.completed",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    itemId: RuntimeItemId.make(input.itemId),
    payload: {
      itemType: "assistant_message",
      status: "completed",
    },
    raw: {
      source: "msp.jsonrpc",
      method: "item/completed",
      payload: input.rawPayload,
    },
  };
}

export function makeMspContentDeltaEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
  readonly itemId: string;
  readonly streamKind?: "assistant_text" | "reasoning_text" | "command_output" | undefined;
  readonly delta: string;
  readonly rawPayload: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "content.delta",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    itemId: RuntimeItemId.make(input.itemId),
    payload: {
      streamKind: input.streamKind ?? "assistant_text",
      delta: input.delta,
    },
    raw: {
      source: "msp.jsonrpc",
      method: "item/delta",
      payload: input.rawPayload,
    },
  };
}

export function makeMspToolCallItemEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId: TurnId;
  readonly itemId: string;
  readonly status: "inProgress" | "completed" | "failed";
  readonly toolName?: string | undefined;
  readonly visibleOutput?: string | undefined;
  readonly rawPayload: unknown;
}): ProviderRuntimeEvent {
  const isTerminal = input.status === "completed" || input.status === "failed";
  return {
    type: isTerminal ? "item.completed" : "item.started",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    itemId: RuntimeItemId.make(input.itemId),
    payload: {
      itemType: canonicalItemTypeFromMuseTool(input.toolName),
      status: input.status,
      title: input.toolName,
      ...(input.visibleOutput ? { detail: input.visibleOutput } : {}),
    },
    raw: {
      source: "msp.jsonrpc",
      method: isTerminal ? "item/completed" : "item/started",
      payload: input.rawPayload,
    },
  };
}

export function makeMspRequestOpenedEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly approvalRequest: MspApprovalRequest;
  readonly rawPayload: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "request.opened",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    requestId: RuntimeRequestId.make(input.approvalRequest.approvalId),
    payload: {
      requestType: canonicalRequestTypeFromMuseSubject(input.approvalRequest.subject),
      detail:
        input.approvalRequest.subject?.command ??
        input.approvalRequest.subject?.path ??
        input.approvalRequest.toolName,
      args: input.approvalRequest.rawArgs,
      options: mapMuseApprovalChoices(input.approvalRequest.availableChoices),
    },
    raw: {
      source: "msp.jsonrpc",
      method: "approval/requested",
      payload: input.rawPayload,
    },
  };
}

export function makeMspRequestResolvedEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly approvalId: string;
  readonly decision: ProviderApprovalDecision;
}): ProviderRuntimeEvent {
  return {
    type: "request.resolved",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    requestId: RuntimeRequestId.make(input.approvalId),
    payload: {
      requestType: "dynamic_tool_call",
      decision: input.decision,
    },
  };
}

export function makeMspUserInputRequestedEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly userInputRequest: MspUserInputRequest;
  readonly rawPayload: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "user-input.requested",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    requestId: RuntimeRequestId.make(input.userInputRequest.userInputId),
    payload: {
      questions: mapMuseUserInputQuestions(input.userInputRequest.questions),
    },
    raw: {
      source: "msp.jsonrpc",
      method: "userInput/requested",
      payload: input.rawPayload,
    },
  };
}

export function makeMspUserInputResolvedEvent(input: {
  readonly stamp: MspEventStamp;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId?: ProviderInstanceId | undefined;
  readonly threadId: ThreadId;
  readonly turnId?: TurnId | undefined;
  readonly userInputId: string;
  readonly answers: Record<string, unknown>;
}): ProviderRuntimeEvent {
  return {
    type: "user-input.resolved",
    eventId: input.stamp.eventId,
    createdAt: input.stamp.createdAt,
    provider: input.provider,
    ...(input.providerInstanceId ? { providerInstanceId: input.providerInstanceId } : {}),
    threadId: input.threadId,
    turnId: input.turnId,
    requestId: RuntimeRequestId.make(input.userInputId),
    payload: {
      answers: input.answers,
    },
  };
}
