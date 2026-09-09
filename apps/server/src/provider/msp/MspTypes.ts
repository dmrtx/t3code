/**
 * Shared types for the Muse MSP integration.
 *
 * @module provider/msp/MspTypes
 */

export interface MspTurnInputPart {
  readonly type: "text" | "image";
  readonly text?: string | undefined;
  readonly base64Data?: string | undefined;
  readonly mediaType?: string | undefined;
  readonly width?: number | undefined;
  readonly height?: number | undefined;
}

export interface MspSessionStartParams {
  readonly commandId: string;
  readonly workspaceRoot?: string | undefined;
  readonly modelId?: string | undefined;
  readonly providerId?: string | null | undefined;
  readonly approvalMode?: string | null | undefined;
}

export interface MspSessionSummary {
  readonly sessionId: string;
  readonly status: string;
  readonly activeTurnId: string | null;
  readonly modelId?: string | undefined;
  readonly workspaceRoot?: string | undefined;
  readonly providerId?: string | undefined;
}

export interface MspSessionStartResult {
  readonly session: MspSessionSummary;
  readonly viewCursor: string;
}

export interface MspTurnStartParams {
  readonly commandId: string;
  readonly sessionId: string;
  readonly input: ReadonlyArray<MspTurnInputPart>;
  readonly displayText?: string | undefined;
  readonly ifBusy?: "queue" | "steer" | "replace" | undefined;
  readonly reasoningEffort?: string | undefined;
}

export interface MspTurnStartResult {
  readonly commandId: string;
  readonly disposition: string;
  readonly startedNewTurn: boolean;
  readonly status: string;
  readonly turnId: string;
}

export interface MspApprovalChoice {
  readonly choiceId: string;
  readonly decision: string;
  readonly label: string;
  readonly acceptsFeedback?: boolean | undefined;
  readonly scope: string;
}

export interface MspApprovalSubject {
  readonly kind: string;
  readonly command?: string | undefined;
  readonly path?: string | undefined;
  readonly toolName?: string | undefined;
  readonly target?: string | undefined;
}

export interface MspApprovalRequirementRef {
  readonly approvalId: string;
  readonly sourceIndex: number;
}

export interface MspApprovalRequest {
  readonly approvalId: string;
  readonly sessionId: string;
  readonly turnId: string;
  readonly itemId: string;
  readonly toolCallId: string;
  readonly toolName: string;
  readonly currentRequirementId: MspApprovalRequirementRef;
  readonly availableChoices: ReadonlyArray<MspApprovalChoice>;
  readonly subject: MspApprovalSubject;
  readonly rawArgs: string;
  readonly viewCursor: string;
}

export interface MspUserInputQuestionOption {
  readonly label: string;
  readonly description?: string | undefined;
}

export interface MspUserInputQuestion {
  readonly id: string;
  readonly header: string;
  readonly question: string;
  readonly selection?:
    | {
        readonly mode: "single" | "multiple";
        readonly minSelections?: number | undefined;
        readonly maxSelections?: number | undefined;
      }
    | undefined;
  readonly options: ReadonlyArray<MspUserInputQuestionOption>;
}

export interface MspUserInputRequest {
  readonly userInputId: string;
  readonly sessionId: string;
  readonly turnId: string;
  readonly itemId: string;
  readonly toolCallId: string;
  readonly toolName: string;
  readonly questions: ReadonlyArray<MspUserInputQuestion>;
  readonly viewCursor: string;
}

export interface MspUserInputAnswer {
  readonly questionId: string;
  readonly selectedLabel?: string | undefined;
  readonly selectedLabels?: ReadonlyArray<string> | undefined;
  readonly freeText?: string | undefined;
  readonly note?: string | undefined;
}

export interface MspNotification {
  readonly method: string;
  readonly params: Record<string, unknown>;
}

export interface MspModelInfo {
  readonly modelId: string;
  readonly displayLabel: string;
  readonly providerId?: string | undefined;
  readonly profileId?: string | undefined;
  readonly contextLimit?: number | undefined;
  readonly outputLimit?: number | undefined;
  readonly isDefault?: boolean | undefined;
}

export interface MspItem {
  readonly itemId: string;
  readonly kind: string;
  readonly revision: number;
  readonly status: string;
  readonly turnId?: string | undefined;
  readonly text?: string | undefined;
  readonly tool?: string | undefined;
  readonly visibleOutput?: string | undefined;
  readonly rawArgs?: string | undefined;
  readonly summary?: ReadonlyArray<string> | undefined;
}
