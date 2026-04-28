export interface StartCallRequest {
  rowNumber: number;
  firstName?: string;
  lastName?: string;
  fullName?: string;
  callAttemptNumber?: number;
  phone: string;
  email?: string;
  listingAddress: string;
  createdAt?: string;
  scheduledForEt?: string;
  responseStatus?: string;
  notes?: string;
  sheetName?: string;
}

export interface CallMetadata {
  rowNumber: number;
  firstName?: string;
  lastName?: string;
  fullName: string;
  email?: string;
  callAttemptNumber: number;
  listingAddress: string;
  sheetName?: string;
  requestedPhone: string;
  dialedPhone: string;
  testMode: boolean;
}

export interface LiveTransferCallMetadata {
  kind: "live_transfer_yoni";
  originalCallControlId: string;
  rowNumber: number;
  agentName: string;
  listingAddress: string;
  whisperMessage: string;
  yoniPrompt: string;
}

export interface CallState {
  callControlId: string;
  callLegId?: string;
  callSessionId?: string;
  rowNumber: number;
  firstName?: string;
  lastName?: string;
  fullName: string;
  email?: string;
  listingAddress: string;
  destinationPhone: string;
  startedAt: string;
  answeredAt?: string;
  currentStep: CallStep;
  testOutcome?: TestOutcome;
  hasProcessedFinalOutcome: boolean;
  hasEnded: boolean;
  lastEventType?: string;
  lastEventAt: string;
}

export type CallStep =
  | "starting"
  | "initiated"
  | "answered"
  | "initial_prompt"
  | "callback_time_prompt"
  | "live_transfer_wait"
  | "pre_connection_message"
  | "transfer_wait"
  | "transfer_fallback_prompt"
  | "closing_message"
  | "completed"
  | "hangup"
  | "ignored"
  | "error";

export type TestOutcome =
  | "live_transfer_requested"
  | "not_interested"
  | "callback_requested"
  | "callback_unknown"
  | "no_response_to_prompt";

export interface TelnyxWebhookEvent {
  data?: {
    id?: string;
    event_type?: string;
    occurred_at?: string;
    payload?: TelnyxWebhookPayload;
  };
}

export interface TelnyxWebhookPayload {
  call_control_id?: string;
  call_leg_id?: string;
  call_session_id?: string;
  client_state?: string | null;
  connection_id?: string;
  direction?: string;
  from?: string;
  to?: string;
  state?: string;
  hangup_cause?: string;
  digit?: string;
  digits?: string;
  status?: string;
  result?: Record<string, unknown>;
  message_history?: Array<{ role?: string; content?: string }>;
  [key: string]: unknown;
}

export interface TelnyxDialResponse {
  data?: {
    call_control_id?: string;
    call_leg_id?: string;
    call_session_id?: string;
    client_state?: string;
    record_type?: string;
    [key: string]: unknown;
  };
}

export interface ElevenLabsOutboundCallResponse {
  success?: boolean;
  message?: string;
  conversation_id?: string | null;
  sip_call_id?: string | null;
  [key: string]: unknown;
}

export interface SheetUpdateRequest {
  rowNumber?: number;
  sheetName?: string;
  status?: string;
  notes?: string;
  callResult?: string;
  responseStatus?: string;
  leadStatusCode?: string;
  callAttemptNumber?: number;
  callScheduledFor?: string;
  liveTransferRequested?: string;
  liveTransferCompleted?: string;
  vmLeft?: string;
  callbackRequested?: string;
  callbackTime?: string;
  voiceNotes?: string;
  [key: string]: unknown;
}
