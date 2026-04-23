import type { CallMetadata, CallState, CallStep, TelnyxWebhookEvent, TestOutcome } from "../types";
import { decodeClientState } from "./telnyx";

const callStates = new Map<string, CallState>();

function now(): string {
  return new Date().toISOString();
}

function createState(callControlId: string, metadata: CallMetadata, step: CallStep): CallState {
  return {
    callControlId,
    rowNumber: metadata.rowNumber,
    firstName: metadata.firstName,
    lastName: metadata.lastName,
    fullName: metadata.fullName,
    listingAddress: metadata.listingAddress,
    destinationPhone: metadata.dialedPhone,
    startedAt: now(),
    currentStep: step,
    hasProcessedFinalOutcome: false,
    hasEnded: false,
    lastEventAt: now(),
  };
}

export function saveInitialCallState(callControlId: string, metadata: CallMetadata): CallState {
  const state = createState(callControlId, metadata, "starting");
  callStates.set(callControlId, state);
  return state;
}

export function upsertCallStateFromWebhook(
  callControlId: string,
  event: TelnyxWebhookEvent,
  step: CallStep,
): CallState {
  const payload = event.data?.payload ?? {};
  const existing = callStates.get(callControlId);
  const metadata = decodeClientState(payload.client_state);

  const base =
    existing ??
    (metadata
      ? createState(callControlId, metadata, step)
      : {
          callControlId,
          rowNumber: 0,
          fullName: "Unknown",
          listingAddress: "Unknown",
          destinationPhone: typeof payload.to === "string" ? payload.to : "Unknown",
          startedAt: now(),
          currentStep: step,
          hasProcessedFinalOutcome: false,
          hasEnded: false,
          lastEventAt: now(),
        });
  const eventType = event.data?.event_type;

  const nextState: CallState = {
    ...base,
    callLegId: payload.call_leg_id ?? base.callLegId,
    callSessionId: payload.call_session_id ?? base.callSessionId,
    rowNumber: metadata?.rowNumber ?? base.rowNumber,
    firstName: metadata?.firstName ?? base.firstName,
    lastName: metadata?.lastName ?? base.lastName,
    fullName: metadata?.fullName ?? base.fullName,
    listingAddress: metadata?.listingAddress ?? base.listingAddress,
    destinationPhone: metadata?.dialedPhone ?? base.destinationPhone,
    answeredAt:
      step === "answered" || step === "initial_prompt" || step === "transfer_fallback_prompt"
        ? (base.answeredAt ?? now())
        : base.answeredAt,
    currentStep: step,
    hasProcessedFinalOutcome: base.hasProcessedFinalOutcome,
    hasEnded: base.hasEnded || eventType === "call.hangup",
    lastEventType: eventType ?? base.lastEventType,
    lastEventAt: event.data?.occurred_at ?? now(),
  };

  callStates.set(callControlId, nextState);
  return nextState;
}

export function updateCallState(
  callControlId: string,
  updates: Partial<
    Pick<
      CallState,
      | "currentStep"
      | "testOutcome"
      | "answeredAt"
      | "hasProcessedFinalOutcome"
      | "hasEnded"
      | "lastEventType"
      | "lastEventAt"
    >
  >,
): CallState | undefined {
  const existing = callStates.get(callControlId);

  if (!existing) {
    return undefined;
  }

  const nextState: CallState = {
    ...existing,
    ...updates,
    lastEventAt: updates.lastEventAt ?? now(),
  };

  callStates.set(callControlId, nextState);
  return nextState;
}

export function markTestOutcome(
  callControlId: string,
  testOutcome: TestOutcome,
  currentStep: CallStep,
): CallState | undefined {
  return updateCallState(callControlId, {
    currentStep,
    testOutcome,
  });
}

export function markFinalOutcome(
  callControlId: string,
  testOutcome: TestOutcome,
  currentStep: CallStep,
): CallState | undefined {
  return updateCallState(callControlId, {
    currentStep,
    testOutcome,
    hasProcessedFinalOutcome: true,
  });
}

export function getCallState(callControlId: string): CallState | undefined {
  return callStates.get(callControlId);
}
