import type { VoiceContactResult } from "./elevenLabsPostCall";

const requestContinuation =
  "If the latest caller turn contains an unanswered question or correction, address it first instead of giving a closing acknowledgment. " +
  "After a brief answer or acknowledgment, pause for a NEW caller turn. Do not ask an anything-else question or restart qualification. " +
  "Questions and corrections are not farewells. Silence, thanks, okay, or an earlier caller turn is not by itself permission to end. " +
  "Honor new corrections or cancellations; they supersede the earlier request. " +
  "A new explicit stop or cancellation received while this tool was pending also overrides acknowledgment and pause instructions. " +
  "Only the guarded ending workflow may end the call, based on a current explicit goodbye or request to stop or end this call. " +
  "Do not make the caller repeat a still-current explicit stop received while this tool was pending. Cancelling a callback is not by itself a request to end the call. " +
  "Do not call end_call or manufacture a goodbye because this tool returned. This receipt is not new consent or proof of durable persistence, booking, sending, or delivery.";

type LiveTransferStatus = "pending" | "accepted" | "declined" | "timeout" | "call_failed";
const currentCallerPriority =
  "Read the latest caller turn before acting on this result. An unanswered question, correction, cancellation, or changed contact preference takes priority over the earlier request. " +
  "Answer a current question first. An earlier yes does not override a later callback-only or email-only preference, refusal, or request to wait. ";

export function buildElevenLabsLiveTransferResponse(status: LiveTransferStatus) {
  const nextAction = status === "pending"
    ? "The original availability check is still in progress. Do not start another availability attempt or automatically create a callback as fallback. " +
      currentCallerPriority +
      "If the caller now explicitly chooses a callback or email instead, honor that new request using the corresponding request tool without waiting for the old result. " +
      "If the caller asks for a status, say briefly: I'm still checking. Otherwise, while the caller still wants the live transfer, wait quietly for the original result while listening. " +
      "Do not claim another place was tried, that the underlying attempt was cancelled, or that a transfer completed."
    : status === "accepted"
      ? "Yoni accepted the availability request. This is not proof of caller consent or a completed connection. " +
        currentCallerPriority +
        "Continue the configured transfer only if the caller explicitly chose to speak with Yoni now and that choice remains current, with no unanswered question or request to wait. " +
        "A stale accepted result never authorizes a transfer after cancellation or a choice of callback or email instead. Do not promise a connection before it completes."
      : "The attempted connection could not be completed now; do not call transfer_to_number or retry the availability tool. " +
        currentCallerPriority +
        "If the caller has already chosen a next action, honor that choice without offering a different one. Otherwise, when no question or stop request is pending, ask once: I couldn't connect with Yoni right now. Would you like to request a callback? Then listen. " +
        "Only an explicit callback request or an unambiguous direct answer to that current callback offer permits callback_requested. " +
        "An intervening question or correction makes the old offer no longer current; answer it, and do not treat a later bare yes as callback consent without clarifying its meaning. " +
        "Use only the caller's requested timing; leave timing unspecified when they gave none. Do not invent ASAP, promise a text or a callback, or say an appointment is booked. " +
        "Questions, thanks, and tool results do not authorize ending. Follow the guarded ending workflow only for a current caller-ending request.";
  return {
    ok: true,
    intent: "live_transfer",
    transferApproved: status === "accepted",
    approvalStatus: status === "pending" ? "in_progress" : status,
    nextAction,
  } as const;
}

export function buildElevenLabsContactOutcomeResponse(
  callResult: VoiceContactResult,
  persistenceStatus: "confirmed" | "unconfirmed",
) {
  return {
    ok: persistenceStatus === "confirmed",
    intent: callResult,
    persistenceStatus,
    requiresReview: persistenceStatus !== "confirmed" || callResult.endsWith("_review"),
    nextAction:
      "This is a contact-outcome receipt, not permission to end the call. " + currentCallerPriority +
      "If a question or correction is current, address it without restarting the sales pitch. " +
      "If the caller's explicit goodbye, service refusal, or request to end this call is still current, use the guarded ending workflow; do not ask them to repeat it. " +
      "Do not manufacture a goodbye, call end_call directly, retry this tool, or infer a callback or transfer. " +
      "A callback cancellation alone is not a request to end this call. Do not promise future contact or claim suppression unless it was confirmed. " +
      "An unconfirmed or review-needed disposition is not evidence that the caller declined the service.",
  } as const;
}

export function buildElevenLabsCallbackRequestResponse(callbackTime: string) {
  return {
    ok: true,
    intent: "callback_requested",
    callbackTime,
    requestCaptured: true,
    queued: true,
    persistenceStatus: "queued",
    durablePersistenceConfirmed: false,
    appointmentConfirmed: false,
    nextAction:
      "When no question or correction is pending, acknowledge once: Thanks. I've received your callback request. " +
      "Only if the caller asks whether it is booked or confirmed, explain that the request was received but no appointment is confirmed. " +
      "Keep the acknowledgment time-free. Do not repeat, reinterpret, or complete callbackTime from this tool result; it is not independently verified caller wording. " +
      "If the caller asks what timing you heard, quote their own words from the conversation without adding missing details. " +
      "ASAP is a timing request, not a promised response time. Do not say the callback is set up, booked, or scheduled, or promise that Yoni will call. " +
      requestContinuation,
  } as const;
}

export function buildElevenLabsInformationRequestResponse(email: string) {
  return {
    ok: true,
    intent: "information_requested",
    email,
    requestCaptured: true,
    queued: true,
    persistenceStatus: "queued",
    durablePersistenceConfirmed: false,
    emailSent: false,
    nextAction:
      "A receipt acknowledgment requires information_requested to have returned requestCaptured: true for this request. Missing or failed tool results are not confirmation, and a verbal promise is not tool execution. " +
      "When no question or correction is pending, acknowledge once: Thanks. I've received your request for information. " +
      "Only if the caller asks whether it was sent, explain that the request was received but sending or delivery is not confirmed. " +
      "Do not promise that Yoni will send it, say it was sent, or imply delivery. An information request is not permission for a callback or transfer. " +
      requestContinuation,
  } as const;
}
