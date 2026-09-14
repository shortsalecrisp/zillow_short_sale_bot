const requestContinuation =
  "If the latest caller turn contains an unanswered question or correction, address it first instead of giving a closing acknowledgment. " +
  "After a brief answer or acknowledgment, pause for a NEW caller turn. Do not ask an anything-else question or restart qualification. " +
  "Questions and corrections are not farewells. Silence, thanks, okay, or an earlier caller turn is not by itself permission to end. " +
  "Honor new corrections or cancellations; they supersede the earlier request. " +
  "A new explicit stop or cancellation received while this tool was pending also overrides acknowledgment and pause instructions. " +
  "End only after a NEW explicit goodbye or request to stop or end the current call, following the existing contact-preference policy. " +
  "Do not call end_call merely because this tool returned. This receipt is not new consent or proof of durable persistence, booking, sending, or delivery.";

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
