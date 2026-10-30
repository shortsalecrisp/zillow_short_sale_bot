import { normalizeMarker, normalizeString, parseVoiceBotDate } from "./voiceSheet";
import { getVoicePhoneAttemptIdentity } from "./voicePhoneIdentity";

export type ReviewedVoiceDisposition = "verified_final_decline" | "verified_positive_needs" | "verified_caller_goodbye";

export function reviewedDispositionState(row: unknown[], attempt: number, reason: ReviewedVoiceDisposition) {
  const marked = (v: unknown) => !["", "no", "false", "0"].includes(normalizeMarker(v));
  if ([12, 13, 15, 16, 17, 19, 28, 35, 36, 37, 38, 43, 44].some(i => marked(row[i])) ||
      getVoicePhoneAttemptIdentity(row).reviewReason ||
      normalizeString(row[41]).includes("CODEX_VOICE_EXPIRED_TO_MAILSHAKE_V1") ||
      [row[33], row[40]].some(v => ["do_not_call", "callback_requested", "information_requested", "live_transfer_requested", "live_transfer_completed", "deferred_contact"].includes(normalizeString(v)))) {
    throw new Error("Reviewed disposition held: protected contact state");
  }
  const result = normalizeString(row[attempt === 1 ? 33 : 40]), K = normalizeString(row[10]);
  if (!parseVoiceBotDate(row[attempt === 1 ? 32 : 39]) || (attempt === 1 && row[39])) {
    throw new Error("Reviewed disposition held: attempt changed");
  }
  if (reason === "verified_final_decline" && result === "contact_request_review" && !K) {
    return { callResult: "answered_not_interested", leadStatusCode: "R" };
  }
  if (reason === "verified_positive_needs" && attempt === 1 && result === "call_received_agent_hung_up" && K === "N") {
    return { callResult: "interested_followup_review", leadStatusCode: "Y" };
  }
  if (reason === "verified_caller_goodbye" && attempt === 2 && result === "no_response_second_attempt" && K === "N") {
    return { callResult: "call_end_requested", leadStatusCode: "" };
  }
  throw new Error("Reviewed disposition held: previous result/status does not match reviewed scope");
}

export async function verifyReviewedDisposition(row: unknown[], attempt: number, reason: ReviewedVoiceDisposition, receipt: unknown) {
  const target = reviewedDispositionState(row, attempt, reason);
  const classifier = await import("./elevenLabsPostCall");
  const conversation = receipt as Parameters<typeof classifier.getVoiceContactRequestResult>[0];
  const actual = reason === "verified_positive_needs"
    ? classifier.getVoiceContactRequestResult(conversation)
    : classifier.getAuthoritativeTerminalCallResult(conversation, attempt,
      { firstName: normalizeString(row[0]), lastName: normalizeString(row[1]), email: normalizeString(row[3]) });
  if (actual !== target.callResult || classifier.shouldTreatAsDoNotCall(conversation) ||
      classifier.getExplicitCallbackConsent(conversation)) throw new Error("Reviewed disposition final receipt does not prove the requested classification");
  return { ...target, responseStatus: classifier.buildVoiceResponseStatus(target.callResult) };
}
