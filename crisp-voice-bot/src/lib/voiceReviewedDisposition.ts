import { normalizeMarker, normalizeString, parseVoiceBotDate } from "./voiceSheet";
import { getVoicePhoneAttemptIdentity } from "./voicePhoneIdentity";

export type ReviewedVoiceDisposition = "verified_final_decline" | "verified_final_opt_out" | "verified_completed_transfer" | "verified_positive_needs" | "verified_caller_goodbye" | "verified_historical_gatekeeper" | "verified_terminal_voicemail_menu" | "verified_completed_gatekeeper_message";

export function reviewedDispositionState(row: unknown[], attempt: number, reason: ReviewedVoiceDisposition) {
  const marked = (v: unknown) => !["", "no", "false", "0"].includes(normalizeMarker(v));
  const owningResultIndex = attempt === 1 ? 33 : 40;
  const completingTransfer = reason === "verified_completed_transfer" &&
    ["interested_followup_review", "live_transfer_requested"].includes(normalizeString(row[owningResultIndex]));
  if ([12, 13, 15, 16, 17, 19, 28, 35, 36, 37, 38, 43, 44].some(i =>
      !(completingTransfer && i === 35 && normalizeMarker(row[i]) === "yes") && marked(row[i])) ||
      getVoicePhoneAttemptIdentity(row).reviewReason ||
      normalizeString(row[41]).includes("CODEX_VOICE_EXPIRED_TO_MAILSHAKE_V1") ||
      [33, 40].some(i => !(completingTransfer && i === owningResultIndex && normalizeString(row[i]) === "live_transfer_requested") &&
        ["do_not_call", "callback_requested", "information_requested", "live_transfer_requested", "live_transfer_completed", "deferred_contact"].includes(normalizeString(row[i])))) {
    throw new Error("Reviewed disposition held: protected contact state");
  }
  const result = normalizeString(row[attempt === 1 ? 33 : 40]), K = normalizeString(row[10]);
  if (!parseVoiceBotDate(row[attempt === 1 ? 32 : 39]) || (attempt === 1 && row[39] && reason !== "verified_historical_gatekeeper")) {
    throw new Error("Reviewed disposition held: attempt changed");
  }
  if (reason === "verified_historical_gatekeeper" && attempt === 1 && result === "voicemail_reached" && K === "N" &&
      parseVoiceBotDate(row[39]) && normalizeString(row[40]) === "call_received_agent_hung_up" &&
      [29, 30, 31, 34].every(i => !normalizeString(row[i]))) {
    return { callResult: "gatekeeper_admin_early_exit", leadStatusCode: K };
  }
  if (reason === "verified_final_decline" && result === "contact_request_review" && !K) {
    return { callResult: "answered_not_interested", leadStatusCode: "R" };
  }
  if (reason === "verified_completed_gatekeeper_message" && result === "contact_request_review" && !K) {
    return { callResult: "gatekeeper_admin_early_exit", leadStatusCode: attempt === 2 ? "N" : "" };
  }
  if (reason === "verified_final_opt_out" &&
      ((result === "contact_request_review" && !K) || (result === "interested_followup_review" && K === "Y"))) {
    return { callResult: "do_not_call", leadStatusCode: "R" };
  }
  if (reason === "verified_completed_transfer" && completingTransfer && ["", "Y", "G"].includes(K)) {
    return { callResult: "warm_transfer_completed", leadStatusCode: "G" };
  }
  if (reason === "verified_positive_needs" && attempt === 1 && result === "call_received_agent_hung_up" && K === "N") {
    return { callResult: "interested_followup_review", leadStatusCode: "Y" };
  }
  if (reason === "verified_caller_goodbye" && attempt === 2 && result === "no_response_second_attempt" && K === "N") {
    return { callResult: "call_end_requested", leadStatusCode: "" };
  }
  if (reason === "verified_terminal_voicemail_menu" && attempt === 2 && result === "call_received_agent_hung_up" && K === "N" &&
      parseVoiceBotDate(row[32]) && [29, 30, 31, 34].every(i => !normalizeString(row[i]))) {
    return { callResult: "voicemail_reached_final_attempt", leadStatusCode: K };
  }
  throw new Error("Reviewed disposition held: previous result/status does not match reviewed scope");
}

export async function verifyReviewedDisposition(row: unknown[], attempt: number, reason: ReviewedVoiceDisposition, receipt: unknown) {
  const target = reviewedDispositionState(row, attempt, reason);
  const classifier = await import("./elevenLabsPostCall");
  const conversation = receipt as Parameters<typeof classifier.getVoiceContactRequestResult>[0];
  if (reason === "verified_terminal_voicemail_menu") {
    const { hasTerminalVoicemailManagementMenu } = await import("./elevenLabsRecordingState");
    if (!hasTerminalVoicemailManagementMenu(conversation.transcript ?? [])) throw new Error("Reviewed voicemail requires an exact terminal management menu");
  }
  const actual = reason === "verified_completed_transfer"
    ? (conversation.status === "done" && classifier.hasSuccessfulTransfer(conversation) ? "warm_transfer_completed" : undefined)
    : reason === "verified_positive_needs"
    ? classifier.getVoiceContactRequestResult(conversation)
    : classifier.getAuthoritativeTerminalCallResult(conversation, attempt,
      { firstName: normalizeString(row[0]), lastName: normalizeString(row[1]), email: normalizeString(row[3]) });
  if (actual !== target.callResult ||
      (reason !== "verified_final_opt_out" && classifier.shouldTreatAsDoNotCall(conversation)) ||
      classifier.getExplicitCallbackConsent(conversation) ||
      (reason === "verified_historical_gatekeeper" && classifier.getVoiceContactRequestResult(conversation)) ||
      (reason === "verified_completed_gatekeeper_message" &&
        (!classifier.hasCompletedGatekeeperMessage(conversation) || classifier.hasSuccessfulTransfer(conversation)))) {
    throw new Error("Reviewed disposition final receipt does not prove the requested classification");
  }
  return { ...target, responseStatus: classifier.buildVoiceResponseStatus(target.callResult) };
}
