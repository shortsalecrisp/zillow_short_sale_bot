import { normalizeMarker, normalizeString, parseVoiceBotDate } from "./voiceSheet";
import { getVoicePhoneAttemptIdentity } from "./voicePhoneIdentity";
import { currentPhoneVoiceRow } from "./voicePhoneEpisode";

export function isFinalUnansweredVoiceResult(callResult: unknown, callAttemptNumber: unknown): boolean {
  return Number(callAttemptNumber) === 2 && [
    "voicemail_reached_final_attempt", "no_response_second_attempt",
    "agent_not_available", "gatekeeper_admin_early_exit",
  ].includes(normalizeString(callResult));
}

export function finalUnansweredPreservationReason(row: unknown[]): string | undefined {
  row = currentPhoneVoiceRow(row);
  if (normalizeString(row[10])) return "existing_lead_status";
  if (getVoicePhoneAttemptIdentity(row).reviewReason) return "attempt_phone_requires_review";
  const marked = (value: unknown) => {
    const marker = normalizeMarker(value);
    return Boolean(marker) && !["no", "false", "0"].includes(marker);
  };
  if ([15, 16, 19, 35, 36, 37, 38].some(index => marked(row[index]))) return "handoff_callback_or_human_takeover";
  if (/handoff|callback|booking|takeover|opt.?out|do.?not.?contact/i.test(normalizeString(row[13]))) return "contact_preference";
  if (/\b(?:interested|callback|call me|send.{0,12}info|information|email|transfer|handoff|remove|stop|wrong number|not a short sale)\b/i
    .test(normalizeString(row[9]))) return "response_requires_review";
  if ([row[33], row[40]].some(result => [
    "do_not_call", "callback_requested", "information_requested", "interested_followup_review",
    "live_transfer_requested", "live_transfer_completed", "call_end_requested", "call_ended_by_request", "contact_request_review",
  ].includes(normalizeString(result)))) return "protected_contact_outcome";
  if (!parseVoiceBotDate(row[32])) return "missing_first_attempt_history";
  return undefined;
}
