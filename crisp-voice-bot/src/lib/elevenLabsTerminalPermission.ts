import { createHash, randomUUID } from "node:crypto";
import { looksLikeCallEndingRequest } from "./elevenLabsDoNotCall";

export const MAX_TERMINAL_HISTORY_BYTES = 256 * 1024;
const MAX_ENTRIES = 2000;
const MAX_MESSAGE_LENGTH = 12000;

type HistoryEntry = { role: "user" | "agent" | "tool"; message?: string };
type EndDecision = "invalid_input" | "no_caller_words" | "recording_or_hold" | "explicit_stop"
  | "pending_question_or_correction" | "caller_goodbye" | "service_declined"
  | "repeated_purpose_confusion" | "no_ending_request";

export type TerminalPermission = {
  ok: boolean;
  permission: boolean;
  decision: EndDecision;
  conversation_id: string | null;
  latest_user_turn_hash: string | null;
  request_id: string;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function normalize(value: string): string {
  return value.toLowerCase().replace(/[\u2018\u2019]/g, "'").replace(/\s+/g, " ").trim();
}

function readHistory(value: unknown): HistoryEntry[] | null {
  if (typeof value !== "string" || Buffer.byteLength(value, "utf8") > MAX_TERMINAL_HISTORY_BYTES) return null;
  let parsed: unknown;
  try { parsed = JSON.parse(value); } catch { return null; }
  if (!isRecord(parsed) || parsed["x-elevenlabs-history"] !== true || !Array.isArray(parsed.entries)
    || parsed.entries.length > MAX_ENTRIES) return null;
  const entries: HistoryEntry[] = [];
  for (const entry of parsed.entries) {
    if (!isRecord(entry) || !["user", "agent", "tool"].includes(String(entry.role))) return null;
    if (entry.message !== undefined && (typeof entry.message !== "string" || entry.message.length > MAX_MESSAGE_LENGTH)) return null;
    if (entry.role === "user" && (typeof entry.message !== "string" || entry.tool_requests !== undefined || entry.tool_results !== undefined)) return null;
    entries.push({ role: entry.role as HistoryEntry["role"], message: entry.message as string | undefined });
  }
  return entries;
}

function isRecording(text: string): boolean {
  return /\b(?:automated (?:call )?screener|record (?:your )?name|name and (?:the )?reason|connecting your call|after (?:the )?(?:tone|beep)|you(?:'ve| have) reached|press [0-9])\b/.test(text)
    || /(?:^|[.!?]\s+)(?:(?:please|you (?:can|may)) )?leave (?:a |your )?message\b/.test(text)
    || /\b(?:the (?:mailbox|voicemail box) (?:is|has been)|welcome to (?:the )?voicemail)\b/.test(text);
}

function isHold(text: string): boolean {
  return /\b(?:please stay on the line|please hold|hold on|wait (?:a |one )?(?:moment|second))\b/.test(text);
}

function hasUnresolvedRecordingContext(messages: string[]): boolean {
  let recorded = false;
  for (const message of messages) {
    const text = normalize(message);
    if (isRecording(text)) recorded = true;
    else if (/^(?:hi|hello|hey|good (?:morning|afternoon|evening))\b/.test(text)
      || /\b(?:i(?:'m| am) (?:the|an) (?:agent|realtor)|this is [a-z]+ speaking|(?:who|why) (?:are you|is this)|who(?:'s| is) calling)\b/.test(text)) recorded = false;
  }
  return recorded;
}

function withoutPunctuation(text: string): string {
  return text.replace(/^[,\s]+|[.!?;,\s]+$/g, "");
}

function explicitStopIndex(clauses: string[]): number {
  const reversedIndex = [...clauses].reverse().findIndex((clause) => {
    const text = withoutPunctuation(clause).replace(/^(?:ok(?:ay)?|alright|all right|before you go)[, ]+/, "");
    return looksLikeCallEndingRequest(text)
      || /^(?:please[, ]+)?stop(?:[, ]+please)?$/.test(text)
      || /^(?:please[, ]+)?(?:do not|don't|dont|never) (?:call|contact|text|email) (?:me|us|(?:this|my|our) (?:phone )?number)(?: (?:again|anymore|any more|ever|now|please))*$/.test(text)
      || /^(?:please[, ]+)?stop (?:calling|contacting|texting|emailing)(?: (?:me|us|(?:this|my|our) (?:phone )?number))?(?: (?:again|anymore|now|please))*$/.test(text)
      || /^(?:please[, ]+)?(?:take|remove) me (?:off|from) (?:your |the )?(?:call(?:ing)? )?list(?: (?:now|please))*$/.test(text)
      || /^(?:please[, ]+)?no more calls?(?: to (?:this|my|our) (?:phone )?number)?(?: (?:now|please))*$/.test(text);
  });
  return reversedIndex < 0 ? -1 : clauses.length - 1 - reversedIndex;
}

function isClosingCourtesy(text: string): boolean {
  return /^(?:ok(?:ay)?|alright|all right|thanks|thank you|please|bye|goodbye|thanks bye|thank you goodbye|have a (?:good|nice) day)$/.test(withoutPunctuation(text));
}

function isContactComplaint(text: string): boolean {
  return !hasPendingQuestionOrCorrection(text)
    && /^you (?:keep|kept|have been) (?:leaving (?:me )?(?:unwanted )?voicemails?|calling (?:me|us|this number))(?: (?:repeatedly|all day|every day))*[.!]*$/.test(text);
}

function hasOpenQuotation(messages: string[]): boolean {
  let quoting = false;
  for (const message of messages) {
    const text = normalize(message);
    if (/\b(?:i(?:'m| am) (?:going to |about to )?(?:read|reading|quote|quoting)|let me (?:read|quote))\b/.test(text)
      && /\b(?:script|example|line|quote|not ask(?:ing)? you to)\b/.test(text)) quoting = true;
    else if (/\b(?:end of (?:the )?quote|that's (?:the end of )?the quote|that was (?:just )?the (?:quote|example)|i(?:'m| am) (?:done|finished|not) (?:reading|quoting)|now i (?:am|'m) actually asking)\b/.test(text)) quoting = false;
  }
  return quoting;
}

function declinesOnlyOfferedAction(entries: HistoryEntry[], latest: string): boolean {
  if (/\b(?:in (?:the|your|this) (?:service|help|offer)|(?:don't|do not) (?:need|want) (?:any |your |the )?(?:help|service|services|assistance))\b/.test(latest)) return false;
  const reversed = [...entries].reverse();
  const latestUserIndex = reversed.findIndex((entry) => entry.role === "user");
  const precedingAgent = reversed.slice(latestUserIndex + 1).find((entry) => entry.role === "agent" && entry.message?.trim());
  const offered = normalize(precedingAgent?.message ?? "");
  return /\b(?:would you like|do you want|want me to|shall i|should i|can i|would you prefer)\b/.test(offered)
    && /\b(?:transfer|callback|call back|email|send (?:you )?(?:the )?information|bring yoni|(?:yoni|him|a person) (?:on|onto|to join) (?:this|the) call)\b/.test(offered);
}

function hasPendingQuestionOrCorrection(text: string): boolean {
  return /\?/.test(text)
    || /(?:^|[.!;,] |\b(?:but|and) )(?:(?:sorry|okay|ok|so|then)[, ]+)?(?:what|who|where|which|why|when|how (?:much|does|do|would|will|can)|can you|could you|would you|do you|are you|will you)\b/.test(text)
    || /\b(?:wait|before you go|one more|not yet|hold on|instead|unless|except)\b/.test(text)
    || /\bbut\s+(?!(?:thanks|thank you|goodbye|bye)\b)/.test(text)
    || /\b(?:tell me|explain|repeat|i (?:have|had) (?:a|another) question|i only asked|i (?:didn't|did not) (?:say|ask)|i(?:'m| am) not (?:saying|asking)|(?:don't|do not) (?:end|hang up|disconnect|stop))\b/.test(text);
}

function isReportedOrConditional(text: string): boolean {
  return /\b(?:if|unless|hypothetically|for example|suppose|imagine)\b/.test(text)
    || /\b(?:i(?:'m| am) (?:just )?(?:reading|quoting)|(?:the|this|a) (?:script|example|recording) (?:says|said)|(?:said|says) the following|not asking you to|i (?:didn't|did not) (?:say|ask)|i(?:'m| am) not (?:saying|asking))\b/.test(text);
}

function isGoodbye(clauses: string[]): boolean {
  const last = withoutPunctuation(clauses.at(-1) ?? "").replace(/,/g, " ").replace(/\s+/g, " ").trim();
  return /^(?:(?:ok(?:ay)?|alright|all right|no thanks|no thank you|thanks|thank you|that's all|that is all|have a (?:good|nice) day|but) )*(?:bye(?: bye)?|goodbye|have a (?:good|nice) day)(?: (?:thanks|thank you))?$/.test(last)
    || /^(?:ok(?:ay)?[, ]+)?(?:that's all|that is all|that's everything|that is everything)$/.test(last);
}

function isServiceDeclined(text: string): boolean {
  const cleaned = text.replace(/[.!;,]+/g, " ").replace(/\s+/g, " ").trim();
  return /^(?:no(?: (?:thanks|thank you))? )?(?:(?:i(?:'m| am)|we(?:'re| are)) )?not interested(?: in (?:the|your|this) (?:service|help|offer))?(?: (?:thanks|thank you))?$/.test(cleaned)
    || /^(?:no(?: (?:thanks|thank you))? )?(?:i|we) (?:don't|do not) (?:need|want) (?:any |your |the )?(?:help|service|services|assistance)(?: (?:thanks|thank you))?$/.test(cleaned);
}

function isPurposeConfusion(text: string): boolean {
  const cleaned = withoutPunctuation(text).replace(/[,]+/g, " ").replace(/\s+/g, " ").trim();
  return /^(?:what|huh|sorry what|i (?:still )?(?:do not|don't|dont) understand|i(?:'m| am) (?:still )?confused)$/.test(cleaned)
    || /^(?:what do you want(?: from me)?|why (?:are you calling|did you call|are you contacting me)|what is this (?:about|regarding)|what are you calling about)$/.test(cleaned)
    || /^(?:are you (?:a computer|ai|an ai|a bot)[? ]*)?(?:what do you want(?: from me)?|why are you calling)$/.test(cleaned);
}

function hasApprovedRepeatedPurposeExit(entries: HistoryEntry[], latest: string): boolean {
  if (!isPurposeConfusion(latest)) return false;
  const userIndexes = entries.map((entry, index) => entry.role === "user" ? index : -1).filter((index) => index >= 0);
  if (userIndexes.length < 2) return false;
  const latestIndex = userIndexes.at(-1)!;
  const priorIndex = userIndexes.at(-2)!;
  if (!isPurposeConfusion(normalize(entries[priorIndex].message ?? ""))) return false;
  const clarification = entries.slice(priorIndex + 1, latestIndex)
    .filter((entry) => entry.role === "agent")
    .map((entry) => normalize(entry.message ?? ""))
    .join(" ");
  return /i'm calling because .+ is listed as a short sale/.test(clarification)
    && /we take lender paperwork and calls off the listing agent/.test(clarification)
    && /would you like me to explain/.test(clarification);
}

// Only a system-bound, authenticated history may reach this function in production.
// The hash identifies the latest observed user history; it is not an atomic hang-up lock.
export function assessElevenLabsTerminalPermission(input: unknown, requestId = randomUUID()): TerminalPermission {
  const response: TerminalPermission = { ok: false, permission: false, decision: "invalid_input",
    conversation_id: null, latest_user_turn_hash: null, request_id: requestId };
  if (!isRecord(input) || typeof input.conversation_id !== "string"
    || !/^conv_[a-zA-Z0-9]{10,80}$/.test(input.conversation_id)) return response;
  response.conversation_id = input.conversation_id;
  const entries = readHistory(input.history);
  if (!entries) return response;
  response.ok = true;
  const callerMessages = entries.filter((entry) => entry.role === "user").map((entry) => entry.message!);
  const latest = normalize(callerMessages.at(-1) ?? "");
  if (!latest || !/[a-z]/i.test(latest)) return { ...response, decision: "no_caller_words" };
  response.latest_user_turn_hash = createHash("sha256").update(JSON.stringify([input.conversation_id, callerMessages])).digest("hex");
  const clauses = latest.split(/(?<=[.!?;])|\b(?:but|actually)\b|\band (?=(?:please )?(?:stop|end|hang|disconnect|remove|take|never|do not|don't|no more)\b)/)
    .map((clause) => clause.replace(/^[,\s]+|[,\s]+$/g, "")).filter(Boolean);
  // Recordings and reported examples are not live instructions, even if they contain STOP.
  if (isHold(latest) || hasUnresolvedRecordingContext(callerMessages)) return { ...response, decision: "recording_or_hold" };
  if (isReportedOrConditional(latest) || hasOpenQuotation(callerMessages)) return { ...response, decision: "pending_question_or_correction" };
  const stopIndex = explicitStopIndex(clauses);
  if (stopIndex >= 0) {
    // A later wait/question/revocation must not be lost behind an earlier ending request.
    if (clauses.slice(stopIndex + 1).some((clause) => !isClosingCourtesy(clause) && !isContactComplaint(clause))) {
      return { ...response, decision: "pending_question_or_correction" };
    }
    return { ...response, permission: true, decision: "explicit_stop" };
  }
  if (hasApprovedRepeatedPurposeExit(entries, latest)) {
    return { ...response, permission: true, decision: "repeated_purpose_confusion" };
  }
  if (hasPendingQuestionOrCorrection(latest)) return { ...response, decision: "pending_question_or_correction" };
  if (isGoodbye(clauses)) return { ...response, permission: true, decision: "caller_goodbye" };
  if (isServiceDeclined(latest) && !declinesOnlyOfferedAction(entries, latest)) return { ...response, permission: true, decision: "service_declined" };
  return { ...response, decision: "no_ending_request" };
}
