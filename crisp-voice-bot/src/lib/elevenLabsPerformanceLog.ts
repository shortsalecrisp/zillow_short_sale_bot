import type { CallMetadata } from "../types";
import {
  hasCallbackOrLaterSignal,
  hasClearLiveTransferConsent,
  isMisfiredLiveTransferRequest,
} from "./elevenLabsTransferConsent";
import {
  ELEVENLABS_TTS_CONTROL_BRANCH_ID,
  ELEVENLABS_TTS_CONTROL_MODEL,
  ELEVENLABS_TTS_EXPERIMENT_STARTED_AT,
  ELEVENLABS_TTS_TEST_BRANCH_ID,
  ELEVENLABS_TTS_TEST_MODEL,
  ELEVENLABS_TURN_MODEL,
  ELEVENLABS_UNIFORM_COMPARISON,
  uniformComparisonArm,
} from "./elevenLabsRuntimeExperiment";

export const VOICE_PERFORMANCE_LOG_MARKER = "CODEX_VOICE_CALL_METRICS_V1";

const MAX_SUMMARY_CHARS = 3_000;
const MAX_TRANSCRIPT_CHARS = 16_000;
const VOICE_AB_TEST_COHORT = "time_bucket_and_voice_rotation";
const VOICE_AB_TEST_STARTED_AT = "2026-05-29T23:33:59Z";
const VOICE_AB_TEST_STARTED_LOCAL = "May 29, 2026 7:33 PM ET";
const VOICE_AB_TEST_INCLUDED_VARIANTS = ["eryn", "finch"] as const;
const VOICE_PROVE_IT_COHORT_STARTED_AT = "2026-09-03T14:20:21Z";
const VOICE_PROVE_IT_BASELINE_CONVERSATION_COUNT = 1063;
const VOICE_PROVE_IT_TARGET_ADDITIONAL_CALLS_MIN = 300;
const VOICE_PROVE_IT_TARGET_ADDITIONAL_CALLS_MAX = 400;
const POLICY_STRATIFICATION_INSTRUCTIONS =
  "First stratify by call.initialOpeningPolicy, call.declaredConversationPolicyVersion, and providerIdentity.agentId, versionId and branchId. Missing or null historical values are unknown; do not backfill them from current configuration or pool unknown and known policy/provider versions. Declared policy labels describe the call-start code's intended policy, not the provider version or proof that it ran. Provider identity comes only from a matching final conversation receipt. call.openerVariant is a post-intro continuation assignment, not proof of delivery. The permission-screener-20260918 initial introduction is uniform and permission-first. Before that policy, row-parity assignments paired Eryn/direct_reason and Finch/benefit_hook, so compare those older calls as joint arms. Starting with permission-screener-20260918, voiceVariant and openerVariant rotated independently as a 2x2 design; evaluate voice and opener within that stratum separately while still checking transcript/playback evidence for what was actually delivered. Starting with eryn-self-handler-ai-optout-20260925, voice is owner-fixed to Eryn/Maya and Finch is historical only; continue opener and timing analysis without treating voice as an active experiment. Starting with maya-short-opener-ai-closeout-20261002 and listen_first_listing_agent_v2, the live opening is a short listing-agent check and AI-plus-rejection uses a hard closeout; do not pool this stratum with the earlier permission-first opening. Starting with the 2026-09-28 runtime experiment, compare TTS only from matching final provider branch receipts: the main branch is Flash v2 control and the experiment branch is v4 Turbo; both use turn_v3. Do not infer a TTS arm from row number, current configuration, or a missing branch receipt. Actual overrides must be evaluated from their recorded assignments. Existing delivery flags are transcript-derived heuristics, not proof of audible delivery; use transcript/playback evidence to establish which continuation was actually delivered.";
const CODEX_ANALYSIS_INSTRUCTIONS =
  "When asked how the voice bot performance is going, parse every CODEX_VOICE_CALL_METRICS_V1 block in AP/voice_notes. " +
  "For contact-evidence-v2 blocks, use contactEvidence instead of provider call status or rawSignals.hasMeaningfulUserTranscript to count human contact. The legacy flags.liveAnswered now means apparent human speech, including separately labeled gatekeepers; targetAgentAnswered requires transcript identity/role evidence. Null means unknown, never zero. Greeting-only contact is apparent, not authenticated. Older blocks without contactEvidence require transcript reclassification; do not pool their inflated liveAnswered flag. Automated replies never count as agent engagement. Latency fields are transcript turn-start differences, not acoustic response gaps. " +
  POLICY_STRATIFICATION_INSTRUCTIONS + " " +
  "For handoff-evidence-v3 blocks, use handoffEvidence for requests, receipts and completion separately. callbackToolFired is an attempted tool action, not caller consent. callbackRequested requires actual caller consent; callbackRequestCaptured additionally requires requestCaptured true in a matching final receipt. callbackCompleted and positiveHandoffVerified remain unknown without owner completion evidence. Transfer completion requires clear live-now consent plus a successful transfer_to_number result in a matching final receipt; summary/outcome labels alone are insufficient. contactEvidence.reviewBucket separates target_live, human_gatekeeper, apparent_human, recorded_screener, voicemail, ivr and unknown. Exclude recorded_screener and human_gatekeeper from the per-arm true-live winner gate. " +
  "Compare voiceVariant on live answered calls separately from voicemail/no-answer within historical voice-test strata, and compare scheduledWindow by agent local time bucket without treating observational differences as causal lift. For the historical Eryn/Finch comparison, ignore any call before 2026-05-29T23:33:59Z or without call.voiceVariant. Exclude previous single-voice Emmy calls and any call.voiceVariant other than eryn or finch. Current calls under eryn-self-handler-ai-optout-20260925 should show Eryn/Maya only; if Finch appears after that policy, flag it as a production configuration issue instead of an experiment result. Also compare call.openerVariant as the post-intro continuation assignment within policy/provider and joint-arm strata: total calls, answered calls, hangupBeforeReason, hangupBeforeOpeningQuestion, reasonDelivered, openingQuestionDelivered, agentRespondedAfterReason, agentRespondedAfterOpeningQuestion, repeatedIdentityStatement, liveYoniNowOfferDelivered, agentRespondedAfterLiveYoniNowOffer, durationSecs, AI suspicion, callbacks, clear live-transfer consent, and completed transfers. Prioritize positiveOutcomeRate, earlyHangupRate, avgAgentToAssistantDelaySecs, durationSecs, aiSuspicion, audioConfusion, repeatedIdentityStatement, callback and transfer outcomes. For transfer rate, count flags.liveTransferRequested / flags.clearLiveTransferConsent only; flags.liveTransferToolFired means only the tool fired, not that the caller understood or requested transfer. Do not count a live_transfer_requested tool call alone as success, and treat flags.misfiredLiveTransferRequest as a negative/ambiguous outcome. For the Pro prove-it cohort, evaluate calls after 2026-09-03T14:20:21Z against the 1063-conversation ElevenLabs baseline with policy/provider strata kept separate, and trigger a decision review once 300-400 additional calls have accumulated. Count bot-labeled positives separately from transcript/playback-verified handoff-ready leads; continue only if the cohort produces at least 3 verified handoff-ready leads or 1 owner-confirmed serious file opportunity, otherwise recommend pausing or narrowing the test.";

type TranscriptToolCall = {
  tool_name?: string;
  name?: string;
};

type TranscriptToolResult = {
  tool_name?: string;
  result_value?: string;
  result?: {
    status?: string;
    [key: string]: unknown;
  };
};

type PerformanceTranscriptItem = {
  role?: string;
  message?: string;
  time_in_call_secs?: number | null;
  start_time_in_call_secs?: number | null;
  start_time_secs?: number | null;
  start_time?: number | null;
  tool_calls?: TranscriptToolCall[];
  tool_results?: TranscriptToolResult[];
};

type PerformanceConversation = {
  conversation_id?: unknown;
  agent_id?: unknown;
  version_id?: unknown;
  branch_id?: unknown;
  status?: string;
  analysis?: {
    call_successful?: unknown;
    transcript_summary?: string;
    call_summary_title?: string;
  };
  metadata?: {
    termination_reason?: string | null;
    call_duration_secs?: number | null;
    error?: {
      code?: number;
      reason?: string;
      [key: string]: unknown;
    } | null;
    [key: string]: unknown;
  };
  transcript?: PerformanceTranscriptItem[];
};

type BuildVoicePerformanceLogInput = {
  conversationId: string;
  metadata: CallMetadata;
  conversation: PerformanceConversation;
  outcome: string;
  summary: string;
  transcript: string;
  callbackConsent?: { callbackTime: string } | null;
};

function truncate(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }

  return `${value.slice(0, maxLength)}...[truncated ${value.length - maxLength} chars]`;
}

function optionalLabel(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/[\u2018\u2019]/g, "'").replace(/\s+/g, " ").trim();
}

type AutomatedContact = "voicemail" | "screening" | "ivr";

function automatedContactType(message: string): AutomatedContact | null {
  const text = normalizeText(message);
  if (/\b(?:call assist|google (?:call )?screening|automated (?:call )?screener)\b/.test(text)) return "screening";
  if (/\b(?:your call has been forwarded|after (?:the )?(?:tone|beep)|at the (?:tone|beep)|leave (?:me |us )?(?:a |your )?(?:(?:brief|detailed|short|voice) )?(?:message|name)|record (?:a |your )?message|you(?:'ve| have) reached|you (?:have )?reached (?:the )?(?:voice ?mail|mailbox)|welcome to (?:the )?voice ?mail|(?:sorry,? )?i missed your call|your voicemail is being transcribed)\b/.test(text) ||
      /\b(?:message with your name|message or send me a text|call is very important to me)\b/.test(text) ||
      /^(?:hi[, ]+)?(?:this is|you(?:'ve| have) reached) [\p{L}'-]+\b.{0,100}\b(?:away from (?:my )?phone|unable to (?:answer|take) (?:your )?call|can't (?:answer|come to) (?:the )?phone|cannot (?:answer|come to) (?:the )?phone)\b/u.test(text) ||
      /\b(?:mailbox|voice ?mail(?: box)?)\b.{0,60}\b(?:full|not (?:been )?set up|hasn't been set up|not initialized|cannot accept|can't accept|unavailable|not accepting)\b/.test(text)) {
    return "voicemail";
  }
  if (/\b(?:record|state) (?:your )?name(?: and (?:the )?reason)?\b/.test(text) ||
      /\b(?:call assist by google|call screening|screening (?:service|assistant)|automated (?:system|assistant)|(?:i am|i'm|this is)\b.{0,35}\b(?:ai|virtual|automated) (?:assistant|receptionist)|please stay on the(?: line)?|please hold while|one moment while|i'll (?:see if (?:this person|they|he|she) is available|try to connect you)|checking with the person you called|please say who you are and why|this person is (?:not )?available)\b/.test(text) ||
      /^\.{2,}\s*(?:person )?is available/.test(text) || /\bthis call is being recorded for quality assurance\b/.test(text)) {
    return "screening";
  }
  if (/\b(?:press|dial) (?:one|two|three|four|five|six|seven|eight|nine|zero|[0-9])\b/.test(text) ||
      /\b(?:call cannot be completed|number (?:you (?:have )?dialed )?is (?:not in service|disconnected)|all (?:of our )?(?:representatives|agents) are busy)\b/.test(text)) {
    return "ivr";
  }
  return null;
}

function classifyContact(transcript: PerformanceTranscriptItem[], fullName: string) {
  const humanIndexes = new Set<number>();
  const automatedIndexes = new Set<number>();
  const automation = new Set<AutomatedContact>();
  const expectedFirstName = normalizeText(fullName).split(/\s+/)[0]?.replace(/[^\p{L}\p{N}'-]/gu, "");
  let pending: number[] = [];
  let hasContextualHumanTurn = false;
  let inAutomation = false;
  const gatekeeperIndexes = new Set<number>();
  const targetIndexes = new Set<number>();
  let lastAssistantMessage = "";
  let hasSpokenUserContent = false;

  const flush = (beforeAutomation: boolean) => {
    // A greeting split from the rest of a recording is not a human pickup.
    if (!beforeAutomation || hasContextualHumanTurn) pending.forEach((index) => humanIndexes.add(index));
    pending = [];
    hasContextualHumanTurn = false;
  };

  for (let index = 0; index < transcript.length; index += 1) {
    const item = transcript[index];
    if (isAssistantRole(item.role) && typeof item.message === "string" && hasMeaningfulSpokenContent(item.message)) {
      lastAssistantMessage = normalizeText(item.message);
      continue;
    }
    if (item.role !== "user" || typeof item.message !== "string" || !hasMeaningfulSpokenContent(item.message)) continue;
    hasSpokenUserContent = true;
    const text = normalizeText(item.message);
    const automatedType = automatedContactType(text);
    if (automatedType) {
      flush(true);
      automatedIndexes.add(index);
      automation.add(automatedType);
      inAutomation = true;
      lastAssistantMessage = "";
      continue;
    }

    const bareText = text.replace(/[.!?,]+/g, " ").replace(/\s+/g, " ").trim();
    const canned = /^(?:(?:thank you|thanks|goodbye|please hold|not available|as soon as possible|one moment)\s*)+$/.test(bareText) ||
      /^(?:thank you|thanks)(?:[,. ]+(?:and )?have a (?:great|good|wonderful) day|[, ]+maya[.! ]*(?:please[-.]*)?)?[.!]*$/.test(text);
    const greeting = /^(?:(?:hi|hello|hey|good (?:morning|afternoon|evening))\b|(?:this is|it's|it is)\s+[\p{L}'-]+\b|(?:thank you|thanks) for calling\b)/u.test(text);
    const identity = text.match(/\b(?:this is|it is|it's|i am|i'm)\s+([\p{L}'-]+)\b/u)?.[1];
    const namedPickup = Boolean(identity) && /\b(?:this is|it is|it's) [\p{L}'-]+(?: speaking)?(?:[.!?]|$)/u.test(text);
    const nameOnly = bareText === normalizeText(fullName) || bareText === expectedFirstName;
    const targetIdentity = Boolean(expectedFirstName && (identity === expectedFirstName || nameOnly));
    const explicitRole = /\bi(?: am|'m) (?:the )?(?:listing agent|agent of record)\b/.test(text);
    const explicitAdmin = /\b(?:i(?: am|'m)|this is) (?:his |her |their |the |an? )?(?:admin(?:istrative)?(?: assistant)?|assistant|receptionist|transaction coordinator)\b/.test(text) ||
      /\bthis is [\p{L}'-]+[, ]+[^.!?]{0,35}'s (?:assistant|admin|receptionist)\b/u.test(text);
    const officeGreeting = Boolean(identity) && /\b(?:real estate|realty|brokerage|office|group|team)\b/.test(text);
    const differentOfficeSpeaker = Boolean(officeGreeting && identity && expectedFirstName && identity !== expectedFirstName);
    const lastQuestion = lastAssistantMessage.match(/(?:^|[.!?])\s*([^.!?]+\?)\s*$/)?.[1] ?? lastAssistantMessage;
    const simpleIdentityQuestion = /\b(?:is this|am i speaking (?:with|to))\b/.test(lastQuestion) ||
      /\bare you the listing agent\b/.test(lastQuestion) || /\bis .{1,120} your listing\?/.test(lastQuestion);
    const identityAnswer = /^(?:yes|yeah|yep|speaking|this is (?:he|she)|i am)[.!?]*$/.test(text) &&
      simpleIdentityQuestion && !/\band\b|\b(?:handling|want|help|may i|can i)\b/.test(lastQuestion);
    const directHumanQuestion = /\b(?:who is this|why are you calling|what (?:is this|are you calling) about|can you hear me|i can't hear you)\b/.test(text);
    const newLiveIdentity = greeting || namedPickup || explicitRole || explicitAdmin || identityAnswer || nameOnly || officeGreeting || directHumanQuestion;
    const automationContinuation = inAutomation && !newLiveIdentity && (
      canned || /^(?:yes|no|okay|ok|sure)[.!?]*$/.test(text) ||
      /\b(?:can you tell me more about the details|person (?:you called|you're calling|you are calling) (?:is|was)|leave an additional message)\b/.test(text)
    );
    if (automationContinuation || (canned && pending.length === 0)) {
      automatedIndexes.add(index);
      continue;
    }

    // Unknown fragments without dialogue context do not establish a person.
    const contextual = Boolean(lastAssistantMessage) && !canned && !greeting &&
      !/^\.{2,}|^[\d\s()+.-]+$|^(?:phone rings|ringing)$/i.test(text);
    if (!newLiveIdentity && !contextual && pending.length === 0) continue;
    if (inAutomation && !newLiveIdentity) continue;
    pending.push(index);
    hasContextualHumanTurn ||= contextual || explicitRole || explicitAdmin || (officeGreeting && /\bhow (?:can|may) (?:i|we) help\b/.test(text));
    if (newLiveIdentity || contextual) inAutomation = false;
    if (explicitAdmin || differentOfficeSpeaker) gatekeeperIndexes.add(index);
    if (explicitRole || identityAnswer || targetIdentity) targetIndexes.add(index);
  }
  flush(false);

  const gatekeeper = [...gatekeeperIndexes].some((index) => humanIndexes.has(index));
  const targetConfirmed = [...targetIndexes].some((index) => humanIndexes.has(index));
  const humanAnswered = humanIndexes.size > 0 ? true : automation.size > 0 ? false : null;
  const targetAgentAnswered = humanAnswered === false ? false : humanAnswered === null ? null :
    targetConfirmed ? true : gatekeeper ? false : null;
  const category = humanAnswered ? targetConfirmed ? "target_agent" : gatekeeper ? "human_gatekeeper" : "apparent_human" :
    automation.has("voicemail") ? "voicemail" : automation.has("ivr") ? "ivr" : automation.has("screening") ? "screening" : "unknown";
  return { humanIndexes, automatedIndexes, hasSpokenUserContent, humanAnswered, targetAgentAnswered, category,
    gatekeeper: humanAnswered === true && gatekeeper, automation: [...automation] };
}

function hasMeaningfulSpokenContent(value: string): boolean {
  return /[\p{L}\p{N}]/u.test(value);
}

function isAssistantRole(role?: string): boolean {
  const normalizedRole = role?.toLowerCase();
  return normalizedRole === "assistant" || normalizedRole === "agent";
}

function words(value: string): string[] {
  return value.trim().split(/\s+/).filter(Boolean);
}

function roundOne(value: number): number {
  return Math.round(value * 10) / 10;
}

function average(values: number[]): number | null {
  if (values.length === 0) {
    return null;
  }

  return roundOne(values.reduce((sum, value) => sum + value, 0) / values.length);
}

function getItemTimeSecs(item: PerformanceTranscriptItem): number | null {
  const candidates = [
    item.time_in_call_secs,
    item.start_time_in_call_secs,
    item.start_time_secs,
    item.start_time,
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "number" && Number.isFinite(candidate)) {
      return roundOne(candidate);
    }
  }

  return null;
}

function getAgentToAssistantLatencies(transcript: PerformanceTranscriptItem[]): number[] {
  const latencies: number[] = [];
  let latestAgentTime: number | null = null;

  for (const item of transcript) {
    const role = item.role?.toLowerCase();
    const timeSecs = getItemTimeSecs(item);

    if (timeSecs === null) {
      continue;
    }

    if (role === "user" && typeof item.message === "string" && hasMeaningfulSpokenContent(item.message)) {
      latestAgentTime = timeSecs;
      continue;
    }

    if (isAssistantRole(role) && typeof item.message === "string" && hasMeaningfulSpokenContent(item.message) && latestAgentTime !== null) {
      const latency = roundOne(timeSecs - latestAgentTime);
      if (latency >= 0 && latency <= 60) {
        latencies.push(latency);
      }
      latestAgentTime = null;
    }
  }

  return latencies;
}

function getToolCallNames(transcript: PerformanceTranscriptItem[]): string[] {
  return transcript.flatMap((item) =>
    (item.tool_calls ?? [])
      .map((toolCall) => toolCall.tool_name ?? toolCall.name ?? "")
      .filter((toolName) => toolName.trim() !== ""),
  );
}

function readToolResult(toolResult: TranscriptToolResult): Record<string, unknown> | null {
  if (toolResult.result && typeof toolResult.result === "object") return toolResult.result;
  if (typeof toolResult.result_value !== "string") return null;
  try {
    const parsed: unknown = JSON.parse(toolResult.result_value);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed as Record<string, unknown> : null;
  } catch { return null; }
}

function hasCapturedCallbackResult(transcript: PerformanceTranscriptItem[], requestedTime: string): boolean {
  return transcript.some((item) => (item.tool_results ?? []).some((result) => {
    const receipt = readToolResult(result);
    return result.tool_name === "callback_requested" && receipt?.requestCaptured === true &&
      typeof receipt.callbackTime === "string" && normalizeText(receipt.callbackTime) === normalizeText(requestedTime);
  }));
}

function hasSuccessfulTransferResult(transcript: PerformanceTranscriptItem[]): boolean {
  return transcript.some((item) =>
    (item.tool_results ?? []).some((toolResult) => {
      if (toolResult.tool_name !== "transfer_to_number") {
        return false;
      }

      return readToolResult(toolResult)?.status === "success";
    }),
  );
}

function firstAssistantMessageIndexMatching(
  transcript: PerformanceTranscriptItem[],
  pattern: RegExp,
): number {
  return transcript.findIndex(
    (item) => isAssistantRole(item.role) && typeof item.message === "string" && pattern.test(item.message),
  );
}

function hasUserMessageAfter(transcript: PerformanceTranscriptItem[], index: number): boolean {
  if (index < 0) {
    return false;
  }

  return transcript
    .slice(index + 1)
    .some(
      (item) =>
        item.role === "user" &&
        typeof item.message === "string" &&
        hasMeaningfulSpokenContent(item.message),
    );
}

function getMessageTimeAtIndex(transcript: PerformanceTranscriptItem[], index: number): number | null {
  if (index < 0) {
    return null;
  }

  return getItemTimeSecs(transcript[index]);
}

export function buildVoicePerformanceLog(input: BuildVoicePerformanceLogInput): string {
  const transcript = input.conversation.transcript ?? [];
  const contact = classifyContact(transcript, input.metadata.fullName);
  const humanTranscript = transcript.map((item, index) => item.role === "user" && !contact.humanIndexes.has(index)
    ? { ...item, message: undefined }
    : item);
  const firstHumanIndex = [...contact.humanIndexes][0] ?? -1;
  const automationBeforeHuman = [...contact.automatedIndexes].some((index) => index < firstHumanIndex);
  const liveSpeechTranscript = humanTranscript.map((item, index) =>
    contact.humanAnswered !== true || (automationBeforeHuman && index < firstHumanIndex)
      ? { ...item, message: undefined }
      : item);
  const assistantMessages = transcript
    .filter((item) => isAssistantRole(item.role) && typeof item.message === "string" && item.message.trim() !== "")
    .map((item) => item.message!.trim());
  const rawUserMessages = transcript
    .filter(
      (item) =>
        item.role === "user" &&
        typeof item.message === "string" &&
        hasMeaningfulSpokenContent(item.message),
    )
    .map((item) => item.message!.trim());
  const agentMessages = [...contact.humanIndexes].map((index) => transcript[index].message!.trim());
  const assistantText = normalizeText(assistantMessages.join(" "));
  const agentText = normalizeText(agentMessages.join(" "));
  const combinedText = normalizeText(`${input.outcome} ${input.summary} ${input.transcript}`);
  const toolCallNames = getToolCallNames(transcript);
  const agentToAssistantLatencies = getAgentToAssistantLatencies(humanTranscript);
  const liveTransferToolFired = toolCallNames.includes("live_transfer_requested");
  const clearLiveTransferConsent = contact.humanAnswered === true && hasClearLiveTransferConsent(humanTranscript, "");
  const misfiredLiveTransferRequest = isMisfiredLiveTransferRequest(humanTranscript, "");
  const callbackOrLaterSignal = contact.humanAnswered === true && hasCallbackOrLaterSignal(humanTranscript, "");
  const usesServiceFirstOpening = ["maya-service-first-recovery-20261003", "maya-uniform-lead-conversion-20261009"]
    .includes(input.metadata.declaredConversationPolicyVersion ?? "");
  const usesListingAgentOpening = input.metadata.initialOpeningPolicy === "listen_first_listing_agent_v2";
  const reasonMessageIndex = firstAssistantMessageIndexMatching(
    liveSpeechTranscript,
    usesServiceFirstOpening ? /\bwe help with short-sale lender paperwork\b/i :
      usesListingAgentOpening ? /\bwe help with lender paperwork and calls\b/i : /\bshort sale\b/i,
  );
  const openingQuestionIndex = firstAssistantMessageIndexMatching(
    liveSpeechTranscript,
    usesServiceFirstOpening
      ? /\bis .{1,120} your listing\?/i
      : usesListingAgentOpening
      ? /\bare you the listing agent for the short sale at\b/i
      : /\b(?:handling the bank side|handling that one|handling the short sale paperwork|short sale paperwork and lender calls|looking for help with that|looking for help with this)\b/i,
  );
  const liveYoniNowOfferIndex = firstAssistantMessageIndexMatching(
    liveSpeechTranscript,
    /\b(?:bring Yoni|get Yoni|Yoni.*onto (?:this|the) call|Yoni.*on the phone|try him (?:right )?now|available (?:right )?now)\b/i,
  );
  const needsQuestionIndex = firstAssistantMessageIndexMatching(liveSpeechTranscript,
    /\bwould help with lender paperwork or calls be useful for this listing\?/i);
  const reasonDelivered = reasonMessageIndex !== -1;
  const openingQuestionDelivered = openingQuestionIndex !== -1;
  const agentRespondedAfterReason = hasUserMessageAfter(humanTranscript, reasonMessageIndex);
  const agentRespondedAfterOpeningQuestion = hasUserMessageAfter(humanTranscript, openingQuestionIndex);
  const durationSecs =
    typeof input.conversation.metadata?.call_duration_secs === "number"
      ? input.conversation.metadata.call_duration_secs
      : null;
  const terminationReason = input.conversation.metadata?.termination_reason ?? null;
  const identityAskCount = assistantMessages.filter((message) =>
    /\b(?:is this|can i speak with|trying to reach)\b/i.test(message),
  ).length;
  const identityStatementCount = assistantMessages.filter((message) =>
    /\b(?:this is|i'm)\s+\S+\s+with Crisp Short Sales\b/i.test(message),
  ).length;
  const aiSuspicion = /\b(?:ai|chatbot|robot|actual human|real person|human being)\b/i.test(agentText);
  const finalReceiptMatched = input.conversation.conversation_id === input.conversationId &&
    ["done", "failed"].includes(input.conversation.status ?? "");
  const callbackConsent = contact.humanAnswered === true && Boolean(input.callbackConsent);
  const callbackRequestCaptured = finalReceiptMatched && callbackConsent &&
    hasCapturedCallbackResult(transcript, input.callbackConsent!.callbackTime);
  const transferCompleted = finalReceiptMatched && clearLiveTransferConsent && hasSuccessfulTransferResult(transcript);

  const payload = {
    schema: "voice_call_metrics_v1",
    measurementRevision: "handoff-evidence-v3",
    codexInstructions: CODEX_ANALYSIS_INSTRUCTIONS,
    abTestScope: {
      cohort: VOICE_AB_TEST_COHORT,
      startedAt: VOICE_AB_TEST_STARTED_AT,
      startedLocalTime: VOICE_AB_TEST_STARTED_LOCAL,
      includeOnlyVoiceVariants: [...VOICE_AB_TEST_INCLUDED_VARIANTS],
      excludeMissingVoiceVariant: true,
      excludePriorSingleVoiceEmmyCalls: true,
      analysisRule:
        "Only include calls where call.voiceVariant is present and the call happened after the voice split started. Exclude all previous single-voice Emmy calls. " +
        POLICY_STRATIFICATION_INSTRUCTIONS,
    },
    proveItCohort: {
      startedAt: VOICE_PROVE_IT_COHORT_STARTED_AT,
      baselineConversationCount: VOICE_PROVE_IT_BASELINE_CONVERSATION_COUNT,
      targetAdditionalCallsMin: VOICE_PROVE_IT_TARGET_ADDITIONAL_CALLS_MIN,
      targetAdditionalCallsMax: VOICE_PROVE_IT_TARGET_ADDITIONAL_CALLS_MAX,
      decisionRule:
        "Analyze calls after startedAt once 300-400 additional calls have accumulated. Continue scaling only if there are at least 3 transcript/playback-verified handoff-ready leads or 1 owner-confirmed serious file opportunity.",
    },
    ttsModelExperiment: {
      startedAt: ELEVENLABS_TTS_EXPERIMENT_STARTED_AT,
      uniformComparison: {
        ...ELEVENLABS_UNIFORM_COMPARISON,
        eligibleArm: uniformComparisonArm({ finalReceiptMatched, testMode: Boolean(input.metadata.testMode || input.metadata.providerProofCall),
          durationSecs, startTimeUnixSecs: input.conversation.metadata?.start_time_unix_secs,
          branchId: input.conversation.branch_id, versionId: input.conversation.version_id }),
      },
      commonTurnModel: ELEVENLABS_TURN_MODEL,
      commonVoice: "Eryn as Maya",
      arms: {
        [ELEVENLABS_TTS_CONTROL_BRANCH_ID]: {
          key: "flash_v2_control",
          ttsModel: ELEVENLABS_TTS_CONTROL_MODEL,
          trafficPercent: 50,
        },
        [ELEVENLABS_TTS_TEST_BRANCH_ID]: {
          key: "v4_turbo_test",
          ttsModel: ELEVENLABS_TTS_TEST_MODEL,
          trafficPercent: 50,
        },
      },
      analysisRule:
        "Use providerIdentity.branchId only from a matching final conversation receipt. Compare live answered calls separately from voicemail/no-answer and hold opener, time bucket, and policy strata constant.",
    },
    call: {
      conversationId: input.conversationId,
      rowNumber: input.metadata.rowNumber,
      callAttemptNumber: input.metadata.callAttemptNumber,
      agentName: input.metadata.fullName,
      listingAddress: input.metadata.listingAddress,
      requestedPhone: input.metadata.requestedPhone,
      dialedPhone: input.metadata.dialedPhone,
      testMode: input.metadata.testMode,
      providerProofCall: input.metadata.providerProofCall ?? false,
      assistantName: input.metadata.assistantName ?? "Maya",
      voiceName: input.metadata.voiceName ?? null,
      voiceVariant: input.metadata.voiceVariant ?? null,
      voiceId: input.metadata.voiceId ?? null,
      openerVariant: input.metadata.openerVariant ?? null,
      openerVariantLabel: input.metadata.openerVariantLabel ?? null,
      openerScript: input.metadata.openerScript ?? null,
      initialOpeningPolicy: optionalLabel(input.metadata.initialOpeningPolicy),
      declaredConversationPolicyVersion: optionalLabel(input.metadata.declaredConversationPolicyVersion),
      scheduledWindow: input.metadata.scheduledWindow ?? null,
      agentTimeZone: input.metadata.agentTimeZone ?? null,
      outcome: input.outcome,
      status: input.conversation.status ?? null,
      terminationReason,
      errorCode: input.conversation.metadata?.error?.code ?? null,
      errorReason: input.conversation.metadata?.error?.reason ?? null,
    },
    providerIdentity: {
      source: finalReceiptMatched ? "final_conversation_receipt" : null,
      agentId: finalReceiptMatched ? optionalLabel(input.conversation.agent_id) : null,
      versionId: finalReceiptMatched ? optionalLabel(input.conversation.version_id) : null,
      branchId: finalReceiptMatched ? optionalLabel(input.conversation.branch_id) : null,
    },
    rawSignals: {
      // This preserves the old liveAnswered heuristic, not a provider human verdict.
      hasMeaningfulUserTranscript: Array.isArray(input.conversation.transcript) ? rawUserMessages.length > 0 : null,
      userTurns: Array.isArray(input.conversation.transcript) ? rawUserMessages.length : null,
      userWords: Array.isArray(input.conversation.transcript) ? words(rawUserMessages.join(" ")).length : null,
      providerStatus: input.conversation.status ?? null,
      providerCallSuccessful: input.conversation.analysis?.call_successful ?? null,
      providerVoicemailDetectionUsed: input.conversation.metadata?.features_usage &&
        typeof input.conversation.metadata.features_usage === "object"
        ? (input.conversation.metadata.features_usage as { voicemail_detection?: { used?: boolean } }).voicemail_detection?.used ?? null
        : null,
    },
    contactEvidence: {
      source: "transcript_heuristic",
      category: contact.category,
      reviewBucket: contact.category === "target_agent" ? "target_live" :
        contact.category === "screening" ? "recorded_screener" : contact.category,
      humanAnswered: contact.humanAnswered,
      targetAgentAnswered: contact.targetAgentAnswered,
      gatekeeper: contact.gatekeeper,
      automatedStages: contact.automation,
      greetingOnly: contact.humanAnswered === true && agentMessages.every((message) =>
        /^(?:(?:hi|hello|hey|good (?:morning|afternoon|evening))\b|(?:this is|it's|it is)\s+)/i.test(message)),
      humanTurnIndexes: [...contact.humanIndexes],
      humanRespondedAfterReason: contact.humanAnswered === null ? null : agentRespondedAfterReason,
      humanRespondedAfterOpeningQuestion: contact.humanAnswered === null ? null : agentRespondedAfterOpeningQuestion,
      interpretation: "Apparent human speech is not authenticated identity. Target contact requires a spoken name/role confirmation; admins are separate. Null is unknown. Review playback for audio delivery and disputed classifications.",
    },
    handoffEvidence: {
      source: "caller_transcript_and_matching_final_tool_receipt",
      callbackToolFired: toolCallNames.includes("callback_requested"),
      callbackRequested: input.callbackConsent === undefined ? null : callbackConsent,
      callbackTime: callbackConsent ? input.callbackConsent!.callbackTime : null,
      callbackRequestCaptured,
      callbackCompleted: null,
      clearLiveTransferConsent,
      transferToolFired: liveTransferToolFired,
      transferCompleted,
      positiveHandoffVerified: null,
      interpretation: "A request receipt is not a completed callback. A successful provider transfer result with consent is technical completion, not owner-confirmed business success. Missing owner completion evidence remains unknown.",
    },
    metrics: {
      durationSecs,
      agentTurns: Array.isArray(input.conversation.transcript) ? agentMessages.length : null,
      assistantTurns: Array.isArray(input.conversation.transcript) ? assistantMessages.length : null,
      agentWords: Array.isArray(input.conversation.transcript) ? words(agentMessages.join(" ")).length : null,
      assistantWords: Array.isArray(input.conversation.transcript) ? words(assistantMessages.join(" ")).length : null,
      firstAgentToAssistantDelaySecs: agentToAssistantLatencies[0] ?? null,
      avgAgentToAssistantDelaySecs: average(agentToAssistantLatencies),
      maxAgentToAssistantDelaySecs: agentToAssistantLatencies.length
        ? Math.max(...agentToAssistantLatencies)
        : null,
      latencyMeasurement: "transcript_turn_start_to_start_not_audible_response_gap",
      reasonMentionedAtSecs: getMessageTimeAtIndex(transcript, reasonMessageIndex),
      openingQuestionAtSecs: getMessageTimeAtIndex(transcript, openingQuestionIndex),
      needsQuestionAtSecs: getMessageTimeAtIndex(transcript, needsQuestionIndex),
      liveYoniNowOfferAtSecs: getMessageTimeAtIndex(transcript, liveYoniNowOfferIndex),
      identityAskCount,
      identityStatementCount,
      areYouThereCount: (assistantText.match(/\bare you (?:still )?(?:there|on the line)\b/g) ?? []).length,
      clarificationCount: (assistantText.match(/\b(?:what was that|say that again|repeat that|sorry,? i caught)\b/g) ?? [])
        .length,
      audioConfusionCount: (agentText.match(/\b(?:can'?t hear|can you hear|going in and out|breaking up|static|hello\?)\b/g) ?? [])
        .length,
    },
    flags: {
      liveAnswered: contact.humanAnswered === true,
      humanAnswered: contact.humanAnswered,
      targetAgentAnswered: contact.targetAgentAnswered,
      humanGatekeeperAnswered: contact.gatekeeper,
      earlyHangupUnder20Secs:
        contact.humanAnswered === true && durationSecs !== null && durationSecs < 20 && normalizeText(terminationReason ?? "").includes("client disconnected"),
      reasonDelivered,
      openingQuestionDelivered,
      needsQuestionDelivered: needsQuestionIndex !== -1,
      agentRespondedAfterNeedsQuestion: hasUserMessageAfter(humanTranscript, needsQuestionIndex),
      agentRespondedAfterReason,
      agentRespondedAfterOpeningQuestion,
      hangupBeforeReason:
        agentMessages.length > 0 &&
        durationSecs !== null &&
        normalizeText(terminationReason ?? "").includes("client disconnected") &&
        !reasonDelivered,
      hangupBeforeOpeningQuestion:
        agentMessages.length > 0 &&
        durationSecs !== null &&
        normalizeText(terminationReason ?? "").includes("client disconnected") &&
        !openingQuestionDelivered,
      repeatedIdentityAsk: identityAskCount > 1,
      repeatedIdentityStatement: identityStatementCount > 1,
      liveYoniNowOfferDelivered: liveYoniNowOfferIndex !== -1,
      agentRespondedAfterLiveYoniNowOffer: hasUserMessageAfter(humanTranscript, liveYoniNowOfferIndex),
      aiSuspicion,
      audioConfusion: /\b(?:can'?t hear|can you hear|going in and out|breaking up|static|hello\?)\b/i.test(agentText),
      callbackRequested: callbackConsent,
      callbackToolFired: toolCallNames.includes("callback_requested"),
      callbackRequestCaptured,
      liveTransferToolFired,
      liveTransferRequested: clearLiveTransferConsent,
      clearLiveTransferConsent,
      misfiredLiveTransferRequest,
      callbackOrLaterSignal,
      transferCompleted,
      voicemailDetected: contact.automation.includes("voicemail"),
      noAnswer: combinedText.includes("no answer") || combinedText.includes("no response after second call"),
      notInterested: /not interested/i.test(input.outcome),
      notShortSale: /not a short sale/i.test(input.outcome),
      alreadyHasHelp: /already working/i.test(input.outcome),
    },
    summary: truncate(input.summary.trim(), MAX_SUMMARY_CHARS),
    transcript: truncate(input.transcript.trim(), MAX_TRANSCRIPT_CHARS),
  };

  return `--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\n${JSON.stringify(payload)}`;
}
