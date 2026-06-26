import type { CallMetadata } from "../types";
import {
  hasCallbackOrLaterSignal,
  hasClearLiveTransferConsent,
  isMisfiredLiveTransferRequest,
} from "./elevenLabsTransferConsent";

export const VOICE_PERFORMANCE_LOG_MARKER = "CODEX_VOICE_CALL_METRICS_V1";

const MAX_SUMMARY_CHARS = 3_000;
const MAX_TRANSCRIPT_CHARS = 16_000;
const VOICE_AB_TEST_COHORT = "time_bucket_and_voice_rotation";
const VOICE_AB_TEST_STARTED_AT = "2026-05-29T23:33:59Z";
const VOICE_AB_TEST_STARTED_LOCAL = "May 29, 2026 7:33 PM ET";
const VOICE_AB_TEST_INCLUDED_VARIANTS = ["eryn", "finch", "rachel", "bella"] as const;
const CODEX_ANALYSIS_INSTRUCTIONS =
  "When asked how the voice bot performance is going, parse every CODEX_VOICE_CALL_METRICS_V1 block in AP/voice_notes. Compare voiceVariant on live answered calls separately from voicemail/no-answer, and compare scheduledWindow by agent local time bucket for answer-rate lift. For the historical Eryn/Finch A/B test, ignore any call before 2026-05-29T23:33:59Z or without call.voiceVariant. Exclude previous single-voice Emmy calls. Current production rotates Eryn, Finch, Rachel, and Bella behind the public Maya caller name when voice testing is enabled. Also compare call.openerVariant for the opener test: total calls, answered calls, hangupBeforeReason, hangupBeforeOpeningQuestion, reasonDelivered, openingQuestionDelivered, agentRespondedAfterReason, agentRespondedAfterOpeningQuestion, durationSecs, AI suspicion, callbacks, clear live-transfer consent, and completed transfers. Prioritize positiveOutcomeRate, earlyHangupRate, avgAgentToAssistantDelaySecs, durationSecs, aiSuspicion, audioConfusion, repeatedIdentityAsk, callback and transfer outcomes. For transfer rate, count flags.liveTransferRequested / flags.clearLiveTransferConsent only; flags.liveTransferToolFired means only the tool fired, not that the caller understood or requested transfer. Do not count a live_transfer_requested tool call alone as success, and treat flags.misfiredLiveTransferRequest as a negative/ambiguous outcome.";

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
  status?: string;
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
};

function truncate(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }

  return `${value.slice(0, maxLength)}...[truncated ${value.length - maxLength} chars]`;
}

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
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

    if (role === "user") {
      latestAgentTime = timeSecs;
      continue;
    }

    if (isAssistantRole(role) && latestAgentTime !== null) {
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

function hasSuccessfulTransferResult(transcript: PerformanceTranscriptItem[]): boolean {
  return transcript.some((item) =>
    (item.tool_results ?? []).some((toolResult) => {
      if (toolResult.tool_name !== "transfer_to_number") {
        return false;
      }

      if (toolResult.result?.status === "success") {
        return true;
      }

      return typeof toolResult.result_value === "string" && toolResult.result_value.includes('"status":"success"');
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

  return transcript.slice(index + 1).some((item) => item.role === "user" && typeof item.message === "string" && item.message.trim() !== "");
}

function getMessageTimeAtIndex(transcript: PerformanceTranscriptItem[], index: number): number | null {
  if (index < 0) {
    return null;
  }

  return getItemTimeSecs(transcript[index]);
}

export function buildVoicePerformanceLog(input: BuildVoicePerformanceLogInput): string {
  const transcript = input.conversation.transcript ?? [];
  const assistantMessages = transcript
    .filter((item) => isAssistantRole(item.role) && typeof item.message === "string" && item.message.trim() !== "")
    .map((item) => item.message!.trim());
  const agentMessages = transcript
    .filter((item) => item.role === "user" && typeof item.message === "string" && item.message.trim() !== "")
    .map((item) => item.message!.trim());
  const assistantText = normalizeText(assistantMessages.join(" "));
  const agentText = normalizeText(agentMessages.join(" "));
  const combinedText = normalizeText(`${input.outcome} ${input.summary} ${input.transcript}`);
  const toolCallNames = getToolCallNames(transcript);
  const agentToAssistantLatencies = getAgentToAssistantLatencies(transcript);
  const liveTransferToolFired = toolCallNames.includes("live_transfer_requested");
  const clearLiveTransferConsent = hasClearLiveTransferConsent(transcript, input.summary);
  const misfiredLiveTransferRequest = isMisfiredLiveTransferRequest(transcript, input.summary);
  const callbackOrLaterSignal = hasCallbackOrLaterSignal(transcript, input.summary);
  const reasonMessageIndex = firstAssistantMessageIndexMatching(transcript, /\bshort sale\b/i);
  const openingQuestionIndex = firstAssistantMessageIndexMatching(
    transcript,
    /\b(?:handling the bank side|handling that one|already have someone)\b/i,
  );
  const reasonDelivered = reasonMessageIndex !== -1;
  const openingQuestionDelivered = openingQuestionIndex !== -1;
  const agentRespondedAfterReason = hasUserMessageAfter(transcript, reasonMessageIndex);
  const agentRespondedAfterOpeningQuestion = hasUserMessageAfter(transcript, openingQuestionIndex);
  const durationSecs =
    typeof input.conversation.metadata?.call_duration_secs === "number"
      ? input.conversation.metadata.call_duration_secs
      : null;
  const terminationReason = input.conversation.metadata?.termination_reason ?? null;
  const identityAskCount = assistantMessages.filter((message) =>
    /\b(?:is this|can i speak with|trying to reach)\b/i.test(message),
  ).length;
  const aiSuspicion = /\b(?:ai|chatbot|robot|actual human|real person|human being)\b/i.test(agentText);

  const payload = {
    schema: "voice_call_metrics_v1",
    codexInstructions: CODEX_ANALYSIS_INSTRUCTIONS,
    abTestScope: {
      cohort: VOICE_AB_TEST_COHORT,
      startedAt: VOICE_AB_TEST_STARTED_AT,
      startedLocalTime: VOICE_AB_TEST_STARTED_LOCAL,
      includeOnlyVoiceVariants: [...VOICE_AB_TEST_INCLUDED_VARIANTS],
      excludeMissingVoiceVariant: true,
      excludePriorSingleVoiceEmmyCalls: true,
      analysisRule:
        "Only include calls where call.voiceVariant is present and the call happened after the voice split started. Exclude all previous single-voice Emmy calls. Compare current voice variants and scheduledWindow buckets separately.",
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
      assistantName: input.metadata.assistantName ?? "Maya",
      voiceName: input.metadata.voiceName ?? null,
      voiceVariant: input.metadata.voiceVariant ?? null,
      voiceId: input.metadata.voiceId ?? null,
      openerVariant: input.metadata.openerVariant ?? null,
      openerVariantLabel: input.metadata.openerVariantLabel ?? null,
      openerScript: input.metadata.openerScript ?? null,
      scheduledWindow: input.metadata.scheduledWindow ?? null,
      agentTimeZone: input.metadata.agentTimeZone ?? null,
      outcome: input.outcome,
      status: input.conversation.status ?? null,
      terminationReason,
      errorCode: input.conversation.metadata?.error?.code ?? null,
      errorReason: input.conversation.metadata?.error?.reason ?? null,
    },
    metrics: {
      durationSecs,
      agentTurns: agentMessages.length,
      assistantTurns: assistantMessages.length,
      agentWords: words(agentMessages.join(" ")).length,
      assistantWords: words(assistantMessages.join(" ")).length,
      firstAgentToAssistantDelaySecs: agentToAssistantLatencies[0] ?? null,
      avgAgentToAssistantDelaySecs: average(agentToAssistantLatencies),
      maxAgentToAssistantDelaySecs: agentToAssistantLatencies.length
        ? Math.max(...agentToAssistantLatencies)
        : null,
      reasonMentionedAtSecs: getMessageTimeAtIndex(transcript, reasonMessageIndex),
      openingQuestionAtSecs: getMessageTimeAtIndex(transcript, openingQuestionIndex),
      identityAskCount,
      areYouThereCount: (assistantText.match(/\bare you (?:still )?(?:there|on the line)\b/g) ?? []).length,
      clarificationCount: (assistantText.match(/\b(?:what was that|say that again|repeat that|sorry,? i caught)\b/g) ?? [])
        .length,
      audioConfusionCount: (agentText.match(/\b(?:can'?t hear|can you hear|going in and out|breaking up|static|hello\?)\b/g) ?? [])
        .length,
    },
    flags: {
      liveAnswered: agentMessages.length > 0,
      earlyHangupUnder20Secs:
        durationSecs !== null && durationSecs < 20 && normalizeText(terminationReason ?? "").includes("client disconnected"),
      reasonDelivered,
      openingQuestionDelivered,
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
      aiSuspicion,
      audioConfusion: /\b(?:can'?t hear|can you hear|going in and out|breaking up|static|hello\?)\b/i.test(agentText),
      callbackRequested: toolCallNames.includes("callback_requested") || /requested callback/i.test(input.outcome),
      liveTransferToolFired,
      liveTransferRequested: clearLiveTransferConsent,
      clearLiveTransferConsent,
      misfiredLiveTransferRequest,
      callbackOrLaterSignal,
      transferCompleted:
        clearLiveTransferConsent && (hasSuccessfulTransferResult(transcript) || /warm transfer accepted/i.test(input.outcome)),
      voicemailDetected: combinedText.includes("voicemail") || combinedText.includes("voice mail"),
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
