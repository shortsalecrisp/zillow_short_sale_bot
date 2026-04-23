import axios, { AxiosError } from "axios";
import { config } from "./config";
import { logger } from "./logger";
import { sendCallbackEmail } from "./sendCallbackEmail";
import { sendCallTranscriptEmail } from "./sendCallTranscriptEmail";
import { postSheetUpdate } from "./sheetUpdateClient";
import type { CallMetadata } from "../types";

const FIRST_CHECK_DELAY_MS = 90_000;
const RETRY_DELAY_MS = 30_000;
const MAX_ATTEMPTS = 5;

type ElevenLabsTranscriptItem = {
  role?: string;
  message?: string;
  tool_calls?: Array<{ tool_name?: string; name?: string; [key: string]: unknown }>;
  tool_results?: Array<{
    tool_name?: string;
    result_value?: string;
    result?: {
      status?: string;
      result_type?: string;
      [key: string]: unknown;
    };
    [key: string]: unknown;
  }>;
};

type ElevenLabsConversation = {
  status?: string;
  analysis?: {
    transcript_summary?: string;
    call_summary_title?: string;
  };
  transcript?: ElevenLabsTranscriptItem[];
};

const processedConversationIds = new Set<string>();

const elevenLabsApi = axios.create({
  baseURL: config.elevenLabs.baseUrl,
  timeout: 15_000,
  headers: {
    ...(config.elevenLabs.apiKey ? { "xi-api-key": config.elevenLabs.apiKey } : {}),
  },
});

function getErrorDetails(error: unknown): Record<string, unknown> {
  if (error instanceof AxiosError) {
    return {
      status: error.response?.status,
      data: error.response?.data,
      message: error.message,
    };
  }

  return {
    message: error instanceof Error ? error.message : String(error),
  };
}

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function transcriptText(conversation: ElevenLabsConversation): string {
  return (conversation.transcript ?? [])
    .map((item) => `${item.role ?? "unknown"}: ${item.message ?? ""}`.trim())
    .filter(Boolean)
    .join(" | ");
}

function transcriptForEmail(conversation: ElevenLabsConversation): string {
  return (conversation.transcript ?? [])
    .filter((item) => typeof item.message === "string" && item.message.trim() !== "")
    .map((item) => {
      const role = (item.role ?? "unknown").toLowerCase();
      const speaker = role === "user" ? "Agent" : role === "assistant" ? "Emmy" : role;
      return `${speaker}: ${item.message?.trim() ?? ""}`;
    })
    .join("\n");
}

function assistantMessages(conversation: ElevenLabsConversation): string[] {
  return (conversation.transcript ?? [])
    .filter((item) => item.role === "assistant" && typeof item.message === "string" && item.message.trim() !== "")
    .map((item) => item.message!.trim());
}

function hasToolCall(conversation: ElevenLabsConversation, toolName: string): boolean {
  return (conversation.transcript ?? []).some((item) =>
    (item.tool_calls ?? []).some((toolCall) => toolCall.tool_name === toolName || toolCall.name === toolName),
  );
}

function hasSuccessfulTransfer(conversation: ElevenLabsConversation): boolean {
  return (conversation.transcript ?? []).some((item) =>
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

function hasLiveTransferRequest(conversation: ElevenLabsConversation): boolean {
  return hasToolCall(conversation, "live_transfer_requested");
}

function buildVoiceResponseStatus(callResult: string, callbackTime?: string): string {
  if (callResult === "warm_transfer_completed") {
    return "Warm transfer accepted";
  }

  if (callResult === "callback_requested") {
    const normalizedCallbackTime = callbackTime?.trim();
    if (!normalizedCallbackTime || normalizedCallbackTime.toLowerCase() === "asap") {
      return "Requested callback ASAP";
    }

    return `Requested callback at ${normalizedCallbackTime}`;
  }

  if (callResult === "answered_not_interested") {
    return "Not interested";
  }

  if (callResult === "voicemail_left") {
    return "Left voicemail";
  }

  if (callResult === "no_answer_first_attempt") {
    return "No answer on first call";
  }

  if (callResult === "no_response_second_attempt") {
    return "No response after second call";
  }

  return callResult;
}

function extractCallbackTime(conversation: ElevenLabsConversation): string | undefined {
  const userMessages = (conversation.transcript ?? [])
    .filter((item) => item.role === "user" && item.message)
    .map((item) => item.message as string);
  const summary = conversation.analysis?.transcript_summary ?? "";
  const candidates = [...userMessages.reverse(), summary];

  for (const candidate of candidates) {
    const match = candidate.match(
      /\b(?:at\s+)?((?:today|tomorrow|later today|this afternoon|this evening)\s+)?(\d{1,2}(?::\d{2})?\s*(?:a\.?m\.?|p\.?m\.?|am|pm))\b/i,
    );

    if (match) {
      return `${match[1] ?? ""}${match[2]}`.replace(/\s+/g, " ").trim();
    }
  }

  return undefined;
}

function shouldTreatAsCallback(conversation: ElevenLabsConversation): boolean {
  const text = normalizeText(`${conversation.analysis?.transcript_summary ?? ""} ${transcriptText(conversation)}`);
  return (
    text.includes("call back") ||
    text.includes("callback") ||
    text.includes("call you") ||
    text.includes("reach out")
  );
}

function isLiveTransferFallback(conversation: ElevenLabsConversation): boolean {
  const text = normalizeText(`${conversation.analysis?.transcript_summary ?? ""} ${transcriptText(conversation)}`);
  return (
    text.includes("live transfer") ||
    text.includes("transfer was attempted") ||
    text.includes("transfer failed") ||
    text.includes("facilitating the transfer") ||
    text.includes("patch him in") ||
    text.includes("waiting for yoni") ||
    text.includes("come on the line") ||
    text.includes("was not available") ||
    text.includes("did not answer")
  );
}

function shouldTreatAsNotInterested(conversation: ElevenLabsConversation): boolean {
  const text = normalizeText(`${conversation.analysis?.transcript_summary ?? ""} ${transcriptText(conversation)}`);
  return text.includes("not interested") || text.includes("has it handled") || text.includes("have it handled");
}

function shouldTreatAsVoicemail(conversation: ElevenLabsConversation): boolean {
  const text = normalizeText(`${conversation.analysis?.transcript_summary ?? ""} ${transcriptText(conversation)}`);
  return (
    text.includes("voicemail") ||
    text.includes("voice mail") ||
    text.includes("answering machine") ||
    text.includes("leave a message") ||
    text.includes("left a message") ||
    text.includes("after the tone") ||
    text.includes("at the beep")
  );
}

function hasDeliveredVoicemailMessage(conversation: ElevenLabsConversation): boolean {
  const assistantText = normalizeText(assistantMessages(conversation).join(" "));

  if (!assistantText) {
    return false;
  }

  return (
    assistantText.includes("this is emmy with crisp short sales") &&
    assistantText.includes("call back at 404-300-9526")
  );
}

function shouldTreatAsNoAnswer(conversation: ElevenLabsConversation): boolean {
  const text = normalizeText(`${conversation.analysis?.transcript_summary ?? ""} ${transcriptText(conversation)}`);
  return (
    text.includes("no answer") ||
    text.includes("did not answer") ||
    text.includes("went unanswered") ||
    text.includes("could not reach") ||
    text.includes("didn't pick up") ||
    text.includes("did not pick up")
  );
}

async function fetchConversation(conversationId: string): Promise<ElevenLabsConversation> {
  const response = await elevenLabsApi.get<ElevenLabsConversation>(`/v1/convai/conversations/${conversationId}`);
  return response.data;
}

async function sendTranscriptEmailIfEnabled(params: {
  conversationId: string;
  metadata: CallMetadata;
  outcome: string;
  summary: string;
  transcript: string;
}): Promise<void> {
  if (!config.emailAlerts.sendCallTranscripts) {
    return;
  }

  try {
    await sendCallTranscriptEmail({
      agentName: params.metadata.fullName,
      requestedPhone: params.metadata.requestedPhone,
      dialedPhone: params.metadata.dialedPhone,
      listingAddress: params.metadata.listingAddress,
      rowNumber: params.metadata.rowNumber,
      callAttemptNumber: params.metadata.callAttemptNumber,
      conversationId: params.conversationId,
      outcome: params.outcome,
      summary: params.summary,
      transcript: params.transcript,
      testMode: params.metadata.testMode,
    });
  } catch (error) {
    logger.error("Call transcript email failed", {
      conversationId: params.conversationId,
      rowNumber: params.metadata.rowNumber,
      callAttemptNumber: params.metadata.callAttemptNumber,
      ...getErrorDetails(error),
    });
  }
}

async function processPostCallOutcome(conversationId: string, metadata: CallMetadata): Promise<boolean> {
  if (processedConversationIds.has(conversationId)) {
    logger.info("Skipping already processed ElevenLabs post-call outcome", {
      conversationId,
      rowNumber: metadata.rowNumber,
    });
    return true;
  }

  const conversation = await fetchConversation(conversationId);
  const summary = conversation.analysis?.transcript_summary ?? transcriptText(conversation);
  const fullTranscript = transcriptForEmail(conversation);

  if (hasToolCall(conversation, "callback_requested")) {
    const callbackTime = isLiveTransferFallback(conversation) ? "asap" : (extractCallbackTime(conversation) ?? "unspecified");

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("callback_requested", callbackTime),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback skipped outcome handling because callback tool already ran", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callbackTime,
    });
    return true;
  }

  if (hasToolCall(conversation, "not_interested")) {
    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("answered_not_interested"),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback skipped because tool already ran", {
      conversationId,
      rowNumber: metadata.rowNumber,
    });
    return true;
  }

  if (hasSuccessfulTransfer(conversation)) {
    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult: "warm_transfer_completed",
      responseStatus: buildVoiceResponseStatus("warm_transfer_completed"),
      leadStatusCode: "G",
      liveTransferRequested: "yes",
      liveTransferCompleted: "yes",
      voiceNotes: summary,
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("warm_transfer_completed"),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback recorded successful warm transfer", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
    });
    return true;
  }

  if (conversation.status === "failed" && hasLiveTransferRequest(conversation)) {
    const callbackTime = "asap";

    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult: "callback_requested",
      responseStatus: buildVoiceResponseStatus("callback_requested", callbackTime),
      leadStatusCode: "Y",
      callbackRequested: "yes",
      callbackTime,
      liveTransferRequested: "yes",
      voiceNotes: summary,
    });

    await sendCallbackEmail({
      agentName: metadata.fullName,
      phone: metadata.dialedPhone,
      listingAddress: metadata.listingAddress,
      rowNumber: metadata.rowNumber,
      action: "Call this lead back ASAP",
      callbackTime,
      conversationDescription: summary,
      details:
        "The caller agreed to a live transfer, but the patch-through did not complete cleanly. Call this lead back ASAP.",
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("callback_requested", callbackTime),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback converted failed live transfer into callback follow-up", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      status: conversation.status,
    });
    return true;
  }

  if (conversation.status !== "done") {
    if (conversation.status === "failed") {
      await sendTranscriptEmailIfEnabled({
        conversationId,
        metadata,
        outcome: "Call failed before completion",
        summary,
        transcript: fullTranscript,
      });

      processedConversationIds.add(conversationId);
      logger.info("ElevenLabs post-call fallback skipped because conversation failed before a final outcome", {
        conversationId,
        rowNumber: metadata.rowNumber,
        status: conversation.status,
      });
      return true;
    }

    logger.info("ElevenLabs conversation not finished yet; will retry post-call outcome", {
      conversationId,
      rowNumber: metadata.rowNumber,
      status: conversation.status,
    });
    return false;
  }

  if (shouldTreatAsCallback(conversation)) {
    const callbackTime = isLiveTransferFallback(conversation) ? "asap" : (extractCallbackTime(conversation) ?? "unspecified");

    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult: "callback_requested",
      responseStatus: buildVoiceResponseStatus("callback_requested", callbackTime),
      leadStatusCode: "Y",
      callbackRequested: "yes",
      callbackTime,
      voiceNotes: summary,
    });

    await sendCallbackEmail({
      agentName: metadata.fullName,
      phone: metadata.dialedPhone,
      listingAddress: metadata.listingAddress,
      rowNumber: metadata.rowNumber,
      action: "Call this lead back at the requested time",
      callbackTime,
      conversationDescription: summary,
      details: "Post-call fallback detected a callback request from the ElevenLabs transcript.",
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("callback_requested", callbackTime),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback sent callback email", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callbackTime,
    });
    return true;
  }

  if (shouldTreatAsNotInterested(conversation)) {
    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult: "answered_not_interested",
      responseStatus: buildVoiceResponseStatus("answered_not_interested"),
      leadStatusCode: "R",
      voiceNotes: summary,
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("answered_not_interested"),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback recorded not interested", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
    });
    return true;
  }

  if (shouldTreatAsVoicemail(conversation) || shouldTreatAsNoAnswer(conversation)) {
    const isFirstAttempt = metadata.callAttemptNumber <= 1;
    const voicemailDetected = shouldTreatAsVoicemail(conversation);
    const voicemailLeft = voicemailDetected && hasDeliveredVoicemailMessage(conversation);
    const callResult = isFirstAttempt
      ? voicemailLeft
        ? "voicemail_left"
        : "no_answer_first_attempt"
      : "no_response_second_attempt";

    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult,
      responseStatus: buildVoiceResponseStatus(callResult),
      ...(isFirstAttempt ? {} : { leadStatusCode: "N" }),
      ...(voicemailLeft && isFirstAttempt ? { vmLeft: "yes" } : {}),
      voiceNotes: summary,
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus(callResult),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback recorded unanswered call", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      voicemailDetected,
      voicemailLeft,
      hadAssistantVoicemailMessage: hasDeliveredVoicemailMessage(conversation),
      finalOutcome: isFirstAttempt ? "follow_up_call_pending" : "no_response_closed_out",
    });
    return true;
  }

  if (metadata.callAttemptNumber <= 1) {
    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult: "no_answer_first_attempt",
      responseStatus: buildVoiceResponseStatus("no_answer_first_attempt"),
      voiceNotes: summary,
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("no_answer_first_attempt"),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback converted unclear answered call into retryable first miss", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      summary,
    });
    return true;
  }

  if (metadata.callAttemptNumber > 1) {
    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult: "no_response_second_attempt",
      responseStatus: buildVoiceResponseStatus("no_response_second_attempt"),
      leadStatusCode: "N",
      voiceNotes: summary,
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("no_response_second_attempt"),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback converted unclear second attempt into no-response closeout", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      summary,
    });
    return true;
  }

  await sendTranscriptEmailIfEnabled({
    conversationId,
    metadata,
    outcome: "No actionable outcome detected",
    summary,
    transcript: fullTranscript,
  });

  logger.info("ElevenLabs post-call fallback found no actionable outcome", {
    conversationId,
    rowNumber: metadata.rowNumber,
    summary,
  });
  processedConversationIds.add(conversationId);
  return true;
}

export function scheduleElevenLabsPostCallFallback(params: {
  conversationId: string;
  metadata: CallMetadata;
}): void {
  const run = (attempt: number) => {
    void processPostCallOutcome(params.conversationId, params.metadata)
      .then((done) => {
        if (!done && attempt < MAX_ATTEMPTS) {
          setTimeout(() => run(attempt + 1), RETRY_DELAY_MS);
        }
      })
      .catch((error) => {
    logger.error("ElevenLabs post-call fallback failed", {
      conversationId: params.conversationId,
      rowNumber: params.metadata.rowNumber,
      callAttemptNumber: params.metadata.callAttemptNumber,
      attempt,
      ...getErrorDetails(error),
    });

        if (attempt < MAX_ATTEMPTS) {
          setTimeout(() => run(attempt + 1), RETRY_DELAY_MS);
        }
      });
  };

  setTimeout(() => run(1), FIRST_CHECK_DELAY_MS);

  logger.info("Scheduled ElevenLabs post-call fallback", {
    conversationId: params.conversationId,
    rowNumber: params.metadata.rowNumber,
    callAttemptNumber: params.metadata.callAttemptNumber,
    firstCheckDelayMs: FIRST_CHECK_DELAY_MS,
  });
}
