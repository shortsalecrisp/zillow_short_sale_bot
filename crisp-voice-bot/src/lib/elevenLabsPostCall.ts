import axios, { AxiosError } from "axios";
import { config } from "./config";
import { logger } from "./logger";
import { sendCallbackEmail } from "./sendCallbackEmail";
import { sendCallTranscriptEmail } from "./sendCallTranscriptEmail";
import { getElevenLabsCallContextByConversationId } from "./elevenLabsCallContext";
import { postSheetUpdate, requestVoiceQueueRefill } from "./sheetUpdateClient";
import type { CallMetadata } from "../types";

const FIRST_CHECK_DELAY_MS = 90_000;
const RETRY_DELAY_MS = 30_000;
const MAX_ATTEMPTS = 5;
const CALL_CONNECT_RETRY_DELAY_MS = 30_000;
const MAX_CALL_CONNECT_RETRIES = 1;

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
  has_audio?: boolean;
  has_user_audio?: boolean;
  has_response_audio?: boolean;
  metadata?: {
    termination_reason?: string | null;
    accepted_time_unix_secs?: number | null;
    call_duration_secs?: number | null;
    error?: {
      code?: number;
      reason?: string;
      [key: string]: unknown;
    } | null;
    features_usage?: {
      voicemail_detection?: {
        enabled?: boolean;
        used?: boolean;
      };
      [key: string]: unknown;
    };
    [key: string]: unknown;
  };
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

export function buildVoiceResponseStatus(callResult: string, callbackTime?: string): string {
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

  if (callResult === "not_short_sale") {
    return "Not a short sale";
  }

  if (callResult === "voicemail_left") {
    return "Left Vm";
  }

  if (callResult === "no_answer_first_attempt") {
    return "No answer on first call";
  }

  if (callResult === "no_response_second_attempt") {
    return "No response after second call";
  }

  if (callResult === "call_received_agent_hung_up") {
    return "Call received but agent hung up on Emmy";
  }

  if (callResult === "call_failed_invalid_number") {
    return "Call failed - invalid phone number";
  }

  return callResult;
}

function getFailedConversationReason(conversation: ElevenLabsConversation): string {
  const reason = conversation.metadata?.error?.reason;
  if (typeof reason === "string" && reason.trim()) {
    return reason.trim();
  }

  const terminationReason = conversation.metadata?.termination_reason;
  if (typeof terminationReason === "string" && terminationReason.trim()) {
    return terminationReason.trim();
  }

  return "Call failed before completion";
}

function isInvalidDestinationNumberFailure(conversation: ElevenLabsConversation): boolean {
  const errorCode = conversation.metadata?.error?.code;
  const reason = normalizeText(getFailedConversationReason(conversation));

  return (
    errorCode === 404 &&
    (reason.includes("invalid destination number") ||
      reason.includes("invalid number") ||
      reason.includes("sip status: 404"))
  );
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
  if (shouldTreatAsVoicemail(conversation) || hasDeliveredVoicemailMessage(conversation)) {
    return false;
  }

  if (endedBecauseCallerStoppedResponding(conversation)) {
    return false;
  }

  const summary = normalizeText(conversation.analysis?.transcript_summary ?? "");
  const userText = normalizeText(userMessages(conversation).join(" "));

  return (
    userText.includes("call me back") ||
    userText.includes("call us back") ||
    userText.includes("call you back") ||
    userText.includes("call back later") ||
    userText.includes("give me a call") ||
    userText.includes("have yoni call") ||
    userText.includes("have him call") ||
    userText.includes("ask him to call") ||
    userText.includes("yoni can call") ||
    userText.includes("reach out to me") ||
    userText.includes("reach back out") ||
    userText.includes("call later") ||
    userText.includes("call tomorrow") ||
    summary.includes("requested a callback") ||
    summary.includes("asked for a callback") ||
    summary.includes("requested a call back") ||
    summary.includes("asked for a call back") ||
    summary.includes("asked yoni to call") ||
    summary.includes("wanted yoni to call") ||
    summary.includes("arranged for yoni to call") ||
    summary.includes("scheduled callback")
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

export function shouldTreatAsNotShortSale(conversation: ElevenLabsConversation): boolean {
  const text = normalizeText(`${conversation.analysis?.transcript_summary ?? ""} ${transcriptText(conversation)}`);

  return (
    /\b(?:not|isn't|isnt|wasn't|wasnt)\s+(?:actually\s+)?(?:a\s+)?short sale\b/.test(text) ||
    /\b(?:not|isn't|isnt|wasn't|wasnt)\s+(?:actually\s+)?(?:a\s+)?short-sale\b/.test(text)
  );
}

function shouldTreatAsNotInterested(conversation: ElevenLabsConversation): boolean {
  const text = normalizeText(`${conversation.analysis?.transcript_summary ?? ""} ${transcriptText(conversation)}`);
  return (
    text.includes("not interested") ||
    text.includes("has it handled") ||
    text.includes("have it handled") ||
    text.includes("already have someone handling") ||
    text.includes("already handling it") ||
    text.includes("already working with an attorney") ||
    text.includes("already working with attorney") ||
    text.includes("already working with a negotiator") ||
    text.includes("already working with negotiator") ||
    text.includes("already have a specialist") ||
    text.includes("already got someone handling") ||
    text.includes("already got it handled")
  );
}

function shouldTreatAsVoicemail(conversation: ElevenLabsConversation): boolean {
  if (usedVoicemailDetectionTool(conversation)) {
    return true;
  }

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

function getVoicemailDetectionMessage(conversation: ElevenLabsConversation): string {
  for (const item of conversation.transcript ?? []) {
    for (const toolResult of item.tool_results ?? []) {
      if (toolResult.tool_name !== "voicemail_detection") {
        continue;
      }

      const voicemailMessage =
        typeof toolResult.result?.voicemail_message === "string"
          ? toolResult.result.voicemail_message
          : undefined;
      if (voicemailMessage?.trim()) {
        return voicemailMessage.trim();
      }

      if (typeof toolResult.result_value === "string" && toolResult.result_value.trim()) {
        try {
          const parsed = JSON.parse(toolResult.result_value) as { voicemail_message?: unknown };
          if (typeof parsed.voicemail_message === "string" && parsed.voicemail_message.trim()) {
            return parsed.voicemail_message.trim();
          }
        } catch {
          // Ignore malformed result_value payloads and fall through to transcript heuristics.
        }
      }
    }
  }

  return "";
}

function usedVoicemailDetectionTool(conversation: ElevenLabsConversation): boolean {
  const voicemailFeatureUsed = conversation.metadata?.features_usage?.voicemail_detection?.used === true;
  const terminationReason = normalizeText(conversation.metadata?.termination_reason ?? "");

  if (voicemailFeatureUsed || terminationReason.includes("voicemail_detection tool was called")) {
    return true;
  }

  return (conversation.transcript ?? []).some((item) =>
    (item.tool_calls ?? []).some((toolCall) => toolCall.tool_name === "voicemail_detection" || toolCall.name === "voicemail_detection") ||
    (item.tool_results ?? []).some((toolResult) => toolResult.tool_name === "voicemail_detection"),
  );
}

function hasDeliveredVoicemailMessage(conversation: ElevenLabsConversation): boolean {
  if (getVoicemailDetectionMessage(conversation)) {
    return true;
  }

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

export function shouldTreatAsUnconnectedInitiatedConversation(conversation: ElevenLabsConversation): boolean {
  return (
    conversation.status === "initiated" &&
    !conversation.metadata?.accepted_time_unix_secs &&
    !conversation.metadata?.call_duration_secs &&
    conversation.has_audio !== true &&
    conversation.has_user_audio !== true &&
    conversation.has_response_audio !== true &&
    (conversation.transcript ?? []).length === 0
  );
}

export function shouldRetryUnconnectedConversation(
  conversation: ElevenLabsConversation,
  metadata: Pick<CallMetadata, "callConnectRetryCount">,
): boolean {
  return (
    shouldTreatAsUnconnectedInitiatedConversation(conversation) &&
    (metadata.callConnectRetryCount ?? 0) < MAX_CALL_CONNECT_RETRIES
  );
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function retryUnconnectedElevenLabsCall(params: {
  originalConversationId: string;
  metadata: CallMetadata;
}): Promise<void> {
  const nextMetadata: CallMetadata = {
    ...params.metadata,
    callConnectRetryCount: (params.metadata.callConnectRetryCount ?? 0) + 1,
  };

  logger.info("Retrying unconnected ElevenLabs call after delay", {
    originalConversationId: params.originalConversationId,
    rowNumber: params.metadata.rowNumber,
    callAttemptNumber: params.metadata.callAttemptNumber,
    callConnectRetryCount: nextMetadata.callConnectRetryCount,
    retryDelayMs: CALL_CONNECT_RETRY_DELAY_MS,
  });

  await delay(CALL_CONNECT_RETRY_DELAY_MS);

  const { placeElevenLabsOutboundCall } = await import("./elevenLabs");
  const retryResponse = await placeElevenLabsOutboundCall({
    to: nextMetadata.dialedPhone,
    metadata: nextMetadata,
    schedulePostCallFallback: false,
  });

  if (!retryResponse.conversation_id) {
    throw new Error("ElevenLabs retry call accepted without a conversation_id");
  }

  logger.info("Unconnected ElevenLabs call retry accepted", {
    originalConversationId: params.originalConversationId,
    retryConversationId: retryResponse.conversation_id,
    rowNumber: params.metadata.rowNumber,
    callAttemptNumber: params.metadata.callAttemptNumber,
    sipCallId: retryResponse.sip_call_id,
  });

  scheduleElevenLabsPostCallFallback({
    conversationId: retryResponse.conversation_id,
    metadata: nextMetadata,
  });
}

function userMessages(conversation: ElevenLabsConversation): string[] {
  return (conversation.transcript ?? [])
    .filter((item) => item.role === "user" && typeof item.message === "string" && item.message.trim() !== "")
    .map((item) => item.message!.trim());
}

function endedBecauseCallerStoppedResponding(conversation: ElevenLabsConversation): boolean {
  const summary = normalizeText(conversation.analysis?.transcript_summary ?? "");
  if (
    summary.includes("no verbal responses") ||
    summary.includes("lack of engagement") ||
    summary.includes("lack of response") ||
    summary.includes("no response from the user")
  ) {
    return true;
  }

  return (conversation.transcript ?? []).some((item) =>
    (item.tool_calls ?? []).some((toolCall) => {
      if (toolCall.tool_name !== "end_call" && toolCall.name !== "end_call") {
        return false;
      }

      const params = typeof toolCall.params_as_json === "string" ? toolCall.params_as_json : "";
      const normalizedParams = normalizeText(params);
      return normalizedParams.includes("no response") || normalizedParams.includes("lost connection");
    }),
  );
}

function hasMeaningfulUserInteraction(conversation: ElevenLabsConversation): boolean {
  const messages = userMessages(conversation);
  if (messages.length >= 2) {
    return true;
  }

  return messages.some((message) => normalizeText(message).split(" ").filter(Boolean).length >= 3);
}

export function shouldTreatAsAgentHungUp(conversation: ElevenLabsConversation): boolean {
  if (
    shouldTreatAsVoicemail(conversation) ||
    shouldTreatAsNoAnswer(conversation) ||
    shouldTreatAsCallback(conversation) ||
    shouldTreatAsNotShortSale(conversation) ||
    shouldTreatAsNotInterested(conversation) ||
    hasToolCall(conversation, "callback_requested") ||
    hasToolCall(conversation, "not_interested") ||
    hasSuccessfulTransfer(conversation)
  ) {
    return false;
  }

  const terminationReason = normalizeText(conversation.metadata?.termination_reason ?? "");
  if (!terminationReason.includes("client disconnected")) {
    return false;
  }

  return hasMeaningfulUserInteraction(conversation);
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

async function processPostCallOutcome(
  conversationId: string,
  metadata: CallMetadata,
  options: { finalAttempt?: boolean } = {},
): Promise<boolean> {
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

  if (shouldTreatAsNotShortSale(conversation)) {
    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult: "not_short_sale",
      responseStatus: buildVoiceResponseStatus("not_short_sale"),
      leadStatusCode: "R",
      voiceNotes: summary,
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("not_short_sale"),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback recorded listing is not a short sale", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
    });
    return true;
  }

  if (shouldTreatAsAgentHungUp(conversation)) {
    await postSheetUpdate({
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      callResult: "call_received_agent_hung_up",
      responseStatus: buildVoiceResponseStatus("call_received_agent_hung_up"),
      leadStatusCode: "N",
      voiceNotes: summary,
    });

    await sendTranscriptEmailIfEnabled({
      conversationId,
      metadata,
      outcome: buildVoiceResponseStatus("call_received_agent_hung_up"),
      summary,
      transcript: fullTranscript,
    });

    processedConversationIds.add(conversationId);
    logger.info("ElevenLabs post-call fallback recorded answered call that ended with agent hangup", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      summary,
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
      email: metadata.email,
      listingAddress: metadata.listingAddress,
      rowNumber: metadata.rowNumber,
      action: "Call this lead back ASAP",
      callbackTime,
      conversationDescription: summary,
      conversationTranscript: fullTranscript,
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

  if (conversation.status === "failed" && (shouldTreatAsVoicemail(conversation) || shouldTreatAsNoAnswer(conversation))) {
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
    logger.info("ElevenLabs post-call fallback converted failed voicemail/no-answer into normal outcome handling", {
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
      voicemailDetected,
      voicemailLeft,
      status: conversation.status,
    });
    return true;
  }

  if (conversation.status !== "done") {
    if (conversation.status === "failed") {
      const failureReason = getFailedConversationReason(conversation);
      const invalidDestinationNumber = isInvalidDestinationNumberFailure(conversation);
      const outcome = invalidDestinationNumber ? "call_failed_invalid_number" : "Call failed before completion";
      const outcomeSummary = `${failureReason}${summary ? ` ${summary}` : ""}`.trim();

      if (invalidDestinationNumber) {
        await postSheetUpdate({
          rowNumber: metadata.rowNumber,
          callAttemptNumber: metadata.callAttemptNumber,
          callResult: outcome,
          responseStatus: buildVoiceResponseStatus(outcome),
          leadStatusCode: "N",
          voiceNotes: failureReason,
        });
      }

      await sendTranscriptEmailIfEnabled({
        conversationId,
        metadata,
        outcome: buildVoiceResponseStatus(outcome),
        summary: outcomeSummary,
        transcript: fullTranscript,
      });

      processedConversationIds.add(conversationId);
      logger.info(
        invalidDestinationNumber
          ? "ElevenLabs post-call fallback recorded invalid destination number"
          : "ElevenLabs post-call fallback skipped because conversation failed before a final outcome",
        {
          conversationId,
          rowNumber: metadata.rowNumber,
          callAttemptNumber: metadata.callAttemptNumber,
          status: conversation.status,
          failureReason,
        },
      );
      return true;
    }

    if (options.finalAttempt && shouldRetryUnconnectedConversation(conversation, metadata)) {
      await retryUnconnectedElevenLabsCall({
        originalConversationId: conversationId,
        metadata,
      });

      processedConversationIds.add(conversationId);
      return true;
    }

    if (options.finalAttempt && shouldTreatAsUnconnectedInitiatedConversation(conversation)) {
      const isFirstAttempt = metadata.callAttemptNumber <= 1;
      const callResult = isFirstAttempt ? "no_answer_first_attempt" : "no_response_second_attempt";
      const noConnectSummary =
        "ElevenLabs accepted the outbound call request, but the call never connected and produced no audio or transcript.";

      await postSheetUpdate({
        rowNumber: metadata.rowNumber,
        callAttemptNumber: metadata.callAttemptNumber,
        callResult,
        responseStatus: buildVoiceResponseStatus(callResult),
        ...(isFirstAttempt ? {} : { leadStatusCode: "N" }),
        voiceNotes: noConnectSummary,
      });

      await sendTranscriptEmailIfEnabled({
        conversationId,
        metadata,
        outcome: buildVoiceResponseStatus(callResult),
        summary: noConnectSummary,
        transcript: fullTranscript,
      });

      processedConversationIds.add(conversationId);
      logger.info("ElevenLabs post-call fallback recorded unconnected initiated conversation", {
        conversationId,
        rowNumber: metadata.rowNumber,
        callAttemptNumber: metadata.callAttemptNumber,
        callResult,
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
      email: metadata.email,
      listingAddress: metadata.listingAddress,
      rowNumber: metadata.rowNumber,
      action: "Call this lead back at the requested time",
      callbackTime,
      conversationDescription: summary,
      conversationTranscript: fullTranscript,
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

export async function handleElevenLabsPostCallWebhook(conversationId: string): Promise<boolean> {
  const metadata = getElevenLabsCallContextByConversationId(conversationId);

  if (!metadata) {
    logger.warn("Skipping ElevenLabs post-call webhook because call context was not found", {
      conversationId,
    });
    return false;
  }

  const done = await processPostCallOutcome(conversationId, metadata);

  if (done) {
    await requestVoiceQueueRefill({
      conversationId,
      rowNumber: metadata.rowNumber,
      callAttemptNumber: metadata.callAttemptNumber,
    });
  }

  return done;
}

export function scheduleElevenLabsPostCallFallback(params: {
  conversationId: string;
  metadata: CallMetadata;
}): void {
  const run = (attempt: number) => {
    void processPostCallOutcome(params.conversationId, params.metadata, { finalAttempt: attempt >= MAX_ATTEMPTS })
      .then((done) => {
        if (done) {
          void requestVoiceQueueRefill({
            conversationId: params.conversationId,
            rowNumber: params.metadata.rowNumber,
            callAttemptNumber: params.metadata.callAttemptNumber,
          });
          return;
        }

        if (attempt < MAX_ATTEMPTS) {
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
