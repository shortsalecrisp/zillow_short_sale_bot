export type TransferConsentTranscriptItem = {
  role?: string;
  message?: string | null;
  tool_calls?: Array<{ tool_name?: string; name?: string }>;
};

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function isAssistantRole(role?: string): boolean {
  const normalizedRole = role?.toLowerCase();
  return normalizedRole === "assistant" || normalizedRole === "agent";
}

function hasToolCall(item: TransferConsentTranscriptItem, toolName: string): boolean {
  return (item.tool_calls ?? []).some((toolCall) => toolCall.tool_name === toolName || toolCall.name === toolName);
}

function getLiveTransferToolIndex(transcript: TransferConsentTranscriptItem[]): number {
  return transcript.findIndex((item) => hasToolCall(item, "live_transfer_requested"));
}

function latestMessageBefore(
  transcript: TransferConsentTranscriptItem[],
  beforeIndex: number,
  predicate: (item: TransferConsentTranscriptItem) => boolean,
): string {
  for (let index = beforeIndex - 1; index >= 0; index -= 1) {
    const item = transcript[index];
    if (predicate(item) && typeof item.message === "string" && item.message.trim() !== "") {
      return item.message.trim();
    }
  }

  return "";
}

function assistantOfferedLiveTransfer(message: string): boolean {
  const text = normalizeText(message);
  return (
    /\b(?:yoni|him)\b/.test(text) &&
    /\b(?:available|hop on|get (?:him|yoni) on|on the phone|talk|explain|connect|try|see if)\b/.test(text)
  );
}

function isDirectLiveTransferRequest(message: string): boolean {
  const text = normalizeText(message);
  return (
    /\b(?:connect me|transfer me|patch (?:me|him)|put (?:him|yoni) on|get (?:him|yoni) on|try (?:him|yoni)|see if (?:he|yoni) (?:is )?(?:available|free)|talk to yoni now|right now is fine|go ahead and (?:connect|try))\b/.test(text)
  );
}

function isSimplePositiveReply(message: string): boolean {
  const text = normalizeText(message)
    .replace(/[.!?]/g, "")
    .replace(/,+/g, " ")
    .trim();

  return (
    /^(?:yes|yeah|yep|sure|ok|okay|sounds good|go ahead|please do|that's fine|that works|if (?:he|yoni)'?s available|if (?:he|yoni) is available|right now is fine)$/.test(text) ||
    /^(?:yes|yeah|yep|sure|ok|okay)\s+go ahead$/.test(text) ||
    /^(?:yes|yeah|yep|sure|ok|okay)\s+(?:please do|that's fine|that works)$/.test(text)
  );
}

function hasAmbiguousOrConflictingSignal(message: string): boolean {
  const text = normalizeText(message);

  return (
    text.includes("...") ||
    /\b(yes|yeah|ok|okay)\b[\s,.]+\1\b/.test(text) ||
    /\b(?:i,?\s+so|uh|um|huh|what|which|meeting|busy|driving|later|tomorrow|afternoon|not now|call (?:you|me|us)?\s*back|call later|can't talk|cannot talk)\b/.test(text)
  );
}

export function hasLiveTransferToolCall(transcript: TransferConsentTranscriptItem[]): boolean {
  return getLiveTransferToolIndex(transcript) !== -1;
}

export function hasCallbackOrLaterSignal(
  transcript: TransferConsentTranscriptItem[],
  summary = "",
): boolean {
  const userText = normalizeText(
    transcript
      .filter((item) => item.role === "user" && typeof item.message === "string")
      .map((item) => item.message)
      .join(" "),
  );
  const text = normalizeText(`${summary} ${userText}`);

  return (
    /\b(?:meeting|busy|driving|can't talk|cannot talk|not a good time|with a client)\b/.test(text) &&
    /\b(?:later|tomorrow|afternoon|call (?:you|me|us)?\s*back|call later|callback|follow up)\b/.test(text)
  ) || /\b(?:later|tomorrow|afternoon)\b.{0,80}\bcall (?:you|me|us)?\s*back\b/.test(text);
}

export function hasClearLiveTransferConsent(
  transcript: TransferConsentTranscriptItem[],
  summary = "",
): boolean {
  const transferIndex = getLiveTransferToolIndex(transcript);
  if (transferIndex === -1 || hasCallbackOrLaterSignal(transcript, summary)) {
    return false;
  }

  const latestUserMessage = latestMessageBefore(
    transcript,
    transferIndex,
    (item) => item.role === "user",
  );
  if (!latestUserMessage || hasAmbiguousOrConflictingSignal(latestUserMessage)) {
    return false;
  }

  if (isDirectLiveTransferRequest(latestUserMessage)) {
    return true;
  }

  const latestAssistantMessage = latestMessageBefore(transcript, transferIndex, (item) => isAssistantRole(item.role));
  return assistantOfferedLiveTransfer(latestAssistantMessage) && isSimplePositiveReply(latestUserMessage);
}

export function isMisfiredLiveTransferRequest(
  transcript: TransferConsentTranscriptItem[],
  summary = "",
): boolean {
  return hasLiveTransferToolCall(transcript) && !hasClearLiveTransferConsent(transcript, summary);
}
