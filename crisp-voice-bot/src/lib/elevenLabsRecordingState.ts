export type RecordingStateTranscriptItem = {
  role?: string;
  message?: string | null;
};

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/[\u2018\u2019]/g, "'").replace(/\s+/g, " ").trim();
}

function isAutomatedMessage(message: string): boolean {
  const text = normalizeText(message);

  return (
    /\b(?:automated (?:system|attendant)|recording|pre-?recorded|call screening|screening service|phone tree|ivr|virtual assistant)\b/.test(
      text,
    ) ||
    /\b(?:record|state)\s+(?:your\s+)?name(?:\s+and\s+(?:the\s+)?reason)?\b/.test(text) ||
    /\b(?:press|dial)\s+(?:one|two|three|[0-9])\b/.test(text) ||
    /\b(?:please\s+)?stay on the line\b/.test(text) ||
    /\bone moment while\b/.test(text) ||
    /\bi(?:'|’)ll see if (?:this person|they|he|she) is available\b/.test(text)
  );
}

function isCannedFragment(message: string): boolean {
  const text = normalizeText(message).replace(/[.!?,]+/g, " ").replace(/\s+/g, " ").trim();
  return (
    /^(?:(?:as soon as possible|thank you|thanks|goodbye|not available|please hold)\s*)+$/.test(text) ||
    text === "as soon as possible" ||
    text === "thank you" ||
    text === "thanks" ||
    text === "goodbye" ||
    text === "not available" ||
    text === "please hold"
  );
}

function isLikelyHumanMessage(message: string): boolean {
  const text = normalizeText(message);
  return text !== "" && text !== "..." && !isAutomatedMessage(text) && !isCannedFragment(text);
}

export function isRecordingOrScreeningArtifact(
  transcript: RecordingStateTranscriptItem[],
  summary = "",
): boolean {
  const messages = transcript
    .filter((item) => item.role === "user" && typeof item.message === "string")
    .map((item) => item.message!.trim())
    .filter(Boolean);
  const combined = normalizeText(`${summary} ${messages.join(" ")}`);
  const hasAutomationSignal =
    messages.some(isAutomatedMessage) ||
    /\b(?:automated (?:system|attendant)|recording|pre-?recorded|call screening|screening service|phone tree|ivr|virtual assistant)\b/.test(
      combined,
    );

  if (!hasAutomationSignal) {
    return false;
  }

  let lastAutomationIndex = -1;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    if (isAutomatedMessage(messages[index])) {
      lastAutomationIndex = index;
      break;
    }
  }
  const humanAfterAutomation = messages.slice(lastAutomationIndex + 1).some(isLikelyHumanMessage);
  return !humanAfterAutomation;
}
