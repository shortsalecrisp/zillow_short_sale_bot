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
    /\bif you know (?:your )?party'?s extension\b/.test(text) ||
    /\b(?:please\s+)?stay on the line\b/.test(text) ||
    /\bplease hold while (?:i|we) (?:try to )?connect you\b/.test(text) ||
    /\b(?:the )?call could not be connected\b/.test(text) ||
    /\bone moment while\b/.test(text) ||
    /\bi(?:'|’)ll see if (?:this person|they|he|she) is available\b/.test(text)
    || /\bcall assist by google\b/.test(text)
  );
}

function isGoogleCallAssistContinuation(message: string): boolean {
  const text = normalizeText(message);
  return /\bchecking with the person you called\b/.test(text) ||
    /\bcan you tell me more about the details\b/.test(text);
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

export function isRepeatedPhoneMenuFragment(message: string, earlierMessages: string[]): boolean {
  const words = (value: string) => normalizeText(value).replace(/[^\p{L}\p{N}']+/gu, " ").trim();
  const text = words(message);
  if (!text.startsWith("thank you for calling")) return false;
  const menus = earlierMessages.filter(value => /\b(?:press|dial)\s+(?:one|two|three|four|five|six|seven|eight|nine|zero|[0-9])\b/i.test(value));
  return menus.length > 0 && (menus.some(value => words(value).startsWith(text)) ||
    /^thank you for calling(?: good(?:bye)?)?$/.test(text));
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
  const hasGoogleCallAssist = /\bcall assist by google\b/.test(combined);
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
  const humanAfterAutomation = messages.slice(lastAutomationIndex + 1).some((message, offset) =>
    isLikelyHumanMessage(message) &&
    !isRepeatedPhoneMenuFragment(message, messages.slice(0, lastAutomationIndex + 1 + offset)) &&
    !(hasGoogleCallAssist && isGoogleCallAssistContinuation(message)),
  );
  return !humanAfterAutomation;
}
