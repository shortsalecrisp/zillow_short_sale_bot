function normalizeText(value: string): string {
  return value.toLowerCase().replace(/[\u2018\u2019]/g, "'").replace(/\s+/g, " ").trim();
}

export function looksLikeDoNotCall(value: string): boolean {
  const text = normalizeText(value);
  const hasCallerOptOut = [...text.matchAll(/\b(?:do not|don't|dont)\s+call\b/g)].some((match) => {
    const target = text.slice(match.index! + match[0].length);
    // A named specialist refusal and "call that a commitment" do not opt the
    // caller out. Check each occurrence so a separate caller opt-out still wins.
    const namedCallee = target.match(/^\s+(?:yoni(?:\s+kutler)?|kutler)\b/);
    if (namedCallee) {
      return /^(?:\s+(?:now|right now|today|yet))?\s+(?:or|and)\s+(?:me|us)\b/.test(target.slice(namedCallee[0].length));
    }
    return !/^\s+(?:that|this|it)\s+a\s+commitment(?=\s*(?:[.!?;,]|$))/.test(target);
  });

  return (
    text.split(/[.!?;]+/).some((sentence) =>
      /^(?:please[, ]+)?stop(?:[, ]+please)?$/.test(sentence.trim().replace(/^(?:ok(?:ay)?|alright|all right)[, ]+/, "")),
    ) ||
    hasCallerOptOut ||
    /\bstop\s+calling\b/.test(text) ||
    /\bnever\s+call(?:\s+me)?\s+again\b/.test(text) ||
    /\bno\s+more\s+calls?\b/.test(text) ||
    /\b(?:take|remove)\s+me\s+(?:off|from)\s+(?:your\s+|the\s+)?(?:call(?:ing)?\s+)?list\b/.test(text)
  );
}

export function looksLikeCallEndingRequest(value: string): boolean {
  const text = normalizeText(value);

  // A scoped instruction to end this call is distinct from a protected bare
  // STOP. Do not match a question or a negated instruction about ending.
  return text.split(/[.!?;]+/).some((sentence) => {
    const request = sentence.trim().replace(/^(?:ok(?:ay)?|alright|all right)[, ]+/, "");
    return (
      /^(?:please[, ]+)?(?:stop|end)\s+(?:this|the|our)\s+(?:phone\s+)?call(?:\s+(?:now|please))?$/.test(request) ||
      /^(?:please[, ]+)?(?:hang\s+up|disconnect)(?:\s+(?:now|please))?$/.test(request) ||
      /^(?:let's|let us)\s+stop\s+here(?:\s+please)?$/.test(request) ||
      /^(?:i (?:need|want) (?:you )?to|can you|could you|would you)\s+(?:please\s+)?(?:end|stop)\s+(?:this|the)\s+call(?:\s+(?:now|please))?$/.test(request)
    );
  });
}
