function normalizeText(value: string): string {
  return value.toLowerCase().replace(/[\u2018\u2019]/g, "'").replace(/\s+/g, " ").trim();
}

export function looksLikeDoNotCall(value: string): boolean {
  const text = normalizeText(value);

  return (
    /\b(?:do not|don't|dont)\s+call\b/.test(text) ||
    /\bstop\s+calling\b/.test(text) ||
    /\bnever\s+call(?:\s+me)?\s+again\b/.test(text) ||
    /\bno\s+more\s+calls?\b/.test(text) ||
    /\b(?:take|remove)\s+me\s+off\s+(?:your\s+|the\s+)?(?:call(?:ing)?\s+)?list\b/.test(text)
  );
}
