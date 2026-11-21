const reviewedFirstNameVariants = [
  ["montserrat", "monty"],
  ["emanuel", "emmanuel"],
  ["kerry", "carrie"],
];

export function matchesReviewedFirstName(actual: string | undefined, expected: string | undefined): boolean {
  if (!actual || !expected) return false;
  const first = actual.toLowerCase();
  const second = expected.toLowerCase();
  return first === second ||
    reviewedFirstNameVariants.some((group) => group.includes(first) && group.includes(second));
}

export function isOrganizationServiceGreeting(message: string): boolean {
  const text = message.toLowerCase().replace(/[‘’]/g, "'").replace(/\s+/g, " ").trim();
  const organization = text.match(/^(.{3,80}?)[.!?,]\s*(?:how )?(?:can|may) i help (?:you|ya)\b/)?.[1];
  return Boolean(organization &&
    /\b(?:real estate|realty|properties|brokerage|office|group|team|auctioneers?|keller williams|coldwell banker|century 21|compass|e\s*x\s*p|re\/?max|sotheby'?s|berkshire hathaway)\b/.test(organization) &&
    !/\b(?:this is|it's|it is|i am|i'm)\s+[\p{L}'-]+/u.test(organization) &&
    !/\b(?:voicemail|leave (?:a |your )?message|after the (?:tone|beep)|recorded|automated)\b/.test(text));
}
