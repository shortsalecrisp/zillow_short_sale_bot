export function voiceListingAddressBlockReason(value: unknown): string | undefined {
  const address = typeof value === "string" ? value.trim() : "";
  if (!address) return "missing_listing_address";
  if (address.length > 200 || /[\r\n]/.test(address) ||
      /https?:\/\/|www\.|\b[^\s@]+@[^\s@]+\.[^\s@]+\b/i.test(address) ||
      /^(?:verified\b|contact[_ -]verification\b|sources?\s*:|CODEX_)/i.test(address) ||
      /\b(?:corroborates|listing identifies|no same[- ]phone|pending[- ]sms owner|agent profile|personal email)\b/i.test(address)) {
    return "listing_address_contains_contact_notes";
  }
  return undefined;
}

export function assertVoiceListingAddress(value: unknown): void {
  const reason = voiceListingAddressBlockReason(value);
  if (reason) throw new Error(`Voice listing address held for review: ${reason}`);
}
