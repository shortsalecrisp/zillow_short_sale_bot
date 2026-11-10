import {
  normalizePhoneToE164,
  normalizeString,
  VOICE_BOT_COL_PHONE,
  VOICE_BOT_COL_VOICE_NOTES,
} from "./voiceSheet";

export const VOICE_AUTOMATED_CALL_CANCELLATION_MARKER = "CODEX_VOICE_AUTOMATED_CALL_CANCELLATION_V1";

export type VoiceAutomatedCallCancellation = {
  rowNumber: number;
  phone: string;
  approvedAt: string;
  reason: string;
};

export function formatVoiceAutomatedCallCancellation(
  cancellation: VoiceAutomatedCallCancellation,
): string {
  return `${VOICE_AUTOMATED_CALL_CANCELLATION_MARKER} ${JSON.stringify(cancellation)}`;
}

export function parseVoiceAutomatedCallCancellation(
  value: unknown,
): VoiceAutomatedCallCancellation | undefined {
  const lines = normalizeString(value)
    .split(/\r?\n/)
    .filter((line) => line.includes(VOICE_AUTOMATED_CALL_CANCELLATION_MARKER));
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const jsonText = lines[index]
      .slice(lines[index].indexOf(VOICE_AUTOMATED_CALL_CANCELLATION_MARKER) + VOICE_AUTOMATED_CALL_CANCELLATION_MARKER.length)
      .trim();
    try {
      const parsed = JSON.parse(jsonText) as Partial<VoiceAutomatedCallCancellation>;
      const phone = normalizePhoneToE164(parsed.phone);
      if (
        Number.isInteger(parsed.rowNumber) &&
        phone && phone === parsed.phone &&
        typeof parsed.approvedAt === "string" && !Number.isNaN(Date.parse(parsed.approvedAt)) &&
        typeof parsed.reason === "string" && parsed.reason.trim()
      ) {
        return { ...parsed, phone } as VoiceAutomatedCallCancellation;
      }
    } catch (_) {}
  }
  return undefined;
}

/**
 * A cancellation follows the exact current phone episode. A later verified phone
 * can be reviewed independently instead of inheriting an old-number block.
 */
export function isVoiceAutomatedCallCancelled(rowNumber: number, rowValues: unknown[]): boolean {
  const cancellation = parseVoiceAutomatedCallCancellation(
    rowValues[VOICE_BOT_COL_VOICE_NOTES - 1],
  );
  return Boolean(
    cancellation &&
    cancellation.rowNumber === rowNumber &&
    cancellation.phone === normalizePhoneToE164(rowValues[VOICE_BOT_COL_PHONE - 1]),
  );
}
