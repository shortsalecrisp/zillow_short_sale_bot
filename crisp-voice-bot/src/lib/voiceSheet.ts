import { resolveListingTimeZone } from "./listingTimeZone";

export const VOICE_BOT_SHEET_NAME = "Sheet1";
export const VOICE_BOT_TIMEZONE = "America/New_York";

export const VOICE_BOT_COL_FIRST_NAME = 1; // A
export const VOICE_BOT_COL_LAST_NAME = 2; // B
export const VOICE_BOT_COL_PHONE = 3; // C
export const VOICE_BOT_COL_EMAIL = 4; // D
export const VOICE_BOT_COL_LISTING_ADDRESS = 5; // E
export const VOICE_BOT_COL_CITY = 6; // F
export const VOICE_BOT_COL_STATE = 7; // G
export const VOICE_BOT_COL_FOLLOWUP_TEXT_SENT = 9; // I
export const VOICE_BOT_COL_RESPONSE_STATUS = 10; // J
export const VOICE_BOT_COL_LEAD_STATUS_CODE = 11; // K
export const VOICE_BOT_COL_FOLLOWUP_SENT_AT_PROXY = 24; // X
export const VOICE_BOT_COL_CREATED_AT = 28; // AB
export const VOICE_BOT_COL_CALL_ELIGIBLE = 30; // AD
export const VOICE_BOT_COL_CALL_TIME_BUCKET = 31; // AE
export const VOICE_BOT_COL_CALL_SCHEDULED_FOR = 32; // AF
export const VOICE_BOT_COL_CALL_1_SENT = 33; // AG
export const VOICE_BOT_COL_CALL_1_RESULT = 34; // AH
export const VOICE_BOT_COL_VM_LEFT = 35; // AI
export const VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED = 36; // AJ
export const VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED = 37; // AK
export const VOICE_BOT_COL_CALLBACK_REQUESTED = 38; // AL
export const VOICE_BOT_COL_CALLBACK_TIME = 39; // AM
export const VOICE_BOT_COL_CALL_2_SENT = 40; // AN
export const VOICE_BOT_COL_CALL_2_RESULT = 41; // AO
export const VOICE_BOT_COL_VOICE_NOTES = 42; // AP

export const VOICE_BOT_VOICE_NOTES_SEPARATOR = "\n\n---\n\n";
export const VOICE_BOT_MAX_VOICE_NOTES_CHARS = 49_000;
export const VOICE_BOT_ACTIVE_CALL_STALE_AFTER_MINUTES = 60;
export const VOICE_BOT_MAX_CALLS_PER_QUEUE_RUN = 10;
export const VOICE_BOT_MAX_ACTIVE_CALLS = 2;
export const VOICE_BOT_PROVIDER_QUOTA_RETRY_DELAY_MINUTES = 240;
export const VOICE_BOT_BUSINESS_DAY_START_HOUR_ET = 8;
export const VOICE_BOT_QUEUE_RUN_END_HOUR_ET = 24;

export type VoiceSheetRow = {
  rowNumber: number;
  values: unknown[];
};

export type VoiceCallWindow = {
  name: string;
  startMinutes: number;
  endMinutes: number;
};

const WEEKDAY_CALL_WINDOWS: VoiceCallWindow[] = [
  { name: "reach_morning_v1", startMinutes: 9 * 60 + 15, endMinutes: 9 * 60 + 45 },
  { name: "reach_afternoon_v1", startMinutes: 15 * 60, endMinutes: 15 * 60 + 45 },
];

const WEEKEND_CALL_WINDOWS: VoiceCallWindow[] = [];

const VOICE_BOT_MORNING_WINDOW_NAME = "reach_morning_v1";
const VOICE_BOT_MID_AFTERNOON_WINDOW_NAME = "reach_afternoon_v1";


export function columnToLetter(columnNumber: number): string {
  let letter = "";
  let temp = columnNumber;

  while (temp > 0) {
    const remainder = (temp - 1) % 26;
    letter = String.fromCharCode(65 + remainder) + letter;
    temp = Math.floor((temp - remainder - 1) / 26);
  }

  return letter;
}

export function escapeSheetName(sheetName: string): string {
  return sheetName.replace(/'/g, "''");
}

export function cellRange(sheetName: string, column: string, rowNumber: number): string {
  return `'${escapeSheetName(sheetName)}'!${column}${rowNumber}`;
}

export function normalizeString(value: unknown): string {
  if (value === undefined || value === null) {
    return "";
  }

  return String(value).trim();
}

export function normalizeMarker(value: unknown): string {
  return normalizeString(value).toLowerCase();
}

export function normalizePhoneToE164(value: unknown): string {
  const text = normalizeString(value);
  if (!text) {
    return "";
  }

  if (text.charAt(0) === "+" && /^\+[1-9]\d{6,14}$/.test(text)) {
    return text;
  }

  const digits = text.replace(/\D/g, "");
  if (digits.length === 10) {
    return `+1${digits}`;
  }

  if (digits.length === 11 && digits.charAt(0) === "1") {
    return `+${digits}`;
  }

  return "";
}

export function parseVoiceBotDate(value: unknown): Date | undefined {
  if (!value) {
    return undefined;
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }

  const text = normalizeString(value);
  if (!text) {
    return undefined;
  }

  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? undefined : parsed;
}

export function isRetryableVoiceBotResult(callResult: unknown): boolean {
  const normalized = normalizeString(callResult).toLowerCase();
  return [
    "voicemail_left",
    "voicemail_reached",
    "no_answer_first_attempt",
    "human_answered_no_response_first_attempt",
    "human_answered_no_bot_response",
    "agent_not_available",
    "call_start_failed",
    "call_start_receipt_missing",
  ].includes(normalized);
}

export function buildVoiceBotListingAddress(rowValues: unknown[]): string {
  return [
    normalizeString(rowValues[VOICE_BOT_COL_LISTING_ADDRESS - 1]),
    normalizeString(rowValues[VOICE_BOT_COL_CITY - 1]),
    normalizeString(rowValues[VOICE_BOT_COL_STATE - 1]),
  ]
    .filter(Boolean)
    .join(", ");
}

export function getVoiceBotAgentTimeZone(rowValues: unknown[]): string {
  return getVoiceBotTimeZoneResolution(rowValues).timeZone;
}

export function getVoiceBotTimeZoneResolution(rowValues: unknown[]) {
  return resolveListingTimeZone({
    streetAddress: normalizeString(rowValues[VOICE_BOT_COL_LISTING_ADDRESS - 1]),
    city: normalizeString(rowValues[VOICE_BOT_COL_CITY - 1]),
    state: normalizeString(rowValues[VOICE_BOT_COL_STATE - 1]),
  });
}

type LocalParts = {
  year: string;
  month: string;
  day: string;
  hour: string;
  minute: string;
  weekday: string;
};

function getLocalParts(date: Date, timeZone: string): LocalParts {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    weekday: "short",
  });
  const parts: Partial<LocalParts> = {};

  for (const part of formatter.formatToParts(date)) {
    if (part.type !== "literal") {
      parts[part.type as keyof LocalParts] = part.value;
    }
  }

  return parts as LocalParts;
}

function weekdayNumber(date: Date, timeZone: string): number {
  const parts = getLocalParts(date, timeZone);
  return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].indexOf(parts.weekday) + 1;
}

export function getVoiceBotLocalMinutes(date: Date, timeZone: string): number {
  const parts = getLocalParts(date, timeZone);
  return Number(parts.hour) * 60 + Number(parts.minute);
}

export function getVoiceBotLocalDateKey(date: Date, timeZone: string): string {
  const parts = getLocalParts(date, timeZone);
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function getOffset(date: Date, timeZone: string): string {
  const parts = getLocalParts(date, timeZone);
  const utcForLocal = Date.UTC(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour),
    Number(parts.minute),
  );
  const offsetMinutes = Math.round((utcForLocal - date.getTime()) / 60000);
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absolute = Math.abs(offsetMinutes);
  const hours = String(Math.floor(absolute / 60)).padStart(2, "0");
  const minutes = String(absolute % 60).padStart(2, "0");

  return `${sign}${hours}${minutes}`;
}

export function formatVoiceBotDateEt(date: Date): string {
  const parts = getLocalParts(date, VOICE_BOT_TIMEZONE);
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:00`;
}

export function isWithinVoiceBotQueueRunWindow(date: Date): boolean {
  const day = weekdayNumber(date, VOICE_BOT_TIMEZONE);
  const hour = Math.floor(getVoiceBotLocalMinutes(date, VOICE_BOT_TIMEZONE) / 60);

  return day >= 1 && day <= 7 && hour >= VOICE_BOT_BUSINESS_DAY_START_HOUR_ET && hour < VOICE_BOT_QUEUE_RUN_END_HOUR_ET;
}

export function getVoiceBotCallWindowsForDay(day: number): VoiceCallWindow[] {
  if (day >= 1 && day <= 5) {
    return WEEKDAY_CALL_WINDOWS;
  }

  if (day === 6 || day === 7) {
    return WEEKEND_CALL_WINDOWS;
  }

  return [];
}

export function getVoiceBotPreferredCallWindowName(date: Date, timeZone: string): string {
  if (!timeZone) return "";
  const callWindows = getVoiceBotCallWindowsForDay(weekdayNumber(date, timeZone));
  const localMinutes = getVoiceBotLocalMinutes(date, timeZone);

  for (const callWindow of callWindows) {
    if (localMinutes >= callWindow.startMinutes && localMinutes < callWindow.endMinutes) {
      return callWindow.name;
    }
  }

  return "";
}

function getVoiceBotCallWindowForDateKey(dateKey: string, timeZone: string): VoiceCallWindow | undefined {
  const probeDate = buildVoiceBotDateInTimeZone(dateKey, 12 * 60, timeZone);
  const windows = getVoiceBotCallWindowsForDay(weekdayNumber(probeDate, timeZone));
  return windows[0];
}

function getVoiceBotCallWindowByNameForDateKey(
  dateKey: string,
  timeZone: string,
  windowName: string,
): VoiceCallWindow | undefined {
  const probeDate = buildVoiceBotDateInTimeZone(dateKey, 12 * 60, timeZone);
  const windows = getVoiceBotCallWindowsForDay(weekdayNumber(probeDate, timeZone));
  return windows.find((callWindow) => callWindow.name === windowName);
}

function getVoiceBotCallWindowIndexByName(callWindows: VoiceCallWindow[], windowName: string): number {
  return callWindows.findIndex((callWindow) => callWindow.name === windowName);
}

function shiftVoiceBotDateKey(dateKey: string, daysToAdd: number): string {
  const [year, month, day] = dateKey.split("-").map(Number);
  const cursor = new Date(Date.UTC(year, month - 1, day + daysToAdd));
  const yyyy = cursor.getUTCFullYear();
  const mm = String(cursor.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(cursor.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function getNextVoiceBotCallDateKey(dateKey: string, timeZone: string): string {
  let cursorDateKey = shiftVoiceBotDateKey(dateKey, 1);

  while (true) {
    if (getVoiceBotCallWindowForDateKey(cursorDateKey, timeZone)) {
      return cursorDateKey;
    }

    cursorDateKey = shiftVoiceBotDateKey(cursorDateKey, 1);
  }
}

export function getVoiceBotFirstAttemptWindowName(phone: unknown): string {
  const normalizedPhone = normalizePhoneToE164(phone);
  if (!normalizedPhone) return "";
  // Stable across row moves and duplicate listings; parity gives equal assignment probability.
  let hash = 2166136261;
  for (const char of normalizedPhone) hash = Math.imul(hash ^ char.charCodeAt(0), 16777619) >>> 0;
  return hash % 2 === 0 ? VOICE_BOT_MORNING_WINDOW_NAME : VOICE_BOT_MID_AFTERNOON_WINDOW_NAME;
}

function getOppositeVoiceBotCallWindowName(windowName: string): string {
  if (windowName === VOICE_BOT_MORNING_WINDOW_NAME) {
    return VOICE_BOT_MID_AFTERNOON_WINDOW_NAME;
  }

  if (windowName === VOICE_BOT_MID_AFTERNOON_WINDOW_NAME) {
    return VOICE_BOT_MORNING_WINDOW_NAME;
  }

  return "";
}

export function buildVoiceBotDateInTimeZone(dateKey: string, localMinutes: number, timeZone: string): Date {
  const hour = Math.floor(localMinutes / 60);
  const minute = localMinutes % 60;
  const probeDate = new Date(`${dateKey}T12:00:00Z`);
  const offset = getOffset(probeDate, timeZone);
  const offsetWithColon = `${offset.slice(0, 3)}:${offset.slice(3)}`;
  const localTime = `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00`;

  return new Date(`${dateKey}T${localTime}${offsetWithColon}`);
}

export function getNextVoiceBotFirstAttemptWindowStart(
  followupSentAt: Date,
  timeZone: string,
  phone: unknown,
  windowName?: string,
): Date {
  if (!timeZone) throw new Error("Listing time zone must be resolved before scheduling");
  const followupDateKey = getVoiceBotLocalDateKey(followupSentAt, timeZone);
  const followupMinutes = getVoiceBotLocalMinutes(followupSentAt, timeZone);
  const firstAttemptWindowName = windowName || getVoiceBotFirstAttemptWindowName(phone);
  if (!WEEKDAY_CALL_WINDOWS.some((window) => window.name === firstAttemptWindowName)) {
    throw new Error("Valid phone-scoped call window is required before scheduling");
  }
  let cursorDateKey = followupDateKey;

  while (true) {
    const callWindow = getVoiceBotCallWindowByNameForDateKey(cursorDateKey, timeZone, firstAttemptWindowName);

    if (callWindow) {
      if (cursorDateKey !== followupDateKey) {
        return buildVoiceBotDateInTimeZone(cursorDateKey, callWindow.startMinutes, timeZone);
      }

      if (followupMinutes < callWindow.startMinutes) {
        return buildVoiceBotDateInTimeZone(cursorDateKey, callWindow.startMinutes, timeZone);
      }

      if (followupMinutes < callWindow.endMinutes) {
        return new Date(followupSentAt.getTime());
      }
    }

    cursorDateKey = shiftVoiceBotDateKey(cursorDateKey, 1);
  }
}

export function getNextVoiceBotFollowupAttemptWindowStart(firstAttemptSentAt: Date, timeZone: string, businessDays = 0): Date {
  if (!timeZone) throw new Error("Listing time zone must be resolved before scheduling");
  let nextCallDateKey = getVoiceBotLocalDateKey(firstAttemptSentAt, timeZone);
  for (let day = 0; day < businessDays; day++) nextCallDateKey = getNextVoiceBotCallDateKey(nextCallDateKey, timeZone);
  // Legacy first calls may have occurred outside the new narrow experiment slots.
  const firstAttemptWindowName = getVoiceBotLocalMinutes(firstAttemptSentAt, timeZone) < 12 * 60
    ? VOICE_BOT_MORNING_WINDOW_NAME : VOICE_BOT_MID_AFTERNOON_WINDOW_NAME;
  const oppositeWindowName = getOppositeVoiceBotCallWindowName(firstAttemptWindowName);
  // Normal retries alternate slots immediately; provider-start recovery can
  // still request a separate business-day delay explicitly.
  if (businessDays === 0 && (
    firstAttemptWindowName === VOICE_BOT_MID_AFTERNOON_WINDOW_NAME ||
    !getVoiceBotCallWindowByNameForDateKey(nextCallDateKey, timeZone, oppositeWindowName)
  )) {
    nextCallDateKey = getNextVoiceBotCallDateKey(nextCallDateKey, timeZone);
  }
  const nextCallProbeDate = buildVoiceBotDateInTimeZone(nextCallDateKey, 12 * 60, timeZone);
  const nextCallWindows = getVoiceBotCallWindowsForDay(weekdayNumber(nextCallProbeDate, timeZone));
  const oppositeWindow = oppositeWindowName
    ? getVoiceBotCallWindowByNameForDateKey(nextCallDateKey, timeZone, oppositeWindowName)
    : undefined;
  const firstAttemptWindowIndexInNextDay = getVoiceBotCallWindowIndexByName(nextCallWindows, firstAttemptWindowName);
  const nextCallWindowIndex =
    firstAttemptWindowIndexInNextDay >= 0 ? (firstAttemptWindowIndexInNextDay + 1) % nextCallWindows.length : 0;
  const nextCallWindow =
    oppositeWindow ?? nextCallWindows[nextCallWindowIndex] ?? getVoiceBotCallWindowForDateKey(nextCallDateKey, timeZone);

  if (!nextCallWindow) {
    throw new Error(`No voice call window found for ${nextCallDateKey} in ${timeZone}`);
  }

  return buildVoiceBotDateInTimeZone(nextCallDateKey, nextCallWindow.startMinutes, timeZone);
}

export function appendVoiceNotesValue(previous: unknown, next: string): string {
  const previousText = previous === undefined || previous === null ? "" : String(previous);
  if (previousText.split(VOICE_BOT_VOICE_NOTES_SEPARATOR).includes(next)) {
    return previousText;
  }
  const combined = previousText ? `${previousText}${VOICE_BOT_VOICE_NOTES_SEPARATOR}${next}` : next;
  return trimVoiceBotVoiceNotes(combined);
}

export function trimVoiceBotVoiceNotes(value: string): string {
  if (value.length <= VOICE_BOT_MAX_VOICE_NOTES_CHARS) {
    return value;
  }

  const prefix = "[Older voice performance log data trimmed to fit Google Sheets cell limit]\n";
  return prefix + value.slice(value.length - (VOICE_BOT_MAX_VOICE_NOTES_CHARS - prefix.length));
}
