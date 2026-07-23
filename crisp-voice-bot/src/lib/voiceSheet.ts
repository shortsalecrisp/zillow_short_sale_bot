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
  { name: "morning_probe", startMinutes: 9 * 60, endMinutes: 10 * 60 },
  { name: "early_afternoon", startMinutes: 12 * 60 + 30, endMinutes: 13 * 60 + 30 },
  { name: "mid_afternoon", startMinutes: 14 * 60 + 30, endMinutes: 15 * 60 + 30 },
  { name: "late_afternoon_control", startMinutes: 16 * 60, endMinutes: 17 * 60 },
];

const WEEKEND_CALL_WINDOWS: VoiceCallWindow[] = [
  { name: "late_afternoon_control", startMinutes: 16 * 60, endMinutes: 17 * 60 },
];

const STATE_TIMEZONES: Record<string, string> = {
  AL: "America/Chicago",
  AK: "America/Anchorage",
  AR: "America/Chicago",
  AZ: "America/Phoenix",
  CA: "America/Los_Angeles",
  CO: "America/Denver",
  CT: "America/New_York",
  DC: "America/New_York",
  DE: "America/New_York",
  FL: "America/New_York",
  GA: "America/New_York",
  HI: "Pacific/Honolulu",
  IA: "America/Chicago",
  ID: "America/Denver",
  IL: "America/Chicago",
  IN: "America/New_York",
  KS: "America/Chicago",
  KY: "America/Chicago",
  LA: "America/Chicago",
  MA: "America/New_York",
  MD: "America/New_York",
  ME: "America/New_York",
  MI: "America/New_York",
  MN: "America/Chicago",
  MO: "America/Chicago",
  MS: "America/Chicago",
  MT: "America/Denver",
  NC: "America/New_York",
  ND: "America/Chicago",
  NE: "America/Chicago",
  NH: "America/New_York",
  NJ: "America/New_York",
  NM: "America/Denver",
  NV: "America/Los_Angeles",
  NY: "America/New_York",
  OH: "America/New_York",
  OK: "America/Chicago",
  OR: "America/Los_Angeles",
  PA: "America/New_York",
  RI: "America/New_York",
  SC: "America/New_York",
  SD: "America/Chicago",
  TN: "America/Chicago",
  TX: "America/Chicago",
  UT: "America/Denver",
  VA: "America/New_York",
  VT: "America/New_York",
  WA: "America/Los_Angeles",
  WI: "America/Chicago",
  WV: "America/New_York",
  WY: "America/Denver",
};

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
  return ["voicemail_left", "no_answer_first_attempt", "agent_not_available"].includes(normalized);
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
  const state = normalizeString(rowValues[VOICE_BOT_COL_STATE - 1]).toUpperCase();
  return STATE_TIMEZONES[state] || VOICE_BOT_TIMEZONE;
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

export function buildVoiceBotDateInTimeZone(dateKey: string, localMinutes: number, timeZone: string): Date {
  const hour = Math.floor(localMinutes / 60);
  const minute = localMinutes % 60;
  const probeDate = new Date(`${dateKey}T12:00:00Z`);
  const offset = getOffset(probeDate, timeZone);
  const offsetWithColon = `${offset.slice(0, 3)}:${offset.slice(3)}`;
  const localTime = `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00`;

  return new Date(`${dateKey}T${localTime}${offsetWithColon}`);
}

export function getNextVoiceBotFirstAttemptWindowStart(followupSentAt: Date, timeZone: string): Date {
  const followupDateKey = getVoiceBotLocalDateKey(followupSentAt, timeZone);
  const followupDay = weekdayNumber(followupSentAt, timeZone);
  const followupMinutes = getVoiceBotLocalMinutes(followupSentAt, timeZone);
  const followupCallWindows = getVoiceBotCallWindowsForDay(followupDay);

  for (const followupCallWindow of followupCallWindows) {
    if (followupMinutes < followupCallWindow.startMinutes) {
      return buildVoiceBotDateInTimeZone(followupDateKey, followupCallWindow.startMinutes, timeZone);
    }

    if (followupMinutes < followupCallWindow.endMinutes) {
      return new Date(followupSentAt.getTime());
    }
  }

  const nextCallDateKey = getNextVoiceBotCallDateKey(followupDateKey, timeZone);
  const nextCallWindow = getVoiceBotCallWindowForDateKey(nextCallDateKey, timeZone);

  if (!nextCallWindow) {
    throw new Error(`No voice call window found for ${nextCallDateKey} in ${timeZone}`);
  }

  return buildVoiceBotDateInTimeZone(nextCallDateKey, nextCallWindow.startMinutes, timeZone);
}

export function getNextVoiceBotFollowupAttemptWindowStart(firstAttemptSentAt: Date, timeZone: string): Date {
  const nextCallDateKey = getNextVoiceBotCallDateKey(getVoiceBotLocalDateKey(firstAttemptSentAt, timeZone), timeZone);
  const firstAttemptWindowName = getVoiceBotPreferredCallWindowName(firstAttemptSentAt, timeZone);
  const nextCallProbeDate = buildVoiceBotDateInTimeZone(nextCallDateKey, 12 * 60, timeZone);
  const nextCallWindows = getVoiceBotCallWindowsForDay(weekdayNumber(nextCallProbeDate, timeZone));
  const firstAttemptWindowIndexInNextDay = getVoiceBotCallWindowIndexByName(nextCallWindows, firstAttemptWindowName);
  const nextCallWindowIndex =
    firstAttemptWindowIndexInNextDay >= 0 ? (firstAttemptWindowIndexInNextDay + 1) % nextCallWindows.length : 0;
  const nextCallWindow = nextCallWindows[nextCallWindowIndex] ?? getVoiceBotCallWindowForDateKey(nextCallDateKey, timeZone);

  if (!nextCallWindow) {
    throw new Error(`No voice call window found for ${nextCallDateKey} in ${timeZone}`);
  }

  return buildVoiceBotDateInTimeZone(nextCallDateKey, nextCallWindow.startMinutes, timeZone);
}

export function appendVoiceNotesValue(previous: unknown, next: string): string {
  const previousText = previous === undefined || previous === null ? "" : String(previous);
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
