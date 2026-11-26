export const OWNER_CALL_BLACKOUT_DATES = Object.freeze(["2026-11-26"]);

export function isOwnerCallBlackoutDate(dateKey: string): boolean {
  return OWNER_CALL_BLACKOUT_DATES.includes(dateKey);
}

export function getOwnerCallBlackoutReason(now: Date, timeZone: string): string | null {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(now);
  const part = (type: string) => parts.find(value => value.type === type)?.value;
  const key = `${part("year")}-${part("month")}-${part("day")}`;
  return isOwnerCallBlackoutDate(key) ? "Owner-approved Thanksgiving call blackout: November 26, 2026" : null;
}
