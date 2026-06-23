import type { StartCallRequest } from "../types";

const REQUIRED_SCHEDULED_WINDOW = "late_afternoon_control";
const CALL_WINDOW_START_MINUTES = 16 * 60;
const CALL_WINDOW_END_MINUTES = 17 * 60;

function getLocalCallMinutes(date: Date, timeZone: string): number {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const hour = Number(parts.find((part) => part.type === "hour")?.value);
  const minute = Number(parts.find((part) => part.type === "minute")?.value);

  if (!Number.isFinite(hour) || !Number.isFinite(minute)) {
    throw new Error("agentTimeZone could not be used to calculate the local call window");
  }

  return hour * 60 + minute;
}

export function getStartCallWindowBlockReason(
  payload: Pick<StartCallRequest, "agentTimeZone" | "scheduledWindow">,
  now = new Date(),
): string | null {
  if (payload.scheduledWindow !== REQUIRED_SCHEDULED_WINDOW) {
    return `scheduledWindow must be ${REQUIRED_SCHEDULED_WINDOW}`;
  }

  if (!payload.agentTimeZone) {
    return "agentTimeZone is required for local call-window enforcement";
  }

  let localMinutes: number;
  try {
    localMinutes = getLocalCallMinutes(now, payload.agentTimeZone);
  } catch (error) {
    if (error instanceof RangeError) {
      return "agentTimeZone must be a valid IANA time zone";
    }

    throw error;
  }

  if (localMinutes < CALL_WINDOW_START_MINUTES || localMinutes >= CALL_WINDOW_END_MINUTES) {
    return "current listing-local time is outside the 4:00-5:00pm call window";
  }

  return null;
}
