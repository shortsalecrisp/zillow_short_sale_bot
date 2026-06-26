import type { StartCallRequest } from "../types";

export const CALL_WINDOWS: Record<string, { startMinutes: number; endMinutes: number }> = {
  morning_probe: {
    startMinutes: 9 * 60,
    endMinutes: 10 * 60,
  },
  early_afternoon: {
    startMinutes: 12 * 60 + 30,
    endMinutes: 13 * 60 + 30,
  },
  mid_afternoon: {
    startMinutes: 14 * 60 + 30,
    endMinutes: 15 * 60 + 30,
  },
  late_afternoon_control: {
    startMinutes: 16 * 60,
    endMinutes: 17 * 60,
  },
};

export function getConfiguredCallWindows() {
  return Object.entries(CALL_WINDOWS).map(([name, window]) => ({
    name,
    startMinutes: window.startMinutes,
    endMinutes: window.endMinutes,
  }));
}

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
  const callWindow = payload.scheduledWindow ? CALL_WINDOWS[payload.scheduledWindow] : undefined;
  if (!callWindow) {
    return `scheduledWindow must be one of ${Object.keys(CALL_WINDOWS).join(", ")}`;
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

  if (localMinutes < callWindow.startMinutes || localMinutes >= callWindow.endMinutes) {
    return `current listing-local time is outside the ${payload.scheduledWindow} call window`;
  }

  return null;
}
