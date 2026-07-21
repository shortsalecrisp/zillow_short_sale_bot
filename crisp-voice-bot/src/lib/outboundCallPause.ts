const DEFAULT_PAUSED_UNTIL_ISO = "2026-07-22T13:00:00.000Z";
const DEFAULT_PAUSE_REASON = "Paused until ElevenLabs billing cycle refreshes on July 22";

export type OutboundCallPause = {
  pausedUntil: Date;
  reason: string;
};

export function getOutboundCallPause(now = new Date()): OutboundCallPause | undefined {
  const pauseSetting = (process.env.OUTBOUND_CALLS_PAUSED_UNTIL_ISO ?? DEFAULT_PAUSED_UNTIL_ISO).trim();
  if (["", "off", "disabled", "none"].includes(pauseSetting.toLowerCase())) {
    return undefined;
  }

  const pausedUntil = new Date(pauseSetting);
  if (Number.isNaN(pausedUntil.getTime())) {
    return undefined;
  }

  if (now < pausedUntil) {
    return {
      pausedUntil,
      reason: (process.env.OUTBOUND_CALLS_PAUSE_REASON ?? DEFAULT_PAUSE_REASON).trim() || DEFAULT_PAUSE_REASON,
    };
  }

  return undefined;
}
