const PAUSED_UNTIL_ISO = "2026-05-26T04:00:00.000Z";
const PAUSE_REASON = "Memorial Day pause";

export type OutboundCallPause = {
  pausedUntil: Date;
  reason: string;
};

export function getOutboundCallPause(now = new Date()): OutboundCallPause | undefined {
  const pausedUntil = new Date(PAUSED_UNTIL_ISO);

  if (now < pausedUntil) {
    return {
      pausedUntil,
      reason: PAUSE_REASON,
    };
  }

  return undefined;
}
