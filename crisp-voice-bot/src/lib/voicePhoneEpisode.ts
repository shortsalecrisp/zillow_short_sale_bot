import { createHash } from "node:crypto";
import { normalizePhoneToE164, normalizeString, parseVoiceBotDate } from "./voiceSheet";

export const VOICE_PHONE_EPISODE_MARKER = "CODEX_VOICE_PHONE_EPISODE_V1";
export type VoicePhoneEpisode = {
  phone: string;
  formerPhone: string;
  formerConversationIds: string[];
  prior: string[];
  initialRequestId: string;
  followupRequestId: string;
  recordedAt: string;
  digest: string;
};

function episodeDigest(row: unknown[], episode: Omit<VoicePhoneEpisode, "digest">): string {
  return createHash("sha256").update(JSON.stringify({
    identity: [0, 1, 2, 3, 4, 5, 6, 27].map(i => normalizeString(row[i])),
    sms: [22, 23].map(i => parseVoiceBotDate(row[i])?.toISOString()), ...episode,
  })).digest("hex");
}

export function createVoicePhoneEpisode(row: unknown[], formerPhone: string, formerConversationIds: string[],
  initialRequestId: string, followupRequestId: string, now: Date): VoicePhoneEpisode {
  const episode = { phone: normalizePhoneToE164(row[2]), formerPhone, formerConversationIds,
    prior: [32, 33, 39, 40].map(i => [32, 39].includes(i) ? parseVoiceBotDate(row[i])?.toISOString() ?? "" : normalizeString(row[i])),
    initialRequestId, followupRequestId, recordedAt: now.toISOString() };
  return { ...episode, digest: episodeDigest(row, episode) };
}

export function getVoicePhoneEpisode(row: unknown[]): VoicePhoneEpisode | undefined {
  const lines = normalizeString(row[41]).split(/\r?\n/).filter(l => l.startsWith(VOICE_PHONE_EPISODE_MARKER + " "));
  if (lines.length !== 1) return undefined;
  try {
    const data = JSON.parse(lines[0].slice(VOICE_PHONE_EPISODE_MARKER.length + 1)) as VoicePhoneEpisode;
    const { digest, ...episode } = data;
    if (data.phone !== normalizePhoneToE164(row[2]) || data.phone === data.formerPhone ||
      normalizePhoneToE164(data.formerPhone) !== data.formerPhone || !parseVoiceBotDate(data.recordedAt) ||
      !Array.isArray(data.formerConversationIds) || !data.formerConversationIds.length ||
      !data.formerConversationIds.every(id => /^conv_[a-zA-Z0-9]+$/.test(id)) ||
      !Array.isArray(data.prior) || data.prior.length !== 4 || !data.prior.every(v => typeof v === "string") ||
      !data.initialRequestId || !data.followupRequestId || digest !== episodeDigest(row, episode)) return undefined;
    return data;
  } catch { return undefined; }
}

/** A policy view only. Original owning timestamps/results and receipts stay intact. */
export function currentPhoneVoiceRow(row: unknown[]): unknown[] {
  const episode = getVoicePhoneEpisode(row);
  if (!episode) return row;
  const current = [...row];
  for (const [index, prior] of [[32, 0], [39, 2]]) {
    if (parseVoiceBotDate(row[index])?.toISOString() === episode.prior[prior] &&
        normalizeString(row[index + 1]) === episode.prior[prior + 1]) {
      current[index] = ""; current[index + 1] = "";
    }
  }
  current[41] = normalizeString(row[41]).split(/\r?\n/).filter(line => {
    const marker = "CODEX_VOICE_SCHEDULED_NO_START_V1 ";
    if (!line.startsWith(marker)) return true;
    try {
      const detected = Date.parse(JSON.parse(line.slice(marker.length)).detectedAt);
      return !Number.isFinite(detected) || detected > Date.parse(episode.recordedAt);
    }
    catch { return true; }
  }).join("\n");
  return current;
}
