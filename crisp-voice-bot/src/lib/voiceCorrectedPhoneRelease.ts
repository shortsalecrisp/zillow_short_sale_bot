import { createHash } from "node:crypto";
import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";
import { correctedPhoneReleaseDigest, VOICE_CORRECTED_PHONE_RELEASE_MARKER } from "./voicePhoneIdentity";
import { hasUnresolvedVoiceStart, transportFailureKind, VOICE_START_RESOLUTION_MARKER } from "./voiceTransportRecovery";
import { getVoiceBotCallCandidateFromRowValues, hasVoiceQueueDispatchHeadroom, isVoiceBotPhoneBlocked } from "./voiceQueue";
import { getNextVoiceBotFirstAttemptWindowStart, getVoiceBotAgentTimeZone, normalizePhoneToE164,
  normalizeString, parseVoiceBotDate, appendVoiceNotesValue } from "./voiceSheet";

const SMS_MARKER = "CODEX_CORRECTED_PHONE_CONTACT_V1";
export const correctedReleaseStateHash = (row: unknown[]) => createHash("sha256")
  .update(JSON.stringify(Array.from({ length: 45 }, (_, i) => row[i] ?? ""))).digest("hex");

export function correctedPhoneContactProof(rowNumber: number, row: unknown[], entries: unknown[][], now: Date) {
  const phone = normalizePhoneToE164(row[2]);
  if (!phone || normalizeString(row[30]) !== "phone_identity_review_required" ||
      [9, 10, 12, 13, 15, 16, 17, 19, 20, 21, 28, 29, 31, 32, 33, 35, 36, 37, 38, 39, 40, 43, 44].some(i => normalizeString(row[i]))) {
    throw new Error("Corrected phone has protected state, attempts or a changed hold");
  }
  const lines = normalizeString(row[25]).split(/\r?\n/).filter(l => l.startsWith(SMS_MARKER + " "));
  if (lines.length !== 1) throw new Error("Reviewed corrected-number SMS episode is required");
  const episode = JSON.parse(lines[0].slice(SMS_MARKER.length + 1));
  if (episode.rowNumber !== rowNumber || "+1" + episode.phone !== phone || episode.textOnly !== true ||
      JSON.stringify(episode.identity) !== JSON.stringify([0, 1, 2, 3, 4, 5, 6, 27].map(i => normalizeString(row[i])))) {
    throw new Error("Reviewed SMS episode identity changed");
  }
  const current = entries.filter(e => normalizePhoneToE164(e[4]) === phone);
  const initial = current.filter(e => e[2] === episode.initialReceipt?.request_id);
  const followups = current.filter(e => normalizeString(e[6]).startsWith("__scheduled_followup__:"));
  if (current.length !== 2 || initial.length !== 1 || followups.length !== 1) throw new Error("Exact initial and follow-up receipts are required; competing requests must be reconciled");
  const receiptTimes = [initial[0], followups[0]].map((e, index) => {
    const prefix = index ? "__scheduled_followup__:" : "__initial_outreach__:";
    const data = JSON.parse(normalizeString(e[6]).slice(prefix.length));
    const created = parseVoiceBotDate(e[0]), started = parseVoiceBotDate(e[13]), sent = parseVoiceBotDate(e[14]);
    if (e[1] !== "sent" || Number(e[17]) !== rowNumber || data.crm_row !== rowNumber ||
        data.stable_id !== normalizeString(row[27]) || !created || !started || !sent ||
        created > started || started > sent || sent > now ||
        entries.filter(x => x[2] === e[2] || x[3] === e[3]).length !== 1) throw new Error("SMS receipt ownership/terminal interval is not proven");
    if (createHash("sha256").update(normalizeString(e[5])).digest("hex") !==
        (index ? episode.followupMessageSha256 : episode.initialMessageSha256)) throw new Error("Approved SMS copy changed");
    if (!index && (episode.initialReceipt.message_id !== e[3] || episode.initialReceipt.sent_at !== sent.toISOString() ||
        episode.initialReceipt.send_started_at !== started.toISOString() || episode.initialReceipt.created_at !== created.toISOString())) {
      throw new Error("Reviewed initial receipt changed");
    }
    if (index && (data.video_variant !== "text" || !data.initial_receipt ||
        data.initial_receipt.request_id !== episode.initialReceipt.request_id ||
        data.initial_receipt.message_id !== episode.initialReceipt.message_id ||
        data.initial_receipt.phone !== episode.phone)) throw new Error("Follow-up is not bound to the reviewed initial receipt");
    const owning = parseVoiceBotDate(row[index ? 23 : 22]);
    if (!owning || owning < started || owning > sent) throw new Error("SMS owning timestamp is not durably inside its sent interval");
    return sent;
  });
  if (receiptTimes[0] >= receiptTimes[1] || normalizeString(row[8]).toLowerCase() !== "x") throw new Error("Current-phone follow-up is not durably recorded");
  if (hasUnresolvedVoiceStart(normalizeString(row[41]))) throw new Error("An unresolved call start remains");
  const resolutions = normalizeString(row[41]).split(/\r?\n/).filter(l => l.startsWith(VOICE_START_RESOLUTION_MARKER + " "))
    .map(l => JSON.parse(l.slice(VOICE_START_RESOLUTION_MARKER.length + 1)));
  if (!resolutions.length || resolutions.some(r => r.rowNumber !== rowNumber || r.status !== "definitive_failure" ||
      r.phone !== "+1" + episode.formerPhone || r.callAttemptNumber !== 1 || !r.conversationId || !r.callStartRequestId)) {
    throw new Error("All former-number starts require exact terminal-failure resolutions");
  }
  return { phone, episode, resolutions };
}

async function providerGet(path: string): Promise<any> {
  if (!config.elevenLabs.apiKey) throw new Error("Provider receipt access is unavailable");
  const r = await fetch(`${config.elevenLabs.baseUrl}/v1/convai/${path}`, {
    headers: { "xi-api-key": config.elevenLabs.apiKey }, signal: AbortSignal.timeout(30_000),
  });
  if (!r.ok) throw new Error(`Provider receipt GET failed (${r.status})`);
  return r.json();
}

export async function reconcileCorrectedVoicePhone(payload: { row: number; apply?: boolean; expectedStateHash?: string }) {
  const sheets = await getGoogleSheetsClient();
  const read = async () => (await sheets.spreadsheets.values.get({ spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName}'!A${payload.row}:AS${payload.row}`, valueRenderOption: "UNFORMATTED_VALUE" })).data.values?.[0] ?? [];
  const row = await read(), now = new Date();
  const all = await sheets.spreadsheets.values.batchGet({ spreadsheetId: config.googleSheets.sheetId,
    ranges: [`'${config.googleSheets.tabName}'!A2:AS`, "sms_pending_sends!A2:S", "sms_inbound_queue!A2:H", "Replies!A2:C"], valueRenderOption: "UNFORMATTED_VALUE" });
  const [fleet, entries, inbound, replies] = (all.data.valueRanges ?? []).map(r => r.values ?? []);
  if (!fleet || !entries || !inbound || !replies) throw new Error("Incomplete contact controls");
  const proof = correctedPhoneContactProof(payload.row, row, entries, now);
  if (fleet.filter(r => normalizePhoneToE164(r[2]) === proof.phone || normalizeString(r[27]) === normalizeString(row[27])).length !== 1 ||
      inbound.some(r => normalizePhoneToE164(r[5]) === proof.phone) ||
      replies.some(r => r.some(v => normalizePhoneToE164(v) === proof.phone))) throw new Error("Duplicate or inbound contact requires review");
  const expectedIds = new Set<string>(proof.resolutions.map(r => r.conversationId));
  const found = new Set<string>();
  for (const filter of [`rowNumber:eq:${payload.row}`, `phone:in:${proof.phone}|+1${proof.episode.formerPhone}`,
    `requestedPhone:in:${proof.phone}|+1${proof.episode.formerPhone}`]) {
    let cursor: string | undefined;
    for (let page = 0; ; page++) {
      if (page >= 10) throw new Error("Provider history exceeded bounded pagination");
      const q = new URLSearchParams({ page_size: "100", dynamic_variable_params: filter });
      if (cursor) q.set("cursor", cursor);
      const result = await providerGet("conversations?" + q);
      if (!Array.isArray(result.conversations)) throw new Error("Incomplete provider history");
      result.conversations.forEach((c: any) => found.add(c.conversation_id));
      if (!result.has_more) break;
      if (!result.next_cursor || result.next_cursor === cursor) throw new Error("Incomplete provider pagination");
      cursor = result.next_cursor;
    }
  }
  if (found.size !== expectedIds.size || [...found].some(id => !expectedIds.has(id))) throw new Error("Additional/uncertain provider history requires reconciliation");
  for (const resolution of proof.resolutions) {
    const c = await providerGet("conversations/" + encodeURIComponent(resolution.conversationId));
    const v = c.conversation_initiation_client_data?.dynamic_variables ?? {};
    if (!transportFailureKind(c) || c.agent_id !== config.elevenLabs.agentId || Number(v.rowNumber) !== payload.row ||
        Number(v.callAttemptNumber) !== 1 || normalizePhoneToE164(c.metadata?.phone_call?.external_number) !== resolution.phone ||
        c.metadata?.phone_call?.direction !== "outbound" ||
        normalizePhoneToE164(v.requestedPhone ?? v.phone) !== resolution.phone ||
        [v.testMode, v.providerProofCall, v.isTestCall].some(x => x === true || x === "true")) throw new Error("Former-number terminal provider proof failed");
  }
  const zone = getVoiceBotAgentTimeZone(row);
  if (!zone) throw new Error("Verified listing time zone is required");
  const released = [...row];
  const release = { phone: proof.phone, formerPhones: ["+1" + proof.episode.formerPhone],
    digest: correctedPhoneReleaseDigest(row), initialRequestId: proof.episode.initialReceipt.request_id,
    followupRequestId: entries.find(e => normalizePhoneToE164(e[4]) === proof.phone && normalizeString(e[6]).startsWith("__scheduled_followup__:"))![2],
    oldFailedConversations: [...expectedIds], recordedAt: now.toISOString() };
  released[41] = appendVoiceNotesValue(row[41], `${VOICE_CORRECTED_PHONE_RELEASE_MARKER} ${JSON.stringify(release)}`);
  let slot = getNextVoiceBotFirstAttemptWindowStart(new Date(now.getTime() + 120_000), zone, proof.phone);
  released[29] = "yes"; released[30] = "voice_call_1_due"; released[31] = slot.toISOString();
  let candidate = getVoiceBotCallCandidateFromRowValues(payload.row, released, slot);
  if (candidate && !hasVoiceQueueDispatchHeadroom(candidate, slot)) {
    slot = getNextVoiceBotFirstAttemptWindowStart(new Date(slot.getTime() + 60 * 60_000), zone, proof.phone);
    released[31] = slot.toISOString(); candidate = getVoiceBotCallCandidateFromRowValues(payload.row, released, slot);
  }
  if (!candidate || candidate.callAttemptNumber !== 1 || !hasVoiceQueueDispatchHeadroom(candidate, slot) ||
      isVoiceBotPhoneBlocked(candidate, fleet.map((values, i) => ({ rowNumber: i + 2, values })), now)) throw new Error("Unchanged production eligibility/duplicate guards reject release");
  const stateHash = correctedReleaseStateHash(row);
  const plan = { ok: true, stateHash, row: payload.row, phone: proof.phone, attempt: 1, zone, scheduledFor: slot.toISOString(),
    providerReceiptsChecked: [...expectedIds], called: false, applied: false };
  if (payload.apply !== true) return plan;
  if (payload.expectedStateHash !== stateHash || correctedReleaseStateHash(await read()) !== stateHash) throw new Error("Owning state changed; review again");
  await sheets.spreadsheets.values.batchUpdate({ spreadsheetId: config.googleSheets.sheetId, requestBody: {
    valueInputOption: "RAW", data: [29, 30, 31, 41].map((i, n) => ({
      range: `'${config.googleSheets.tabName}'!${["AD", "AE", "AF", "AP"][n]}${payload.row}`, values: [[released[i]]],
    })),
  } });
  if (correctedReleaseStateHash(await read()) !== correctedReleaseStateHash(released)) throw new Error("Release readback failed; reconcile before retry");
  return { ...plan, applied: true, durableReadback: true };
}
