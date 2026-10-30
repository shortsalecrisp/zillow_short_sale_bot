import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";
import { correctedReleaseStateHash } from "./voiceCorrectedPhoneRelease";
import { createVoicePhoneEpisode, currentPhoneVoiceRow, VOICE_PHONE_EPISODE_MARKER } from "./voicePhoneEpisode";
import { getVoiceBotCallCandidateFromRowValues, hasVoiceQueueDispatchHeadroom, isVoiceBotPhoneBlocked } from "./voiceQueue";
import { hasUnresolvedVoiceStart } from "./voiceTransportRecovery";
import { appendVoiceNotesValue, getNextVoiceBotFirstAttemptWindowStart, getVoiceBotAgentTimeZone,
  normalizeMarker, normalizePhoneToE164, normalizeString, parseVoiceBotDate } from "./voiceSheet";

export function reviewedIdentitySmsProof(rowNumber: number, row: unknown[], entries: unknown[][], now: Date) {
  const phone = normalizePhoneToE164(row[2]);
  const own = entries.filter(e => normalizePhoneToE164(e[4]) === phone);
  const initial = own.filter(e => normalizeString(e[6]).startsWith("__initial_outreach__:"));
  const followup = own.filter(e => normalizeString(e[6]).startsWith("__scheduled_followup__:"));
  if (own.length !== 2 || initial.length !== 1 || followup.length !== 1 || normalizeMarker(row[7]) !== "x" || normalizeMarker(row[8]) !== "x") {
    throw new Error("Reviewed identity requires exactly two current-number sent SMS receipts, not old flags or competing requests");
  }
  const data = [initial[0], followup[0]].map((e, i) => {
    const prefix = i ? "__scheduled_followup__:" : "__initial_outreach__:";
    const body = JSON.parse(normalizeString(e[6]).slice(prefix.length));
    const created = parseVoiceBotDate(e[0]), started = parseVoiceBotDate(e[13]), sent = parseVoiceBotDate(e[14]);
    const owning = parseVoiceBotDate(row[i ? 23 : 22]);
    if (e[1] !== "sent" || Number(e[17]) !== rowNumber || body.crm_row !== rowNumber || body.stable_id !== normalizeString(row[27]) ||
      !created || !started || !sent || !owning || created > started || started > sent || sent > now || owning < started || owning > sent ||
      entries.filter(x => x[2] === e[2] || x[3] === e[3]).length !== 1) throw new Error("Current-number SMS owning receipt is not proven");
    return { entry: e, body, sent };
  });
  const anchor = data[1].body.initial_receipt;
  if (data[0].sent >= data[1].sent || !anchor || anchor.request_id !== data[0].entry[2] || anchor.message_id !== data[0].entry[3] ||
      normalizePhoneToE164(anchor.phone) !== phone || anchor.stable_id !== normalizeString(row[27]) ||
      parseVoiceBotDate(anchor.sent_at)?.getTime() !== data[0].sent.getTime()) throw new Error("Follow-up is not bound to the current initial receipt");
  return { phone, initialRequestId: String(data[0].entry[2]), followupRequestId: String(data[1].entry[2]) };
}

export function assertReviewedIdentityState(row: unknown[]) {
  const marked = (v: unknown) => !["", "no", "false", "0"].includes(normalizeMarker(v));
  if ([10, 12, 13, 15, 16, 17, 19, 20, 21, 28, 35, 36, 37, 38, 39, 40, 43, 44].some(i => marked(row[i])) ||
      !["", "No accepted provider receipt; replacement attempt safely scheduled"].includes(normalizeString(row[9])) ||
      normalizeString(row[41]).includes("CODEX_VOICE_EXPIRED_TO_MAILSHAKE_V1") || hasUnresolvedVoiceStart(normalizeString(row[41]))) {
    throw new Error("Reviewed identity held: consent, history, campaign or pending-start state requires review");
  }
}

async function providerGet(path: string): Promise<any> {
  if (!config.elevenLabs.apiKey) throw new Error("Provider history access unavailable");
  const r = await fetch(`${config.elevenLabs.baseUrl}/v1/convai/${path}`, { headers: { "xi-api-key": config.elevenLabs.apiKey }, signal: AbortSignal.timeout(30_000) });
  if (!r.ok) throw new Error(`Provider history GET failed (${r.status})`);
  return r.json();
}

export async function reconcileReviewedVoiceIdentity(payload: { row: number; formerPhone?: string; apply?: boolean; expectedStateHash?: string }) {
  const sheets = await getGoogleSheetsClient(), now = new Date();
  const metadata = (await sheets.spreadsheets.get({ spreadsheetId: config.googleSheets.sheetId,
    fields: "sheets.properties" })).data.sheets ?? [];
  const limit = (title: string, columns: number) => {
    const p = metadata.find(s => s.properties?.title === title)?.properties;
    if (!p?.gridProperties?.rowCount || (p.gridProperties.columnCount ?? 0) < columns) throw new Error("Fresh bounded sheet metadata unavailable");
    return p.gridProperties.rowCount;
  };
  const fleetEnd = limit(config.googleSheets.tabName, 45), smsEnd = limit("sms_pending_sends", 19), inboundEnd = limit("sms_inbound_queue", 8), repliesEnd = limit("Replies", 3);
  if (payload.row > fleetEnd) throw new Error("Reviewed row is outside the current sheet grid");
  const read = async () => (await sheets.spreadsheets.values.get({ spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName}'!A${payload.row}:AS${payload.row}`, valueRenderOption: "UNFORMATTED_VALUE" })).data.values?.[0] ?? [];
  const row = await read(); assertReviewedIdentityState(row);
  if (normalizeString(row[41]).includes(VOICE_PHONE_EPISODE_MARKER)) throw new Error("An existing phone episode requires reconciliation, not another release");
  const all = await sheets.spreadsheets.values.batchGet({ spreadsheetId: config.googleSheets.sheetId,
    ranges: [`'${config.googleSheets.tabName}'!A2:AS${fleetEnd}`, `sms_pending_sends!A2:S${smsEnd}`, `sms_inbound_queue!A2:H${inboundEnd}`, `Replies!A2:C${repliesEnd}`], valueRenderOption: "UNFORMATTED_VALUE" });
  const [fleet, entries, inbound, replies] = (all.data.valueRanges ?? []).map(r => r.values ?? []);
  if (!fleet || !entries || !inbound || !replies) throw new Error("Incomplete identity safety evidence");
  const sms = reviewedIdentitySmsProof(payload.row, row, entries, now);
  const former = payload.formerPhone ? normalizePhoneToE164(payload.formerPhone) : "";
  if (payload.formerPhone && (!former || former === sms.phone)) throw new Error("Distinct reviewed former phone is required");
  const phones = [sms.phone, ...(former ? [former] : [])];
  if (fleet.filter(r => phones.includes(normalizePhoneToE164(r[2])) || normalizeString(r[27]) === normalizeString(row[27])).length !== 1 ||
      inbound.some(r => phones.includes(normalizePhoneToE164(r[5]))) || replies.some(r => r.some(v => phones.includes(normalizePhoneToE164(v))))) {
    throw new Error("Reviewed identity held: duplicate identity or inbound/return preference");
  }
  const found = new Set<string>();
  for (const filter of [`rowNumber:eq:${payload.row}`, `phone:in:${phones.join("|")}`, `requestedPhone:in:${phones.join("|")}`]) {
    let cursor: string | undefined;
    for (let page = 0; ; page++) {
      if (page >= 10) throw new Error("Provider history exceeds review bound");
      const q = new URLSearchParams({ page_size: "100", dynamic_variable_params: filter }); if (cursor) q.set("cursor", cursor);
      const result = await providerGet("conversations?" + q);
      if (!Array.isArray(result.conversations)) throw new Error("Incomplete provider history");
      result.conversations.forEach((c: any) => found.add(c.conversation_id));
      if (!result.has_more) break;
      if (!result.next_cursor || result.next_cursor === cursor) throw new Error("Incomplete provider pagination"); cursor = result.next_cursor;
    }
  }
  const priorIds: string[] = [];
  for (const cid of found) {
    const c = await providerGet("conversations/" + encodeURIComponent(cid)), v = c.conversation_initiation_client_data?.dynamic_variables ?? {}, m = c.metadata;
    if (!former || c.agent_id !== config.elevenLabs.agentId || c.status !== "done" || !m?.accepted_time_unix_secs || !(m.call_duration_secs > 0) ||
      Number(v.rowNumber) !== payload.row || Number(v.callAttemptNumber) !== 1 || m.phone_call?.direction !== "outbound" ||
      normalizePhoneToE164(m.phone_call.external_number) !== former || normalizePhoneToE164(v.phone) !== former ||
      normalizePhoneToE164(v.requestedPhone) !== former || c.conversation_id !== cid ||
      [v.testMode, v.providerProof, v.providerProofCall, v.isTestCall].some(x => x === true || x === "true")) throw new Error("A current, ambiguous or unexpected call blocks first-call release");
    const sent = parseVoiceBotDate(row[32]);
    if (!sent || Math.abs(m.start_time_unix_secs * 1000 - sent.getTime()) > 600_000 || row[39]) throw new Error("Former call does not match preserved first-attempt history");
    const classifier = await import("./elevenLabsPostCall");
    if (!classifier.getVoicemailOrNoAnswerCallResult(c, 1)?.startsWith("voicemail_")) throw new Error("Former live preferences require a separate review");
    priorIds.push(cid);
  }
  if (former ? priorIds.length !== 1 : Boolean(row[32] || row[33] || found.size)) throw new Error("Reviewed prior-call ownership is not proven");
  const zone = getVoiceBotAgentTimeZone(row); if (!zone) throw new Error("Verified listing timezone is required");
  const released = [...row];
  const episode = former ? createVoicePhoneEpisode(row, former, priorIds, sms.initialRequestId, sms.followupRequestId, now) : undefined;
  if (episode) released[41] = appendVoiceNotesValue(row[41], `${VOICE_PHONE_EPISODE_MARKER} ${JSON.stringify(episode)}`);
  const slot = getNextVoiceBotFirstAttemptWindowStart(new Date(now.getTime() + 120_000), zone, sms.phone);
  released[29] = "yes"; released[30] = "voice_call_1_due"; released[31] = slot.toISOString();
  const candidate = getVoiceBotCallCandidateFromRowValues(payload.row, released, slot);
  const projectedFleet = fleet.map((values, i) => ({ rowNumber: i + 2, values: i + 2 === payload.row ? released : values }));
  if (!candidate || candidate.callAttemptNumber !== 1 || !hasVoiceQueueDispatchHeadroom(candidate, slot) ||
      isVoiceBotPhoneBlocked(candidate, projectedFleet, slot) || currentPhoneVoiceRow(released)[32]) throw new Error("Unchanged first-attempt/local-window/duplicate guards reject release");
  released[41] = appendVoiceNotesValue(released[41], `CODEX_VOICE_IDENTITY_SCHEDULE_RECONCILED_V1 ${JSON.stringify({ phone: sms.phone, rowNumber: payload.row,
    formerConversationIds: priorIds, initialRequestId: sms.initialRequestId, followupRequestId: sms.followupRequestId, zone, attempt: 1, nextSlot: slot.toISOString(), recordedAt: now.toISOString() })}`);
  const stateHash = correctedReleaseStateHash(row);
  const plan = { ok: true, row: payload.row, phone: sms.phone, attempt: 1, zone, scheduledFor: slot.toISOString(), stateHash, priorIds, called: false, applied: false };
  if (payload.apply !== true) return plan;
  if (payload.expectedStateHash !== stateHash || correctedReleaseStateHash(await read()) !== stateHash) throw new Error("Reviewed identity owning state changed");
  const freshInbound = (await sheets.spreadsheets.values.get({ spreadsheetId: config.googleSheets.sheetId, range: `sms_inbound_queue!A2:H${inboundEnd}`, valueRenderOption: "UNFORMATTED_VALUE" })).data.values ?? [];
  if (freshInbound.some(r => phones.includes(normalizePhoneToE164(r[5])))) throw new Error("Reviewed identity inbound state changed");
  await sheets.spreadsheets.values.batchUpdate({ spreadsheetId: config.googleSheets.sheetId, requestBody: { valueInputOption: "RAW",
    data: [29, 30, 31, 41].map((i, n) => ({ range: `'${config.googleSheets.tabName}'!${["AD", "AE", "AF", "AP"][n]}${payload.row}`, values: [[released[i]]] })) } });
  if (correctedReleaseStateHash(await read()) !== correctedReleaseStateHash(released)) throw new Error("Reviewed identity readback failed; reconcile before retry");
  return { ...plan, applied: true, durableReadback: true };
}
