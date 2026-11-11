import { createHash } from "node:crypto";
import addressparser from "nodemailer/lib/addressparser";
import { config } from "./config";
import { logger } from "./logger";
import { getGoogleSheetsClient } from "./googleSheets";
import { readGmailImportState, writeGmailImportState, listGmailImportStates } from "./gmailImportState";
import { createEmailTransporter, requireEmailConfig } from "./emailAlerts";
import { buildCallbackEmailMessage } from "./sendCallbackEmail";
import { fetchVoiceTerminalReceipt, verifyVoiceTerminalReceipt } from "./voiceTerminalReceipt";
import { getVoicePhoneAttemptIdentity } from "./voicePhoneIdentity";
import { getInboundQuietGate } from "./voiceSafety";
import { normalizePhoneToE164, normalizeString } from "./voiceSheet";
import { looksLikeDoNotCall } from "./elevenLabsDoNotCall";

const PREFIX = "voice_callback_owner_alert_v1:";
export type CallbackAlertIdentity = { conversationId: string; rowNumber: number; callAttemptNumber: number;
  agentName: string; phone: string; listingAddress: string; testMode?: boolean; providerProofCall?: boolean };
type Status = "awaiting_final" | "sending" | "accepted" | "uncertain" | "cancelled" | "held";
export type CallbackAlertState = CallbackAlertIdentity & { schema: "voice_callback_owner_alert_v1";
  status: Status; messageId: string; createdAt: string; updatedAt: string; reason?: string; requestedTiming?: string };
type Mail = { subject: string; text: string; html: string; messageId: string };
export type CallbackAlertDependencies = {
  read?: (key: string) => Promise<CallbackAlertState | undefined>;
  write?: (key: string, value: CallbackAlertState) => Promise<void>;
  list?: () => Promise<Array<{ key: string; value: CallbackAlertState }>>;
  receipt?: typeof fetchVoiceTerminalReceipt;
  row?: (rowNumber: number) => Promise<unknown[]>;
  inboundBlocked?: (phone: string) => Promise<boolean>;
  send?: (message: Mail) => Promise<{ accepted?: unknown[]; rejected?: unknown[] }>;
  recipient?: string;
  playback?: (id: string) => string;
  policy?: () => Promise<{ activatedAt: string } | undefined>;
  savePolicy?: (value: { activatedAt: string }) => Promise<void>;
};
const POLICY_KEY = "voice_callback_owner_alert_policy_v1";

function validate(state: CallbackAlertState, id: string): void {
  if (state.schema !== "voice_callback_owner_alert_v1" || state.conversationId !== id ||
      !["awaiting_final", "sending", "accepted", "uncertain", "cancelled", "held"].includes(state.status) ||
      !state.messageId || !Number.isFinite(Date.parse(state.createdAt))) throw new Error("Invalid callback alert state; manual review required");
}

async function readRow(rowNumber: number): Promise<unknown[]> {
  const sheets = await getGoogleSheetsClient();
  const result = await sheets.spreadsheets.values.get({ spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName.replace(/'/g, "''")}'!A${rowNumber}:AS${rowNumber}`, valueRenderOption: "UNFORMATTED_VALUE" });
  return result.data.values?.[0] ?? [];
}

function holdReason(row: unknown[]): string | undefined {
  if (getVoicePhoneAttemptIdentity(row).reviewReason) return "phone_identity_review";
  const preferences = [row[9], row[12], row[13], row[28], row[33], row[40]].map(normalizeString);
  if (normalizeString(row[10]) === "R" || preferences.some(looksLikeDoNotCall) ||
      /\b(?:opt.?out|do.?not.?call|cancel(?:led)? (?:the |that |my )?callback|no callback|email only)\b/i.test(preferences.join(" "))) return "superseding_suppression";
  if (!["yes", "true", "1"].includes(normalizeString(row[37]).toLowerCase()) &&
      [row[33], row[40]].some(value => ["call_end_requested", "call_ended_by_request", "information_requested", "deferred_contact"].includes(normalizeString(value)))) return "superseding_contact_preference";
  return undefined;
}

export function createVoiceCallbackAlertService(deps: CallbackAlertDependencies = {}) {
  const read = deps.read ?? ((key: string) => readGmailImportState<CallbackAlertState | undefined>(key, undefined, true));
  const write = deps.write ?? writeGmailImportState;
  const policy = deps.policy ?? (() => readGmailImportState<{ activatedAt: string } | undefined>(POLICY_KEY, undefined, true));
  const savePolicy = deps.savePolicy ?? ((value: { activatedAt: string }) => writeGmailImportState(POLICY_KEY, value));
  const locks = new Map<string, Promise<unknown>>();
  const lock = async <T>(key: string, operation: () => Promise<T>): Promise<T> => {
    const previous = locks.get(key) ?? Promise.resolve();
    const pending = previous.catch(() => undefined).then(operation);
    locks.set(key, pending);
    try { return await pending; } finally { if (locks.get(key) === pending) locks.delete(key); }
  };
  const save = async (key: string, state: CallbackAlertState, patch: Partial<CallbackAlertState>) => {
    const next = { ...state, ...patch, updatedAt: new Date().toISOString() };
    await write(key, next);
    const durable = await read(key);
    if (!durable || durable.status !== next.status || durable.messageId !== next.messageId) throw new Error("Callback alert durable readback failed");
    return next;
  };
  const inboundBlocked = deps.inboundBlocked ?? (async (phone: string) =>
    (await getInboundQuietGate(await getGoogleSheetsClient(), phone, new Date())).blocked);

  async function enqueue(input: CallbackAlertIdentity): Promise<string> {
    if (input.testMode || input.providerProofCall) return "test_excluded";
    if (!/^conv_[a-zA-Z0-9]+$/.test(input.conversationId) || !Number.isInteger(input.rowNumber) || input.rowNumber < 2 ||
        ![1, 2].includes(input.callAttemptNumber) || !normalizePhoneToE164(input.phone) || !input.agentName || !input.listingAddress) {
      throw new Error("Exact current call identity is required for a callback alert");
    }
    const key = PREFIX + input.conversationId;
    return lock(key, async () => {
      const existing = await read(key);
      if (existing) {
        validate(existing, input.conversationId);
        if (["rowNumber", "callAttemptNumber", "agentName", "listingAddress"].some(k => existing[k as keyof CallbackAlertIdentity] !== input[k as keyof CallbackAlertIdentity]) ||
            normalizePhoneToE164(existing.phone) !== normalizePhoneToE164(input.phone)) throw new Error("Callback alert identity changed; no replay");
        return existing.status;
      }
      const now = new Date().toISOString();
      const state: CallbackAlertState = { ...input, phone: normalizePhoneToE164(input.phone), schema: "voice_callback_owner_alert_v1",
        status: "awaiting_final", messageId: `<voice-callback-${createHash("sha256").update(input.conversationId).digest("hex")}@crisp-voice-bot.onrender.com>`, createdAt: now, updatedAt: now };
      await save(key, state, {});
      return "awaiting_final";
    });
  }

  // Single production instance is required: BotState is durable, not compare-and-swap.
  // The sending claim is saved before SMTP; uncertain acceptance is never auto-retried.
  async function deliver(key: string, state: CallbackAlertState, mail: Mail, current: () => Promise<boolean>): Promise<string> {
    const email = deps.send ? undefined : requireEmailConfig();
    const recipient = deps.recipient ?? email?.to;
    const expected = addressparser(recipient ?? "", { flatten: true }).map(value => value.address?.toLowerCase()).filter(Boolean);
    if (!expected.length) throw new Error("Callback alert recipient is not configured");
    if (!await current()) return (await save(key, state, { status: "held", reason: "owning_state_changed_before_send" })).status;
    state = await save(key, state, { status: "sending" });
    try {
      if (!await current()) return (await save(key, state, { status: "held", reason: "owning_state_changed_after_claim" })).status;
      const info = deps.send ? await deps.send(mail) : await createEmailTransporter(email!).sendMail({ ...mail, to: email!.to, from: email!.from });
      const accepted = (info.accepted ?? []).map(value => typeof value === "string" ? value.toLowerCase() :
        typeof value === "object" && value && "address" in value ? String(value.address).toLowerCase() : "");
      if (!expected.every(value => accepted.includes(value!)) || (info.rejected?.length ?? 0) > 0) throw new Error("SMTP recipient acceptance is unconfirmed");
      await save(key, state, { status: "accepted" });
      return "accepted";
    } catch {
      await write(key, { ...state, status: "uncertain", reason: "Reconcile the stable Message-ID before any resend", updatedAt: new Date().toISOString() }).catch(() => undefined);
      return "uncertain";
    }
  }

  async function settle(conversationId: string): Promise<string> {
    const key = PREFIX + conversationId;
    return lock(key, async () => {
      let state = await read(key);
      if (!state) return "not_queued";
      validate(state, conversationId);
      if (state.status !== "awaiting_final") return state.status;
      const activation = await policy();
      if (!activation) return "awaiting_activation";
      if (!Number.isFinite(Date.parse(activation.activatedAt))) throw new Error("Invalid callback alert activation policy");
      const receipt: any = await (deps.receipt ?? fetchVoiceTerminalReceipt)(conversationId);
      if (!["done", "failed"].includes(receipt?.status)) return "awaiting_final";
      if (!(Number(receipt.metadata?.start_time_unix_secs) * 1000 >= Date.parse(activation.activatedAt))) {
        return (await save(key, state, { status: "held", reason: "pre_activation_history_needs_duplicate_review" })).status;
      }
      const variables = receipt.conversation_initiation_client_data?.dynamic_variables ?? {};
      if ([variables.testMode, variables.providerProofCall, variables.providerProof, variables.isTestCall].some(value => value === true || value === "true")) {
        return (await save(key, state, { status: "cancelled", reason: "test_excluded" })).status;
      }
      const classifier = await import("./elevenLabsPostCall");
      const consent = classifier.getExplicitCallbackConsent(receipt);
      if (!consent) return (await save(key, state, { status: "cancelled", reason: "no_current_live_callback_consent" })).status;
      const row = await (deps.row ?? readRow)(state.rowNumber);
      const proof = { conversationId, phone: state.phone, fullName: state.agentName, listingAddress: state.listingAddress };
      try { verifyVoiceTerminalReceipt(state.rowNumber, row, state.callAttemptNumber, proof, receipt); }
      catch { return (await save(key, state, { status: "held", reason: "current_phone_row_attempt_receipt_mismatch" })).status; }
      const held = holdReason(row);
      if (held) return (await save(key, state, { status: "held", reason: held })).status;
      if (await inboundBlocked(state.phone)) return "awaiting_final";
      state = await save(key, state, { requestedTiming: consent.callbackTime });
      const transcript = (receipt.transcript ?? []).filter((item: any) => item.message?.trim())
        .map((item: any) => `${item.role === "user" ? "Caller" : "Maya"}: ${item.message}`).join("\n");
      const summary = String(receipt.analysis?.transcript_summary ?? "No provider summary available.").slice(0, 2000);
      const input = { agentName: state.agentName, phone: state.phone, email: normalizeString(row[3]), listingAddress: state.listingAddress,
        rowNumber: state.rowNumber, callbackTime: consent.callbackTime === "unspecified" ? "Not specified by caller" : consent.callbackTime,
        conversationId, conversationDescription: summary, conversationTranscript: transcript.slice(0, 16000) + (transcript.length > 16000 ? "\n[Transcript shortened; use the recording for the full call.]" : ""),
        action: "Review the caller's request and arrange the manual callback; no appointment is confirmed.",
        details: `Caller: ${consent.callerText}${consent.offerText ? ` Accepted offer: ${consent.offerText}` : ""}. Requested timing is preserved without inventing AM/PM or a time zone. ${normalizeString(row[19]) ? "Human takeover is active and unchanged. " : ""}This alert does not place or schedule a call.` };
      const message = buildCallbackEmailMessage(input);
      if (deps.playback) {
        // Tests can substitute a non-secret recording URL without changing production signing.
        const { buildElevenLabsPlaybackUrl } = await import("./elevenLabsPlayback");
        const original = buildElevenLabsPlaybackUrl(conversationId);
        message.text = message.text.split(original).join(deps.playback(conversationId));
        message.html = message.html.split(original).join(deps.playback(conversationId));
      }
      const current = async () => {
        const latest = await (deps.row ?? readRow)(state!.rowNumber);
        try { verifyVoiceTerminalReceipt(state!.rowNumber, latest, state!.callAttemptNumber, proof, receipt); }
        catch { return false; }
        return !holdReason(latest) && !await inboundBlocked(state!.phone);
      };
      const result = await deliver(key, state, { ...message, messageId: state.messageId }, current);
      logger.info("Phone callback owner alert reconciled", { conversationId, rowNumber: state.rowNumber, deliveryStatus: result });
      return result;
    });
  }

  let recovering = false;
  async function recover(): Promise<void> {
    if (recovering) return;
    recovering = true;
    try {
      const queued = await (deps.list ?? (() => listGmailImportStates<CallbackAlertState>(PREFIX)))();
      for (const entry of queued.filter(entry => entry.value.status === "awaiting_final").slice(0, 20)) {
        if (entry.key !== PREFIX + entry.value.conversationId) throw new Error("Callback outbox identity mismatch");
        await settle(entry.value.conversationId).catch(() => logger.error("Callback owner alert requires retry/review", { conversationId: entry.value.conversationId }));
      }
    } finally { recovering = false; }
  }

  async function activate(): Promise<{ activatedAt: string }> {
    return lock(POLICY_KEY, async () => {
      const existing = await policy();
      if (existing) {
        if (!Number.isFinite(Date.parse(existing.activatedAt))) throw new Error("Invalid callback alert activation policy");
        return existing;
      }
      const next = { activatedAt: new Date().toISOString() };
      await savePolicy(next);
      if ((await policy())?.activatedAt !== next.activatedAt) throw new Error("Callback activation readback failed");
      return next;
    });
  }

  async function health() {
    const states = await (deps.list ?? (() => listGmailImportStates<CallbackAlertState>(PREFIX)))();
    for (const state of states) validate(state.value, state.value.conversationId);
    const counts = Object.fromEntries(["awaiting_final", "sending", "accepted", "uncertain", "cancelled", "held"].map(status =>
      [status, states.filter(state => state.value.status === status).length]));
    const review = states.filter(state => ["sending", "uncertain", "held"].includes(state.value.status) ||
      (state.value.status === "awaiting_final" && Date.now() - Date.parse(state.value.createdAt) > 15 * 60_000));
    return { activatedAt: (await policy())?.activatedAt ?? null, counts, needsReview: review.length > 0,
      exceptions: review.slice(0, 20).map(state => ({ conversationId: state.value.conversationId, row: state.value.rowNumber,
        status: state.value.status, reason: state.value.reason ?? "pending_more_than_15_minutes" })) };
  }

  async function selfTest(): Promise<{ status: string; messageId: string }> {
    const id = "callback-alert-self-test-20261111-v1", key = "voice_callback_owner_alert_test_v1:" + id;
    return lock(key, async () => {
      let state = await read(key);
      if (!state) {
        const now = new Date().toISOString();
        state = { conversationId: id, rowNumber: 0, callAttemptNumber: 1, agentName: "Synthetic verification", phone: "+12025550123",
          listingAddress: "NOT A REAL LISTING", schema: "voice_callback_owner_alert_v1", status: "awaiting_final", createdAt: now, updatedAt: now,
          messageId: `<${id}@crisp-voice-bot.onrender.com>` };
        await save(key, state, {});
      }
      validate(state, id);
      if (state.status !== "awaiting_final") return { status: state.status, messageId: state.messageId };
      const text = "This is the one owner-approved production callback-alert test. It is NOT a real hot lead. No lead row was changed, no call was placed, and no callback was scheduled.\n\nSynthetic contact: Synthetic verification\nSynthetic phone: +12025550123 (do not call)\nListing: NOT A REAL LISTING\nRequested timing example: tomorrow at 3 (caller words; not an appointment)\nRecording: not applicable, because no call was placed.\n\nReal callback alerts include the verified contact, phone, listing, caller request, requested timing, summary, transcript and protected recording link.";
      const message = buildCallbackEmailMessage({ agentName: state.agentName, phone: state.phone,
        listingAddress: state.listingAddress, rowNumber: 0, callbackTime: "tomorrow at 3 (synthetic; not an appointment)",
        conversationDescription: text });
      const status = await deliver(key, state, { ...message, subject: `[TEST] ${message.subject}`,
        messageId: state.messageId }, async () => true);
      return { status, messageId: state.messageId };
    });
  }
  return { enqueue, settle, recover, selfTest, activate, health };
}

export const voiceCallbackAlerts = createVoiceCallbackAlertService();
export function getVoiceCallbackAlertStatus() {
  const names = ["to", "from", "smtpHost", "smtpPort", "smtpUser", "smtpPass"] as const;
  return { enabled: true, configured: names.every(name => Boolean(config.emailAlerts[name])),
    provider: config.voiceProvider,
    independentOfTranscriptEmailToggle: true, durableOutbox: true, finalCallerConsentRequired: true,
    uncertainSendsHeld: true, automaticCallPlacement: false, recoveryPollingSeconds: 60, singleInstanceRequired: true };
}
let workerStarted = false;
export function startVoiceCallbackAlertWorker(): void {
  if (workerStarted) return;
  workerStarted = true;
  const run = () => { void voiceCallbackAlerts.recover().catch(() => logger.error("Callback owner alert outbox could not be read; delivery unverified")); };
  run();
  setInterval(run, 60_000).unref();
}
