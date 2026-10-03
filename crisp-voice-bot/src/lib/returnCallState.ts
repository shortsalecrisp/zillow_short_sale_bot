import { createHash } from "node:crypto";
import { createEmailTransporter, escapeHtml, formatPhoneNumber, requireEmailConfig } from "./emailAlerts";
import { readGmailImportState, writeGmailImportState } from "./gmailImportState";

export type ReturnCallState = {
  parentCallSid: string;
  callerFrom: string;
  accountSid?: string;
  officeCallSid?: string;
  createdAt: string;
  acceptedAt?: string;
  voicemailStartedAt?: string;
  recordingId?: string;
  recordingDuration?: number;
  emailStatus?: "sending" | "sent" | "uncertain";
  emailError?: string;
};

export type ReturnCallEmailMessage = { subject: string; text: string; html: string; messageId: string };
export type ReturnCallStateDependencies = {
  readState?: (key: string) => Promise<ReturnCallState | null>;
  writeState?: (key: string, state: ReturnCallState) => Promise<void>;
  sendMail?: (message: ReturnCallEmailMessage) => Promise<unknown>;
};

const locks = new Map<string, Promise<void>>();
const validEmailStatuses = new Set(["sending", "sent", "uncertain"]);

export function returnCallStateKey(parentCallSid: string): string {
  if (!parentCallSid.trim() || parentCallSid.length > 512) throw new Error("Invalid return-call identifier");
  return `telnyx_return_call_v1:${createHash("sha256").update(parentCallSid).digest("hex")}`;
}

// The release must remain single-instance: BotState is durable but does not provide compare-and-swap.
async function withReturnCallLock<T>(parentCallSid: string, work: () => Promise<T>): Promise<T> {
  const key = returnCallStateKey(parentCallSid);
  const previous = locks.get(key) ?? Promise.resolve();
  let release!: () => void;
  const current = new Promise<void>((resolve) => { release = resolve; });
  locks.set(key, current);
  await previous;
  try {
    return await work();
  } finally {
    release();
    if (locks.get(key) === current) locks.delete(key);
  }
}

function validateState(state: ReturnCallState, parentCallSid: string): void {
  if (!state || typeof state !== "object" || state.parentCallSid !== parentCallSid || typeof state.callerFrom !== "string" ||
      !state.callerFrom || typeof state.createdAt !== "string" || !Number.isFinite(Date.parse(state.createdAt)) ||
      (state.emailStatus !== undefined && !validEmailStatuses.has(state.emailStatus))) {
    throw new Error("Invalid persisted return-call state; manual review required");
  }
}

async function readUnlocked(parentCallSid: string, deps: ReturnCallStateDependencies): Promise<ReturnCallState | null> {
  const key = returnCallStateKey(parentCallSid);
  const state = deps.readState
    ? await deps.readState(key)
    : await readGmailImportState<ReturnCallState | undefined>(key, undefined, true);
  if (state === undefined || (deps.readState && state === null)) return null;
  validateState(state as ReturnCallState, parentCallSid);
  return state as ReturnCallState;
}

async function writeUnlocked(state: ReturnCallState, deps: ReturnCallStateDependencies): Promise<void> {
  validateState(state, state.parentCallSid);
  const key = returnCallStateKey(state.parentCallSid);
  await (deps.writeState ?? writeGmailImportState)(key, state);
}

export async function readReturnCallState(
  parentCallSid: string,
  deps: ReturnCallStateDependencies = {},
): Promise<ReturnCallState | null> {
  return withReturnCallLock(parentCallSid, () => readUnlocked(parentCallSid, deps));
}

export async function updateReturnCallState(
  parentCallSid: string,
  update: (current: ReturnCallState | null) => ReturnCallState | Promise<ReturnCallState>,
  deps: ReturnCallStateDependencies = {},
): Promise<ReturnCallState> {
  return withReturnCallLock(parentCallSid, async () => {
    const current = await readUnlocked(parentCallSid, deps);
    const next = await update(current ? { ...current } : null);
    validateState(next, parentCallSid);
    if (current && (next.callerFrom !== current.callerFrom || next.createdAt !== current.createdAt || next.accountSid !== current.accountSid)) {
      throw new Error("Return-call identity cannot be replaced");
    }
    if (current?.emailStatus && next.emailStatus !== current.emailStatus) {
      throw new Error("Cannot replace or remove return-call email receipt");
    }
    if (current?.acceptedAt && next.acceptedAt !== current.acceptedAt) {
      throw new Error("Cannot replace or remove return-call acceptance receipt");
    }
    if (current?.recordingId && next.recordingId !== current.recordingId) {
      throw new Error("Cannot replace return-call voicemail recording");
    }
    await writeUnlocked(next, deps);
    return next;
  });
}

export function buildReturnCallVoicemailEmail(state: ReturnCallState, playbackUrl: string): ReturnCallEmailMessage {
  if (!state.recordingId) throw new Error("Return-call recording is required");
  const parsedUrl = new URL(playbackUrl);
  if (parsedUrl.protocol !== "https:" || parsedUrl.username || parsedUrl.password) {
    throw new Error("Invalid return-call playback URL");
  }
  const caller = /^\+\d{8,15}$/.test(state.callerFrom)
    ? formatPhoneNumber(state.callerFrom)
    : "Unavailable or non-telephone caller ID";
  const duration = Number.isFinite(state.recordingDuration) ? `${state.recordingDuration} seconds` : "Unavailable";
  const subject = `Crisp return-call voicemail - ${caller}`;
  const text = `A caller left a voicemail on the Crisp Short Sales return-call line.\n\nCaller number (not verified identity): ${caller}\nCall received (UTC): ${state.createdAt}\nVoicemail duration: ${duration}\nPlayback: ${playbackUrl}\n\nListen before following up. This notification does not establish the caller's identity, interest, or agreement to a callback.\nCall reference: ${state.parentCallSid}\nRecording reference: ${state.recordingId}`;
  const html = `<p>A caller left a voicemail on the Crisp Short Sales return-call line.</p><p><strong>Caller number (not verified identity):</strong> ${escapeHtml(caller)}<br><strong>Call received (UTC):</strong> ${escapeHtml(state.createdAt)}<br><strong>Voicemail duration:</strong> ${escapeHtml(duration)}</p><p><a href="${escapeHtml(playbackUrl)}">Play voicemail</a></p><p>Listen before following up. This notification does not establish the caller's identity, interest, or agreement to a callback.</p><p>Call reference: ${escapeHtml(state.parentCallSid)}<br>Recording reference: ${escapeHtml(state.recordingId)}</p>`;
  return {
    subject, text, html,
    messageId: `<return-call-voicemail-${createHash("sha256").update(state.recordingId).digest("hex")}@crisp-voice-bot.onrender.com>`,
  };
}

export type ReturnCallNotificationResult = {
  status: "sent" | "skipped" | "uncertain" | "no_recording";
  emailStatus?: ReturnCallState["emailStatus"];
  messageId?: string;
};

export async function notifyReturnCallVoicemail(
  parentCallSid: string,
  playbackUrl: string,
  deps: ReturnCallStateDependencies = {},
): Promise<ReturnCallNotificationResult> {
  return withReturnCallLock(parentCallSid, async () => {
    const state = await readUnlocked(parentCallSid, deps);
    if (!state?.recordingId) return { status: "no_recording" };
    const message = buildReturnCallVoicemailEmail(state, playbackUrl);
    if (state.emailStatus) return { status: "skipped", emailStatus: state.emailStatus, messageId: message.messageId };
    const sending: ReturnCallState = { ...state, emailStatus: "sending" };
    await writeUnlocked(sending, deps);
    try {
      if (deps.sendMail) {
        await deps.sendMail(message);
      } else {
        const emailConfig = requireEmailConfig();
        await createEmailTransporter(emailConfig).sendMail({
          ...message, to: emailConfig.to, from: emailConfig.from,
        });
      }
      await writeUnlocked({ ...sending, emailStatus: "sent" }, deps);
      return { status: "sent", emailStatus: "sent", messageId: message.messageId };
    } catch {
      // SMTP acceptance can be ambiguous. Never send again before manual Sent-folder reconciliation.
      await writeUnlocked({
        ...sending,
        emailStatus: "uncertain",
        emailError: "Send or receipt persistence failed; reconcile the stable Message-ID before retrying.",
      }, deps).catch(() => undefined);
      return { status: "uncertain", emailStatus: "uncertain", messageId: message.messageId };
    }
  });
}
