import express, { Router } from "express";
import axios from "axios";
import { config } from "../lib/config";
import { logger } from "../lib/logger";
import { dialOfficeXml, parseTelnyxForm, responseXml, RETURN_CALL_NUMBER, RETURN_CALL_OFFICE, returnCallUrl, screenOfficeXml, verifyReturnCallLink, verifyReturnCallIncomingToken, verifyTelnyxForm, voicemailXml, type ReturnCallStage } from "../lib/returnCallFlow";
import { notifyReturnCallVoicemail, readReturnCallState, updateReturnCallState, type ReturnCallStateDependencies } from "../lib/returnCallState";

const hangup = responseXml("<Hangup/>");
const webhookStages = new Set(["screen", "accept", "dial-result", "recorded", "record-done"]);
export type RouteSettings = { enabled: boolean; publicKey?: string; applicationId?: string; signingSecret?: string; baseUrl: string };

export function returnCallsReady(settings: RouteSettings): boolean {
  return settings.enabled && Boolean(settings.publicKey && settings.applicationId && settings.signingSecret && settings.signingSecret.length >= 32 && settings.baseUrl.startsWith("https://"));
}

export function authorizeReturnCallRequest(
  stage: string, query: Record<string, unknown>, raw: unknown, timestamp: string | undefined,
  signature: string | undefined, settings: RouteSettings,
): { parent: string; form: Record<string, string>; stage: "incoming" | ReturnCallStage } {
  if (!returnCallsReady(settings) || typeof raw !== "string") throw new Error("Return-call authorization unavailable");
  if (stage !== "incoming" && !webhookStages.has(stage)) throw new Error("Invalid call stage");
  // Provider signatures are guaranteed for recording webhooks, but not for instruction fetches.
  if (((timestamp || signature) || stage === "recorded") && !verifyTelnyxForm(raw, timestamp ?? "", signature ?? "", settings.publicKey!)) throw new Error("Invalid provider signature");
  const form = parseTelnyxForm(raw);
  if (!form.AccountSid || form.AccountSid.length > 128 || (form.ConnectionId && form.ConnectionId !== settings.applicationId)) throw new Error("Wrong provider account or application");
  const parent = stage === "incoming" ? form.CallSid : typeof query.call === "string" ? query.call : "";
  if (!parent || parent.length > 512) throw new Error("Invalid call identifier");
  // Every request carries a capability: a dedicated initial token, or an expiring call-and-stage link.
  if (stage === "incoming") {
    if (form.ConnectionId !== settings.applicationId || !verifyReturnCallIncomingToken(String(query.token ?? ""), settings.signingSecret!)) throw new Error("Invalid incoming authorization");
  } else if (!verifyReturnCallLink(parent, stage as ReturnCallStage, String(query.expires ?? ""), String(query.sig ?? ""), settings.signingSecret!)) throw new Error("Invalid call stage signature");
  return { parent, form, stage: stage as "incoming" | ReturnCallStage };
}

export async function handleReturnCall(
  stage: "incoming" | ReturnCallStage,
  parent: string,
  form: Record<string, string>,
  url: (stage: ReturnCallStage) => string,
  deps: ReturnCallStateDependencies = {},
): Promise<string> {
  const now = new Date().toISOString();
  if (stage === "incoming") {
    if (form.To !== RETURN_CALL_NUMBER || !form.From || form.From.length > 512 || form.CallSid !== parent) throw new Error("Invalid inbound destination");
    const state = await updateReturnCallState(parent, (existing) => existing ?? { parentCallSid: parent, callerFrom: form.From, accountSid: form.AccountSid, createdAt: now }, deps);
    if (state.callerFrom !== form.From || state.accountSid !== form.AccountSid) throw new Error("Inbound identity mismatch");
    // Never redial the office when an instruction fetch is retried after an accepted call or voicemail.
    if (state.acceptedAt || state.voicemailStartedAt) return hangup;
    return dialOfficeXml(url);
  }
  const state = await readReturnCallState(parent, deps);
  if (!state) throw new Error("Unknown return call");
  if (state.accountSid && form.AccountSid !== state.accountSid) throw new Error("Return-call account mismatch");
  if (stage === "screen" || stage === "accept") {
    if (!form.CallSid || form.CallSid === parent || form.To !== RETURN_CALL_OFFICE || (form.ParentCallSid && form.ParentCallSid !== parent)) throw new Error("Invalid office leg");
    if (state.voicemailStartedAt) return hangup;
    if (stage === "screen") {
      const screenState = await updateReturnCallState(parent, (current) => {
        if (!current || (current.officeCallSid && current.officeCallSid !== form.CallSid)) throw new Error("Office leg mismatch");
        if (current.voicemailStartedAt) return current;
        return { ...current, officeCallSid: form.CallSid };
      }, deps);
      return screenState.voicemailStartedAt ? hangup : screenState.acceptedAt ? responseXml("") : screenOfficeXml(url);
    }
    if (state.officeCallSid !== form.CallSid) throw new Error("Office leg mismatch");
    if (form.Digits !== "1") return hangup;
    await updateReturnCallState(parent, (current) => {
      if (!current || current.voicemailStartedAt) throw new Error("Call is no longer accepting an office answer");
      return { ...current, acceptedAt: current.acceptedAt ?? now };
    }, deps);
    return responseXml("");
  }
  if (form.CallSid !== parent) throw new Error("Caller leg mismatch");
  if (stage === "dial-result") {
    if (state.acceptedAt || state.recordingId) return hangup;
    const resultState = await updateReturnCallState(parent, (current) => {
      if (!current) throw new Error("Unknown return call");
      if (current.acceptedAt || current.recordingId) return current;
      return { ...current, voicemailStartedAt: current.voicemailStartedAt ?? now };
    }, deps);
    return resultState.acceptedAt || resultState.recordingId ? hangup : voicemailXml(url);
  }
  if (!state.voicemailStartedAt || state.acceptedAt) throw new Error("Voicemail was not offered");
  if (stage === "recorded") {
    if (form.RecordingStatus !== "completed") return responseXml("");
    if (!/^[a-f0-9-]{36}$/i.test(form.RecordingSid ?? "")) throw new Error("Invalid recording ID");
    const duration = Number(form.RecordingDuration);
    if (!Number.isFinite(duration) || duration < 0 || duration > 180) throw new Error("Invalid recording duration");
    await updateReturnCallState(parent, (current) => {
      if (!current) throw new Error("Unknown return call");
      return { ...current, recordingId: form.RecordingSid, recordingDuration: duration };
    }, deps);
    const notification = await notifyReturnCallVoicemail(parent, url("playback"), deps);
    if (notification.status === "uncertain") logger.error("Return-call voicemail email needs reconciliation", { parentCallSid: parent });
    return responseXml("");
  }
  if (stage === "record-done") return responseXml("<Say>Thank you. Goodbye.</Say><Hangup/>");
  throw new Error("Invalid return-call stage");
}

const router = Router();
const settings = { ...config.returnCalls, baseUrl: config.baseUrl };

router.get("/playback", async (req, res) => {
  res.set({ "Cache-Control": "no-store", "Referrer-Policy": "no-referrer" });
  const parent = typeof req.query.call === "string" ? req.query.call : "";
  if (!returnCallsReady(settings) || !verifyReturnCallLink(parent, "playback", String(req.query.expires ?? ""), String(req.query.sig ?? ""), settings.signingSecret!)) {
    res.status(403).send("This voicemail link is invalid or expired."); return;
  }
  try {
    const state = await readReturnCallState(parent);
    if (!state?.recordingId) { res.status(404).send("Voicemail is not available."); return; }
    if (!state.accountSid) throw new Error("Missing recording account");
    const recording = await axios.get(`https://api.telnyx.com/v2/texml/Accounts/${encodeURIComponent(state.accountSid)}/Recordings/${encodeURIComponent(state.recordingId)}.json`, {
      headers: { Authorization: `Bearer ${config.telnyx.apiKey}` }, timeout: 15000,
    });
    const data = recording.data;
    if (data.sid !== state.recordingId || data.account_sid !== state.accountSid || data.call_sid !== parent) throw new Error("Recording identity mismatch");
    const destination = new URL(data.media_url);
    if (destination.protocol !== "https:" || destination.username || destination.password) throw new Error("Invalid recording URL");
    res.redirect(302, destination.toString());
  } catch {
    logger.error("Return-call voicemail playback unavailable", { parentCallSid: parent });
    res.status(502).send("Voicemail playback is temporarily unavailable. Please try again shortly.");
  }
});

// This router is mounted before express.json: signatures cover the exact URL-encoded bytes.
router.post("/:stage", express.text({ type: "application/x-www-form-urlencoded", limit: "32kb" }), async (req, res) => {
  res.set("Cache-Control", "no-store");
  if (!returnCallsReady(settings)) { res.status(503).send("Return-call routing is unavailable"); return; }
  let authorized: ReturnType<typeof authorizeReturnCallRequest>;
  try {
    authorized = authorizeReturnCallRequest(req.params.stage, req.query, req.body, req.get("telnyx-timestamp"), req.get("telnyx-signature-ed25519"), settings);
  } catch { res.status(401).send("Invalid return-call authorization"); return; }
  try {
    const { parent, stage, form } = authorized;
    const xml = await handleReturnCall(stage, parent, form, (next) => returnCallUrl(settings.baseUrl, parent, next, settings.signingSecret!));
    res.type("text/xml").send(xml);
  } catch (error) {
    logger.error("Return-call callback failed", { stage: req.params.stage, error: error instanceof Error ? error.message : "Unexpected failure" });
    res.status(500).type("text/xml").send(hangup);
  }
});

export default router;
