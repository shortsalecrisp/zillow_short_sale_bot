import { createHmac, createPublicKey, timingSafeEqual, verify } from "node:crypto";

export const RETURN_CALL_NUMBER = "+12176341017";
export const RETURN_CALL_OFFICE = "+14043009526";
export const RETURN_CALL_ANNOUNCEMENT = "Crisp return call. Press 1 to connect.";
export type ReturnCallStage = "screen" | "accept" | "dial-result" | "recorded" | "record-done" | "playback";

export function xmlEscape(value: string): string {
  return value.replace(/[<>&"']/g, (char) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;", '"': "&quot;", "'": "&apos;" }[char]!));
}

export function responseXml(body: string): string {
  return `<?xml version="1.0" encoding="UTF-8"?><Response>${body}</Response>`;
}

export function verifyTelnyxForm(raw: string, timestamp: string, signature: string, publicKey: string, now = Date.now()): boolean {
  try {
    if (!/^\d{10}$/.test(timestamp) || Math.abs(now / 1000 - Number(timestamp)) > 300) return false;
    const keyBytes = Buffer.from(publicKey, "base64");
    const signatureBytes = Buffer.from(signature, "base64");
    if (keyBytes.length !== 32 || signatureBytes.length !== 64) return false;
    const key = createPublicKey({
      key: Buffer.concat([Buffer.from("302a300506032b6570032100", "hex"), keyBytes]),
      format: "der", type: "spki",
    });
    return verify(null, Buffer.from(`${timestamp}|${raw}`), key, signatureBytes);
  } catch { return false; }
}

export function parseTelnyxForm(raw: string): Record<string, string> {
  const result: Record<string, string> = Object.create(null);
  for (const [key, value] of new URLSearchParams(raw)) {
    if (Object.hasOwn(result, key)) throw new Error("Duplicate callback field");
    result[key] = value;
  }
  return result;
}

export function returnCallIncomingToken(secret: string): string {
  return createHmac("sha256", secret).update("crisp-return-call-incoming-v1").digest("hex");
}

export function verifyReturnCallIncomingToken(token: string, secret: string): boolean {
  return /^[a-f0-9]{64}$/.test(token) && timingSafeEqual(Buffer.from(token, "hex"), Buffer.from(returnCallIncomingToken(secret), "hex"));
}

function signLink(parent: string, stage: ReturnCallStage, expires: number, secret: string): string {
  return createHmac("sha256", secret).update(JSON.stringify([parent, stage, expires])).digest("hex");
}

export function returnCallUrl(baseUrl: string, parent: string, stage: ReturnCallStage, secret: string, now = Date.now()): string {
  const expires = Math.floor(now / 1000) + (stage === "playback" ? 30 * 86400 : 6 * 3600);
  const url = new URL(`${baseUrl}/return-calls/${stage}`);
  url.searchParams.set("call", parent);
  url.searchParams.set("expires", String(expires));
  url.searchParams.set("sig", signLink(parent, stage, expires, secret));
  return url.toString();
}

export function verifyReturnCallLink(parent: string, stage: ReturnCallStage, expires: string, sig: string, secret: string, now = Date.now()): boolean {
  if (!parent || parent.length > 512 || !/^\d{10}$/.test(expires) || Number(expires) < now / 1000 || !/^[a-f0-9]{64}$/.test(sig)) return false;
  return timingSafeEqual(Buffer.from(sig, "hex"), Buffer.from(signLink(parent, stage, Number(expires), secret), "hex"));
}

export function dialOfficeXml(url: (stage: ReturnCallStage) => string): string {
  return responseXml(`<Dial action="${xmlEscape(url("dial-result"))}" method="POST" timeout="20" callerId="${RETURN_CALL_NUMBER}" record="do-not-record"><Number url="${xmlEscape(url("screen"))}">${RETURN_CALL_OFFICE}</Number></Dial>`);
}

export function screenOfficeXml(url: (stage: ReturnCallStage) => string): string {
  return responseXml(`<Gather numDigits="1" timeout="5" action="${xmlEscape(url("accept"))}"><Say>${RETURN_CALL_ANNOUNCEMENT}</Say></Gather><Hangup/>`);
}

export function voicemailXml(url: (stage: ReturnCallStage) => string): string {
  return responseXml(`<Say>You've reached Yoni Kutler at Crisp Short Sales. I can't take your call right now. After the tone, please leave your name, phone number, and the property you're calling about. Thank you.</Say><Record action="${xmlEscape(url("record-done"))}" method="POST" maxLength="120" timeout="0" finishOnKey="#" playBeep="true" recordingStatusCallback="${xmlEscape(url("recorded"))}" recordingStatusCallbackMethod="POST" recordingStatusCallbackEvent="completed"/><Hangup/>`);
}
