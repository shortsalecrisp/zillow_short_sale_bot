import { gmail_v1 } from "googleapis";

export type GmailMessage = gmail_v1.Schema$Message;
export type GmailMessageSummary = gmail_v1.Schema$Message;

type GmailPart = gmail_v1.Schema$MessagePart;

export function gmailHeader(message: GmailMessage, name: string): string {
  const headers = message.payload?.headers ?? [];
  const found = headers.find((header) => String(header.name ?? "").toLowerCase() === name.toLowerCase());
  return found ? String(found.value ?? "") : "";
}

function flattenParts(part: GmailPart | undefined): GmailPart[] {
  if (!part) {
    return [];
  }

  const result = [part];
  for (const child of part.parts ?? []) {
    result.push(...flattenParts(child));
  }
  return result;
}

export function decodeGmailBody(data: string | null | undefined): string {
  const raw = String(data ?? "").replace(/\s/g, "");
  if (!raw) {
    return "";
  }

  const standard = raw.replace(/-/g, "+").replace(/_/g, "/");
  const candidates = [raw, padBase64(raw), standard, padBase64(standard)].filter(
    (value, index, array) => value && array.indexOf(value) === index,
  );

  for (const candidate of candidates) {
    try {
      return Buffer.from(candidate, "base64").toString("utf8");
    } catch {
      // Keep trying the alternate padding/encoding forms.
    }
  }

  return "";
}

function padBase64(value: string): string {
  let padded = value;
  while (padded.length % 4 !== 0) {
    padded += "=";
  }
  return padded;
}

async function partBodyText(gmail: gmail_v1.Gmail, messageId: string | undefined | null, part: GmailPart): Promise<string> {
  if (part.body?.data) {
    return decodeGmailBody(part.body.data);
  }

  if (!messageId || !part.body?.attachmentId) {
    return "";
  }

  const attachment = await gmail.users.messages.attachments.get({
    userId: "me",
    messageId,
    id: part.body.attachmentId,
  });

  return decodeGmailBody(attachment.data.data);
}

export async function gmailPlainText(gmail: gmail_v1.Gmail, message: GmailMessage): Promise<string> {
  const payload = message.payload;
  const parts = flattenParts(payload ?? undefined);
  const plain = parts.find((part) => part.mimeType === "text/plain" && part.body && (part.body.data || part.body.attachmentId));
  const html = parts.find((part) => part.mimeType === "text/html" && part.body && (part.body.data || part.body.attachmentId));
  const calendar = parts.find(
    (part) => part.mimeType === "text/calendar" && part.body && (part.body.data || part.body.attachmentId),
  );
  const anyText = parts.find((part) => /^text\//i.test(part.mimeType ?? "") && part.body && (part.body.data || part.body.attachmentId));

  if (plain) return partBodyText(gmail, message.id, plain);
  if (html) return partBodyText(gmail, message.id, html);
  if (calendar) return partBodyText(gmail, message.id, calendar);
  if (anyText) return partBodyText(gmail, message.id, anyText);
  return payload?.body?.data ? decodeGmailBody(payload.body.data) : "";
}
