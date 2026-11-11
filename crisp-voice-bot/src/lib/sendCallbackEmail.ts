import { createEmailTransporter, escapeHtml, formatPhoneNumber, requireEmailConfig } from "./emailAlerts";
import { buildElevenLabsPlaybackUrl } from "./elevenLabsPlayback";

export type CallbackEmailInput = {
  agentName: string;
  phone: string;
  email?: string;
  listingAddress: string;
  rowNumber: number;
  subject?: string;
  action?: string;
  conversationDescription?: string;
  conversationTranscript?: string;
  conversationId?: string;
  callbackTime?: string;
  handoffType?: "Callback Request" | "Information Request";
  details?: string;
};

type CallbackEmailMessage = {
  subject: string;
  text: string;
  html: string;
};

function formatDashedPhoneNumber(phone: string): string {
  const formatted = formatPhoneNumber(phone);
  const digits = formatted.replace(/\D/g, "");

  if (digits.length !== 10) {
    return phone;
  }

  return `${digits.slice(0, 3)}-${digits.slice(3, 6)}-${digits.slice(6)}`;
}

export function buildCallbackEmailMessage({
  agentName,
  phone,
  email,
  listingAddress,
  rowNumber,
  subject,
  action,
  conversationDescription,
  conversationTranscript,
  conversationId,
  callbackTime,
  handoffType = "Callback Request",
  details,
}: CallbackEmailInput): CallbackEmailMessage {
  const formattedPhone = formatDashedPhoneNumber(phone);
  const effectiveCallbackTime = callbackTime?.trim() || "Not specified by caller";
  const effectiveEmail = email?.trim() || "";
  const safeConversationId = conversationId?.trim();
  const playbackUrl = safeConversationId ? buildElevenLabsPlaybackUrl(safeConversationId) : undefined;
  const fullConversation =
    conversationTranscript?.trim() ||
    conversationDescription?.trim() ||
    "No conversation transcript available.";
  const effectiveSubject = subject ?? `HOT LEAD - PHONE CALLBACK REQUEST - ${agentName}`;
  const scheduledTimeLine = handoffType === "Callback Request" ? `Requested Timing: ${effectiveCallbackTime}\nThis is a request, not a booked appointment.\n` : "";
  const scheduledTimeHtml =
    handoffType === "Callback Request"
      ? `<strong>Requested Timing:</strong> ${escapeHtml(effectiveCallbackTime)}<br>\nThis is a request, not a booked appointment.<br>\n`
      : "";

  const text = `We have a new lead interested in your services, and a manual follow-up is now needed.

Handoff Type: ${handoffType}
${scheduledTimeLine}Agent Name: ${agentName}
Phone: ${formattedPhone}
Email: ${effectiveEmail}
Address: ${listingAddress}
Sheet row: ${rowNumber}
${safeConversationId ? `Conversation ID: ${safeConversationId}\n` : ""}${playbackUrl ? `Playback: ${playbackUrl}\n` : ""}
${action ? `Next action: ${action}\n` : ""}${details ? `Request evidence: ${details}\n` : ""}${conversationDescription ? `Call summary: ${conversationDescription}\n` : ""}
Full Convo:
${fullConversation}`;

  const html = `<p>We have a new lead interested in your services, and a manual follow-up is now needed.</p>
<p><strong>Handoff Type:</strong> ${escapeHtml(handoffType)}<br>
${scheduledTimeHtml}<strong>Agent Name:</strong> ${escapeHtml(agentName)}<br>
<strong>Phone:</strong> ${escapeHtml(formattedPhone)}<br>
<strong>Email:</strong> ${escapeHtml(effectiveEmail)}<br>
<strong>Address:</strong> ${escapeHtml(listingAddress)}<br><strong>Sheet row:</strong> ${rowNumber}${safeConversationId ? `<br>\n<strong>Conversation ID:</strong> ${escapeHtml(safeConversationId)}` : ""}</p>
${playbackUrl ? `<p><strong>Playback:</strong> <a href="${escapeHtml(playbackUrl)}" target="_blank" rel="noopener noreferrer">Play call recording</a></p>` : ""}
${action ? `<p><strong>Next action:</strong> ${escapeHtml(action)}</p>` : ""}
${details ? `<p><strong>Request evidence:</strong> ${escapeHtml(details)}</p>` : ""}
${conversationDescription ? `<p><strong>Call summary:</strong> ${escapeHtml(conversationDescription)}</p>` : ""}
<p><strong>Full Convo:</strong><br>${escapeHtml(fullConversation).replace(/\n/g, "<br>")}</p>`;

  return {
    subject: effectiveSubject,
    text,
    html,
  };
}

export async function sendInformationOwnerAlertEmail(input: CallbackEmailInput): Promise<void> {
  if (input.handoffType !== "Information Request") throw new Error("Phone callbacks must use the verified durable callback alert path");
  await sendLegacyTelnyxCallbackEmail(input);
}

// Retained for the inactive keypad/Telnyx flow; production Maya uses voiceCallbackAlerts.
export async function sendLegacyTelnyxCallbackEmail(input: CallbackEmailInput): Promise<void> {
  const emailConfig = requireEmailConfig();
  const transporter = createEmailTransporter(emailConfig);
  const { subject, text, html } = buildCallbackEmailMessage(input);

  await transporter.sendMail({
    to: emailConfig.to,
    from: emailConfig.from,
    subject,
    text,
    html,
  });
}
