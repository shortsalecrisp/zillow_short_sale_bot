import { createEmailTransporter, escapeHtml, formatPhoneNumber, requireEmailConfig } from "./emailAlerts";
import { buildElevenLabsPlaybackUrl } from "./elevenLabsPlayback";

type CallbackEmailInput = {
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
  details,
}: CallbackEmailInput): CallbackEmailMessage {
  const formattedPhone = formatDashedPhoneNumber(phone);
  const effectiveCallbackTime = callbackTime?.trim() || "Unspecified";
  const effectiveEmail = email?.trim() || "";
  const safeConversationId = conversationId?.trim();
  const playbackUrl = safeConversationId ? buildElevenLabsPlaybackUrl(safeConversationId) : undefined;
  const fullConversation =
    conversationTranscript?.trim() ||
    conversationDescription?.trim() ||
    "No conversation transcript available.";
  const effectiveSubject = subject ?? `NEW LEAD 🔥 - SCHEDULED CALLBACK - ${agentName}`;

  const text = `We have a new lead interested in your services, and a manual follow-up is now needed.

Handoff Type: Callback Request
Scheduled Time: ${effectiveCallbackTime}
Agent Name: ${agentName}
Phone: ${formattedPhone}
Email: ${effectiveEmail}
Address: ${listingAddress}
${safeConversationId ? `Conversation ID: ${safeConversationId}\n` : ""}${playbackUrl ? `Playback: ${playbackUrl}\n` : ""}

Full Convo:
${fullConversation}`;

  const html = `<p>We have a new lead interested in your services, and a manual follow-up is now needed.</p>
<p><strong>Handoff Type:</strong> Callback Request<br>
<strong>Scheduled Time:</strong> ${escapeHtml(effectiveCallbackTime)}<br>
<strong>Agent Name:</strong> ${escapeHtml(agentName)}<br>
<strong>Phone:</strong> ${escapeHtml(formattedPhone)}<br>
<strong>Email:</strong> ${escapeHtml(effectiveEmail)}<br>
<strong>Address:</strong> ${escapeHtml(listingAddress)}${safeConversationId ? `<br>\n<strong>Conversation ID:</strong> ${escapeHtml(safeConversationId)}` : ""}</p>
${playbackUrl ? `<p><strong>Playback:</strong> <a href="${escapeHtml(playbackUrl)}" target="_blank" rel="noopener noreferrer">Play call recording</a></p>` : ""}
<p><strong>Full Convo:</strong><br>${escapeHtml(fullConversation).replace(/\n/g, "<br>")}</p>`;

  return {
    subject: effectiveSubject,
    text,
    html,
  };
}

export async function sendCallbackEmail(input: CallbackEmailInput): Promise<void> {
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
