import { createEmailTransporter, escapeHtml, formatPhoneNumber, requireEmailConfig } from "./emailAlerts";

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
  callbackTime?: string;
  details?: string;
};

function formatDashedPhoneNumber(phone: string): string {
  const formatted = formatPhoneNumber(phone);
  const digits = formatted.replace(/\D/g, "");

  if (digits.length !== 10) {
    return phone;
  }

  return `${digits.slice(0, 3)}-${digits.slice(3, 6)}-${digits.slice(6)}`;
}

export async function sendCallbackEmail({
  agentName,
  phone,
  email,
  listingAddress,
  rowNumber,
  subject,
  action,
  conversationDescription,
  conversationTranscript,
  callbackTime,
  details,
}: CallbackEmailInput): Promise<void> {
  const emailConfig = requireEmailConfig();
  const transporter = createEmailTransporter(emailConfig);

  const formattedPhone = formatDashedPhoneNumber(phone);
  const effectiveCallbackTime = callbackTime?.trim() || "Unspecified";
  const effectiveEmail = email?.trim() || "";
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

Full Convo:
${fullConversation}`;

  const html = `<p>We have a new lead interested in your services, and a manual follow-up is now needed.</p>
<p><strong>Handoff Type:</strong> Callback Request<br>
<strong>Scheduled Time:</strong> ${escapeHtml(effectiveCallbackTime)}<br>
<strong>Agent Name:</strong> ${escapeHtml(agentName)}<br>
<strong>Phone:</strong> ${escapeHtml(formattedPhone)}<br>
<strong>Email:</strong> ${escapeHtml(effectiveEmail)}<br>
<strong>Address:</strong> ${escapeHtml(listingAddress)}</p>
<p><strong>Full Convo:</strong><br>${escapeHtml(fullConversation).replace(/\n/g, "<br>")}</p>`;

  await transporter.sendMail({
    to: emailConfig.to,
    from: emailConfig.from,
    subject: effectiveSubject,
    text,
    html,
  });
}
