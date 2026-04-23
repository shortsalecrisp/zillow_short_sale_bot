import { createEmailTransporter, escapeHtml, formatPhoneNumber, requireEmailConfig } from "./emailAlerts";

type CallbackEmailInput = {
  agentName: string;
  phone: string;
  listingAddress: string;
  rowNumber: number;
  subject?: string;
  action?: string;
  conversationDescription?: string;
  callbackTime?: string;
  details?: string;
};

export async function sendCallbackEmail({
  agentName,
  phone,
  listingAddress,
  rowNumber,
  subject = "🔥 Callback Requested - New Lead",
  action,
  conversationDescription,
  callbackTime,
  details,
}: CallbackEmailInput): Promise<void> {
  const emailConfig = requireEmailConfig();
  const transporter = createEmailTransporter(emailConfig);

  const formattedPhone = formatPhoneNumber(phone);
  const effectiveCallbackTime = callbackTime?.trim() || "ASAP";
  const conversationOutline = conversationDescription?.trim() || "No conversation outline provided.";
  const text = `Agent: ${agentName}
Phone: ${formattedPhone}
Property: ${listingAddress}
Callback Time: ${effectiveCallbackTime}

Conversation Outline:
${conversationOutline}`;

  const html = `<p><strong>Agent:</strong> ${escapeHtml(agentName)}</p>
<p><strong>Phone:</strong> ${escapeHtml(formattedPhone)}</p>
<p><strong>Property:</strong> ${escapeHtml(listingAddress)}</p>
<p><strong>Callback Time: ${escapeHtml(effectiveCallbackTime)}</strong></p>
<p><strong>Conversation Outline:</strong><br>${escapeHtml(conversationOutline).replace(/\n/g, "<br>")}</p>`;

  await transporter.sendMail({
    to: emailConfig.to,
    from: emailConfig.from,
    subject,
    text,
    html,
  });
}
