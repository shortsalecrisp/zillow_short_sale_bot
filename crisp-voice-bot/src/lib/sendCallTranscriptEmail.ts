import { createEmailTransporter, escapeHtml, formatPhoneNumber, requireEmailConfig } from "./emailAlerts";

type SendCallTranscriptEmailInput = {
  agentName: string;
  requestedPhone: string;
  dialedPhone: string;
  listingAddress: string;
  rowNumber: number;
  callAttemptNumber: number;
  conversationId: string;
  outcome: string;
  summary: string;
  transcript: string;
  testMode: boolean;
};

export async function sendCallTranscriptEmail({
  agentName,
  requestedPhone,
  dialedPhone,
  listingAddress,
  rowNumber,
  callAttemptNumber,
  conversationId,
  outcome,
  summary,
  transcript,
  testMode,
}: SendCallTranscriptEmailInput): Promise<void> {
  const emailConfig = requireEmailConfig();
  const transporter = createEmailTransporter(emailConfig);
  const formattedRequestedPhone = formatPhoneNumber(requestedPhone);
  const formattedDialedPhone = formatPhoneNumber(dialedPhone);
  const transcriptBody = transcript.trim() || "No transcript was available for this call.";
  const safeSummary = summary.trim() || "No summary was available for this call.";
  const subject = `Call Transcript - ${agentName} - ${outcome}`;

  const textLines = [
    `Agent: ${agentName}`,
    `Agent Phone: ${formattedRequestedPhone}`,
    ...(testMode && dialedPhone !== requestedPhone ? [`Dialed Number (Test Mode): ${formattedDialedPhone}`] : []),
    `Property: ${listingAddress}`,
    `Outcome: ${outcome}`,
    `Call Attempt: ${callAttemptNumber}`,
    `Row: ${rowNumber}`,
    `Conversation ID: ${conversationId}`,
    "",
    "Summary:",
    safeSummary,
    "",
    "Full Transcript:",
    transcriptBody,
  ];

  const html = `<p><strong>Agent:</strong> ${escapeHtml(agentName)}</p>
<p><strong>Agent Phone:</strong> ${escapeHtml(formattedRequestedPhone)}</p>
${testMode && dialedPhone !== requestedPhone ? `<p><strong>Dialed Number (Test Mode):</strong> ${escapeHtml(formattedDialedPhone)}</p>` : ""}
<p><strong>Property:</strong> ${escapeHtml(listingAddress)}</p>
<p><strong>Outcome:</strong> ${escapeHtml(outcome)}</p>
<p><strong>Call Attempt:</strong> ${callAttemptNumber}</p>
<p><strong>Row:</strong> ${rowNumber}</p>
<p><strong>Conversation ID:</strong> ${escapeHtml(conversationId)}</p>
<p><strong>Summary:</strong><br>${escapeHtml(safeSummary).replace(/\n/g, "<br>")}</p>
<p><strong>Full Transcript:</strong></p>
<pre style="white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size: 13px; line-height: 1.45;">${escapeHtml(
    transcriptBody,
  )}</pre>`;

  await transporter.sendMail({
    to: emailConfig.to,
    from: emailConfig.from,
    subject,
    text: textLines.join("\n"),
    html,
  });
}
