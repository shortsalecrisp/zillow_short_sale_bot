import { createEmailTransporter } from "./emailAlerts";
import { config } from "./config";

export type AgentInfoEmailInput = {
  to: string;
  subject: string;
  body: string;
};

export type AgentInfoEmailResult = {
  messageId?: string;
  accepted: string[];
  rejected: string[];
};

function normalizeEmail(value: string): string {
  return value.trim().toLowerCase();
}

export function isValidInfoEmailAddress(value: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value.trim());
}

export function requireInfoEmailApiSecret(): string {
  const secret = config.infoEmail.apiSecret?.trim();
  if (!secret) {
    throw Object.assign(new Error("INFO_EMAIL_API_SECRET is not configured"), { statusCode: 503 });
  }
  return secret;
}

function requireInfoEmailSmtpConfig() {
  const missing = [
    ["INFO_EMAIL_FROM", config.infoEmail.from],
    ["INFO_EMAIL_SMTP_HOST", config.infoEmail.smtpHost],
    ["INFO_EMAIL_SMTP_PORT", config.infoEmail.smtpPort],
    ["INFO_EMAIL_SMTP_USER", config.infoEmail.smtpUser],
    ["INFO_EMAIL_SMTP_PASS", config.infoEmail.smtpPass],
  ]
    .filter(([, value]) => !value)
    .map(([name]) => name);

  if (missing.length > 0) {
    throw Object.assign(new Error(`Missing info email SMTP config: ${missing.join(", ")}`), { statusCode: 503 });
  }

  return {
    to: "",
    from: config.infoEmail.from,
    smtpHost: config.infoEmail.smtpHost,
    smtpPort: config.infoEmail.smtpPort,
    smtpUser: config.infoEmail.smtpUser,
    smtpPass: config.infoEmail.smtpPass as string,
  };
}

export function validateAgentInfoEmailInput(input: AgentInfoEmailInput): AgentInfoEmailInput {
  const to = normalizeEmail(String(input.to || ""));
  const subject = String(input.subject || "").trim();
  const body = String(input.body || "").trim();

  if (!isValidInfoEmailAddress(to)) {
    throw Object.assign(new Error("Invalid info email recipient"), { statusCode: 400 });
  }
  if (!subject || subject.length > 160) {
    throw Object.assign(new Error("Invalid info email subject"), { statusCode: 400 });
  }
  if (!body || body.length > 12000) {
    throw Object.assign(new Error("Invalid info email body"), { statusCode: 400 });
  }

  return { to, subject, body };
}

export async function sendAgentInfoEmail(input: AgentInfoEmailInput): Promise<AgentInfoEmailResult> {
  const payload = validateAgentInfoEmailInput(input);
  const emailConfig = requireInfoEmailSmtpConfig();
  const transporter = createEmailTransporter(emailConfig);
  const result = await transporter.sendMail({
    to: payload.to,
    from: emailConfig.from,
    replyTo: config.infoEmail.replyTo,
    subject: payload.subject,
    text: payload.body,
  });

  return {
    messageId: result.messageId,
    accepted: Array.isArray(result.accepted) ? result.accepted.map(String) : [],
    rejected: Array.isArray(result.rejected) ? result.rejected.map(String) : [],
  };
}

export async function verifyInfoEmailSmtp(): Promise<void> {
  const emailConfig = requireInfoEmailSmtpConfig();
  const transporter = createEmailTransporter(emailConfig);
  await transporter.verify();
}
