import nodemailer from "nodemailer";
import { config } from "./config";

export type EmailAlertConfig = {
  to: string;
  from: string;
  smtpHost: string;
  smtpPort: number;
  smtpUser: string;
  smtpPass: string;
};

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export function formatPhoneNumber(phone: string): string {
  const digits = phone.replace(/\D/g, "");
  const normalized = digits.length === 11 && digits.startsWith("1") ? digits.slice(1) : digits;

  if (normalized.length !== 10) {
    return phone;
  }

  return `(${normalized.slice(0, 3)}) ${normalized.slice(3, 6)}-${normalized.slice(6)}`;
}

export function requireEmailConfig(): EmailAlertConfig {
  const { to, from, smtpHost, smtpPort, smtpUser, smtpPass } = config.emailAlerts;
  const missing = [
    ["ALERT_EMAIL_TO", to],
    ["ALERT_EMAIL_FROM", from],
    ["SMTP_HOST", smtpHost],
    ["SMTP_PORT", smtpPort],
    ["SMTP_USER", smtpUser],
    ["SMTP_PASS", smtpPass],
  ]
    .filter(([, value]) => !value)
    .map(([name]) => name);

  if (missing.length > 0) {
    throw new Error(`Missing email alert config: ${missing.join(", ")}`);
  }

  return {
    to: to as string,
    from: from as string,
    smtpHost: smtpHost as string,
    smtpPort: smtpPort as number,
    smtpUser: smtpUser as string,
    smtpPass: smtpPass as string,
  };
}

export function createEmailTransporter(emailConfig: EmailAlertConfig) {
  return nodemailer.createTransport({
    host: emailConfig.smtpHost,
    port: emailConfig.smtpPort,
    secure: emailConfig.smtpPort === 465,
    auth: {
      user: emailConfig.smtpUser,
      pass: emailConfig.smtpPass,
    },
  });
}
