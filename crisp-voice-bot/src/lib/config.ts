import dotenv from "dotenv";

dotenv.config();

function readEnv(name: string, fallback?: string): string {
  const value = process.env[name]?.trim() || fallback;

  if (!value || value.trim() === "") {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value.trim();
}

function readOptionalEnv(name: string): string | undefined {
  const value = process.env[name]?.trim();
  return value || undefined;
}

function readBoolean(name: string, fallback = false): boolean {
  const value = process.env[name];

  if (value === undefined || value.trim() === "") {
    return fallback;
  }

  return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function readPort(): number {
  const raw = process.env.PORT ?? "3000";
  const port = Number(raw);

  if (!Number.isInteger(port) || port <= 0 || port > 65535) {
    throw new Error(`PORT must be a valid TCP port. Received: ${raw}`);
  }

  return port;
}

function readOptionalNumber(name: string): number | undefined {
  const raw = readOptionalEnv(name);

  if (!raw) {
    return undefined;
  }

  const value = Number(raw);

  if (!Number.isInteger(value)) {
    throw new Error(`${name} must be an integer. Received: ${raw}`);
  }

  return value;
}

function stripTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}

function readLiveTransferForceResult(): "none" | "accept" | "fail" {
  const value = readEnv("LIVE_TRANSFER_FORCE_RESULT", "none").toLowerCase();

  if (value === "none" || value === "accept" || value === "fail") {
    return value;
  }

  throw new Error(`LIVE_TRANSFER_FORCE_RESULT must be one of: none, accept, fail. Received: ${value}`);
}

function readVoiceProvider(): "telnyx" | "elevenlabs" {
  const value = readEnv("VOICE_PROVIDER", "telnyx").toLowerCase();

  if (value === "telnyx" || value === "elevenlabs") {
    return value;
  }

  throw new Error(`VOICE_PROVIDER must be one of: telnyx, elevenlabs. Received: ${value}`);
}

const telnyxCallerId = readEnv("TELNYX_CALLER_ID");

export const config = {
  port: readPort(),
  baseUrl: stripTrailingSlash(readEnv("BASE_URL", "http://localhost:3000")),
  voiceProvider: readVoiceProvider(),
  testMode: readBoolean("TEST_MODE", true),
  liveTransferTestMode: readBoolean("LIVE_TRANSFER_TEST_MODE", true),
  liveTransferNumber: readEnv("LIVE_TRANSFER_NUMBER", "+19542053205"),
  liveTransferForceResult: readLiveTransferForceResult(),
  googleAppsScript: {
    webhookUrl: readOptionalEnv("GOOGLE_APPS_SCRIPT_WEBHOOK_URL"),
    token: readOptionalEnv("GOOGLE_APPS_SCRIPT_TOKEN"),
  },
  googleSheets: {
    sheetId: readEnv(
      "GOOGLE_SHEETS_SPREADSHEET_ID",
      readOptionalEnv("GOOGLE_SHEET_ID") ?? "12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70",
    ),
    tabName: readEnv("GOOGLE_SHEETS_TAB_NAME", "ShortSaleLeads"),
  },
  emailAlerts: {
    to: readOptionalEnv("ALERT_EMAIL_TO"),
    from: readOptionalEnv("ALERT_EMAIL_FROM"),
    smtpHost: readOptionalEnv("SMTP_HOST"),
    smtpPort: readOptionalNumber("SMTP_PORT"),
    smtpUser: readOptionalEnv("SMTP_USER"),
    smtpPass: readOptionalEnv("SMTP_PASS"),
    sendCallTranscripts: readBoolean("CALL_TRANSCRIPT_EMAILS_ENABLED", true),
  },
  elevenLabs: {
    apiKey: readOptionalEnv("ELEVENLABS_API_KEY"),
    baseUrl: stripTrailingSlash(readEnv("ELEVENLABS_BASE_URL", "https://api.elevenlabs.io")),
    agentName: readEnv("ELEVENLABS_AGENT_NAME", "Emmy"),
    agentId: readOptionalEnv("ELEVENLABS_AGENT_ID"),
    branchId: readOptionalEnv("ELEVENLABS_BRANCH_ID"),
    agentPhoneNumberId: readOptionalEnv("ELEVENLABS_AGENT_PHONE_NUMBER_ID"),
    toolSecret: readOptionalEnv("ELEVENLABS_TOOL_SECRET"),
    voiceName: readEnv("ELEVENLABS_VOICE_NAME", "Liza"),
    voiceId: readOptionalEnv("ELEVENLABS_VOICE_ID"),
    ttsModel: readEnv("ELEVENLABS_TTS_MODEL", "eleven_v3_conversational"),
    primaryLlm: readEnv("ELEVENLABS_PRIMARY_LLM", "gpt-4o"),
    timezone: readEnv("ELEVENLABS_AGENT_TIMEZONE", "Eastern Time (US)"),
  },
  yoniAlertSms: {
    enabled: readBoolean("YONI_ALERT_SMS_ENABLED", false),
    destinationNumber: readEnv("YONI_ALERT_DESTINATION_NUMBER", "+19542053205"),
  },
  telnyx: {
    apiKey: readEnv("TELNYX_API_KEY"),
    callerId: telnyxCallerId,
    connectionId: readEnv("TELNYX_CONNECTION_ID"),
    outboundVoiceProfileId: readEnv("TELNYX_OUTBOUND_VOICE_PROFILE_ID"),
    messagingProfileId: readOptionalEnv("TELNYX_MESSAGING_PROFILE_ID"),
    alertFromNumber: readOptionalEnv("TELNYX_ALERT_FROM_NUMBER") ?? telnyxCallerId,
  },
  testDestinationNumber: readEnv("TEST_DESTINATION_NUMBER"),
};

export const telnyxWebhookUrl = `${config.baseUrl}/telnyx/webhook`;
