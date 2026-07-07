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

function readNumber(name: string, fallback: number): number {
  const raw = process.env[name];

  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }

  const value = Number(raw);
  if (!Number.isFinite(value)) {
    throw new Error(`${name} must be a number. Received: ${raw}`);
  }

  return value;
}

function stripTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}

const gmailImportSchedulerEnabled = readBoolean("GMAIL_IMPORT_SCHEDULER_ENABLED", false);

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
  voiceQueue: {
    schedulerEnabled: readBoolean("VOICE_QUEUE_SCHEDULER_ENABLED", false),
    intervalMinutes: readNumber("VOICE_QUEUE_INTERVAL_MINUTES", 15),
  },
  mailshakeSync: {
    apiKey: readOptionalEnv("MAILSHAKE_API_KEY"),
    schedulerEnabled: readBoolean("MAILSHAKE_SYNC_SCHEDULER_ENABLED", false),
    intervalMinutes: readNumber("MAILSHAKE_SYNC_INTERVAL_MINUTES", 60),
    startAtRow: readNumber("MAILSHAKE_SYNC_START_AT_ROW", 3000),
    windowSize: readNumber("MAILSHAKE_SYNC_WINDOW_SIZE", 50),
  },
  gmailImporters: {
    oauthCredentialsJson: readOptionalEnv("GMAIL_OAUTH_CREDENTIALS_JSON") ?? readOptionalEnv("GMAIL_AUTHORIZED_USER_JSON"),
    sourceAccount: readEnv("GMAIL_IMPORT_SOURCE_ACCOUNT", "ygkutler@gmail.com"),
    stateSheetName: readEnv("GMAIL_IMPORT_STATE_SHEET_NAME", "BotState"),
    calendlySchedulerEnabled: readBoolean("CALENDLY_GMAIL_IMPORT_ENABLED", gmailImportSchedulerEnabled),
    calendlyIntervalMinutes: readNumber("CALENDLY_GMAIL_IMPORT_INTERVAL_MINUTES", 15),
    calendlyLookbackDays: readNumber("CALENDLY_GMAIL_LOOKBACK_DAYS", 30),
    calendlyMaxMessages: readNumber("CALENDLY_GMAIL_MAX_MESSAGES", 25),
    calendlyExtraQueries: readOptionalEnv("CALENDLY_GMAIL_EXTRA_QUERIES"),
    calendlyWebhookUrl: readOptionalEnv("CALENDLY_GMAIL_WEBHOOK_URL"),
    calendlyWebhookSecret: readOptionalEnv("CALENDLY_WEBHOOK_SECRET") ?? readOptionalEnv("CRON_SECRET"),
    googleMarketingSchedulerEnabled: readBoolean("GOOGLE_MARKETING_ALERT_IMPORT_ENABLED", gmailImportSchedulerEnabled),
    googleMarketingIntervalMinutes: readNumber("GOOGLE_MARKETING_ALERT_IMPORT_INTERVAL_MINUTES", 60),
    googleMarketingLookbackDays: readNumber("GOOGLE_MARKETING_ALERT_LOOKBACK_DAYS", 45),
    googleMarketingMaxMessages: readNumber("GOOGLE_MARKETING_ALERT_MAX_MESSAGES", 30),
    googleMarketingBaseUrl: stripTrailingSlash(readEnv("MARKETING_PLATFORM_BASE_URL", "https://crisp-marketing-platform.vercel.app")),
    googleMarketingWebhookSecret: readOptionalEnv("GOOGLE_MARKETING_ALERT_WEBHOOK_SECRET") ?? readOptionalEnv("CRON_SECRET"),
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
    agentName: readEnv("ELEVENLABS_AGENT_NAME", "Maya"),
    agentId: readOptionalEnv("ELEVENLABS_AGENT_ID"),
    branchId: readOptionalEnv("ELEVENLABS_BRANCH_ID"),
    agentPhoneNumberId: readOptionalEnv("ELEVENLABS_AGENT_PHONE_NUMBER_ID"),
    toolSecret: readOptionalEnv("ELEVENLABS_TOOL_SECRET"),
    voiceName: readEnv("ELEVENLABS_VOICE_NAME", "Eryn"),
    voiceId: readOptionalEnv("ELEVENLABS_VOICE_ID"),
    voiceAbTestEnabled: readBoolean("ELEVENLABS_VOICE_AB_TEST_ENABLED", false),
    erynVoiceId: readEnv("ELEVENLABS_ERYN_VOICE_ID", "dMyQqiVXTU80dDl2eNK8"),
    finchVoiceId: readEnv("ELEVENLABS_FINCH_VOICE_ID", "hFskf6X0TFndppvQxiEF"),
    rachelVoiceId: readEnv("ELEVENLABS_RACHEL_VOICE_ID", "21m00Tcm4TlvDq8ikWAM"),
    bellaVoiceId: readEnv("ELEVENLABS_BELLA_VOICE_ID", "EXAVITQu4vr4xnSDxMaL"),
    ttsModel: readEnv("ELEVENLABS_TTS_MODEL", "eleven_flash_v2"),
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
