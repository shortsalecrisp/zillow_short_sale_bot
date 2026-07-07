import { google, gmail_v1 } from "googleapis";
import { OAuth2Client } from "google-auth-library";
import { config } from "./config";
import { logger } from "./logger";

type AuthorizedUserCredentials = {
  type?: string;
  client_id?: string;
  client_secret?: string;
  refresh_token?: string;
};

let gmailClient: gmail_v1.Gmail | undefined;

function parseJsonOrBase64(raw: string): AuthorizedUserCredentials {
  const trimmed = raw.trim();
  const decoded = trimmed.startsWith("{") ? trimmed : Buffer.from(trimmed, "base64").toString("utf8");
  const credentials = JSON.parse(decoded) as AuthorizedUserCredentials;

  if (!credentials.client_id || !credentials.client_secret || !credentials.refresh_token) {
    throw new Error("Gmail OAuth credentials must include client_id, client_secret, and refresh_token");
  }

  return credentials;
}

export function hasGmailOAuthCredentials(): boolean {
  return Boolean(config.gmailImporters.oauthCredentialsJson);
}

export function getGmailClient(): gmail_v1.Gmail {
  if (gmailClient) {
    return gmailClient;
  }

  const raw = config.gmailImporters.oauthCredentialsJson;
  if (!raw) {
    throw new Error("Missing GMAIL_OAUTH_CREDENTIALS_JSON for Gmail importers");
  }

  const credentials = parseJsonOrBase64(raw);
  const auth = new OAuth2Client(credentials.client_id, credentials.client_secret);
  auth.setCredentials({ refresh_token: credentials.refresh_token });

  logger.info("Using Gmail auth mode: authorized_user_env", {
    sourceAccount: config.gmailImporters.sourceAccount,
  });

  gmailClient = google.gmail({ version: "v1", auth });
  return gmailClient;
}
