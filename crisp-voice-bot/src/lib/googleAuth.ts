import { authenticate } from "@google-cloud/local-auth";
import fs from "node:fs/promises";
import path from "node:path";
import { google } from "googleapis";
import type { AuthClient, OAuth2Client } from "google-auth-library";
import { logger } from "./logger";

const SCOPES = ["https://www.googleapis.com/auth/spreadsheets"];
const CONFIG_DIR = path.resolve(process.cwd(), "config");
const CREDENTIALS_PATH = path.join(CONFIG_DIR, "google-oauth-client.json");
const TOKEN_PATH = path.join(CONFIG_DIR, "google-token.json");

type OAuthClientConfig = {
  client_id: string;
  client_secret: string;
};

type OAuthCredentialsFile = {
  installed?: OAuthClientConfig;
  web?: OAuthClientConfig;
};

type SavedToken = {
  type: "authorized_user";
  client_id: string;
  client_secret: string;
  refresh_token: string;
};

type ServiceAccountCredentials = {
  client_email?: string;
  private_key?: string;
  type?: string;
};

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

function parseServiceAccountEnvValue(raw: string): ServiceAccountCredentials {
  const trimmed = raw.trim();
  const decoded = trimmed.startsWith("{") ? trimmed : Buffer.from(trimmed, "base64").toString("utf8");
  const credentials = JSON.parse(decoded) as ServiceAccountCredentials;

  if (credentials.private_key) {
    credentials.private_key = credentials.private_key.replace(/\\n/g, "\n");
  }

  return credentials;
}

function loadServiceAccountCredentialsFromEnv(): ServiceAccountCredentials | undefined {
  const names = ["GOOGLE_SERVICE_ACCOUNT_JSON", "GCP_SERVICE_ACCOUNT_JSON", "GOOGLE_CREDENTIALS_JSON"];

  for (const name of names) {
    const raw = process.env[name]?.trim();
    if (!raw) {
      continue;
    }

    try {
      const credentials = parseServiceAccountEnvValue(raw);
      logger.info("Using Google Sheets auth mode: service_account_env", {
        envVar: name,
        clientEmail: credentials.client_email,
      });
      return credentials;
    } catch (error) {
      throw new Error(
        `${name} must contain a service account JSON object or base64-encoded JSON. ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  return undefined;
}

async function loadSavedCredentials(): Promise<OAuth2Client | undefined> {
  if (!(await fileExists(TOKEN_PATH))) {
    return undefined;
  }

  const token = JSON.parse(await fs.readFile(TOKEN_PATH, "utf8")) as SavedToken;
  return google.auth.fromJSON(token) as OAuth2Client;
}

async function saveCredentials(client: OAuth2Client): Promise<void> {
  const rawCredentials = await fs.readFile(CREDENTIALS_PATH, "utf8");
  const credentials = JSON.parse(rawCredentials) as OAuthCredentialsFile;
  const oauthClient = credentials.installed ?? credentials.web;

  if (!oauthClient) {
    throw new Error("OAuth client JSON must include an installed or web client");
  }

  if (!client.credentials.refresh_token) {
    throw new Error("OAuth login did not return a refresh token");
  }

  await fs.mkdir(CONFIG_DIR, { recursive: true });
  const token: SavedToken = {
    type: "authorized_user",
    client_id: oauthClient.client_id,
    client_secret: oauthClient.client_secret,
    refresh_token: client.credentials.refresh_token,
  };

  await fs.writeFile(TOKEN_PATH, JSON.stringify(token, null, 2));
}

export async function getGoogleAuthClient(): Promise<AuthClient> {
  const serviceAccountCredentials = loadServiceAccountCredentialsFromEnv();

  if (serviceAccountCredentials) {
    const client = google.auth.fromJSON(serviceAccountCredentials) as AuthClient & {
      scopes?: string | string[];
      createScoped?: (scopes: string[]) => AuthClient;
    };

    if (typeof client.createScoped === "function") {
      return client.createScoped(SCOPES);
    }

    client.scopes = SCOPES;
    return client;
  }

  const savedClient = await loadSavedCredentials();

  if (savedClient) {
    logger.info("Using Google Sheets auth mode: local_oauth_token", {
      tokenPath: TOKEN_PATH,
    });
    return savedClient;
  }

  if (!(await fileExists(CREDENTIALS_PATH))) {
    throw new Error(
      `Missing OAuth client JSON at ${CREDENTIALS_PATH}. Place your Desktop OAuth client file there before running Sheets writeback.`,
    );
  }

  logger.info("Starting Google Sheets local OAuth consent flow", {
    credentialsPath: CREDENTIALS_PATH,
    tokenPath: TOKEN_PATH,
    scopes: SCOPES,
  });

  const client = (await authenticate({
    scopes: SCOPES,
    keyfilePath: CREDENTIALS_PATH,
  })) as unknown as OAuth2Client;

  await saveCredentials(client);

  logger.info("Google Sheets OAuth token saved", {
    tokenPath: TOKEN_PATH,
  });

  return client;
}
