import { authenticate } from "@google-cloud/local-auth";
import fs from "node:fs/promises";
import path from "node:path";
import { google } from "googleapis";
import type { OAuth2Client } from "google-auth-library";
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

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
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

export async function getGoogleAuthClient(): Promise<OAuth2Client> {
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
