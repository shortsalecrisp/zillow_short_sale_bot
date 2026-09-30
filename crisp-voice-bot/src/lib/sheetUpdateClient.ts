import axios, { AxiosError } from "axios";
import { config } from "./config";
import { logger } from "./logger";
import { updateVoiceLeadRow } from "./updateVoiceLeadRow";
import { processVoiceQueue } from "./voiceQueue";
import type { SheetUpdateRequest } from "../types";

type AppsScriptPost = (
  url: string,
  body: Record<string, unknown>,
  options: {
    timeout: number;
    headers: Record<string, string>;
  },
) => Promise<{ data?: unknown }>;

type DirectSheetUpdate = (
  rowNumber: number,
  payload: SheetUpdateRequest,
) => Promise<string[]>;

type SheetUpdateDependencies = {
  appsScriptPost?: AppsScriptPost;
  directSheetUpdate?: DirectSheetUpdate;
};

export function buildVoiceQueueRefillPayload(): Record<string, string> {
  return {
    ...(config.googleAppsScript.token ? { token: config.googleAppsScript.token } : {}),
    action: "process_voice_queue",
  };
}

function redactLargeSheetFields(payload: Record<string, unknown>): Record<string, unknown> {
  if (typeof payload.voiceNotes !== "string") {
    return payload;
  }

  return {
    ...payload,
    voiceNotes: `[redacted ${payload.voiceNotes.length} chars]`,
  };
}

function parseAppsScriptResponse(data: unknown): Record<string, unknown> | undefined {
  if (typeof data === "string") {
    try {
      const parsed = JSON.parse(data) as unknown;
      return parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? parsed as Record<string, unknown>
        : undefined;
    } catch {
      return undefined;
    }
  }

  return data && typeof data === "object" && !Array.isArray(data)
    ? data as Record<string, unknown>
    : undefined;
}

export function isAppsScriptSheetUpdateAccepted(data: unknown): boolean {
  return parseAppsScriptResponse(data)?.ok === true;
}

function summarizeAppsScriptResponse(data: unknown): Record<string, unknown> {
  const parsed = parseAppsScriptResponse(data);
  if (!parsed) {
    return { responseType: typeof data };
  }

  return {
    ok: parsed.ok,
    code: parsed.code,
    error: parsed.error,
    fieldsWritten: parsed.fieldsWritten,
  };
}

async function persistDirectSheetFallback(
  payload: SheetUpdateRequest,
  directSheetUpdate: DirectSheetUpdate,
  reason: string,
): Promise<void> {
  if (!Number.isInteger(payload.rowNumber) || Number(payload.rowNumber) < 2) {
    logger.error("Direct sheet fallback skipped because rowNumber is invalid", {
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
      callResult: payload.callResult,
      reason,
    });
    return;
  }

  try {
    const fieldsWritten = await directSheetUpdate(Number(payload.rowNumber), payload);
    logger.info("Direct sheet fallback accepted", {
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
      callResult: payload.callResult,
      reason,
      fieldsWritten,
    });
  } catch (error) {
    logger.error("Direct sheet fallback failed", {
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
      callResult: payload.callResult,
      reason,
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

export async function requestVoiceQueueRefill(context: {
  conversationId?: string;
  rowNumber?: number;
  callAttemptNumber?: number;
} = {}): Promise<void> {
  if (!config.googleAppsScript.webhookUrl) {
    logger.info("Requesting direct voice queue refill", context);
    await processVoiceQueue();
    return;
  }

  const payload = buildVoiceQueueRefillPayload();
  const logPayload = {
    ...payload,
    ...(payload.token ? { token: "[redacted]" } : {}),
  };

  logger.info("Requesting voice queue refill", {
    ...context,
    url: config.googleAppsScript.webhookUrl,
    payload: logPayload,
  });

  try {
    await axios.post(config.googleAppsScript.webhookUrl, payload, {
      timeout: 10_000,
      headers: {
        "Content-Type": "application/json",
        ...(config.googleAppsScript.token ? { "X-Crisp-Token": config.googleAppsScript.token } : {}),
      },
    });

    logger.info("Voice queue refill accepted", context);
  } catch (error) {
    if (error instanceof AxiosError) {
      logger.error("Voice queue refill failed", {
        ...context,
        status: error.response?.status,
        data: error.response?.data,
        message: error.message,
      });
      return;
    }

    logger.error("Voice queue refill failed", {
      ...context,
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

export async function postSheetUpdate(
  payload: SheetUpdateRequest,
  dependencies: SheetUpdateDependencies = {},
): Promise<void> {
  const useAppsScript = Boolean(config.googleAppsScript.webhookUrl);
  const directSheetUpdate = dependencies.directSheetUpdate ?? updateVoiceLeadRow;
  const appsScriptPost = dependencies.appsScriptPost ?? ((url, body, options) => axios.post(url, body, options));

  if (!useAppsScript) {
    if (!Number.isInteger(payload.rowNumber) || Number(payload.rowNumber) < 2) {
      logger.error("Direct sheet update skipped because rowNumber is invalid", {
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        callResult: payload.callResult,
      });
      return;
    }

    const safeLogBody = redactLargeSheetFields(payload);

    logger.info("Posting direct sheet update", {
      mode: "direct_google_sheets",
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
      callResult: payload.callResult,
      responseStatus: payload.responseStatus,
      leadStatusCode: payload.leadStatusCode,
      copyPayload: JSON.stringify(safeLogBody),
    });

    try {
      const fieldsWritten = await directSheetUpdate(Number(payload.rowNumber), payload);
      logger.info("Direct sheet update accepted", {
        mode: "direct_google_sheets",
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        callResult: payload.callResult,
        fieldsWritten,
      });
    } catch (error) {
      logger.error("Direct sheet update failed", {
        mode: "direct_google_sheets",
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        message: error instanceof Error ? error.message : String(error),
      });
    }
    return;
  }

  const url = config.googleAppsScript.webhookUrl ?? `${config.baseUrl}/sheet-update`;
  const body = config.googleAppsScript.token
    ? {
        token: config.googleAppsScript.token,
        ...payload,
      }
    : payload;
  const logBody = config.googleAppsScript.token
    ? {
        ...body,
        token: "[redacted]",
      }
    : body;
  const safeLogBody = redactLargeSheetFields(logBody);

  logger.info("Posting sheet update", {
    mode: useAppsScript ? "google_apps_script" : "local_stub",
    url,
    rowNumber: payload.rowNumber,
    callAttemptNumber: payload.callAttemptNumber,
    callResult: payload.callResult,
    responseStatus: payload.responseStatus,
    leadStatusCode: payload.leadStatusCode,
    copyPayload: JSON.stringify(safeLogBody),
  });

  try {
    const response = await appsScriptPost(url, body, {
      timeout: 10_000,
      headers: {
        "Content-Type": "application/json",
        ...(config.googleAppsScript.token ? { "X-Crisp-Token": config.googleAppsScript.token } : {}),
      },
    });

    if (!isAppsScriptSheetUpdateAccepted(response.data)) {
      logger.error("Sheet update rejected by Apps Script", {
        mode: "google_apps_script",
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        callResult: payload.callResult,
        response: summarizeAppsScriptResponse(response.data),
      });
      await persistDirectSheetFallback(payload, directSheetUpdate, "apps_script_rejected");
      return;
    }

    logger.info("Sheet update accepted", {
      mode: useAppsScript ? "google_apps_script" : "local_stub",
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
      callResult: payload.callResult,
    });
  } catch (error) {
    if (error instanceof AxiosError) {
      logger.error("Sheet update failed", {
        mode: useAppsScript ? "google_apps_script" : "local_stub",
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        status: error.response?.status,
        data: error.response?.data,
        message: error.message,
      });
      await persistDirectSheetFallback(payload, directSheetUpdate, "apps_script_http_failure");
      return;
    }

    logger.error("Sheet update failed", {
        mode: useAppsScript ? "google_apps_script" : "local_stub",
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        message: error instanceof Error ? error.message : String(error),
    });
    await persistDirectSheetFallback(payload, directSheetUpdate, "apps_script_transport_failure");
  }
}
