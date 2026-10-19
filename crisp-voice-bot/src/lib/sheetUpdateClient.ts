import axios, { AxiosError } from "axios";
import { config } from "./config";
import { logger } from "./logger";
import { updateVoiceLeadRow } from "./updateVoiceLeadRow";
import { processVoiceQueue } from "./voiceQueue";
import { isRetryableVoiceBotResult } from "./voiceSheet";
import type { SheetUpdateRequest } from "../types";

type AppsScriptPost = (
  url: string,
  body: Record<string, unknown>,
  options: {
    timeout: number;
    headers: Record<string, string>;
  },
) => Promise<{ data?: unknown }>;

class RetrySchedulingReadbackError extends Error {}

type DirectSheetUpdate = (
  rowNumber: number,
  payload: SheetUpdateRequest,
) => Promise<string[]>;

type SheetUpdateDependencies = {
  appsScriptPost?: AppsScriptPost;
  directSheetUpdate?: DirectSheetUpdate;
};

type VoiceQueueRefillDependencies = {
  processQueue?: () => Promise<unknown>;
};

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

function needsCallFailureSchedulingReconciliation(payload: SheetUpdateRequest, data: unknown): boolean {
  if (!["call_failed_before_completion", "call_failed_before_completion_retryable"].includes(payload.callResult ?? "")) {
    return false;
  }

  const fieldsWritten = parseAppsScriptResponse(data)?.fieldsWritten;
  if (!Array.isArray(fieldsWritten)) {
    return true;
  }

  const written = new Set(fieldsWritten.filter((field): field is string => typeof field === "string"));
  return ["AD:call_eligible", "AE:call_time_bucket", "AF:call_scheduled_for"]
    .some((field) => !written.has(field));
}

function needsRetrySchedulingReadback(payload: SheetUpdateRequest): boolean {
  return Number(payload.callAttemptNumber) === 1 && isRetryableVoiceBotResult(payload.callResult);
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
  failClosed = false,
): Promise<void> {
  if (!Number.isInteger(payload.rowNumber) || Number(payload.rowNumber) < 2) {
    logger.error("Direct sheet fallback skipped because rowNumber is invalid", {
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
      callResult: payload.callResult,
      reason,
    });
    if (failClosed) throw new RetrySchedulingReadbackError("Direct sheet readback requires a valid row number");
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
    if (failClosed) {
      throw new RetrySchedulingReadbackError(error instanceof Error ? error.message : String(error));
    }
  }
}

export async function requestVoiceQueueRefill(context: {
  conversationId?: string;
  rowNumber?: number;
  callAttemptNumber?: number;
} = {}, dependencies: VoiceQueueRefillDependencies = {}): Promise<void> {
  const processQueue = dependencies.processQueue ?? (() => processVoiceQueue());
  // The backend owns capacity and local-window guards. An Apps Script transport
  // acknowledgement does not prove that a free slot was actually checked.
  logger.info("Requesting direct voice queue refill", context);
  const result = await processQueue();
  logger.info("Direct voice queue refill completed", { ...context, result });
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

    // Apps Script acceptance is not owner-surface proof that a retryable first
    // attempt actually retained its next-slot schedule. The direct writer
    // rereads the row, reapplies the idempotent scheduling fields and verifies
    // their persisted values before returning.
    if (needsRetrySchedulingReadback(payload)) {
      await persistDirectSheetFallback({
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        callResult: payload.callResult,
      }, directSheetUpdate, "apps_script_retry_scheduling_readback", true);
    } else if (needsCallFailureSchedulingReconciliation(payload, response.data)) {
      await persistDirectSheetFallback({
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        callResult: payload.callResult,
      }, directSheetUpdate, "apps_script_missing_terminal_scheduling_cleanup");
    }
  } catch (error) {
    if (error instanceof RetrySchedulingReadbackError) throw error;
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
