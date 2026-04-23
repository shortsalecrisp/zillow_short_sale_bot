import axios, { AxiosError } from "axios";
import { config } from "./config";
import { logger } from "./logger";
import type { SheetUpdateRequest } from "../types";

export async function postSheetUpdate(payload: SheetUpdateRequest): Promise<void> {
  const useAppsScript = Boolean(config.googleAppsScript.webhookUrl);
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

  logger.info("Posting sheet update", {
    mode: useAppsScript ? "google_apps_script" : "local_stub",
    url,
    rowNumber: payload.rowNumber,
    callAttemptNumber: payload.callAttemptNumber,
    callResult: payload.callResult,
    responseStatus: payload.responseStatus,
    leadStatusCode: payload.leadStatusCode,
    copyPayload: JSON.stringify(logBody),
  });

  try {
    await axios.post(url, body, {
      timeout: 10_000,
      headers: {
        "Content-Type": "application/json",
        ...(config.googleAppsScript.token ? { "X-Crisp-Token": config.googleAppsScript.token } : {}),
      },
    });

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
      return;
    }

    logger.error("Sheet update failed", {
        mode: useAppsScript ? "google_apps_script" : "local_stub",
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        message: error instanceof Error ? error.message : String(error),
    });
  }
}
