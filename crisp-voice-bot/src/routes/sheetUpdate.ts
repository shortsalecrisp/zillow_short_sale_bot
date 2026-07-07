import { Router, type NextFunction, type Request, type Response } from "express";
import { config } from "../lib/config";
import { logger } from "../lib/logger";
import { updateVoiceLeadRow } from "../lib/updateVoiceLeadRow";
import type { SheetUpdateRequest } from "../types";

const router = Router();

function requestToken(req: Request): string | undefined {
  const header = req.header("X-Crisp-Token")?.trim();
  const bodyToken = typeof req.body?.token === "string" ? req.body.token.trim() : undefined;
  return header || bodyToken;
}

router.post("/", async (req: Request, res: Response, next: NextFunction) => {
  try {
  const payload = req.body as SheetUpdateRequest;
  const expectedToken = config.googleAppsScript.token;

  if (expectedToken && requestToken(req) !== expectedToken) {
    logger.warn("Direct sheet update rejected", {
      reason: "bad_token",
      rowNumber: payload.rowNumber,
    });

    res.status(401).json({ ok: false, error: "unauthorized" });
    return;
  }

  if (!Number.isInteger(payload.rowNumber) || Number(payload.rowNumber) < 2) {
    res.status(400).json({ ok: false, error: "rowNumber must be a sheet row number greater than 1" });
    return;
  }

  logger.info("Direct sheet-update route received structured payload", {
    mode: "direct_google_sheets",
    copyPayload: JSON.stringify(payload),
    rowNumber: payload.rowNumber,
    sheetName: payload.sheetName,
    callResult: payload.callResult,
    responseStatus: payload.responseStatus,
    liveTransferRequested: payload.liveTransferRequested,
    callbackRequested: payload.callbackRequested,
    callbackTime: payload.callbackTime,
    voiceNotes: payload.voiceNotes,
  });

  const fieldsWritten = await updateVoiceLeadRow(Number(payload.rowNumber), payload);

  res.status(200).json({
    ok: true,
    mode: "direct_google_sheets",
    rowNumber: payload.rowNumber,
    fieldsWritten,
  });
  } catch (error) {
    next(error);
  }
});

export default router;
