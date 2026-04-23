import { Router, type Request, type Response } from "express";
import { logger } from "../lib/logger";
import type { SheetUpdateRequest } from "../types";

const router = Router();

router.post("/", (req: Request, res: Response) => {
  const payload = req.body as SheetUpdateRequest;

  // TODO: Remove this legacy local stub once direct Google Sheets writeback is fully settled.
  // TODO: Validate that rowNumber maps to the expected sheet row before writing real data.
  logger.info("Local sheet-update stub received structured payload", {
    mode: "local_stub",
    copyPayload: JSON.stringify(payload),
    rowNumber: payload.rowNumber,
    sheetName: payload.sheetName,
    callResult: payload.callResult,
    responseStatus: payload.responseStatus,
    liveTransferRequested: payload.liveTransferRequested,
    callbackRequested: payload.callbackRequested,
    callbackTime: payload.callbackTime,
    voiceNotes: payload.voiceNotes,
    rawPayload: payload,
  });

  res.status(200).json({ ok: true, mode: "local_stub" });
});

export default router;
