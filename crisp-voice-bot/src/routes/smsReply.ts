import { Router, type Request, type Response } from "express";
import { getMostRecentPendingTransferCallControlId, resolveTransfer, type LiveTransferReply } from "../lib/liveTransferStore";
import { logger } from "../lib/logger";

const router = Router();

type SmsReplyBody = {
  from?: unknown;
  message?: unknown;
  callControlId?: unknown;
  id?: unknown;
};

function normalizeMessage(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function readOptionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function getReplyResult(message: string): LiveTransferReply | undefined {
  if (message === "yes") {
    return "yes";
  }

  if (message === "no") {
    return "no";
  }

  return undefined;
}

router.post("/", (req: Request, res: Response) => {
  try {
    const body = req.body as SmsReplyBody;
    const from = readOptionalString(body.from);
    const rawMessage = readOptionalString(body.message) ?? "";
    const normalizedMessage = normalizeMessage(body.message);
    const requestedCallControlId = readOptionalString(body.callControlId) ?? readOptionalString(body.id);

    logger.info("SMS reply received", {
      from,
      message: rawMessage,
      normalizedMessage,
      requestedCallControlId,
    });

    const result = getReplyResult(normalizedMessage);

    if (!result) {
      logger.info("Ignoring SMS reply because it is not a transfer response", {
        from,
        normalizedMessage,
      });
      res.status(200).json({ ok: true, ignored: true });
      return;
    }

    const callControlId = requestedCallControlId ?? getMostRecentPendingTransferCallControlId();

    if (!callControlId) {
      logger.info("Ignoring SMS reply because no pending transfer exists", {
        from,
        result,
      });
      res.status(200).json({ ok: true, ignored: true, reason: "no_pending_transfer" });
      return;
    }

    const resolved = resolveTransfer(callControlId, result);

    res.status(200).json({
      ok: true,
      resolved,
      callControlId,
      result,
    });
  } catch (error) {
    logger.error("SMS reply webhook failed safely", {
      message: error instanceof Error ? error.message : String(error),
    });
    res.status(200).json({ ok: false, ignored: true });
  }
});

export default router;
