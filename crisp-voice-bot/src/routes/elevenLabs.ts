import { Router, type NextFunction, type Request, type Response } from "express";
import { config } from "../lib/config";
import {
  beginElevenLabsLiveTransferAttempt,
  buildElevenLabsCallContextKey,
  completeElevenLabsLiveTransferAttempt,
  getLatestElevenLabsCallContext,
} from "../lib/elevenLabsCallContext";
import { requestElevenLabsLiveTransferApproval } from "../lib/elevenLabsLiveTransferApproval";
import { handleElevenLabsPostCallWebhook } from "../lib/elevenLabsPostCall";
import { logger } from "../lib/logger";
import { sendCallbackEmail } from "../lib/sendCallbackEmail";
import { postSheetUpdate } from "../lib/sheetUpdateClient";

const router = Router();

class ElevenLabsValidationError extends Error {
  public readonly statusCode: number;

  constructor(message: string, statusCode = 400) {
    super(message);
    this.statusCode = statusCode;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function verifyToolSecret(req: Request): void {
  if (!config.elevenLabs.toolSecret) {
    return;
  }

  const authorization = req.header("authorization");
  const bearerToken = authorization?.startsWith("Bearer ") ? authorization.slice("Bearer ".length).trim() : undefined;
  const headerToken = req.header("x-crisp-elevenlabs-secret");
  const bodyToken = isRecord(req.body) && typeof req.body.token === "string" ? req.body.token : undefined;
  const providedToken = bearerToken ?? headerToken ?? bodyToken;

  if (providedToken !== config.elevenLabs.toolSecret) {
    throw new ElevenLabsValidationError("Invalid ElevenLabs tool secret", 401);
  }
}

function readString(body: Record<string, unknown>, key: string, fallback = ""): string {
  const value = body[key];

  if (typeof value !== "string") {
    return fallback;
  }

  return value.trim() || fallback;
}

function readBoolean(body: Record<string, unknown>, key: string): boolean | undefined {
  const value = body[key];

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes"].includes(normalized)) {
      return true;
    }
    if (["false", "0", "no", ""].includes(normalized)) {
      return false;
    }
  }

  return undefined;
}

function readRowNumber(body: Record<string, unknown>): number {
  const rowNumber = body.rowNumber;

  if (typeof rowNumber === "number" && Number.isInteger(rowNumber) && rowNumber > 0) {
    return rowNumber;
  }

  if (typeof rowNumber === "string") {
    const parsed = Number(rowNumber);
    if (Number.isInteger(parsed) && parsed > 0) {
      return parsed;
    }
  }

  throw new ElevenLabsValidationError("rowNumber must be a positive integer");
}

function readPositiveInteger(body: Record<string, unknown>, key: string, fallback: number): number {
  const value = body[key];

  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  if (typeof value === "number" && Number.isInteger(value) && value > 0) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isInteger(parsed) && parsed > 0) {
      return parsed;
    }
  }

  return fallback;
}

function buildVoiceResponseStatus(callResult: string, callbackTime?: string): string {
  if (callResult === "answered_not_interested") {
    return "Not interested";
  }

  if (callResult === "not_short_sale") {
    return "Not a short sale";
  }

  if (callResult === "warm_transfer_completed") {
    return "Warm transfer accepted";
  }

  if (callResult === "callback_requested") {
    const normalizedCallbackTime = callbackTime?.trim();
    if (!normalizedCallbackTime || normalizedCallbackTime.toLowerCase() === "asap") {
      return "Requested callback ASAP";
    }

    return `Requested callback at ${normalizedCallbackTime}`;
  }

  return callResult;
}

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function looksLikeNotShortSale(value: string): boolean {
  const text = normalizeText(value);
  return (
    /\b(?:not|isn't|isnt|wasn't|wasnt)\s+(?:actually\s+)?(?:a\s+)?short sale\b/.test(text) ||
    /\b(?:not|isn't|isnt|wasn't|wasnt)\s+(?:actually\s+)?(?:a\s+)?short-sale\b/.test(text)
  );
}

function formatCallbackConfirmationTime(callbackTime: string): string {
  const trimmed = callbackTime.trim();
  const normalized = trimmed.toLowerCase();

  if (!trimmed) {
    return "later";
  }

  if (normalized === "asap") {
    return "ASAP";
  }

  if (normalized === "in an hour" || normalized === "within an hour") {
    return "in about an hour";
  }

  if (normalized === "later_today" || normalized === "later today") {
    return "later today";
  }

  if (normalized === "tomorrow") {
    return "tomorrow";
  }

  if (normalized.startsWith("in ")) {
    return trimmed;
  }

  return `at ${trimmed}`;
}

function readLeadPayload(body: unknown): {
  rowNumber: number;
  callAttemptNumber: number;
  agentName: string;
  phone: string;
  email: string;
  listingAddress: string;
  callbackTime: string;
  conversationSummary: string;
} {
  if (!isRecord(body)) {
    throw new ElevenLabsValidationError("Request body must be a JSON object");
  }

  return {
    rowNumber: readRowNumber(body),
    callAttemptNumber: readPositiveInteger(body, "callAttemptNumber", 1),
    agentName: readString(body, "agentName", "Unknown"),
    phone: readString(body, "phone", "Unknown"),
    email: readString(body, "email"),
    listingAddress: readString(body, "listingAddress", "Unknown"),
    callbackTime: readString(body, "callbackTime", "unspecified"),
    conversationSummary: readString(body, "conversationSummary", "No conversation summary provided."),
  };
}

function looksLikeElevenLabsPlaceholder(payload: ReturnType<typeof readLeadPayload>): boolean {
  return (
    payload.rowNumber <= 1 ||
    payload.phone === "+15551234567" ||
    payload.listingAddress.toLowerCase() === "123 main st" ||
    payload.agentName.toLowerCase() === "john"
  );
}

function applyLatestCallContextIfNeeded(payload: ReturnType<typeof readLeadPayload>): ReturnType<typeof readLeadPayload> {
  if (!looksLikeElevenLabsPlaceholder(payload)) {
    return payload;
  }

  const latestContext = getLatestElevenLabsCallContext();
  if (!latestContext) {
    return payload;
  }

  logger.info("Replacing ElevenLabs placeholder lead fields with latest call context", {
    incomingRowNumber: payload.rowNumber,
    rowNumber: latestContext.rowNumber,
    incomingAgentName: payload.agentName,
    agentName: latestContext.fullName,
  });

  return {
    ...payload,
    rowNumber: latestContext.rowNumber,
    callAttemptNumber: latestContext.callAttemptNumber,
    agentName: latestContext.fullName,
    phone: latestContext.dialedPhone,
    email: latestContext.email ?? payload.email,
    listingAddress: latestContext.listingAddress,
  };
}

function queueElevenLabsBackgroundTask(taskName: string, metadata: Record<string, unknown>, task: () => Promise<unknown>): void {
  void task()
    .then(() => {
      logger.info(`${taskName} completed`, metadata);
    })
    .catch((error) => {
      logger.error(`${taskName} failed`, {
        ...metadata,
        message: error instanceof Error ? error.message : String(error),
      });
    });
}

router.post("/tool/live-transfer-requested", async (req: Request, res: Response, next: NextFunction) => {
  let liveTransferContextKey: string | undefined;

  try {
    verifyToolSecret(req);
    const payload = applyLatestCallContextIfNeeded(readLeadPayload(req.body));
    liveTransferContextKey = buildElevenLabsCallContextKey({
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
    });
    const liveTransferState = beginElevenLabsLiveTransferAttempt(liveTransferContextKey);

    if (liveTransferState === "pending") {
      logger.info("Ignoring duplicate ElevenLabs live transfer request", {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        phone: payload.phone,
        listingAddress: payload.listingAddress,
        liveTransferState,
      });

      res.status(200).json({
        ok: true,
        intent: "live_transfer",
        transferApproved: false,
        approvalStatus: "in_progress",
        nextAction:
          "A live transfer attempt is already in progress. Do not call live_transfer_requested again. Do not fall back yet. Do not call callback_requested yet. If you absolutely need to say something while waiting, say exactly once: Hold on one minute, let me just try him one other place. Then stay quiet and wait for the original transfer result.",
      });
      return;
    }

    if (liveTransferState === "accepted") {
      logger.info("ElevenLabs live transfer already approved; returning transfer-now instructions", {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        phone: payload.phone,
        listingAddress: payload.listingAddress,
      });

      res.status(200).json({
        ok: true,
        intent: "live_transfer",
        transferApproved: true,
        approvalStatus: "accepted",
      });
      return;
    }

    if (liveTransferState === "declined" || liveTransferState === "timeout" || liveTransferState === "call_failed") {
      logger.info("ElevenLabs live transfer already completed with fallback outcome", {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        phone: payload.phone,
        listingAddress: payload.listingAddress,
        liveTransferState,
      });

      res.status(200).json({
        ok: true,
        intent: "live_transfer",
        transferApproved: false,
        approvalStatus: liveTransferState,
        nextAction:
          "Tell the caller exactly: Sorry, he was not available right now, but I will text him and ask him to call you back ASAP. Is that ok? Then wait for the caller to respond. If they say yes, sure, ok, sounds good, or anything similar, call callback_requested with callbackTime=asap. After that tool succeeds, say exactly: Ok, thanks, sounds good. Bye! Then immediately call end_call. Do not call transfer_to_number.",
      });
      return;
    }

    void postSheetUpdate({
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
      callResult: "live_transfer_requested",
      liveTransferRequested: "yes",
      voiceNotes: payload.conversationSummary || "ElevenLabs: live transfer requested",
    }).catch((error) => {
      logger.error("ElevenLabs live transfer sheet update failed; continuing warm handoff", {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        message: error instanceof Error ? error.message : String(error),
      });
    });

    logger.info("ElevenLabs live transfer tool handled", {
      rowNumber: payload.rowNumber,
      agentName: payload.agentName,
      phone: payload.phone,
      listingAddress: payload.listingAddress,
      transferNumber: config.liveTransferNumber,
    });

    const approval = await requestElevenLabsLiveTransferApproval({
      rowNumber: payload.rowNumber,
      agentName: payload.agentName,
      phone: payload.phone,
      listingAddress: payload.listingAddress,
      liveTransferNumber: config.liveTransferNumber,
    });

    completeElevenLabsLiveTransferAttempt(approval.status, liveTransferContextKey);

    if (approval.status !== "accepted") {
      logger.info("ElevenLabs live transfer approval did not complete; returning fallback instructions", {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        phone: payload.phone,
        listingAddress: payload.listingAddress,
        transferNumber: config.liveTransferNumber,
        approvalStatus: approval.status,
      });

      res.status(409).json({
        ok: false,
        intent: "live_transfer",
        transferApproved: false,
        approvalStatus: approval.status,
        nextAction:
          "Tell the caller exactly: Sorry, he was not available right now, but I will text him and ask him to call you back ASAP. Is that ok? Then wait for the caller to respond. If they say yes, sure, ok, sounds good, or anything similar, call callback_requested with callbackTime=asap. After that tool succeeds, say exactly: Ok, thanks, sounds good. Bye! Then immediately call end_call. If they ask another question, answer briefly, then call callback_requested with callbackTime=asap and end the call. Do not call transfer_to_number.",
      });
      return;
    }

    res.status(200).json({
      ok: true,
      intent: "live_transfer",
      transferApproved: true,
      approvalStatus: approval.status,
    });
  } catch (error) {
    completeElevenLabsLiveTransferAttempt("call_failed", liveTransferContextKey);
    next(error);
  }
});

router.post("/tool/callback-requested", async (req: Request, res: Response, next: NextFunction) => {
  try {
    verifyToolSecret(req);
    const payload = applyLatestCallContextIfNeeded(readLeadPayload(req.body));
    const callbackRequested = isRecord(req.body) ? readBoolean(req.body, "callbackRequested") : undefined;

    queueElevenLabsBackgroundTask(
      "ElevenLabs callback sheet update",
      {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        callAttemptNumber: payload.callAttemptNumber,
        callbackTime: payload.callbackTime,
      },
      () =>
        postSheetUpdate({
          rowNumber: payload.rowNumber,
          callAttemptNumber: payload.callAttemptNumber,
          callResult: "callback_requested",
          responseStatus: buildVoiceResponseStatus("callback_requested", payload.callbackTime),
          leadStatusCode: "Y",
          callbackRequested: callbackRequested === false ? "" : "yes",
          callbackTime: payload.callbackTime,
          voiceNotes: payload.conversationSummary,
        }),
    );

    queueElevenLabsBackgroundTask(
      "ElevenLabs callback email",
      {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        callbackTime: payload.callbackTime,
      },
      () =>
        sendCallbackEmail({
          agentName: payload.agentName,
          phone: payload.phone,
          email: payload.email,
          listingAddress: payload.listingAddress,
          rowNumber: payload.rowNumber,
          callbackTime: payload.callbackTime,
          conversationDescription: payload.conversationSummary,
        }),
    );

    logger.info("ElevenLabs callback requested tool handled", {
      rowNumber: payload.rowNumber,
      agentName: payload.agentName,
      phone: payload.phone,
      listingAddress: payload.listingAddress,
      callbackTime: payload.callbackTime,
    });

    res.status(200).json({
      ok: true,
      intent: "callback_requested",
      callbackTime: payload.callbackTime,
      nextAction:
        payload.callbackTime.toLowerCase() === "asap"
          ? "Say exactly: Ok, I set that up and I'll have Yoni reach out to you ASAP. Before I let you go, is there anything else you need from me? Then wait for the caller's answer. If they say no, all set, thanks, bye, or anything similar, say exactly: Ok thanks, bye. Then immediately call end_call."
          : `Say exactly: Ok, I set up the callback with Yoni and I'll have him reach out to you ${formatCallbackConfirmationTime(
              payload.callbackTime,
            )}. Before I let you go, is there anything else you need from me? Then wait for the caller's answer. If they say no, all set, thanks, bye, or anything similar, say exactly: Ok thanks, bye. Then immediately call end_call.`,
    });
  } catch (error) {
    next(error);
  }
});

router.post("/tool/not-interested", async (req: Request, res: Response, next: NextFunction) => {
  try {
    verifyToolSecret(req);
    const payload = applyLatestCallContextIfNeeded(readLeadPayload(req.body));

    queueElevenLabsBackgroundTask(
      "ElevenLabs not interested sheet update",
      {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        callAttemptNumber: payload.callAttemptNumber,
      },
      () => {
        const callResult = looksLikeNotShortSale(payload.conversationSummary)
          ? "not_short_sale"
          : "answered_not_interested";

        return postSheetUpdate({
          rowNumber: payload.rowNumber,
          callAttemptNumber: payload.callAttemptNumber,
          callResult,
          responseStatus: buildVoiceResponseStatus(callResult),
          leadStatusCode: "R",
          voiceNotes: payload.conversationSummary || "ElevenLabs: lead not interested",
        });
      },
    );

    logger.info("ElevenLabs not interested tool handled", {
      rowNumber: payload.rowNumber,
      agentName: payload.agentName,
      phone: payload.phone,
      listingAddress: payload.listingAddress,
    });

    res.status(200).json({
      ok: true,
      intent: "not_interested",
      nextAction:
        "Say exactly: Ok, thanks for letting me know. If anything ever changes in the future and you're looking for some additional help with these deals, please just keep us in mind. Thanks. Then pause briefly. If the caller says anything short like ok, thanks, or will do, say exactly: Ok, bye. Then immediately call end_call. If the caller says nothing, after a brief beat say exactly: Ok, bye. Then immediately call end_call. Do not pitch again. Do not reopen the conversation.",
    });
  } catch (error) {
    next(error);
  }
});

router.post("/post-call", async (req: Request, res: Response) => {
  const conversationId =
    isRecord(req.body) && isRecord(req.body.data) && typeof req.body.data.conversation_id === "string"
      ? req.body.data.conversation_id
      : undefined;

  logger.info("Received ElevenLabs post-call webhook", {
    type: isRecord(req.body) ? req.body.type : undefined,
    conversationId,
  });

  if (conversationId) {
    queueElevenLabsBackgroundTask(
      "ElevenLabs post-call webhook outcome",
      { conversationId },
      () => handleElevenLabsPostCallWebhook(conversationId),
    );
  }

  res.status(200).json({ ok: true });
});

export default router;
