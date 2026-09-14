import { Router, type NextFunction, type Request, type Response } from "express";
import { config } from "../lib/config";
import {
  beginElevenLabsLiveTransferAttempt,
  buildElevenLabsCallContextKey,
  completeElevenLabsLiveTransferAttempt,
  getElevenLabsConversationIdByCallContext,
  getLatestElevenLabsCallContext,
} from "../lib/elevenLabsCallContext";
import { requestElevenLabsLiveTransferApproval } from "../lib/elevenLabsLiveTransferApproval";
import {
  buildVoiceContactOutcomeUpdates,
  classifyElevenLabsNotInterested,
  getElevenLabsNotInterestedEvidence,
  isElevenLabsContactEvidenceBound,
  persistVoiceContactOutcome,
  processPostCallOutcomeFromConversationId,
} from "../lib/elevenLabsPostCall";
import {
  assertValidElevenLabsConversationId,
  fetchElevenLabsConversationAudio,
  verifyElevenLabsPlaybackSignature,
} from "../lib/elevenLabsPlayback";
import {
  buildElevenLabsCallbackRequestResponse,
  buildElevenLabsInformationRequestResponse,
  buildElevenLabsLiveTransferResponse,
  buildElevenLabsContactOutcomeResponse,
} from "../lib/elevenLabsRequestResponse";
import { logger } from "../lib/logger";
import { sendCallbackEmail } from "../lib/sendCallbackEmail";
import { postSheetUpdate } from "../lib/sheetUpdateClient";
import { createElevenLabsTerminalRouter } from "./elevenLabsTerminal";

const router = Router();
router.use("/conversation-control", createElevenLabsTerminalRouter(config.elevenLabs.toolSecret));

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
  if (callResult === "do_not_call") {
    return "Do not call";
  }

  if (callResult === "answered_not_interested") {
    return "Not interested";
  }

  if (callResult === "deferred_contact") {
    return "Will follow up when ready";
  }

  if (callResult === "already_working_with_negotiator") {
    return "Already working with negotiator";
  }

  if (callResult === "not_short_sale") {
    return "Not a short sale";
  }

  if (callResult === "warm_transfer_completed") {
    return "Warm transfer accepted";
  }

  if (callResult === "information_requested") {
    return "Information requested - handoff ready";
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

function normalizedText(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function isHandoffReadyCallback(conversationSummary: string): boolean {
  const summary = normalizedText(conversationSummary);

  return (
    summary.includes("handoff-ready") ||
    summary.includes("handoff ready") ||
    summary.includes("interested callback") ||
    summary.includes("expressed interest") ||
    summary.includes("showed interest") ||
    summary.includes("is interested") ||
    summary.includes("sounds interested") ||
    summary.includes("wants to talk to yoni") ||
    summary.includes("asked to talk to yoni") ||
    summary.includes("wants yoni to call") ||
    summary.includes("asked for yoni to call") ||
    summary.includes("needs help") ||
    summary.includes("could use help")
  );
}

function buildCallbackResponseStatus(callbackTime: string, handoffReady: boolean): string {
  const baseStatus = buildVoiceResponseStatus("callback_requested", callbackTime);
  return handoffReady ? `${baseStatus} - handoff ready` : baseStatus;
}

router.get("/playback/:conversationId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const conversationId = req.params.conversationId;
    const signature = typeof req.query.sig === "string" ? req.query.sig : undefined;

    assertValidElevenLabsConversationId(conversationId);

    if (!verifyElevenLabsPlaybackSignature(conversationId, signature)) {
      throw new ElevenLabsValidationError("Invalid playback link", 401);
    }

    const audio = await fetchElevenLabsConversationAudio(conversationId);
    res.setHeader("Content-Type", audio.contentType);
    res.setHeader("Content-Disposition", `inline; filename="${conversationId}.mp3"`);
    res.setHeader("Cache-Control", "private, max-age=3600");

    audio.stream.on("error", (error: Error) => {
      logger.error("ElevenLabs playback stream failed", {
        conversationId,
        message: error.message,
        stack: error.stack,
      });

      if (!res.headersSent) {
        next(error);
        return;
      }

      res.destroy(error);
    });

    audio.stream.pipe(res);
  } catch (error) {
    next(error);
  }
});

function normalizeText(value: string): string {
  return value.toLowerCase().replace(/\s+/g, " ").trim();
}

function looksLikeNotShortSale(value: string): boolean {
  const text = normalizeText(value);
  return (
    /\b(?:not|isn't|isnt|wasn't|wasnt)\s+(?:actually\s+)?(?:a\s+)?short sale\b/.test(text) ||
    /\b(?:not|isn't|isnt|wasn't|wasnt)\s+(?:actually\s+)?(?:a\s+)?short-sale\b/.test(text) ||
    /\b(?:do not|don't|dont|does not|doesn't|doesnt)\s+have\s+(?:a\s+)?short sale\b/.test(text) ||
    /\bno\s+short sale\b/.test(text) ||
    /\bnot\s+involved\s+in\s+(?:a\s+)?short sale\b/.test(text) ||
    /\b(?:bought|purchased)\s+(?:it|the property|this property|that property)?\s*(?:from|through)\s+the bank\b/.test(text)
  );
}

function looksLikeAlreadyHasShortSaleHelp(value: string): boolean {
  const text = normalizeText(value);
  return (
    /\b(?:already\s+)?(?:have|has|got)\s+(?:a\s+|an\s+|the\s+|my\s+|our\s+)?(?:short sale\s+)?(?:negotiator|attorney|lawyer|specialist)\b/.test(
      text,
    ) ||
    /\b(?:already\s+)?(?:working|work)\s+with\s+(?:a\s+|an\s+|the\s+|my\s+|our\s+)?(?:short sale\s+)?(?:negotiator|attorney|lawyer|specialist)\b/.test(
      text,
    ) ||
    /\b(?:negotiator|attorney|lawyer|specialist)\s+(?:is\s+)?(?:already\s+)?handling\b/.test(text) ||
    /\b(?:already\s+)?(?:have|has|got)\s+(?:someone|somebody)\s+handling\b/.test(text) ||
    /\b(?:someone|somebody)\s+(?:is\s+)?(?:already\s+)?handling\b/.test(text)
  );
}

function looksLikeDeferredContact(value: string): boolean {
  return normalizedText(value).startsWith("deferred contact:");
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
  conversationId: string;
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
    conversationId: readString(body, "conversationId") || readString(body, "conversation_id"),
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
    conversationId: latestContext.conversationId ?? payload.conversationId,
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

      res.status(200).json(buildElevenLabsLiveTransferResponse("pending"));
      return;
    }

    if (liveTransferState === "accepted") {
      logger.info("ElevenLabs live transfer already approved; returning transfer-now instructions", {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        phone: payload.phone,
        listingAddress: payload.listingAddress,
      });

      res.status(200).json(buildElevenLabsLiveTransferResponse("accepted"));
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

      res.status(200).json(buildElevenLabsLiveTransferResponse(liveTransferState));
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

      res.status(200).json(buildElevenLabsLiveTransferResponse(approval.status));
      return;
    }

    res.status(200).json(buildElevenLabsLiveTransferResponse(approval.status));
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
    const handoffReady = isHandoffReadyCallback(payload.conversationSummary);
    const conversationId =
      payload.conversationId ||
      getElevenLabsConversationIdByCallContext({
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
      });

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
          responseStatus: buildCallbackResponseStatus(payload.callbackTime, handoffReady),
          leadStatusCode: handoffReady ? "G" : "Y",
          callbackRequested: callbackRequested === false ? "" : "yes",
          callbackTime: payload.callbackTime,
          voiceNotes: payload.conversationSummary,
        }),
    );

    if (conversationId) {
      logger.info("ElevenLabs callback email deferred until post-call transcript is available", {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        callbackTime: payload.callbackTime,
        conversationId,
      });
    } else {
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
    }

    logger.info("ElevenLabs callback requested tool handled", {
      rowNumber: payload.rowNumber,
      agentName: payload.agentName,
      phone: payload.phone,
      listingAddress: payload.listingAddress,
      callbackTime: payload.callbackTime,
      conversationId,
      handoffReady,
    });

    res.status(200).json(buildElevenLabsCallbackRequestResponse(payload.callbackTime));
  } catch (error) {
    next(error);
  }
});

router.post("/tool/information-requested", async (req: Request, res: Response, next: NextFunction) => {
  try {
    verifyToolSecret(req);
    const payload = applyLatestCallContextIfNeeded(readLeadPayload(req.body));
    const conversationId =
      payload.conversationId ||
      getElevenLabsConversationIdByCallContext({
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
      });
    const outcome = buildVoiceResponseStatus("information_requested");

    queueElevenLabsBackgroundTask(
      "ElevenLabs information request sheet update",
      {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        callAttemptNumber: payload.callAttemptNumber,
      },
      () =>
        postSheetUpdate({
          rowNumber: payload.rowNumber,
          callAttemptNumber: payload.callAttemptNumber,
          callResult: "information_requested",
          responseStatus: outcome,
          leadStatusCode: "G",
          callbackRequested: "",
          callbackTime: "",
          liveTransferRequested: "",
          liveTransferCompleted: "",
          voiceNotes: payload.conversationSummary,
        }),
    );

    if (conversationId) {
      logger.info("ElevenLabs information request email deferred until post-call transcript is available", {
        rowNumber: payload.rowNumber,
        agentName: payload.agentName,
        conversationId,
      });
    } else {
      queueElevenLabsBackgroundTask(
        "ElevenLabs information request email",
        {
          rowNumber: payload.rowNumber,
          agentName: payload.agentName,
        },
        () =>
          sendCallbackEmail({
            agentName: payload.agentName,
            phone: payload.phone,
            email: payload.email,
            listingAddress: payload.listingAddress,
            rowNumber: payload.rowNumber,
            subject: `NEW LEAD 🔥 - INFORMATION REQUEST - ${payload.agentName}`,
            handoffType: "Information Request",
            conversationDescription: payload.conversationSummary,
          }),
      );
    }

    logger.info("ElevenLabs information requested tool handled", {
      rowNumber: payload.rowNumber,
      agentName: payload.agentName,
      email: payload.email,
      conversationId,
    });

    res.status(200).json(buildElevenLabsInformationRequestResponse(payload.email));
  } catch (error) {
    next(error);
  }
});

router.post("/tool/not-interested", async (req: Request, res: Response, next: NextFunction) => {
  try {
    verifyToolSecret(req);
    const payload = applyLatestCallContextIfNeeded(readLeadPayload(req.body));

    const knownConversationId = getElevenLabsConversationIdByCallContext({
      rowNumber: payload.rowNumber,
      callAttemptNumber: payload.callAttemptNumber,
    });
    const conversationId = knownConversationId || payload.conversationId;
    let conversation: Awaited<ReturnType<typeof getElevenLabsNotInterestedEvidence>> | undefined;
    if (conversationId) {
      try {
        const evidence = await getElevenLabsNotInterestedEvidence(conversationId);
        if (isElevenLabsContactEvidenceBound(evidence, {
          rowNumber: payload.rowNumber,
          callAttemptNumber: payload.callAttemptNumber,
          matchesKnownCallContext: knownConversationId === conversationId,
        })) {
          conversation = evidence;
        } else {
          logger.warn("ElevenLabs not-interested evidence did not match this lead and attempt; terminal review required", {
            rowNumber: payload.rowNumber,
            callAttemptNumber: payload.callAttemptNumber,
            conversationId,
          });
        }
      } catch {
        logger.warn("ElevenLabs not-interested caller evidence unavailable; terminal review required", {
          rowNumber: payload.rowNumber,
          conversationId,
        });
      }
    }
    const callResult = classifyElevenLabsNotInterested(conversation);
    let persistenceStatus: "confirmed" | "unconfirmed" = "unconfirmed";
    try {
      await persistVoiceContactOutcome({
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        ...buildVoiceContactOutcomeUpdates(callResult, conversation),
        voiceNotes: `${callResult === "call_ended_by_request"
          ? "CALL ENDED BY REQUEST: caller asked to end the current call only. "
          : ""}Tool-provided summary (not caller evidence): ${payload.conversationSummary}`,
      });
      persistenceStatus = "confirmed";
    } catch {
      logger.error("ElevenLabs terminal contact write unconfirmed; manual review required, no retry authorized", {
        rowNumber: payload.rowNumber,
        callAttemptNumber: payload.callAttemptNumber,
        conversationId,
        callResult,
      });
    }

    logger.info("ElevenLabs not interested tool handled", {
      rowNumber: payload.rowNumber,
      agentName: payload.agentName,
      phone: payload.phone,
      listingAddress: payload.listingAddress,
      callResult,
      persistenceStatus,
    });

    res.status(200).json(buildElevenLabsContactOutcomeResponse(callResult, persistenceStatus));
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
      async () => {
        await processPostCallOutcomeFromConversationId(conversationId);
      },
    );
  }

  res.status(200).json({ ok: true });
});

export default router;
