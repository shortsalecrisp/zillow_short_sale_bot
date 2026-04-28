import { Router, type NextFunction, type Request, type Response } from "express";
import {
  getCallState,
  markFinalOutcome,
  markTestOutcome,
  updateCallState,
  upsertCallStateFromWebhook,
} from "../lib/callState";
import { handleLiveTransfer, resolvePreConnectionMessage } from "../lib/handleLiveTransfer";
import {
  getPendingTransfer,
  getPendingTransferByYoniCallControlId,
  resolveTransferByYoniCallControlId,
  setYoniCallControlId,
} from "../lib/liveTransferStore";
import { logger } from "../lib/logger";
import { sendCallbackEmail } from "../lib/sendCallbackEmail";
import { postSheetUpdate } from "../lib/sheetUpdateClient";
import { decodeLiveTransferClientState, gatherUsingAi, gatherUsingSpeak, hangupCall, speakText } from "../lib/telnyx";
import type { CallState, SheetUpdateRequest, TelnyxWebhookEvent, TelnyxWebhookPayload, TestOutcome } from "../types";

const router = Router();
const TRANSFER_WAIT_MS = 1_500;

type PromptOutcome = {
  testOutcome: TestOutcome;
  sheetUpdate: SheetUpdateRequest;
  closingMessage: string;
};

type CloseOutcomeOptions = {
  skipClosingMessage?: boolean;
  skipReason?: string;
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function getFirstDtmfDigit(payload: TelnyxWebhookPayload): string | undefined {
  const rawDigit =
    typeof payload.digit === "string"
      ? payload.digit
      : typeof payload.digits === "string"
        ? payload.digits
        : undefined;

  return rawDigit?.slice(0, 1);
}

function buildOpeningScript(state: CallState): string {
  const firstName = state.firstName?.trim();
  const opener = firstName ? `Hey, is this ${firstName}? Hi, this is Emmy.` : "Hi, this is Emmy.";

  return `${opener} I work with Yoni Kutler. He sent you a quick text yesterday about your short sale at ${state.listingAddress}. Do you have a second right now to talk?

If you want to speak with Yoni now, press 1.
If you're not interested, press 2.
If you want him to call you back later, press 3.`;
}

function buildTransferFallbackScript(): string {
  return `Looks like he's tied up at the moment. Let's find a better time for him to call you back.

Press 1 for later today.
Press 2 for tomorrow.
Press 3 if it's not needed anymore.`;
}

function getInitialPromptOutcome(
  state: CallState,
  digit?: string,
): PromptOutcome | "transfer_requested" | "callback_time_requested" {
  if (digit === "1") {
    return "transfer_requested";
  }

  if (digit === "2") {
    return {
      testOutcome: "not_interested",
      sheetUpdate: {
        rowNumber: state.rowNumber,
        callResult: "answered_not_interested",
        responseStatus: "R",
        voiceNotes: "Test call: not interested via keypad",
      },
      closingMessage: "No problem. If anything changes in the future, just keep us in mind. Thanks.",
    };
  }

  if (digit === "3") {
    return "callback_time_requested";
  }

  return {
    testOutcome: "no_response_to_prompt",
    sheetUpdate: {
      rowNumber: state.rowNumber,
      callResult: "no_response_to_prompt",
      voiceNotes: "Test call answered but no keypad input received",
    },
    closingMessage: "No worries. We'll follow up another time. Thanks.",
  };
}

function getTransferFallbackOutcome(state: CallState, digit?: string): PromptOutcome {
  if (digit === "1") {
    return {
      testOutcome: "callback_requested",
      sheetUpdate: {
        rowNumber: state.rowNumber,
        callResult: "callback_requested",
        responseStatus: "Y",
        callbackRequested: "yes",
        callbackTime: "later_today",
        voiceNotes: "Test call: requested callback later today after transfer fallback",
      },
      closingMessage: "Perfect. We'll have Yoni give you a call later today.",
    };
  }

  if (digit === "2") {
    return {
      testOutcome: "callback_requested",
      sheetUpdate: {
        rowNumber: state.rowNumber,
        callResult: "callback_requested",
        responseStatus: "Y",
        callbackRequested: "yes",
        callbackTime: "tomorrow",
        voiceNotes: "Test call: requested callback tomorrow after transfer fallback",
      },
      closingMessage: "Sounds good. We'll have Yoni reach out tomorrow.",
    };
  }

  if (digit === "3") {
    return {
      testOutcome: "not_interested",
      sheetUpdate: {
        rowNumber: state.rowNumber,
        callResult: "answered_not_interested",
        responseStatus: "R",
        voiceNotes: "Test call: declined after transfer fallback",
      },
      closingMessage: "No worries. If anything changes in the future, just keep us in mind. Thanks.",
    };
  }

  return {
    testOutcome: "callback_unknown",
    sheetUpdate: {
      rowNumber: state.rowNumber,
      callResult: "callback_unknown",
      responseStatus: "Y",
      callbackRequested: "yes",
      voiceNotes: "Test call: transfer requested but no callback slot selected",
    },
    closingMessage: "No problem. We'll follow up with you soon.",
  };
}

function getStringFromRecord(record: Record<string, unknown> | undefined, keys: string[]): string | undefined {
  if (!record) {
    return undefined;
  }

  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return undefined;
}

function extractCallbackTime(payload: Record<string, unknown>): string | undefined {
  const result = payload.result && typeof payload.result === "object" ? (payload.result as Record<string, unknown>) : undefined;
  const parameters =
    result?.parameters && typeof result.parameters === "object"
      ? (result.parameters as Record<string, unknown>)
      : undefined;

  return (
    getStringFromRecord(result, ["callback_time", "callbackTime", "time"]) ??
    getStringFromRecord(parameters, ["callback_time", "callbackTime", "time"])
  );
}

function buildCallbackTimeConversationDescription(payload: Record<string, unknown>, callbackTime: string): string {
  const messageHistory = Array.isArray(payload.message_history) ? payload.message_history : undefined;

  if (!messageHistory?.length) {
    return `Lead requested a callback time: ${callbackTime}`;
  }

  const transcript = messageHistory
    .map((message) => {
      if (!message || typeof message !== "object") {
        return undefined;
      }

      const role = "role" in message && typeof message.role === "string" ? message.role : "unknown";
      const content = "content" in message && typeof message.content === "string" ? message.content.trim() : "";

      return content ? `${role}: ${content}` : undefined;
    })
    .filter((line): line is string => Boolean(line))
    .join(" | ");

  return transcript || `Lead requested a callback time: ${callbackTime}`;
}

function getCallbackTimeOutcome(state: CallState, callbackTime?: string, conversationDescription?: string): PromptOutcome {
  const normalizedCallbackTime = callbackTime?.trim() || "unspecified";

  return {
    testOutcome: "callback_requested",
    sheetUpdate: {
      rowNumber: state.rowNumber,
      callResult: "callback_requested",
      responseStatus: "Y",
      callbackRequested: "yes",
      callbackTime: normalizedCallbackTime,
      voiceNotes: conversationDescription ?? `Lead requested a callback time: ${normalizedCallbackTime}`,
    },
    closingMessage:
      normalizedCallbackTime === "unspecified"
        ? "Sounds good. We'll have Yoni reach out later."
        : `Sounds good. We'll have Yoni call you ${normalizedCallbackTime}.`,
  };
}

function isCallEnded(callControlId: string): boolean {
  return Boolean(getCallState(callControlId)?.hasEnded);
}

function logSkippedEndedAction(callControlId: string, action: string, reason = "call already ended"): void {
  const state = getCallState(callControlId);

  logger.info("Skipping Telnyx action because call already ended", {
    callControlId,
    action,
    reason,
    rowNumber: state?.rowNumber,
    currentStep: state?.currentStep,
    testOutcome: state?.testOutcome,
    lastEventType: state?.lastEventType,
  });
}

async function safeSpeakText(callControlId: string, message: string): Promise<boolean> {
  if (isCallEnded(callControlId)) {
    logSkippedEndedAction(callControlId, "speak");
    return false;
  }

  await speakText(callControlId, message);
  return true;
}

async function safeGatherUsingSpeak(params: {
  callControlId: string;
  message: string;
  clientState?: string;
}): Promise<boolean> {
  if (isCallEnded(params.callControlId)) {
    logSkippedEndedAction(params.callControlId, "gather_using_speak");
    return false;
  }

  await gatherUsingSpeak(params);
  return true;
}

async function safeGatherUsingAi(params: {
  callControlId: string;
  message: string;
  clientState?: string;
}): Promise<boolean> {
  if (isCallEnded(params.callControlId)) {
    logSkippedEndedAction(params.callControlId, "gather_using_ai");
    return false;
  }

  await gatherUsingAi(params);
  return true;
}

async function safeHangupCall(callControlId: string): Promise<boolean> {
  if (isCallEnded(callControlId)) {
    logSkippedEndedAction(callControlId, "hangup");
    return false;
  }

  await hangupCall(callControlId);
  updateCallState(callControlId, { hasEnded: true });
  return true;
}

function queueCallbackEmail(callControlId: string, outcome: PromptOutcome): void {
  if (outcome.sheetUpdate.callResult !== "callback_requested") {
    return;
  }

  const state = getCallState(callControlId);
  const rowNumber = outcome.sheetUpdate.rowNumber ?? state?.rowNumber;

  if (!rowNumber) {
    logger.error("Callback email failed", {
      callControlId,
      reason: "missing row number",
    });
    return;
  }

  void (async () => {
    try {
      await sendCallbackEmail({
        agentName: state?.fullName ?? "Unknown",
        phone: state?.destinationPhone ?? "Unknown",
        email: state?.email,
        listingAddress: state?.listingAddress ?? "Unknown",
        rowNumber,
        action: "Call this lead back at the requested time",
        callbackTime:
          typeof outcome.sheetUpdate.callbackTime === "string" ? outcome.sheetUpdate.callbackTime : undefined,
        conversationDescription:
          typeof outcome.sheetUpdate.voiceNotes === "string" ? outcome.sheetUpdate.voiceNotes : undefined,
        details: "The lead requested a callback during the voice bot call.",
      });

      logger.info("Callback email sent", {
        callControlId,
        rowNumber,
        fullName: state?.fullName,
        listingAddress: state?.listingAddress,
        phone: state?.destinationPhone,
      });
    } catch (error) {
      logger.error("Callback email failed", {
        callControlId,
        rowNumber,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  })();
}

async function closeCallWithOutcome(
  callControlId: string,
  outcome: PromptOutcome,
  options: CloseOutcomeOptions = {},
): Promise<void> {
  markFinalOutcome(callControlId, outcome.testOutcome, "closing_message");
  await postSheetUpdate(outcome.sheetUpdate);
  queueCallbackEmail(callControlId, outcome);

  if (options.skipClosingMessage) {
    const state = getCallState(callControlId);
    logger.info("Skipping closing message for final outcome", {
      callControlId,
      rowNumber: state?.rowNumber,
      testOutcome: outcome.testOutcome,
      reason: options.skipReason,
      hasEnded: state?.hasEnded,
      lastEventType: state?.lastEventType,
    });
    return;
  }

  const didSpeak = await safeSpeakText(callControlId, outcome.closingMessage);
  if (didSpeak) {
    updateCallState(callControlId, { currentStep: "closing_message" });
  }
}

async function handleInitialPromptResult(callControlId: string, state: CallState, digit?: string): Promise<void> {
  const outcome = getInitialPromptOutcome(state, digit);

  if (outcome === "transfer_requested") {
    markTestOutcome(callControlId, "live_transfer_requested", "live_transfer_wait");
    const didSpeak = await safeSpeakText(callControlId, "Got it. Let me check if Yoni is available now. One moment.");
    if (didSpeak) {
      updateCallState(callControlId, { currentStep: "live_transfer_wait" });
    }

    void handleLiveTransfer({
      agentName: state.fullName,
      phone: state.destinationPhone,
      listingAddress: state.listingAddress,
      rowNumber: state.rowNumber,
      callControlId,
    }).catch((error) => {
      logger.error("Live transfer handler failed", {
        callControlId,
        rowNumber: state.rowNumber,
        message: error instanceof Error ? error.message : String(error),
      });
    });
    return;
  }

  if (outcome === "callback_time_requested") {
    markTestOutcome(callControlId, "callback_requested", "callback_time_prompt");

    try {
      const didGather = await safeGatherUsingAi({
        callControlId,
        message: "Ok sure, no problem. What time should he call?",
      });

      if (didGather) {
        updateCallState(callControlId, { currentStep: "callback_time_prompt" });
      }

      logger.info("Started callback time voice gather", {
        callControlId,
        rowNumber: state.rowNumber,
        didGather,
      });

      if (didGather) {
        return;
      }
    } catch (error) {
      logger.error("Callback time voice gather failed; using unspecified callback time", {
        callControlId,
        rowNumber: state.rowNumber,
        message: error instanceof Error ? error.message : String(error),
      });
    }

    await closeCallWithOutcome(callControlId, {
      testOutcome: "callback_requested",
      sheetUpdate: {
        rowNumber: state.rowNumber,
        callResult: "callback_requested",
        responseStatus: "Y",
        callbackRequested: "yes",
        callbackTime: "unspecified",
        voiceNotes: "Callback requested, but callback time could not be captured during the test call",
      },
      closingMessage: "Sounds good. We'll have Yoni reach out later.",
    });
    return;
  }

  await closeCallWithOutcome(callControlId, outcome);
}

async function handleTransferFallbackResult(callControlId: string, state: CallState, digit?: string): Promise<void> {
  const outcome = getTransferFallbackOutcome(state, digit);
  await closeCallWithOutcome(callControlId, outcome);
}

async function handleYoniTransferWebhook(
  eventType: string,
  event: TelnyxWebhookEvent,
  callControlId: string,
): Promise<boolean> {
  const payload = event.data?.payload ?? {};
  const metadata = decodeLiveTransferClientState(payload.client_state);
  const pendingTransferByYoniCall = getPendingTransferByYoniCallControlId(callControlId);
  const pendingTransferByOriginalCall = metadata?.originalCallControlId
    ? getPendingTransfer(metadata.originalCallControlId)
    : undefined;
  const pendingTransfer = pendingTransferByYoniCall ?? pendingTransferByOriginalCall;
  const originalCallControlId = metadata?.originalCallControlId ?? pendingTransfer?.originalCallControlId;

  if (!metadata && !pendingTransfer) {
    return false;
  }

  if (metadata?.originalCallControlId && pendingTransferByOriginalCall) {
    setYoniCallControlId(metadata.originalCallControlId, callControlId);
  }

  switch (eventType) {
    case "call.initiated": {
      logger.info("Yoni live transfer call initiated", {
        originalCallControlId,
        yoniCallControlId: callControlId,
        rowNumber: metadata?.rowNumber,
      });
      return true;
    }

    case "call.answered": {
      const yoniPrompt =
        metadata?.yoniPrompt ??
        pendingTransfer?.yoniPrompt ??
        "You have a live transfer request. Press 1 to accept.";

      logger.info("Yoni answered", {
        originalCallControlId,
        yoniCallControlId: callControlId,
        rowNumber: metadata?.rowNumber,
      });

      await gatherUsingSpeak({
        callControlId,
        message: yoniPrompt,
        clientState: payload.client_state ?? undefined,
      });

      logger.info("Started Yoni live transfer acceptance prompt", {
        originalCallControlId,
        yoniCallControlId: callControlId,
        rowNumber: metadata?.rowNumber,
      });
      return true;
    }

    case "call.dtmf.received": {
      const digit = getFirstDtmfDigit(payload);

      if (digit === "1") {
        const didResolve = resolveTransferByYoniCallControlId(callControlId, "yes");
        logger.info("Yoni accepted transfer via DTMF event", {
          originalCallControlId,
          yoniCallControlId: callControlId,
          digit,
          didResolve,
        });
        return true;
      }

      if (digit === "2") {
        const didResolve = resolveTransferByYoniCallControlId(callControlId, "no");
        logger.info("Yoni declined transfer via DTMF event", {
          originalCallControlId,
          yoniCallControlId: callControlId,
          digit,
          didResolve,
        });
        return true;
      }

      logger.info("Ignoring unsupported Yoni transfer DTMF digit", {
        originalCallControlId,
        yoniCallControlId: callControlId,
        digit,
      });
      return true;
    }

    case "call.gather.ended": {
      const digit = getFirstDtmfDigit(payload);

      if (!pendingTransfer) {
        logger.info("Ignoring Yoni gather end because transfer is no longer pending", {
          originalCallControlId,
          yoniCallControlId: callControlId,
          digit,
          gatherStatus: payload.status,
        });
        return true;
      }

      if (digit === "1") {
        logger.info("Yoni accepted transfer", {
          originalCallControlId,
          yoniCallControlId: callControlId,
          digit,
        });
        resolveTransferByYoniCallControlId(callControlId, "yes");
        return true;
      }

      logger.info(digit === "2" ? "Yoni declined transfer" : "Yoni did not respond to transfer prompt", {
        originalCallControlId,
        yoniCallControlId: callControlId,
        digit,
        gatherStatus: payload.status,
      });
      resolveTransferByYoniCallControlId(callControlId, "no");

      await hangupCall(callControlId).catch((error) => {
        logger.error("Unable to hang up Yoni call after declined transfer", {
          originalCallControlId,
          yoniCallControlId: callControlId,
          message: error instanceof Error ? error.message : String(error),
        });
      });
      return true;
    }

    case "call.hangup": {
      logger.info("Yoni live transfer call hung up", {
        originalCallControlId,
        yoniCallControlId: callControlId,
        hangupCause: payload.hangup_cause,
        hadPendingTransfer: Boolean(pendingTransfer),
      });
      if (pendingTransfer) {
        resolveTransferByYoniCallControlId(callControlId, "no");
      }
      return true;
    }

    case "call.bridged": {
      logger.info("Yoni live transfer call bridged", {
        originalCallControlId,
        yoniCallControlId: callControlId,
      });
      return true;
    }

    default: {
      logger.info("Yoni live transfer webhook ignored", {
        eventType,
        originalCallControlId,
        yoniCallControlId: callControlId,
      });
      return true;
    }
  }
}

router.post("/", async (req: Request, res: Response, next: NextFunction) => {
  const event = req.body as TelnyxWebhookEvent;
  const eventType = event.data?.event_type ?? "unknown";
  const payload = event.data?.payload ?? {};
  const callControlId = payload.call_control_id;

  logger.info("Received Telnyx webhook", {
    eventType,
    eventId: event.data?.id,
    callControlId,
    callLegId: payload.call_leg_id,
    callSessionId: payload.call_session_id,
    from: payload.from,
    to: payload.to,
    digit: payload.digit,
    digits: payload.digits,
    gatherStatus: payload.status,
    hangupCause: payload.hangup_cause,
  });

  if (!callControlId) {
    logger.warn("Ignoring Telnyx webhook without call_control_id", { eventType });
    res.status(200).json({ ok: true, ignored: true });
    return;
  }

  try {
    const handledYoniTransferEvent = await handleYoniTransferWebhook(eventType, event, callControlId);

    if (handledYoniTransferEvent) {
      res.status(200).json({ ok: true });
      return;
    }

    switch (eventType) {
      case "call.initiated": {
        const state = upsertCallStateFromWebhook(callControlId, event, "initiated");
        logger.info("Call initiated", {
          callControlId,
          rowNumber: state.rowNumber,
          fullName: state.fullName,
          listingAddress: state.listingAddress,
        });
        break;
      }

      case "call.answered": {
        const state = upsertCallStateFromWebhook(callControlId, event, "answered");

        const didGather = await safeGatherUsingSpeak({
          callControlId,
          message: buildOpeningScript(state),
          clientState: payload.client_state ?? undefined,
        });

        if (didGather) {
          updateCallState(callControlId, {
            currentStep: "initial_prompt",
            answeredAt: state.answeredAt ?? new Date().toISOString(),
          });
        }

        logger.info("Started Emmy opening script and initial DTMF gather", {
          callControlId,
          rowNumber: state.rowNumber,
          firstName: state.firstName,
          listingAddress: state.listingAddress,
          didGather,
        });
        break;
      }

      case "call.dtmf.received": {
        const state = upsertCallStateFromWebhook(callControlId, event, getCallState(callControlId)?.currentStep ?? "ignored");
        logger.info("Received DTMF during active prompt", {
          callControlId,
          rowNumber: state.rowNumber,
          currentStep: state.currentStep,
          digits: payload.digits,
        });
        break;
      }

      case "call.gather.ended": {
        const existingState = getCallState(callControlId);

        if (existingState?.hasProcessedFinalOutcome) {
          logger.info("Ignoring duplicate gather result after outcome already handled", {
            callControlId,
            rowNumber: existingState?.rowNumber,
            currentStep: existingState?.currentStep,
            testOutcome: existingState?.testOutcome,
            lastEventType: existingState?.lastEventType,
            gatherStatus: payload.status,
            digits: payload.digits,
          });
          break;
        }

        if (existingState?.currentStep === "live_transfer_wait") {
          logger.info("Ignoring gather result while live transfer approval is pending", {
            callControlId,
            rowNumber: existingState.rowNumber,
            currentStep: existingState.currentStep,
            testOutcome: existingState.testOutcome,
            gatherStatus: payload.status,
            digits: payload.digits,
          });
          break;
        }

        const activeStep = existingState?.currentStep === "transfer_fallback_prompt" ? "transfer_fallback_prompt" : "initial_prompt";
        const state = upsertCallStateFromWebhook(callControlId, event, activeStep);
        const digit = typeof payload.digits === "string" ? payload.digits.slice(0, 1) : undefined;
        const didGatherEndBecauseCallHungUp = payload.status === "call_hangup";
        const shouldSkipClosingMessage = didGatherEndBecauseCallHungUp && !digit;

        if (didGatherEndBecauseCallHungUp) {
          updateCallState(callControlId, {
            hasEnded: true,
            lastEventType: "call.gather.ended",
          });
        }

        if (activeStep === "transfer_fallback_prompt") {
          const outcome = getTransferFallbackOutcome(state, digit);
          await closeCallWithOutcome(callControlId, outcome, {
            skipClosingMessage: shouldSkipClosingMessage,
            skipReason: shouldSkipClosingMessage ? "gather ended because call hung up without digits" : undefined,
          });
        } else {
          await handleInitialPromptResult(callControlId, state, digit);
        }

        logger.info("Handled gather result", {
          callControlId,
          rowNumber: state.rowNumber,
          digit,
          promptStep: activeStep,
          gatherStatus: payload.status,
          hasEnded: didGatherEndBecauseCallHungUp,
          skippedClosingMessage: shouldSkipClosingMessage,
        });
        break;
      }

      case "call.ai_gather.ended": {
        const existingState = getCallState(callControlId);

        if (existingState?.hasProcessedFinalOutcome) {
          logger.info("Ignoring duplicate AI gather result after outcome already handled", {
            callControlId,
            rowNumber: existingState.rowNumber,
            currentStep: existingState.currentStep,
            testOutcome: existingState.testOutcome,
            lastEventType: existingState.lastEventType,
          });
          break;
        }

        const state = upsertCallStateFromWebhook(callControlId, event, "callback_time_prompt");
        const callbackTime = extractCallbackTime(payload);
        const conversationDescription = buildCallbackTimeConversationDescription(
          payload,
          callbackTime ?? "unspecified",
        );
        const outcome = getCallbackTimeOutcome(state, callbackTime, conversationDescription);

        logger.info("Captured callback time from voice gather", {
          callControlId,
          rowNumber: state.rowNumber,
          callbackTime: outcome.sheetUpdate.callbackTime,
          hasMessageHistory: Array.isArray(payload.message_history) && payload.message_history.length > 0,
        });

        await closeCallWithOutcome(callControlId, outcome);
        break;
      }

      case "call.speak.ended": {
        const currentStep = getCallState(callControlId)?.currentStep ?? "ignored";
        const state = upsertCallStateFromWebhook(callControlId, event, currentStep);

        if (state.currentStep === "transfer_wait") {
          await sleep(TRANSFER_WAIT_MS);
          const didGather = await safeGatherUsingSpeak({
            callControlId,
            message: buildTransferFallbackScript(),
            clientState: payload.client_state ?? undefined,
          });
          if (didGather) {
            updateCallState(callControlId, { currentStep: "transfer_fallback_prompt" });
          }

          logger.info("Started transfer fallback prompt", {
            callControlId,
            rowNumber: state.rowNumber,
            didGather,
          });
          break;
        }

        if (state.currentStep === "pre_connection_message") {
          logger.info("Pre-connection message completed", {
            callControlId,
            rowNumber: state.rowNumber,
          });
          resolvePreConnectionMessage(callControlId);
          break;
        }

        if (state.currentStep === "closing_message") {
          const didHangup = await safeHangupCall(callControlId);
          updateCallState(callControlId, { currentStep: "completed" });

          logger.info("Hung up after closing message", {
            callControlId,
            rowNumber: state.rowNumber,
            testOutcome: state.testOutcome,
            hasEnded: true,
            didHangup,
          });
        } else {
          logger.info("Speak ended for non-terminal step", {
            callControlId,
            rowNumber: state.rowNumber,
            currentStep: state.currentStep,
          });
        }
        break;
      }

      case "call.hangup": {
        const existingStep = getCallState(callControlId)?.currentStep;
        const state = upsertCallStateFromWebhook(callControlId, event, existingStep === "completed" ? "completed" : "hangup");
        updateCallState(callControlId, {
          hasEnded: true,
          lastEventType: "call.hangup",
        });
        logger.info("Call hung up", {
          callControlId,
          rowNumber: state.rowNumber,
          currentStep: state.currentStep,
          testOutcome: state.testOutcome,
          hangupCause: payload.hangup_cause,
          hasEnded: true,
        });
        break;
      }

      default: {
        const state = upsertCallStateFromWebhook(callControlId, event, getCallState(callControlId)?.currentStep ?? "ignored");
        logger.info("Telnyx event ignored by transfer-ready handler", {
          eventType,
          callControlId,
          rowNumber: state.rowNumber,
          currentStep: state.currentStep,
        });
        break;
      }
    }

    res.status(200).json({ ok: true });
  } catch (error) {
    updateCallState(callControlId, { currentStep: "error" });
    next(error);
  }
});

export default router;
