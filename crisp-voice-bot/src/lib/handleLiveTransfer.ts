import { getCallState, markFinalOutcome, updateCallState } from "./callState";
import { config } from "./config";
import { formatWhisperMessage } from "./formatWhisperMessage";
import { clearTransfer, getPendingTransfer, setYoniCallControlId } from "./liveTransferStore";
import { logger } from "./logger";
import { sendCallbackEmail } from "./sendCallbackEmail";
import { postSheetUpdate } from "./sheetUpdateClient";
import { bridgeCalls, hangupCall, placeLiveTransferApprovalCall, speakText } from "./telnyx";
import { waitForLiveTransferReply } from "./waitForLiveTransferReply";

const LIVE_TRANSFER_APPROVAL_TIMEOUT_MS = 20_000;
const PRE_CONNECTION_MESSAGE = "Perfect, I’ve got Yoni on the line now. Connecting you.";
const PRE_CONNECTION_MESSAGE_TIMEOUT_MS = 12_000;

const preConnectionMessageResolvers = new Map<string, () => void>();

type LiveTransferInput = {
  agentName: string;
  phone: string;
  listingAddress: string;
  rowNumber: number;
  callControlId: string;
};

function waitForPreConnectionMessage(callControlId: string): Promise<void> {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      preConnectionMessageResolvers.delete(callControlId);
      logger.warn("Pre-connection message completion timed out; continuing to bridge", {
        callControlId,
      });
      resolve();
    }, PRE_CONNECTION_MESSAGE_TIMEOUT_MS);

    preConnectionMessageResolvers.set(callControlId, () => {
      clearTimeout(timeout);
      preConnectionMessageResolvers.delete(callControlId);
      resolve();
    });
  });
}

export function resolvePreConnectionMessage(callControlId: string): boolean {
  const resolve = preConnectionMessageResolvers.get(callControlId);

  if (!resolve) {
    return false;
  }

  resolve();
  return true;
}

async function sendFallbackCallback({
  agentName,
  phone,
  listingAddress,
  rowNumber,
  callControlId,
}: LiveTransferInput): Promise<void> {
  markFinalOutcome(callControlId, "callback_requested", "closing_message");

  const sheetUpdate = {
    rowNumber,
    callResult: "callback_requested",
    responseStatus: "Y",
    callbackRequested: "yes",
    callbackTime: "asap",
    voiceNotes: "Live transfer missed: call back ASAP",
  };

  void postSheetUpdate(sheetUpdate)
    .then(() =>
      sendCallbackEmail({
        agentName,
        phone,
        listingAddress,
        rowNumber,
        subject: "🔥 ASAP Callback Needed - Live Transfer Missed",
        action: "Call this lead back ASAP",
        callbackTime: "ASAP",
        conversationDescription:
          "The lead requested a live transfer. Yoni did not answer or declined the bridge call. Call this lead back ASAP.",
        details: "The lead asked to be transferred live, but Yoni did not answer or declined the transfer call.",
      }),
    )
    .then(() => {
      logger.info("Callback email sent", {
        callControlId,
        rowNumber,
        fullName: agentName,
        listingAddress,
        phone,
      });
    })
    .catch((error) => {
      logger.error("Callback writeback or email failed", {
        callControlId,
        rowNumber,
        message: error instanceof Error ? error.message : String(error),
      });
    });

  if (getCallState(callControlId)?.hasEnded) {
    logger.info("Skipping live transfer fallback message because call already ended", {
      callControlId,
      rowNumber,
    });
    return;
  }

  updateCallState(callControlId, { currentStep: "closing_message" });
  await speakText(
    callControlId,
    "Sorry, I guess he's not available, but I'll send him a text to call you back ASAP.",
  );
}

async function simulateTransferSuccess(callControlId: string, rowNumber: number): Promise<void> {
  logger.info("TEST MODE: Transfer would have been executed", {
    callControlId,
    destinationNumber: config.liveTransferNumber,
  });

  if (getCallState(callControlId)?.hasEnded) {
    logger.info("Skipping test mode transfer confirmation because call already ended", {
      callControlId,
      rowNumber,
    });
    return;
  }

  markFinalOutcome(callControlId, "live_transfer_requested", "closing_message");
  logger.info("Pre-connection message started", {
    callControlId,
    rowNumber,
    simulated: true,
  });
  await speakText(callControlId, PRE_CONNECTION_MESSAGE);
}

function buildYoniPrompt(whisperMessage: string): string {
  const transferSubject = whisperMessage.replace(/^Live transfer from\s+/i, "");
  return `You have a live transfer request from ${transferSubject}. Press 1 to accept.`;
}

async function executeAcceptedTransfer({
  callControlId,
  rowNumber,
  liveTransferNumber,
}: {
  callControlId: string;
  rowNumber: number;
  liveTransferNumber: string;
}): Promise<void> {
  if (getCallState(callControlId)?.hasEnded) {
    logger.info("Skipping transfer because call already ended", {
      callControlId,
      rowNumber,
      to: liveTransferNumber,
    });
    return;
  }

  if (config.liveTransferTestMode) {
    await simulateTransferSuccess(callControlId, rowNumber);
    const yoniCallControlId = getPendingTransfer(callControlId)?.yoniCallControlId;

    if (yoniCallControlId) {
      await hangupCall(yoniCallControlId).catch((error) => {
        logger.error("Unable to hang up Yoni test call after simulated transfer", {
          callControlId,
          yoniCallControlId,
          message: error instanceof Error ? error.message : String(error),
        });
      });
    }

    clearTransfer(callControlId);
    logger.info("Transfer accepted", {
      callControlId,
      rowNumber,
      to: liveTransferNumber,
      simulated: true,
    });
    return;
  }

  try {
    const yoniCallControlId = getPendingTransfer(callControlId)?.yoniCallControlId;

    if (!yoniCallControlId) {
      throw new Error("Missing Yoni call control ID for bridge");
    }

    logger.info("REAL TRANSFER EXECUTING", {
      callControlId,
      yoniCallControlId,
      destinationNumber: liveTransferNumber,
    });
    updateCallState(callControlId, { currentStep: "pre_connection_message" });
    logger.info("Pre-connection message started", {
      callControlId,
      rowNumber,
      yoniCallControlId,
    });
    await speakText(callControlId, PRE_CONNECTION_MESSAGE);
    await waitForPreConnectionMessage(callControlId);

    if (getCallState(callControlId)?.hasEnded) {
      logger.info("Skipping bridge because caller leg ended during pre-connection message", {
        callControlId,
        rowNumber,
        yoniCallControlId,
      });
      clearTransfer(callControlId);
      return;
    }

    logger.info("Bridging calls", {
      originalCallControlId: callControlId,
      yoniCallControlId,
    });
    await bridgeCalls(callControlId, yoniCallControlId);
    clearTransfer(callControlId);
    updateCallState(callControlId, { currentStep: "completed" });
    logger.info("Transfer accepted", {
      callControlId,
      rowNumber,
      to: liveTransferNumber,
      yoniCallControlId,
    });
  } catch (error) {
    logger.error("Transfer failed; falling back to callback", {
      callControlId,
      rowNumber,
      message: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}

export async function handleLiveTransfer(input: LiveTransferInput): Promise<void> {
  const { agentName, phone, listingAddress, rowNumber, callControlId } = input;
  const liveTransferNumber = config.liveTransferNumber;
  const whisperMessage = formatWhisperMessage({ agentName, listingAddress });
  const yoniPrompt = buildYoniPrompt(whisperMessage);

  logger.info("Live transfer requested", {
    callControlId,
    rowNumber,
    agentName,
    phone,
    listingAddress,
  });

  await postSheetUpdate({
    rowNumber,
    callResult: "live_transfer_requested",
    liveTransferRequested: "yes",
    voiceNotes: "Test call: live transfer requested via keypad",
  });

  logger.info("Whisper message prepared", {
    callControlId,
    rowNumber,
    whisperMessage,
  });

  const replyPromise = waitForLiveTransferReply({
    callControlId,
    timeoutMs: LIVE_TRANSFER_APPROVAL_TIMEOUT_MS,
    yoniPrompt,
  });

  try {
    const yoniCall = await placeLiveTransferApprovalCall({
      to: liveTransferNumber,
      metadata: {
        kind: "live_transfer_yoni",
        originalCallControlId: callControlId,
        rowNumber,
        agentName,
        listingAddress,
        whisperMessage,
        yoniPrompt,
      },
    });
    const yoniCallControlId = yoniCall.data?.call_control_id;

    if (yoniCallControlId) {
      setYoniCallControlId(callControlId, yoniCallControlId);
    }

    logger.info("Calling Yoni for live transfer", {
      callControlId,
      yoniCallControlId,
      rowNumber,
      to: liveTransferNumber,
    });
  } catch (error) {
    logger.error("Yoni live transfer call failed; falling back to callback", {
      callControlId,
      rowNumber,
      message: error instanceof Error ? error.message : String(error),
    });
    clearTransfer(callControlId);
    await sendFallbackCallback(input);
    return;
  }

  logger.info("Waiting for Yoni transfer response", {
    callControlId,
    rowNumber,
    timeoutMs: LIVE_TRANSFER_APPROVAL_TIMEOUT_MS,
  });

  if (config.liveTransferForceResult === "accept") {
    logger.info("FORCED: transfer accepted", {
      callControlId,
      rowNumber,
    });

    try {
      await executeAcceptedTransfer({ callControlId, rowNumber, liveTransferNumber });
      return;
    } catch {
      await sendFallbackCallback(input);
      return;
    }
  }

  if (config.liveTransferForceResult === "fail") {
    logger.info("FORCED: transfer failed", {
      callControlId,
      rowNumber,
    });
    clearTransfer(callControlId);
    await sendFallbackCallback(input);
    return;
  }

  const reply = await replyPromise;

  if (reply === "yes") {
    try {
      await executeAcceptedTransfer({ callControlId, rowNumber, liveTransferNumber });
      return;
    } catch (error) {
      clearTransfer(callControlId);
      await sendFallbackCallback(input);
      return;
    }
  }

  if (reply === "no") {
    logger.info("Transfer declined", {
      callControlId,
      rowNumber,
    });
  } else {
    logger.info("Transfer timeout", {
      callControlId,
      rowNumber,
    });
  }

  logger.info("Fallback triggered", {
    callControlId,
    rowNumber,
    reason: reply,
  });
  const yoniCallControlId = getPendingTransfer(callControlId)?.yoniCallControlId;
  if (yoniCallControlId) {
    await hangupCall(yoniCallControlId).catch((error) => {
      logger.error("Unable to hang up Yoni call after fallback", {
        callControlId,
        yoniCallControlId,
        message: error instanceof Error ? error.message : String(error),
      });
    });
  }
  clearTransfer(callControlId);
  await sendFallbackCallback(input);
}
