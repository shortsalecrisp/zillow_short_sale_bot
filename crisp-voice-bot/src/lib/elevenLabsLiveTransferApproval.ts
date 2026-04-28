import { randomUUID } from "crypto";
import { clearTransfer, setYoniCallControlId } from "./liveTransferStore";
import { logger } from "./logger";
import { hangupCall, placeLiveTransferApprovalCall, speakText } from "./telnyx";
import { waitForLiveTransferReply } from "./waitForLiveTransferReply";

const ELEVENLABS_TRANSFER_APPROVAL_TIMEOUT_MS = 35_000;

export type ElevenLabsLiveTransferApprovalResult =
  | { status: "accepted"; approvalCallControlId?: string }
  | { status: "declined"; approvalCallControlId?: string }
  | { status: "timeout"; approvalCallControlId?: string }
  | { status: "call_failed"; errorMessage: string };

type ApprovalInput = {
  rowNumber: number;
  agentName: string;
  phone: string;
  listingAddress: string;
  liveTransferNumber: string;
};

function buildApprovalPrompt(input: ApprovalInput): string {
  return `You have a live transfer request from agent ${input.agentName} at ${input.listingAddress}. Press 1 to accept.`;
}

export async function requestElevenLabsLiveTransferApproval(
  input: ApprovalInput,
): Promise<ElevenLabsLiveTransferApprovalResult> {
  const syntheticCallControlId = `elevenlabs-transfer-${input.rowNumber}-${randomUUID()}`;
  const yoniPrompt = buildApprovalPrompt(input);
  let approvalCallControlId: string | undefined;

  const replyPromise = waitForLiveTransferReply({
    callControlId: syntheticCallControlId,
    timeoutMs: ELEVENLABS_TRANSFER_APPROVAL_TIMEOUT_MS,
    yoniPrompt,
  });

  try {
    const approvalCall = await placeLiveTransferApprovalCall({
      to: input.liveTransferNumber,
      metadata: {
        kind: "live_transfer_yoni",
        originalCallControlId: syntheticCallControlId,
        rowNumber: input.rowNumber,
        agentName: input.agentName,
        listingAddress: input.listingAddress,
        whisperMessage: `Live transfer from ${input.agentName} about ${input.listingAddress}.`,
        yoniPrompt,
      },
    });

    approvalCallControlId = approvalCall.data?.call_control_id;

    if (approvalCallControlId) {
      setYoniCallControlId(syntheticCallControlId, approvalCallControlId);
    }

    logger.info("ElevenLabs warm handoff approval call started", {
      rowNumber: input.rowNumber,
      agentName: input.agentName,
      listingAddress: input.listingAddress,
      approvalCallControlId,
      syntheticCallControlId,
      liveTransferNumber: input.liveTransferNumber,
    });
  } catch (error) {
    clearTransfer(syntheticCallControlId);
    const errorMessage = error instanceof Error ? error.message : String(error);

    logger.error("ElevenLabs warm handoff approval call failed", {
      rowNumber: input.rowNumber,
      agentName: input.agentName,
      listingAddress: input.listingAddress,
      syntheticCallControlId,
      errorMessage,
    });

    return { status: "call_failed", errorMessage };
  }

  const reply = await replyPromise;
  clearTransfer(syntheticCallControlId);

  if (reply === "yes") {
    logger.info("ElevenLabs warm handoff accepted by Yoni", {
      rowNumber: input.rowNumber,
      agentName: input.agentName,
      approvalCallControlId,
      syntheticCallControlId,
    });

    if (approvalCallControlId) {
      await speakText(approvalCallControlId, "Thanks. Sending the live transfer now.").catch((error) => {
        logger.error("Unable to play ElevenLabs warm handoff acceptance confirmation", {
          approvalCallControlId,
          syntheticCallControlId,
          message: error instanceof Error ? error.message : String(error),
        });
      });

      await hangupCall(approvalCallControlId).catch((error) => {
        logger.error("Unable to hang up ElevenLabs warm handoff approval call", {
          approvalCallControlId,
          syntheticCallControlId,
          message: error instanceof Error ? error.message : String(error),
        });
      });
    }

    return { status: "accepted", approvalCallControlId };
  }

  if (approvalCallControlId) {
    await hangupCall(approvalCallControlId).catch((error) => {
      logger.error("Unable to hang up unanswered ElevenLabs warm handoff approval call", {
        approvalCallControlId,
        syntheticCallControlId,
        reply,
        message: error instanceof Error ? error.message : String(error),
      });
    });
  }

  logger.info(reply === "no" ? "ElevenLabs warm handoff declined by Yoni" : "ElevenLabs warm handoff timed out", {
    rowNumber: input.rowNumber,
    agentName: input.agentName,
    approvalCallControlId,
    syntheticCallControlId,
    reply,
  });

  return { status: reply === "no" ? "declined" : "timeout", approvalCallControlId };
}
