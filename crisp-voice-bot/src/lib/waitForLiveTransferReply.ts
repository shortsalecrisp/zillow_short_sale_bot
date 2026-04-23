import { clearTransfer, createPendingTransfer, type LiveTransferReply } from "./liveTransferStore";
import { logger } from "./logger";

export type LiveTransferApprovalResult = LiveTransferReply | "timeout";

export async function waitForLiveTransferReply({
  callControlId,
  timeoutMs,
  yoniPrompt,
}: {
  callControlId: string;
  timeoutMs: number;
  yoniPrompt?: string;
}): Promise<LiveTransferApprovalResult> {
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      clearTransfer(callControlId);
      logger.info("Transfer timed out", {
        callControlId,
        timeoutMs,
      });
      resolve("timeout");
    }, timeoutMs);

    createPendingTransfer(callControlId, resolve, timeout, { yoniPrompt });
  });
}
