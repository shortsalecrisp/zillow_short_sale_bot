import { logger } from "./logger";

export type LiveTransferReply = "yes" | "no";

export type PendingTransfer = {
  originalCallControlId: string;
  yoniCallControlId?: string;
  resolve: (result: LiveTransferReply) => void;
  timeout: NodeJS.Timeout;
  createdAt: number;
  resolved: boolean;
  yoniPrompt?: string;
};

const pendingTransfers = new Map<string, PendingTransfer>();

export function createPendingTransfer(
  callControlId: string,
  resolveFn: (result: LiveTransferReply) => void,
  timeout: NodeJS.Timeout,
  details: { yoniPrompt?: string } = {},
): void {
  clearTransfer(callControlId);

  pendingTransfers.set(callControlId, {
    originalCallControlId: callControlId,
    resolve: resolveFn,
    timeout,
    createdAt: Date.now(),
    resolved: false,
    yoniPrompt: details.yoniPrompt,
  });

  logger.info("Pending transfer created", {
    callControlId,
    pendingTransferCount: pendingTransfers.size,
  });
}

export function resolveTransfer(callControlId: string, result: LiveTransferReply): boolean {
  const pendingTransfer = pendingTransfers.get(callControlId);

  if (!pendingTransfer) {
    logger.info("Ignoring transfer resolution because no pending transfer exists", {
      callControlId,
      result,
    });
    return false;
  }

  if (pendingTransfer.resolved) {
    logger.info("Ignoring duplicate transfer resolution", {
      callControlId,
      result,
      existingResult: pendingTransfer.resolved,
    });
    return false;
  }

  clearTimeout(pendingTransfer.timeout);
  pendingTransfer.resolved = true;
  pendingTransfer.resolve(result);

  logger.info("Transfer resolved", {
    callControlId,
    yoniCallControlId: pendingTransfer.yoniCallControlId,
    result,
    pendingTransferCount: pendingTransfers.size,
  });

  return true;
}

export function setYoniCallControlId(originalCallControlId: string, yoniCallControlId: string): void {
  const pendingTransfer = pendingTransfers.get(originalCallControlId);

  if (!pendingTransfer) {
    logger.info("Unable to attach Yoni call because no pending transfer exists", {
      originalCallControlId,
      yoniCallControlId,
    });
    return;
  }

  pendingTransfer.yoniCallControlId = yoniCallControlId;

  logger.info("Yoni transfer call attached", {
    originalCallControlId,
    yoniCallControlId,
  });
}

export function getPendingTransfer(originalCallControlId: string): PendingTransfer | undefined {
  return pendingTransfers.get(originalCallControlId);
}

export function getPendingTransferByYoniCallControlId(yoniCallControlId: string): PendingTransfer | undefined {
  for (const pendingTransfer of pendingTransfers.values()) {
    if (pendingTransfer.yoniCallControlId === yoniCallControlId) {
      return pendingTransfer;
    }
  }

  return undefined;
}

export function resolveTransferByYoniCallControlId(yoniCallControlId: string, result: LiveTransferReply): boolean {
  const pendingTransfer = getPendingTransferByYoniCallControlId(yoniCallControlId);

  if (!pendingTransfer) {
    logger.info("Ignoring Yoni transfer resolution because no pending transfer exists", {
      yoniCallControlId,
      result,
    });
    return false;
  }

  return resolveTransfer(pendingTransfer.originalCallControlId, result);
}

export function clearTransfer(callControlId: string): void {
  const pendingTransfer = pendingTransfers.get(callControlId);

  if (!pendingTransfer) {
    return;
  }

  clearTimeout(pendingTransfer.timeout);
  pendingTransfers.delete(callControlId);
}

export function getMostRecentPendingTransferCallControlId(): string | undefined {
  let mostRecentCallControlId: string | undefined;
  let mostRecentCreatedAt = 0;

  for (const [callControlId, pendingTransfer] of pendingTransfers.entries()) {
    if (pendingTransfer.createdAt > mostRecentCreatedAt) {
      mostRecentCallControlId = callControlId;
      mostRecentCreatedAt = pendingTransfer.createdAt;
    }
  }

  return mostRecentCallControlId;
}
