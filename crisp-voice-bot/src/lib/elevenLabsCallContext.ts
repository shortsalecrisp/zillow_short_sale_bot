import type { CallMetadata } from "../types";

type ElevenLabsCallContext = {
  metadata: CallMetadata;
  liveTransferStatus: "idle" | "pending" | "accepted" | "declined" | "timeout" | "call_failed";
};

let latestCallContext: ElevenLabsCallContext | undefined;

export function rememberElevenLabsCallContext(metadata: CallMetadata): void {
  latestCallContext = {
    metadata,
    liveTransferStatus: "idle",
  };
}

export function getLatestElevenLabsCallContext(): CallMetadata | undefined {
  return latestCallContext?.metadata;
}

export function beginElevenLabsLiveTransferAttempt():
  | "started"
  | "pending"
  | "accepted"
  | "declined"
  | "timeout"
  | "call_failed" {
  if (!latestCallContext) {
    return "started";
  }

  if (latestCallContext.liveTransferStatus === "idle") {
    latestCallContext.liveTransferStatus = "pending";
    return "started";
  }

  return latestCallContext.liveTransferStatus;
}

export function completeElevenLabsLiveTransferAttempt(
  status: "accepted" | "declined" | "timeout" | "call_failed",
): void {
  if (!latestCallContext) {
    return;
  }

  latestCallContext.liveTransferStatus = status;
}
