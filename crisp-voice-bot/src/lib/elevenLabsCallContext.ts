import type { CallMetadata } from "../types";

type ElevenLabsLiveTransferStatus = "idle" | "pending" | "accepted" | "declined" | "timeout" | "call_failed";

type ElevenLabsCallContext = {
  metadata: CallMetadata;
  liveTransferStatus: ElevenLabsLiveTransferStatus;
};

let latestCallContext: ElevenLabsCallContext | undefined;
const callContextsByKey = new Map<string, ElevenLabsCallContext>();
const MAX_CALL_CONTEXT_KEYS = 500;

export function buildElevenLabsCallContextKey(input: {
  rowNumber?: number;
  callAttemptNumber?: number;
}): string | undefined {
  if (!input.rowNumber || !input.callAttemptNumber) {
    return undefined;
  }

  return `${input.rowNumber}:${input.callAttemptNumber}`;
}

export function rememberElevenLabsCallContext(metadata: CallMetadata, conversationId?: string | null): void {
  const context = {
    metadata,
    liveTransferStatus: "idle" as const,
  };

  latestCallContext = context;

  const callContextKey = buildElevenLabsCallContextKey(metadata);
  if (callContextKey) {
    callContextsByKey.set(callContextKey, context);
  }

  if (conversationId) {
    callContextsByKey.set(`conversation:${conversationId}`, context);
  }

  pruneOldCallContexts();
}

export function getLatestElevenLabsCallContext(): CallMetadata | undefined {
  return latestCallContext?.metadata;
}

export function getElevenLabsCallContextByConversationId(conversationId: string): CallMetadata | undefined {
  return callContextsByKey.get(`conversation:${conversationId}`)?.metadata;
}

function getElevenLabsCallContext(contextKey?: string): ElevenLabsCallContext | undefined {
  if (contextKey) {
    return callContextsByKey.get(contextKey);
  }

  return latestCallContext;
}

export function beginElevenLabsLiveTransferAttempt(contextKey?: string):
  | "started"
  | "pending"
  | "accepted"
  | "declined"
  | "timeout"
  | "call_failed" {
  const context = getElevenLabsCallContext(contextKey);

  if (!context) {
    return "started";
  }

  if (context.liveTransferStatus === "idle") {
    context.liveTransferStatus = "pending";
    return "started";
  }

  return context.liveTransferStatus;
}

export function completeElevenLabsLiveTransferAttempt(
  status: Exclude<ElevenLabsLiveTransferStatus, "idle" | "pending">,
  contextKey?: string,
): void {
  const context = getElevenLabsCallContext(contextKey);

  if (!context) {
    return;
  }

  context.liveTransferStatus = status;
}

export function resetElevenLabsCallContextsForTest(): void {
  latestCallContext = undefined;
  callContextsByKey.clear();
}

function pruneOldCallContexts(): void {
  while (callContextsByKey.size > MAX_CALL_CONTEXT_KEYS) {
    const oldestKey = callContextsByKey.keys().next().value;
    if (!oldestKey) {
      return;
    }

    callContextsByKey.delete(oldestKey);
  }
}
