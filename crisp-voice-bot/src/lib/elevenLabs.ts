import axios, { AxiosError, type AxiosInstance } from "axios";
import { randomUUID } from "node:crypto";
import { config } from "./config";
import { rememberElevenLabsCallContext } from "./elevenLabsCallContext";
import { buildElevenLabsOpenerVariant } from "./elevenLabsOpenerVariant";
import { scheduleElevenLabsPostCallFallback } from "./elevenLabsPostCall";
import {
  findElevenLabsVoiceVariant,
  selectElevenLabsVoiceVariant,
  type ElevenLabsVoiceVariant,
} from "./elevenLabsVoiceVariant";
import { logger } from "./logger";
import type { CallMetadata, ElevenLabsOutboundCallResponse } from "../types";

const elevenLabsClient = axios.create({
  baseURL: config.elevenLabs.baseUrl,
  timeout: 45_000,
  headers: {
    "Content-Type": "application/json",
    ...(config.elevenLabs.apiKey ? { "xi-api-key": config.elevenLabs.apiKey } : {}),
  },
});

const CALL_START_RECEIPT_ATTEMPTS = 3;
const CALL_START_RECEIPT_DELAY_MS = 2_000;
const CALL_START_RETRY_JITTER_MIN_MS = 1_500;
const CALL_START_RETRY_JITTER_MAX_MS = 3_000;
const MAX_CALL_START_RECEIPT_CANDIDATES = 12;

type ElevenLabsConversationSummary = {
  conversation_id?: string;
  start_time_unix_secs?: number;
  call_duration_secs?: number;
  status?: string;
};

type ElevenLabsConversationDetails = ElevenLabsConversationSummary & {
  has_audio?: boolean;
  has_user_audio?: boolean;
  has_response_audio?: boolean;
  conversation_initiation_client_data?: {
    dynamic_variables?: Record<string, unknown>;
  };
  metadata?: {
    start_time_unix_secs?: number;
    accepted_time_unix_secs?: number | null;
    call_duration_secs?: number | null;
    error?: Record<string, unknown> | null;
  };
};

type ElevenLabsConversationListResponse = {
  conversations?: ElevenLabsConversationSummary[];
};

export type ElevenLabsCallStartReceipt = {
  status: "accepted" | "definitive_failure";
  conversationId: string;
  conversation: ElevenLabsConversationDetails;
};

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readDynamicNumber(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function normalizePhone(value: unknown): string {
  return typeof value === "string" ? value.replace(/\D/g, "") : "";
}

export function isElevenLabsCallStartTimeout(error: unknown): boolean {
  if (!axios.isAxiosError(error)) {
    return false;
  }

  return error.code === "ECONNABORTED" || /timeout/i.test(error.message);
}

export function classifyElevenLabsCallStartConversation(
  conversation: ElevenLabsConversationDetails,
  metadata: Pick<CallMetadata, "rowNumber" | "callAttemptNumber" | "dialedPhone" | "callStartRequestId">,
  requestStartedAtUnixSecs: number,
): ElevenLabsCallStartReceipt | undefined {
  const conversationId = conversation.conversation_id;
  const dynamicVariables = conversation.conversation_initiation_client_data?.dynamic_variables ?? {};
  const rowNumber = readDynamicNumber(dynamicVariables.rowNumber);
  const callAttemptNumber = readDynamicNumber(dynamicVariables.callAttemptNumber);
  const phone = normalizePhone(dynamicVariables.phone);
  const callStartRequestId =
    typeof dynamicVariables.callStartRequestId === "string" ? dynamicVariables.callStartRequestId : undefined;
  const startTime = conversation.start_time_unix_secs ?? conversation.metadata?.start_time_unix_secs;

  if (
    !conversationId ||
    rowNumber !== metadata.rowNumber ||
    callAttemptNumber !== metadata.callAttemptNumber ||
    (metadata.callStartRequestId && callStartRequestId !== metadata.callStartRequestId) ||
    (phone && phone !== normalizePhone(metadata.dialedPhone)) ||
    (typeof startTime === "number" && startTime < requestStartedAtUnixSecs - 10)
  ) {
    return undefined;
  }

  const accepted =
    Boolean(conversation.metadata?.accepted_time_unix_secs) ||
    Boolean(conversation.metadata?.call_duration_secs) ||
    Boolean(conversation.call_duration_secs) ||
    conversation.has_audio === true ||
    conversation.has_user_audio === true ||
    conversation.has_response_audio === true ||
    conversation.status !== "failed";

  return {
    status: accepted ? "accepted" : "definitive_failure",
    conversationId,
    conversation,
  };
}

async function findElevenLabsCallStartReceipt(params: {
  client: AxiosInstance;
  agentId: string;
  metadata: CallMetadata;
  requestStartedAtUnixSecs: number;
}): Promise<ElevenLabsCallStartReceipt | undefined> {
  const response = await params.client.get<ElevenLabsConversationListResponse>("/v1/convai/conversations", {
    params: {
      agent_id: params.agentId,
      call_start_after_unix: params.requestStartedAtUnixSecs - 10,
      page_size: MAX_CALL_START_RECEIPT_CANDIDATES,
      sort_direction: "desc",
    },
  });

  const summaries = (response.data.conversations ?? []).slice(0, MAX_CALL_START_RECEIPT_CANDIDATES);
  for (const summary of summaries) {
    if (!summary.conversation_id) {
      continue;
    }

    try {
      const detailResponse = await params.client.get<ElevenLabsConversationDetails>(
        `/v1/convai/conversations/${summary.conversation_id}`,
      );
      const receipt = classifyElevenLabsCallStartConversation(
        detailResponse.data,
        params.metadata,
        params.requestStartedAtUnixSecs,
      );
      if (receipt) {
        return receipt;
      }
    } catch (error) {
      logger.warn("ElevenLabs call-start receipt detail lookup failed", {
        conversationId: summary.conversation_id,
        rowNumber: params.metadata.rowNumber,
        callAttemptNumber: params.metadata.callAttemptNumber,
        ...getElevenLabsError(error),
      });
    }
  }

  return undefined;
}

async function reconcileElevenLabsCallStartTimeout(params: {
  client: AxiosInstance;
  agentId: string;
  metadata: CallMetadata;
  requestStartedAtUnixSecs: number;
}): Promise<ElevenLabsCallStartReceipt | undefined> {
  for (let attempt = 1; attempt <= CALL_START_RECEIPT_ATTEMPTS; attempt += 1) {
    if (attempt > 1) {
      await delay(CALL_START_RECEIPT_DELAY_MS);
    }

    try {
      const receipt = await findElevenLabsCallStartReceipt(params);
      if (receipt) {
        return receipt;
      }
    } catch (error) {
      logger.warn("ElevenLabs call-start receipt lookup failed", {
        rowNumber: params.metadata.rowNumber,
        callAttemptNumber: params.metadata.callAttemptNumber,
        receiptAttempt: attempt,
        ...getElevenLabsError(error),
      });
    }
  }

  return undefined;
}

const STREET_TYPE_SUFFIX_PATTERN =
  /\s+(?:ALY|ALLY|AVE|AVENUE|BLVD|BOULEVARD|CIR|CIRCLE|CT|COURT|CV|COVE|DR|DRIVE|HWY|HIGHWAY|LN|LANE|LOOP|PKWY|PARKWAY|PL|PLACE|RD|ROAD|ST|STREET|TER|TERRACE|TRL|TRAIL|WAY)\.?(?:\s+(?:N|S|E|W|NE|NW|SE|SW)\.?)?$/i;

const SMALL_NUMBER_WORDS = [
  "Zero",
  "One",
  "Two",
  "Three",
  "Four",
  "Five",
  "Six",
  "Seven",
  "Eight",
  "Nine",
  "Ten",
  "Eleven",
  "Twelve",
  "Thirteen",
  "Fourteen",
  "Fifteen",
  "Sixteen",
  "Seventeen",
  "Eighteen",
  "Nineteen",
] as const;

const TENS_NUMBER_WORDS: Record<number, string> = {
  2: "Twenty",
  3: "Thirty",
  4: "Forty",
  5: "Fifty",
  6: "Sixty",
  7: "Seventy",
  8: "Eighty",
  9: "Ninety",
};

function getElevenLabsError(error: unknown): Record<string, unknown> {
  if (error instanceof AxiosError) {
    return {
      status: error.response?.status,
      statusText: error.response?.statusText,
      data: error.response?.data,
      message: error.message,
    };
  }

  return {
    message: error instanceof Error ? error.message : String(error),
  };
}

function requireElevenLabsOutboundConfig(): { agentId: string; agentPhoneNumberId: string } {
  const missing: string[] = [];

  if (!config.elevenLabs.apiKey) {
    missing.push("ELEVENLABS_API_KEY");
  }

  if (!config.elevenLabs.agentId) {
    missing.push("ELEVENLABS_AGENT_ID");
  }

  if (!config.elevenLabs.agentPhoneNumberId) {
    missing.push("ELEVENLABS_AGENT_PHONE_NUMBER_ID");
  }

  if (missing.length > 0) {
    throw new Error(`Missing ElevenLabs outbound config: ${missing.join(", ")}`);
  }

  return {
    agentId: config.elevenLabs.agentId as string,
    agentPhoneNumberId: config.elevenLabs.agentPhoneNumberId as string,
  };
}

function twoDigitNumberToWords(raw: string): string {
  const value = Number(raw);

  if (raw.length === 2 && raw.startsWith("0")) {
    return `Oh ${SMALL_NUMBER_WORDS[value] ?? raw[1]}`;
  }

  if (value < 20) {
    return SMALL_NUMBER_WORDS[value] ?? raw;
  }

  const tens = Math.floor(value / 10);
  const ones = value % 10;
  const tensWord = TENS_NUMBER_WORDS[tens] ?? raw[0];

  return ones === 0 ? tensWord : `${tensWord} ${SMALL_NUMBER_WORDS[ones] ?? raw[1]}`;
}

function streetNumberToWords(digits: string): string {
  if (digits.length === 2) {
    return twoDigitNumberToWords(digits);
  }

  if (digits.length === 3) {
    return `${SMALL_NUMBER_WORDS[Number(digits[0])] ?? digits[0]} ${twoDigitNumberToWords(digits.slice(1))}`;
  }

  const groups: string[] = [];

  for (let index = 0; index < digits.length; index += 2) {
    groups.push(twoDigitNumberToWords(digits.slice(index, index + 2)));
  }

  return groups.join(" ");
}

function formatLeadingStreetNumber(address: string): string {
  return address.replace(/^\d{2,}\b/, (digits) => streetNumberToWords(digits));
}

export function getStreetAddress(listingAddress: string): string {
  const streetAddress = listingAddress.split(",")[0]?.trim() || listingAddress;
  const withoutUnit = streetAddress
    .replace(/\s*(?:,|-)?\s*(?:APT|APARTMENT|STE|SUITE|UNIT)\.?\s*[A-Z0-9-]+(?:\s*[A-Z0-9-]+)?/gi, "")
    .replace(/\s*#\s*[A-Z0-9-]+(?:\s*[A-Z0-9-]+)?/gi, "")
    .replace(STREET_TYPE_SUFFIX_PATTERN, "")
    .replace(/\s{2,}/g, " ")
    .trim();

  const normalizedAddress = withoutUnit
    .replace(/\bAVE\b\.?/gi, "Avenue")
    .replace(/\bBLVD\b\.?/gi, "Boulevard")
    .replace(/\bNE\b\.?/gi, "Northeast")
    .replace(/\bNW\b\.?/gi, "Northwest")
    .replace(/\bSE\b\.?/gi, "Southeast")
    .replace(/\bSW\b\.?/gi, "Southwest")
    .replace(/\bN\b\.?/gi, "North")
    .replace(/\bS\b\.?/gi, "South")
    .replace(/\bE\b\.?/gi, "East")
    .replace(/\bW\b\.?/gi, "West");

  return formatLeadingStreetNumber(normalizedAddress);
}

function resolveElevenLabsVoiceVariant(metadata: CallMetadata): ElevenLabsVoiceVariant {
  if (metadata.voiceVariant && metadata.assistantName && metadata.voiceName && metadata.voiceId) {
    const configuredVariant = findElevenLabsVoiceVariant(metadata.voiceVariant);

    return {
      key: metadata.voiceVariant as ElevenLabsVoiceVariant["key"],
      assistantName: metadata.assistantName as ElevenLabsVoiceVariant["assistantName"],
      voiceName: metadata.voiceName as ElevenLabsVoiceVariant["voiceName"],
      voiceId: metadata.voiceId,
      ttsSpeed: configuredVariant?.ttsSpeed,
    };
  }

  return selectElevenLabsVoiceVariant({ rowNumber: metadata.rowNumber });
}

function buildVoicemailMessage(streetAddress: string, callAttemptNumber: number, assistantName: string): string {
  if (callAttemptNumber > 1) {
    return "";
  }

  return `Hi, this is ${assistantName} with Crisp Short Sales calling about the short sale listing at ${streetAddress}. We specialize in helping agents with the short sale process and can handle the paperwork, phone calls, and the whole process with the lender to take that work off your shoulders. Yoni is our short sale specialist, and he can answer any questions you have. Give him a call back at 404-300-9526 when you get a chance. Thanks.`;
}

function resolveElevenLabsOpener(metadata: CallMetadata, assistantName: string) {
  if (metadata.openerVariant && metadata.openerScript) {
    return {
      key: metadata.openerVariant,
      label: metadata.openerVariantLabel ?? metadata.openerVariant,
      script: metadata.openerScript,
    };
  }

  return buildElevenLabsOpenerVariant({
    rowNumber: metadata.rowNumber,
    firstName: metadata.firstName,
    assistantName,
  });
}

export function buildElevenLabsOutboundCallBody(params: {
  agentId: string;
  agentPhoneNumberId: string;
  to: string;
  metadata: CallMetadata;
}) {
  const voiceVariant = resolveElevenLabsVoiceVariant(params.metadata);
  const streetAddress = getStreetAddress(params.metadata.listingAddress);
  const openerVariant = resolveElevenLabsOpener(params.metadata, voiceVariant.assistantName);
  const dynamicVariables = {
    rowNumber: params.metadata.rowNumber,
    agentName: params.metadata.fullName,
    firstName: params.metadata.firstName ?? "",
    lastName: params.metadata.lastName ?? "",
    callAttemptNumber: params.metadata.callAttemptNumber,
    callStartRequestId: params.metadata.callStartRequestId ?? "",
    phone: params.metadata.dialedPhone,
    email: params.metadata.email ?? "",
    requestedPhone: params.metadata.requestedPhone,
    listingAddress: params.metadata.listingAddress,
    streetAddress,
    scheduledWindow: params.metadata.scheduledWindow ?? "",
    agentTimeZone: params.metadata.agentTimeZone ?? "",
    assistantName: voiceVariant.assistantName,
    voiceVariant: voiceVariant.key,
    voiceName: voiceVariant.voiceName,
    openerVariant: openerVariant.key,
    openerVariantLabel: openerVariant.label,
    openerScript: openerVariant.script,
    voicemailMessage: buildVoicemailMessage(streetAddress, params.metadata.callAttemptNumber, voiceVariant.assistantName),
    testMode: params.metadata.testMode,
    liveTransferNumber: config.liveTransferNumber,
    toolWebhookBaseUrl: config.baseUrl,
  };

  return {
    agent_id: params.agentId,
    agent_phone_number_id: params.agentPhoneNumberId,
    to_number: params.to,
    conversation_initiation_client_data: {
      dynamic_variables: dynamicVariables,
      conversation_config_override: {
        tts: {
          voice_id: voiceVariant.voiceId,
          ...(voiceVariant.ttsSpeed === undefined ? {} : { speed: voiceVariant.ttsSpeed }),
        },
      },
    },
  };
}

export async function placeElevenLabsOutboundCall(params: {
  to: string;
  metadata: CallMetadata;
  schedulePostCallFallback?: boolean;
}): Promise<ElevenLabsOutboundCallResponse> {
  const { agentId, agentPhoneNumberId } = requireElevenLabsOutboundConfig();
  const voiceVariant = resolveElevenLabsVoiceVariant(params.metadata);
  const metadata: CallMetadata = {
    ...params.metadata,
    assistantName: voiceVariant.assistantName,
    voiceVariant: voiceVariant.key,
    voiceName: voiceVariant.voiceName,
    voiceId: voiceVariant.voiceId,
  };
  const openerVariant = resolveElevenLabsOpener(metadata, voiceVariant.assistantName);
  metadata.openerVariant = openerVariant.key;
  metadata.openerVariantLabel = openerVariant.label;
  metadata.openerScript = openerVariant.script;
  logger.info("Placing ElevenLabs outbound call via SIP trunk", {
    to: params.to,
    rowNumber: metadata.rowNumber,
    callAttemptNumber: metadata.callAttemptNumber,
    fullName: metadata.fullName,
    scheduledWindow: metadata.scheduledWindow,
    agentTimeZone: metadata.agentTimeZone,
    assistantName: voiceVariant.assistantName,
    voiceVariant: voiceVariant.key,
    voiceName: voiceVariant.voiceName,
    openerVariant: openerVariant.key,
    openerVariantLabel: openerVariant.label,
    agentId,
    agentPhoneNumberId,
  });

  const finalizeAcceptedCall = (response: ElevenLabsOutboundCallResponse, acceptedMetadata: CallMetadata) => {
    logger.info("ElevenLabs outbound call accepted", {
      rowNumber: params.metadata.rowNumber,
      callAttemptNumber: params.metadata.callAttemptNumber,
      callStartRequestId: acceptedMetadata.callStartRequestId,
      conversationId: response.conversation_id,
      sipCallId: response.sip_call_id,
      message: response.message,
      assistantName: voiceVariant.assistantName,
      voiceVariant: voiceVariant.key,
    });

    if (response.conversation_id) {
      rememberElevenLabsCallContext(acceptedMetadata, response.conversation_id);

      if (params.schedulePostCallFallback !== false) {
        scheduleElevenLabsPostCallFallback({
          conversationId: response.conversation_id,
          metadata: acceptedMetadata,
        });
      }
    } else {
      rememberElevenLabsCallContext(acceptedMetadata);
    }

    return response;
  };

  const placeWithTimeoutRecovery = async (retryCount: number): Promise<ElevenLabsOutboundCallResponse> => {
    const attemptMetadata: CallMetadata = {
      ...metadata,
      callStartRequestId: `${metadata.rowNumber}-${metadata.callAttemptNumber}-${randomUUID()}`,
    };
    const body = buildElevenLabsOutboundCallBody({
      agentId,
      agentPhoneNumberId,
      to: params.to,
      metadata: attemptMetadata,
    });
    const requestStartedAtUnixSecs = Math.floor(Date.now() / 1000);

    try {
      const response = await elevenLabsClient.post<ElevenLabsOutboundCallResponse>(
        "/v1/convai/sip-trunk/outbound-call",
        body,
      );
      return finalizeAcceptedCall(response.data, attemptMetadata);
    } catch (error) {
      if (!isElevenLabsCallStartTimeout(error)) {
        logger.error("ElevenLabs outbound call failed", {
          rowNumber: params.metadata.rowNumber,
          callAttemptNumber: params.metadata.callAttemptNumber,
          callStartRequestId: attemptMetadata.callStartRequestId,
          ...getElevenLabsError(error),
        });
        throw error;
      }

      logger.warn("ElevenLabs outbound call timed out; reconciling provider receipt before any retry", {
        rowNumber: params.metadata.rowNumber,
        callAttemptNumber: params.metadata.callAttemptNumber,
        callStartRequestId: attemptMetadata.callStartRequestId,
        retryCount,
      });

      const receipt = await reconcileElevenLabsCallStartTimeout({
        client: elevenLabsClient,
        agentId,
        metadata: attemptMetadata,
        requestStartedAtUnixSecs,
      });

      if (receipt?.status === "accepted") {
        logger.info("Recovered accepted ElevenLabs call receipt after request timeout", {
          rowNumber: params.metadata.rowNumber,
          callAttemptNumber: params.metadata.callAttemptNumber,
          callStartRequestId: attemptMetadata.callStartRequestId,
          conversationId: receipt.conversationId,
          conversationStatus: receipt.conversation.status,
        });
        return finalizeAcceptedCall(
          {
            success: true,
            conversation_id: receipt.conversationId,
            message: "Recovered accepted provider receipt after outbound-call request timeout",
          },
          attemptMetadata,
        );
      }

      if (receipt?.status === "definitive_failure" && retryCount === 0) {
        const jitterMs =
          CALL_START_RETRY_JITTER_MIN_MS +
          Math.floor(Math.random() * (CALL_START_RETRY_JITTER_MAX_MS - CALL_START_RETRY_JITTER_MIN_MS + 1));
        logger.warn("ElevenLabs receipt proves call was not accepted; retrying once", {
          rowNumber: params.metadata.rowNumber,
          callAttemptNumber: params.metadata.callAttemptNumber,
          callStartRequestId: attemptMetadata.callStartRequestId,
          failedConversationId: receipt.conversationId,
          jitterMs,
        });
        await delay(jitterMs);
        return placeWithTimeoutRecovery(1);
      }

      const message = receipt
        ? "ElevenLabs call start definitively failed after the single receipt-safe retry"
        : "ElevenLabs call start delivery is uncertain after timeout; no automatic retry was performed";
      const recoveryError = new Error(message);
      logger.error(message, {
        rowNumber: params.metadata.rowNumber,
        callAttemptNumber: params.metadata.callAttemptNumber,
        callStartRequestId: attemptMetadata.callStartRequestId,
        conversationId: receipt?.conversationId,
        retryCount,
      });
      throw recoveryError;
    }
  };

  return placeWithTimeoutRecovery(0);
}
