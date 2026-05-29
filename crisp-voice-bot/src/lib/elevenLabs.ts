import axios, { AxiosError } from "axios";
import { config } from "./config";
import { rememberElevenLabsCallContext } from "./elevenLabsCallContext";
import { scheduleElevenLabsPostCallFallback } from "./elevenLabsPostCall";
import { selectElevenLabsVoiceVariant, type ElevenLabsVoiceVariant } from "./elevenLabsVoiceVariant";
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
    return {
      key: metadata.voiceVariant as ElevenLabsVoiceVariant["key"],
      assistantName: metadata.assistantName as ElevenLabsVoiceVariant["assistantName"],
      voiceName: metadata.voiceName as ElevenLabsVoiceVariant["voiceName"],
      voiceId: metadata.voiceId,
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

export function buildElevenLabsOutboundCallBody(params: {
  agentId: string;
  agentPhoneNumberId: string;
  to: string;
  metadata: CallMetadata;
}) {
  const voiceVariant = resolveElevenLabsVoiceVariant(params.metadata);
  const streetAddress = getStreetAddress(params.metadata.listingAddress);
  const dynamicVariables = {
    rowNumber: params.metadata.rowNumber,
    agentName: params.metadata.fullName,
    firstName: params.metadata.firstName ?? "",
    lastName: params.metadata.lastName ?? "",
    callAttemptNumber: params.metadata.callAttemptNumber,
    phone: params.metadata.dialedPhone,
    requestedPhone: params.metadata.requestedPhone,
    listingAddress: params.metadata.listingAddress,
    streetAddress,
    assistantName: voiceVariant.assistantName,
    voiceVariant: voiceVariant.key,
    voiceName: voiceVariant.voiceName,
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
  const body = buildElevenLabsOutboundCallBody({
    agentId,
    agentPhoneNumberId,
    to: params.to,
    metadata,
  });

  logger.info("Placing ElevenLabs outbound call via SIP trunk", {
    to: params.to,
    rowNumber: metadata.rowNumber,
    callAttemptNumber: metadata.callAttemptNumber,
    fullName: metadata.fullName,
    assistantName: voiceVariant.assistantName,
    voiceVariant: voiceVariant.key,
    voiceName: voiceVariant.voiceName,
    agentId,
    agentPhoneNumberId,
  });

  try {
    const response = await elevenLabsClient.post<ElevenLabsOutboundCallResponse>(
      "/v1/convai/sip-trunk/outbound-call",
      body,
    );

    logger.info("ElevenLabs outbound call accepted", {
      rowNumber: params.metadata.rowNumber,
      callAttemptNumber: params.metadata.callAttemptNumber,
      conversationId: response.data.conversation_id,
      sipCallId: response.data.sip_call_id,
      message: response.data.message,
      assistantName: voiceVariant.assistantName,
      voiceVariant: voiceVariant.key,
    });

    if (response.data.conversation_id) {
      rememberElevenLabsCallContext(metadata, response.data.conversation_id);

      if (params.schedulePostCallFallback !== false) {
        scheduleElevenLabsPostCallFallback({
          conversationId: response.data.conversation_id,
          metadata,
        });
      }
    } else {
      rememberElevenLabsCallContext(metadata);
    }

    return response.data;
  } catch (error) {
    logger.error("ElevenLabs outbound call failed", {
      rowNumber: params.metadata.rowNumber,
      callAttemptNumber: params.metadata.callAttemptNumber,
      ...getElevenLabsError(error),
    });
    throw error;
  }
}
