import axios, { AxiosError } from "axios";
import { config } from "./config";
import { rememberElevenLabsCallContext } from "./elevenLabsCallContext";
import { scheduleElevenLabsPostCallFallback } from "./elevenLabsPostCall";
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

function getStreetAddress(listingAddress: string): string {
  const streetAddress = listingAddress.split(",")[0]?.trim() || listingAddress;

  return streetAddress.replace(/\bAPT\b\.?/gi, "Apartment");
}

export async function placeElevenLabsOutboundCall(params: {
  to: string;
  metadata: CallMetadata;
}): Promise<ElevenLabsOutboundCallResponse> {
  const { agentId, agentPhoneNumberId } = requireElevenLabsOutboundConfig();
  const dynamicVariables = {
    rowNumber: params.metadata.rowNumber,
    agentName: params.metadata.fullName,
    firstName: params.metadata.firstName ?? "",
    lastName: params.metadata.lastName ?? "",
    callAttemptNumber: params.metadata.callAttemptNumber,
    phone: params.metadata.dialedPhone,
    requestedPhone: params.metadata.requestedPhone,
    listingAddress: params.metadata.listingAddress,
    streetAddress: getStreetAddress(params.metadata.listingAddress),
    testMode: params.metadata.testMode,
    liveTransferNumber: config.liveTransferNumber,
    toolWebhookBaseUrl: config.baseUrl,
  };

  const body = {
    agent_id: agentId,
    agent_phone_number_id: agentPhoneNumberId,
    to_number: params.to,
    conversation_initiation_client_data: {
      dynamic_variables: dynamicVariables,
    },
  };

  logger.info("Placing ElevenLabs outbound call via SIP trunk", {
    to: params.to,
    rowNumber: params.metadata.rowNumber,
    callAttemptNumber: params.metadata.callAttemptNumber,
    fullName: params.metadata.fullName,
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
    });

    rememberElevenLabsCallContext(params.metadata);

    if (response.data.conversation_id) {
      scheduleElevenLabsPostCallFallback({
        conversationId: response.data.conversation_id,
        metadata: params.metadata,
      });
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
