import axios, { AxiosError } from "axios";
import { randomUUID } from "crypto";
import { config, telnyxWebhookUrl } from "./config";
import { logger } from "./logger";
import type { CallMetadata, LiveTransferCallMetadata, TelnyxDialResponse } from "../types";

const TELNYX_API_BASE_URL = "https://api.telnyx.com/v2";

const telnyxClient = axios.create({
  baseURL: TELNYX_API_BASE_URL,
  timeout: 15_000,
  headers: {
    Authorization: `Bearer ${config.telnyx.apiKey}`,
    "Content-Type": "application/json",
  },
});

export function encodeClientState(metadata: CallMetadata | LiveTransferCallMetadata): string {
  return Buffer.from(JSON.stringify(metadata), "utf8").toString("base64");
}

function decodeClientStateJson(clientState?: string | null): unknown {
  if (!clientState) {
    return undefined;
  }

  return JSON.parse(Buffer.from(clientState, "base64").toString("utf8"));
}

export function decodeClientState(clientState?: string | null): CallMetadata | undefined {
  if (!clientState) {
    return undefined;
  }

  try {
    const metadata = decodeClientStateJson(clientState) as CallMetadata | LiveTransferCallMetadata | undefined;

    if (metadata && "kind" in metadata && metadata.kind === "live_transfer_yoni") {
      return undefined;
    }

    return metadata as CallMetadata;
  } catch (error) {
    logger.warn("Unable to decode Telnyx client_state", {
      error: error instanceof Error ? error.message : String(error),
    });
    return undefined;
  }
}

export function decodeLiveTransferClientState(clientState?: string | null): LiveTransferCallMetadata | undefined {
  if (!clientState) {
    return undefined;
  }

  try {
    const metadata = decodeClientStateJson(clientState) as Partial<LiveTransferCallMetadata> | undefined;

    if (metadata?.kind !== "live_transfer_yoni" || !metadata.originalCallControlId) {
      return undefined;
    }

    return metadata as LiveTransferCallMetadata;
  } catch (error) {
    logger.warn("Unable to decode Telnyx live transfer client_state", {
      error: error instanceof Error ? error.message : String(error),
    });
    return undefined;
  }
}

function getTelnyxError(error: unknown): Record<string, unknown> {
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

export async function placeOutboundCall(params: {
  to: string;
  metadata: CallMetadata;
}): Promise<TelnyxDialResponse> {
  const clientState = encodeClientState(params.metadata);

  const body = {
    connection_id: config.telnyx.connectionId,
    from: config.telnyx.callerId,
    to: params.to,
    client_state: clientState,
    webhook_url: telnyxWebhookUrl,
    webhook_url_method: "POST",
    timeout_secs: 30,
  };

  logger.info("Placing Telnyx outbound call", {
    to: params.to,
    rowNumber: params.metadata.rowNumber,
    fullName: params.metadata.fullName,
    webhookUrl: telnyxWebhookUrl,
    connectionId: config.telnyx.connectionId,
    outboundVoiceProfileId: config.telnyx.outboundVoiceProfileId,
  });

  try {
    const response = await telnyxClient.post<TelnyxDialResponse>("/calls", body);
    return response.data;
  } catch (error) {
    logger.error("Telnyx outbound call failed", getTelnyxError(error));
    throw error;
  }
}

export async function placeLiveTransferApprovalCall(params: {
  to: string;
  metadata: LiveTransferCallMetadata;
}): Promise<TelnyxDialResponse> {
  const clientState = encodeClientState(params.metadata);

  const body = {
    connection_id: config.telnyx.connectionId,
    from: config.telnyx.callerId,
    to: params.to,
    client_state: clientState,
    webhook_url: telnyxWebhookUrl,
    webhook_url_method: "POST",
    timeout_secs: 30,
  };

  logger.info("Calling Yoni for live transfer", {
    to: params.to,
    originalCallControlId: params.metadata.originalCallControlId,
    rowNumber: params.metadata.rowNumber,
    agentName: params.metadata.agentName,
    webhookUrl: telnyxWebhookUrl,
  });

  try {
    const response = await telnyxClient.post<TelnyxDialResponse>("/calls", body);
    return response.data;
  } catch (error) {
    logger.error("Yoni live transfer call failed", {
      to: params.to,
      originalCallControlId: params.metadata.originalCallControlId,
      ...getTelnyxError(error),
    });
    throw error;
  }
}

export async function speakText(callControlId: string, message: string): Promise<void> {
  try {
    await telnyxClient.post(`/calls/${encodeURIComponent(callControlId)}/actions/speak`, {
      payload: message,
      voice: "female",
      language: "en-US",
      command_id: randomUUID(),
    });
  } catch (error) {
    logger.error("Telnyx speak command failed", {
      callControlId,
      ...getTelnyxError(error),
    });
    throw error;
  }
}

export async function gatherUsingSpeak(params: {
  callControlId: string;
  message: string;
  clientState?: string;
}): Promise<void> {
  try {
    const body = {
      payload: params.message,
      voice: "female",
      service_level: "basic",
      language: "en-US",
      minimum_digits: 1,
      maximum_digits: 1,
      maximum_tries: 1,
      valid_digits: "123",
      timeout_millis: 10_000,
      inter_digit_timeout_millis: 3_000,
      command_id: randomUUID(),
      ...(params.clientState ? { client_state: params.clientState } : {}),
    };

    await telnyxClient.post(`/calls/${encodeURIComponent(params.callControlId)}/actions/gather_using_speak`, body);
  } catch (error) {
    logger.error("Telnyx gather_using_speak command failed", {
      callControlId: params.callControlId,
      ...getTelnyxError(error),
    });
    throw error;
  }
}

export async function gatherUsingAi(params: {
  callControlId: string;
  message: string;
  clientState?: string;
}): Promise<void> {
  try {
    const body = {
      greeting: params.message,
      voice: "Telnyx.KokoroTTS.af",
      parameters: {
        type: "object",
        properties: {
          callback_time: {
            type: "string",
            description:
              "The callback time requested by the caller. Preserve their wording, including day, time, and timezone if they say one.",
          },
        },
        required: ["callback_time"],
      },
      message_history: [
        {
          role: "assistant",
          content:
            "The caller pressed 3 because they want Yoni to call them back later. Ask for the best callback time and capture only that time.",
        },
      ],
      user_response_timeout_ms: 15_000,
      gather_ended_speech: "Thanks, I got it.",
      command_id: randomUUID(),
      ...(params.clientState ? { client_state: params.clientState } : {}),
    };

    await telnyxClient.post(`/calls/${encodeURIComponent(params.callControlId)}/actions/gather_using_ai`, body);
  } catch (error) {
    logger.error("Telnyx gather_using_ai command failed", {
      callControlId: params.callControlId,
      ...getTelnyxError(error),
    });
    throw error;
  }
}

export async function hangupCall(callControlId: string): Promise<void> {
  try {
    await telnyxClient.post(`/calls/${encodeURIComponent(callControlId)}/actions/hangup`, {
      command_id: randomUUID(),
    });
  } catch (error) {
    logger.error("Telnyx hangup command failed", {
      callControlId,
      ...getTelnyxError(error),
    });
    throw error;
  }
}

export async function transferCall(callControlId: string, toNumber: string): Promise<void> {
  try {
    await telnyxClient.post(`/calls/${encodeURIComponent(callControlId)}/actions/transfer`, {
      to: toNumber,
      from: config.telnyx.callerId,
      command_id: randomUUID(),
    });
  } catch (error) {
    logger.error("Telnyx transfer command failed", {
      callControlId,
      toNumber,
      ...getTelnyxError(error),
    });
    throw error;
  }
}

export async function bridgeCalls(callControlId: string, callControlIdToBridgeWith: string): Promise<void> {
  try {
    await telnyxClient.post(`/calls/${encodeURIComponent(callControlId)}/actions/bridge`, {
      call_control_id: callControlIdToBridgeWith,
      prevent_double_bridge: true,
      command_id: randomUUID(),
    });
  } catch (error) {
    logger.error("Telnyx bridge command failed", {
      callControlId,
      callControlIdToBridgeWith,
      ...getTelnyxError(error),
    });
    throw error;
  }
}

export async function sendSms(params: { to: string; text: string }): Promise<void> {
  // Telnyx long-code SMS requires the from number to be assigned to a Messaging Profile.
  // TELNYX_MESSAGING_PROFILE_ID is included when configured for sender setups that require it.
  const body = {
    from: config.telnyx.alertFromNumber,
    to: params.to,
    text: params.text,
    ...(config.telnyx.messagingProfileId ? { messaging_profile_id: config.telnyx.messagingProfileId } : {}),
  };

  try {
    await telnyxClient.post("/messages", body);
  } catch (error) {
    logger.error("Telnyx SMS send failed", {
      to: params.to,
      from: config.telnyx.alertFromNumber,
      hasMessagingProfileId: Boolean(config.telnyx.messagingProfileId),
      ...getTelnyxError(error),
    });
    throw error;
  }
}
