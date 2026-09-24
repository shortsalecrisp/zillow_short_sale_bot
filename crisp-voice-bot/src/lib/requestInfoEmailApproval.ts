import axios, { AxiosError } from "axios";
import { config } from "./config";
import { buildElevenLabsPlaybackUrl } from "./elevenLabsPlayback";
import { logger } from "./logger";

export type InfoEmailApprovalRequest = {
  rowNumber: number;
  phone: string;
  email?: string;
  conversationId?: string;
  conversationSummary?: string;
  conversationTranscript?: string;
};

type InfoEmailApprovalResponse = {
  ok?: boolean;
  approval_id?: string;
  agent_email?: string;
  subject?: string;
};

export function buildInfoEmailApprovalRequestPayload(input: InfoEmailApprovalRequest): Record<string, unknown> {
  const conversationId = input.conversationId?.trim();

  return {
    ...(config.googleAppsScript.token ? { token: config.googleAppsScript.token } : {}),
    action: "request_info_email_approval",
    row: input.rowNumber,
    phone: input.phone,
    email: input.email?.trim() || "",
    conversation_id: conversationId || "",
    playback_url: conversationId ? buildElevenLabsPlaybackUrl(conversationId) : "",
    conversation_summary: input.conversationSummary?.trim() || "",
    conversation_transcript: input.conversationTranscript?.trim() || "",
  };
}

export async function requestInfoEmailApproval(input: InfoEmailApprovalRequest): Promise<InfoEmailApprovalResponse> {
  const url = config.googleAppsScript.webhookUrl;
  if (!url) {
    throw new Error("GOOGLE_APPS_SCRIPT_WEBHOOK_URL is required for info-email approval");
  }

  const payload = buildInfoEmailApprovalRequestPayload(input);

  try {
    const response = await axios.post<InfoEmailApprovalResponse>(url, payload, {
      timeout: 15_000,
      headers: {
        "Content-Type": "application/json",
        ...(config.googleAppsScript.token ? { "X-Crisp-Token": config.googleAppsScript.token } : {}),
      },
    });

    if (!response.data?.ok || !response.data.approval_id) {
      throw new Error("Apps Script did not create an info-email approval");
    }

    logger.info("Information email approval requested", {
      rowNumber: input.rowNumber,
      conversationId: input.conversationId,
      agentEmail: response.data.agent_email,
      approvalId: response.data.approval_id,
    });
    return response.data;
  } catch (error) {
    if (error instanceof AxiosError) {
      logger.error("Information email approval request failed", {
        rowNumber: input.rowNumber,
        conversationId: input.conversationId,
        status: error.response?.status,
        data: error.response?.data,
        message: error.message,
      });
    }
    throw error;
  }
}
