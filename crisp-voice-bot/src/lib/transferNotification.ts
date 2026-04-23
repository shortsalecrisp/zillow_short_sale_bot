import type { CallState } from "../types";
import { config } from "./config";
import { logger } from "./logger";
import { sendSms } from "./telnyx";

export function buildLiveTransferSmsBody(state: CallState): string {
  return `Live transfer requested. Row ${state.rowNumber}. ${state.fullName}. ${state.listingAddress}. Call target: ${state.destinationPhone}. Call back if available.`;
}

export async function notifyLiveTransferRequested(state: CallState): Promise<"sms_sent" | "logged_only" | "sms_failed"> {
  const smsBody = buildLiveTransferSmsBody(state);

  logger.info(`LIVE TRANSFER REQUESTED: row ${state.rowNumber}, ${state.fullName}, ${state.listingAddress}`, {
    rowNumber: state.rowNumber,
    fullName: state.fullName,
    listingAddress: state.listingAddress,
    destinationPhone: state.destinationPhone,
    smsEnabled: config.yoniAlertSms.enabled,
    smsDestinationNumber: config.yoniAlertSms.destinationNumber,
    smsBody,
  });

  // TODO: Add push notification or escalation fanout here if SMS is not enough.
  if (!config.yoniAlertSms.enabled) {
    logger.info("Live transfer alert logged only; SMS disabled", {
      rowNumber: state.rowNumber,
      fullName: state.fullName,
      listingAddress: state.listingAddress,
      destinationPhone: state.destinationPhone,
      smsSent: false,
    });
    return "logged_only";
  }

  try {
    await sendSms({
      to: config.yoniAlertSms.destinationNumber,
      text: smsBody,
    });

    logger.info("Live transfer SMS alert sent", {
      rowNumber: state.rowNumber,
      fullName: state.fullName,
      listingAddress: state.listingAddress,
      destinationPhone: state.destinationPhone,
      smsDestinationNumber: config.yoniAlertSms.destinationNumber,
      smsSent: true,
    });
    return "sms_sent";
  } catch (error) {
    logger.error("Live transfer SMS alert failed; continuing call flow", {
      rowNumber: state.rowNumber,
      fullName: state.fullName,
      listingAddress: state.listingAddress,
      destinationPhone: state.destinationPhone,
      smsDestinationNumber: config.yoniAlertSms.destinationNumber,
      smsSent: false,
      message: error instanceof Error ? error.message : String(error),
    });
    return "sms_failed";
  }
}
