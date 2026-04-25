import { Router, type NextFunction, type Request, type Response } from "express";
import { saveInitialCallState } from "../lib/callState";
import { config } from "../lib/config";
import { placeElevenLabsOutboundCall } from "../lib/elevenLabs";
import { logger } from "../lib/logger";
import { placeOutboundCall } from "../lib/telnyx";
import type { CallMetadata, StartCallRequest } from "../types";

const router = Router();

const E164_PHONE = /^\+[1-9]\d{6,14}$/;

type ValidatedStartCallRequest = StartCallRequest & {
  fullName: string;
  listingAddress: string;
  phone: string;
  rowNumber: number;
};

class ValidationError extends Error {
  public readonly statusCode = 400;

  constructor(message: string) {
    super(message);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function readString(body: Record<string, unknown>, key: keyof StartCallRequest, required = false): string | undefined {
  const value = body[key];

  if (value === undefined || value === null || value === "") {
    if (required) {
      throw new ValidationError(`${String(key)} is required`);
    }

    return undefined;
  }

  if (typeof value !== "string") {
    throw new ValidationError(`${String(key)} must be a string`);
  }

  const trimmed = value.trim();
  if (trimmed === "") {
    if (required) {
      throw new ValidationError(`${String(key)} is required`);
    }

    return undefined;
  }

  return trimmed;
}

function readPositiveInteger(
  body: Record<string, unknown>,
  key: keyof StartCallRequest,
  fallback: number,
): number {
  const value = body[key];

  if (value === undefined || value === null || value === "") {
    return fallback;
  }

  if (typeof value === "number" && Number.isInteger(value) && value > 0) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isInteger(parsed) && parsed > 0) {
      return parsed;
    }
  }

  throw new ValidationError(`${String(key)} must be a positive integer when provided`);
}

function validateStartCallRequest(body: unknown): ValidatedStartCallRequest {
  if (!isRecord(body)) {
    throw new ValidationError("Request body must be a JSON object");
  }

  const rowNumber = body.rowNumber;
  if (typeof rowNumber !== "number" || !Number.isInteger(rowNumber) || rowNumber <= 0) {
    throw new ValidationError("rowNumber must be a positive integer");
  }

  const firstName = readString(body, "firstName");
  const lastName = readString(body, "lastName");
  const providedFullName = readString(body, "fullName");
  const fullName = providedFullName ?? ([firstName, lastName].filter(Boolean).join(" ") || "Unknown");
  const phone = readString(body, "phone", true)!;
  const listingAddress = readString(body, "listingAddress", true)!;

  if (!E164_PHONE.test(phone)) {
    throw new ValidationError("phone must be in E.164 format, for example +19542053205");
  }

  const email = readString(body, "email");
  if (email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    throw new ValidationError("email must be a valid email address when provided");
  }

  return {
    rowNumber,
    firstName,
    lastName,
    fullName,
    callAttemptNumber: readPositiveInteger(body, "callAttemptNumber", 1),
    phone,
    email,
    listingAddress,
    createdAt: readString(body, "createdAt"),
    scheduledForEt: readString(body, "scheduledForEt"),
    responseStatus: readString(body, "responseStatus"),
    notes: readString(body, "notes"),
    sheetName: readString(body, "sheetName"),
  };
}

router.post("/", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const payload = validateStartCallRequest(req.body);
    const dialedPhone = config.testMode ? config.testDestinationNumber : payload.phone;

    if (!E164_PHONE.test(dialedPhone)) {
      throw new ValidationError("Configured destination number must be in E.164 format");
    }

    const metadata: CallMetadata = {
      rowNumber: payload.rowNumber,
      firstName: payload.firstName,
      lastName: payload.lastName,
      fullName: payload.fullName,
      email: payload.email,
      callAttemptNumber: payload.callAttemptNumber ?? 1,
      listingAddress: payload.listingAddress,
      sheetName: payload.sheetName,
      requestedPhone: payload.phone,
      dialedPhone,
      testMode: config.testMode,
    };

    logger.info("Starting outbound call attempt", {
      rowNumber: payload.rowNumber,
      providedPhone: payload.phone,
      actualDialedPhone: dialedPhone,
      listingAddress: payload.listingAddress,
      callAttemptNumber: payload.callAttemptNumber ?? 1,
      testMode: config.testMode,
    });

    if (config.voiceProvider === "elevenlabs") {
      try {
        const elevenLabsResponse = await placeElevenLabsOutboundCall({
          to: dialedPhone,
          metadata,
        });

        res.status(202).json({
          ok: true,
          provider: "elevenlabs",
          testMode: config.testMode,
          requestedPhone: payload.phone,
          dialedPhone,
          rowNumber: payload.rowNumber,
          elevenLabs: {
            conversationId: elevenLabsResponse.conversation_id,
            sipCallId: elevenLabsResponse.sip_call_id,
            message: elevenLabsResponse.message,
          },
        });
        return;
      } catch (error) {
        logger.error("ElevenLabs call failed; not falling back to Telnyx because VOICE_PROVIDER=elevenlabs", {
          rowNumber: payload.rowNumber,
          message: error instanceof Error ? error.message : String(error),
        });
        res.status(502).json({
          ok: false,
          provider: "elevenlabs",
          error: "ElevenLabs outbound call failed",
          message: error instanceof Error ? error.message : String(error),
        });
        return;
      }
    }

    const telnyxResponse = await placeOutboundCall({
      to: dialedPhone,
      metadata,
    });

    if (telnyxResponse.data?.call_control_id) {
      saveInitialCallState(telnyxResponse.data.call_control_id, metadata);
    }

    logger.info("Outbound call accepted by Telnyx", {
      rowNumber: payload.rowNumber,
      providedPhone: payload.phone,
      actualDialedPhone: dialedPhone,
      listingAddress: payload.listingAddress,
      fullName: payload.fullName,
      callControlId: telnyxResponse.data?.call_control_id,
      callLegId: telnyxResponse.data?.call_leg_id,
      testMode: config.testMode,
    });

    res.status(202).json({
      ok: true,
      provider: "telnyx",
      testMode: config.testMode,
      requestedPhone: payload.phone,
      dialedPhone,
      rowNumber: payload.rowNumber,
      telnyx: {
        callControlId: telnyxResponse.data?.call_control_id,
        callLegId: telnyxResponse.data?.call_leg_id,
        callSessionId: telnyxResponse.data?.call_session_id,
      },
    });
  } catch (error) {
    next(error);
  }
});

export default router;
