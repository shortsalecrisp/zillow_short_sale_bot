import crypto from "node:crypto";
import type { Readable } from "node:stream";
import axios from "axios";
import { config } from "./config";

const PLAYBACK_SIG_LENGTH = 32;
const CONVERSATION_ID_PATTERN = /^conv_[a-zA-Z0-9]+$/;

export function assertValidElevenLabsConversationId(conversationId: string): void {
  if (!CONVERSATION_ID_PATTERN.test(conversationId)) {
    throw new Error("Invalid ElevenLabs conversation id");
  }
}

function getPlaybackSigningSecret(): string {
  const secret = config.elevenLabs.toolSecret ?? config.elevenLabs.apiKey;

  if (!secret) {
    throw new Error("Missing ElevenLabs playback signing secret");
  }

  return secret;
}

export function signElevenLabsPlayback(conversationId: string): string {
  assertValidElevenLabsConversationId(conversationId);

  return crypto
    .createHmac("sha256", getPlaybackSigningSecret())
    .update(conversationId)
    .digest("hex")
    .slice(0, PLAYBACK_SIG_LENGTH);
}

export function verifyElevenLabsPlaybackSignature(conversationId: string, signature?: string | null): boolean {
  if (!signature || !/^[a-f0-9]{32}$/i.test(signature)) {
    return false;
  }

  try {
    const expected = Buffer.from(signElevenLabsPlayback(conversationId), "hex");
    const actual = Buffer.from(signature, "hex");
    return expected.length === actual.length && crypto.timingSafeEqual(expected, actual);
  } catch {
    return false;
  }
}

export function buildElevenLabsPlaybackUrl(conversationId: string): string {
  assertValidElevenLabsConversationId(conversationId);

  const encodedConversationId = encodeURIComponent(conversationId);
  const signature = signElevenLabsPlayback(conversationId);
  return `${config.baseUrl}/elevenlabs/playback/${encodedConversationId}?sig=${signature}`;
}

export async function fetchElevenLabsConversationAudio(conversationId: string): Promise<{
  stream: Readable;
  contentType: string;
}> {
  assertValidElevenLabsConversationId(conversationId);

  if (!config.elevenLabs.apiKey) {
    throw new Error("Missing ELEVENLABS_API_KEY");
  }

  const response = await axios.get<Readable>(
    `${config.elevenLabs.baseUrl}/v1/convai/conversations/${conversationId}/audio`,
    {
      responseType: "stream",
      timeout: 30_000,
      headers: {
        "xi-api-key": config.elevenLabs.apiKey,
      },
    },
  );

  return {
    stream: response.data,
    contentType:
      typeof response.headers["content-type"] === "string" && response.headers["content-type"].trim()
        ? response.headers["content-type"]
        : "audio/mpeg",
  };
}
