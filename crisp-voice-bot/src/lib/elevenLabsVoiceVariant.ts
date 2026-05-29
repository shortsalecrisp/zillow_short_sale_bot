import { config } from "./config";

export type ElevenLabsVoiceVariantKey = "eryn" | "finch";

export type ElevenLabsVoiceVariant = {
  key: ElevenLabsVoiceVariantKey;
  assistantName: "Emmy" | "Finch";
  voiceName: "Eryn" | "Finch";
  voiceId: string;
};

export function selectElevenLabsVoiceVariant(input: { rowNumber: number }): ElevenLabsVoiceVariant {
  if (!config.elevenLabs.voiceAbTestEnabled) {
    return {
      key: "eryn",
      assistantName: "Emmy",
      voiceName: "Eryn",
      voiceId: config.elevenLabs.voiceId ?? config.elevenLabs.erynVoiceId,
    };
  }

  if (input.rowNumber % 2 === 0) {
    return {
      key: "eryn",
      assistantName: "Emmy",
      voiceName: "Eryn",
      voiceId: config.elevenLabs.erynVoiceId,
    };
  }

  return {
    key: "finch",
    assistantName: "Finch",
    voiceName: "Finch",
    voiceId: config.elevenLabs.finchVoiceId,
  };
}
