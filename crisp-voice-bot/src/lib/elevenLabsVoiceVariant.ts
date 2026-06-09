import { config } from "./config";

export type ElevenLabsVoiceVariantKey = "eryn" | "finch";

export type ElevenLabsVoiceVariant = {
  key: ElevenLabsVoiceVariantKey;
  assistantName: "Maya" | "Emmy" | "Finch";
  voiceName: "Eryn" | "Finch";
  voiceId: string;
};

export function selectElevenLabsVoiceVariant(_input: { rowNumber: number }): ElevenLabsVoiceVariant {
  return {
    key: "eryn",
    assistantName: "Maya",
    voiceName: "Eryn",
    voiceId: config.elevenLabs.voiceId ?? config.elevenLabs.erynVoiceId,
  };
}
