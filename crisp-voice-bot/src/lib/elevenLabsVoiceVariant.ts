import { config } from "./config";

export type ElevenLabsVoiceVariantKey = "eryn" | "finch" | "rachel" | "bella";

export type ElevenLabsVoiceVariant = {
  key: ElevenLabsVoiceVariantKey;
  assistantName: "Maya";
  voiceName: "Eryn" | "Finch" | "Rachel" | "Bella";
  voiceId: string;
};

const ASSISTANT_NAME = "Maya";

function buildVoiceVariants(): ElevenLabsVoiceVariant[] {
  return [
    {
      key: "eryn",
      assistantName: ASSISTANT_NAME,
      voiceName: "Eryn",
      voiceId: config.elevenLabs.voiceId ?? config.elevenLabs.erynVoiceId,
    },
    {
      key: "finch",
      assistantName: ASSISTANT_NAME,
      voiceName: "Finch",
      voiceId: config.elevenLabs.finchVoiceId,
    },
    {
      key: "rachel",
      assistantName: ASSISTANT_NAME,
      voiceName: "Rachel",
      voiceId: config.elevenLabs.rachelVoiceId,
    },
    {
      key: "bella",
      assistantName: ASSISTANT_NAME,
      voiceName: "Bella",
      voiceId: config.elevenLabs.bellaVoiceId,
    },
  ];
}

export function getElevenLabsVoiceExperimentStatus() {
  const variants = buildVoiceVariants();

  return {
    enabled: config.elevenLabs.voiceAbTestEnabled,
    selectionRule: config.elevenLabs.voiceAbTestEnabled ? "abs(rowNumber) % voiceCount" : "fixed_primary_voice",
    publicAssistantName: ASSISTANT_NAME,
    variants: variants.map((variant) => ({
      key: variant.key,
      voiceName: variant.voiceName,
      assistantName: variant.assistantName,
      voiceIdConfigured: Boolean(variant.voiceId),
    })),
  };
}

export function selectElevenLabsVoiceVariant(input: { rowNumber: number }): ElevenLabsVoiceVariant {
  const variants = buildVoiceVariants();

  if (!config.elevenLabs.voiceAbTestEnabled) {
    return variants[0];
  }

  return variants[Math.abs(input.rowNumber) % variants.length];
}
