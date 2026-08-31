import { config } from "./config";

export type ElevenLabsVoiceVariantKey = "eryn" | "finch";

export type ElevenLabsVoiceVariant = {
  key: ElevenLabsVoiceVariantKey;
  assistantName: "Maya" | "Finn";
  voiceName: "Eryn" | "Finch";
  voiceId: string;
  ttsSpeed?: number;
};

const ERYN_ASSISTANT_NAME = "Maya";
const FINCH_ASSISTANT_NAME = "Finn";

function buildVoiceVariants(): ElevenLabsVoiceVariant[] {
  return [
    {
      key: "eryn",
      assistantName: ERYN_ASSISTANT_NAME,
      voiceName: "Eryn",
      voiceId: config.elevenLabs.voiceId ?? config.elevenLabs.erynVoiceId,
    },
    {
      key: "finch",
      assistantName: FINCH_ASSISTANT_NAME,
      voiceName: "Finch",
      voiceId: config.elevenLabs.finchVoiceId,
      ttsSpeed: config.elevenLabs.finchTtsSpeed,
    },
  ];
}

export function findElevenLabsVoiceVariant(key: string): ElevenLabsVoiceVariant | undefined {
  return buildVoiceVariants().find((variant) => variant.key === key);
}

export function getElevenLabsVoiceExperimentStatus() {
  const variants = buildVoiceVariants();

  return {
    enabled: config.elevenLabs.voiceAbTestEnabled,
    selectionRule: config.elevenLabs.voiceAbTestEnabled ? "abs(rowNumber) % voiceCount" : "fixed_primary_voice",
    publicAssistantName: "Maya/Finn",
    variants: variants.map((variant) => ({
      key: variant.key,
      voiceName: variant.voiceName,
      assistantName: variant.assistantName,
      voiceIdConfigured: Boolean(variant.voiceId),
      ttsSpeed: variant.ttsSpeed ?? null,
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
