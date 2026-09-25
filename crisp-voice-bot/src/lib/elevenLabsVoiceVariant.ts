import { config } from "./config";

export type ElevenLabsVoiceVariantKey = "eryn" | "finch";

export type ElevenLabsVoiceVariant = {
  key: ElevenLabsVoiceVariantKey;
  assistantName: "Maya";
  voiceName: "Eryn";
  voiceId: string;
  ttsSpeed?: number;
};

const ERYN_ASSISTANT_NAME = "Maya";

function buildVoiceVariants(): ElevenLabsVoiceVariant[] {
  return [
    {
      key: "eryn",
      assistantName: ERYN_ASSISTANT_NAME,
      voiceName: "Eryn",
      voiceId: config.elevenLabs.voiceId ?? config.elevenLabs.erynVoiceId,
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
    selectionRule: config.elevenLabs.voiceAbTestEnabled
      ? "fixed_primary_voice; Finch removed by owner approval"
      : "fixed_primary_voice",
    publicAssistantName: "Maya",
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

  void input;
  return variants[0];
}
