export const ELEVENLABS_TURN_MODEL = "turn_v3";
export const ELEVENLABS_TTS_CONTROL_MODEL = "eleven_flash_v2";
export const ELEVENLABS_TTS_TEST_MODEL = "eleven_v4_turbo";
export const ELEVENLABS_TTS_EXPERIMENT_BRANCH_NAME = "crisp-v4-turbo-turn-v3-20260928";
export const ELEVENLABS_TTS_EXPERIMENT_STARTED_AT = "2026-09-28T22:30:50.974Z";
export const ELEVENLABS_TTS_CONTROL_BRANCH_ID = "agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks";
export const ELEVENLABS_TTS_TEST_BRANCH_ID = "agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv";

export function getElevenLabsRuntimeExperimentStatus() {
  return {
    enabled: true,
    startedAt: ELEVENLABS_TTS_EXPERIMENT_STARTED_AT,
    selectionRule: "ElevenLabs native deterministic branch traffic split",
    commonSettings: {
      publicAssistantName: "Maya",
      voiceName: "Eryn",
      turnModel: ELEVENLABS_TURN_MODEL,
    },
    arms: [
      {
        key: "flash_v2_control",
        branchId: ELEVENLABS_TTS_CONTROL_BRANCH_ID,
        ttsModel: ELEVENLABS_TTS_CONTROL_MODEL,
        trafficPercent: 50,
      },
      {
        key: "v4_turbo_test",
        branchId: ELEVENLABS_TTS_TEST_BRANCH_ID,
        ttsModel: ELEVENLABS_TTS_TEST_MODEL,
        trafficPercent: 50,
      },
    ],
  };
}

export type ElevenLabsRuntimeConfig = {
  turn?: Record<string, unknown>;
  tts?: Record<string, unknown>;
  [key: string]: unknown;
};

export function buildTurnV3Patch(conversationConfig: ElevenLabsRuntimeConfig) {
  return {
    conversation_config: {
      turn: {
        ...(conversationConfig.turn ?? {}),
        turn_model: ELEVENLABS_TURN_MODEL,
      },
    },
  };
}

export function buildV4TurboBranchOverrides(conversationConfig: ElevenLabsRuntimeConfig) {
  const tts: Record<string, unknown> = {
    ...(conversationConfig.tts ?? {}),
    model_id: ELEVENLABS_TTS_TEST_MODEL,
  };

  return {
    conversation_config: {
      turn: {
        ...(conversationConfig.turn ?? {}),
        turn_model: ELEVENLABS_TURN_MODEL,
      },
      tts,
    },
  };
}

export function expectedRuntimeConfig(
  conversationConfig: ElevenLabsRuntimeConfig,
  modelId: string,
): ElevenLabsRuntimeConfig {
  const expected = structuredClone(conversationConfig);
  expected.turn = {
    ...(expected.turn ?? {}),
    turn_model: ELEVENLABS_TURN_MODEL,
  };
  expected.tts = {
    ...(expected.tts ?? {}),
    model_id: modelId,
  };

  return expected;
}
