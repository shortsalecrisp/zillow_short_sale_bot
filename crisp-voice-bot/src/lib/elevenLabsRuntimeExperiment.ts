export const ELEVENLABS_TURN_MODEL = "turn_v3";
export const ELEVENLABS_TTS_CONTROL_MODEL = "eleven_flash_v2";
export const ELEVENLABS_TTS_TEST_MODEL = "eleven_v4_turbo";
export const ELEVENLABS_TTS_EXPERIMENT_BRANCH_NAME = "crisp-v4-turbo-turn-v3-20260928";
export const ELEVENLABS_TTS_EXPERIMENT_STARTED_AT = "2026-09-28T22:30:50.974Z";
export const ELEVENLABS_TTS_CONTROL_BRANCH_ID = "agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks";
export const ELEVENLABS_TTS_TEST_BRANCH_ID = "agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv";

export const ELEVENLABS_UNIFORM_COMPARISON = Object.freeze({
  cohort: "maya_uniform_lead_conversion_20261016_r4",
  startedAt: "2026-10-16T12:57:55.149Z",
  controlVersionId: "agtvrsn_4601m4yhj7xsejw8wcm77xkcn08x",
  testVersionId: "agtvrsn_3401m52cmpgcecsbsfd89s046bsp",
  sharedConfigSha256: "7f8f349d8b4e16f3aa5263933707d2fe1f2fd464de88cfa9acdd37611d38cab4",
  workflowSha256: "f2d992bbd8c2b0d082910bf13490ee8305a3080eee32a38b204aeef9f98a785b",
  minimumTrueLiveConversationsPerArm: 15,
  dedicatedReviewGenuineCalls: 100,
  primaryOutcome: "Verified positive handoffs per genuine call; require caller consent and completion evidence",
  analysisRule: "Require a matching final receipt, exact branch/version below, start at or after this boundary, positive duration and non-test call. Older calls are historical only. Stratify scheduledWindow, listing-local time and lead quality. No winner until each arm has 15 verified true live-agent conversations; prefer the dedicated 100-genuine-call review. Missing identity/start evidence is unknown, never inferred from current settings. A TTS difference does not prove a turn-model effect.",
});

export function uniformComparisonArm(input: { finalReceiptMatched: boolean; testMode: boolean; durationSecs: number | null;
  startTimeUnixSecs: unknown; branchId: unknown; versionId: unknown }): "flash_v2_control" | "v4_turbo_test" | null {
  if (!input.finalReceiptMatched || input.testMode || input.durationSecs === null || input.durationSecs <= 0
    || typeof input.startTimeUnixSecs !== "number" || !Number.isFinite(input.startTimeUnixSecs)
    || input.startTimeUnixSecs < Math.ceil(Date.parse(ELEVENLABS_UNIFORM_COMPARISON.startedAt) / 1000)) return null;
  if (input.branchId === ELEVENLABS_TTS_CONTROL_BRANCH_ID && input.versionId === ELEVENLABS_UNIFORM_COMPARISON.controlVersionId) return "flash_v2_control";
  if (input.branchId === ELEVENLABS_TTS_TEST_BRANCH_ID && input.versionId === ELEVENLABS_UNIFORM_COMPARISON.testVersionId) return "v4_turbo_test";
  return null;
}

export function getElevenLabsRuntimeExperimentStatus() {
  return {
    enabled: true,
    startedAt: ELEVENLABS_TTS_EXPERIMENT_STARTED_AT,
    uniformComparison: ELEVENLABS_UNIFORM_COMPARISON,
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
