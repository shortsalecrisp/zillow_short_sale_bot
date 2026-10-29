export const ELEVENLABS_TURN_MODEL = "turn_v3";
export const ELEVENLABS_TTS_CONTROL_MODEL = "eleven_flash_v2";
export const ELEVENLABS_TTS_TEST_MODEL = "eleven_v4_turbo";
export const ELEVENLABS_TTS_EXPERIMENT_BRANCH_NAME = "crisp-v4-turbo-turn-v3-20260928";
export const ELEVENLABS_TTS_EXPERIMENT_STARTED_AT = "2026-09-28T22:30:50.974Z";
export const ELEVENLABS_TTS_CONTROL_BRANCH_ID = "agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks";
export const ELEVENLABS_TTS_TEST_BRANCH_ID = "agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv";

export const ELEVENLABS_UNIFORM_COMPARISON = Object.freeze({
  cohort: "maya_uniform_lead_conversion_20261029_r11",
  startedAt: "2026-10-29T18:51:46.460Z",
  controlVersionId: "agtvrsn_0901m64g1vjxfrqabsamjpzph4wk",
  testVersionId: "agtvrsn_5301m64g1z3kf1ssnxk5rwncswga",
  sharedConfigSha256: "aac44538ffbdd2353425a3aaaaf954f4bb8b6348bda99106ccc96489fc92e025",
  workflowSha256: "302126b2cc13ddd3f7f1252b0b3773a365f28af8b591029495ab7a1afc149363",
  waitingAudio: { sound: "elevator1", availabilityToolId: "tool_4101m64g0xqteevsgfwtpgxv4he6",
    availabilityToolHash: "6eb7797a911ea9dc3b2feb834932fa63f38f7121858f22476eb54a245f7fae63" },
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
