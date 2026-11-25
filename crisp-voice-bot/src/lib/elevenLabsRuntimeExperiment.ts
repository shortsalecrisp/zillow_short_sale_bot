export const ELEVENLABS_TURN_MODEL = "turn_v3";
export const ELEVENLABS_TTS_CONTROL_MODEL = "eleven_flash_v2";
export const ELEVENLABS_TTS_TEST_MODEL = "eleven_v4_turbo";
export const ELEVENLABS_TTS_EXPERIMENT_BRANCH_NAME = "crisp-v4-turbo-turn-v3-20260928";
export const ELEVENLABS_TTS_EXPERIMENT_STARTED_AT = "2026-09-28T22:30:50.974Z";
export const ELEVENLABS_TTS_CONTROL_BRANCH_ID = "agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks";
export const ELEVENLABS_TTS_TEST_BRANCH_ID = "agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv";
export const ELEVENLABS_TTS_COMPARISON_ENDED_AT = "2026-11-25T17:40:58.101Z";
export const ELEVENLABS_V4_PRODUCTION = Object.freeze({
  mode: "v4_production",
  startedAt: "2026-11-25T17:40:58.963Z",
  branchId: ELEVENLABS_TTS_TEST_BRANCH_ID,
  versionId: "agtvrsn_1301m89e87r0expr4tenxnxjqngj",
  ttsModel: ELEVENLABS_TTS_TEST_MODEL,
  trafficPercent: 100,
  selectionReason: "Owner-selected production model; not a statistically established A/B winner",
});

export const ELEVENLABS_UNIFORM_COMPARISON = Object.freeze({
  cohort: "maya_uniform_lead_conversion_20261125_r28",
  startedAt: "2026-11-25T13:27:50.274Z",
  endedAt: ELEVENLABS_TTS_COMPARISON_ENDED_AT,
  controlVersionId: "agtvrsn_5901m89e84k7f28rmq7bqzwshx2q",
  testVersionId: "agtvrsn_1301m89e87r0expr4tenxnxjqngj",
  sharedConfigSha256: "589bcded16b57433a442cd0ab5caaeda0e7786c00f7288cbe7b774295d9d301c",
  workflowSha256: "7267d6f9a5cb7321486d349f81ee5040dbb409d59baa5832076d1b2ce0b12a74",
  waitingAudio: { sound: "elevator1", availabilityToolId: "tool_4101m64g0xqteevsgfwtpgxv4he6",
    availabilityToolHash: "6eb7797a911ea9dc3b2feb834932fa63f38f7121858f22476eb54a245f7fae63" },
  minimumTrueLiveConversationsPerArm: 15,
  dedicatedReviewGenuineCalls: 100,
  primaryOutcome: "Verified positive handoffs per genuine call; require caller consent and completion evidence",
  analysisRule: "Require a matching final receipt, exact branch/version below, start at or after this boundary, positive duration and non-test call. Older calls are historical only. Stratify scheduledWindow, listing-local time and lead quality. No winner until each arm has 15 verified true live-agent conversations; prefer the dedicated 100-genuine-call review. Missing identity/start evidence is unknown, never inferred from current settings. A TTS difference does not prove a turn-model effect.",
});

type FinalReceiptInput = { finalReceiptMatched: boolean; testMode: boolean; durationSecs: number | null;
  startTimeUnixSecs: unknown; branchId: unknown; versionId: unknown };

export function uniformComparisonArm(input: FinalReceiptInput): "flash_v2_control" | "v4_turbo_test" | null {
  if (!input.finalReceiptMatched || input.testMode || input.durationSecs === null || input.durationSecs <= 0
    || typeof input.startTimeUnixSecs !== "number" || !Number.isFinite(input.startTimeUnixSecs)
    || input.startTimeUnixSecs < Math.ceil(Date.parse(ELEVENLABS_UNIFORM_COMPARISON.startedAt) / 1000)
    || input.startTimeUnixSecs >= Math.floor(Date.parse(ELEVENLABS_TTS_COMPARISON_ENDED_AT) / 1000)) return null;
  if (input.branchId === ELEVENLABS_TTS_CONTROL_BRANCH_ID && input.versionId === ELEVENLABS_UNIFORM_COMPARISON.controlVersionId) return "flash_v2_control";
  if (input.branchId === ELEVENLABS_TTS_TEST_BRANCH_ID && input.versionId === ELEVENLABS_UNIFORM_COMPARISON.testVersionId) return "v4_turbo_test";
  return null;
}

export function v4ProductionReceiptMatched(input: FinalReceiptInput): boolean {
  return input.finalReceiptMatched && !input.testMode && typeof input.durationSecs === "number"
    && Number.isFinite(input.durationSecs) && input.durationSecs > 0
    && typeof input.startTimeUnixSecs === "number" && Number.isFinite(input.startTimeUnixSecs)
    && input.startTimeUnixSecs >= Math.ceil(Date.parse(ELEVENLABS_V4_PRODUCTION.startedAt) / 1000)
    && input.branchId === ELEVENLABS_V4_PRODUCTION.branchId && input.versionId === ELEVENLABS_V4_PRODUCTION.versionId;
}

export function getElevenLabsRuntimeExperimentStatus() {
  return {
    enabled: false,
    mode: "v4_production",
    startedAt: ELEVENLABS_TTS_EXPERIMENT_STARTED_AT,
    endedAt: ELEVENLABS_TTS_COMPARISON_ENDED_AT,
    production: ELEVENLABS_V4_PRODUCTION,
    uniformComparison: ELEVENLABS_UNIFORM_COMPARISON,
    selectionRule: "Owner-approved ElevenLabs native allocation: v4 Turbo 100%, Flash v2 0%",
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
        trafficPercent: 0,
      },
      {
        key: "v4_turbo_test",
        branchId: ELEVENLABS_TTS_TEST_BRANCH_ID,
        ttsModel: ELEVENLABS_TTS_TEST_MODEL,
        trafficPercent: 100,
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
