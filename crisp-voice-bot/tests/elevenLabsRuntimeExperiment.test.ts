import assert from "node:assert/strict";
import test from "node:test";
import {
  buildTurnV3Patch,
  buildV4TurboBranchOverrides,
  ELEVENLABS_TTS_TEST_MODEL,
  ELEVENLABS_TURN_MODEL,
  expectedRuntimeConfig,
  getElevenLabsRuntimeExperimentStatus,
  ELEVENLABS_TTS_COMPARISON_ENDED_AT,
  ELEVENLABS_V4_PRODUCTION,
  ELEVENLABS_V4_PRODUCTION_RELEASES,
  ELEVENLABS_UNIFORM_COMPARISON,
  uniformComparisonArm,
  v4ProductionReceiptMatched,
  v4ProductionReceiptRelease,
} from "../src/lib/elevenLabsRuntimeExperiment";

function baseline() {
  return {
    turn: {
      turn_timeout: 1.5,
      initial_wait_time: 1.6,
      turn_model: "turn_v2",
      retranscribe_on_turn_timeout: true,
    },
    tts: {
      model_id: "eleven_flash_v2",
      voice_id: "eryn-voice-id",
      stability: 0.55,
      similarity_boost: 0.8,
      speed: 0.95,
    },
    agent: { prompt: { prompt: "unchanged" } },
  };
}

test("turn patch preserves all existing turn settings and changes only the turn model", () => {
  const current = baseline();
  const before = structuredClone(current);
  assert.deepEqual(buildTurnV3Patch(current), {
    conversation_config: {
      turn: { ...current.turn, turn_model: ELEVENLABS_TURN_MODEL },
    },
  });
  assert.deepEqual(current, before);
});

test("v4 branch keeps Maya voice settings and adopts turn_v3", () => {
  const current = baseline();
  const overrides = buildV4TurboBranchOverrides(current);
  assert.equal(overrides.conversation_config.turn.turn_model, "turn_v3");
  assert.equal(overrides.conversation_config.tts.model_id, ELEVENLABS_TTS_TEST_MODEL);
  assert.equal(overrides.conversation_config.tts.voice_id, "eryn-voice-id");
  assert.equal(overrides.conversation_config.tts.stability, 0.55);
  assert.equal(overrides.conversation_config.tts.similarity_boost, 0.8);
  assert.equal(overrides.conversation_config.tts.speed, 0.95);
  assert.equal(current.tts.speed, 0.95);
});

test("expected branch configs preserve unrelated agent settings", () => {
  const current = baseline();
  const expected = expectedRuntimeConfig(current, ELEVENLABS_TTS_TEST_MODEL);
  assert.deepEqual(expected.agent, current.agent);
  assert.equal(expected.turn?.turn_model, "turn_v3");
  assert.equal(expected.tts?.model_id, ELEVENLABS_TTS_TEST_MODEL);
  assert.equal(expected.tts?.speed, 0.95);
});

test("runtime declares owner-selected v4-only production and closes the historical experiment", () => {
  const status = getElevenLabsRuntimeExperimentStatus();
  assert.equal(status.enabled, false);
  assert.equal(status.mode, "v4_production");
  assert.equal(status.commonSettings.publicAssistantName, "Maya");
  assert.equal(status.commonSettings.voiceName, "Eryn");
  assert.equal(status.commonSettings.turnModel, "turn_v3");
  assert.deepEqual(status.arms.map((arm) => arm.trafficPercent), [0, 100]);
  assert.deepEqual(status.arms.map((arm) => arm.ttsModel), ["eleven_flash_v2", "eleven_v4_turbo"]);
});

test("new production calls and the ambiguous cutover second never enter the historical comparison", () => {
  const input = { finalReceiptMatched: true, testMode: false, durationSecs: 20,
    startTimeUnixSecs: Math.ceil(Date.parse(ELEVENLABS_UNIFORM_COMPARISON.startedAt) / 1000),
    branchId: ELEVENLABS_V4_PRODUCTION.branchId, versionId: ELEVENLABS_UNIFORM_COMPARISON.testVersionId };
  assert.equal(uniformComparisonArm(input), "v4_turbo_test");
  assert.equal(v4ProductionReceiptMatched(input), false);
  input.startTimeUnixSecs = Math.floor(Date.parse(ELEVENLABS_TTS_COMPARISON_ENDED_AT) / 1000);
  assert.equal(uniformComparisonArm(input), null);
  assert.equal(v4ProductionReceiptMatched(input), false);
  input.startTimeUnixSecs = Math.ceil(Date.parse(ELEVENLABS_V4_PRODUCTION.startedAt) / 1000);
  assert.equal(uniformComparisonArm(input), null);
  assert.equal(v4ProductionReceiptMatched(input), true);
  for (const edit of [{ testMode: true }, { finalReceiptMatched: false }, { versionId: 'unknown' },
    { branchId: 'unknown' }, { durationSecs: 0 }, { durationSecs: Number.NaN }, { startTimeUnixSecs: null }]) {
    assert.equal(v4ProductionReceiptMatched({ ...input, ...edit }), false);
  }
});

test("production script repair preserves the adoption boundary and older receipt phase without admitting transition seconds", () => {
  const [old, current] = ELEVENLABS_V4_PRODUCTION_RELEASES;
  const input = { finalReceiptMatched: true, testMode: false, durationSecs: 20,
    branchId: ELEVENLABS_V4_PRODUCTION.branchId, versionId: old.versionId,
    startTimeUnixSecs: Math.ceil(Date.parse(old.releaseStartedAt) / 1000) };
  assert.equal(v4ProductionReceiptRelease(input)?.releaseId, old.releaseId);
  assert.equal(ELEVENLABS_V4_PRODUCTION.startedAt, "2026-11-25T17:40:58.963Z");
  assert.equal(ELEVENLABS_UNIFORM_COMPARISON.testVersionId, old.versionId);
  input.startTimeUnixSecs = Math.floor(Date.parse(current.releaseStartedAt) / 1000);
  assert.equal(v4ProductionReceiptRelease(input), null);
  input.versionId = current.versionId;
  assert.equal(v4ProductionReceiptRelease(input), null);
  input.startTimeUnixSecs = Math.ceil(Date.parse(current.releaseStartedAt) / 1000);
  assert.equal(v4ProductionReceiptRelease(input)?.releaseId, current.releaseId);
  input.versionId = old.versionId;
  assert.equal(v4ProductionReceiptRelease(input), null);
});
