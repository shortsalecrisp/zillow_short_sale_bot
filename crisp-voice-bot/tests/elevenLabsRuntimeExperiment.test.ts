import assert from "node:assert/strict";
import test from "node:test";
import {
  buildTurnV3Patch,
  buildV4TurboBranchOverrides,
  ELEVENLABS_TTS_TEST_MODEL,
  ELEVENLABS_TURN_MODEL,
  expectedRuntimeConfig,
  getElevenLabsRuntimeExperimentStatus,
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

test("runtime status declares a 50/50 Maya/Eryn experiment with turn_v3 on both arms", () => {
  const status = getElevenLabsRuntimeExperimentStatus();
  assert.equal(status.enabled, true);
  assert.equal(status.commonSettings.publicAssistantName, "Maya");
  assert.equal(status.commonSettings.voiceName, "Eryn");
  assert.equal(status.commonSettings.turnModel, "turn_v3");
  assert.deepEqual(status.arms.map((arm) => arm.trafficPercent), [50, 50]);
  assert.deepEqual(status.arms.map((arm) => arm.ttsModel), ["eleven_flash_v2", "eleven_v4_turbo"]);
});
