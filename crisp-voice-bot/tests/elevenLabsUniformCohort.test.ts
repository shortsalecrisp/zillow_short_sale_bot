import { test } from "node:test";
import assert from "node:assert/strict";
import { ELEVENLABS_UNIFORM_COMPARISON as cohort, ELEVENLABS_TTS_CONTROL_BRANCH_ID as branchId,
  ELEVENLABS_TTS_TEST_BRANCH_ID, uniformComparisonArm, getElevenLabsRuntimeExperimentStatus } from "../src/lib/elevenLabsRuntimeExperiment";
const eligible = () => ({ finalReceiptMatched: true, testMode: false, durationSecs: 25,
  startTimeUnixSecs: Math.ceil(Date.parse(cohort.startedAt) / 1000), branchId, versionId: cohort.controlVersionId });
test("includes only exact matching final versions after the uniform boundary", () => {
  assert.equal(uniformComparisonArm(eligible()), "flash_v2_control");
  assert.equal(uniformComparisonArm({ ...eligible(), branchId: ELEVENLABS_TTS_TEST_BRANCH_ID, versionId: cohort.testVersionId }), "v4_turbo_test");
  for (const changes of [{ finalReceiptMatched: false }, { testMode: true }, { durationSecs: 0 }, { durationSecs: null },
    { startTimeUnixSecs: undefined }, { startTimeUnixSecs: eligible().startTimeUnixSecs - 1 },
    { versionId: "previous" }, { versionId: cohort.testVersionId }, { branchId: "unknown" }]) {
    assert.equal(uniformComparisonArm({ ...eligible(), ...changes }), null);
  }
});
test("preserves the historical start while exposing a new winner gate", () => {
  const status = getElevenLabsRuntimeExperimentStatus();
  assert.equal(status.startedAt, "2026-09-28T22:30:50.974Z");
  assert.equal(status.uniformComparison.minimumTrueLiveConversationsPerArm, 15);
  assert.equal(status.uniformComparison.dedicatedReviewGenuineCalls, 100);
});
