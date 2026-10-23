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
    { versionId: "previous" }, { versionId: "agtvrsn_7201m4gvk2q2fx3bm45pft78zhqz" },
    { versionId: "agtvrsn_3501m4gvk4hhf2gt2a26wbgw11e0" }, { versionId: cohort.testVersionId }, { branchId: "unknown" }]) {
    assert.equal(uniformComparisonArm({ ...eligible(), ...changes }), null);
  }
});
test("r7 restarts the clean comparison and excludes old r6, r5 and r4 receipts", () => {
  assert.equal(cohort.cohort, "maya_uniform_lead_conversion_20261023_r7");
  assert.equal(cohort.startedAt, "2026-10-23T12:27:33.399Z");
  for (const versionId of ["agtvrsn_8701m5bdwq8af46r1rpnzmb04b4g", "agtvrsn_6801m5bdwvh2fnkb9hfycj82vzj0",
    "agtvrsn_9501m554nxfaee2rps63c9zznh6k", "agtvrsn_1801m554p0e8edbshc6dkrjcf3rj",
    "agtvrsn_4601m4yhj7xsejw8wcm77xkcn08x", "agtvrsn_3401m52cmpgcecsbsfd89s046bsp"]) {
    assert.equal(uniformComparisonArm({ ...eligible(), versionId }), null);
  }
  assert.equal(uniformComparisonArm({ ...eligible(), startTimeUnixSecs: Date.parse("2026-10-15T13:22:14Z") / 1000 }), null);
});
test("preserves the historical start while exposing a new winner gate", () => {
  const status = getElevenLabsRuntimeExperimentStatus();
  assert.equal(status.startedAt, "2026-09-28T22:30:50.974Z");
  assert.equal(status.uniformComparison.minimumTrueLiveConversationsPerArm, 15);
  assert.equal(status.uniformComparison.dedicatedReviewGenuineCalls, 100);
});
