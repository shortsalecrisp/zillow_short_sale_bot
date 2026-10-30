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
test("r12 recording guard release excludes earlier workflow and prompt cohorts", () => {
  assert.equal(cohort.cohort, "maya_uniform_lead_conversion_20261029_r12");
  assert.equal(cohort.startedAt, "2026-10-30T01:12:22.737Z");
  for (const versionId of ["agtvrsn_7001m64ex5v3fvx8794e269sj033", "agtvrsn_1201m64eyzthfc0r8kdcednrjpw4",
    "agtvrsn_7201m62kcfnwf28tzhjppg5seej9", "agtvrsn_0301m62kcj00fn98azqm64hmer07",
    "agtvrsn_1101m6015jj3e7r8xem6crdzbv08", "agtvrsn_8601m6015mjxf1f8m8gqqygf3n33",
    "agtvrsn_5901m5mbp1pafv1skkvbhzh4cqfn", "agtvrsn_4301m5mbp4jgfn5ayjenb7yjjtby",
    "agtvrsn_8701m5bdwq8af46r1rpnzmb04b4g", "agtvrsn_6801m5bdwvh2fnkb9hfycj82vzj0",
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
