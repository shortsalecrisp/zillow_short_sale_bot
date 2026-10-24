import assert from "node:assert/strict";
import test from "node:test";
import {
  getPendingVoiceOutcomeRecoveries, isMatchingTerminalVoiceReceipt, voiceOutcomeRecoveryBlockReason,
  POST_CALL_FALLBACK_MAX_ATTEMPTS, shouldContinuePostCallFallback,
} from "../src/lib/voiceOutcomeRecovery";

const now = new Date("2026-10-23T14:00:00Z");
const monitor = new Date("2026-09-30T09:46:05.445Z");
function pending() {
  const values: unknown[] = Array(42).fill("");
  values[0] = "Synthetic"; values[2] = "202-555-0123";
  values[32] = "2026-10-23T13:15:22.242Z";
  return { rowNumber: 123, values };
}
test("post-call polling survives a ten-minute call without extending automatic connect retries", () => {
  for (const attempt of [5, 6, 18, 30, 119]) assert.equal(shouldContinuePostCallFallback(attempt), true);
  assert.equal(POST_CALL_FALLBACK_MAX_ATTEMPTS, 120);
  assert.equal(shouldContinuePostCallFallback(120), false);
});
test("durable sent fields reconstruct pending recovery after a restart, not another call", () => {
  const row = pending();
  const [candidate] = getPendingVoiceOutcomeRecoveries([row], now, monitor);
  assert.deepEqual(candidate, { rowNumber: 123, proof: {
    phone: "+12025550123", callAttemptNumber: 1, sentAt: "2026-10-23T13:15:22.242Z", previousResult: "",
    fullName: "Synthetic", listingAddress: "",
  } });
  row.values[39] = "2026-10-23T13:30:00Z";
  assert.equal(getPendingVoiceOutcomeRecoveries([row], now, monitor)[0].proof.callAttemptNumber, 2);
  row.values[40] = "voicemail_reached_final_attempt";
  assert.deepEqual(getPendingVoiceOutcomeRecoveries([row], now, monitor), []);
});
test("recovery preserves contact preferences, existing K, current attempt and final outcomes", () => {
  const row = pending(); const proof = getPendingVoiceOutcomeRecoveries([row], now, monitor)[0].proof;
  for (const [index, value] of [[10, "N"], [10, "G"], [19, "x"], [16, "handoff"], [37, "yes"],
    [38, "tomorrow"], [35, "yes"], [9, "Requested information"], [13, "human-takeover"],
    [33, "do_not_call"], [39, "2026-10-23T13:30:00Z"], [2, "202-555-0199"],
    [32, "2026-10-23T13:16:00Z"], [4, "Changed listing"], [0, "Changed owner"],
    [41, "CODEX_VOICE_EXPIRED_TO_MAILSHAKE_V1"]] as Array<[number, string]>) {
    const current = pending(); current.values[index] = value;
    assert.ok(voiceOutcomeRecoveryBlockReason(current.values, proof), String(index));
  }
  const recent = pending(); recent.values[32] = "2026-10-23T13:59:30Z";
  assert.deepEqual(getPendingVoiceOutcomeRecoveries([recent], now, monitor), []);
  const old = pending(); old.values[32] = "2026-09-01T13:15:00Z";
  assert.deepEqual(getPendingVoiceOutcomeRecoveries([old], now, monitor), []);
});
test("only an exact terminal non-test outbound receipt can release pending processing", () => {
  const proof = getPendingVoiceOutcomeRecoveries([pending()], now, monitor)[0].proof;
  const receipt = {
    conversation_id: "conv_synthetic", agent_id: "agent_synthetic", status: "done",
    metadata: { start_time_unix_secs: Date.parse(proof.sentAt) / 1000 + 20,
      phone_call: { external_number: proof.phone, direction: "outbound" } },
    conversation_initiation_client_data: { dynamic_variables: { rowNumber: 123, callAttemptNumber: 1, phone: proof.phone, testMode: false } },
  };
  assert.equal(isMatchingTerminalVoiceReceipt(receipt, 123, proof, "agent_synthetic"), true);
  for (const change of [
    (r: typeof receipt) => { r.status = "in-progress"; }, (r: typeof receipt) => { r.agent_id = "other"; },
    (r: typeof receipt) => { r.metadata.phone_call.external_number = "+12025550199"; },
    (r: typeof receipt) => { r.metadata.phone_call.direction = "inbound"; },
    (r: typeof receipt) => { r.metadata.start_time_unix_secs += 601; },
    (r: typeof receipt) => { r.conversation_initiation_client_data.dynamic_variables.testMode = true; },
    (r: typeof receipt) => { r.conversation_initiation_client_data.dynamic_variables.callAttemptNumber = 2; },
  ]) {
    const changed = structuredClone(receipt); change(changed);
    assert.equal(isMatchingTerminalVoiceReceipt(changed, 123, proof, "agent_synthetic"), false);
  }
});

test("explicit decline reconciliation can replace only the exact prior generic N/hangup state", () => {
  const row = pending(); row.values[33] = "call_received_agent_hung_up"; row.values[10] = "N";
  const proof = { phone: "+12025550123", callAttemptNumber: 1 as const, sentAt: "2026-10-23T13:15:22.242Z",
    previousResult: "call_received_agent_hung_up", fullName: "Synthetic", listingAddress: "", expectedLeadStatusCode: "N" as const };
  assert.equal(voiceOutcomeRecoveryBlockReason(row.values, proof), undefined);
  for (const [index, value] of [[10, "G"], [10, "R"], [19, "x"], [37, "yes"], [13, "opt-out"], [33, "do_not_call"]] as Array<[number, string]>) {
    const changed = [...row.values]; changed[index] = value;
    assert.ok(voiceOutcomeRecoveryBlockReason(changed, proof));
  }
});
