import assert from 'node:assert/strict';
import test from 'node:test';
import { comparisonBody, digest, DISABLED_NOISY_ENVIRONMENT_MESSAGE, R7_COMPARISON, validateComparison, verifyLiveComparison } from '../src/scripts/verifyElevenLabsComparison.mjs';

function fixture(index = 0) {
  const arm = R7_COMPARISON.arms[index];
  return { agent_id: R7_COMPARISON.agentId, branch_id: arm.branchId, version_id: arm.versionId,
    conversation_config: { tts: { model_id: arm.ttsModel, pvc_strength: 0, voice_id: 'synthetic_eryn', speed: 1 },
      agent: { subagents: [], prompt: { prompt: 'Synthetic policy', tool_ids: ['synthetic_tool'], tools: [{ expanded: true }], llm: 'gpt-4.1' } },
      turn: { turn_model: 'turn_v3' } },
    workflow: { edges: [{ order: 1 }, { order: 2 }] }, platform_settings: { test: false }, phone_numbers: [] };
}
function newShape(agent) {
  const result = structuredClone(agent);
  result.conversation_config.tts.clone_strength = 0;
  delete result.conversation_config.tts.pvc_strength;
  delete result.conversation_config.agent.subagents;
  result.conversation_config.agent.subagent_config = { agents: [], self_subagent: null };
  return result;
}
const expected = { ...R7_COMPARISON, sharedHash: digest(comparisonBody(fixture())), workflowHash: digest(fixture().workflow) };
const allocation = () => ({ meta: { total: 2 }, results: R7_COMPARISON.arms.map(arm => ({ id: arm.branchId, is_archived: false, current_live_percentage: 50 })) });

function disabledNoiseShape(agent) {
  const result = newShape(agent);
  result.conversation_config.turn.noisy_environment_detection = false;
  result.conversation_config.turn.noisy_environment_message = DISABLED_NOISY_ENVIRONMENT_MESSAGE;
  return result;
}

test('the exact disabled noise pair reproduces the approved digest without mutating the response', () => {
  const fresh = disabledNoiseShape(fixture()), before = JSON.stringify(fresh);
  assert.deepEqual(comparisonBody(fresh), comparisonBody(fixture()));
  assert.equal(JSON.stringify(fresh), before);
  assert.equal(validateComparison([fresh, disabledNoiseShape(fixture(1))], allocation(), expected).ok, true);
  assert.equal(validateComparison([fixture(), disabledNoiseShape(fixture(1))], allocation(), expected).ok, true);
});

test('enabled, malformed, partial or customized noise settings remain drift', () => {
  for (const change of [
    a => { a.conversation_config.turn.noisy_environment_detection = true; },
    a => { a.conversation_config.turn.noisy_environment_detection = null; },
    a => { a.conversation_config.turn.noisy_environment_detection = 0; },
    a => { a.conversation_config.turn.noisy_environment_detection = 'false'; },
    a => { delete a.conversation_config.turn.noisy_environment_detection; },
    a => { delete a.conversation_config.turn.noisy_environment_message; },
    a => { a.conversation_config.turn.noisy_environment_message = ''; },
    a => { a.conversation_config.turn.noisy_environment_message = null; },
    a => { a.conversation_config.turn.noisy_environment_message += ' changed'; },
    a => { a.conversation_config.turn.another_default = false; },
  ]) {
    const value = disabledNoiseShape(fixture()); change(value);
    assert.throws(() => validateComparison([value, fixture(1)], allocation(), expected), /configuration digest/);
  }
});

test('old and exact approved new response shapes share one digest without mutating either readback', () => {
  const old = fixture(), fresh = newShape(old), before = JSON.stringify(fresh);
  assert.deepEqual(comparisonBody(old), comparisonBody(fresh));
  assert.equal(JSON.stringify(fresh), before);
  assert.equal(validateComparison([old, newShape(fixture(1))], allocation(), expected).ok, true);
});

test('non-neutral voice strength, ambiguous aliases and missing both fields remain visible', () => {
  for (const change of [
    a => { a.conversation_config.tts.clone_strength = 0.2; },
    a => { a.conversation_config.tts.clone_strength = null; },
    a => { a.conversation_config.tts.clone_strength = '0'; },
    a => { a.conversation_config.tts.pvc_strength = 0; },
    a => { delete a.conversation_config.tts.clone_strength; },
  ]) {
    const value = newShape(fixture()); change(value);
    assert.notEqual(digest(comparisonBody(value)), expected.sharedHash);
    assert.throws(() => validateComparison([value, fixture(1)], allocation(), expected), /configuration digest/);
  }
});

test('configured, null or differently shaped subagents must never be normalized away', () => {
  for (const change of [
    a => { a.conversation_config.agent.subagents = [{ id: 'unexpected' }]; },
    a => { a.conversation_config.agent.subagents = null; },
    a => { a.conversation_config.agent.subagents = {}; },
    a => { a.conversation_config.agent.subagent_config.agents.push({ id: 'unexpected' }); },
    a => { a.conversation_config.agent.subagent_config.self_subagent = { id: 'unexpected' }; },
    a => { a.conversation_config.agent.subagent_config.extra = false; },
    a => { a.conversation_config.agent.subagent_config = null; },
    a => { a.conversation_config.agent.subagent_config = []; },
  ]) {
    const value = newShape(fixture()); change(value);
    assert.throws(() => validateComparison([value, fixture(1)], allocation(), expected), /configuration digest/);
  }
});

test('prompt, tool IDs, voice, workflow order, new fields and LLM/turn changes still fail', () => {
  for (const change of [
    a => { a.conversation_config.agent.prompt.prompt += ' unreviewed'; },
    a => { a.conversation_config.agent.prompt.tool_ids = ['other_tool']; },
    a => { a.conversation_config.tts.voice_id = 'other_voice'; },
    a => { a.conversation_config.tts.speed = 0.95; },
    a => { a.conversation_config.agent.prompt.llm = 'other_model'; },
    a => { a.conversation_config.turn.turn_model = 'turn_v2'; },
    a => { a.workflow.edges.reverse(); },
    a => { a.conversation_config.new_default = null; },
  ]) {
    const value = newShape(fixture()); change(value);
    assert.throws(() => validateComparison([value, fixture(1)], allocation(), expected), /configuration digest/);
  }
});

test('unknown versions or branches are rejected even when their bodies match', () => {
  for (const key of ['agent_id', 'branch_id', 'version_id']) {
    const value = fixture(); value[key] = 'unexpected';
    assert.throws(() => validateComparison([value, fixture(1)], allocation(), expected), /mismatch|unknown version/);
  }
});

test('traffic drift and incomplete branch pagination fail closed', () => {
  for (const change of [
    a => { a.results[0].current_live_percentage = 60; },
    a => { a.results[0].is_archived = true; },
    a => { a.meta.total = 3; },
    a => { a.results.push({ id: 'third', current_live_percentage: 1 }); a.meta.total = 3; },
  ]) {
    const value = allocation(); change(value);
    assert.throws(() => validateComparison([fixture(), fixture(1)], value, expected), /traffic|branches|allocation/);
  }
});

test('protected platform, phone, WhatsApp and procedure differences remain failures', () => {
  for (const key of ['platform_settings', 'phone_numbers', 'whatsapp_accounts', 'procedures']) {
    const value = fixture(1); value[key] = { unexpected: true };
    assert.throws(() => validateComparison([fixture(), value], allocation(), expected), /protected arm mismatch/);
  }
});

test('live verifier makes exactly three GETs and never invokes a publisher or other mutation', async () => {
  const requests = [];
  const fetcher = async (url, options) => {
    assert.equal(options.method, 'GET'); requests.push(url);
    const value = url.endsWith('/branches') ? allocation()
      : disabledNoiseShape(fixture(url.includes(R7_COMPARISON.arms[1].branchId) ? 1 : 0));
    return { ok: true, json: async () => value };
  };
  const result = await verifyLiveComparison({ apiKey: 'synthetic_key', expected }, fetcher);
  assert.equal(result.ok, true); assert.equal(result.readOnly, true); assert.equal(requests.length, 3);
  assert.equal(JSON.stringify(result).includes('synthetic_key'), false);
  await assert.rejects(verifyLiveComparison({ apiKey: 'synthetic_key', expected }, async () => ({ ok: false, status: 401 })), /HTTP 401/);
});
