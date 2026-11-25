import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { R8_COMPARISON, comparisonBody, digest, validateComparison, validateV4Production,
  verifyLiveV4Production } from '../src/scripts/verifyElevenLabsComparison.mjs';
import { rolloutV4Production } from '../src/scripts/rolloutElevenLabsV4Production.mjs';

async function setup(t) {
  const receiptDir = await mkdtemp(path.join(os.tmpdir(), 'v4-production-test-'));
  t.after(() => rm(receiptDir, { recursive: true, force: true }));
  const agents = R8_COMPARISON.arms.map(arm => ({ agent_id: R8_COMPARISON.agentId,
    branch_id: arm.branchId, version_id: arm.versionId,
    conversation_config: { tts: { model_id: arm.ttsModel, voice_id: 'eryn' },
      agent: { subagents: [], prompt: { prompt: 'unchanged', llm: 'gpt-4.1' } }, turn: { turn_model: 'turn_v3' } },
    workflow: { guarded: true }, platform_settings: { protected: true }, phone_numbers: [] }));
  const tool = { id: 'tool_synthetic', tool_config: { name: 'live_transfer_requested', tool_call_sound: 'elevator1',
    tool_call_sound_behavior: 'always', interruption_mode: 'allow', disable_interruptions: false, response_timeout_secs: 60 } };
  const expected = { ...R8_COMPARISON, sharedHash: digest(comparisonBody(agents[0])), workflowHash: digest(agents[0].workflow),
    waitingAudio: { availabilityToolId: tool.id, availabilityToolHash: digest(tool.tool_config), sound: 'elevator1' } };
  const state = { agents, allocation: { meta: { total: 2 }, results: expected.arms.map(arm => ({
    id: arm.branchId, current_live_percentage: 50, is_archived: false })) }, requests: [], posts: 0,
    ambiguous: false, unexpectedAfter: false };
  const fetcher = async (url, options) => {
    state.requests.push({ url, method: options.method });
    if (options.method === 'POST') {
      assert.ok(url.endsWith('/deployments'));
      assert.deepEqual(JSON.parse(options.body).deployment_request.requests.map(r => r.deployment_strategy.traffic_percentage), [0, 100]);
      state.posts++;
      state.allocation.results.forEach((branch, i) => { branch.current_live_percentage = i ? 100 : 0; });
      if (state.unexpectedAfter) state.agents[1].platform_settings.protected = false;
      if (state.ambiguous) throw new Error('response timeout');
      return { ok: true, json: async () => ({}) };
    }
    assert.equal(options.method, 'GET');
    const result = url.endsWith('/branches') ? state.allocation : url.includes('/tools/') ? tool
      : agents[expected.arms.findIndex(arm => url.includes(arm.branchId))];
    assert.ok(result, 'Only exact branches, allocation and shared audio can be read');
    return { ok: true, json: async () => structuredClone(result) };
  };
  const options = { apiKey: 'synthetic', receiptDir, expected };
  return { state, options, fetcher };
}

test('dry-run is read-only; exact guarded apply changes allocation only and reread proves 0/100', async t => {
  const s = await setup(t), before = structuredClone(s.state.agents);
  const plan = await rolloutV4Production(s.options, s.fetcher);
  assert.equal(s.state.posts, 0);
  const receipt = await rolloutV4Production({ ...s.options, apply: true, expectedPlanHash: plan.planHash }, s.fetcher);
  assert.equal(receipt.status, 'live_v4_production_verified');
  assert.equal(s.state.posts, 1);
  assert.deepEqual(s.state.agents, before);
  assert.deepEqual(receipt.verification.arms.map(a => a.trafficPercent), [0, 100]);
  assert.equal((await rolloutV4Production(s.options, s.fetcher)).applied, false);
  assert.equal(s.state.posts, 1);
});

test('comparison remains strict 50/50; production requires exact sole v4 allocation', async t => {
  const s = await setup(t);
  assert.throws(() => validateV4Production(s.state.agents, s.state.allocation, s.options.expected));
  s.state.allocation.results.forEach((b, i) => { b.current_live_percentage = i ? 100 : 0; });
  assert.throws(() => validateComparison(s.state.agents, s.state.allocation, s.options.expected));
  assert.equal(validateV4Production(s.state.agents, s.state.allocation, s.options.expected).ok, true);
  s.state.requests = [];
  await verifyLiveV4Production(s.options, s.fetcher);
  assert.equal(s.state.requests.length, 3);
  s.state.allocation.results.push({ id: 'unexpected', is_archived: false, current_live_percentage: 1 });
  s.state.allocation.meta.total++;
  assert.throws(() => validateV4Production(s.state.agents, s.state.allocation, s.options.expected));
});

test('missing guard, changed version and protected drift prevent deployment', async t => {
  const s = await setup(t), plan = await rolloutV4Production(s.options, s.fetcher);
  await assert.rejects(rolloutV4Production({ ...s.options, apply: true }, s.fetcher));
  s.state.agents[1].version_id = 'unknown';
  await assert.rejects(rolloutV4Production({ ...s.options, apply: true, expectedPlanHash: plan.planHash }, s.fetcher));
  assert.equal(s.state.posts, 0);
});

test('ambiguous accepted deployment is reconciled without a second POST', async t => {
  const s = await setup(t), plan = await rolloutV4Production(s.options, s.fetcher);
  s.state.ambiguous = true;
  const result = await rolloutV4Production({ ...s.options, apply: true, expectedPlanHash: plan.planHash }, s.fetcher);
  assert.equal(result.status, 'live_v4_production_verified');
  assert.match(result.writeError, /not retried/);
  assert.equal(s.state.posts, 1);
});

test('post-write protected drift is held, never retried or reported verified', async t => {
  const s = await setup(t), plan = await rolloutV4Production(s.options, s.fetcher);
  s.state.unexpectedAfter = true;
  await assert.rejects(rolloutV4Production({ ...s.options, apply: true, expectedPlanHash: plan.planHash }, s.fetcher));
  assert.equal(s.state.posts, 1);
  const receipt = JSON.parse(await readFile(path.join(s.options.receiptDir, 'receipt.json')));
  assert.equal(receipt.status, 'allocation_unverified_hold');
});
