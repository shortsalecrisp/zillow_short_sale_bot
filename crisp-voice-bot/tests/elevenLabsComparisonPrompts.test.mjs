import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { syncComparisonPrompts } from '../src/scripts/syncElevenLabsComparisonPrompts.mjs';
import { sha256 } from '../src/scripts/syncElevenLabsPromptOnly.mjs';
import { comparisonBody, digest, DISABLED_NOISY_ENVIRONMENT_MESSAGE } from '../src/scripts/verifyElevenLabsComparison.mjs';

async function setup(t, bothOld = false) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'voice-comparison-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const prompt = 'Approved shared deferred-contact policy. '.repeat(10).trim();
  const old = 'Previous shared policy. '.repeat(10);
  const models = ['eleven_flash_v2', 'eleven_v4_turbo'];
  const agents = models.map((model, i) => ({ agent_id: 'agent_test', branch_id: `branch_${i}`, main_branch_id: 'branch_0', version_id: `v_${i}`,
    conversation_config: { tts: { model_id: model, voice_id: 'eryn', speed: 0.95 }, turn: { turn_model: 'turn_v3' },
      agent: { prompt: { prompt: i === 0 && !bothOld ? prompt : old, llm: 'gpt-4.1', tool_ids: ['tool_test'] } } },
    workflow: { guarded: true }, platform_settings: { unchanged: true }, phone_numbers: [], procedures: {} }));
  const promptPath = path.join(dir, 'prompt.md');
  await writeFile(promptPath, `# Approved\n## Prompt\n${prompt}`);
  const options = { apiKey: 'test', agentId: 'agent_test', branches: agents.map(a => ({ id: a.branch_id, version: a.version_id,
    promptHash: sha256(a.conversation_config.agent.prompt.prompt) })), promptPath, receiptDir: path.join(dir, 'receipts') };
  let allocation = agents.map(a => ({ id: a.branch_id, current_live_percentage: 50, is_archived: false }));
  const writes = [];
  let failure = false;
  const fetcher = async (url, init) => {
    if (url.endsWith('/branches')) return { ok: true, json: async () => structuredClone({ results: allocation }) };
    const branch = new URL(url).searchParams.get('branch_id');
    const current = agents.find(a => a.branch_id === branch);
    assert.ok(current);
    if (init.method === 'PATCH') {
      writes.push(branch);
      if (failure) throw new Error('ambiguous timeout');
      const body = JSON.parse(init.body);
      assert.deepEqual(Object.keys(body.conversation_config.agent.prompt), ['prompt']);
      current.conversation_config.agent.prompt.prompt = body.conversation_config.agent.prompt.prompt;
      current.version_id += '_updated';
    }
    return { ok: true, status: 200, json: async () => structuredClone(current) };
  };
  return { options, fetcher, agents, prompt, writes, setAllocation: value => allocation = value, failWrites: () => failure = true };
}

test('comparison dry-run makes no writes and binds the reviewed plan', async t => {
  const s = await setup(t);
  const r = await syncComparisonPrompts(s.options, s.fetcher);
  assert.equal(r.status, 'prepared_not_applied');
  assert.equal(s.writes.length, 0);
});
test('release plans use the same canonical hash as the read-only verifier', async t => {
  const s = await setup(t);
  const expectedHash = digest(comparisonBody(s.agents[0]));
  for (const a of s.agents) {
    a.conversation_config.turn.noisy_environment_detection = false;
    a.conversation_config.turn.noisy_environment_message = DISABLED_NOISY_ENVIRONMENT_MESSAGE;
  }
  const r = await syncComparisonPrompts(s.options, s.fetcher);
  assert.equal(r.sharedHash, expectedHash);
  assert.equal(s.writes.length, 0);
});
test('synchronizes the stale arm, preserving the already-updated arm and protected settings', async t => {
  const s = await setup(t);
  await syncComparisonPrompts(s.options, s.fetcher);
  const r = await syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher);
  assert.deepEqual(s.writes, ['branch_1']);
  assert.deepEqual(r.versions, ['v_0', 'v_1_updated']);
  assert.equal(r.status, 'live_uniform_comparison_verified');
  assert.ok(Number.isFinite(Date.parse(r.verifiedAt)));
  assert.deepEqual(r.trafficPercent, [50, 50]);
  assert.equal(r.realCallsPlaced, false);
  assert.equal(r.audioBehaviorValidated, false);
});
test('updates both arms when both need the approved prompt', async t => {
  const s = await setup(t, true);
  await syncComparisonPrompts(s.options, s.fetcher);
  await syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher);
  assert.deepEqual(s.writes, ['branch_0', 'branch_1']);
});
test('rejects voice, workflow, tool, and speed confounds before any write', async t => {
  for (const change of [a => a.conversation_config.tts.voice_id = 'other', a => a.workflow.guarded = false,
    a => a.conversation_config.agent.prompt.tool_ids = [], a => a.conversation_config.tts.speed = 1.2]) {
    const s = await setup(t); change(s.agents[1]);
    await assert.rejects(syncComparisonPrompts(s.options, s.fetcher), /differ only/);
    assert.equal(s.writes.length, 0);
  }
});
test('rejects allocation or reviewed-version drift', async t => {
  const s = await setup(t);
  s.setAllocation([{ id: 'branch_0', current_live_percentage: 100 }]);
  await assert.rejects(syncComparisonPrompts(s.options, s.fetcher), /50\/50/);
  const v = await setup(t); v.agents[1].version_id = 'unexpected';
  await assert.rejects(syncComparisonPrompts(v.options, v.fetcher), /drifted/);
});
test('does not retry an ambiguous first write or report uniform completion', async t => {
  const s = await setup(t, true);
  await syncComparisonPrompts(s.options, s.fetcher); s.failWrites();
  await assert.rejects(syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher), /ambiguous timeout/);
  assert.deepEqual(s.writes, ['branch_0']);
});
