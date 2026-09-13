import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { syncPromptOnly, sha256, extractPrompt } from '../src/scripts/syncElevenLabsPromptOnly.mjs';

async function setup(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'voice-copy-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const beforePrompt = 'Before. '.repeat(20);
  const nextPrompt = 'Approved wording only. '.repeat(10);
  const promptPath = path.join(dir, 'prompt.md');
  await writeFile(promptPath, `# Test\n## Prompt\n${nextPrompt}`);
  const current = {
    agent_id: 'agent_test', version_id: 'version_before', branch_id: 'main', main_branch_id: 'main',
    conversation_config: { agent: { first_message: 'Unchanged', prompt: { prompt: beforePrompt, tool_ids: ['tool_mock'] } }, turn: { initial_wait_time: 1.6 } },
    workflow: { unchanged: true }, platform_settings: { unchanged: true }, phone_numbers: [],
  };
  return { dir, current, nextPrompt, options: { apiKey: 'test-only', agentId: current.agent_id,
    expectedVersion: current.version_id, expectedPromptHash: sha256(beforePrompt), promptPath, receiptDir: path.join(dir, 'receipt') } };
}
const response = value => ({ ok: true, status: 200, json: async () => structuredClone(value) });

test('defaults to a single read with no provider write', async t => {
  const { current, options } = await setup(t); const calls = [];
  const result = await syncPromptOnly(options, async (url, init) => { calls.push(init.method); return response(current); });
  assert.deepEqual(calls, ['GET']); assert.equal(result.status, 'prepared_not_applied');
});

test('only approved prompt leaf changes and effective readback is required', async t => {
  const { current, options, nextPrompt } = await setup(t); const calls = [];
  const result = await syncPromptOnly({ ...options, apply: true }, async (url, init) => {
    calls.push(init.method);
    assert.match(url, /^https:\/\/api.elevenlabs.io\/v1\/convai\/agents\/agent_test/);
    if (init.method === 'PATCH') {
      const body = JSON.parse(init.body);
      assert.deepEqual(body, { conversation_config: { agent: { prompt: { prompt: nextPrompt.trim() } } } });
      current.conversation_config.agent.prompt.prompt = body.conversation_config.agent.prompt.prompt;
      current.version_id = 'version_after';
    }
    return response(current);
  });
  assert.deepEqual(calls, ['GET', 'GET', 'PATCH', 'GET']);
  assert.equal(result.status, 'live_prompt_verified_audio_behavior_not_tested');
  assert.deepEqual(result.protected_before, result.protected_after);
});

test('version drift blocks update', async t => {
  const { current, options } = await setup(t); current.version_id = 'unexpected'; const methods = [];
  await assert.rejects(syncPromptOnly({ ...options, apply: true }, async (url, init) => { methods.push(init.method); return response(current); }), /drifted/);
  assert.deepEqual(methods, ['GET']);
});

test('non-main branch and second-read settings drift block update', async t => {
  const { current, options } = await setup(t);
  current.branch_id = 'draft';
  await assert.rejects(syncPromptOnly({ ...options, apply: true }, async () => response(current)), /main branch/);
  current.branch_id = 'main'; let reads = 0;
  await assert.rejects(syncPromptOnly({ ...options, apply: true }, async (url, init) => {
    assert.equal(init.method, 'GET'); if (++reads === 2) current.conversation_config.turn.initial_wait_time = 2;
    return response(current);
  }), /Protected configuration changed/);
});

test('provider side effects cannot be reported as a verified prompt-only release', async t => {
  const { current, options } = await setup(t);
  await assert.rejects(syncPromptOnly({ ...options, apply: true }, async (url, init) => {
    if (init.method === 'PATCH') {
      current.conversation_config.agent.prompt.prompt = JSON.parse(init.body).conversation_config.agent.prompt.prompt;
      current.conversation_config.turn.initial_wait_time = 0;
    }
    return response(current);
  }), /readback differs/);
  const receipt = JSON.parse(await readFile(path.join(options.receiptDir, 'receipt.json'), 'utf8'));
  assert.equal(receipt.status, 'needs_readback_review_do_not_repeat_patch');
});

test('ambiguous patch response is not retried or claimed complete', async t => {
  const { current, options } = await setup(t); let patches = 0;
  await assert.rejects(syncPromptOnly({ ...options, apply: true }, async (url, init) => {
    if (init.method === 'PATCH') { patches++; throw new Error('timeout'); }
    return response(current);
  }), /timeout/);
  assert.equal(patches, 1);
  const receipt = JSON.parse(await readFile(path.join(options.receiptDir, 'receipt.json'), 'utf8'));
  assert.equal(receipt.status, 'needs_readback_review_do_not_repeat_patch');
});

test('missing or duplicate prompt markers are rejected', () => {
  assert.throws(() => extractPrompt('missing'), /Expected one/);
  assert.throws(() => extractPrompt('## Prompt\nfirst\n## Prompt\nsecond'), /Expected one/);
});
