import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { applyNoVoicemailWorkflow, noVoicemailProtectedHashes, NO_VOICEMAIL_EXIT, NO_VOICEMAIL_CONDITION } from '../src/scripts/elevenLabsNoVoicemailWorkflow.mjs';
import { syncComparisonPrompts } from '../src/scripts/syncElevenLabsComparisonPrompts.mjs';
import { sha256 } from '../src/scripts/syncElevenLabsPromptOnly.mjs';

const message = '\n"Hi, this is {{assistantName}} with Crisp Short Sales calling about the short sale listing. Synthetic exact message."';
const old = '# Role and style\n\nKeep every live consent, callback, opt-out and takeover rule.\n\n# Voicemail and recorded exits\nOld mailbox policy.' + message;
const prompt = old.replace('Old mailbox policy.', 'A completed explicit recorded refusal uses the silent no-message exit, not voicemail_detection. Infer no live opt-out, callback or SMS permission.');
const suffix = '\n\n[CRISP_OPENING_HANDOFF_POLICY]\nKeep pending questions.\n[END_CRISP_OPENING_HANDOFF_POLICY]\n\nKeep the listener and transfer audio.';

function fixture(i = 0) {
  const a = { agent_id: 'agent_test', branch_id: 'branch_' + i, version_id: 'version_' + i,
    conversation_config: { tts: { model_id: i ? 'eleven_v4_turbo' : 'eleven_flash_v2', voice_id: 'eryn' },
      turn: { turn_model: 'turn_v3' }, agent: { prompt: { prompt: old, llm: 'gpt-4.1', tool_ids: ['tool_keep'] } } },
    workflow: { nodes: { unrelated_recording_exit: { type: 'end', position: { x: 1, y: 2 }, edge_order: [], outcome: 'success', exit_slot: null } }, edges: {} },
    platform_settings: { keep: true }, phone_numbers: [], whatsapp_accounts: [], procedures: {} };
  for (const id of ['opening_listener', 'main_conversation']) {
    a.workflow.nodes[id] = { type: 'override_agent', additional_prompt: 'Keep guarded transfer/hold audio.',
      conversation_config: { agent: { prompt: { prompt: id === 'opening_listener' ? old + suffix : 'Keep live dialog.', built_in_tools: { end_call: null } } } },
      edge_order: ['terminal_contact_from_' + id, 'terminal_guard_from_' + id, 'keep_' + id] };
    for (const edge of a.workflow.nodes[id].edge_order) a.workflow.edges[edge] = { source: id, target: 'keep', forward_condition: { type: 'llm', condition: 'Existing protected condition.' } };
  }
  a.workflow.edges.opening_to_unrelated_recording_exit = { source: 'opening_listener', target: 'unrelated_recording_exit', backward_condition: null, reroutes: [],
    forward_condition: { type: 'llm', exit_slot: { type: 'any' }, label: null, condition: 'Only an unrelated recording.' } };
  a.workflow.nodes.opening_listener.edge_order.push('opening_to_unrelated_recording_exit');
  return a;
}

test('no-message route changes only reviewed policy and additive recording leaves', () => {
  const a = fixture(), before = JSON.stringify(a), next = applyNoVoicemailWorkflow(a, prompt);
  assert.equal(JSON.stringify(a), before);
  assert.equal(next.conversation_config.agent.prompt.prompt, prompt);
  assert.equal(next.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt, prompt + suffix);
  assert.deepEqual(noVoicemailProtectedHashes(next), noVoicemailProtectedHashes(a));
  for (const context of ['opening_listener', 'main_conversation']) {
    const id = context + '_to_recorded_no_voicemail_exit';
    assert.deepEqual(next.workflow.nodes[context].edge_order.slice(0, 3), ['terminal_contact_from_' + context, 'terminal_guard_from_' + context, id]);
    assert.deepEqual(next.workflow.nodes[context].edge_order.filter(e => e !== id), a.workflow.nodes[context].edge_order);
    assert.equal(next.workflow.edges[id].target, NO_VOICEMAIL_EXIT);
    assert.equal(next.workflow.edges[id].forward_condition.condition, NO_VOICEMAIL_CONDITION);
  }
  assert.match(NO_VOICEMAIL_CONDITION, /PRERECORDED/);
  assert.match(NO_VOICEMAIL_CONDITION, /later genuine live interruption, callback\/email request, opt-out or takeover/);
  assert.deepEqual(applyNoVoicemailWorkflow(next, prompt), next);
  assert.throws(() => applyNoVoicemailWorkflow(a, prompt.replace('Keep every live consent', 'Change consent')), /reviewed voicemail policy/);
  assert.throws(() => applyNoVoicemailWorkflow(a, prompt.replace('Synthetic exact message', 'New voicemail message')), /reviewed voicemail policy/);
  for (const change of [a => { a.workflow.nodes[NO_VOICEMAIL_EXIT] = { type: 'phone_number' }; },
    a => { a.workflow.edges.opening_listener_to_recorded_no_voicemail_exit = { source: 'wrong' }; },
    a => { delete a.workflow.edges.terminal_guard_from_main_conversation; }]) {
    const changed = fixture(); change(changed); assert.throws(() => applyNoVoicemailWorkflow(changed, prompt), /collision|guards required/);
  }
});

async function setup(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'no-voicemail-release-')); t.after(() => rm(dir, { recursive: true, force: true }));
  const agents = [fixture(), fixture(1)], writes = [];
  const promptPath = path.join(dir, 'prompt.md'); await writeFile(promptPath, '## Prompt\n' + prompt);
  let fail = false;
  const fetcher = async (url, options) => {
    if (url.endsWith('/branches')) return { ok: true, json: async () => ({ meta: { total: 2 }, results: agents.map(a => ({ id: a.branch_id, is_archived: false, current_live_percentage: 50 })) }) };
    const a = agents.find(a => a.branch_id === new URL(url).searchParams.get('branch_id')); assert.ok(a);
    if (options.method === 'PATCH') {
      writes.push(a.branch_id); if (fail) throw new Error('ambiguous timeout');
      const body = JSON.parse(options.body); assert.deepEqual(Object.keys(body), ['workflow', 'conversation_config']);
      assert.deepEqual(Object.keys(body.conversation_config.agent.prompt), ['prompt']);
      a.workflow = body.workflow; a.conversation_config.agent.prompt.prompt = body.conversation_config.agent.prompt.prompt; a.version_id += '_updated';
    } else assert.equal(options.method, 'GET');
    return { ok: true, json: async () => structuredClone(a) };
  };
  return { agents, writes, fetcher, fail: () => { fail = true; }, options: { apiKey: 'synthetic', agentId: 'agent_test',
    branches: agents.map(a => ({ id: a.branch_id, version: a.version_id, promptHash: sha256(old) })), promptPath,
    receiptDir: path.join(dir, 'receipts'), repairNoVoicemail: true } };
}

test('paired no-message publisher is dry-run first, sequential and independently read back', async t => {
  const s = await setup(t), protectedBefore = s.agents.map(noVoicemailProtectedHashes);
  const plan = await syncComparisonPrompts(s.options, s.fetcher); assert.equal(plan.release, 'recorded-no-voicemail-r17'); assert.deepEqual(s.writes, []);
  const receipt = await syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher);
  assert.equal(receipt.status, 'live_uniform_comparison_verified'); assert.deepEqual(s.writes, ['branch_0', 'branch_1']);
  assert.deepEqual(receipt.trafficPercent, [50, 50]); assert.deepEqual(s.agents.map(noVoicemailProtectedHashes), protectedBefore);
});

test('ambiguous no-message write never retries or advances to the other arm', async t => {
  const s = await setup(t); await syncComparisonPrompts(s.options, s.fetcher); s.fail();
  await assert.rejects(syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher), /ambiguous timeout/); assert.deepEqual(s.writes, ['branch_0']);
});
