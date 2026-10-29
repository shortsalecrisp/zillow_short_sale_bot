import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { applyAcceptedTransferWorkflow, transferProtectedHashes, TRANSFER_CONTEXTS } from '../src/scripts/elevenLabsAcceptedTransferWorkflow.mjs';
import { syncComparisonPrompts } from '../src/scripts/syncElevenLabsComparisonPrompts.mjs';
import { sha256 } from '../src/scripts/syncElevenLabsPromptOnly.mjs';

function fixture(model = 'eleven_flash_v2', id = 'control') {
  const node = () => ({ type: 'override_agent', additional_prompt: 'Preserve guarded ending and caller preferences.',
    edge_order: ['terminal_contact', 'terminal_guard'], conversation_config: { agent: { prompt: {
      tool_ids: ['tool_callback', 'tool_information'], built_in_tools: { end_call: null, transfer_to_number: null } } } } });
  return { agent_id: 'agent_test', branch_id: id, version_id: 'version_' + id,
    conversation_config: { tts: { model_id: model, voice_id: 'eryn', speed: 0.95 }, turn: { turn_model: 'turn_v3' },
      agent: { prompt: { prompt: 'Approved shared prompt. '.repeat(10).trim(), llm: 'gpt-4.1', tool_ids: ['tool_approval'],
        built_in_tools: { end_call: null, transfer_to_number: null } } } },
    workflow: { nodes: { ...Object.fromEntries(TRANSFER_CONTEXTS.map(id => [id, node()])),
      terminal_guard_wait: node(), callback_receipt_wait: node(),
      phone_transfer: { type: 'phone_number', transfer_type: 'conference', transfer_destination: { type: 'phone', phone_number: '+12025550199' } } },
      edges: { start_to_main: { source: 'start_node', target: 'opening_listener' }, protected_optout: { condition: 'unchanged' } } },
    platform_settings: { protected: true }, phone_numbers: ['owned_number'], whatsapp_accounts: [], procedures: {} };
}

test('approval can perform the actual phone action without leaving the opening listener', () => {
  const before = fixture(), next = applyAcceptedTransferWorkflow(before);
  assert.deepEqual(transferProtectedHashes(next), transferProtectedHashes(before));
  assert.deepEqual(before, fixture(), 'builder cannot mutate its input');
  for (const id of TRANSFER_CONTEXTS) {
    const node = next.workflow.nodes[id], tool = node.conversation_config.agent.prompt.built_in_tools.transfer_to_number;
    assert.equal(tool.params.system_tool_type, 'transfer_to_number');
    assert.equal(tool.params.transfers.length, 1);
    assert.deepEqual(tool.params.transfers[0].transfer_destination, before.workflow.nodes.phone_transfer.transfer_destination);
    assert.equal(tool.params.transfers[0].transfer_type, 'conference');
    assert.equal(tool.interruption_mode, 'allow');
    for (const requirement of ['transferApproved true', 'approvalStatus accepted', 'current caller consent', 'unanswered question',
      'callback-only', 'email-only', 'cancellation', 'opt-out', 'human takeover', 'previous phone-transfer attempt']) {
      assert.ok(tool.description.includes(requirement), requirement);
    }
    assert.match(node.additional_prompt, /invoke transfer_to_number exactly once/);
    assert.match(node.additional_prompt, /Do not merely say the line, skip_turn/);
    assert.match(node.additional_prompt, /failed connection permits only the existing one permission-first callback offer/);
    assert.match(node.additional_prompt, /Preserve guarded ending and caller preferences/);
    assert.deepEqual(node.edge_order, before.workflow.nodes[id].edge_order);
  }
  for (const id of ['terminal_guard_wait', 'callback_receipt_wait']) assert.deepEqual(next.workflow.nodes[id], before.workflow.nodes[id]);
  assert.deepEqual(next.workflow.edges, before.workflow.edges);
  assert.deepEqual(next.conversation_config, before.conversation_config, 'global prompt, registry, voice and turn config are untouched');
  assert.deepEqual(applyAcceptedTransferWorkflow(next), next, 'idempotent proposal');
});

test('unknown destination, unguarded state or entry path fails closed', () => {
  for (const change of [a => a.workflow.nodes.phone_transfer.transfer_type = 'sip_refer',
    a => a.workflow.nodes.phone_transfer.transfer_destination.phone_number = 'unknown',
    a => a.workflow.nodes.opening_listener.conversation_config.agent.prompt.built_in_tools.end_call = {},
    a => a.workflow.edges.start_to_main.target = 'other', a => delete a.workflow.nodes.main_conversation]) {
    const a = fixture(); change(a); assert.throws(() => applyAcceptedTransferWorkflow(a), /required/);
  }
});

async function setup(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'accepted-transfer-release-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const agents = [fixture(), fixture('eleven_v4_turbo', 'test')], writes = [], methods = [];
  let fail = false, stale = false, allocation = { results: agents.map(a => ({ id: a.branch_id, is_archived: false, current_live_percentage: 50 })), meta: { total: 2 } };
  const promptPath = path.join(dir, 'prompt.md');
  await writeFile(promptPath, '## Prompt\n' + agents[0].conversation_config.agent.prompt.prompt);
  const options = { apiKey: 'synthetic', agentId: 'agent_test', branches: agents.map(a => ({ id: a.branch_id, version: a.version_id,
    promptHash: sha256(a.conversation_config.agent.prompt.prompt) })), promptPath, receiptDir: dir, repairAcceptedTransfer: true };
  const fetcher = async (url, init) => {
    methods.push(init.method);
    if (url.endsWith('/branches')) return { ok: true, json: async () => structuredClone(allocation) };
    const a = agents.find(a => a.branch_id === new URL(url).searchParams.get('branch_id'));
    assert.ok(a);
    if (init.method === 'PATCH') {
      writes.push(a.branch_id);
      const body = JSON.parse(init.body); assert.deepEqual(Object.keys(body), ['workflow']);
      if (fail) throw Error('ambiguous timeout');
      if (!stale) { a.workflow = body.workflow; a.version_id += '_new'; }
    }
    return { ok: true, json: async () => structuredClone(a) };
  };
  return { options, fetcher, agents, writes, methods, fail: () => fail = true, stale: () => stale = true,
    allocation: value => allocation = value };
}

test('paired release requires a reviewed no-write plan and exact complete 50/50 allocation', async t => {
  const s = await setup(t);
  assert.equal((await syncComparisonPrompts(s.options, s.fetcher)).status, 'prepared_not_applied');
  assert.equal(s.writes.length, 0); assert.ok(s.methods.every(m => m === 'GET'));
  const r = await syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher);
  assert.deepEqual(s.writes, ['control', 'test']);
  assert.deepEqual(r.versions, ['version_control_new', 'version_test_new']);
  assert.equal(r.status, 'live_uniform_comparison_verified');
  assert.equal(r.realCallsPlaced, false); assert.equal(r.audioBehaviorValidated, false);
  assert.deepEqual(transferProtectedHashes(s.agents[0]), transferProtectedHashes(fixture()));
});

test('incomplete allocation and changes outside the reviewed transfer profile cannot publish', async t => {
  const s = await setup(t); await syncComparisonPrompts(s.options, s.fetcher);
  s.agents[0].workflow.edges.protected_optout.condition = 'changed';
  await assert.rejects(syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher), /only in TTS|drifted/);
  assert.equal(s.writes.length, 0);
  s.allocation({ results: [], meta: { total: 2 } });
  await assert.rejects(syncComparisonPrompts(s.options, s.fetcher), /50\/50/);
});

test('reconciliation never republishes an already verified arm', async t => {
  const s = await setup(t);
  s.agents[0].workflow = applyAcceptedTransferWorkflow(s.agents[0]).workflow;
  s.agents[0].version_id = 'verified_control'; s.options.branches[0].version = 'verified_control';
  await syncComparisonPrompts(s.options, s.fetcher);
  const r = await syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher);
  assert.deepEqual(s.writes, ['test']);
  assert.equal(r.versions[0], 'verified_control');
  assert.equal(r.receipts[0].status, 'already_matches');
});

test('an ambiguous write or stale readback stops before touching the other arm', async t => {
  for (const mode of ['fail', 'stale']) {
    const s = await setup(t); await syncComparisonPrompts(s.options, s.fetcher); s[mode]();
    await assert.rejects(syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher), /timeout|readback differs/);
    assert.deepEqual(s.writes, ['control']);
    assert.equal(JSON.parse(await readFile(path.join(s.options.receiptDir, 'transfer-0.json'), 'utf8')).status, 'needs_readback_review_do_not_repeat_patch');
  }
});
