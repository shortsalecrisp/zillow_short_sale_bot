import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import { AVAILABILITY_TOOL_ID as oldId, buildAvailabilityHoldAudio, availabilityAudioProtected,
  applyTransferHoldAudio, holdAudioProtectedHashes, prepareAvailabilityHoldAudio } from '../src/scripts/elevenLabsTransferHoldAudio.mjs';
import { TRANSFER_CONTEXTS } from '../src/scripts/elevenLabsAcceptedTransferWorkflow.mjs';
import { syncComparisonPrompts } from '../src/scripts/syncElevenLabsComparisonPrompts.mjs';
import { digest, verifyLiveTransferAudio } from '../src/scripts/verifyElevenLabsComparison.mjs';
import { sha256 } from '../src/scripts/syncElevenLabsPromptOnly.mjs';

const newId = 'tool_syntheticmusiconly';
const webhook = () => ({ type: 'webhook', name: 'live_transfer_requested', tool_call_sound: null,
  tool_call_sound_behavior: 'auto', disable_interruptions: true, interruption_mode: 'disable_during_tool_and_turn',
  response_timeout_secs: 20, execution_mode: 'immediate', api_schema: {
    method: 'POST', url: 'https://crisp-voice-bot.onrender.com/elevenlabs/tool/live-transfer-requested',
    request_headers: { 'x-crisp-elevenlabs-secret': 'synthetic-secret' }, request_body_schema: { preserved: true } } });

function fixture(model = 'eleven_flash_v2', branch = 'control') {
  const native = () => ({ name: 'transfer_to_number', type: 'system', interruption_mode: 'allow',
    tool_call_sound: null, tool_call_sound_behavior: 'auto', description: 'Preserve current consent, callback, opt-out and takeover gates.',
    params: { system_tool_type: 'transfer_to_number', transfers: [{ transfer_type: 'conference', transfer_destination: { type: 'phone', phone_number: '+12025550199' } }] } });
  return { agent_id: 'agent_test', branch_id: branch, version_id: 'v_' + branch,
    conversation_config: { tts: { model_id: model, voice_id: 'eryn' }, turn: { turn_model: 'turn_v3' },
      agent: { prompt: { llm: 'gpt-4.1', prompt: 'Approved unchanged business and safety policy. '.repeat(8).trim(),
        tool_ids: [oldId, 'tool_callback', 'tool_optout'], tools: [webhook()] } } },
    workflow: { nodes: { ...Object.fromEntries(TRANSFER_CONTEXTS.map(id => [id, { type: 'override_agent',
      additional_prompt: 'Preserved current-caller consent, takeover and guarded ending.', edge_order: ['guarded'],
      conversation_config: { agent: { prompt: { tool_ids: [oldId, 'tool_callback'],
        built_in_tools: { end_call: null, transfer_to_number: native() } } } } }])),
      transfer_check: { type: 'tool', tools: [{ tool_id: oldId, schema_overrides: null }] },
      callback_receipt_wait: { type: 'override_agent', additional_prompt: 'Wait for caller', conversation_config: { agent: { prompt: { tool_ids: [] } } } } },
      edges: { guarded: { source: 'opening_listener', target: 'guard', condition: 'unchanged' } } },
    platform_settings: { unchanged: true }, phone_numbers: ['owned'], procedures: {}, whatsapp_accounts: [] };
}

test('availability music stays interruptible and outlasts the 35-second owner-approval guard', () => {
  const before = webhook(), after = buildAvailabilityHoldAudio(before);
  assert.equal(after.tool_call_sound, 'elevator1'); assert.equal(after.tool_call_sound_behavior, 'always');
  assert.equal(after.interruption_mode, 'allow'); assert.equal(after.disable_interruptions, false);
  assert.ok(after.response_timeout_secs > 35);
  assert.equal(availabilityAudioProtected(after), availabilityAudioProtected(before));
  assert.deepEqual(after.api_schema, before.api_schema, 'No routing, auth or request changes');
  assert.deepEqual(before, webhook());
});

test('both transfer stages receive the same audio without altering any safety or connection rule', () => {
  const before = fixture(), next = applyTransferHoldAudio(before, newId);
  assert.deepEqual(holdAudioProtectedHashes(next, newId), holdAudioProtectedHashes(before, newId));
  assert.deepEqual(before, fixture()); assert.deepEqual(applyTransferHoldAudio(next, newId), next);
  assert.deepEqual(next.conversation_config.agent.prompt.tool_ids, [newId, 'tool_callback', 'tool_optout']);
  for (const id of TRANSFER_CONTEXTS) {
    const node = next.workflow.nodes[id], tool = node.conversation_config.agent.prompt.built_in_tools.transfer_to_number;
    assert.equal(tool.tool_call_sound, 'elevator1'); assert.equal(tool.tool_call_sound_behavior, 'always');
    assert.equal(tool.interruption_mode, 'allow'); assert.deepEqual(tool.params, before.workflow.nodes[id].conversation_config.agent.prompt.built_in_tools.transfer_to_number.params);
    assert.equal(node.additional_prompt, before.workflow.nodes[id].additional_prompt); assert.deepEqual(node.edge_order, before.workflow.nodes[id].edge_order);
  }
  assert.equal(next.workflow.nodes.transfer_check.tools[0].tool_id, newId);
  assert.deepEqual(next.workflow.nodes.callback_receipt_wait, before.workflow.nodes.callback_receipt_wait);
  assert.deepEqual(next.workflow.edges, before.workflow.edges);
  for (const change of [a => a.workflow.nodes.opening_listener.conversation_config.agent.prompt.built_in_tools.transfer_to_number.interruption_mode = 'disable_during_tool',
    a => a.conversation_config.agent.prompt.tool_ids = ['tool_unknown']]) {
    const bad = fixture(); change(bad); assert.throws(() => applyTransferHoldAudio(bad, newId), /required|missing/);
  }
});

async function setup(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'transfer-audio-')); t.after(() => rm(dir, { recursive: true, force: true }));
  const original = webhook(), clone = buildAvailabilityHoldAudio(original), agents = [fixture(), fixture('eleven_v4_turbo', 'test')];
  let posts = 0, failPost = false, failPatch = false; const writes = [];
  const fetcher = async (url, init) => {
    if (url.endsWith('/branches')) return { ok: true, json: async () => ({ results: agents.map(a => ({ id: a.branch_id, is_archived: false, current_live_percentage: 50 })), meta: { total: 2 } }) };
    if (url.includes('/tools')) {
      if (init.method === 'POST') { posts++; if (failPost) throw Error('uncertain create'); return { ok: true, json: async () => ({ id: newId }) }; }
      const id = url.split('/').at(-1); assert.ok([oldId, newId].includes(id));
      return { ok: true, json: async () => ({ id, tool_config: structuredClone(id === oldId ? original : clone) }) };
    }
    const a = agents.find(a => a.branch_id === new URL(url).searchParams.get('branch_id')); assert.ok(a);
    if (init.method === 'PATCH') {
      writes.push(a.branch_id); if (failPatch) throw Error('uncertain patch');
      const b = JSON.parse(init.body); assert.deepEqual(Object.keys(b.conversation_config.agent.prompt), ['tool_ids']);
      a.workflow = b.workflow; a.conversation_config.agent.prompt.tool_ids = b.conversation_config.agent.prompt.tool_ids;
      a.version_id += '_audio';
    }
    return { ok: true, json: async () => structuredClone(a) };
  };
  const promptPath = path.join(dir, 'prompt.md'); await writeFile(promptPath, '## Prompt\n' + agents[0].conversation_config.agent.prompt.prompt);
  const holdAudio = { id: newId, sourceHash: digest(original), candidateHash: digest(clone) };
  const options = { apiKey: 'synthetic', agentId: 'agent_test', branches: agents.map(a => ({ id: a.branch_id, version: a.version_id,
    promptHash: sha256(a.conversation_config.agent.prompt.prompt) })), promptPath, receiptDir: path.join(dir, 'comparison'), holdAudio };
  return { dir, options, fetcher, agents, writes, original, clone, posts: () => posts, failCreate: () => failPost = true, failPatch: () => failPatch = true };
}

test('clone creation is exact, journaled, read back, never blindly repeated, and does not attach or call', async t => {
  const s = await setup(t), o = { apiKey: 'synthetic', expectedHash: digest(s.original), receiptDir: s.dir };
  assert.equal((await prepareAvailabilityHoldAudio(o, s.fetcher)).status, 'prepared_not_applied'); assert.equal(s.posts(), 0);
  const r = await prepareAvailabilityHoldAudio({ ...o, apply: true }, s.fetcher);
  assert.equal(r.id, newId); assert.equal(r.status, 'clone_verified_not_attached'); assert.equal(s.posts(), 1);
  await assert.rejects(prepareAvailabilityHoldAudio({ ...o, apply: true }, s.fetcher), /EEXIST/); assert.equal(s.posts(), 1); assert.equal(s.writes.length, 0);
});

test('uncertain creation and patch never repeat or proceed to the other arm', async t => {
  const s = await setup(t), o = { apiKey: 'synthetic', expectedHash: digest(s.original), receiptDir: s.dir };
  await prepareAvailabilityHoldAudio(o, s.fetcher); s.failCreate();
  await assert.rejects(prepareAvailabilityHoldAudio({ ...o, apply: true }, s.fetcher), /uncertain/);
  await assert.rejects(prepareAvailabilityHoldAudio({ ...o, apply: true }, s.fetcher), /EEXIST/); assert.equal(s.posts(), 1);
  await syncComparisonPrompts(s.options, s.fetcher); s.failPatch();
  await assert.rejects(syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher), /uncertain/); assert.deepEqual(s.writes, ['control']);
});

test('two-arm audio release requires exact original/clone hashes and final native audio equality', async t => {
  const s = await setup(t); await syncComparisonPrompts(s.options, s.fetcher); assert.equal(s.writes.length, 0);
  const r = await syncComparisonPrompts({ ...s.options, apply: true }, s.fetcher);
  assert.equal(r.status, 'live_uniform_comparison_verified'); assert.deepEqual(s.writes, ['control', 'test']);
  assert.equal(r.audioBehaviorValidated, false, 'Configuration is not acoustic connection-stage proof'); assert.equal(r.realCallsPlaced, false);
  const bad = await setup(t); bad.options.holdAudio.candidateHash = '0'.repeat(64);
  await assert.rejects(syncComparisonPrompts(bad.options, bad.fetcher), /drifted/); assert.equal(bad.writes.length, 0);
});

test('separate shared-tool verification is GET-only and never claims acoustic connection proof', async () => {
  const tool = buildAvailabilityHoldAudio(webhook()); let calls = 0;
  const expected = { availabilityToolId: newId, availabilityToolHash: digest(tool), sound: 'elevator1' };
  const fetcher = async (url, init) => { calls++; assert.equal(init.method, 'GET'); assert.ok(url.endsWith(newId));
    return { ok: true, json: async () => ({ id: newId, tool_config: tool }) }; };
  const r = await verifyLiveTransferAudio({ apiKey: 'synthetic', expected }, fetcher);
  assert.equal(calls, 1); assert.equal(r.ok, true); assert.equal(r.audioPlaybackVerified, false);
  tool.api_schema.request_body_schema.preserved = false;
  await assert.rejects(verifyLiveTransferAudio({ apiKey: 'synthetic', expected }, fetcher), /drifted/);
});
