import assert from 'node:assert/strict';
import test from 'node:test';
import { readFile, mkdtemp, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { extractPrompt, sha256 } from '../src/scripts/syncElevenLabsPromptOnly.mjs';
import { comparisonBody, digest } from '../src/scripts/verifyElevenLabsComparison.mjs';
import { syncComparisonPrompts } from '../src/scripts/syncElevenLabsComparisonPrompts.mjs';
import { SCREENING_RECOVERY_POLICY } from '../src/scripts/elevenLabsScreeningRecovery.mjs';
import { NATIVE_RECORDING_POLICY } from '../src/scripts/elevenLabsLiveFlowRepair.mjs';
import { applyTerminalOpeningRepair, buildTerminalOpeningPrompt, restoreR28Prompt, terminalOpeningProtectedHashes,
  ROOT_REPLACEMENTS, TERMINAL_RECORDING_POLICY, LISTENER_STATE_POLICY, TERMINAL_OPENING_PATHS } from '../src/scripts/elevenLabsTerminalOpeningRepair.mjs';

const prompt = extractPrompt(await readFile(new URL('../docs/elevenlabs-agent-prompt.md', import.meta.url), 'utf8'));
const old = restoreR28Prompt(prompt);
const terminal = 'On attempt two, a recorded screener ending with “not available” and an additional-message invitation goes directly to recorded_no_voicemail_exit. On either attempt, the same completed automated phone menu played twice without an intervening human also goes directly to recorded_no_voicemail_exit. End silently before skip_turn. Preserve a real hold, later live speech and normal mailbox handling.';
const ids = ['agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks', 'agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv'];
function fixture(i = 0) {
  const nodes = { recorded_no_voicemail_exit: { type: 'end', edge_order: [] } }, edges = {};
  for (const source of ['opening_listener', 'main_conversation', 'short_listing_recovery']) {
    const guards = ['terminal_contact_from_' + source, 'terminal_guard_from_' + source];
    nodes[source] = { type: 'override_agent', edge_order: [...guards], additional_prompt: '',
      conversation_config: { agent: { prompt: { prompt: 'protected', tool_ids: ['protected'] } } } };
    for (const id of guards) edges[id] = { source, target: 'guard', forward_condition: { condition: 'Preserve live requests, questions and consent.' } };
    if (source === 'short_listing_recovery') continue;
    const id = source + '_to_recorded_no_voicemail_exit';
    nodes[source].edge_order.push(id);
    edges[id] = { source, target: 'recorded_no_voicemail_exit', forward_condition: { type: 'llm', condition: terminal + ' old fallback' } };
  }
  nodes.opening_listener.conversation_config.agent.prompt.prompt = old + '\n\n[CRISP_OPENING_HANDOFF_POLICY]' +
    [terminal, SCREENING_RECOVERY_POLICY, NATIVE_RECORDING_POLICY].join('\n\n');
  nodes.main_conversation.additional_prompt = ROOT_REPLACEMENTS[2][0] + '\n\n' + terminal + '\n\nProtected transfer policy.';
  nodes.short_listing_recovery.conversation_config.agent.prompt.prompt = SCREENING_RECOVERY_POLICY + '\n\n' +
    'If this is the same listener who already heard your identity and is only repeating hello or checking your presence, say exactly:';
  return { agent_id: 'agent_9901kpkhs1cmfgj8w9vm199nc92j', branch_id: ids[i], version_id: 'old_' + i,
    conversation_config: { agent: { prompt: { prompt: old, llm: 'gpt-4.1', tool_ids: ['protected'], tools: [] } },
      tts: { model_id: i ? 'eleven_v4_turbo' : 'eleven_flash_v2', voice_id: 'eryn', pvc_strength: 0 }, turn: { turn_model: 'turn_v3' } },
    workflow: { nodes, edges }, platform_settings: { protected: true }, phone_numbers: [], whatsapp_accounts: [], procedures: {} };
}

test('r29 replaces the exact r28 root, preserves live safeguards and closes contradictory recording instructions', () => {
  assert.equal(sha256(old), '8b8d58e09f7c55af188845fb524a8cc387456e4c47c7e1d7e941cc8c3e38cb48');
  assert.equal(buildTerminalOpeningPrompt(old), prompt);
  assert.ok(prompt.trim().split(/\s+/).length < 6300);
  const a = fixture(), before = structuredClone(a), b = applyTerminalOpeningRepair(a, prompt);
  assert.deepEqual(a, before);
  assert.deepEqual(terminalOpeningProtectedHashes(a), terminalOpeningProtectedHashes(b));
  assert.equal(digest(comparisonBody(b)), digest(comparisonBody(applyTerminalOpeningRepair(fixture(1), prompt))));
  const changed = [];
  function walk(x, y, p = '') {
    if (JSON.stringify(x) === JSON.stringify(y)) return;
    if (!x || !y || typeof x !== 'object' || typeof y !== 'object' || Array.isArray(x)) { changed.push(p); return; }
    for (const key of new Set([...Object.keys(x), ...Object.keys(y)])) walk(x[key], y[key], p ? p + '.' + key : key);
  }
  walk(a, b);
  assert.deepEqual(changed.sort(), [...TERMINAL_OPENING_PATHS].sort());
  for (const source of ['opening_listener', 'main_conversation', 'short_listing_recovery']) {
    assert.deepEqual(b.workflow.nodes[source].edge_order.slice(0, 3), [
      'terminal_contact_from_' + source, 'terminal_guard_from_' + source, source + '_to_recorded_no_voicemail_exit']);
    assert.equal(b.workflow.edges[source + '_to_recorded_no_voicemail_exit'].forward_condition.condition, TERMINAL_RECORDING_POLICY);
  }
  assert.match(TERMINAL_RECORDING_POLICY, /Active connecting\/hold and incomplete recordings are NOT terminal/);
  assert.match(TERMINAL_RECORDING_POLICY, /Later actual live speech, questions, callbacks, opt-outs, takeover and pending live questions retain priority/);
  assert.match(LISTENER_STATE_POLICY, /BOTH delivered identity AND this listener's explicit answer confirming the exact listing/);
  assert.match(LISTENER_STATE_POLICY, /Somebody there\?/);
  assert.doesNotMatch(b.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt, /invoke voicemail_detection immediately ONLY/);
});

test('r29 rejects drift, unintended root changes, missing guards and already applied release', () => {
  for (const mutate of [a => a.conversation_config.agent.prompt.prompt += ' drift',
    a => a.workflow.nodes.opening_listener.edge_order.reverse(),
    a => a.workflow.nodes.recorded_no_voicemail_exit.type = 'phone_number',
    a => a.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt += '\n\n[CRISP_OPENING_HANDOFF_POLICY]',
    a => a.workflow.nodes.short_listing_recovery.edge_order.reverse(),
    a => a.workflow.edges.short_listing_recovery_to_recorded_no_voicemail_exit = {}]) {
    const a = fixture(); mutate(a); assert.throws(() => applyTerminalOpeningRepair(a, prompt));
  }
  assert.throws(() => applyTerminalOpeningRepair(fixture(), prompt + ' pitch'));
  assert.throws(() => applyTerminalOpeningRepair(applyTerminalOpeningRepair(fixture(), prompt), prompt));
});

test('production publisher keeps 0/100, patches inactive branch first, requires dry-run and never changes allocation/tools', async t => {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'terminal-opening-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const promptPath = path.join(dir, 'prompt.md'); await writeFile(promptPath, '## Prompt\n' + prompt);
  let agents = [fixture(), fixture(1)], percentages = [0, 100], ambiguous = false;
  const writes = [];
  const fetcher = async (url, options) => {
    if (url.endsWith('/branches')) return { ok: true, json: async () => ({ meta: { total: 2 }, results: agents.map((a, i) => ({ id: a.branch_id, is_archived: false, current_live_percentage: percentages[i] })) }) };
    const i = ids.indexOf(new URL(url).searchParams.get('branch_id'));
    assert.ok(i >= 0);
    if (options.method === 'PATCH') {
      writes.push(i); if (ambiguous) throw Error('ambiguous write');
      const body = JSON.parse(options.body);
      assert.deepEqual(Object.keys(body).sort(), ['conversation_config', 'workflow']);
      assert.deepEqual(body.conversation_config, { agent: { prompt: { prompt } } });
      agents[i].conversation_config.agent.prompt.prompt = prompt; agents[i].workflow = body.workflow; agents[i].version_id = 'new_' + i;
    } else assert.equal(options.method, 'GET');
    return { ok: true, json: async () => structuredClone(agents[i]) };
  };
  const opts = { apiKey: 'test', agentId: agents[0].agent_id, branches: agents.map(a => ({ id: a.branch_id, version: a.version_id, promptHash: sha256(old) })), promptPath, receiptDir: dir, repairTerminalOpening: true };
  await assert.rejects(syncComparisonPrompts({ ...opts, apply: true }, fetcher)); assert.deepEqual(writes, []);
  percentages = [50, 50]; await assert.rejects(syncComparisonPrompts(opts, fetcher), /0 \/ v4 100/);
  percentages = [0, 100]; await assert.rejects(syncComparisonPrompts({ ...opts, branches: [...opts.branches].reverse() }, fetcher), /inactive Flash/);
  const plan = await syncComparisonPrompts(opts, fetcher); assert.equal(plan.release, 'v4-production-terminal-opening-r29');
  const receipt = await syncComparisonPrompts({ ...opts, apply: true }, fetcher);
  assert.equal(receipt.status, 'live_v4_production_release_verified'); assert.deepEqual(writes, [0, 1]);
  assert.deepEqual(receipt.trafficPercent, [0, 100]); assert.equal(receipt.realCallsPlaced, false);
  agents = [fixture(), fixture(1)]; writes.length = 0;
  await syncComparisonPrompts(opts, fetcher); ambiguous = true;
  await assert.rejects(syncComparisonPrompts({ ...opts, apply: true }, fetcher), /ambiguous write/); assert.deepEqual(writes, [0]);
});
