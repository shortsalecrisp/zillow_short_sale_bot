import { mkdir, readFile, writeFile, open } from 'node:fs/promises';
import path from 'node:path';
import { comparisonBody, digest } from './verifyElevenLabsComparison.mjs';
import { TRANSFER_CONTEXTS } from './elevenLabsAcceptedTransferWorkflow.mjs';

export const AVAILABILITY_TOOL_ID = 'tool_8801kptzeqqwen0tmr29ykzddem0';
export const TRANSFER_HOLD_SOUND = 'elevator1';
const audioKeys = ['tool_call_sound', 'tool_call_sound_behavior', 'interruption_mode', 'disable_interruptions', 'response_timeout_secs'];

export function availabilityAudioProtected(tool) {
  const next = structuredClone(tool);
  for (const key of audioKeys) delete next[key];
  return digest(next);
}

export function buildAvailabilityHoldAudio(tool) {
  if (tool?.type !== 'webhook' || tool.name !== 'live_transfer_requested'
    || tool.api_schema?.url !== 'https://crisp-voice-bot.onrender.com/elevenlabs/tool/live-transfer-requested'
    || tool.api_schema?.method !== 'POST') throw new Error('Exact approved availability webhook required');
  const next = { ...structuredClone(tool), tool_call_sound: TRANSFER_HOLD_SOUND, tool_call_sound_behavior: 'always',
    interruption_mode: 'allow', disable_interruptions: false, response_timeout_secs: 60 };
  if (availabilityAudioProtected(tool) !== availabilityAudioProtected(next)) throw new Error('Protected availability fields changed');
  return next;
}

export async function prepareAvailabilityHoldAudio({ apiKey, expectedHash, receiptDir, apply = false }, fetcher = fetch) {
  if (!apiKey || !/^[a-f0-9]{64}$/.test(expectedHash ?? '') || !receiptDir) throw new Error('Reviewed availability tool hash required');
  const endpoint = 'https://api.elevenlabs.io/v1/convai/tools';
  const read = async id => {
    const r = await fetcher(endpoint + '/' + id, { method: 'GET', headers: { 'xi-api-key': apiKey } });
    if (!r.ok) throw new Error('Availability tool GET HTTP ' + r.status);
    const t = await r.json(); if (t.id !== id) throw new Error('Availability identity mismatch'); return t.tool_config;
  };
  const before = await read(AVAILABILITY_TOOL_ID);
  if (digest(before) !== expectedHash) throw new Error('Reviewed availability tool drifted');
  const candidate = buildAvailabilityHoldAudio(before);
  const plan = { sourceId: AVAILABILITY_TOOL_ID, sourceHash: expectedHash, candidateHash: digest(candidate),
    protectedHash: availabilityAudioProtected(before), sound: TRANSFER_HOLD_SOUND, behavior: 'always', interruptionMode: 'allow', responseTimeoutSeconds: 60 };
  await mkdir(receiptDir, { recursive: true, mode: 0o700 });
  const planPath = path.join(receiptDir, 'availability-audio-plan.json');
  if (!apply) { await writeFile(planPath, JSON.stringify(plan, null, 2), { mode: 0o600 }); return { ...plan, status: 'prepared_not_applied' }; }
  if (digest(JSON.parse(await readFile(planPath, 'utf8'))) !== digest(plan)) throw new Error('Availability audio plan changed');
  const journalPath = path.join(receiptDir, 'availability-audio-create.json');
  const journal = await open(journalPath, 'wx', 0o600);
  await journal.writeFile(JSON.stringify({ ...plan, status: 'creation_attempted_do_not_repeat' })); await journal.sync(); await journal.close();
  // A shared clone leaves the old tool and historical branch versions untouched.
  const r = await fetcher(endpoint, { method: 'POST', headers: { 'xi-api-key': apiKey, 'Content-Type': 'application/json' },
    body: JSON.stringify({ tool_config: candidate }), signal: AbortSignal.timeout(45000) });
  if (!r.ok) throw new Error('Availability clone HTTP ' + r.status + '; reconcile before repeating');
  const created = await r.json();
  if (!/^tool_[a-z0-9]+$/.test(created.id ?? '') || created.id === AVAILABILITY_TOOL_ID) throw new Error('New availability tool identity missing');
  await writeFile(journalPath, JSON.stringify({ ...plan, id: created.id, status: 'created_readback_required' }, null, 2), { mode: 0o600 });
  const after = await read(created.id), original = await read(AVAILABILITY_TOOL_ID);
  if (digest(after) !== plan.candidateHash || digest(original) !== expectedHash) throw new Error('Availability clone or original readback changed');
  const receipt = { ...plan, id: created.id, status: 'clone_verified_not_attached', verifiedAt: new Date().toISOString() };
  await writeFile(journalPath, JSON.stringify(receipt, null, 2), { mode: 0o600 }); return receipt;
}

function replaceBindings(body, from, to) {
  let count = 0;
  function walk(value, at = []) {
    if (!value || typeof value !== 'object') return;
    for (const [key, child] of Object.entries(value)) {
      const p = [...at, key];
      if (child === from) {
        const list = ['tool_ids', 'additional_tool_ids'].includes(at.at(-1));
        const binding = key === 'tool_id' || (list && Array.isArray(value));
        if (!binding) throw new Error('Tool identity outside a reviewed binding: ' + p.join('.'));
        value[key] = to; count++;
      } else walk(child, p);
    }
  }
  walk(body); return count;
}

export function applyTransferHoldAudio(agent, toolId) {
  if (!/^tool_[a-z0-9]+$/.test(toolId ?? '') || toolId === AVAILABILITY_TOOL_ID) throw new Error('Verified shared audio tool clone required');
  const next = structuredClone(agent); delete next.conversation_config.agent.prompt.tools;
  if (!next.conversation_config.agent.prompt.tool_ids.includes(AVAILABILITY_TOOL_ID)
    && !next.conversation_config.agent.prompt.tool_ids.includes(toolId)) throw new Error('Availability registry binding missing');
  replaceBindings({ conversation_config: next.conversation_config, workflow: next.workflow }, AVAILABILITY_TOOL_ID, toolId);
  for (const id of TRANSFER_CONTEXTS) {
    const tool = next.workflow?.nodes?.[id]?.conversation_config?.agent?.prompt?.built_in_tools?.transfer_to_number;
    if (tool?.params?.system_tool_type !== 'transfer_to_number' || tool.params.transfers?.length !== 1
      || tool.params.transfers[0].transfer_type !== 'conference' || tool.interruption_mode !== 'allow') {
      throw new Error('Reviewed interruptible conference tool required: ' + id);
    }
    tool.tool_call_sound = TRANSFER_HOLD_SOUND; tool.tool_call_sound_behavior = 'always';
  }
  if (digest(holdAudioProtectedHashes(agent, toolId)) !== digest(holdAudioProtectedHashes(next, toolId))) throw new Error('Protected hold-audio configuration changed');
  return next;
}

export function holdAudioProtectedHashes(agent, toolId) {
  const body = comparisonBody(agent);
  replaceBindings(body, toolId, AVAILABILITY_TOOL_ID);
  for (const id of TRANSFER_CONTEXTS) {
    const tool = body.workflow.nodes[id].conversation_config.agent.prompt.built_in_tools.transfer_to_number;
    delete tool.tool_call_sound; delete tool.tool_call_sound_behavior;
  }
  return { unchanged: digest(body), ttsModel: agent.conversation_config.tts.model_id,
    ...Object.fromEntries(['platform_settings', 'phone_numbers', 'whatsapp_accounts', 'procedures'].map(k => [k, digest(agent[k] ?? null)])) };
}
