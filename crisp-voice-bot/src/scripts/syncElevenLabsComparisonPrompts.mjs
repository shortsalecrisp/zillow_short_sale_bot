import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { assertExpected, extractPrompt, protectedHashes, sha256, syncPromptOnly } from './syncElevenLabsPromptOnly.mjs';
import { comparisonBody, digest } from './verifyElevenLabsComparison.mjs';
import { applyAcceptedTransferWorkflow, transferProtectedHashes } from './elevenLabsAcceptedTransferWorkflow.mjs';
import { applyTransferHoldAudio, holdAudioProtectedHashes, AVAILABILITY_TOOL_ID, buildAvailabilityHoldAudio } from './elevenLabsTransferHoldAudio.mjs';
export { comparisonBody, digest };
function contactProtectedHashes(agent) {
  const config = structuredClone(agent.conversation_config);
  delete config.agent.prompt.prompt;
  delete config.agent.prompt.tool_ids;
  delete config.agent.prompt.tools;
  return {
    conversationConfig: digest(config), platformSettings: digest(agent.platform_settings ?? null),
    phoneNumbers: digest(agent.phone_numbers ?? []), whatsappAccounts: digest(agent.whatsapp_accounts ?? []),
    procedures: digest(agent.procedures ?? {}),
  };
}
export function replaceContactBindings(body, replacements) {
  const oldIds = Object.keys(replacements ?? {}), newIds = Object.values(replacements ?? {});
  if (oldIds.length !== 3 || new Set(oldIds).size !== 3 || new Set(newIds).size !== 3
    || [...oldIds, ...newIds].some(id => !/^tool_[a-z0-9]+$/.test(id)) || newIds.some(id => oldIds.includes(id))) {
    throw new Error('Exact reviewed three-tool replacement map required');
  }
  const result = structuredClone(body);
  const global = result.conversation_config?.agent?.prompt?.tool_ids;
  if (!Array.isArray(global) || oldIds.some(id => global.filter(value => value === id).length !== 1)
    || newIds.some(id => global.includes(id))) throw new Error('Reviewed contact-tool bindings drifted');
  const supported = path => path.length === 5 && path.slice(0, 4).join('.') === 'conversation_config.agent.prompt.tool_ids'
    || path[0] === 'workflow' && (path.at(-1) === 'tool_id'
      || ['tool_ids', 'additional_tool_ids'].includes(String(path.at(-2))));
  let replaced = 0;
  const walk = (value, at = []) => {
    if (Array.isArray(value)) return value.forEach((item, index) => {
      const next = [...at, index];
      if (oldIds.includes(item)) {
        if (!supported(next)) throw new Error('Contact tool appears outside a supported binding path');
        value[index] = replacements[item]; replaced++;
      } else walk(item, next);
    });
    if (!value || typeof value !== 'object') return;
    for (const [key, item] of Object.entries(value)) {
      const next = [...at, key];
      if (oldIds.includes(item)) {
        if (!supported(next)) throw new Error('Contact tool appears outside a supported binding path');
        value[key] = replacements[item]; replaced++;
      } else walk(item, next);
    }
  };
  walk({ conversation_config: result.conversation_config, workflow: result.workflow });
  const remaining = [];
  const findOld = value => {
    if (Array.isArray(value)) return value.forEach(findOld);
    if (value && typeof value === 'object') return Object.values(value).forEach(findOld);
    if (oldIds.includes(value)) remaining.push(value);
  };
  findOld({ conversation_config: result.conversation_config, workflow: result.workflow });
  if (replaced < 3 || remaining.length) {
    throw new Error('Contact-tool binding replacement was incomplete');
  }
  return result;
}
export function assertComparison(control, test) {
  if (control.conversation_config.tts.model_id !== 'eleven_flash_v2'
    || test.conversation_config.tts.model_id !== 'eleven_v4_turbo'
    || digest(comparisonBody(control)) !== digest(comparisonBody(test))) {
    throw new Error('Comparison arms must differ only in TTS model');
  }
  for (const key of ['platform_settings', 'phone_numbers', 'whatsapp_accounts', 'procedures']) {
    if (digest(control[key] ?? null) !== digest(test[key] ?? null)) throw new Error('Comparison arms must differ only in TTS model');
  }
}

export async function syncComparisonPrompts({ apiKey, agentId, branches, promptPath, receiptDir, apply = false, repairAcceptedTransfer = false, holdAudio,
  contactToolReplacements, contactToolMapHash, verifyContactTools }, fetcher = fetch) {
  if (!apiKey || !/^agent_[a-z0-9]+$/.test(agentId ?? '') || branches?.length !== 2
    || new Set(branches.map(b => b.id)).size !== 2 || !receiptDir) throw new Error('Two explicit reviewed branches required');
  if ([repairAcceptedTransfer, Boolean(holdAudio), Boolean(contactToolReplacements)].filter(Boolean).length > 1) throw new Error('One reviewed release profile at a time');
  const contactRelease = Boolean(contactToolReplacements);
  if (contactRelease && (!/^[a-f0-9]{64}$/.test(contactToolMapHash ?? '') || typeof verifyContactTools !== 'function')) {
    throw new Error('Verified contact-tool map and fresh resource verifier required');
  }
  const managedRelease = repairAcceptedTransfer || Boolean(holdAudio) || contactRelease;
  const endpoint = `https://api.elevenlabs.io/v1/convai/agents/${agentId}`;
  const get = async url => {
    const r = await fetcher(url, { method: 'GET', headers: { 'xi-api-key': apiKey }, signal: AbortSignal.timeout(45000) });
    if (!r.ok) throw new Error(`Provider read failed HTTP ${r.status}`);
    return r.json();
  };
  const read = branch => get(`${endpoint}?branch_id=${encodeURIComponent(branch.id)}`);
  const traffic = async () => {
    const allocation = await get(`${endpoint}/branches`);
    const active = allocation.results?.filter(b => !b.is_archived && b.current_live_percentage > 0);
    if ((managedRelease && allocation.meta?.total !== allocation.results?.length)
      || active?.length !== 2 || !branches.every(b => active.some(a => a.id === b.id && a.current_live_percentage === 50))) {
      throw new Error('Existing exact 50/50 comparison required');
    }
  };
  const prompt = extractPrompt(await readFile(promptPath, 'utf8'));
  const protectedForRelease = contactRelease ? contactProtectedHashes : holdAudio ? a => holdAudioProtectedHashes(a, holdAudio.id) : repairAcceptedTransfer ? transferProtectedHashes : protectedHashes;
  const verifyAudioTool = async () => {
    if (!holdAudio || !/^tool_[a-z0-9]+$/.test(holdAudio.id ?? '') || holdAudio.id === AVAILABILITY_TOOL_ID
      || !/^[a-f0-9]{64}$/.test(holdAudio.sourceHash ?? '') || !/^[a-f0-9]{64}$/.test(holdAudio.candidateHash ?? '')) throw new Error('Exact reviewed shared audio-tool clone required');
    const old = await get('https://api.elevenlabs.io/v1/convai/tools/' + AVAILABILITY_TOOL_ID);
    const clone = await get('https://api.elevenlabs.io/v1/convai/tools/' + holdAudio.id);
    if (old.id !== AVAILABILITY_TOOL_ID || clone.id !== holdAudio.id || digest(old.tool_config) !== holdAudio.sourceHash
      || digest(clone.tool_config) !== holdAudio.candidateHash || digest(buildAvailabilityHoldAudio(old.tool_config)) !== holdAudio.candidateHash) {
      throw new Error('Shared availability audio-tool readback drifted');
    }
  };
  if (holdAudio) await verifyAudioTool();
  if (contactRelease) await verifyContactTools();
  const before = [];
  await traffic();
  for (const branch of branches) {
    const agent = await read(branch);
    assertExpected(agent, { agentId, branchId: branch.id, expectedVersion: branch.version, expectedPromptHash: branch.promptHash });
    if (agent.conversation_config.agent.prompt.llm !== 'gpt-4.1' || agent.conversation_config.turn.turn_model !== 'turn_v3') {
      throw new Error('Preserve the approved LLM and turn model');
    }
    before.push(agent);
  }
  const candidates = before.map(agent => {
    const result = contactRelease ? replaceContactBindings(agent, contactToolReplacements)
      : holdAudio ? applyTransferHoldAudio(agent, holdAudio.id) : repairAcceptedTransfer ? applyAcceptedTransferWorkflow(agent) : structuredClone(agent);
    if (managedRelease && !contactRelease && result.conversation_config.agent.prompt.prompt !== prompt) {
      throw new Error('Managed transfer repair must preserve the existing business prompt');
    }
    result.conversation_config.agent.prompt.prompt = prompt;
    return result;
  });
  assertComparison(...candidates);
  const plan = { agentId, branches, promptHash: sha256(prompt), protected: before.map(protectedForRelease),
    ...(managedRelease ? { release: contactRelease ? 'recorded-voicemail-contact-guard-r12' : holdAudio ? 'transfer-hold-audio-v1' : 'accepted-transfer-v1',
      ...(holdAudio ? { holdAudio } : {}), ...(contactRelease ? { contactToolMapHash } : {}), beforeBodies: before.map(a => digest(comparisonBody(a))) } : {}),
    sharedHash: digest(comparisonBody(candidates[0])), workflowHash: digest(candidates[0].workflow) };
  await mkdir(receiptDir, { recursive: true, mode: 0o700 });
  const planPath = path.join(receiptDir, 'comparison-plan.json');
  if (!apply) {
    await writeFile(planPath, JSON.stringify(plan, null, 2), { mode: 0o600 });
    return { ...plan, status: 'prepared_not_applied' };
  }
  if (digest(JSON.parse(await readFile(planPath, 'utf8'))) !== digest(plan)) throw new Error('Reviewed comparison plan drifted');
  if (contactRelease) await verifyContactTools();
  // Check both arms immediately before the first write. Ambiguous writes are never retried here.
  for (let i = 0; i < branches.length; i++) {
    const fresh = await read(branches[i]);
    assertExpected(fresh, { agentId, branchId: branches[i].id, expectedVersion: branches[i].version, expectedPromptHash: branches[i].promptHash });
    if (digest(protectedForRelease(fresh)) !== digest(plan.protected[i])
      || (managedRelease && digest(comparisonBody(fresh)) !== plan.beforeBodies[i])) throw new Error('Protected configuration drifted');
  }
  const receipts = [];
  for (let i = 0; i < branches.length; i++) {
    if (managedRelease) {
      // Managed profiles preserve the business prompt; audio may replace only the reviewed availability binding.
      const fresh = await read(branches[i]);
      assertExpected(fresh, { agentId, branchId: branches[i].id, expectedVersion: branches[i].version, expectedPromptHash: branches[i].promptHash });
      if (digest(comparisonBody(fresh)) !== plan.beforeBodies[i]) throw new Error('Reviewed workflow drifted before write');
      if (digest(comparisonBody(fresh)) === plan.sharedHash) {
        receipts.push({ branchId: branches[i].id, status: 'already_matches', afterVersion: fresh.version_id });
        continue;
      }
      const attempt = { branchId: branches[i].id, beforeVersion: branches[i].version, status: 'patch_attempted_readback_required' };
      const attemptPath = path.join(receiptDir, `transfer-${i}.json`);
      await writeFile(attemptPath, JSON.stringify(attempt, null, 2), { mode: 0o600 });
      try {
        const r = await fetcher(`${endpoint}?branch_id=${encodeURIComponent(branches[i].id)}&enable_versioning_if_not_enabled=true`, {
          method: 'PATCH', headers: { 'xi-api-key': apiKey, 'Content-Type': 'application/json' },
          body: JSON.stringify({ workflow: candidates[i].workflow, ...((holdAudio || contactRelease) ? {
            conversation_config: { agent: { prompt: { ...(contactRelease ? { prompt } : {}), tool_ids: candidates[i].conversation_config.agent.prompt.tool_ids } } } } : {}) }), signal: AbortSignal.timeout(60000) });
        if (!r.ok) throw new Error(`Provider transfer PATCH failed HTTP ${r.status}; read back before retrying`);
        const after = await read(branches[i]);
        if (after.agent_id !== agentId || after.branch_id !== branches[i].id || !after.version_id
          || digest(comparisonBody(after)) !== plan.sharedHash
          || digest(protectedForRelease(after)) !== digest(plan.protected[i])) throw new Error('Transfer workflow readback differs');
        Object.assign(attempt, { status: 'branch_verified', afterVersion: after.version_id });
        receipts.push(attempt);
        await writeFile(attemptPath, JSON.stringify(attempt, null, 2), { mode: 0o600 });
      } catch (error) {
        attempt.status = 'needs_readback_review_do_not_repeat_patch';
        await writeFile(attemptPath, JSON.stringify(attempt, null, 2), { mode: 0o600 });
        throw error;
      }
      continue;
    }
    if (sha256(prompt) === branches[i].promptHash) continue;
    receipts.push(await syncPromptOnly({ apiKey, agentId, branchId: branches[i].id,
      expectedVersion: branches[i].version, expectedPromptHash: branches[i].promptHash,
      promptPath, receiptDir: path.join(receiptDir, branches[i].id), apply: true, comparisonManaged: true }, fetcher));
  }
  const after = [];
  for (let i = 0; i < branches.length; i++) {
    const agent = await read(branches[i]);
    if (agent.agent_id !== agentId || agent.branch_id !== branches[i].id || sha256(agent.conversation_config.agent.prompt.prompt) !== plan.promptHash
      || digest(protectedForRelease(agent)) !== digest(plan.protected[i])
      || (managedRelease && digest(comparisonBody(agent)) !== plan.sharedHash)) throw new Error('Final comparison readback failed');
    after.push(agent);
  }
  assertComparison(...after);
  await traffic();
  if (holdAudio) await verifyAudioTool();
  const receipt = { ...plan, status: 'live_uniform_comparison_verified', verifiedAt: new Date().toISOString(),
    versions: after.map(a => a.version_id), trafficPercent: [50, 50], receipts,
    realCallsPlaced: false, audioBehaviorValidated: false };
  await writeFile(path.join(receiptDir, 'comparison-receipt.json'), JSON.stringify(receipt, null, 2), { mode: 0o600 });
  return receipt;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2), at = key => args[args.indexOf(key) + 1];
  const required = ['--agent-id', '--control-branch', '--control-version', '--control-prompt-hash', '--test-branch', '--test-version', '--test-prompt-hash', '--receipt-dir'];
  if (required.some(key => !args.includes(key))) throw new Error('Explicit reviewed branch/version/prompt hashes and receipt directory required');
  syncComparisonPrompts({ apiKey: process.env.ELEVENLABS_API_KEY, agentId: at('--agent-id'),
    branches: ['control', 'test'].map(arm => ({ id: at('--' + arm + '-branch'), version: at('--' + arm + '-version'), promptHash: at('--' + arm + '-prompt-hash') })),
    promptPath: path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../docs/elevenlabs-agent-prompt.md'),
    receiptDir: path.resolve(at('--receipt-dir')), apply: args.includes('--apply'), repairAcceptedTransfer: args.includes('--repair-accepted-transfer'),
    holdAudio: args.includes('--hold-audio-tool-id') ? { id: at('--hold-audio-tool-id'), sourceHash: at('--hold-audio-source-hash'),
      candidateHash: at('--hold-audio-tool-hash') } : undefined })
    .then(receipt => console.log(JSON.stringify(receipt)))
    .catch(error => { console.error(error.message); process.exitCode = 1; });
}
