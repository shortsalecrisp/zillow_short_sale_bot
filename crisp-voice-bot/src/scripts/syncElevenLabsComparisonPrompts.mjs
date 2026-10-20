import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { assertExpected, extractPrompt, protectedHashes, sha256, syncPromptOnly } from './syncElevenLabsPromptOnly.mjs';
import { comparisonBody, digest } from './verifyElevenLabsComparison.mjs';
export { comparisonBody, digest };
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

export async function syncComparisonPrompts({ apiKey, agentId, branches, promptPath, receiptDir, apply = false }, fetcher = fetch) {
  if (!apiKey || !/^agent_[a-z0-9]+$/.test(agentId ?? '') || branches?.length !== 2
    || new Set(branches.map(b => b.id)).size !== 2 || !receiptDir) throw new Error('Two explicit reviewed branches required');
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
    if (active?.length !== 2 || !branches.every(b => active.some(a => a.id === b.id && a.current_live_percentage === 50))) {
      throw new Error('Existing exact 50/50 comparison required');
    }
  };
  const prompt = extractPrompt(await readFile(promptPath, 'utf8'));
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
    const result = structuredClone(agent);
    result.conversation_config.agent.prompt.prompt = prompt;
    return result;
  });
  assertComparison(...candidates);
  const plan = { agentId, branches, promptHash: sha256(prompt), protected: before.map(protectedHashes),
    sharedHash: digest(comparisonBody(candidates[0])), workflowHash: digest(candidates[0].workflow) };
  await mkdir(receiptDir, { recursive: true, mode: 0o700 });
  const planPath = path.join(receiptDir, 'comparison-plan.json');
  if (!apply) {
    await writeFile(planPath, JSON.stringify(plan, null, 2), { mode: 0o600 });
    return { ...plan, status: 'prepared_not_applied' };
  }
  if (digest(JSON.parse(await readFile(planPath, 'utf8'))) !== digest(plan)) throw new Error('Reviewed comparison plan drifted');
  // Check both arms immediately before the first write. Ambiguous writes are never retried here.
  for (let i = 0; i < branches.length; i++) {
    const fresh = await read(branches[i]);
    assertExpected(fresh, { agentId, branchId: branches[i].id, expectedVersion: branches[i].version, expectedPromptHash: branches[i].promptHash });
    if (digest(protectedHashes(fresh)) !== digest(plan.protected[i])) throw new Error('Protected configuration drifted');
  }
  const receipts = [];
  for (let i = 0; i < branches.length; i++) {
    if (sha256(prompt) === branches[i].promptHash) continue;
    receipts.push(await syncPromptOnly({ apiKey, agentId, branchId: branches[i].id,
      expectedVersion: branches[i].version, expectedPromptHash: branches[i].promptHash,
      promptPath, receiptDir: path.join(receiptDir, branches[i].id), apply: true, comparisonManaged: true }, fetcher));
  }
  const after = [];
  for (let i = 0; i < branches.length; i++) {
    const agent = await read(branches[i]);
    if (agent.agent_id !== agentId || agent.branch_id !== branches[i].id || sha256(agent.conversation_config.agent.prompt.prompt) !== plan.promptHash
      || digest(protectedHashes(agent)) !== digest(plan.protected[i])) throw new Error('Final comparison readback failed');
    after.push(agent);
  }
  assertComparison(...after);
  await traffic();
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
    receiptDir: path.resolve(at('--receipt-dir')), apply: args.includes('--apply') })
    .then(receipt => console.log(JSON.stringify(receipt)))
    .catch(error => { console.error(error.message); process.exitCode = 1; });
}
