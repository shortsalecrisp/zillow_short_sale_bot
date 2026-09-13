import { createHash } from 'node:crypto';
import { readFile, mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const sha256 = value => createHash('sha256').update(value).digest('hex');
const canonical = value => Array.isArray(value) ? value.map(canonical)
  : value && typeof value === 'object'
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
const objectHash = value => sha256(JSON.stringify(canonical(value)));

export function extractPrompt(markdown) {
  const parts = markdown.split(/^## Prompt\s*$/m);
  if (parts.length !== 2 || parts[1].trim().length < 100) throw new Error('Expected one nonempty Prompt section');
  return parts[1].trim();
}

export function protectedHashes(agent) {
  const config = structuredClone(agent.conversation_config);
  delete config.agent.prompt.prompt;
  return {
    other_conversation_settings: objectHash(config),
    workflow: objectHash(agent.workflow ?? null),
    platform_settings: objectHash(agent.platform_settings ?? null),
    phone_numbers: objectHash(agent.phone_numbers ?? []),
    whatsapp_accounts: objectHash(agent.whatsapp_accounts ?? []),
    procedures: objectHash(agent.procedures ?? {}),
  };
}

export function assertExpected(agent, { agentId, expectedVersion, expectedPromptHash }) {
  if (agent.agent_id !== agentId || agent.version_id !== expectedVersion
    || sha256(agent.conversation_config.agent.prompt.prompt) !== expectedPromptHash) {
    throw new Error('Live agent drifted from the reviewed version; no update is authorized by this receipt');
  }
  if (!agent.branch_id || agent.branch_id !== agent.main_branch_id) throw new Error('Expected the effective main branch');
}

export async function syncPromptOnly({ apiKey, agentId, expectedVersion, expectedPromptHash,
  promptPath, receiptDir, apply = false }, fetcher = fetch) {
  if (!apiKey || !/^agent_[a-z0-9]+$/.test(agentId ?? '') || !expectedVersion
    || !/^[a-f0-9]{64}$/.test(expectedPromptHash ?? '') || !receiptDir) {
    throw new Error('Explicit agent, version, prompt hash, credential and receipt directory required');
  }
  const prompt = extractPrompt(await readFile(promptPath, 'utf8'));
  const endpoint = `https://api.elevenlabs.io/v1/convai/agents/${agentId}`;
  async function call(url, method = 'GET', body) {
    const response = await fetcher(url, {
      method, headers: { 'xi-api-key': apiKey, ...(body ? { 'Content-Type': 'application/json' } : {}) },
      body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(45000),
    });
    if (!response.ok) throw new Error(`ElevenLabs ${method} failed with HTTP ${response.status}; inspect readback before retrying`);
    return response.json();
  }
  const before = await call(endpoint);
  assertExpected(before, { agentId, expectedVersion, expectedPromptHash });
  const protectedBefore = protectedHashes(before);
  const receipt = {
    checked_at: new Date().toISOString(), agent_id: agentId, branch_id: before.branch_id,
    before_version: before.version_id, before_prompt_sha256: expectedPromptHash,
    proposed_prompt_sha256: sha256(prompt), protected_before: protectedBefore,
    allowed_changes: ['conversation_config.agent.prompt.prompt'], status: 'prepared_not_applied',
    real_calls_placed: false, metered_sessions_started: false,
  };
  await mkdir(receiptDir, { recursive: true });
  const save = () => writeFile(path.join(receiptDir, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n');
  await writeFile(path.join(receiptDir, 'before-prompt.txt'), before.conversation_config.agent.prompt.prompt);
  await save();
  if (!apply) return receipt;
  if (receipt.proposed_prompt_sha256 === expectedPromptHash) throw new Error('Prompt is unchanged');

  // A second read narrows the race with dashboard edits; never overwrite unrelated settings.
  const fresh = await call(endpoint);
  assertExpected(fresh, { agentId, expectedVersion, expectedPromptHash });
  if (JSON.stringify(protectedHashes(fresh)) !== JSON.stringify(protectedBefore)) throw new Error('Protected configuration changed');
  receipt.status = 'patch_attempted_readback_required';
  await save();
  try {
    await call(`${endpoint}?branch_id=${encodeURIComponent(before.branch_id)}&enable_versioning_if_not_enabled=true`, 'PATCH', {
      conversation_config: { agent: { prompt: { prompt } } },
    });
    const after = await call(endpoint);
    receipt.after_version = after.version_id;
    receipt.after_prompt_sha256 = sha256(after.conversation_config.agent.prompt.prompt);
    receipt.protected_after = protectedHashes(after);
    if (after.agent_id !== agentId || after.branch_id !== before.branch_id
      || receipt.after_prompt_sha256 !== receipt.proposed_prompt_sha256
      || JSON.stringify(receipt.protected_after) !== JSON.stringify(protectedBefore)) {
      throw new Error('Effective main-branch readback differs from the prompt-only change');
    }
    receipt.status = 'live_prompt_verified_audio_behavior_not_tested';
    receipt.verified_at = new Date().toISOString();
    await save();
    return receipt;
  } catch (error) {
    receipt.status = 'needs_readback_review_do_not_repeat_patch';
    await save();
    throw error;
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const value = name => args[args.indexOf(name) + 1];
  const required = ['--agent-id', '--expected-version', '--expected-prompt-sha256', '--receipt-dir'];
  if (required.some(name => !args.includes(name))) {
    throw new Error('Usage: --agent-id ID --expected-version ID --expected-prompt-sha256 HASH --receipt-dir PATH [--apply]');
  }
  syncPromptOnly({ apiKey: process.env.ELEVENLABS_API_KEY, agentId: value('--agent-id'),
    expectedVersion: value('--expected-version'), expectedPromptHash: value('--expected-prompt-sha256'),
    promptPath: path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../docs/elevenlabs-agent-prompt.md'),
    receiptDir: path.resolve(value('--receipt-dir')), apply: args.includes('--apply'),
  }).then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(error => { console.error(error.message); process.exitCode = 1; });
}
