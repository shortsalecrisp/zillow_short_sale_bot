import axios from 'axios';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { syncComparisonPrompts } from './syncElevenLabsComparisonPrompts.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const loaded = await import('../../dist/scripts/prepareElevenLabsContactTools.js');
const contact = loaded.default ?? loaded;
const configLoaded = await import('../../dist/lib/config.js');
const config = (configLoaded.default ?? configLoaded).config;

const args = process.argv.slice(2);
const at = name => args[args.indexOf(name) + 1];
const required = ['--agent-id', '--main-branch', '--main-version', '--main-prompt-hash', '--test-branch', '--test-version', '--test-prompt-hash', '--receipt-dir'];
if (required.some(name => !args.includes(name))) throw new Error('Exact reviewed comparison identities and receipt directory required');
const apply = args.includes('--apply');
if (apply && !args.includes('--expected-contact-plan-hash')) throw new Error('Apply requires the reviewed contact-tool plan SHA256');

const agentId = at('--agent-id'), mainBranch = at('--main-branch'), mainVersion = at('--main-version');
if (config.elevenLabs.agentId !== agentId || config.elevenLabs.branchId !== mainBranch || !config.elevenLabs.apiKey) {
  throw new Error('Configured provider identity must match the reviewed production main branch');
}
const receiptDir = path.resolve(at('--receipt-dir'));
const toolIds = {
  callback_requested: at('--callback-tool-id'),
  not_interested: at('--not-interested-tool-id'),
  information_requested: at('--information-tool-id'),
};
if (Object.values(toolIds).some(value => !/^tool_[a-z0-9]+$/.test(value ?? ''))) throw new Error('Exact three contact-tool IDs required');
const identity = { agent_id: agentId, branch_id: mainBranch, main_branch_id: mainBranch, version_id: mainVersion };
const client = axios.create({ baseURL: config.elevenLabs.baseUrl, timeout: 45_000,
  headers: { 'Content-Type': 'application/json', 'xi-api-key': config.elevenLabs.apiKey } });
const toolDir = path.join(receiptDir, 'tools');

const dry = await contact.prepareContactTools({ identity, toolIds, receiptDir: toolDir }, client);
if (!apply) {
  console.log(JSON.stringify({ ...dry, agent_id: agentId, branch_id: mainBranch, version_id: mainVersion }));
  process.exit(0);
}
const created = await contact.prepareContactTools({ identity, toolIds, receiptDir: toolDir, create: true,
  expectedPlanHash: at('--expected-contact-plan-hash') }, client);
const map = JSON.parse(await readFile(created.map_path, 'utf8'));
console.log(JSON.stringify({ event: 'contact_tool_candidates_verified_not_attached', map_sha256: map.map_sha256,
  replacements: map.replacements }));

const verifyContactTools = async () => {
  const { data: current } = await client.get(`/v1/convai/agents/${agentId}`, { params: { branch_id: mainBranch } });
  await contact.verifyContactToolMap(client, map, current, map.map_sha256);
};
const options = {
  apiKey: config.elevenLabs.apiKey, agentId,
  branches: [
    { id: mainBranch, version: mainVersion, promptHash: at('--main-prompt-hash') },
    { id: at('--test-branch'), version: at('--test-version'), promptHash: at('--test-prompt-hash') },
  ],
  promptPath: path.resolve(here, '../../docs/elevenlabs-agent-prompt.md'),
  receiptDir: path.join(receiptDir, 'comparison'), contactToolReplacements: created.replacements,
  contactToolMapHash: created.map_sha256, verifyContactTools,
};
const plan = await syncComparisonPrompts(options);
console.log(JSON.stringify({ event: 'comparison_release_prepared', plan }));
const receipt = await syncComparisonPrompts({ ...options, apply: true });
console.log(JSON.stringify({ event: 'comparison_release_verified', receipt }));
