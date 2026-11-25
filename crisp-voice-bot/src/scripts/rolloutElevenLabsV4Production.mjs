import assert from 'node:assert/strict';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { R8_COMPARISON, comparisonBody, comparisonPlatformSettings, digest,
  validateComparison, validateV4Production, verifyLiveTransferAudio } from './verifyElevenLabsComparison.mjs';

export async function rolloutV4Production({ apiKey, receiptDir, apply = false, expectedPlanHash,
  expected = R8_COMPARISON }, fetcher = fetch) {
  assert.ok(apiKey && receiptDir, 'API key and receipt directory required');
  const root = path.resolve(receiptDir);
  await mkdir(root, { recursive: true, mode: 0o700 });
  const save = (name, value) => writeFile(path.join(root, name), JSON.stringify(value, null, 2), { mode: 0o600 });
  const endpoint = `https://api.elevenlabs.io/v1/convai/agents/${expected.agentId}`;
  const request = async (url, method = 'GET', body) => {
    const response = await fetcher(url, { method,
      headers: { 'xi-api-key': apiKey, ...(body ? { 'Content-Type': 'application/json' } : {}) },
      ...(body ? { body: JSON.stringify(body) } : {}), signal: AbortSignal.timeout(45000) });
    assert.ok(response.ok, `Provider ${method} failed HTTP ${response.status}`);
    return response.json();
  };
  const read = async () => {
    const agents = [];
    for (const arm of expected.arms) agents.push(await request(`${endpoint}?branch_id=${arm.branchId}`));
    return { agents, allocation: await request(`${endpoint}/branches`) };
  };
  const protectedState = agents => agents.map(agent => ({
    body: digest(comparisonBody(agent)),
    platform: digest(comparisonPlatformSettings(agent.platform_settings ?? null)),
    phone: digest(agent.phone_numbers ?? null), whatsapp: digest(agent.whatsapp_accounts ?? null),
    procedures: digest(agent.procedures ?? null),
  }));
  const before = await read();
  const alreadyLive = before.allocation.results?.some(branch => branch.id === expected.arms[1].branchId
    && branch.current_live_percentage === 100);
  if (alreadyLive) {
    const verification = validateV4Production(before.agents, before.allocation, expected);
    const audio = await verifyLiveTransferAudio({ apiKey, expected: expected.waitingAudio }, fetcher);
    return { status: 'already_v4_production_verified', applied: false, verification, audio };
  }
  validateComparison(before.agents, before.allocation, expected);
  const audioBefore = await verifyLiveTransferAudio({ apiKey, expected: expected.waitingAudio }, fetcher);
  const deploymentRequest = { deployment_request: { requests: expected.arms.map((arm, index) => ({
    branch_id: arm.branchId, deployment_strategy: { type: 'percentage', traffic_percentage: index === 1 ? 100 : 0 },
  })) } };
  const plan = { agentId: expected.agentId, cohort: expected.cohort,
    versions: expected.arms.map(arm => arm.versionId), before: [50, 50], after: [0, 100],
    protectedState: protectedState(before.agents), audioHash: audioBefore.configHash, deploymentRequest };
  const planHash = digest(plan);
  if (!apply) {
    await save('plan.json', { preparedAt: new Date().toISOString(), planHash, plan });
    return { status: 'prepared_not_applied', planHash, plan };
  }
  const reviewed = JSON.parse(await readFile(path.join(root, 'plan.json'), 'utf8'));
  assert.equal(expectedPlanHash, planHash, 'Reviewed plan changed; no write authorized');
  assert.equal(reviewed.planHash, planHash, 'Saved reviewed plan changed');
  assert.deepEqual(reviewed.plan, plan, 'Saved reviewed plan content changed');
  const writeStartedAt = new Date().toISOString();
  await save('receipt.json', { status: 'allocation_write_started', writeStartedAt, planHash });
  let writeError = null;
  try { await request(`${endpoint}/deployments`, 'POST', deploymentRequest); }
  catch { writeError = 'Allocation response uncertain; not retried'; }
  // Reconcile one ambiguous write by reading; never repeat the deployment POST.
  try {
    const after = await read();
    const verification = validateV4Production(after.agents, after.allocation, expected);
    assert.deepEqual(protectedState(after.agents), plan.protectedState, 'Protected configuration changed');
    const audio = await verifyLiveTransferAudio({ apiKey, expected: expected.waitingAudio }, fetcher);
    const receipt = { status: 'live_v4_production_verified', applied: true, writeStartedAt,
      verifiedAt: new Date().toISOString(), planHash, writeError, verification, audio,
      protectedConfigurationUnchanged: true, providerSettingsPatched: false, callsPlaced: 0 };
    await save('receipt.json', receipt);
    return receipt;
  } catch (error) {
    await save('receipt.json', { status: 'allocation_unverified_hold', writeStartedAt, planHash,
      writeError, error: error.message, automaticRetry: false });
    throw error;
  }
}

if (path.resolve(process.argv[1] ?? '') === fileURLToPath(import.meta.url)) {
  const args = process.argv.slice(2);
  const value = key => args.find(arg => arg.startsWith(`--${key}=`))?.slice(key.length + 3);
  if (args.some(arg => arg !== '--apply' && !/^--(receipt-dir|expected-plan-hash)=.+$/.test(arg))) {
    throw new Error('Unsupported rollout argument');
  }
  rolloutV4Production({ apiKey: process.env.ELEVENLABS_API_KEY, receiptDir: value('receipt-dir'),
    expectedPlanHash: value('expected-plan-hash'), apply: args.includes('--apply') })
    .then(result => console.log(JSON.stringify(result)))
    .catch(error => { console.error(error.message); process.exitCode = 1; });
}
