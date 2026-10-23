import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const R7_COMPARISON = {
  cohort: 'maya_uniform_lead_conversion_20261023_r7',
  startedAt: '2026-10-23T12:27:33.399Z',
  agentId: 'agent_9901kpkhs1cmfgj8w9vm199nc92j',
  sharedHash: '18d8a334dbb09f6f6c196d84dac6331c0a4a45882aea4f89c591d820197f90a8',
  workflowHash: '31e9628299acf53fda9153f686a003bb1a25cf1a7c00a4bd9e89bcf9d2af0a35',
  arms: [
    { name: 'Flash', branchId: 'agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks', versionId: 'agtvrsn_5901m5mbp1pafv1skkvbhzh4cqfn', ttsModel: 'eleven_flash_v2' },
    { name: 'v4', branchId: 'agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv', versionId: 'agtvrsn_4301m5mbp4jgfn5ayjenb7yjjtby', ttsModel: 'eleven_v4_turbo' },
  ],
};

export const canonical = value => Array.isArray(value) ? value.map(canonical)
  : value && typeof value === 'object'
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
export const digest = value => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
export const DISABLED_NOISY_ENVIRONMENT_MESSAGE = "Uhh ... sorry, I'm picking up some noise in the background. Could you move somewhere quieter?";

export function comparisonBody(agent) {
  const body = structuredClone({ conversation_config: agent.conversation_config, workflow: agent.workflow });
  const { agent: config, tts } = body.conversation_config;
  delete tts.model_id;
  delete config.prompt.tools;
  const builtInTools = config.prompt.built_in_tools;
  // The approved r7 baseline includes the provider's disabled identity-tool default.
  if (builtInTools && typeof builtInTools === 'object' && !Array.isArray(builtInTools)
      && !Object.hasOwn(builtInTools, 'identity_challenge')) {
    builtInTools.identity_challenge = null;
  }
  const sub = config.subagent_config;
  if (sub && typeof sub === 'object' && !Array.isArray(sub) && Object.keys(sub).length === 2
      && Object.hasOwn(sub, 'agents') && Array.isArray(sub.agents) && sub.agents.length === 0
      && Object.hasOwn(sub, 'self_subagent') && sub.self_subagent === null) {
    delete config.subagent_config;
  }
  // Reconstruct only the exact neutral r5 GET-response shapes approved by Yoni.
  if (Object.hasOwn(tts, 'clone_strength') && tts.clone_strength === 0 && !Object.hasOwn(tts, 'pvc_strength')) {
    delete tts.clone_strength;
    tts.pvc_strength = 0;
  }
  if (!Object.hasOwn(config, 'subagents')) config.subagents = [];
  const turn = body.conversation_config.turn;
  // Reconcile only the exact disabled pair added to the pinned r6 GET responses.
  if (turn && Object.hasOwn(turn, 'noisy_environment_detection') && turn.noisy_environment_detection === false
      && Object.hasOwn(turn, 'noisy_environment_message') && turn.noisy_environment_message === DISABLED_NOISY_ENVIRONMENT_MESSAGE) {
    delete turn.noisy_environment_detection;
    delete turn.noisy_environment_message;
  }
  return body;
}

export function validateComparison(agents, allocation, expected = R7_COMPARISON) {
  assert.equal(agents.length, 2, 'Exactly two provider readbacks required');
  const results = allocation?.results;
  assert.ok(Array.isArray(results) && allocation.meta?.total === results.length,
    'Complete provider branch allocation required');
  const active = results.filter(branch => !branch.is_archived && branch.current_live_percentage > 0);
  assert.equal(active.length, 2, 'Exactly two live comparison branches required');
  const bodies = [];
  const arms = agents.map((agent, index) => {
    const arm = expected.arms[index];
    assert.equal(agent.agent_id, expected.agentId, `${arm.name}: agent identity mismatch`);
    assert.equal(agent.branch_id, arm.branchId, `${arm.name}: branch mismatch`);
    assert.equal(agent.version_id, arm.versionId, `${arm.name}: unknown version requires fresh review`);
    assert.equal(agent.conversation_config?.tts?.model_id, arm.ttsModel, `${arm.name}: TTS model mismatch`);
    assert.ok(active.some(branch => branch.id === arm.branchId && branch.current_live_percentage === 50),
      `${arm.name}: exact 50/50 live traffic required`);
    const body = comparisonBody(agent);
    bodies.push(body);
    assert.equal(digest(body), expected.sharedHash, `${arm.name}: shared configuration digest mismatch`);
    assert.equal(digest(agent.workflow), expected.workflowHash, `${arm.name}: workflow digest mismatch`);
    return { name: arm.name, branchId: agent.branch_id, versionId: agent.version_id,
      ttsModel: arm.ttsModel, trafficPercent: 50, sharedHash: digest(body), workflowHash: digest(agent.workflow) };
  });
  assert.deepEqual(canonical(bodies[0]), canonical(bodies[1]), 'Normalized bodies must be directly identical');
  for (const key of ['platform_settings', 'phone_numbers', 'whatsapp_accounts', 'procedures']) {
    assert.equal(digest(agents[0][key] ?? null), digest(agents[1][key] ?? null), `${key}: protected arm mismatch`);
  }
  return { ok: true, readOnly: true, cohort: expected.cohort, startedAt: expected.startedAt,
    sharedHash: expected.sharedHash, workflowHash: expected.workflowHash, arms };
}

export async function verifyLiveComparison({ apiKey, expected = R7_COMPARISON }, fetcher = fetch) {
  assert.ok(apiKey, 'ELEVENLABS_API_KEY is required');
  const endpoint = `https://api.elevenlabs.io/v1/convai/agents/${expected.agentId}`;
  const get = async url => {
    const response = await fetcher(url, { method: 'GET', headers: { 'xi-api-key': apiKey }, signal: AbortSignal.timeout(45000) });
    if (!response.ok) throw new Error(`Read-only provider verification failed HTTP ${response.status}`);
    return response.json();
  };
  const agents = [];
  for (const arm of expected.arms) agents.push(await get(`${endpoint}?branch_id=${encodeURIComponent(arm.branchId)}`));
  return { ...validateComparison(agents, await get(`${endpoint}/branches`), expected), verifiedAt: new Date().toISOString() };
}

if (path.resolve(process.argv[1] ?? '') === fileURLToPath(import.meta.url)) {
  if (process.argv.slice(2).length) {
    process.stderr.write('This read-only verifier accepts no mutation or cohort-override arguments.\n');
    process.exitCode = 1;
  } else {
    verifyLiveComparison({ apiKey: process.env.ELEVENLABS_API_KEY })
      .then(result => process.stdout.write(`${JSON.stringify(result)}\n`))
      .catch(error => { process.stderr.write(`${error.message}\n`); process.exitCode = 1; });
  }
}
