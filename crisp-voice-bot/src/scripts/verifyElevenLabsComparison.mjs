import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const R8_COMPARISON = {
  cohort: 'maya_uniform_lead_conversion_20261105_r17',
  startedAt: '2026-11-06T01:57:48.117Z',
  agentId: 'agent_9901kpkhs1cmfgj8w9vm199nc92j',
  sharedHash: 'f424ed3d2e25e241e98ae76b8150b3f0b8cd0e424efba937f903f27305b93f35',
  workflowHash: '505039bfc7d39c957e1a8bc99a0415b0b7c527ff4c7f3e93fbf23323acb8637d',
  waitingAudio: { sound: 'elevator1', availabilityToolId: 'tool_4101m64g0xqteevsgfwtpgxv4he6',
    availabilityToolHash: '6eb7797a911ea9dc3b2feb834932fa63f38f7121858f22476eb54a245f7fae63' },
  arms: [
    { name: 'Flash', branchId: 'agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks', versionId: 'agtvrsn_7601m6q96zm5e1astf8q4314k3wt', ttsModel: 'eleven_flash_v2' },
    { name: 'v4', branchId: 'agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv', versionId: 'agtvrsn_1701m6q972srexdrxgcf1meegsmr', ttsModel: 'eleven_v4_turbo' },
  ],
};

export const canonical = value => Array.isArray(value) ? value.map(canonical)
  : value && typeof value === 'object'
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
export const digest = value => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
export const DISABLED_NOISY_ENVIRONMENT_MESSAGE = "Uhh ... sorry, I'm picking up some noise in the background. Could you move somewhere quieter?";
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);

export function comparisonPlatformSettings(settings) {
  const result = structuredClone(settings);
  const path = ['overrides', 'conversation_config_override', 'agent', 'prompt'];
  let prompt = result;
  for (const key of path) {
    if (!isObject(prompt) || !Object.hasOwn(prompt, key)) return result;
    prompt = prompt[key];
  }
  // Disabled override permission only; this is not the agent's actual LLM reasoning setting.
  if (isObject(prompt) && Object.hasOwn(prompt, 'reasoning_effort') && prompt.reasoning_effort === false) {
    delete prompt.reasoning_effort;
  }
  return result;
}

export function comparisonWorkflow(workflow) {
  const result = structuredClone(workflow);
  const edges = result && typeof result === 'object' && !Array.isArray(result) ? result.edges : undefined;
  // Reconcile only the provider's empty GET-only edge rerouting default.
  if (edges && typeof edges === 'object' && !Array.isArray(edges)) {
    for (const edge of Object.values(edges)) {
      if (edge && typeof edge === 'object' && !Array.isArray(edge)
          && Object.hasOwn(edge, 'reroutes') && Array.isArray(edge.reroutes) && edge.reroutes.length === 0) {
        delete edge.reroutes;
      }
    }
  }
  return result;
}

export function comparisonBody(agent) {
  const body = structuredClone({ conversation_config: agent.conversation_config, workflow: agent.workflow });
  body.workflow = comparisonWorkflow(body.workflow);
  if (isObject(body.conversation_config) && Object.hasOwn(body.conversation_config, 'avatar')
      && body.conversation_config.avatar === null) {
    delete body.conversation_config.avatar;
  }
  const asr = body.conversation_config.asr;
  if (asr && typeof asr === 'object' && !Array.isArray(asr)
      && Object.hasOwn(asr, 'keyterm_libraries') && Array.isArray(asr.keyterm_libraries)
      && asr.keyterm_libraries.length === 0) {
    delete asr.keyterm_libraries;
  }
  const { agent: config, tts } = body.conversation_config;
  delete tts.model_id;
  delete config.prompt.tools;
  const builtInTools = config.prompt.built_in_tools;
  // The approved r8 baseline includes the provider's disabled identity-tool default.
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

export function validateComparison(agents, allocation, expected = R8_COMPARISON) {
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
    assert.equal(digest(body.workflow), expected.workflowHash, `${arm.name}: workflow digest mismatch`);
    return { name: arm.name, branchId: agent.branch_id, versionId: agent.version_id,
      ttsModel: arm.ttsModel, trafficPercent: 50, sharedHash: digest(body), workflowHash: digest(body.workflow) };
  });
  assert.deepEqual(canonical(bodies[0]), canonical(bodies[1]), 'Normalized bodies must be directly identical');
  for (const key of ['platform_settings', 'phone_numbers', 'whatsapp_accounts', 'procedures']) {
    const normalized = agents.map(agent => key === 'platform_settings'
      ? comparisonPlatformSettings(agent[key] ?? null) : agent[key] ?? null);
    assert.equal(digest(normalized[0]), digest(normalized[1]), `${key}: protected arm mismatch`);
  }
  return { ok: true, readOnly: true, cohort: expected.cohort, startedAt: expected.startedAt,
    sharedHash: expected.sharedHash, workflowHash: expected.workflowHash, arms };
}

export async function verifyLiveComparison({ apiKey, expected = R8_COMPARISON }, fetcher = fetch) {
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

// Separate from the canonical three-GET arm verifier: shared webhook settings are independent resources.
export async function verifyLiveTransferAudio({ apiKey, expected = R8_COMPARISON.waitingAudio }, fetcher = fetch) {
  assert.ok(apiKey && /^tool_[a-z0-9]+$/.test(expected?.availabilityToolId ?? ''), 'Exact availability audio tool required');
  const r = await fetcher('https://api.elevenlabs.io/v1/convai/tools/' + expected.availabilityToolId,
    { method: 'GET', headers: { 'xi-api-key': apiKey }, signal: AbortSignal.timeout(45000) });
  assert.ok(r.ok, `Availability audio read failed HTTP ${r.status}`);
  const t = await r.json(), c = t.tool_config;
  assert.equal(t.id, expected.availabilityToolId, 'Availability audio identity mismatch');
  assert.equal(digest(c), expected.availabilityToolHash, 'Availability audio config drifted');
  assert.equal(c.name, 'live_transfer_requested'); assert.equal(c.tool_call_sound, expected.sound);
  assert.equal(c.tool_call_sound_behavior, 'always'); assert.equal(c.interruption_mode, 'allow');
  assert.equal(c.disable_interruptions, false); assert.equal(c.response_timeout_secs, 60);
  return { ok: true, readOnly: true, toolId: t.id, configHash: digest(c), sound: c.tool_call_sound,
    behavior: 'always', interruptible: true, responseTimeoutSeconds: 60, audioPlaybackVerified: false };
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
