import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const R26_COMPARISON = {
  cohort: 'maya_uniform_lead_conversion_20261124_r26',
  startedAt: '2026-11-24T12:32:44.718Z',
  agentId: 'agent_9901kpkhs1cmfgj8w9vm199nc92j',
  sharedHash: 'd432abcf04075ad1175b4de50bcde27877622ca5131173e8212c804c1a5c2982',
  workflowHash: '86a1d97d080dea9d2ec47afe59c2146c2768aa6f7518a70792ad82e7f71aaf42',
  waitingAudio: { sound: 'elevator1', availabilityToolId: 'tool_4101m64g0xqteevsgfwtpgxv4he6',
    availabilityToolHash: '6eb7797a911ea9dc3b2feb834932fa63f38f7121858f22476eb54a245f7fae63' },
  arms: [
    { name: 'Flash', branchId: 'agtbrch_1101kpkhs3b4eg5v0q8ppq4hayks', versionId: 'agtvrsn_6001m86rnxehf518mvew9ag9cpzd', ttsModel: 'eleven_flash_v2' },
    { name: 'v4', branchId: 'agtbrch_8401m3n23qvbewvtr9kfzx7nw4gv', versionId: 'agtvrsn_8901m86rp00wff4a7rd97dfr2ca7', ttsModel: 'eleven_v4_turbo' },
  ],
};

// Retain the verifier's existing export name for current-release callers.
export const R8_COMPARISON = {
  ...R26_COMPARISON,
  cohort: 'maya_uniform_lead_conversion_20261125_r28',
  startedAt: '2026-11-25T13:27:50.274Z',
  sharedHash: '589bcded16b57433a442cd0ab5caaeda0e7786c00f7288cbe7b774295d9d301c',
  workflowHash: '7267d6f9a5cb7321486d349f81ee5040dbb409d59baa5832076d1b2ce0b12a74',
  arms: [
    { ...R26_COMPARISON.arms[0], versionId: 'agtvrsn_5901m89e84k7f28rmq7bqzwshx2q' },
    { ...R26_COMPARISON.arms[1], versionId: 'agtvrsn_1301m89e87r0expr4tenxnxjqngj' },
  ],
};

export const canonical = value => Array.isArray(value) ? value.map(canonical)
  : value && typeof value === 'object'
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])])) : value;
export const digest = value => createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
export const DISABLED_NOISY_ENVIRONMENT_MESSAGE = "Uhh ... sorry, I'm picking up some noise in the background. Could you move somewhere quieter?";
const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const approvedNullWidgetLabels = [
  'resume_call', 'end_chat', 'expand_widget', 'collapse_widget', 'dismiss', 'open_chat',
  'end_chat_confirmation_title', 'end_chat_confirmation_description',
  'end_call_confirmation_title', 'end_call_confirmation_description',
  'end_call_confirm', 'cancel_end_confirmation', 'reconnecting_status',
  'voice_mode_label', 'text_mode_label', 'thinking_status', 'carousel_previous',
  'carousel_next', 'new_message', 'new_messages',
];

export function comparisonAvailabilityTool(config) {
  const result = structuredClone(config);
  const schema = isObject(result) && Object.hasOwn(result, 'api_schema') ? result.api_schema : undefined;
  if (isObject(schema) && Object.hasOwn(schema, 'mtls_on_token_request') && schema.mtls_on_token_request === false) {
    delete schema.mtls_on_token_request;
  }
  return result;
}

// Historical r22 only: later cohorts explicitly pin these fields and must retain them.
export function comparisonBodyR22(agent) {
  const body = comparisonBody(agent), config = body.conversation_config;
  const turn = isObject(config) && Object.hasOwn(config, 'turn') ? config.turn : undefined;
  const timeout = isObject(turn) && Object.hasOwn(turn, 'soft_timeout_config') ? turn.soft_timeout_config : undefined;
  if (isObject(timeout) && Object.hasOwn(timeout, 'pause_after_filler') && timeout.pause_after_filler === false) {
    delete timeout.pause_after_filler;
  }
  const tts = isObject(config) && Object.hasOwn(config, 'tts') ? config.tts : undefined;
  if (isObject(tts) && Object.hasOwn(tts, 'gain_db') && tts.gain_db === 0) delete tts.gain_db;
  return body;
}

export function comparisonPlatformSettings(settings) {
  const result = structuredClone(settings);
  const widget = isObject(result) && Object.hasOwn(result, 'widget') ? result.widget : undefined;
  if (isObject(widget) && Object.hasOwn(widget, 'layout') && widget.layout === 'default') delete widget.layout;
  const labels = isObject(widget) && Object.hasOwn(widget, 'text_contents') ? widget.text_contents : undefined;
  if (isObject(labels)) {
    for (const key of approvedNullWidgetLabels) {
      if (Object.hasOwn(labels, key) && labels[key] === null) delete labels[key];
    }
  }
  const reports = isObject(result) && Object.hasOwn(result, 'bi_reports') ? result.bi_reports : undefined;
  if (isObject(reports) && Object.keys(reports).length === 1 && Object.hasOwn(reports, 'enabled')
      && reports.enabled === null) delete result.bi_reports;
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
  const analysis = isObject(result) && Object.hasOwn(result, 'analysis_settings') ? result.analysis_settings : undefined;
  if (isObject(analysis) && Object.keys(analysis).length === 3
      && Object.hasOwn(analysis, 'enabled') && analysis.enabled === true
      && Object.hasOwn(analysis, 'strict') && analysis.strict === false
      && Object.hasOwn(analysis, 'ignore') && isObject(analysis.ignore) && Object.keys(analysis.ignore).length === 0) {
    delete result.analysis_settings;
  }
  // Reconcile only the provider's numeric GET-only workflow schema marker.
  if (isObject(result) && Object.hasOwn(result, 'version') && result.version === 1) {
    delete result.version;
  }
  const edges = isObject(result) ? result.edges : undefined;
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
  const configHash = digest(comparisonAvailabilityTool(c));
  assert.equal(configHash, expected.availabilityToolHash, 'Availability audio config drifted');
  assert.equal(c.name, 'live_transfer_requested'); assert.equal(c.tool_call_sound, expected.sound);
  assert.equal(c.tool_call_sound_behavior, 'always'); assert.equal(c.interruption_mode, 'allow');
  assert.equal(c.disable_interruptions, false); assert.equal(c.response_timeout_secs, 60);
  return { ok: true, readOnly: true, toolId: t.id, configHash, sound: c.tool_call_sound,
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
