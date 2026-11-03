import { digest } from './verifyElevenLabsComparison.mjs';

const marker = '\n\n[CRISP_OPENING_HANDOFF_POLICY]';

function openingPolicy(agent) {
  const node = agent.workflow?.nodes?.opening_listener;
  const prompt = node?.conversation_config?.agent?.prompt?.prompt;
  if (node?.type !== 'override_agent' || typeof prompt !== 'string'
      || prompt.split(marker).length !== 2 || !prompt.startsWith('# Role and style\n')) {
    throw new Error('Exact embedded opening business-policy boundary required');
  }
  return { prompt, suffix: prompt.slice(prompt.indexOf(marker)) };
}

export function sharedBusinessPromptProtectedHashes(agent) {
  const next = structuredClone(agent), { suffix } = openingPolicy(next);
  delete next.conversation_config.agent.prompt.prompt;
  delete next.conversation_config.agent.prompt.tools;
  next.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt = suffix;
  return { unchanged: digest({ conversation_config: next.conversation_config, workflow: next.workflow }),
    ...Object.fromEntries(['platform_settings', 'phone_numbers', 'whatsapp_accounts', 'procedures'].map(key => [key, digest(agent[key] ?? null)])) };
}

export function applySharedBusinessPrompt(agent, prompt) {
  if (typeof prompt !== 'string' || !prompt.startsWith('# Role and style\n') || prompt.includes(marker)) {
    throw new Error('One reviewed shared business prompt required');
  }
  const next = structuredClone(agent), { suffix } = openingPolicy(agent);
  next.conversation_config.agent.prompt.prompt = prompt;
  next.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt = prompt + suffix;
  if (digest(sharedBusinessPromptProtectedHashes(next)) !== digest(sharedBusinessPromptProtectedHashes(agent))) {
    throw new Error('Shared business prompt changed protected configuration');
  }
  return next;
}
