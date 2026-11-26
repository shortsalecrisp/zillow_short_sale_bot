import { digest } from './verifyElevenLabsComparison.mjs';
import { sha256 } from './syncElevenLabsPromptOnly.mjs';
import { SCREENING_RECOVERY_POLICY } from './elevenLabsScreeningRecovery.mjs';
import { NATIVE_RECORDING_POLICY } from './elevenLabsLiveFlowRepair.mjs';

const marker = '\n\n[CRISP_OPENING_HANDOFF_POLICY]';
const predecessorRoot = '8b8d58e09f7c55af188845fb524a8cc387456e4c47c7e1d7e941cc8c3e38cb48';
const predecessorTerminal = 'On attempt two, a recorded screener ending with “not available” and an additional-message invitation goes directly to recorded_no_voicemail_exit. On either attempt, the same completed automated phone menu played twice without an intervening human also goes directly to recorded_no_voicemail_exit. End silently before skip_turn. Preserve a real hold, later live speech and normal mailbox handling.';
export const TERMINAL_RECORDING_POLICY = 'Before skip_turn: a completed recorded mailbox greeting or a terminal unavailable screener inviting an additional message on attempt two goes directly to recorded_no_voicemail_exit, silently. Do not invoke voicemail_detection or wait for another beep. On attempt one, a completed message invitation uses voicemail_detection immediately with its existing message. A completed no-message greeting or repeated completed automated menu uses the existing silent exit. An earlier stay-on-the-line instruction no longer applies after terminal unavailability. Active connecting/hold and incomplete recordings are NOT terminal. Later actual live speech, questions, callbacks, opt-outs, takeover and pending live questions retain priority.';
export const LISTENER_STATE_POLICY = 'Before any speech, check THIS listener\'s actual transcript, not the lead name or a prior attempt. Without delivered Maya/Crisp identity, a first greeting, name, readiness or "Somebody there?" requires the unscreened opening once; after actual screening/hold use the short identity-and-listing opening. A matching name never proves they heard your identity. "I\'m here" plus the pending needs question requires BOTH delivered identity AND this listener\'s explicit answer confirming the exact listing. Otherwise ask only the unanswered listing question after identity. Questions, hearing trouble, callbacks, opt-outs and takeover keep priority; silence proves nothing.';
export const SCREENED_LISTENER_POLICY = 'After actual screening/hold, a NEW live listener who has not heard your identity gets "Hi, {{assistantName}} with Crisp Short Sales. Is {{streetAddress}} your listing?" A matching name does not prove delivered identity. The SAME listener who already heard it gets only the pending listing question while unconfirmed; after explicit listing confirmation repair only the pending question. Questions, hearing trouble, callbacks, opt-outs and takeover retain priority. Incomplete recordings are not live pickups; listen without speaking.';

export const ROOT_REPLACEMENTS = [
  ["Choose the response mode from the caller's entire turn AS RECEIVED. A turn containing a question, correction or hearing problem remains answer-only for your whole response. Answering it does not make qualification eligible later in that same response. After answering, only an explicitly requested contact action may follow; otherwise stop and wait for a new caller turn.",
    "Choose the response mode from the caller's entire turn AS RECEIVED. A turn containing a substantive question, correction or hearing problem remains answer-only for your whole response. Answering it does not make qualification eligible later in that same response. After answering, only an explicitly requested contact action may follow; otherwise stop and wait for a new caller turn. A bare pre-introduction presence check is a pickup: give the opening, never the needs question."],
  ['- If the latest caller message is exactly "..." or contains only noise, a cough, breath, static, or a bump, call skip_turn and wait. Do not ask whether they are still there because of placeholder-only text. An intelligible question is not noise.',
    '- Resolve completed recording exits BEFORE skip_turn. Otherwise, for "...", noise, a cough, breath, static or a bump, use skip_turn. Never invent a live turn or presence question. Intelligible speech is not noise.'],
  ['An unscreened first hello, name or readiness cue requires this entire opening once. Silence or skip_turn never proves identity or screening.',
    'Before delivered Maya/Crisp identity, an unscreened hello, name, readiness or "Somebody there?" requires this opening once. Never infer identity from the lead name, silence or skip_turn. A clipped "Hi, if you record your..." is incomplete screening: listen silently for the rest, not a live pickup or permission to end.'],
  ['Answer first-turn questions directly, without repeating an introduction. Shortening requires actual earlier automated screening/hold, never just a name or hello. Only AFTER that announcement: a matching spoken {{firstName}} gets "Is {{streetAddress}} your listing?"; an unnamed or differently named new listener gets "Hi, {{assistantName}} with Crisp Short Sales. Is {{streetAddress}} your listing?" Do not repeat the full opener after screening or infer identity from numbers, screeners or placeholders. After delivered identity, repeat only the pending listing check while unconfirmed; after confirmation repair only the pending question.',
    'Answer first-turn questions directly. Shortening requires actual earlier screening/hold, not a name or hello. A new listener gets "Hi, {{assistantName}} with Crisp Short Sales. Is {{streetAddress}} your listing?" regardless of name. Only the same listener who heard your identity gets a bare listing check. Needs/presence recovery requires that listener\'s explicit listing confirmation; otherwise repair only the listing question.'],
  ['- Attempt 2: after a completed mailbox greeting or terminal busy/unavailable screener inviting a message, invoke voicemail_detection immediately ONLY when {{voicemailMessage}} is empty. Its empty-message result ends silently. Never speak, improvise a message, skip_turn or wait for a beep afterward. If the tool is unavailable or the payload is not empty, use recorded_no_voicemail_exit instead. Active connecting/hold, uncertain/incomplete recordings and later genuine live questions, callbacks, opt-outs or takeover keep priority.',
    '- Attempt 2: a completed mailbox greeting or terminal unavailable screener inviting a message goes directly to recorded_no_voicemail_exit, silently, before skip_turn. Never use voicemail_detection, speak, leave a message or wait for another beep. Terminal unavailability supersedes earlier hold instructions. Active connecting/hold, incomplete recordings and later live questions, callbacks, opt-outs, takeover or pending questions retain priority.'],
];

function replaceOnce(text, from, to) {
  if (typeof text !== 'string' || text.split(from).length !== 2) throw Error('Exact reviewed predecessor instruction required');
  return text.replace(from, to);
}
export function buildTerminalOpeningPrompt(old) {
  if (sha256(old) !== predecessorRoot) throw Error('Exact reviewed r28 root required');
  const prompt = ROOT_REPLACEMENTS.reduce((text, [from, to]) => replaceOnce(text, from, to), old);
  if (prompt.trim().split(/\s+/).length >= 6300) throw Error('Root must remain strictly below 6300 words');
  return prompt;
}
export function restoreR28Prompt(prompt) {
  return ROOT_REPLACEMENTS.reduce((text, [from, to]) => text.includes(to) ? replaceOnce(text, to, from) : text, prompt);
}

export const TERMINAL_OPENING_PATHS = [
  'conversation_config.agent.prompt.prompt',
  'workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt',
  'workflow.nodes.main_conversation.additional_prompt',
  'workflow.nodes.short_listing_recovery.conversation_config.agent.prompt.prompt',
  'workflow.nodes.short_listing_recovery.edge_order',
  'workflow.edges.opening_listener_to_recorded_no_voicemail_exit.forward_condition.condition',
  'workflow.edges.main_conversation_to_recorded_no_voicemail_exit.forward_condition.condition',
  'workflow.edges.short_listing_recovery_to_recorded_no_voicemail_exit',
];
export function terminalOpeningProtectedHashes(agent) {
  const copy = structuredClone(agent);
  delete copy.conversation_config.agent.prompt.tools;
  for (const path of TERMINAL_OPENING_PATHS) {
    const keys = path.split('.'), key = keys.pop();
    let parent = copy;
    for (const part of keys) parent = parent?.[part];
    if (!parent) throw Error('Reviewed release container missing');
    delete parent[key];
  }
  return { unchanged: digest({ conversation_config: copy.conversation_config, workflow: copy.workflow }),
    ...Object.fromEntries(['platform_settings', 'phone_numbers', 'whatsapp_accounts', 'procedures'].map(key => [key, digest(agent[key] ?? null)])) };
}
export function applyTerminalOpeningRepair(agent, prompt) {
  const result = structuredClone(agent), old = agent.conversation_config.agent.prompt.prompt;
  if (buildTerminalOpeningPrompt(old) !== prompt) throw Error('Unreviewed root replacement');
  const opening = result.workflow.nodes.opening_listener.conversation_config.agent.prompt;
  const parts = opening.prompt.split(marker);
  if (parts.length !== 2 || parts[0] !== old || result.workflow.nodes.recorded_no_voicemail_exit?.type !== 'end') {
    throw Error('Exact embedded root and existing silent end required');
  }
  let suffix = replaceOnce(parts[1], predecessorTerminal, TERMINAL_RECORDING_POLICY);
  suffix = replaceOnce(suffix, SCREENING_RECOVERY_POLICY, SCREENED_LISTENER_POLICY);
  suffix = replaceOnce(suffix, NATIVE_RECORDING_POLICY, TERMINAL_RECORDING_POLICY);
  result.conversation_config.agent.prompt.prompt = prompt;
  opening.prompt = prompt + marker + '\n\n' + LISTENER_STATE_POLICY + '\n\n' + suffix;
  const main = result.workflow.nodes.main_conversation;
  main.additional_prompt = LISTENER_STATE_POLICY + '\n\n' + replaceOnce(
    replaceOnce(main.additional_prompt, ROOT_REPLACEMENTS[2][0], ROOT_REPLACEMENTS[2][1]), predecessorTerminal, TERMINAL_RECORDING_POLICY);
  const recovery = result.workflow.nodes.short_listing_recovery;
  recovery.conversation_config.agent.prompt.prompt = LISTENER_STATE_POLICY + '\n\n' + TERMINAL_RECORDING_POLICY + '\n\n' + replaceOnce(
    replaceOnce(recovery.conversation_config.agent.prompt.prompt, SCREENING_RECOVERY_POLICY, SCREENED_LISTENER_POLICY),
    'If this is the same listener who already heard your identity and is only repeating hello or checking your presence, say exactly:',
    'ONLY if this SAME listener already heard your Maya/Crisp identity in this call and is checking presence while listing ownership is unconfirmed, say exactly:');
  for (const source of ['opening_listener', 'main_conversation']) {
    const id = source + '_to_recorded_no_voicemail_exit', node = result.workflow.nodes[source], edge = result.workflow.edges[id];
    if (node.edge_order[0] !== 'terminal_contact_from_' + source || node.edge_order[1] !== 'terminal_guard_from_' + source ||
      node.edge_order[2] !== id || edge?.source !== source || edge.target !== 'recorded_no_voicemail_exit' ||
      !edge.forward_condition.condition.startsWith(predecessorTerminal)) throw Error('Existing live guards and recording exit must remain first');
    edge.forward_condition.condition = TERMINAL_RECORDING_POLICY;
  }
  const exitId = 'short_listing_recovery_to_recorded_no_voicemail_exit';
  if (Object.hasOwn(result.workflow.edges, exitId) || recovery.edge_order[0] !== 'terminal_contact_from_short_listing_recovery' ||
    recovery.edge_order[1] !== 'terminal_guard_from_short_listing_recovery') throw Error('Recovery guards drifted');
  recovery.edge_order.splice(2, 0, exitId);
  result.workflow.edges[exitId] = { ...structuredClone(result.workflow.edges.opening_listener_to_recorded_no_voicemail_exit), source: 'short_listing_recovery' };
  if (digest(terminalOpeningProtectedHashes(result)) !== digest(terminalOpeningProtectedHashes(agent))) throw Error('Protected release fields changed');
  return result;
}
