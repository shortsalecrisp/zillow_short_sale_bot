import { digest } from './verifyElevenLabsComparison.mjs';
import { applySharedBusinessPrompt, sharedBusinessPromptProtectedHashes } from './elevenLabsSharedBusinessPrompt.mjs';

export const NO_VOICEMAIL_EXIT = 'recorded_no_voicemail_exit';
export const NO_VOICEMAIL_CONTEXTS = ['opening_listener', 'main_conversation'];
export const NO_VOICEMAIL_CONDITION = 'Only after the latest completed turn is clearly a PRERECORDED voicemail greeting explicitly saying do not leave a voicemail/message, or text instead OF voicemail. Let that greeting finish, then exit silently before voicemail_detection or any spoken message. A faster-text suggestion alongside permission to leave a message is NOT a prohibition. Never route on a live admin/person, a quote, a question, uncertainty about recording origin, incomplete greeting, screening/hold still trying to connect someone, or any later genuine live interruption, callback/email request, opt-out or takeover. Those use their existing live consent and guarded-ending paths. This recorded instruction creates no live opt-out, callback, information request, SMS permission or completed handoff.';
const edgeId = context => context + '_to_recorded_no_voicemail_exit';

export function noVoicemailProtectedHashes(agent) {
  const copy = structuredClone(agent);
  for (const context of NO_VOICEMAIL_CONTEXTS) {
    delete copy.workflow.edges[edgeId(context)];
    copy.workflow.nodes[context].edge_order = copy.workflow.nodes[context].edge_order.filter(id => id !== edgeId(context));
  }
  delete copy.workflow.nodes[NO_VOICEMAIL_EXIT];
  return sharedBusinessPromptProtectedHashes(copy);
}

export function applyNoVoicemailWorkflow(agent, prompt) {
  const original = agent.conversation_config?.agent?.prompt?.prompt;
  const heading = '# Voicemail and recorded exits\n';
  const before = typeof original === 'string' ? original.indexOf(heading) : -1;
  const after = typeof prompt === 'string' ? prompt.indexOf(heading) : -1;
  const message = '\n"Hi, this is {{assistantName}} with Crisp Short Sales calling about the short sale listing';
  const originalMessage = typeof original === 'string' ? original.lastIndexOf(message) : -1;
  const updatedMessage = typeof prompt === 'string' ? prompt.lastIndexOf(message) : -1;
  if (before < 0 || after < 0 || original.slice(0, before) !== prompt.slice(0, after)
      || originalMessage < 0 || updatedMessage < 0 || original.slice(originalMessage) !== prompt.slice(updatedMessage)
      || !prompt.includes('uses the silent no-message exit, not voicemail_detection')
      || prompt.trim().split(/\s+/).length >= 6300) throw new Error('Only the reviewed voicemail policy section may change');
  const next = applySharedBusinessPrompt(agent, prompt), nodes = next.workflow?.nodes, edges = next.workflow?.edges;
  const existingExit = nodes?.unrelated_recording_exit, sourceEdge = edges?.opening_to_unrelated_recording_exit;
  if (existingExit?.type !== 'end' || sourceEdge?.target !== 'unrelated_recording_exit'
      || sourceEdge.forward_condition?.type !== 'llm') throw new Error('Existing silent recording route required');
  const node = { ...structuredClone(existingExit), position: { x: -700, y: 140 } };
  if (nodes[NO_VOICEMAIL_EXIT] && digest(nodes[NO_VOICEMAIL_EXIT]) !== digest(node)) throw new Error('No-voicemail exit collision');
  nodes[NO_VOICEMAIL_EXIT] = node;
  for (const context of NO_VOICEMAIL_CONTEXTS) {
    const owner = nodes[context], id = edgeId(context);
    const guards = ['terminal_contact_from_' + context, 'terminal_guard_from_' + context];
    if (owner?.type !== 'override_agent' || !Array.isArray(owner.edge_order)
        || guards.some(g => !edges[g] || !owner.edge_order.includes(g))) throw new Error('Existing live contact/ending guards required');
    const edge = { ...structuredClone(sourceEdge), source: context, target: NO_VOICEMAIL_EXIT,
      forward_condition: { ...structuredClone(sourceEdge.forward_condition), condition: NO_VOICEMAIL_CONDITION } };
    if (edges[id] && digest(edges[id]) !== digest(edge)) throw new Error('No-voicemail edge collision');
    edges[id] = edge;
    owner.edge_order = [...guards, id, ...owner.edge_order.filter(e => !guards.includes(e) && e !== id)];
  }
  if (digest(noVoicemailProtectedHashes(next)) !== digest(noVoicemailProtectedHashes(agent))) throw new Error('No-voicemail repair changed protected configuration');
  return next;
}
