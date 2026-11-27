import { digest } from './verifyElevenLabsComparison.mjs';
import { applySharedBusinessPrompt, sharedBusinessPromptProtectedHashes } from './elevenLabsSharedBusinessPrompt.mjs';

export const LOOP_ROUTE = 'After four identical correct callback-number confirmation questions, only after Maya answers the latest question exactly "Yes, that number is correct. Thank you for taking the message.", use the existing ending-permission guard. Never use this for a different number, new substantive question, correction, opt-out, hold, stop, or request to wait. The backend validates the actual complete history and may deny ending.';
const contexts = ['main_conversation', 'callback_after_unavailable', 'patch_transfer', 'opening_listener', 'short_listing_recovery', 'callback_receipt_wait'];
export function loopProtectedHashes(agent) {
  const clone = structuredClone(agent);
  for (const context of contexts) {
    const condition = clone.workflow?.edges?.['terminal_guard_from_' + context]?.forward_condition;
    if (typeof condition?.condition !== 'string') throw Error('Missing existing guarded ending edge');
    delete condition.condition;
  }
  return sharedBusinessPromptProtectedHashes(clone);
}
export function applyNumberLoopRepair(agent, prompt) {
  const next = applySharedBusinessPrompt(agent, prompt);
  for (const context of contexts) {
    const edge = next.workflow?.edges?.['terminal_guard_from_' + context];
    if (edge?.source !== context || edge.target !== 'terminal_guard_reset' || edge.forward_condition?.type !== 'llm' ||
        !edge.forward_condition.condition.includes('A NEW live caller turn contains a genuine goodbye') || edge.forward_condition.condition.includes(LOOP_ROUTE)) {
      throw Error('Existing guarded ending condition drifted');
    }
    edge.forward_condition.condition += ' ' + LOOP_ROUTE;
  }
  if (digest(loopProtectedHashes(agent)) !== digest(loopProtectedHashes(next))) throw Error('Protected settings changed');
  return next;
}
