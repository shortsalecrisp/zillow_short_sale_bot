import { digest } from './verifyElevenLabsComparison.mjs';
import {sha256} from './syncElevenLabsPromptOnly.mjs';
const marker = '\n\n[CRISP_OPENING_HANDOFF_POLICY]';
const pending = 'If THIS same live listener already confirmed the listing, preserve that fact. A later hello/presence check must never restart identity or the listing question. Say "I\'m here." and repeat only the unanswered question once; when the needs question is pending say "I\'m here. Would help with lender paperwork or calls be useful for this listing?" Then wait. Explicit hearing trouble, questions, corrections, callbacks, opt-outs and takeover retain priority. Silence is not permission to end.';
const edgeGuard = ' Do not enter listing recovery if this same listener already confirmed the listing. Keep or route to main_conversation for the pending-question repair, never another listing check.';
export const CONVERSATION_RECOVERY_PATHS = [
  'conversation_config.agent.prompt.prompt',
  'workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt',
  'workflow.nodes.short_listing_recovery.conversation_config.agent.prompt.prompt',
  'workflow.edges.opening_to_short_listing_recovery.forward_condition.condition',
  'workflow.edges.main_to_short_listing_recovery.forward_condition.condition',
];
function parent(value, path) { const keys=path.split('.'), key=keys.pop(); for(const part of keys) value=value?.[part]; if(!value || typeof value[key] !== 'string') throw new Error('Reviewed recovery leaf missing: '+path); return [value,key]; }
export function recoveryProtectedHashes(agent) {
  const next=structuredClone(agent);
  delete next.conversation_config.agent.prompt.tools;
  for(const path of CONVERSATION_RECOVERY_PATHS){const [value,key]=parent(next,path);delete value[key];}
  return { unchanged:digest({conversation_config:next.conversation_config,workflow:next.workflow}),
    ...Object.fromEntries(['platform_settings','phone_numbers','whatsapp_accounts','procedures'].map(key=>[key,digest(agent[key]??null)])) };
}
export function applyConversationRecovery(agent,prompt) {
  if(typeof prompt !== 'string' || !prompt.startsWith('# Role and style\n') || prompt.trim().split(/\s+/).length>=6300) throw new Error('Reviewed prompt must remain below 6300 words');
  const next=structuredClone(agent), old=agent.conversation_config.agent.prompt.prompt;
  const [opening,key]=parent(next,CONVERSATION_RECOVERY_PATHS[1]);
  const parts=opening[key].split(marker);
  const approvedR18Prefix=sha256(old)==='ddcaab684d7f2a4c62621318d0772fd093e49462ad877a39fbe0ac371d10ad58' &&
    sha256(parts[0])==='6bddb3c43b64d8763a0d37402b5ed455171ddbe5985343e41688d0efa7bca530';
  if(parts.length!==2 || (parts[0]!==old && !approvedR18Prefix) || opening[key].includes(pending)) throw new Error('Reviewed r18 opening boundary required');
  const suffix=marker+parts[1];
  next.conversation_config.agent.prompt.prompt=prompt;
  const oldRecovery='A repeated hello or presence check from the same listener who already heard your identity gets only "Is {{streetAddress}} your listing?".';
  const newRecovery='ONLY while listing ownership is unconfirmed, a repeated hello from the same listener gets "Is {{streetAddress}} your listing?". After confirmation, use the pending-question repair above, never the listing check.';
  if(suffix.split(oldRecovery).length!==2) throw new Error('Exact r18 repeated-hello rule required');
  opening[key]=prompt+suffix.replace('# Opening listener entry\n', '# Opening listener entry\n\n'+pending+'\n').replace(oldRecovery,newRecovery);
  if(opening[key]===prompt+suffix) throw new Error('Opening listener boundary missing');
  const [recovery,rkey]=parent(next,CONVERSATION_RECOVERY_PATHS[2]);
  if(!recovery[rkey].startsWith('# Short listing-check recovery state\n')) throw new Error('Reviewed short recovery state required');
  recovery[rkey]=recovery[rkey].replace('# Short listing-check recovery state\n','# Short listing-check recovery state\n\n'+pending+'\nThe listing-check rules below apply ONLY while listing ownership remains unconfirmed.\n');
  for(const path of CONVERSATION_RECOVERY_PATHS.slice(3)){const [value,key]=parent(next,path);if(!value[key].includes('listing check') || value[key].includes(edgeGuard)) throw new Error('Reviewed recovery edge required');value[key]+=edgeGuard;}
  if(digest(recoveryProtectedHashes(next))!==digest(recoveryProtectedHashes(agent))) throw new Error('Recovery changed protected settings');
  return next;
}
