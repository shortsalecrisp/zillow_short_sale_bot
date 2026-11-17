import {digest} from './verifyElevenLabsComparison.mjs';
import {sha256} from './syncElevenLabsPromptOnly.mjs';
const marker='\n\n[CRISP_OPENING_HANDOFF_POLICY]';
export const SCREENING_RECOVERY_POLICY='After automated screening or hold, this rule overrides the normal full opening and generic NEW-listener instructions. Before speaking, distinguish the actual live listener: a spoken matching {{firstName}} (for example "This is Jasmine", even with throat-clearing/nonverbal cues) gets only "Is {{streetAddress}} your listing?" An unnamed Hello or a different name gets "Hi, {{assistantName}} with Crisp Short Sales. Is {{streetAddress}} your listing?" Never use the full opener after screening or infer identity from the number, screener or placeholder. If this same listener already confirmed the listing, preserve it and repair only the pending question. Questions, hearing trouble, callbacks, opt-outs and takeover retain priority. Recordings, silence commentary and unaddressed broadcasts are not live pickups; use skip_turn without speaking stage directions.';
export const SCREENING_RECOVERY_PATHS=[
  'conversation_config.agent.prompt.prompt',
  'workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt',
  'workflow.nodes.short_listing_recovery.conversation_config.agent.prompt.prompt',
];
function parent(agent,path){const keys=path.split('.'),key=keys.pop();for(const k of keys)agent=agent?.[k];if(!agent||typeof agent[key]!=='string')throw Error('Reviewed screening leaf missing');return[agent,key];}
export function screeningRecoveryProtectedHashes(agent){const a=structuredClone(agent);delete a.conversation_config.agent.prompt.tools;for(const p of SCREENING_RECOVERY_PATHS){const[v,k]=parent(a,p);delete v[k];}return{unchanged:digest({conversation_config:a.conversation_config,workflow:a.workflow}),...Object.fromEntries(['platform_settings','phone_numbers','whatsapp_accounts','procedures'].map(k=>[k,digest(agent[k]??null)]))};}
export function applyScreeningRecovery(agent,prompt){
  if(typeof prompt!=='string'||!prompt.startsWith('# Role and style\n')||prompt.trim().split(/\s+/).length>=6300)throw Error('Screening prompt must remain strictly below 6300 words');
  const a=structuredClone(agent),old=agent.conversation_config.agent.prompt.prompt;
  const[opening,okey]=parent(a,SCREENING_RECOVERY_PATHS[1]);const parts=opening[okey].split(marker);
  // The exact r20 root retained the r19 embedded policy. Repair only this pinned predecessor pair.
  const reviewedR20Prefix=sha256(old)==='d09194078aa2a49ec6ab2e201b8725566c687a3bf83c08513406d67586e3bc30'&&
    sha256(parts[0])==='1504f87b2f36ee90e7864c88b815650c3c724fff657c035c5763700fdcdf189a';
  if(parts.length!==2||(parts[0]!==old&&!reviewedR20Prefix)||opening[okey].includes(SCREENING_RECOVERY_POLICY))throw Error('Exact reviewed r20 opening boundary required');
  const[recovery,rkey]=parent(a,SCREENING_RECOVERY_PATHS[2]);
  if(!recovery[rkey].startsWith('# Short listing-check recovery state\n')||recovery[rkey].includes(SCREENING_RECOVERY_POLICY))throw Error('Exact reviewed short-listing recovery required');
  a.conversation_config.agent.prompt.prompt=prompt;
  opening[okey]=prompt+marker+'\n'+SCREENING_RECOVERY_POLICY+'\n'+parts[1];
  recovery[rkey]=recovery[rkey].replace('# Short listing-check recovery state\n','# Short listing-check recovery state\n\n'+SCREENING_RECOVERY_POLICY+'\n');
  if(digest(screeningRecoveryProtectedHashes(a))!==digest(screeningRecoveryProtectedHashes(agent)))throw Error('Screening release changed protected fields');
  return a;
}
