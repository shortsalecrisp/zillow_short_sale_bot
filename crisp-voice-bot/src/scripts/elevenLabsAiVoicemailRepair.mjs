import {digest} from './verifyElevenLabsComparison.mjs';
import {sha256} from './syncElevenLabsPromptOnly.mjs';
import {NO_VOICEMAIL_CONDITION} from './elevenLabsNoVoicemailWorkflow.mjs';

const marker='\n\n[CRISP_OPENING_HANDOFF_POLICY]';
export const AI_STATUS_OLD='- Are you AI: "Yes, I\'m an AI assistant with Crisp Short Sales."\n- If the caller asks whether you want to switch to AI voice: "I\'m already an AI assistant with Crisp Short Sales." Then wait. Do not restart the listing check, repeat the service explanation, or offer Yoni unless the caller makes a new, clear request.';
export const AI_STATUS_NEW='- AI/robot/automated/robocall questions, including "Is this a robocall?", "Are you AI?" or switching to AI voice: "I\'m already an AI assistant with Crisp Short Sales." Answer an accompanying substantive question; otherwise wait. Do not add a service explanation, listing check or Yoni offer without a new clear request.';
export const SECOND_ATTEMPT_OLD='- Attempt 2: leave no second voicemail. For a matching voicemail greeting, use voicemail_detection after the greeting finishes; the backend supplies an empty voicemailMessage so this path ends without a second message. Explicit no-voicemail greetings use the silent no-message exit.';
export const SECOND_ATTEMPT_NEW='- Attempt 2: never speak a second message, even when a screener says "not available" and invites an "additional message". After that completed recording or a completed matching voicemail greeting, use the silent no-message exit before speech or voicemail_detection. Later genuine live speech, questions, callbacks, opt-outs and takeover retain priority.';
export const OPENING_VOICEMAIL_OLD='Actual voicemail is different from screening or hold. Let the recorded greeting finish. Use voicemail_detection at its invitation to leave a message or the first natural pause after it, not mid-sentence. The backend supplies the approved first-attempt message; attempt two has no second message. Do not speak a live introduction over a recording.';
export const OPENING_VOICEMAIL_NEW='Actual voicemail is different from screening or hold. Let the recorded greeting finish. On attempt one, use voicemail_detection at its invitation or first natural pause, not mid-sentence. On attempt two, a completed voicemail greeting or recorded "not available" plus "additional message" invitation uses recorded_no_voicemail_exit silently before speech or voicemail_detection. Screening/hold still connecting someone is not an exit reason; later genuine live speech, questions, callbacks, opt-outs and takeover retain priority. Do not speak a live introduction over a recording.';
export const SECOND_ATTEMPT_RECORDING_CONDITION='Only after the latest completed turn is clearly PRERECORDED and either (A) the voicemail greeting explicitly says do not leave a voicemail/message or text instead OF voicemail, or (B) {{callAttemptNumber}} is exactly 2 and it is a completed matching voicemail greeting or a terminal screener saying the person is not available and inviting an additional message after the tone. For either case exit silently before any speech or voicemail_detection; never repeat a message or callback number on attempt two. On attempt one a faster-text suggestion alongside a message invitation remains the normal attempt-specific voicemail path, not a prohibition. Never route on a live person/admin, quote, uncertainty about recording origin, incomplete greeting, screening/hold still trying to connect someone, or later genuine live speech, questions, callbacks/email requests, opt-outs or takeover. Existing live contact/ending guards keep priority. Recorded speech creates no contact preference, consent, completed handoff or message-delivery proof.';
export const AI_VOICEMAIL_PATHS=[
  'conversation_config.agent.prompt.prompt',
  'workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt',
  'workflow.edges.opening_listener_to_recorded_no_voicemail_exit.forward_condition.condition',
  'workflow.edges.main_conversation_to_recorded_no_voicemail_exit.forward_condition.condition',
];
function parent(a,path){const keys=path.split('.'),key=keys.pop();for(const k of keys)a=a?.[k];if(!a||typeof a[key]!=='string')throw Error('Reviewed AI/voicemail leaf missing');return[a,key];}
function replaceOnce(text,old,next){if(text.split(old).length!==2)throw Error('Exact reviewed AI/voicemail predecessor required');return text.replace(old,next);}
export function aiVoicemailProtectedHashes(agent){const a=structuredClone(agent);delete a.conversation_config.agent.prompt.tools;for(const p of AI_VOICEMAIL_PATHS){const[v,k]=parent(a,p);delete v[k];}return{unchanged:digest({conversation_config:a.conversation_config,workflow:a.workflow}),...Object.fromEntries(['platform_settings','phone_numbers','whatsapp_accounts','procedures'].map(k=>[k,digest(agent[k]??null)]))};}
export function applyAiVoicemailRepair(agent,prompt){
  const a=structuredClone(agent),old=a.conversation_config?.agent?.prompt?.prompt;
  if(typeof old!=='string'||sha256(old)!=='abe58dfc16e20b8d8a9405e676fce0d815e66b85f463af8957047121f9fb069b')throw Error('Exact reviewed r21 root required');
  const expected=replaceOnce(replaceOnce(old,AI_STATUS_OLD,AI_STATUS_NEW),SECOND_ATTEMPT_OLD,SECOND_ATTEMPT_NEW);
  if(prompt!==expected||prompt.trim().split(/\s+/).length>=6300)throw Error('Only reviewed AI/voicemail business changes below 6300 words allowed');
  const [opening,key]=parent(a,AI_VOICEMAIL_PATHS[1]),parts=opening[key].split(marker);
  if(parts.length!==2||parts[0]!==old)throw Error('Matched r21 embedded policy required');
  opening[key]=prompt+marker+replaceOnce(parts[1],OPENING_VOICEMAIL_OLD,OPENING_VOICEMAIL_NEW);
  a.conversation_config.agent.prompt.prompt=prompt;
  if(a.workflow.nodes.recorded_no_voicemail_exit?.type!=='end')throw Error('Existing silent recording exit required');
  for(const context of ['opening_listener','main_conversation']){
    const id=context+'_to_recorded_no_voicemail_exit',edge=a.workflow.edges[id],order=a.workflow.nodes[context]?.edge_order;
    if(edge?.source!==context||edge.target!=='recorded_no_voicemail_exit'||edge.forward_condition?.type!=='llm'
      ||edge.forward_condition.condition!==NO_VOICEMAIL_CONDITION||!Array.isArray(order)
      ||order.indexOf('terminal_contact_from_'+context)<0||order.indexOf('terminal_guard_from_'+context)<0
      ||order.indexOf('terminal_contact_from_'+context)>order.indexOf(id)||order.indexOf('terminal_guard_from_'+context)>order.indexOf(id))throw Error('Existing live guard priority or recorded exit drifted');
    edge.forward_condition.condition=SECOND_ATTEMPT_RECORDING_CONDITION;
  }
  if(digest(aiVoicemailProtectedHashes(a))!==digest(aiVoicemailProtectedHashes(agent)))throw Error('AI/voicemail repair changed protected fields');
  return a;
}
