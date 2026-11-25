import assert from 'node:assert/strict';
import test from 'node:test';
import {readFile,mkdtemp,writeFile,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {extractPrompt,sha256} from '../src/scripts/syncElevenLabsPromptOnly.mjs';
import {syncComparisonPrompts} from '../src/scripts/syncElevenLabsComparisonPrompts.mjs';
import {NO_VOICEMAIL_CONDITION} from '../src/scripts/elevenLabsNoVoicemailWorkflow.mjs';
import {digest} from '../src/scripts/verifyElevenLabsComparison.mjs';
import {applyAiVoicemailRepair,aiVoicemailProtectedHashes,AI_STATUS_OLD,AI_STATUS_NEW,SECOND_ATTEMPT_OLD,SECOND_ATTEMPT_NEW,OPENING_VOICEMAIL_OLD,OPENING_VOICEMAIL_NEW,SECOND_ATTEMPT_RECORDING_CONDITION,AI_VOICEMAIL_PATHS} from '../src/scripts/elevenLabsAiVoicemailRepair.mjs';
import {OLD_ATTEMPT,NEW_ATTEMPT,OLD_MESSAGE,NEW_MESSAGE,OLD_DELIVERY,NEW_DELIVERY} from '../src/scripts/elevenLabsSilentRecordingExit.mjs';
import {AI_DISCLOSURE_OLD,AI_DISCLOSURE_NEW} from '../src/scripts/elevenLabsMultilingualAiDisclosure.mjs';
import {restoreR24Prompt} from '../src/scripts/elevenLabsLiveFlowRepair.mjs';
import {restoreR26Opening} from '../src/scripts/elevenLabsOpeningAndMenuRepair.mjs';
const prompt=restoreR24Prompt(restoreR26Opening(extractPrompt(await readFile(new URL('../docs/elevenlabs-agent-prompt.md',import.meta.url),'utf8')))).replace(AI_DISCLOSURE_NEW,AI_DISCLOSURE_OLD).replace(NEW_ATTEMPT,OLD_ATTEMPT).replace(NEW_MESSAGE,OLD_MESSAGE).replace(NEW_DELIVERY,OLD_DELIVERY);
const old=prompt.replace(AI_STATUS_NEW,AI_STATUS_OLD).replace(SECOND_ATTEMPT_NEW,SECOND_ATTEMPT_OLD);
function fixture(i=0){
  const edges={},nodes={recorded_no_voicemail_exit:{type:'end',edge_order:[],return_when_nested:true}};
  for(const context of ['opening_listener','main_conversation']){
    const id=context+'_to_recorded_no_voicemail_exit',guards=['terminal_contact_from_'+context,'terminal_guard_from_'+context];
    edges[id]={source:context,target:'recorded_no_voicemail_exit',forward_condition:{type:'llm',condition:NO_VOICEMAIL_CONDITION}};
    for(const g of guards)edges[g]={source:context,target:'guard',forward_condition:{type:'llm',condition:'Keep pending questions, callback consent, opt-outs and takeover.'}};
    nodes[context]={type:'override_agent',edge_order:[...guards,id],conversation_config:{agent:{prompt:{prompt:old+'\n\n[CRISP_OPENING_HANDOFF_POLICY]\nProtected screening policy.\n'+OPENING_VOICEMAIL_OLD+' Keep unrelated recording protection.',tool_ids:['tool_original']}}}};
  }
  return{agent_id:'agent_test',branch_id:'branch_'+i,version_id:'old_'+i,conversation_config:{agent:{prompt:{prompt:old,llm:'gpt-4.1',tool_ids:['tool_original'],tools:[],built_in_tools:{end_call:null}}},tts:{model_id:i?'eleven_v4_turbo':'eleven_flash_v2',voice_id:'eryn',speed:1,pvc_strength:0},turn:{turn_model:'turn_v3'}},workflow:{nodes,edges},platform_settings:{protected:true},phone_numbers:['original'],whatsapp_accounts:[],procedures:{}};
}
test('r22 changes exactly four reviewed strings and leaves routing, tools, audio and live guards intact',()=>{
  assert.equal(sha256(old),'abe58dfc16e20b8d8a9405e676fce0d815e66b85f463af8957047121f9fb069b');
  const a=fixture(),before=JSON.stringify(a),b=applyAiVoicemailRepair(a,prompt);
  assert.equal(JSON.stringify(a),before);assert.deepEqual(aiVoicemailProtectedHashes(a),aiVoicemailProtectedHashes(b));
  const paths=[];function walk(a,b,p=''){if(JSON.stringify(a)===JSON.stringify(b))return;if(a&&b&&typeof a==='object'&&typeof b==='object'&&!Array.isArray(a)&&!Array.isArray(b)){for(const k of new Set([...Object.keys(a),...Object.keys(b)]))walk(a[k],b[k],p?p+'.'+k:k);}else paths.push(p);}walk(a,b);
  assert.deepEqual(paths.sort(),[...AI_VOICEMAIL_PATHS].sort());
  assert.equal(prompt.split(/\s+/).length<6300,true);
  assert.equal(b.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt.split('\n\n[CRISP_OPENING_HANDOFF_POLICY]')[0],prompt);
  assert.match(b.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt,new RegExp(OPENING_VOICEMAIL_NEW.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')));
  for(const c of ['opening_listener','main_conversation'])assert.deepEqual(b.workflow.nodes[c].edge_order,a.workflow.nodes[c].edge_order);
  assert.ok(SECOND_ATTEMPT_RECORDING_CONDITION.includes('{{callAttemptNumber}} is exactly 2'));
  assert.match(SECOND_ATTEMPT_RECORDING_CONDITION,/later genuine live speech, questions, callbacks\/email requests, opt-outs or takeover/);
  assert.match(SECOND_ATTEMPT_RECORDING_CONDITION,/screening\/hold still trying to connect someone/);
  assert.match(SECOND_ATTEMPT_RECORDING_CONDITION,/On attempt one a faster-text suggestion alongside a message invitation remains/);
});
test('r22 rejects prompt drift, changed guard ordering and missing silent exits',()=>{
  for(const edit of [a=>{a.conversation_config.agent.prompt.prompt+=' changed';},
    a=>{a.workflow.nodes.opening_listener.conversation_config.agent.prompt.prompt='wrong';},
    a=>{a.workflow.nodes.recorded_no_voicemail_exit.type='phone_number';},
    a=>{a.workflow.nodes.main_conversation.edge_order.reverse();},
    a=>{a.workflow.edges.main_conversation_to_recorded_no_voicemail_exit.forward_condition.condition+=' changed';}]){
    const a=fixture();edit(a);assert.throws(()=>applyAiVoicemailRepair(a,prompt));
  }
  assert.throws(()=>applyAiVoicemailRepair(fixture(),prompt+' Unreviewed pitch.'),/Only reviewed/);
  assert.throws(()=>applyAiVoicemailRepair(fixture(),'word '.repeat(6300)),/Only reviewed/);
});
async function setup(t){
  const dir=await mkdtemp(path.join(os.tmpdir(),'voice-r22-test-'));t.after(()=>rm(dir,{recursive:true,force:true}));
  const promptPath=path.join(dir,'prompt.md');await writeFile(promptPath,'## Prompt\n'+prompt);
  const agents=[fixture(),fixture(1)],writes=[];let fail=false,reads=0;
  const fetcher=async(url,o)=>{if(url.endsWith('/branches'))return{ok:true,json:async()=>({meta:{total:2},results:agents.map(a=>({id:a.branch_id,is_archived:false,current_live_percentage:50}))})};
    const i=Number(new URL(url).searchParams.get('branch_id').slice(-1));
    if(o.method==='PATCH'){writes.push(i);if(fail)throw Error('ambiguous timeout');const p=JSON.parse(o.body);assert.deepEqual(Object.keys(p).sort(),['conversation_config','workflow']);assert.deepEqual(Object.keys(p.conversation_config.agent.prompt),['prompt']);agents[i].workflow=p.workflow;agents[i].conversation_config.agent.prompt.prompt=p.conversation_config.agent.prompt.prompt;agents[i].version_id='new_'+i;}
    else reads++;
    return{ok:true,json:async()=>structuredClone(agents[i])};};
  return{agents,writes,fetcher,fail:()=>{fail=true;},options:{apiKey:'synthetic',agentId:'agent_test',branches:agents.map(a=>({id:a.branch_id,version:a.version_id,promptHash:sha256(old)})),promptPath,receiptDir:dir,repairAiVoicemail:true}};
}
test('r22 paired publisher requires matching dry run and final two-arm readback',async t=>{
  const s=await setup(t),before=s.agents.map(aiVoicemailProtectedHashes);const plan=await syncComparisonPrompts(s.options,s.fetcher);
  assert.equal(s.writes.length,0);assert.equal(plan.release,'ai-status-and-second-attempt-recording-r22');
  const receipt=await syncComparisonPrompts({...s.options,apply:true},s.fetcher);
  assert.equal(receipt.status,'live_uniform_comparison_verified');assert.deepEqual(s.writes,[0,1]);assert.deepEqual(receipt.trafficPercent,[50,50]);
  for(const [i,a] of s.agents.entries())assert.deepEqual(aiVoicemailProtectedHashes(a),before[i]);
  assert.equal(receipt.realCallsPlaced,false);
});
test('r22 ambiguous writes are held without a second patch or uniform receipt',async t=>{
  const s=await setup(t);await syncComparisonPrompts(s.options,s.fetcher);s.fail();
  await assert.rejects(syncComparisonPrompts({...s.options,apply:true},s.fetcher),/ambiguous timeout/);assert.deepEqual(s.writes,[0]);
});
