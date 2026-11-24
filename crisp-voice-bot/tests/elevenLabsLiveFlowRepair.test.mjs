import assert from 'node:assert/strict';
import test from 'node:test';
import {readFile,mkdtemp,writeFile,rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {extractPrompt,sha256} from '../src/scripts/syncElevenLabsPromptOnly.mjs';
import {comparisonBody,digest} from '../src/scripts/verifyElevenLabsComparison.mjs';
import {syncComparisonPrompts} from '../src/scripts/syncElevenLabsComparisonPrompts.mjs';
import {NEW_OPENING,SILENT_RECORDING_CONDITION} from '../src/scripts/elevenLabsSilentRecordingExit.mjs';
import {AI_DISCLOSURE_OLD,AI_DISCLOSURE_NEW} from '../src/scripts/elevenLabsMultilingualAiDisclosure.mjs';
import {applyLiveFlowRepair,restoreR24Prompt,buildLiveFlowPrompt,liveFlowProtectedHashes,FLOW_PATHS,OLD_HANDOFF,NEW_HANDOFF,NATIVE_RECORDING_POLICY,NATIVE_RECORDING_EXIT_CONDITION} from '../src/scripts/elevenLabsLiveFlowRepair.mjs';
const prompt=extractPrompt(await readFile(new URL('../docs/elevenlabs-agent-prompt.md',import.meta.url),'utf8')),old=restoreR24Prompt(prompt);
function fixture(i=0){
 const nodes={recorded_no_voicemail_exit:{type:'end',edge_order:[]}},edges={};
 for(const c of ['opening_listener','main_conversation']){
  const id=c+'_to_recorded_no_voicemail_exit',guards=['terminal_contact_from_'+c,'terminal_guard_from_'+c];
  nodes[c]={type:'override_agent',edge_order:[...guards,id],additional_prompt:OLD_HANDOFF,conversation_config:{agent:{prompt:{prompt:old.replace(AI_DISCLOSURE_NEW,AI_DISCLOSURE_OLD)+'\n\n[CRISP_OPENING_HANDOFF_POLICY]'+OLD_HANDOFF+' '+NEW_OPENING,tool_ids:['protected']}}}};
  edges[id]={source:c,target:'recorded_no_voicemail_exit',forward_condition:{type:'llm',condition:SILENT_RECORDING_CONDITION}};
  for(const g of guards)edges[g]={source:c,target:'guard',forward_condition:{type:'llm',condition:'Preserve callbacks, opt-outs, pending questions and takeover.'}};
 }
 return{agent_id:'agent_test',branch_id:'branch_'+i,version_id:'old_'+i,conversation_config:{agent:{prompt:{prompt:old,llm:'gpt-4.1',tools:[],tool_ids:['protected'],built_in_tools:{voicemail_detection:{type:'system',force_pre_tool_speech:false,params:{voicemail_message:'{{voicemailMessage}}'}}}}},tts:{model_id:i?'eleven_v4_turbo':'eleven_flash_v2',voice_id:'eryn',pvc_strength:0},turn:{turn_model:'turn_v3'}},workflow:{nodes,edges},platform_settings:{protected:true},phone_numbers:[],whatsapp_accounts:[],procedures:{}};
}
test('r26 changes exactly five reviewed strings, preserving tools, guards and settings',()=>{
 assert.equal(sha256(old),'99611d35ede2f755905f4f0e568c1d0a7ebf9b95d8a059a11dca702ad4c2b1b0');assert.equal(buildLiveFlowPrompt(old),prompt);assert.ok(prompt.split(/\s+/).length<6300);
 const a=fixture(),before=JSON.stringify(a),b=applyLiveFlowRepair(a,prompt),changed=[];
 function walk(x,y,p=''){if(typeof x==='string'&&x!==y)changed.push(p);else if(x&&typeof x==='object')for(const k of Object.keys(x))walk(x[k],y[k],p?p+'.'+k:k);}
 walk(a,b);assert.deepEqual(changed.sort(),[...FLOW_PATHS].sort());assert.equal(JSON.stringify(a),before);assert.deepEqual(liveFlowProtectedHashes(a),liveFlowProtectedHashes(b));
 assert.deepEqual(a.workflow.nodes.opening_listener.edge_order,b.workflow.nodes.opening_listener.edge_order);assert.deepEqual(a.conversation_config.agent.prompt.built_in_tools,b.conversation_config.agent.prompt.built_in_tools);
 assert.match(prompt,/Before any introduction, repeated "Hello\?" still requires the unscreened opening/);assert.match(NEW_HANDOFF,/A matching name or repeated hello alone never permits a bare listing question/);
 assert.match(NATIVE_RECORDING_POLICY,/ONLY with empty voicemailMessage/);assert.match(NATIVE_RECORDING_POLICY,/later genuine live speech, questions, callbacks, opt-outs and takeover retain priority/);assert.match(NATIVE_RECORDING_EXIT_CONDITION,/unavailable or voicemailMessage is not empty/);
 assert.equal(digest(comparisonBody(b)),digest(comparisonBody(applyLiveFlowRepair(fixture(1),prompt))));
});
test('r26 rejects unknown roots, nonempty payload bindings, guard drift and arbitrary edits',()=>{
 for(const mutate of [a=>a.conversation_config.agent.prompt.prompt+=' changed',a=>a.conversation_config.agent.prompt.built_in_tools.voicemail_detection.params.voicemail_message='message',a=>a.conversation_config.agent.prompt.built_in_tools.voicemail_detection.force_pre_tool_speech=true,a=>a.workflow.nodes.opening_listener.edge_order.reverse(),a=>a.workflow.nodes.recorded_no_voicemail_exit.type='phone_number',a=>a.workflow.edges.main_conversation_to_recorded_no_voicemail_exit.forward_condition.condition+=' changed']){const a=fixture();mutate(a);assert.throws(()=>applyLiveFlowRepair(a,prompt));}
 assert.throws(()=>applyLiveFlowRepair(fixture(),prompt+' pitch'));
});
test('r26 paired publisher requires reviewed dry-run and stops after ambiguous write',async t=>{
 const dir=await mkdtemp(path.join(os.tmpdir(),'live-flow-'));t.after(()=>rm(dir,{recursive:true,force:true}));const promptPath=path.join(dir,'prompt.md');await writeFile(promptPath,'## Prompt\n'+prompt);
 const agents=[fixture(),fixture(1)],writes=[];let ambiguous=false;
 const fetcher=async(u,o)=>{if(u.endsWith('/branches'))return{ok:true,json:async()=>({meta:{total:2},results:agents.map(a=>({id:a.branch_id,is_archived:false,current_live_percentage:50}))})};const i=Number(new URL(u).searchParams.get('branch_id').slice(-1));if(o.method==='PATCH'){writes.push(i);if(ambiguous)throw Error('ambiguous');const b=JSON.parse(o.body);assert.deepEqual(Object.keys(b.conversation_config.agent.prompt),['prompt']);agents[i].conversation_config.agent.prompt.prompt=b.conversation_config.agent.prompt.prompt;agents[i].workflow=b.workflow;agents[i].version_id='new_'+i;}return{ok:true,json:async()=>structuredClone(agents[i])};};
 const opts={apiKey:'test',agentId:'agent_test',branches:agents.map(a=>({id:a.branch_id,version:a.version_id,promptHash:sha256(old)})),promptPath,receiptDir:dir,repairLiveFlow:true};
 await assert.rejects(syncComparisonPrompts({...opts,apply:true},fetcher));assert.equal(writes.length,0);
 const plan=await syncComparisonPrompts(opts,fetcher);assert.equal(plan.release,'unscreened-opening-and-native-silent-ending-r26');assert.equal(writes.length,0);
 const receipt=await syncComparisonPrompts({...opts,apply:true},fetcher);assert.equal(receipt.status,'live_uniform_comparison_verified');assert.deepEqual(writes,[0,1]);assert.equal(receipt.realCallsPlaced,false);
 const dir2=await mkdtemp(path.join(os.tmpdir(),'live-flow-ambiguous-'));t.after(()=>rm(dir2,{recursive:true,force:true}));agents.splice(0,2,fixture(),fixture(1));writes.length=0;await syncComparisonPrompts({...opts,receiptDir:dir2},fetcher);ambiguous=true;await assert.rejects(syncComparisonPrompts({...opts,receiptDir:dir2,apply:true},fetcher),/ambiguous/);assert.deepEqual(writes,[0]);
});
