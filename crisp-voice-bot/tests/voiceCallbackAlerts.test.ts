import assert from "node:assert/strict";
import test, { after } from "node:test";
import axios from "axios";
Object.assign(process.env, { BASE_URL:"https://voice.example.com",TELNYX_API_KEY:"synthetic",TELNYX_CALLER_ID:"+12175550100",
  TELNYX_CONNECTION_ID:"synthetic",TELNYX_OUTBOUND_VOICE_PROFILE_ID:"synthetic",TEST_DESTINATION_NUMBER:"+12175550101",
  ELEVENLABS_AGENT_ID:"agent_callback_test",ELEVENLABS_API_KEY:"synthetic",ELEVENLABS_TOOL_SECRET:"synthetic",
  GOOGLE_SHEETS_TAB_NAME:"Synthetic",GOOGLE_APPS_SCRIPT_TOKEN:"callback-test-auth",CALL_TRANSCRIPT_EMAILS_ENABLED:"false",ALERT_EMAIL_TO:"owner@example.invalid" });
const identity={conversationId:"conv_callbacktest",rowNumber:123,callAttemptNumber:1,agentName:"Synthetic Owner",phone:"+12025550123",listingAddress:"123 Fictional St, Miami, FL"};
const prefix="voice_callback_owner_alert_v1:";
const providerFixtures=new Map<string,unknown>(),originalAdapter=axios.defaults.adapter;
axios.defaults.adapter=async request=>{
  const id=request.url?.split('/').at(-1);
  if(request.method!=='get'||!id||!providerFixtures.has(id))throw new Error('Unexpected network request forbidden in callback tests');
  return {status:200,statusText:'OK',headers:{},config:request,data:providerFixtures.get(id)};
};
after(()=>{axios.defaults.adapter=originalAdapter;});
function fixture(){
  const row=Array(45).fill("");Object.assign(row,{0:"Synthetic",1:"Owner",2:identity.phone,3:"contact@example.invalid",4:"123 Fictional St",5:"Miami",6:"FL",10:"Y",32:"2026-11-11T14:20:00Z",33:"callback_requested",37:"yes",38:"Wednesday around 3"});
  const receipt={conversation_id:identity.conversationId,agent_id:"agent_callback_test",status:"done",has_user_audio:true,
    metadata:{start_time_unix_secs:Date.parse(String(row[32]))/1000,accepted_time_unix_secs:Date.parse(String(row[32]))/1000,call_duration_secs:90,phone_call:{external_number:identity.phone,direction:"outbound"}},
    conversation_initiation_client_data:{dynamic_variables:{rowNumber:123,callAttemptNumber:1,phone:identity.phone,requestedPhone:identity.phone,agentName:identity.agentName,listingAddress:identity.listingAddress,testMode:false}},
    analysis:{transcript_summary:"The caller requested a callback to discuss the listing."},transcript:[
      {role:"agent",message:"Would help with lender paperwork or calls be useful for this listing?"},{role:"user",message:"Yes."},
      {role:"user",message:"Please have Yoni call me back Wednesday around 3."},
    ]};
  const states=new Map<string,any>(), sent:any[]=[];
  let blocked=false;
  const deps={read:async(key:string)=>states.get(key)&&structuredClone(states.get(key)),write:async(key:string,value:any)=>{states.set(key,structuredClone(value));},
    list:async()=>[...states].filter(([key])=>key.startsWith(prefix)).map(([key,value])=>({key,value})),receipt:async()=>structuredClone(receipt),row:async()=>[...row],
    inboundBlocked:async()=>blocked,recipient:"owner@example.invalid",send:async(message:any)=>{sent.push(message);return {accepted:["owner@example.invalid"],rejected:[]};}};
  Object.assign(deps,{policy:async()=>({activatedAt:'2026-11-11T14:00:00Z'})});
  return {row,receipt,states,sent,deps,block:()=>{blocked=true;}};
}
test("verified callback sends one complete hot-lead email, independent of transcript toggle; restart and replay cannot duplicate",async()=>{
  const {createVoiceCallbackAlertService,getVoiceCallbackAlertStatus}=await import('../src/lib/voiceCallbackAlerts');
  const f=fixture(),before=[...f.row],service=createVoiceCallbackAlertService(f.deps);
  await service.enqueue(identity);await service.enqueue(identity);
  assert.deepEqual(await Promise.all([service.settle(identity.conversationId),service.settle(identity.conversationId)]),['accepted','accepted']);
  await createVoiceCallbackAlertService(f.deps).recover();assert.equal(f.sent.length,1);assert.deepEqual(f.row,before);
  const message=f.sent[0];assert.match(message.subject,/HOT LEAD - PHONE CALLBACK REQUEST/);
  for(const field of ['Synthetic Owner','202-555-0123','123 Fictional St','Wednesday around 3','Call summary:','Caller: Please have Yoni','Playback:','sig='])assert.ok(message.text.includes(field),field);
  assert.doesNotMatch(message.text,/Scheduled Time:|ASAP/);assert.match(message.text,/not a booked appointment/);
  assert.equal(getVoiceCallbackAlertStatus().independentOfTranscriptEmailToggle,true);
});
test("active/pending receipts wait; callback after a missing tool is recovered by durable outbox, not another call",async()=>{
  const {createVoiceCallbackAlertService}=await import('../src/lib/voiceCallbackAlerts');const f=fixture();f.receipt.status='in-progress';
  const service=createVoiceCallbackAlertService(f.deps);await service.enqueue(identity);await service.recover();assert.equal(f.sent.length,0);
  f.receipt.status='done';await createVoiceCallbackAlertService(f.deps).recover();assert.equal(f.sent.length,1);
});
test("uncertain SMTP and interrupted sending claims survive restart without a resend",async()=>{
  const {createVoiceCallbackAlertService}=await import('../src/lib/voiceCallbackAlerts');const f=fixture();let attempts=0;
  const deps={...f.deps,send:async()=>{attempts++;throw new Error('uncertain SMTP acceptance');}};
  const service=createVoiceCallbackAlertService(deps);await service.enqueue(identity);assert.equal(await service.settle(identity.conversationId),'uncertain');
  await createVoiceCallbackAlertService(deps).recover();assert.equal(attempts,1);
  f.states.get(prefix+identity.conversationId).status='sending';await createVoiceCallbackAlertService(deps).recover();assert.equal(attempts,1);
});
test("final revocation, recordings, information-only preference and test calls never become callback alerts",async()=>{
  const {createVoiceCallbackAlertService}=await import('../src/lib/voiceCallbackAlerts');
  for(const message of ['Cancel the callback.','Please do not call me again.','Email me the information instead.','Just send me an email.','I will call you back.']){
    const f=fixture();f.receipt.transcript.push({role:'user',message});const service=createVoiceCallbackAlertService(f.deps);
    await service.enqueue(identity);assert.equal(await service.settle(identity.conversationId),'cancelled',message);assert.equal(f.sent.length,0);
  }
  const f=fixture();f.receipt.transcript=[{role:'user',message:'This is Synthetic. Sorry I missed your call. I will call you back. Thank you. Please leave a message after the beep.'}];
  const s=createVoiceCallbackAlertService(f.deps);await s.enqueue(identity);assert.equal(await s.settle(identity.conversationId),'cancelled');assert.equal(f.sent.length,0);
  const g=fixture();const t=createVoiceCallbackAlertService(g.deps);assert.equal(await t.enqueue({...identity,testMode:true}),'test_excluded');assert.equal(g.states.size,0);
});
test("current-phone changes, later suppression and pending inbound block delivery without modifying preferences",async()=>{
  const {createVoiceCallbackAlertService}=await import('../src/lib/voiceCallbackAlerts');
  for(const change of [(f:ReturnType<typeof fixture>)=>{f.row[2]='+12025550999';},(f:ReturnType<typeof fixture>)=>{f.row[10]='R';},(f:ReturnType<typeof fixture>)=>f.block()]){
    const f=fixture(),s=createVoiceCallbackAlertService(f.deps);await s.enqueue(identity);change(f);const before=[...f.row];await s.settle(identity.conversationId);
    assert.equal(f.sent.length,0);assert.deepEqual(f.row,before);
  }
  const f=fixture();f.row[19]='yes';const before=[...f.row],s=createVoiceCallbackAlertService(f.deps);await s.enqueue(identity);await s.settle(identity.conversationId);
  assert.equal(f.sent.length,1,'owner-only notification does not place outreach or clear takeover');assert.deepEqual(f.row,before);assert.match(f.sent[0].text,/Human takeover is active and unchanged/);
});
test("source callback identity cannot be replaced and the one approved synthetic test cannot repeat",async()=>{
  const {createVoiceCallbackAlertService}=await import('../src/lib/voiceCallbackAlerts');const f=fixture(),s=createVoiceCallbackAlertService(f.deps);await s.enqueue(identity);
  await assert.rejects(s.enqueue({...identity,phone:'+12025550999'}),/identity changed/);
  const a=await s.selfTest(),b=await createVoiceCallbackAlertService(f.deps).selfTest();assert.equal(a.status,'accepted');assert.equal(b.status,'accepted');
  assert.equal(f.sent.length,1);assert.match(f.sent[0].subject,/^\[TEST\]/);assert.match(f.sent[0].text,/NOT a real hot lead/);
});

test("the real post-call entry point captures a callback before positive-needs/hangup classification, even when its tool never fired",async()=>{
  const {processPostCallOutcomeFromConversationId}=await import('../src/lib/elevenLabsPostCall');
  const f=fixture(),order:string[]=[],writes:any[]=[];
  const id='conv_callbackpostcall';f.receipt.conversation_id=id;
  providerFixtures.set(id,f.receipt);
  try{
    const options={postSheetUpdate:async(payload:any)=>{order.push('write');writes.push(payload);},requestQueueRefill:async()=>{order.push('refill');},
      callbackAlerts:{enqueue:async()=>{order.push('durable_enqueue');return 'awaiting_final';},settle:async()=>{order.push('settle_owner_email');return 'accepted';}}};
    assert.equal(await processPostCallOutcomeFromConversationId(id,options),true);
    assert.deepEqual(order,['durable_enqueue','write','settle_owner_email','refill']);
    assert.equal(writes[0].callResult,'callback_requested');assert.equal(writes[0].callbackRequested,'yes');assert.match(writes[0].callbackTime,/Wednesday around 3/);
    assert.notEqual(writes[0].callResult,'interested_followup_review');
  }finally{providerFixtures.delete(id);}
});

test("notification remains durable if owning-field persistence fails; the owner alert is not silently lost",async()=>{
  const {processPostCallOutcomeFromConversationId}=await import('../src/lib/elevenLabsPostCall');
  const f=fixture(),id='conv_callbackwritefailure',order:string[]=[];f.receipt.conversation_id=id;
  providerFixtures.set(id,f.receipt);
  try{
    await assert.rejects(processPostCallOutcomeFromConversationId(id,{postSheetUpdate:async()=>{order.push('write_failed');throw new Error('persistence unavailable');},
      requestQueueRefill:async()=>{throw new Error('No refill after failure');},callbackAlerts:{enqueue:async()=>{order.push('durable_enqueue');return 'awaiting_final';},settle:async()=>{order.push('settle_owner_email');return 'accepted';}}}),/persistence unavailable/);
    assert.deepEqual(order,['durable_enqueue','write_failed','settle_owner_email']);
  }finally{providerFixtures.delete(id);}
});

test("live callback route saves durable notification work before acknowledgment and never claims capture when that save fails",async()=>{
  const {createElevenLabsRouter}=await import('../src/routes/elevenLabs');
  for(const fail of [false,true]){
    const events:string[]=[],router=createElevenLabsRouter({enqueueCallbackAlert:async()=>{events.push('enqueue');if(fail)throw new Error('outbox unavailable');return 'awaiting_final';},
      postCallbackSheetUpdate:async()=>{events.push('sheet');}});
    const layer=router.stack.find((value:any)=>value.route?.path==='/tool/callback-requested');
    let response:any,error:any;
    const req={body:{rowNumber:123,callAttemptNumber:1,agentName:identity.agentName,phone:identity.phone,listingAddress:identity.listingAddress,
      conversationId:identity.conversationId,callbackTime:'Wednesday around 3'},header:(name:string)=>name==='x-crisp-elevenlabs-secret'?'synthetic':undefined};
    const res={status:()=>res,json:(body:any)=>{events.push('ack');response=body;}};
    await layer!.route.stack[0].handle(req,res,(value:any)=>{error=value;});
    if(fail){assert.equal(response,undefined);assert.match(error.message,/outbox unavailable/);assert.deepEqual(events,['enqueue']);}
    else{assert.equal(response.requestCaptured,true);assert.ok(events.indexOf('enqueue')<events.indexOf('ack'));assert.ok(events.includes('sheet'));}
  }
});

test("self-test requires authenticated matching owner recipient and cannot choose a different destination",async()=>{
  const {createCallbackAlertsRouter}=await import('../src/routes/callbackAlerts');let sends=0;
  const service={enqueue:async()=>'',settle:async()=>'',recover:async()=>{},activate:async()=>({activatedAt:'synthetic'}),health:async()=>({activatedAt:null,counts:{},needsReview:false,exceptions:[]}),selfTest:async()=>{sends++;return {status:'accepted',messageId:'synthetic'};}};
  const router=createCallbackAlertsRouter(service),auth=router.stack[0].handle;
  let status=0,next=0;const res={status:(code:number)=>{status=code;return res;},json:()=>{}};
  auth({header:()=>undefined} as never,res as never,()=>{next++;});assert.equal(status,401);assert.equal(next,0);
  const handler=router.stack.find((layer:any)=>layer.route?.path==='/self-test')!.route.stack[0].handle;
  await handler({body:{expectedRecipient:'unapproved@example.invalid'}} as never,res as never,()=>{});assert.equal(status,409);assert.equal(sends,0);
  await handler({body:{expectedRecipient:'owner@example.invalid'}} as never,res as never,()=>{});assert.equal(sends,1);
});

test("ordinary direct phone-callback requests are recognized without inventing consent from interest, titles or hypothetical speech",async()=>{
  const {getExplicitCallbackConsent}=await import('../src/lib/elevenLabsPostCall');
  for(const message of ['Can you call me at 3?','Could you call me Wednesday around 3?','Please call me.','I would like a callback.','Can I get a call back?','Would you give me a call tomorrow?']){
    assert.ok(getExplicitCallbackConsent({transcript:[{role:'user',message}]}),message);
  }
  for(const message of ['Call me a buyer.','Can I call you back?','If needed, could you call me back?','I want help.','I would like that.']){
    assert.equal(getExplicitCallbackConsent({transcript:[{role:'user',message}]}),undefined,message);
  }
  const retained=fixture();retained.receipt.transcript.push({role:'user',message:'Email me only the fee schedule, please.'});
  assert.ok(getExplicitCallbackConsent(retained.receipt),'restricting email contents is not cancelling an earlier callback');
});

test("new delivery activation does not reset on restart or replay historical completed calls",async()=>{
  const {createVoiceCallbackAlertService}=await import('../src/lib/voiceCallbackAlerts');const f=fixture();let activation:any;
  const deps={...f.deps,policy:async()=>activation,savePolicy:async(value:any)=>{activation=value;}};
  const s=createVoiceCallbackAlertService(deps);await s.enqueue(identity);assert.equal(await s.settle(identity.conversationId),'awaiting_activation');assert.equal(f.sent.length,0);
  const first=await s.activate();assert.equal((await createVoiceCallbackAlertService(deps).activate()).activatedAt,first.activatedAt);
  assert.equal(await s.settle(identity.conversationId),'held','pre-activation call requires duplicate review, never blind historical resend');assert.equal(f.sent.length,0);
});
