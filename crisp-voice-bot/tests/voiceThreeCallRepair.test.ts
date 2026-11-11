import assert from "node:assert/strict";
import test from "node:test";
Object.assign(process.env, { BASE_URL:"https://example.com", TELNYX_API_KEY:"test", TELNYX_CALLER_ID:"+12175550100",TELNYX_CONNECTION_ID:"test",TELNYX_OUTBOUND_VOICE_PROFILE_ID:"test",TEST_DESTINATION_NUMBER:"+12175550101",GOOGLE_SHEETS_TAB_NAME:"Synthetic" });
const menu = "If you're satisfied with the message, press one. To listen to your message, press two. To erase and rerecord, press three. To continue recording where you left off, press four.";
const tami = { status:"done", has_user_audio:true, metadata:{call_duration_secs:72},transcript:[
  {role:"user",message:"9515016155 is not available."},
  {role:"agent",message:"He isn't available right now. Would you like me to request a callback?"},
  ...[20,37,53].map(time=>({role:"user",message:menu,time_in_call_secs:time})),
  {role:"user",message:"Sorry you're having trouble. Your message has been sent. Please try again later. Goodbye."},
] };
const janese = {status:"done",has_user_audio:true,metadata:{call_duration_secs:43},transcript:[
  {role:"user",message:"Hello, this is Denise."},
  {role:"agent",message:"Is 10968 Highridge Court your listing?"},{role:"user",message:"Yes."},
  {role:"agent",message:"Would help with lender paperwork or calls be useful for this listing?"},{role:"user",message:"Yes."},
  {role:"agent",message:"We help collect the documents and follow up with the lender, so you can stay focused on the sale. Would you like me to see if Yoni, our short-sale specialist,..."},
  {role:"user",message:"I'd like that."},{role:"agent",message:"Ok, hold on, let me see if he's available one second. Please hold while I connect you to ..."},
  {role:"user",message:"Hello? Hey, hey, this Michael?"},
]};
test("terminal voicemail menu and canned sent announcement cannot become a live hangup/callback", async()=>{
  const c=await import('../src/lib/elevenLabsPostCall');
  assert.equal(c.getAuthoritativeTerminalCallResult(tami,2),'voicemail_reached_final_attempt');
  assert.equal(c.getExplicitCallbackConsent(tami),undefined);
  assert.equal(c.getVoiceContactRequestResult(tami),undefined);
  assert.equal(c.getVoicemailOrNoAnswerCallResult(tami,1),'voicemail_reached');
  for(const message of ["This is Tami. Please call me back tomorrow at 3.","This is Tami. What is this about?","Please do not call me again."]){
    const live={...tami,transcript:[...tami.transcript,{role:'user',message}]};
    assert.equal(c.shouldTreatAsVoicemail(live),false,'actual later speech supersedes menu');
  }
});
test("missing live connection is review evidence, never fabricated callback or completed transfer",async()=>{
  const c=await import('../src/lib/elevenLabsPostCall');
  assert.equal(c.getMissingHandoffReview(janese)?.evidence,'interrupted_offer');
  assert.equal(c.getExplicitCallbackConsent(janese),undefined);
  const complete=structuredClone(janese); complete.transcript[5].message="Would you like me to see if Yoni, our short-sale specialist, can join this call now?";
  assert.equal(c.getMissingHandoffReview(complete)?.evidence,'explicit_live_now');
  for(const message of ['No thanks.','Do not call me again.','Can you call me back tomorrow?','Cancel.','I will call you back.']){
    assert.equal(c.getMissingHandoffReview({...complete,transcript:[...complete.transcript,{role:'user',message}]}),undefined,message);
  }
  for(const reply of ['Yes, later.','Yes, but wait.','I am busy.','What?','Yes or no?']){
    const changed=structuredClone(complete);changed.transcript[6].message=reply;
    assert.equal(c.getMissingHandoffReview(changed),undefined,reply);
  }
  assert.equal(c.getMissingHandoffReview({...complete,status:'in-progress'}),undefined);
});
test("owner review is durable, coalesced, identity guarded and uncertain mail is not resent",async()=>{
  const {notifyMissingHandoffReview}=await import('../src/lib/voiceHandoffReviewAlert');
  const input={conversationId:'conv_synthetic_missing',metadata:{rowNumber:123,fullName:'Synthetic Owner',requestedPhone:'+12025550123',dialedPhone:'+12025550123',listingAddress:'123 Main St',callAttemptNumber:1,testMode:false},review:{offer:'Would you like Yoni on this call now?',reply:"I'd like that.",evidence:'explicit_live_now'},transcript:'Synthetic transcript'} as const;
  let state:any, sent=0;
  const deps={current:async()=>true,read:async()=>state,write:async(_key:string,next:any)=>{state={...next};},playback:()=> 'https://example.com/playback',send:async(message:any)=>{sent++;assert.match(message.text,/NOT a scheduled callback/);assert.doesNotMatch(message.text,/Scheduled Time:/);}};
  await Promise.all([notifyMissingHandoffReview(input as never,deps),notifyMissingHandoffReview(input as never,deps)]);
  assert.equal(sent,1);assert.equal(state.status,'sent');
  assert.equal(await notifyMissingHandoffReview(input as never,deps),'already_sent');
  state=undefined;
  await assert.rejects(notifyMissingHandoffReview(input as never,{...deps,send:async()=>{sent++;throw new Error('SMTP uncertain');}}),/SMTP uncertain/);
  assert.equal(state.status,'uncertain');await notifyMissingHandoffReview(input as never,deps);assert.equal(sent,2);
  state=undefined;assert.equal(await notifyMissingHandoffReview(input as never,{...deps,current:async()=>false}),'protected_or_changed');assert.equal(sent,2);
});
test("reviewed second-call menu correction preserves K and requires both receipt proof and unchanged safety",async()=>{
  const {reviewedDispositionState,verifyReviewedDisposition}=await import('../src/lib/voiceReviewedDisposition');
  const row=Array(45).fill('');Object.assign(row,{0:'Synthetic',1:'Owner',2:'+12025550123',10:'N',32:'2026-11-09T23:01:24Z',33:'no_answer_first_attempt',39:'2026-11-10T17:18:12Z',40:'call_received_agent_hung_up'});
  assert.deepEqual(await verifyReviewedDisposition(row,2,'verified_terminal_voicemail_menu',tami),{callResult:'voicemail_reached_final_attempt',leadStatusCode:'N',responseStatus:'Voicemail reached on final attempt - message not confirmed'});
  for(const index of [13,16,19,28,35,36,37,38,43,44]){const held=[...row];held[index]='yes';assert.throws(()=>reviewedDispositionState(held,2,'verified_terminal_voicemail_menu'),/protected/);}
  const changed=[...row];changed[10]='Y';assert.throws(()=>reviewedDispositionState(changed,2,'verified_terminal_voicemail_menu'),/scope/);
});
