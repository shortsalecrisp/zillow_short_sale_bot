import assert from 'node:assert/strict';
import test from 'node:test';
Object.assign(process.env, { BASE_URL:'https://example.com', TELNYX_API_KEY:'test', TELNYX_CALLER_ID:'+12175550100',
  TELNYX_CONNECTION_ID:'test', TELNYX_OUTBOUND_VOICE_PROFILE_ID:'test', TEST_DESTINATION_NUMBER:'+12175550101',
  ELEVENLABS_API_KEY:'test', GOOGLE_APPS_SCRIPT_WEBHOOK_URL:'https://example.invalid/relay' });
const turn = (role:string,message:string) => ({role,message});
const done = (transcript: ReturnType<typeof turn>[]) => ({status:'done',metadata:{call_duration_secs:85,termination_reason:'Ending conversation after 45 seconds of silence.'},transcript});
test('Maria explicit all-good decline is terminal only after the service question', async()=>{
  const lib=await import('../src/lib/elevenLabsPostCall');
  for(const text of ["No, we're all good on it. Thank you.", 'We are all good on the file. Thanks.']) {
    const c=done([turn('agent','Would help with lender paperwork or calls be useful for this listing?'),turn('user',text)]);
    assert.equal(lib.getAuthoritativeTerminalCallResult(c,1), 'answered_not_interested');
  }
  assert.equal(lib.shouldTreatAsNotInterested(done([turn('agent','Is the audio working?'),turn('user',"We're all good.")])),false);
});
test('Ana IVR echo cannot be promoted to live review; later human pickup survives',async()=>{
  const lib=await import('../src/lib/elevenLabsPostCall');
  const c=done([turn('user','Thank you for calling Benchmark McCabe Mortgage Group. If you know your party extension, dial it now. For Tina dial 43. Dial zero for assistance.'),
    turn('user','Recording.'),turn('agent','(No action needed—waiting silently, as this is a recording and not a live person.)'),
    turn('user','No action needed. Waiting silently as this is a recording and not a live person.'),
    turn('agent',"Understood. I'll remain silent and wait for a live person or further instructions."),
    turn('user',"Understood. I'll remain silent and wait for a live person or further instructions.")]);
  assert.equal(lib.hasTerminalNonleadPlayback(c),true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(c,1,{firstName:'Ana'}),'agent_not_available');
  c.transcript.push(turn('user','This is Ana.'),turn('agent','Is South Street your listing?'),turn('user','Yes, that is my listing.'));
  assert.equal(lib.hasTerminalNonleadPlayback(c),false);
});
test('uninterrupted financial broadcast is unavailable, financing discussion remains live',async()=>{
  const lib=await import('../src/lib/elevenLabsPostCall');
  const broadcast="My name is William Antebon, the junior here, and I came here to yield a few basis points. Hello. Hey, Louis, in energy, it's crude and Brent more than 3% each. I hope that movements are slowly starting to recover, and we continue to see in Vegas, home has found another 4% today, 23% so far in the month of November, just a few dollars above the low it made on Thursday. We're examining every angle of the NVIDIA story.";
  const c=done([turn('user',broadcast),turn('agent','Are you calling about short sales?')]);
  assert.equal(lib.getAuthoritativeTerminalCallResult(c,1,{firstName:'Daryl'}),'agent_not_available');
  c.transcript.push(turn('user','Yes, this is my listing. I need lender help.'));
  assert.equal(lib.hasTerminalNonleadPlayback(c),false);
});
test('assistant to named target at brokerage is gatekeeper completion',async()=>{
  const lib=await import('../src/lib/elevenLabsPostCall');
  const c=done([turn('user','Hi, this is Alex, assistant to Ray Meyer at Premier Partners Group. I can help with questions about buying or selling real estate or booking a call with Ray. Would you like to proceed?'),
    turn('agent','I am calling about short-sale lender paperwork for Raymond.'),turn('user','Could you share your best callback number?'),
    turn('agent',"Yoni's direct number is 404-300-9526."),turn('user','Ray or someone on the team will get back to you. Have a great day.')]);
  assert.equal(lib.getAuthoritativeTerminalCallResult(c,1,{firstName:'Raymond'}),'gatekeeper_admin_early_exit');
  assert.equal(lib.getExplicitCallbackConsent(c),undefined);
});
test('Darcy filler before explicit time refines existing callback consent without inventing consent',async()=>{
  const lib=await import('../src/lib/elevenLabsPostCall');
  const c=done([turn('agent','Would you like me to have Yoni call you back?'),turn('user','Yes.'),
    turn('agent','What day and time works best?'),turn('user','Mm. Today at 4:00 p.m.')]);
  assert.equal(lib.getExplicitCallbackConsent(c)?.callbackTime,'Today at 4:00 p.m');
  assert.equal(lib.getExplicitCallbackConsent(done(c.transcript.slice(2))),undefined);
  c.transcript.push(turn('user','Cancel the callback.'));
  assert.equal(lib.getExplicitCallbackConsent(c),undefined);
});
test('verified protected receipt acknowledges persistence; empty write still fails',async()=>{
  const lib=await import('../src/lib/elevenLabsPostCall');
  const payload={rowNumber:6907,callAttemptNumber:1,callResult:'callback_requested',voiceTerminalReceipt:{conversationId:'conv_synthetic',phone:'+12025550123',fullName:'Synthetic Owner',listingAddress:'123 Test St'}};
  let relay=0;
  await lib.persistVoiceContactOutcome(payload,{directSheetUpdate:async()=>['AP:voiceTerminalReceipt_verified'],appsScriptPost:async()=>{relay++;throw new Error('relay forbidden');}});
  assert.equal(relay,0);
  await assert.rejects(lib.persistVoiceContactOutcome(payload,{directSheetUpdate:async()=>[]}),/not acknowledged/);
  await assert.rejects(lib.persistVoiceContactOutcome({...payload,voiceTerminalReceipt:undefined},{directSheetUpdate:async()=>['AP:voiceTerminalReceipt_verified'],appsScriptPost:async()=>{throw new Error('relay failed');}}),/not acknowledged/);
});
