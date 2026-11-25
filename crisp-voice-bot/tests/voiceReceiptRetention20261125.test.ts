import assert from "node:assert/strict";
import test from "node:test";
Object.assign(process.env,{BASE_URL:"https://example.invalid",TELNYX_API_KEY:"synthetic",TELNYX_CALLER_ID:"+12025550101",TELNYX_CONNECTION_ID:"synthetic",TELNYX_OUTBOUND_VOICE_PROFILE_ID:"synthetic",TEST_DESTINATION_NUMBER:"+12025550102",ELEVENLABS_API_KEY:"synthetic",ELEVENLABS_AGENT_ID:"agent_synthetic",GOOGLE_SHEETS_SPREADSHEET_ID:"synthetic",GOOGLE_SHEETS_TAB_NAME:"Synthetic",CALL_TRANSCRIPT_EMAILS_ENABLED:"false"});
const agent=(message:string)=>({role:"agent",message}),user=(message:string)=>({role:"user",message});
test("cell pressure preserves all structured measurements while compacting repeated boilerplate",async()=>{
 const {trimVoiceBotVoiceNotes}=await import('../src/lib/voiceSheet');
 const metrics=[1,2].map(attempt=>({schema:'voice_call_metrics_v1',call:{conversationId:'conv_'+attempt,callAttemptNumber:attempt,voiceId:attempt===1?null:'eryn'},providerIdentity:{versionId:'historical'},codexInstructions:'boilerplate '.repeat(4000),abTestScope:{cohort:'historical',analysisRule:'explanatory '.repeat(1000)}}));
 const result=trimVoiceBotVoiceNotes(metrics.map(m=>'CODEX_VOICE_CALL_METRICS_V1\n'+JSON.stringify(m)).join('\n'));
 assert.ok(result.length<=49000);
 for(const m of metrics){const parsed=result.split('\n').flatMap(line=>{try{return[JSON.parse(line)];}catch{return[];}}).find(x=>x.call?.conversationId===m.call.conversationId);assert.deepEqual(parsed.call,m.call);assert.deepEqual(parsed.providerIdentity,m.providerIdentity);assert.equal(parsed.abTestScope.cohort,'historical');}
 assert.ok(!result.includes('boilerplate boilerplate'));
 const oversized='CODEX_VOICE_RECEIPT '+JSON.stringify({protected:'evidence'.repeat(8000)});
 assert.throws(()=>trimVoiceBotVoiceNotes(oversized),/compact audit required/);
});
function fixture(result='contact_request_review',K=''){
 const row:any[]=Array(45).fill('');Object.assign(row,{0:'Synthetic',1:'Owner',2:'+12025550123',4:'123 Main St',5:'Miami',6:'FL',10:K,29:'queued',30:'voice_call_1_due',31:'2026-11-25T14:15:00Z',32:'2026-11-24T14:15:00Z',33:result});
 const receipt:any={conversation_id:'conv_synthetic',agent_id:'agent_synthetic',status:'done',metadata:{start_time_unix_secs:Date.parse(row[32])/1000,call_duration_secs:30,phone_call:{external_number:row[2],direction:'outbound'}},conversation_initiation_client_data:{dynamic_variables:{rowNumber:7000,callAttemptNumber:1,phone:row[2],testMode:false}},transcript:[agent('Is 123 Main St your listing?'),user('Take me off the list.')]};
 const sheets:any={spreadsheets:{values:{get:async({range}:any)=>({data:{values:range.includes('sms_inbound_queue')?[]:[[...row]]}}),batchUpdate:async({requestBody}:any)=>{for(const w of requestBody.data){const letters=w.range.match(/!([A-Z]+)/)[1];const col=[...letters].reduce((n:any,c:any)=>n*26+c.charCodeAt(0)-64,0);row[col-1]=w.values[0][0];}return{data:{}};}}}};
 const payload:any={callAttemptNumber:1,callResult:'do_not_call',leadStatusCode:'R',responseStatus:'Do not call',voiceTerminalReceipt:{conversationId:'conv_synthetic',phone:row[2],fullName:'Synthetic Owner',listingAddress:'123 Main St, Miami, FL'},voiceNotes:'Exact final receipt conv_synthetic reviewed'};
 return{row,receipt,sheets,payload};
}
test("the production writer settles a provisional review from a final opt-out without erasing callbacks",async()=>{
 const {updateVoiceLeadRow}=await import('../src/lib/updateVoiceLeadRow');const {row,receipt,sheets,payload}=fixture();
 await updateVoiceLeadRow(7000,payload,async()=>sheets,async()=>receipt);
 assert.equal(row[10],'R');assert.equal(row[33],'do_not_call');assert.deepEqual(row.slice(29,32),['','','']);assert.match(row[41],/verified_final_opt_out/);assert.equal(row[32],'2026-11-24T14:15:00Z');
 row[10]='';row[33]='contact_request_review';row[37]='yes';row[38]='tomorrow at two';row[30]='callback_due';
 await updateVoiceLeadRow(7000,payload,async()=>sheets,async()=>receipt);
 assert.equal(row[33],'contact_request_review');assert.equal(row[37],'yes');assert.equal(row[38],'tomorrow at two');assert.equal(row[30],'callback_due');
});
test("the final native transfer settles its own request flag but preserves later or unrelated preferences",async()=>{
 const {updateVoiceLeadRow}=await import('../src/lib/updateVoiceLeadRow');const {buildVoiceResponseStatus}=await import('../src/lib/elevenLabsPostCall');
 const {row,receipt,sheets,payload}=fixture('live_transfer_requested','Y');row[35]='yes';
 receipt.transcript=[agent('Would you like me to see if Yoni, our short-sale specialist, can join this call now?'),user('Yes.'),{role:'agent',tool_calls:[{tool_name:'live_transfer_requested'}],tool_results:[{tool_name:'live_transfer_requested',result_value:'{"accepted":true,"ok":true}'}]},{role:'agent',tool_calls:[{tool_name:'transfer_to_number'}],tool_results:[{tool_name:'transfer_to_number',result_value:'{"status":"success"}'}]}];
 await updateVoiceLeadRow(7000,{...payload,callResult:'warm_transfer_completed',leadStatusCode:'G',responseStatus:buildVoiceResponseStatus('warm_transfer_completed')},async()=>sheets,async()=>receipt);
 assert.equal(row[33],'warm_transfer_completed');assert.equal(row[10],'G');assert.equal(row[35],'yes');assert.equal(row[36],'yes');assert.match(row[9],/owner conversation unverified/);
 const {reviewedDispositionState}=await import('../src/lib/voiceReviewedDisposition');
 for(const i of [13,19,36,37,38]){const x=fixture('live_transfer_requested','Y').row;x[35]='yes';x[i]='yes';assert.throws(()=>reviewedDispositionState(x,1,'verified_completed_transfer'),/protected/);}
 const other=fixture('live_transfer_requested','Y').row;other[40]='callback_requested';assert.throws(()=>reviewedDispositionState(other,1,'verified_completed_transfer'),/protected/);
});
