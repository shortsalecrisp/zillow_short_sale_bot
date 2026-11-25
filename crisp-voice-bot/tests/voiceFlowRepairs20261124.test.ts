import assert from "node:assert/strict";
import test from "node:test";
Object.assign(process.env,{BASE_URL:"https://example.invalid",TELNYX_API_KEY:"synthetic",TELNYX_CALLER_ID:"+12025550101",TELNYX_CONNECTION_ID:"synthetic",TELNYX_OUTBOUND_VOICE_PROFILE_ID:"synthetic",TEST_DESTINATION_NUMBER:"+12025550102",ELEVENLABS_API_KEY:"synthetic",CALL_TRANSCRIPT_EMAILS_ENABLED:"false"});
const agent=(message:string)=>({role:"agent",message}),user=(message:string)=>({role:"user",message});
const receipt=(transcript:Array<{role:string;message:string}>)=>({status:"done",has_user_audio:true,metadata:{call_duration_secs:106,termination_reason:"Client disconnected: 1000"},transcript});
test("callback timing preserves Standard and Daylight without inventing consent or a zone",async()=>{
 const {getExplicitCallbackConsent}=await import("../src/lib/elevenLabsPostCall");
 for(const timing of ["Tomorrow morning, 10:00 a.m., Eastern Standard.","Tomorrow morning, ten o'clock a.m., Eastern Standard.","Tuesday at 3 Pacific Daylight Time.","Tomorrow at 4 Central Standard Time."]){
 const c=receipt([agent("Would you like me to request a callback?"),user("Yes."),agent("What time would you like to request?"),user(timing),agent("Thanks. I've received your callback request.")]);
 const expected=timing.replace(/[.!?]+$/,"");
 assert.equal(getExplicitCallbackConsent(c)?.callbackTime,expected);
 assert.equal(getExplicitCallbackConsent(c)?.timingSource,"caller");
 for(const change of ["Cancel that callback.","Don't call me again.","Just email me instead.","I will call you back."]){assert.equal(getExplicitCallbackConsent({...c,transcript:[...c.transcript,user(change)]}),undefined);}
 }
 const noConsent=receipt([agent("What time would you like?"),user("Tomorrow morning at ten Eastern Standard.")]);
 assert.equal(getExplicitCallbackConsent(noConsent),undefined);
 const incidental=receipt([user("Please call me tomorrow at two."),agent("Anything else?"),user("I have a showing at ten Eastern Standard.")]);
 assert.equal(getExplicitCallbackConsent(incidental)?.callbackTime,"tomorrow at two");
});
test("exact Cally final sequence retains timing after callback offer and closing question",async()=>{
 const {getExplicitCallbackConsent}=await import("../src/lib/elevenLabsPostCall");
 const c=receipt([user("This is K- Hello?"),agent("Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is Twenty Nine Twenty Three Four FEDORA your listing?"),user("Is this AI or am I talking to a person?"),agent("I'm already an AI assistant with Crisp Short Sales."),user("Okay. I need to speak to a real person, please."),agent("Totally fair. Would you like me to see if Yoni, our short-sale specialist, can join this call now?"),user("Sure."),agent("Please hold while I connect you to Yoni."),agent("Sorry, I couldn't connect him. Would you like me to request a callback?"),user("Yes."),agent("What time would you like to request?"),user("Tomorrow morning, 10:00 a.m., Eastern Standard."),agent("Thanks. I've received your callback request."),user("So are we done?"),agent("Yes, your request has been received. If you have any other questions, just let me know."),user("No, thanks.")]);
 assert.equal(getExplicitCallbackConsent(c)?.callbackTime,"Tomorrow morning, 10:00 a.m., Eastern Standard");
});
test("a contract would be useful is a scoped negative needs answer, not a generic hangup",async()=>{
 const {getAuthoritativeTerminalCallResult,shouldTreatAsNotInterested}=await import("../src/lib/elevenLabsPostCall");
 const prefix=[user("Hello, this is Greta."),agent("Is Fourteen Eighteen Sheffield your listing?"),user("It is."),agent("Would help with lender paperwork or calls be useful for this listing?")];
 for(const answer of ["No, a contract would be useful.","No, a buyer would be helpful.","No, a signed contract would be useful. Thanks."]){
 const c=receipt([...prefix,user(answer),agent("Understood. Thanks for letting me know."),user("You're welcome."),agent("Thanks again.")]);
 assert.equal(shouldTreatAsNotInterested(c),true);assert.equal(getAuthoritativeTerminalCallResult(c,1,{firstName:"Greta"}),"answered_not_interested");
 }
 for(const answer of ["No, can you find a buyer?","A contract would be useful.","No, can you call tomorrow?","No, can you email me?"]){
 const c=receipt([...prefix,user(answer)]);assert.equal(shouldTreatAsNotInterested(c),false,answer);
 }
 const otherQuestion=receipt([agent("Would you like me to see if Yoni can join?"),user("No, a contract would be useful.")]);
 assert.equal(shouldTreatAsNotInterested(otherQuestion),false);
 const recording=receipt([user("This is a recorded greeting: No, a contract would be useful. Please leave your message after the beep.")]);
 assert.equal(shouldTreatAsNotInterested(recording),false);
});
test("explicit soft decline and repeated automated menu require separate final outcomes", async () => {
 const {getAuthoritativeTerminalCallResult} = await import("../src/lib/elevenLabsPostCall");
 const decline = receipt([user("Hi, this is Angie."), agent("Is this your listing?"), user("Yes, it is."),
   agent("Would help with lender paperwork or calls be useful for this listing?"),
   user("Um, no, I think we're good."), agent("Understood. Thanks for letting me know.")]);
 assert.equal(getAuthoritativeTerminalCallResult(decline, 1, {firstName:"Angie"}), "answered_not_interested");
 const menu = user("For agent support, press two. For accounting, dial three.");
 assert.equal(getAuthoritativeTerminalCallResult(receipt([menu, menu]), 1, {}), "repeated_ivr_review");
 assert.notEqual(getAuthoritativeTerminalCallResult(receipt([menu, menu, user("This is Angie.")]), 1, {}), "repeated_ivr_review");
});
