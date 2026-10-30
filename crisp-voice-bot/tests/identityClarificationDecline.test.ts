import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_API_KEY = "test";
process.env.CALL_TRANSCRIPT_EMAILS_ENABLED = "false";

const transcript = (role="agent", refusal="Oh, no, I'm okay.") => [
  {role:"user",message:"Hello?"},
  {role,message:"Hi, this is Maya with Crisp Short Sales. Is 889 Cardona your listing?"},
  {role:"user",message:"Uh, yes."},
  {role,message:"Would help with lender paperwork or calls be useful for this listing?"},
  {role:"user",message:"Who is speaking with us?"},
  {role,message:"I'm Maya, an AI assistant with Crisp Short Sales."},
  {role:"user",message:refusal},
];
const done = (messages: ReturnType<typeof transcript>) => ({status:"done",metadata:{call_duration_secs:30,termination_reason:"Client disconnected: 1000"},transcript:messages});

test("a clear refusal after identity clarification answers the pending service question, not a generic hangup", async () => {
  const lib=await import("../src/lib/elevenLabsPostCall");
  for (const role of ["agent","assistant"]) for (const answer of ["Oh, no, I'm okay.","No, I'm good, thank you.","No thanks."]) {
    const c=done(transcript(role,answer));
    assert.equal(lib.shouldTreatAsNotInterested(c),true);
    assert.equal(lib.getAuthoritativeTerminalCallResult(c,2,{firstName:"Synthetic"}),"answered_not_interested");
    const direct=transcript(role,answer);direct.splice(4,2);
    assert.equal(lib.shouldTreatAsNotInterested(done(direct)),true,"Same clear refusal directly answers the service question");
  }
});

test("identity replies cannot borrow a refusal from a different question or from agreement", async () => {
  const lib=await import("../src/lib/elevenLabsPostCall");
  for(const answer of ["No, I'm okay with that.","Okay, tell me more.","No, I am okay with AI."]) {
    assert.equal(lib.shouldTreatAsNotInterested(done(transcript("agent",answer))),false,answer);
  }
  const other=transcript();other[5].message="I'm Maya with Crisp. Are you having trouble hearing me?";
  assert.equal(lib.shouldTreatAsNotInterested(done(other)),false);
  const noService=transcript();noService[3].message="Is this a good phone number for you?";
  assert.equal(lib.shouldTreatAsNotInterested(done(noService)),false);
});

test("later explicit callback, opt-out and deferred contact keep their existing priority", async () => {
  const lib=await import("../src/lib/elevenLabsPostCall");
  for(const answer of ["Can you call me back tomorrow?","Do not call me again.","I'll get back to you when I'm ready."]) {
    const c=done([...transcript(),{role:"user",message:answer}]);
    assert.equal(lib.shouldTreatAsNotInterested(c),false,answer);
  }
});
