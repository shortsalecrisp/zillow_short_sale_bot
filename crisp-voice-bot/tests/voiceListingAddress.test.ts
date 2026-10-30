import assert from "node:assert/strict";
import test from "node:test";
import { assertVoiceListingAddress, voiceListingAddressBlockReason } from "../src/lib/voiceListingAddress";
import { formatVoiceAttemptIdentity } from "../src/lib/voicePhoneIdentity";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_ERYN_VOICE_ID = "eryn-test";

const contaminated = [
  "Verified 2026-10-28: Compass listing identifies Joe Osborne with 303-532-8400 and joe@example.com. Source: agent profile.",
  "Verified 2026-10-28: active exact 3400 Lazy Ln lists Justin Scott; no same-phone owner found.",
  "151 Grant St https://example.com/agent",
  "151 Grant St\nContact verification note",
  "151 Grant St, contact@example.com",
];

test("listing guard rejects verification prose, links, emails and multiline data without parsing an address from it", () => {
  for (const value of contaminated) {
    assert.equal(voiceListingAddressBlockReason(value), "listing_address_contains_contact_notes");
    assert.throws(() => assertVoiceListingAddress(value), /held for review/);
  }
  assert.equal(voiceListingAddressBlockReason(" "), "missing_listing_address");
  for (const address of ["151 Grant St", "3400 Lazy Ln, Durant, OK", "123 Verified Lane", "TBD County Road 74", "21-23 Main St #2", "89 St. John's Rd"]) {
    assert.equal(voiceListingAddressBlockReason(address), undefined, address);
  }
});

test("contaminated addresses cannot become queue candidates or provider payloads; history/preferences stay unchanged", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  const { buildElevenLabsOutboundCallBody } = await import("../src/lib/elevenLabs");
  const row: unknown[] = Array(45).fill("");
  Object.assign(row, { 0: "Synthetic", 1: "Agent", 2: "+12025550123", 4: "151 Grant St", 5: "Tampa", 6: "FL", 8: "x",
    29: "yes", 30: "voice_call_2_due", 31: "2026-10-30T13:15:00Z", 32: "2026-10-29T19:01:00Z", 33: "voicemail_left" });
  row[41] = formatVoiceAttemptIdentity(7000, String(row[2]), 1, new Date(String(row[32])));
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, row, new Date(String(row[31])))?.callAttemptNumber, 2);
  for (const address of contaminated) {
    row[4] = address;
    const before = structuredClone(row);
    assert.equal(getVoiceBotCallCandidateFromRowValues(7000, row, new Date(String(row[31]))), undefined);
    assert.deepEqual(row, before);
    assert.throws(() => buildElevenLabsOutboundCallBody({agentId:"test",agentPhoneNumberId:"test",to:"+12025550123",metadata:{
      rowNumber:7000,fullName:"Synthetic Agent",callAttemptNumber:1,listingAddress:address,requestedPhone:"+12025550123",dialedPhone:"+12025550123",testMode:true,
    }}), /listing address held for review/);
  }
  row[4] = "151 Grant St";
  for (const [i,value] of [[10,"R"],[19,"x"],[37,"yes"],[38,"tomorrow afternoon"]] as const) {
    row[i] = value;
    assert.equal(getVoiceBotCallCandidateFromRowValues(7000, row, new Date(String(row[31]))), undefined);
    row[i] = "";
  }
});

test("start-call route rejects contaminated listing data before any dialing path", async () => {
  const express = (await import("express")).default;
  const router = (await import("../src/routes/startCall")).default;
  const app = express(); app.use(express.json()); app.use("/start-call", router);
  app.use((error: Error & {statusCode?:number}, _req: unknown, res: any, _next: unknown) => res.status(error.statusCode ?? 500).json({error:error.message}));
  const server = app.listen(0, "127.0.0.1");
  await new Promise<void>(resolve => server.once("listening",resolve));
  try {
    const address = server.address() as {port:number};
    const response = await fetch(`http://127.0.0.1:${address.port}/start-call`,{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({
      rowNumber:7000,phone:"+12025550123",listingAddress:contaminated[0],scheduledWindow:"reach_morning_v1",agentTimeZone:"America/New_York",
    })});
    assert.equal(response.status,400);
    assert.match((await response.json()).error,/listingAddress requires review/);
  } finally { await new Promise<void>((resolve,reject)=>server.close(error=>error?reject(error):resolve())); }
});
