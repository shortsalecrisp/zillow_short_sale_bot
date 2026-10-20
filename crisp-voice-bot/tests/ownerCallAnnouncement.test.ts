import assert from "node:assert/strict";
import test from "node:test";
import { buildLiveTransferAnnouncement, buildReturnCallAnnouncement } from "../src/lib/ownerCallAnnouncement";
import { screenOfficeXml } from "../src/lib/returnCallFlow";

process.env.BASE_URL = "https://voice.example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12176341017";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

const row = (rowNumber = 4705, phone = "318-550-1079", address = "7123 CAPELLA CIR", first = "Loretta", last = "Butler") => ({
  rowNumber, values: [first, last, phone, "", address, "San Antonio", "TX"],
});

test("both owner prompts retain full name, street number, unit, city and state", () => {
  const context = { status: "matched" as const, agentName: "Loretta Butler", listingAddress: "7123 CAPELLA CIR, Unit 2, San Antonio, TX" };
  assert.equal(buildReturnCallAnnouncement(context), "Crisp return call. Caller number matches agent Loretta Butler. Listing: 7123 CAPELLA CIR, Unit 2, San Antonio, TX. Press 1 to connect.");
  assert.equal(buildLiveTransferAnnouncement(context), "Crisp live transfer from agent Loretta Butler. Listing: 7123 CAPELLA CIR, Unit 2, San Antonio, TX. Press 1 to accept.");
});

test("matching uses the exact normalized caller phone, never area code or a different current row", async () => {
  const { matchOwnerCallContext } = await import("../src/lib/ownerCallContext");
  assert.deepEqual(matchOwnerCallContext("+13185501079", [row()]), { status: "matched", agentName: "Loretta Butler", listingAddress: "7123 CAPELLA CIR, San Antonio, TX", rowNumber: 4705 });
  assert.equal(matchOwnerCallContext("+13185551000", [row()]).status, "unknown");
  assert.equal(matchOwnerCallContext("+13185501079", [row()], 4706).status, "unknown");
  assert.equal(matchOwnerCallContext("anonymous", [row()]).status, "unknown");
  assert.equal(matchOwnerCallContext("+13185501079", [row(4705, "+13185559999")]).status, "unknown");
});

test("same-phone duplicate identities and multiple properties are explicit, not silently selected", async () => {
  const { matchOwnerCallContext } = await import("../src/lib/ownerCallContext");
  assert.equal(matchOwnerCallContext("+13185501079", [row(), row(4706)]).status, "matched");
  const multiple = matchOwnerCallContext("+13185501079", [row(), row(4706, "3185501079", "88 Other St")]);
  assert.deepEqual(multiple, { status: "multiple_listings", agentName: "Loretta Butler" });
  assert.match(buildReturnCallAnnouncement(multiple), /Loretta Butler.*Multiple listings/);
  assert.doesNotMatch(buildReturnCallAnnouncement(multiple), /CAPELLA|Other St/);
  const ambiguous = matchOwnerCallContext("+13185501079", [row(), row(4706, "3185501079", "88 Other St", "Another", "Agent")]);
  assert.equal(ambiguous.status, "ambiguous");
  assert.doesNotMatch(buildReturnCallAnnouncement(ambiguous), /Loretta|Another/);
});

test("private XML escapes CRM text and never interprets it as call instructions", () => {
  const text = buildReturnCallAnnouncement({ status: "matched", agentName: "Anne & Joe", listingAddress: "12 <Dial> Street, City, FL" });
  const xml = screenOfficeXml(() => "https://voice.example.com/accept", text);
  assert.match(xml, /Anne &amp; Joe/);
  assert.match(xml, /12 &lt;Dial&gt; Street, City, FL/);
  assert.doesNotMatch(xml, /<Dial>/);
  assert.match(xml, /<\/Say><\/Gather><Hangup\/>/);
});

test("lookup failures and invalid caller IDs fail soft without using another agent", async () => {
  const { lookupOwnerCallContext } = await import("../src/lib/ownerCallContext");
  let reads = 0;
  const read = async () => { reads += 1; throw new Error("read failed"); };
  assert.equal((await lookupOwnerCallContext("anonymous", undefined, read)).status, "unknown");
  assert.equal(reads, 0);
  assert.equal((await lookupOwnerCallContext("+13185501079", 4705, read)).status, "unavailable");
  assert.equal(reads, 1);
  assert.match(buildReturnCallAnnouncement({ status: "unavailable" }), /could not be identified/);
});

test("live approval enriches a shortened bot payload only from its exact current row and phone", async () => {
  const { resolveLiveTransferOwnerContext, lookupOwnerCallContext } = await import("../src/lib/ownerCallContext");
  const input = { rowNumber: 4705, phone: "+13185501079", agentName: "Loretta", listingAddress: "7123 CAPELLA CIR" };
  const context = await resolveLiveTransferOwnerContext(input, (phone, number) => lookupOwnerCallContext(phone, number, async () => [row()]));
  assert.deepEqual(context, { agentName: "Loretta Butler", listingAddress: "7123 CAPELLA CIR, San Antonio, TX" });
  const fallback = await resolveLiveTransferOwnerContext(input, async () => ({ status: "unavailable" }));
  assert.deepEqual(fallback, { agentName: "Loretta", listingAddress: "7123 CAPELLA CIR" });
});
