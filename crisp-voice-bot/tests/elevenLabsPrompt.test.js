const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const prompt = fs.readFileSync(path.resolve(__dirname, "../docs/elevenlabs-agent-prompt.md"), "utf8");
function section(start, end) {
  const a = prompt.indexOf(start), b = prompt.indexOf(end, a + start.length);
  assert.ok(a >= 0 && b > a, "Prompt section anchors exist");
  return prompt.slice(a, b);
}
// These are static contract checks, not evidence of provider adherence or audible behavior.
test("live identity remains truthful and dynamic", () => {
  assert.match(prompt, /You are {{assistantName}}, an AI calling assistant for Crisp Short Sales/);
  assert.match(prompt, /Yes, I'm an AI assistant with Crisp Short Sales/);
  assert.doesNotMatch(prompt, /this is Emmy with Crisp Short Sales/i);
});
test("startup listens for a live greeting and separates intro from qualification", () => {
  const s = section("Opening delivery rule,", "If the caller corrects the name");
  assert.match(s, /Listen before introducing yourself/);
  assert.match(s, /remain silent/);
  assert.match(s, /Do not attach {{openerScript}} or a qualification question/);
  assert.match(s, /If their first live turn asks a question or gives a correction, answer that before/);
  assert.match(s, /do not ask for their identity again/i);
  assert.match(s, /two-variant test/);
  assert.match(s, /Do not repeat your name/);
});
test("clarification answers are bounded without an appended sales question", () => {
  const s = section("Clarification turn contract,", "Core behavior:");
  assert.match(s, /COMPLETE turn/);
  assert.match(s, /Do not append {{openerScript}}/);
  assert.match(s, /If the caller asks TWO questions, answer BOTH/);
  assert.match(s, /organize the documents the bank needs/);
  assert.match(s, /hearing-restoration response/);
  assert.match(s, /NOT a general acknowledgment/);
});
test("hearing repair does not claim technical repairs or infer consent", () => {
  assert.match(prompt, /Sorry, can you hear me now/);
  assert.match(prompt, /Do not combine[^\n]+or claim you fixed the audio or changed the volume/);
  assert.match(prompt, /Difficulty alone is not callback consent, rejection, or transfer consent/);
  assert.match(prompt, /An unclear yes to this choice is not consent/);
});
test("current-call end is distinct from suppression and rejection", () => {
  const s = section("If a live person only asks to end THIS call", "If they say they are not worried");
  assert.match(s, /CALL ENDED BY REQUEST: caller asked to end the current call only/);
  assert.match(s, /not permission for another automated call/);
  assert.match(s, /Do not erase earlier genuine interest/);
  assert.match(s, /Understood. Goodbye/);
});
test("explicit STOP and removal retain highest-priority opt-out handling", () => {
  const s = section('If a live person says "do not call"', "If a live person only asks to end THIS call");
  assert.match(s, /standalone "stop"/);
  assert.match(s, /remove me from your list/);
  assert.match(s, /DO NOT CALL: caller explicitly requested no further calls/);
  assert.match(s, /priority over every pitch/);
  assert.doesNotMatch(s, /We won't call again/);
});
test("recordings cannot create human consent or outcomes", () => {
  assert.match(prompt, /Recording\/automated-system gate, highest priority/);
  assert.match(prompt, /Never call `callback_requested`, `information_requested`, `not_interested`, or `live_transfer_requested` from an automated/);
  assert.match(prompt, /Canned fragments/);
});
test("screening answers once then waits for a new live person", () => {
  assert.match(prompt, /This is {{assistantName}} calling from Crisp Short Sales about your listing at {{streetAddress}}/);
  assert.match(prompt, /After you have spoken that sentence, stay quiet and keep the call open/);
  assert.match(prompt, /Do not call `end_call` while you are being transferred/);
});
test("voicemail and wrong-person protections remain", () => {
  assert.match(prompt, /Wrong-person or unrelated-business voicemail hard stop/);
  assert.match(prompt, /do not call `voicemail_detection` while the mailbox greeting is still mid-sentence/);
  assert.match(prompt, /do not leave the normal voicemail/);
  assert.match(prompt, /{{callAttemptNumber}}/);
});
test("noise does not become a name, consent or a callback", () => {
  assert.match(prompt, /Do not guess the speaker's name from the noisy turn/);
  assert.match(prompt, /Do not treat a single yes, sure, or okay inside that noisy turn as consent/);
  assert.match(prompt, /If the latest caller message is exactly "..."/);
  assert.match(prompt, /Do not finish the sentence over them/);
});
test("the active person's correction outranks the lead name", () => {
  const s = section("If the caller corrects the name", "If they ask which listing");
  assert.match(s, /Treat the current speaker as the agent/);
  assert.match(s, /Do not ask to speak with `{{firstName}}`/);
  assert.match(s, /Do not repeat a handling question they already answered/);
});
test("cost is transparent and unsupported claims stay prohibited", () => {
  assert.match(prompt, /There is no charge to you or the seller. The buyer typically pays a flat fee only if the deal closes/);
  assert.match(prompt, /It can affect the buyer's total budget/);
  assert.match(prompt, /Do not invent a fee amount, guaranteed approval or closing/);
  assert.match(prompt, /I don't have a verified answer on that/);
  assert.doesNotMatch(prompt, /\$5,?000/);
});
test("soft no and self-handling do not suppress new questions", () => {
  assert.match(prompt, /Do not include "I'm handling it myself"/);
  assert.match(prompt, /If they ask any question after this/);
  assert.match(prompt, /answer it instead of calling `not_interested`/);
  assert.match(prompt, /Curiosity, a polite acknowledgment, or understanding the explanation is not by itself a request/);
});
test("information-only and self-initiated contact stay out of callbacks", () => {
  assert.match(prompt, /call `information_requested`/);
  assert.match(prompt, /DEFERRED CONTACT: caller said they will initiate future contact/);
  assert.match(prompt, /This is not a request for Yoni or Crisp to call them/);
  assert.doesNotMatch(prompt, /callbackTime` set to `send info/);
});
test("callbacks require consent and do not promise a booked appointment", () => {
  assert.match(prompt, /Being busy, hesitant, or saying "not now" is not callback consent/);
  assert.match(prompt, /This is a callback request, not a confirmed appointment/);
  assert.match(prompt, /I couldn't confirm that request/);
  assert.doesNotMatch(prompt, /I set up the callback with Yoni|I will text him/);
});
test("transfer consent can change and new questions must be handled", () => {
  assert.match(prompt, /While a transfer is being checked, consent can still change/);
  assert.match(prompt, /Do not proceed from a stale approval/);
  assert.match(prompt, /A failed transfer alone is not an ending or callback instruction/);
  assert.doesNotMatch(prompt, /decision is locked in|Do not answer new questions/);
});
