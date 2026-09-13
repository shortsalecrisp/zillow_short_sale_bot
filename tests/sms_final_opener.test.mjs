import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {smsHarness} from './helpers/sms_core_harness.mjs';

const {template} = JSON.parse(fs.readFileSync(new URL('./fixtures/sms_approved_opener.json', import.meta.url), 'utf8'));
const opener = template.replace('{first}', 'Taylor').replace('{address}', '123 Example Lane');

for (const address of ['5000 Example Lane', '123 Main St Unit 5000']) {
  test(`listing numbers are not delivered fee evidence: ${address}`, () => {
    const text = template.replace('{first}', 'Taylor').replace('{address}', address);
    const h = smsHarness({initial_text: text, last_outbound_text: text,
      history_json: JSON.stringify([{role: 'assistant', text}])});
    assert.equal(h.evaluate(`getDeliveredResponseId_(${JSON.stringify(text)})`), '');
    assert.equal(h.evaluate(`getDeliveredResponseId_(${JSON.stringify('Sorry, I had messaged you earlier: ' + text)})`), '');
    const r = h.incoming('What is your fee?');
    assert.equal(r.should_reply, true);
    assert.equal(r.handoff_needed, false);
    assert.match(r.reply_text, /My fee is \$5,000/);
  });
}

test('identity fallback uses the exact approved final opener', () => {
  const r = smsHarness().incoming('Who is this?');
  assert.equal(r.should_reply, true);
  assert.equal(r.reply_text, 'Sorry, I had messaged you earlier: ' + opener);
});

test('identity resend repeats the original sent text even after the template changes', () => {
  const original = 'Hey Taylor, this is Yoni Kutler with Crisp Short Sales. I saw your short sale at 123 Example Lane. Are you handling that part yourself or do you already have help?';
  const r = smsHarness({initial_text: original}).incoming('Who is this?');
  assert.equal(r.reply_text, 'Sorry, I had messaged you earlier: ' + original);
});

test('identity fallback without a listing never invents an address', () => {
  const r = smsHarness({listing_address: '', initial_text: 'x'}).incoming('Who is this?');
  assert.match(r.reply_text, /I can take the lender paperwork/);
  assert.doesNotMatch(r.reply_text, /I saw|undefined|null|\{address\}/);
});

test('new opener is not counted as a delivered fee answer or self-handling pitch', () => {
  const h = smsHarness({initial_text: opener, last_outbound_text: opener,
    history_json: JSON.stringify([{role: 'assistant', text: opener}])});
  assert.equal(h.evaluate(`getDeliveredResponseId_(${JSON.stringify(opener)})`), '');
  const r = h.incoming("I'm handling it myself thank you");
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /You keep the listing and client relationship/);
  const payer = smsHarness({initial_text: opener, last_outbound_text: opener}).incoming('Who pays you?');
  assert.doesNotMatch(payer.reply_text, /5,000/);
  const amount = smsHarness({initial_text: opener, last_outbound_text: opener}).incoming('What is your fee?');
  assert.match(amount.reply_text, /5,000/);
});

for (const text of ['Who pays you?', 'Does the seller pay anything?', 'Do you take any of my commission?']) {
  test(`distinct payer question after amount is answered: ${text}`, () => {
    const h = smsHarness();
    h.incoming('What is your fee?');
    const r = h.incoming(text);
    assert.equal(r.should_reply, true);
    assert.equal(r.handoff_needed, false);
    assert.match(r.reply_text, /no cost to you or the seller/);
    assert.match(r.reply_text, /commission/);
    assert.match(r.reply_text, /buyer at closing/);
  });
}

test('repeated amount still hands off and distinct payment questions do not bypass the cap', () => {
  const h = smsHarness();
  h.incoming('What is your fee?');
  const repeated = h.incoming('How much is your fee?');
  assert.equal(repeated.should_reply, false);
  assert.equal(repeated.handoff_needed, true);
  const capped = smsHarness({auto_reply_count: 3});
  const r = capped.incoming('Who pays you?');
  assert.equal(r.should_reply, false);
  assert.equal(r.handoff_needed, true);
});

test('buyer requests do not promise a buyer-finding service', () => {
  for (const text of ['Do you have any buyers?', 'I just need a buyer', 'Send me buyers']) {
    const r = smsHarness().incoming(text);
    assert.equal(r.should_reply, true);
    assert.match(r.reply_text, /don't bring the buyer/);
    assert.match(r.reply_text, /processing with the bank/);
    assert.doesNotMatch(r.reply_text, /help you find|necessarily|expedite/);
  }
});

test('legacy or generated buyer-finding promises become scope clarification, not rejection', () => {
  const h = smsHarness();
  for (const text of ["I don't necessarily have a buyer but I can help you find a buyer.", 'I can send you buyer leads.']) {
    const safe = h.evaluate(`sanitizeReplyBuyerOffer_(${JSON.stringify(text)})`);
    assert.match(safe, /don't bring the buyer/);
    assert.doesNotMatch(safe, /help you find|Ok, no problem/);
  }
});

test('payer clarification remains one answer and never escapes existing human ownership', () => {
  const h = smsHarness();
  h.incoming('What is your fee?');
  assert.equal(h.incoming('Who pays you?').should_reply, true);
  const repeated = h.incoming('Who pays you?');
  assert.equal(repeated.should_reply, false);
  assert.equal(repeated.handoff_needed, true);
  const locked = smsHarness({human_override: 'TRUE', handoff_flag: 'TRUE', ai_state: 'handoff'});
  assert.equal(locked.incoming('Does the seller pay anything?').should_reply, false);
});

test('Equator scope is bounded and does not override a refusal', () => {
  const r = smsHarness().incoming('Are you familiar with Equator?');
  assert.equal(r.reply_text, "I'm familiar with Equator and can help manage the lender-side tasks and communication.");
  const rejection = smsHarness().incoming('No thanks, I handle Equator myself.');
  assert.equal(rejection.lead_status, 'R');
  assert.match(rejection.reply_text, /^Ok, no problem/);
});
