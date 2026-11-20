import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {smsHarness} from './helpers/sms_core_harness.mjs';

const approved = JSON.parse(fs.readFileSync(new URL('./fixtures/sms_approved_opener.json', import.meta.url), 'utf8'));

for (const question of ['What is your fee?', 'How much does it cost?', 'Who pays you?', 'How do you get paid?', 'Does the seller pay anything?', 'Fee?']) {
  test(`first fee answer uses the exact new copy: ${question}`, () => {
    const h = smsHarness();
    const result = h.incoming(question);
    assert.equal(result.should_reply, true);
    assert.equal(result.handoff_needed, false);
    assert.equal(result.reply_text, approved.first_fee_reply);
    assert.equal(h.evaluate(`getDeliveredResponseId_(${JSON.stringify(result.reply_text)})`), 'fee_specific');
    assert.equal(h.evaluate(`getDeliveredResponseId_(${JSON.stringify('I work nationwide. ' + result.reply_text + ' Can we talk?')})`), 'fee_specific');
    assert.deepEqual(h.effects, []);
  });
}

test('new initial fallback matches the new opener without inventing a property address', () => {
  const h = smsHarness({initial_text: 'x', listing_address: ''});
  const result = h.incoming('Who is this?');
  assert.equal(result.reply_text, 'Sorry, I had messaged you earlier: ' + approved.template.replace('{first}', 'Taylor').replace(' reaching out about the listing on {address}', ''));
  assert.equal(h.evaluate(`getDeliveredResponseId_(${JSON.stringify(approved.template)})`), '');
});

test('initial identity fallback includes only the provided street, not city or state', () => {
  const h = smsHarness({initial_text: 'x', listing_address: '123 Example Lane, Atlanta, GA 30301'});
  const result = h.incoming('Who is this?');
  assert.equal(result.reply_text, 'Sorry, I had messaged you earlier: ' + approved.template.replace('{first}', 'Taylor').replace('{address}', '123 Example Lane'));
  assert.doesNotMatch(result.reply_text, /Atlanta|30301/);
});

test('new fee copy retains the delivered-amount repeat handoff', () => {
  const h = smsHarness();
  assert.equal(h.incoming('What is your fee?').reply_text, approved.first_fee_reply);
  const repeated = h.incoming('How much is your fee?');
  assert.equal(repeated.should_reply, false);
  assert.equal(repeated.handoff_needed, true);
  assert.equal(h.effects.filter(e => e.type === 'handoff').length, 1);
});

test('legacy fee receipts still count as delivered after the copy change', () => {
  const legacy = "My fee is $5,000, paid by the buyer at closing only if the deal closes. There's no service fee to you or the seller, and I don't take anything from your commission.";
  const h = smsHarness({last_outbound_text: legacy, history_json: JSON.stringify([{role: 'assistant', text: legacy}])});
  assert.equal(h.evaluate(`getDeliveredResponseId_(${JSON.stringify(legacy)})`), 'fee_specific');
  assert.equal(h.incoming('How much is your fee?').should_reply, false);
});

test('new copy does not bypass real takeover, opt-out or negotiation review', () => {
  const held = smsHarness({human_override: 'TRUE', handoff_flag: 'TRUE', ai_state: 'handoff'});
  assert.equal(held.incoming('What is your fee?').should_reply, false);
  const stopped = smsHarness();
  stopped.incoming('STOP');
  assert.equal(stopped.incoming('What is your fee?').should_reply, false);
  const negotiation = smsHarness().incoming('Can you reduce your fee to $2500?');
  assert.equal(negotiation.should_reply, false);
  assert.equal(negotiation.handoff_needed, true);
});
