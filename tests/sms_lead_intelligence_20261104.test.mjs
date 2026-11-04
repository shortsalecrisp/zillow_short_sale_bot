import assert from 'node:assert/strict';
import test from 'node:test';
import { smsHarness } from './helpers/sms_core_harness.mjs';

test('under-contract and MLS updates are acknowledged as status updates, not existing help', () => {
  for (const message of [
    'This property is going under contract. Need to change status on MLS my apologies',
    'This listing is already under contract.',
    'We have a signed contract and I need to update the MLS status.',
  ]) {
    const h = smsHarness();
    assert.equal(h.evaluate(`isAlreadyHandledSignal_(${JSON.stringify(message)})`), false, message);
    const result = h.incoming(message);
    assert.equal(result.reply_text, 'Got it. I hope the contract goes smoothly.', message);
    assert.equal(result.lead_status, 'R', message);
    assert.equal(result.handoff_needed, false, message);
    assert.match(result.reason, /status update without inferring existing short-sale help/i, message);
  }
});

test('real existing-help coverage still uses the approved backup closeout', () => {
  const result = smsHarness().incoming('I already have a negotiator handling this file.');
  assert.match(result.reply_text, /already have help/i);
  assert.equal(result.lead_status, 'H');
});

test('explicit voice-message requests suppress the bot and create one human handoff without a call', () => {
  const message = "I already have help. But I'm happy to talk about your services. Can you send me a voice text and let me know how you get paid on the buy side please.";
  const h = smsHarness();
  const result = h.incoming(message);
  assert.equal(result.should_reply, false);
  assert.equal(result.reply_text, '');
  assert.equal(result.handoff_needed, true);
  assert.equal(h.effects.length, 1);
  assert.equal(h.effects[0].reason, 'VOICE MESSAGE REQUEST');
  assert.equal(h.state.call_booking_status, 'interested_no_call');
  assert.equal(h.state.callback_requested, undefined);
  assert.equal(h.state.call_scheduled_for, undefined);
});

test('ordinary requests to text information are not mistaken for voice-message requests', () => {
  const h = smsHarness();
  const result = h.incoming('Can you text me your website?');
  assert.notEqual(result.handoff_type, 'VOICE MESSAGE REQUEST');
  assert.equal(result.should_reply, true);
});
