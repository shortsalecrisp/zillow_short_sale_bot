import assert from 'node:assert/strict';
import test from 'node:test';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('historical delivery time plus refusal closes without callback or handoff', () => {
  const h = smsHarness();
  const result = h.incoming('Hello! Oh I see you did text me at 4am this morning. No thank you!');
  assert.equal(result.lead_status, 'R');
  assert.equal(result.conversation_done, true);
  assert.equal(result.callback_requested ?? '', '');
  assert.equal(result.callback_time ?? '', '');
  assert.equal(result.handoff_needed, false);
  assert.equal(h.effects.length, 0);
});

test('scheduled callback requires explicit call or conversation intent', () => {
  const h = smsHarness();
  assert.equal(h.evaluate('isSchedulingSignal_("Please call me at 4 pm")'), true);
  assert.equal(h.evaluate('isSchedulingSignal_("Can we talk tomorrow afternoon?")'), true);
  assert.equal(h.evaluate('isSchedulingSignal_("Please text me at 4 pm")'), false);
  assert.equal(h.evaluate('isSchedulingSignal_("Tomorrow at 4 pm works")'), false);
  assert.equal(h.evaluate('isSchedulingSignal_("Feel free to reach out to me Monday")'), false);
  assert.equal(h.evaluate('isSchedulingSignal_("Can you touch base in three weeks time?")'), false);
});

test('agent promise to give us a call does not bind an appointment time as owner callback', () => {
  const h = smsHarness();
  const inbound = "Hey, I have an appointment at 10:30 and then I'll give you a call after.";
  assert.equal(h.evaluate(`isSelfInitiatedDeferredContactSignal_(${JSON.stringify(inbound)})`), true);
  assert.equal(h.evaluate(`isSchedulingSignal_(${JSON.stringify(inbound)})`), false);
  const result = h.incoming(inbound);
  assert.notEqual(result.callback_requested, 'yes');
  assert.equal(result.callback_time ?? '', '');
  assert.equal(result.handoff_needed, false);
});

test('paid-for-by STOP equals END campaign footer is suppressed without blocking real estate context', () => {
  const h = smsHarness();
  const campaign = 'Vote for a stronger Georgia. Paid for by Powering Georgias Future IC STOP=END';
  assert.equal(h.evaluate(`isAutomatedPromotionalSmsSignal_(${JSON.stringify(campaign)})`), true);
  assert.equal(h.evaluate('isAutomatedPromotionalSmsSignal_("The buyer pays at closing for this short-sale listing. Paid for by buyer. STOP=END")'), false);
});
