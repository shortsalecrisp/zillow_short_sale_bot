import assert from 'node:assert/strict';
import test from 'node:test';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('campaign donation and didthey.vote automation are suppressed before reply selection', () => {
  const h = smsHarness();
  for (const inbound of [
    'Keisha needs one more push before Election Day. Can you chip in $1? https://example.org/donate',
    'Ruth, Election Day is Nov 3. Confirm your voting record at https://didthey.vote -Did They Vote Stop to end',
  ]) {
    const result = h.incoming(inbound);
    assert.equal(result.should_reply, false);
    assert.equal(result.handoff_needed, false);
    assert.equal(result.needs_review, false);
  }
  const followup = h.incoming('Thank you for your response. Have a good day.');
  assert.equal(followup.should_reply, false);
  assert.equal(followup.handoff_needed, false);
  assert.equal(h.effects.length, 0);
  assert.equal(h.evaluate(`isAutomatedPromotionalSmsSignal_("I am registered to vote and have a short-sale listing question")`), false);
});
