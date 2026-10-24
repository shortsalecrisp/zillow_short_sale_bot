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
