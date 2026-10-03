import assert from 'node:assert/strict';
import test from 'node:test';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('website plus attorney comparison typo answers the boundary and opens a hot-lead handoff', () => {
  const h = smsHarness({
    mailshake_status: 'O',
    ai_state: 'done',
    call_booking_status: 'closed_no_interest',
    last_outbound_text: 'Thanks for letting me know. If anything changes, feel free to reach out.',
  });

  const result = h.incoming('Do you have a website ? What do you do different then attorney ?');

  assert.equal(result.should_reply, true);
  assert.equal(result.lead_status, 'Y');
  assert.equal(result.handoff_needed, true);
  assert.equal(result.alert_needed, true);
  assert.match(result.reply_text, /https:\/\/www\.crispshortsales\.com/);
  assert.match(result.reply_text, /I'm not an attorney and I don't provide legal advice/);
  assert.equal(h.state.ai_state, 'handoff');
  assert.equal(h.state.handoff_flag, 'TRUE');
  assert.deepEqual(
    h.effects.filter((effect) => effect.type === 'handoff').map((effect) => effect.reason),
    ['HOT LEAD - DIFFERENTIATION QUESTION'],
  );
});

test('plain existing-attorney coverage is not promoted to differentiation', () => {
  const h = smsHarness();

  assert.equal(h.evaluate("isDifferentiationQuestionSignal_('Seller has attorney')"), false);
});
