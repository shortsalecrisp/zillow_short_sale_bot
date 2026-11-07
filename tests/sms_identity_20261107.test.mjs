import test from 'node:test';
import assert from 'node:assert/strict';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('Bryan compound identity question receives deterministic listing context', () => {
  const h = smsHarness({agent_name:'Bryan', listing_address:'645 GREEN VALLEY ROAD #K3'});
  const r = h.incoming('Apologies I dont have a message from earlier. Who is this, and what is this regarding?');
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /Yoni Kutler with Crisp Short Sales/);
  assert.match(r.reply_text, /645 GREEN VALLEY ROAD #K3/);
  assert.doesNotMatch(r.reply_text, /your listing at your listing/);
  assert.equal(r.handoff_needed, false);
});
test('sanitizer repairs duplicated reference while fee and STOP guards retain precedence', () => {
  const h = smsHarness();
  assert.equal(h.evaluate('sanitizeReplyPropertyReference_("Is this about your listing at 123 Example Lane or another file?",state)'), 'Is this about your listing or another file?');
  assert.equal(h.evaluate('isIdentityResendSignal_("Who is this and how much is your fee?")'), false);
  assert.equal(h.incoming('Stop').should_reply, false);
});
