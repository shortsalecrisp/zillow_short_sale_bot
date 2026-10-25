import test from 'node:test';
import assert from 'node:assert/strict';
import {smsHarness} from './helpers/sms_core_harness.mjs';

const unknown = {agent_name: '', last_name: '', listing_address: '', initial_text: ''};

test('personal reminiscence and an image reaction do not become leads at the reply cap', () => {
  const h = smsHarness({...unknown, auto_reply_count: 3});
  for (const message of [
    'Remember our basketball team back in school? Those were good times.',
    'Loved an image',
    'I still miss those days with the team.'
  ]) {
    const result = h.incoming(message);
    assert.equal(result.should_reply, false);
    assert.equal(result.handoff_needed, false);
    assert.equal(h.state.mailshake_status, 'N');
    assert.notEqual(h.state.call_booking_status, 'interested_no_call');
  }
  assert.equal(h.effects.length, 0);
});

test('unattributed ambiguous message stays silent, including an email request without business context', () => {
  const h = smsHarness(unknown);
  for (const message of ['Sounds good', 'Send me an email with more details']) {
    const result = h.incoming(message);
    assert.equal(result.should_reply, false);
    assert.equal(h.state.mailshake_status, 'N');
  }
  assert.equal(h.effects.length, 0);
});

test('known campaign lead retains normal response to a contextual email request', () => {
  const h = smsHarness({initial_text: 'I help with short sales.', listing_address: '123 Example Lane'});
  assert.equal(h.evaluate(`isUnrelatedPersonalSms_('Send me an email with more details', state)`), false);
});

test('explicit short-sale request is eligible without a prior campaign row', () => {
  const h = smsHarness(unknown);
  assert.equal(h.evaluate(`isUnrelatedPersonalSms_('Send me an email with more details about your short-sale service', state)`), false);
  assert.equal(h.evaluate(`isUnrelatedPersonalSms_('Can you help with this file?', state)`), false);
  assert.equal(h.evaluate(`isUnrelatedPersonalSms_('What does the bank need for approval?', state)`), false);
});

test('image reaction with business wording is silent, but a real question after it is eligible', () => {
  const h = smsHarness({...unknown, auto_reply_count: 3});
  const reaction = h.incoming('Liked a photo of the listing');
  assert.equal(reaction.should_reply, false);
  assert.equal(reaction.handoff_needed, false);
  assert.equal(h.effects.length, 0);
  assert.equal(h.incoming('Sounds good').should_reply, false);
  assert.equal(h.state.mailshake_status, 'N');
  assert.equal(h.evaluate(`isUnrelatedPersonalSms_('Liked a photo of the listing. Can you help with the bank?', state)`), false);
});

test('business question mixed with casual conversation stays eligible', () => {
  const h = smsHarness(unknown);
  assert.equal(h.evaluate(`isUnrelatedPersonalSms_('Great seeing you at the game. Can you help with my short sale?', state)`), false);
});

test('opt-out and manual takeover remain active', () => {
  const optOut = smsHarness(unknown);
  assert.equal(optOut.incoming('STOP').lead_status, 'R');
  const manual = smsHarness({...unknown, human_override: 'TRUE', ai_state: 'handoff', handoff_flag: 'TRUE'});
  const result = manual.incoming('Tomorrow at 3 works');
  assert.equal(result.should_reply, false);
  assert.notEqual(result.reason, 'Unrelated personal message; no sales action');
});
