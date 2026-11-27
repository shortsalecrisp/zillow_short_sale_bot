import assert from 'node:assert/strict';
import test from 'node:test';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('human-owned scheduling and file updates alert the owner without a bot reply', () => {
  const h = smsHarness({human_override: 'TRUE', ai_state: 'handoff', handoff_flag: 'TRUE'});
  for (const message of [
    'Thursday September 24th at 11AM PST works.',
    'Switch to video please.',
    'Please send the invite to bekabernard@gmail.com and simplyvegasnelsonteam@gmail.com.',
    'I uploaded the signed contract PDF.',
    'The company also wants our financials.',
    'Said the call was today.',
  ]) {
    const result = h.incoming(message);
    assert.equal(result.should_reply, false);
  }
  assert.equal(h.effects.filter(effect => effect.type === 'handoff' && effect.reason === 'HUMAN HANDOFF UPDATE').length, 6);
  const courtesy = h.incoming('Thank you!');
  assert.equal(courtesy.should_reply, false);
  assert.equal(h.effects.filter(effect => effect.type === 'handoff' && effect.reason === 'HUMAN HANDOFF UPDATE').length, 6);
});

test('exact media placeholders alert only on mapped human-owned conversations', () => {
  for (const message of ['Image', ' VIDEO ', 'Audio', 'File']) {
    const h = smsHarness({human_override: 'TRUE', ai_state: 'handoff'});
    const result = h.incoming(message, {messageId: 'media-event'});
    assert.equal(result.should_reply, false);
    assert.match(result.reason, /review in Google Messages/);
    assert.equal(h.incoming(message, {messageId: 'media-event'}).duplicate, true);
    assert.equal(h.effects.length, 1);
    assert.equal(h.effects[0].reason, 'HUMAN HANDOFF UPDATE');
  }
  const handoff = smsHarness({handoff_flag: 'TRUE'});
  assert.match(handoff.incoming('Image').reason, /review in Google Messages/);

  for (const [message, state] of [
    ['Image', {human_override: 'TRUE', listing_address: ''}],
    ['Image', {human_override: 'TRUE', ai_state: 'done', mailshake_status: 'R', response_status: 'STOP'}],
    ['Liked an image', {human_override: 'TRUE', ai_state: 'handoff'}],
  ]) {
    const h = smsHarness(state);
    assert.equal(h.incoming(message).should_reply, false);
    assert.deepEqual(h.effects, []);
  }
});
