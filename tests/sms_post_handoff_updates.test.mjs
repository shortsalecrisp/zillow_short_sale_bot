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
  ]) {
    const result = h.incoming(message);
    assert.equal(result.should_reply, false);
  }
  assert.equal(h.effects.filter(effect => effect.type === 'handoff' && effect.reason === 'HUMAN HANDOFF UPDATE').length, 4);
  const courtesy = h.incoming('Thank you!');
  assert.equal(courtesy.should_reply, false);
  assert.equal(h.effects.filter(effect => effect.type === 'handoff' && effect.reason === 'HUMAN HANDOFF UPDATE').length, 4);
});
