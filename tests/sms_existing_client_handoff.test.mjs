import test from 'node:test';
import assert from 'node:assert/strict';
import {smsHarness} from './helpers/sms_core_harness.mjs';

for (const text of [
  'Hi Yoni, you are handling this file already :)',
  'Hi Yoni... this is Samantha Taravella... this is your file...',
]) {
  test(`an agent identifying Yoni as the current processor gets a silent handoff: ${text}`, () => {
    const h = smsHarness({agent_name: 'Samantha', last_name: 'Taravella'});
    const result = h.incoming(text);

    assert.equal(result.should_reply, false);
    assert.equal(result.handoff_needed, true);
    assert.equal(h.state.ai_state, 'handoff');
    assert.equal(h.state.human_override, 'TRUE');
    assert.equal(h.effects.length, 1);
    assert.equal(h.effects[0].type, 'handoff');
    assert.equal(h.effects[0].reason, 'EXISTING CRISP CLIENT');
  });
}

test('a second existing-client correction stays with Yoni after the first handoff', () => {
  const h = smsHarness({agent_name: 'Samantha', last_name: 'Taravella'});
  h.incoming('Hi Yoni, you are handling this file already :)');
  const correction = h.incoming('Hi Yoni... this is Samantha Taravella... this is your file...');

  assert.equal(correction.should_reply, false);
  assert.equal(h.state.human_override, 'TRUE');
  assert.equal(h.state.auto_reply_count, 0);
});

test('a different provider handling the file is not mistaken for an existing Crisp client', () => {
  const h = smsHarness();
  const result = h.incoming('I have someone else handling this file already.');

  assert.equal(result.handoff_needed, false);
  assert.equal(h.effects.length, 0);
});
