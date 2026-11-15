import assert from 'node:assert/strict';
import { smsHarness } from './helpers/sms_core_harness.mjs';

const h = smsHarness();
for (const message of [
  'I already have help However, do you service Virginia too?',
  'I already have help. Can you cover VA?',
  'I handle it myself. Do you work in North Carolina?',
  'No thanks. Do you serve Texas?',
  'Do you service all states?',
  'Do you cover nationwide?',
]) {
  const decision = h.evaluate(`applyFastRules_(${JSON.stringify(message)}, {})`);
  assert.match(decision.reply_text, /based in Atlanta and work nationwide/, message);
  assert.equal(decision.handoff_needed, false, message);
  if (/already have help|No thanks/.test(message)) {
    assert.equal(decision.conversation_done, true, message);
    assert.equal(decision.lead_status, 'O', message);
  }
}
for (const message of [
  'I already have help. Your service sounds useful.',
  'I handle it myself and cover Virginia.',
  'Do you work in Equator?',
  'Do you cover the buyer fee?',
  'Do you service mortgages?',
]) {
  assert.equal(h.evaluate(`isLocalQuestionSignal_(${JSON.stringify(message)})`), false, message);
}
const flow = smsHarness();
const reply = flow.incoming('I already have help However, do you service Virginia too?');
assert.match(reply.reply_text, /based in Atlanta and work nationwide/);
assert.equal(flow.effects.some(effect => effect.type === 'handoff'), false);
console.log('service-area question precedence regressions passed');
