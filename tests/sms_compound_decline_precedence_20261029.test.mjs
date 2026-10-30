import assert from 'node:assert/strict';
import { smsHarness } from './helpers/sms_core_harness.mjs';

const h = smsHarness();

for (const message of [
  'Already covered, but thanks bring me a buyer.',
  'I personally handle all my short sales and only charge 1% because I like control.',
]) {
  const decision = h.evaluate(`applyFastRules_(${JSON.stringify(message)}, {})`);
  assert.equal(decision.conversation_done, true, message);
  assert.equal(decision.handoff_needed, false, message);
  assert.equal(decision.lead_status, 'R', message);
  assert.equal(
    decision.reason,
    'Explicit coverage or self-handling decline outranked secondary buyer, fee, or capability remarks',
    message,
  );
  assert.doesNotMatch(decision.reply_text, /don't bring the buyer|\$5,000|flat fee/i, message);
}

for (const message of [
  "We already have a negotiator. What's your rate?",
  'Already covered. Can you bring the buyer?',
  'I already have help, but please send me more information about your services.',
  'I am handling it myself, but I am willing to review what you offer.',
]) {
  const decision = h.evaluate(`applyFastRules_(${JSON.stringify(message)}, {})`);
  assert.notEqual(
    decision.reason,
    'Explicit coverage or self-handling decline outranked secondary buyer, fee, or capability remarks',
    message,
  );
  assert.match(decision.reply_text, /\$5,000|don't bring the buyer|best email|lender/i, message);
}

console.log('sms compound decline precedence tests passed');
