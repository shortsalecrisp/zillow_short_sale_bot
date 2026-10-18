import assert from 'node:assert/strict';
import {smsHarness} from './helpers/sms_core_harness.mjs';

const vendorBot = smsHarness();
const vendorStateBefore = JSON.stringify(vendorBot.state);
for (const message of [
  'Hi Larry, this is Mike in Atlanta. Our crew is working in your area this week, free quotes on tree trimming',
  'Awesome, glad you are interested! Are you looking to get a tree trimmed or do you need a removal done?',
  'Sounds good on the trimming. How big is the tree roughly, small, medium, large? Or feel free to send a photo',
]) {
  const result = vendorBot.incoming(message);
  assert.equal(result.should_reply, false);
  assert.equal(result.handoff_needed, false);
  assert.equal(result.needs_review, false);
  assert.equal(vendorBot.effects.length, 0);
}
assert.equal(JSON.stringify(vendorBot.state), vendorStateBefore);

assert.equal(vendorBot.evaluate(`isAutomatedPromotionalSmsSignal_("Hi, is this Yoni? I have a question about your services.")`), false);
assert.equal(vendorBot.evaluate(`isAutomatedPromotionalSmsSignal_("Our crew is working on the property. Can you help with the lender?")`), false);
assert.equal(vendorBot.evaluate(`isAutomatedPromotionalSmsSignal_("I own a tree trimming company and need help with a short-sale listing.")`), false);
assert.equal(vendorBot.evaluate(`isAutomatedPromotionalSmsSignal_("Can you send me a quote?")`), false);

console.log('Off-topic commercial-solicitation SMS sequence guard passed.');
