import assert from 'node:assert/strict';
import test from 'node:test';
import {smsHarness} from './helpers/sms_core_harness.mjs';

const campaign = ': 3 things just happened: A new poll showed us 4845 after a 6-point swing. ' +
  'I launched a Rapid Response Fund to get our response ads on the air immediately. ' +
  'You chipped in $1 to the Fund to save Georgia wait, you didnt yet! , ? $ ? ' +
  'txt.keishaforgovernor.com/p91973 Thank you, Keisha Stop2End';

const promotion = smsHarness();
for (let attempt = 0; attempt < 3; attempt += 1) {
  const result = promotion.incoming(campaign);
  assert.equal(result.should_reply, false);
  assert.equal(result.handoff_needed, false);
  assert.equal(result.needs_review, false);
  assert.equal(promotion.effects.length, 0);
}
assert.equal(promotion.state.history_json, '[]');
assert.equal(promotion.state.mailshake_status, 'N');
assert.equal(promotion.state.auto_reply_count, 0);
assert.equal(promotion.evaluate(`isAutomatedPromotionalSmsSignal_("How much is the fee for the short sale? I also got a campaign text with a link and Stop2End")`), false);
assert.equal(promotion.evaluate(`isAutomatedPromotionalSmsSignal_("Are you a bot or a real person?")`), false);
for (const automated of [
  'Fair Fight: Check your voter registration at mvp.sos.ga.gov. Reply STOP to unsubscribe.',
  'You have successfully been unsubscribed. Reply START to resubscribe.',
  'Hi, I am Eva with Web Surveys. We are polling GA residents. Can you answer a quick poll? 1) Yes 2) No (or QUIT)',
]) {
  const result = promotion.incoming(automated);
  assert.equal(result.should_reply, false);
  assert.equal(result.handoff_needed, false);
}
assert.equal(promotion.evaluate(`isAutomatedPromotionalSmsSignal_("I am registered to vote and have a short-sale listing question")`), false);
assert.equal(promotion.evaluate(`isAutomatedPromotionalSmsSignal_("I am surveying the property. 1) Vacant 2) Occupied")`), false);
assert.equal(promotion.evaluate(`isAutomatedPromotionalSmsSignal_("Text STOP to quit, but I have a short-sale listing question")`), false);
assert.equal(promotion.evaluate(`isAutomatedPromotionalSmsSignal_("Join #TeamKeisha tomorrow for Get Out the Vote action and canvassing. Tomorrow, October 17. Text STOP to quit")`), true);
const commercialSolicitation = 'Hi Yoni, this is Alex with Acme Tree Service. We offer free estimates for trimming and removal. Call us today.';
const commercial = smsHarness();
const commercialResult = commercial.incoming(commercialSolicitation);
assert.equal(commercialResult.should_reply, false);
assert.equal(commercialResult.handoff_needed, false);
assert.equal(commercialResult.needs_review, false);
assert.equal(promotion.evaluate(`isAutomatedPromotionalSmsSignal_(${JSON.stringify('Hi, this is Alex with Acme Tree Service.')})`), false);
assert.equal(promotion.evaluate(`isAutomatedPromotionalSmsSignal_(${JSON.stringify('I need a tree service quote for my short-sale property.')})`), false);

const repeated = smsHarness();
const question = 'What is your fee?';
const first = repeated.incoming(question);
assert.equal(first.should_reply, true);
assert.equal(repeated.state.auto_reply_count, 1);
const second = repeated.incoming(question);
assert.equal(second.should_reply, false);
assert.equal(second.duplicate, true);
assert.equal(second.reason, 'Durably handled inbound replay ignored');
assert.equal(repeated.effects.length, 0);
assert.equal(repeated.state.human_override, 'FALSE');
assert.equal(repeated.state.auto_reply_count, 1);
// A differently worded fee question must still reach the real follow-up guard.
const followup = 'How much is your fee?';
const clarified = repeated.incoming(followup);
assert.equal(clarified.should_reply, false);
assert.equal(clarified.handoff_needed, true);
assert.equal(repeated.effects.at(-1).reason, 'FEE QUESTION FOLLOW-UP');
assert.equal(repeated.effects.filter(effect => effect.type === 'handoff').length, 1);
assert.equal(repeated.state.human_override, 'TRUE');
assert.equal(repeated.state.auto_reply_count, 1);
const third = repeated.incoming(followup);
assert.equal(third.should_reply, false);
assert.equal(third.duplicate, true);
assert.equal(repeated.effects.filter(effect => effect.type === 'handoff').length, 1);
const newQuestion = repeated.incoming('Can you email the fee details to me?');
assert.equal(newQuestion.should_reply, false);
assert.equal(newQuestion.reason, 'Human override enabled');
assert.equal(repeated.effects.filter(effect => effect.type === 'handoff').length, 2);

const unanswered = smsHarness({history_json: JSON.stringify([{role: 'agent', text: question}])});
assert.equal(unanswered.evaluate(`isAnsweredRepeatedInboundQuestion_(state, ${JSON.stringify(question)})`), false);
const humanOwned = smsHarness({
  human_override: 'TRUE', ai_state: 'handoff', handoff_flag: 'TRUE',
  history_json: JSON.stringify([
    {role: 'agent', text: question},
    {role: 'assistant', text: 'The buyer pays the flat fee at closing.', receipt_id: 'confirmed'},
  ]),
});
const ownedRepeat = humanOwned.incoming(question);
assert.equal(ownedRepeat.should_reply, false);
assert.equal(humanOwned.effects.length, 0);

test('a delayed exact replay stays silent even with a new transport ID', () => {
  const h = smsHarness();
  h.incoming(question, {messageId: 'original', receivedAt: '2026-10-29T14:00:00Z'});
  const before = {...h.state};
  const replay = h.incoming('  WHAT is your fee?  ', {
    messageId: 'delayed-provider-alias', receivedAt: '2026-10-30T14:00:00Z',
  });
  assert.equal(replay.should_reply, false);
  assert.equal(replay.duplicate, true);
  assert.equal(replay.reason, 'Durably handled inbound replay ignored');
  assert.deepEqual(h.state, before);
  assert.deepEqual(h.effects, []);
});

test('returning to an answered question after a different turn still alerts once', () => {
  const h = smsHarness({
    last_inbound_text: 'Will do. Thanks for reaching out.',
    history_json: JSON.stringify([
      {role: 'agent', text: question},
      {role: 'assistant', text: 'The buyer pays at closing.', receipt_id: 'confirmed'},
      {role: 'agent', text: 'Will do. Thanks for reaching out.'},
    ]),
  });
  const result = h.incoming(question);
  assert.equal(result.should_reply, false);
  assert.equal(result.handoff_type, 'REPEATED ANSWERED MESSAGE REVIEW');
  assert.equal(h.state.human_override, 'TRUE');
  assert.equal(h.effects.filter(e => e.type === 'handoff').length, 1);
  assert.equal(h.incoming(question).should_reply, false);
  assert.equal(h.effects.filter(e => e.type === 'handoff').length, 1);
  assert.equal(h.state.auto_reply_count, 0);
});

test('an unreceipted answer alone cannot establish the answered-repeat guard', () => {
  const h = smsHarness({history_json: JSON.stringify([
    {role: 'agent', text: question},
    {role: 'assistant', text: 'The buyer pays at closing.'},
  ])});
  assert.equal(h.evaluate(`isAnsweredRepeatedInboundQuestion_(state, ${JSON.stringify(question)})`), false);
});
console.log('Automated-promotion and answered-repeat SMS guards passed.');
