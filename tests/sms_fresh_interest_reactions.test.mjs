import assert from 'node:assert/strict';
import test from 'node:test';
import {smsHarness} from './helpers/sms_core_harness.mjs';

const buyerInquiry = 'I am thinking about deals where maybe I am the buyer and need arms length.';
const feeAnswer = "There's no cost to you or the seller, and I don't take anything from your commission. I charge a flat fee to the buyer at closing, only if the deal closes.";
const timelineQuestion = 'How long does a short sale take?';

function waitingAnswer() {
  const h = smsHarness({last_outbound_text: feeAnswer, history_json: JSON.stringify([
    {role: 'assistant', text: feeAnswer, receipt_id: 'prior-delivered-answer'}
  ])});
  const answer = h.incoming(timelineQuestion, {deliver: false, messageId: 'real-question'});
  assert.equal(answer.should_reply, true);
  const row = ['', 'queued', 'one-request', 'real-question', h.state.phone, answer.reply_text, timelineQuestion];
  return {h, row};
}

test('fresh buyer-side processing interest after a decline alerts once without model fallback or legal promises', () => {
  const h = smsHarness();
  h.incoming('Already have someone assisting but thanks for the offer.');
  h.incoming('Just for info what is your typical fee ?');
  const r = h.incoming(buyerInquiry, {messageId: 'fresh-buyer-interest'});
  assert.equal(r.handoff_needed, true);
  assert.equal(r.should_reply, false);
  assert.match(h.effects.at(-1).reason, /BUYER.*REVIEW/);
  assert.equal(h.state.human_override, 'TRUE');
  assert.notEqual(h.state.call_booking_status, 'closed_no_interest');
  h.incoming(buyerInquiry, {messageId: 'fresh-buyer-interest'});
  assert.equal(h.effects.filter(x => x.type === 'handoff').length, 1);
});

test('explicit fresh future-service interest after coverage generates a lead alert', () => {
  const h = smsHarness({mailshake_status: 'H', ai_state: 'done', call_booking_status: 'closed_no_interest'});
  const r = h.incoming('I already have this covered, but I am interested in your help with future short sales.');
  assert.equal(r.handoff_needed, true);
  assert.equal(h.effects.filter(x => x.type === 'handoff').length, 1);
  assert.match(h.effects[0].reason, /INTEREST/);
});

test('same-turn current coverage cannot outrank explicit fresh service interest', () => {
  for (const message of [
    'I handle the lender myself, but I am interested in your services for future short sales.',
    'I already have help with this file, but I would like to learn about your fees for future short sales.',
    'I already have help on the listing, but I am the buyer on another deal and need a processor.'
  ]) {
    const h = smsHarness();
    const result = h.incoming(message);
    assert.equal(result.handoff_needed, true, message);
    assert.equal(result.should_reply, false);
    assert.equal(h.effects.filter(x => x.type === 'handoff').length, 1);
  }
});

test('exact and flattened liked reactions retain the pending substantive pointer and answer', () => {
  for (const reaction of [`Liked "${feeAnswer}"`, `Liked ${feeAnswer} to ${feeAnswer}`]) {
    const {h, row} = waitingAnswer();
    const r = h.incoming(reaction, {deliver: false, messageId: 'only-a-reaction'});
    assert.equal(r.reaction, true);
    assert.equal(r.should_reply, false);
    assert.equal(h.state.last_message_id, 'real-question');
    assert.equal(h.state.last_inbound_text, timelineQuestion.toLowerCase());
    assert.equal(h.evaluate(`getPendingSmsStaleReason_(${JSON.stringify(row)})`), '');
    assert.equal(JSON.parse(h.state.history_json).at(-1).text, reaction);
    assert.equal(h.effects.length, 0);
  }
});

test('a newer real question followed by a reaction still cancels the older answer', () => {
  const {h, row} = waitingAnswer();
  h.incoming('Are you local?', {deliver: false, messageId: 'new-real-question'});
  h.incoming(`Liked "${feeAnswer}"`, {deliver: false, messageId: 'reaction-after-new-question'});
  assert.equal(h.state.last_message_id, 'new-real-question');
  assert.match(h.evaluate(`getPendingSmsStaleReason_(${JSON.stringify(row)})`), /newer substantive/);
});

test('reaction protection never permits a pending answer past STOP or a real takeover', () => {
  for (const kind of ['stop', 'human_override', 'handoff_flag', 'ai_state']) {
    const {h, row} = waitingAnswer();
    if (kind === 'stop') h.incoming('STOP', {deliver: false, messageId: 'real-stop'});
    else h.state[kind] = kind === 'ai_state' ? 'handoff' : 'TRUE';
    h.incoming(`Liked "${feeAnswer}"`, {deliver: false, messageId: 'later-reaction'});
    assert.notEqual(h.evaluate(`getPendingSmsStaleReason_(${JSON.stringify(row)})`), '', kind);
  }
});

test('a pending takeover control still blocks the answer after a liked reaction', () => {
  const {h, row} = waitingAnswer();
  h.evaluate('installSmsOutboxTriggers_ = () => {};');
  h.evaluate(`queuePendingSmsControlEventV11_(state.phone, {action: 'mark_override', value: 'TRUE'})`);
  h.incoming(`Liked "${feeAnswer}"`, {deliver: false, messageId: 'reaction-after-pending-control'});
  assert.equal(h.evaluate(`getPendingSmsStaleReason_(${JSON.stringify(row)})`), 'Manual takeover is pending');
});

test('mixed reaction plus real request or opt-out is not discarded', () => {
  for (const extra of ['Can you call me tomorrow?', 'Please stop texting me.']) {
    const h = smsHarness({last_outbound_text: feeAnswer});
    const mixed = `Liked "${feeAnswer}" ${extra}`;
    assert.equal(h.evaluate(`isSmsReactionToLastOutbound_(${JSON.stringify(mixed)}, state)`), false);
    assert.equal(h.evaluate(`isSelfDuplicateSmsReactionArtifact_(${JSON.stringify(mixed)})`), false);
    if (extra.startsWith('Please stop')) assert.equal(h.incoming(mixed, {deliver: false}).should_reply, false);
  }
});

test('fresh buyer interest never overrides a current opt-out or existing human ownership', () => {
  const stop = smsHarness();
  const r = stop.incoming(`${buyerInquiry} Please stop texting me.`);
  assert.equal(r.should_reply, false);
  assert.equal(stop.effects.length, 0);
  for (const fields of [{human_override: 'TRUE'}, {handoff_flag: 'TRUE', ai_state: 'handoff'}]) {
    const h = smsHarness(fields);
    const owned = h.incoming(buyerInquiry);
    assert.equal(owned.should_reply, false);
    assert.equal(h.effects.some(x => /BUYER.*REVIEW/.test(x.reason)), false);
  }
});

test('current-file rejection and muted sources do not create fresh leads', () => {
  for (const text of [
    'I already have help, just bring me a buyer.',
    'No thanks, I am not interested in your help with future short sales.',
    'I am the buyer, but no thanks, I already have help.'
  ]) {
    const h = smsHarness();
    h.incoming(text);
    assert.equal(h.effects.filter(x => x.type === 'handoff').length, 0, text);
  }
  const muted = smsHarness({phone: '4045393880'});
  assert.equal(muted.incoming(buyerInquiry).should_reply, false);
  assert.equal(muted.effects.length, 0);
});

test('vague future courtesy is not promoted into the new fresh-interest handoff', () => {
  const h = smsHarness();
  h.incoming('I will keep your information in case I need help in the future.');
  assert.equal(h.effects.some(x => /BUYER-SIDE|FRESH SERVICE INTEREST/.test(x.reason)), false);
});

test('pure reactions without a substantive pointer retain the existing follow-up suppression marker', () => {
  const h = smsHarness({last_outbound_text: feeAnswer});
  h.incoming(`Liked "${feeAnswer}"`, {messageId: 'reaction-without-question'});
  assert.equal(h.state.last_message_id, 'reaction-without-question');
  assert.equal(h.effects.length, 0);
});

test('buyer-supply requests remain distinct from buyer-side processing questions', () => {
  for (const message of ['Bring me a buyer.', 'Can you find a buyer for this listing?']) {
    const h = smsHarness();
    const r = h.incoming(message);
    assert.equal(r.handoff_needed, false);
    assert.match(r.reply_text, /don't bring|don't provide|processing/i);
  }
  for (const message of [
    "I'm the buyer and need an independent processor for this short sale.",
    'As a buyer, could you help with the lender paperwork?',
    'We may be the buyer and need an arm\'s-length arrangement.'
  ]) {
    const h = smsHarness();
    assert.equal(h.incoming(message).handoff_needed, true, message);
    assert.match(h.effects[0].reason, /BUYER-SIDE/);
  }
});
