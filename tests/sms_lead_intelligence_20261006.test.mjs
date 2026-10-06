import test from 'node:test';
import assert from 'node:assert/strict';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('exact substantive replay with a delivered answer is suppressed before reply or alert paths', () => {
  const text = 'What is your fee and how should I word it in the listing?';
  const h = smsHarness({
    last_inbound_text: text,
    response_status: text,
    history_json: JSON.stringify([
      {role: 'agent', text, ts: '2026-10-05T20:00:00-04:00'},
      {role: 'assistant', text: 'Earlier supported answer', receipt_id: 'sent-1', ts: '2026-10-05T20:01:00-04:00'},
    ]),
  });
  const result = h.incoming(text, {deliver: false, receivedAt: '2026-10-06T02:43:00-04:00'});
  assert.equal(result.duplicate, true);
  assert.notEqual(result.should_reply, true);
  assert.notEqual(result.handoff_needed, true);
  assert.equal(h.effects.length, 0);
});

test('an assistant draft without a delivery receipt does not suppress a substantive inbound', () => {
  const text = 'What is your fee?';
  const h = smsHarness({
    last_inbound_text: text,
    response_status: text,
    history_json: JSON.stringify([
      {role: 'agent', text, ts: '2026-10-05T20:00:00-04:00'},
      {role: 'assistant', text: 'Draft answer with no receipt', ts: '2026-10-05T20:01:00-04:00'},
    ]),
  });
  assert.equal(h.evaluate(`isDurableHandledDuplicateInbound_(state, ${JSON.stringify(text)})`), false);
});

test('exact replay in a human-owned conversation is suppressed but changed callback details remain actionable', () => {
  const text = 'Please have Yoni call me Monday afternoon.';
  const h = smsHarness({
    last_inbound_text: text,
    ai_state: 'handoff',
    handoff_flag: 'TRUE',
    human_override: 'TRUE',
    call_booking_status: 'scheduled_callback',
    callback_requested: 'yes',
    history_json: JSON.stringify([{role: 'agent', text, ts: '2026-10-05T20:00:00-04:00'}]),
  });
  assert.equal(h.incoming(text, {deliver: false}).duplicate, true);
  assert.equal(h.effects.length, 0);
  assert.equal(h.evaluate('isDurableHandledDuplicateInbound_(state, "Please have Yoni call me Tuesday at 3 PM.")'), false);
});

test('referral company-info request gets canonical facts and a named owner handoff', () => {
  const h = smsHarness();
  const result = h.incoming('Can you send me information about your company? I have a colleague looking for short sale help.');
  assert.match(result.reply_text, /Crisp Short Sales/);
  assert.match(result.reply_text, /https:\/\/www\.crispshortsales\.com/);
  assert.doesNotMatch(result.reply_text, /\$5,000/);
  assert.equal(result.handoff_needed, true);
  assert.deepEqual(JSON.parse(JSON.stringify(h.effects)), [{type: 'handoff', reason: 'REFERRAL / COMPANY INFO'}]);
});

test('fee wording request answers only supported facts and routes exact language to Yoni', () => {
  const h = smsHarness();
  const result = h.incoming('What is your fee and what verbiage should I put in the listing and purchase contract?');
  assert.match(result.reply_text, /\$5,000/);
  assert.match(result.reply_text, /Yoni provide the exact contract or listing wording/);
  assert.doesNotMatch(result.reply_text, /MLS remarks should say|purchase contract must say/);
  assert.equal(result.handoff_needed, true);
  assert.deepEqual(JSON.parse(JSON.stringify(h.effects)), [{type: 'handoff', reason: 'FEE DISCLOSURE WORDING REVIEW'}]);
});
