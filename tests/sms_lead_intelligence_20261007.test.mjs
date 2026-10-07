import test from 'node:test';
import assert from 'node:assert/strict';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('Mike Sher wording keeps the current-file opportunity active', () => {
  const text = "So I've not decided which direction I'm gonna go on this. I have handled them on many occasions though that was during the great recession so I have not decided which direction but I'll keep your information";
  const h = smsHarness();
  const result = h.incoming(text);
  assert.equal(result.should_reply, true);
  assert.equal(result.lead_status, 'Y');
  assert.equal(result.conversation_done, false);
  assert.equal(result.handoff_needed, false);
  assert.match(result.reply_text, /lender paperwork, follow-up, and negotiations on this file/);
  assert.match(result.reply_text, /Would it help to see what I'd need to get started\?/);
  assert.equal(h.state.call_booking_status, 'interested_no_call');
});

test('explicit present coverage still outranks undecided relationship wording', () => {
  const h = smsHarness();
  const result = h.incoming("I already have an attorney handling this file, but I haven't decided whether I should keep your information.");
  assert.equal(h.evaluate(`isCurrentFileDecisionUndecidedSignal_("I already have an attorney handling this file, but I haven't decided whether I should keep your information.")`), false);
  assert.equal(result.lead_status, 'O');
  assert.equal(result.conversation_done, true);
});

test('self-identified numbered Web Surveys poll is silent before generation', () => {
  const h = smsHarness();
  const result = h.incoming('Hi, I am Eva with Web Surveys. We are polling GA residents. Can you answer a quick poll? 1) Yes 2) No (or QUIT)');
  assert.equal(result.should_reply, false);
  assert.equal(result.handoff_needed, false);
  assert.equal(result.needs_review, false);
  assert.equal(h.effects.length, 0);
  assert.equal(h.state.history_json, '[]');
});

test('real-estate survey wording is not captured by the automation rule', () => {
  const h = smsHarness();
  assert.equal(h.evaluate(`isAutomatedPromotionalSmsSignal_("I am surveying the property. 1) Vacant 2) Occupied")`), false);
});
