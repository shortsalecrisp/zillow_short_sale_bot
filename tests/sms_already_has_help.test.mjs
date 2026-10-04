import test from 'node:test';
import assert from 'node:assert/strict';
import {smsHarness} from './helpers/sms_core_harness.mjs';

const backupReply = "Totally understand. Glad you already have help on it. If anything changes or the file stalls, please keep me in mind. I'm happy to be backup.";

for (const text of [
  'I already have a negotiator.',
  'We already have a processor handling it.',
  'I have an attorney handling the short sale.',
  'Our title company is handling the short sale.',
]) {
  test(`existing provider receives the approved backup acknowledgment: ${text}`, () => {
    const h = smsHarness();
    const r = h.incoming(text);
    assert.equal(r.reply_text, backupReply);
    assert.equal(r.handoff_needed, false);
    assert.equal(h.state.mailshake_status, 'R');
    assert.equal(h.state.ai_state, 'done');
    assert.doesNotMatch(r.reply_text, /\$|credit|referral|bonus/);
    assert.equal(h.incoming('Thank you').should_reply, false);
  });
}

for (const text of ['No thanks, I already have an attorney.', 'Not interested, we have a negotiator.']) {
  test(`explicit refusal retains the no-interest closeout: ${text}`, () => {
    const h = smsHarness();
    const r = h.incoming(text);
    assert.equal(r.reply_text, h.evaluate('getStandardNoCloseoutReply_()'));
    assert.equal(h.state.mailshake_status, 'R');
  });
}

test('opt-out with existing coverage remains silent', () => {
  const h = smsHarness();
  const r = h.incoming('Stop. I already have a negotiator.');
  assert.equal(r.should_reply, false);
  assert.equal(h.state.mailshake_status, 'R');
});

test('existing coverage does not replace a substantive fee answer', () => {
  const h = smsHarness();
  const r = h.incoming('I already have an attorney, but what is your fee?');
  assert.match(r.reply_text, /\$5,000/);
  assert.notEqual(r.reply_text, backupReply);
});

test('human takeover prevents an automated backup response', () => {
  const h = smsHarness({human_override: 'TRUE', handoff_flag: 'TRUE', ai_state: 'handoff'});
  const r = h.incoming('I already have a negotiator.');
  assert.equal(r.should_reply, false);
});

for (const text of ['Who gets the credit?', 'Can I receive the $1,000?', 'Can the referral fee go to my broker?', 'How is the first file credit applied?']) {
  test(`credit allocation is handed to Yoni without an automated payment promise: ${text}`, () => {
    const h = smsHarness();
    const r = h.incoming(text);
    assert.equal(r.should_reply, false);
    assert.equal(r.handoff_needed, true);
    assert.equal(h.effects.at(-1).reason, 'FIRST-FILE CREDIT REVIEW');
  });
}

test('seller credit score question does not trigger compensation review', () => {
  const h = smsHarness();
  assert.equal(h.evaluate('isFirstFileCreditQuestionSignal_("The seller has a low credit score")'), false);
});
