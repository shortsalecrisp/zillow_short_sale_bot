import test from 'node:test';
import assert from 'node:assert/strict';
import {smsHarness} from './helpers/sms_core_harness.mjs';

const answer = (text, row = {}, options = {}) => {
  const h = smsHarness(row, options);
  return {h, r: h.incoming(text)};
};

test('neutral self-handling gets one transparent value response, then closes', () => {
  const h = smsHarness();
  const first = h.incoming("I'm handling it myself thank you");
  assert.equal(first.should_reply, true);
  assert.match(first.reply_text, /You keep the listing and client relationship/);
  assert.match(first.reply_text, /buyer pays my fee at closing/);
  assert.match(first.reply_text, /Would a brief call/);
  assert.equal(JSON.parse(h.state.history_json).at(-1).response_id, 'self_handling_value');
  const second = h.incoming('I am doing it myself');
  assert.equal(second.lead_status, 'R');
  assert.match(second.reply_text, /^Ok, no problem/);
});

for (const text of ["What's the cost?", 'How much?', 'How much is your fee?', 'What do you charge?', 'What does your service cost?', 'What work would you take off my plate and how much is the flat fee?']) {
  test(`amount question answered directly: ${text}`, () => {
    const {r} = answer(text);
    assert.equal(r.should_reply, true);
    assert.match(r.reply_text, /\$5,000/);
    assert.match(r.reply_text, /only if the deal closes/);
    assert.equal(r.handoff_needed, false);
  });
}

test('payer, amount, and buyer concern are distinct questions, not a loop', () => {
  const h = smsHarness();
  assert.doesNotMatch(h.incoming('How do you get paid?').reply_text, /5,000/);
  assert.match(h.incoming('How much is the fee?').reply_text, /5,000/);
  const r = h.incoming("Won't the buyer lower their offer because of that cost?");
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /buyer does need to consider that cost/);
  assert.equal(r.handoff_needed, false);
});

test('pending-only payer explanation does not advance fee history', () => {
  const {r} = answer('How do you get paid?', {}, {pendingFeeStage: 'initial_pending'});
  assert.doesNotMatch(r.reply_text, /5,000/);
});

for (const text of ["I'm confused about how the buyer pays you", "What if the buyer won't pay the fee?", 'Will the buyer lower their offer because of the cost?']) {
  test(`buyer economics are answered without an automatic loop: ${text}`, () => {
    const h = smsHarness();
    h.incoming('What is your fee?');
    const r = h.incoming(text);
    assert.equal(r.should_reply, true);
    assert.equal(r.handoff_needed, false);
    assert.match(r.reply_text, /buyer does need to consider that cost/);
  });
}

test('existing provider and explicit refusal outrank Equator, including at cap', () => {
  for (const text of ['I have a negotiator handling this in Equator, thanks.', 'No thanks, I handle Equator myself.']) {
    for (const count of [0, 3]) {
      const {r} = answer(text, {auto_reply_count: count});
      assert.equal(r.lead_status, 'R');
      assert.equal(r.handoff_needed, false);
      assert.equal(r.should_reply, count < 3);
    }
  }
});

test('not a short sale at cap still updates R without an extra SMS', () => {
  const {h, r} = answer('This is not a short sale', {auto_reply_count: 3});
  assert.equal(r.should_reply, false);
  assert.equal(h.state.mailshake_status, 'R');
  assert.equal(h.effects.length, 0);
});

test('negated rejection does not close and buyer-role answer is factual', () => {
  const {r} = answer("I'm not saying I'm not interested. So you bring the buyer?!");
  assert.equal(r.should_reply, true);
  assert.notEqual(r.lead_status, 'R');
  assert.match(r.reply_text, /don't bring the buyer/);
  assert.doesNotMatch(r.reply_text, /never any issue/);
});

for (const text of ['Who collects the documents?', 'How can you speed up the process?']) {
  test(`materials sanitizer preserves substantive answer: ${text}`, () => {
    const {r} = answer(text);
    assert.equal(r.should_reply, true);
    assert.match(r.reply_text, /documents/);
    assert.doesNotMatch(r.reply_text, /^I appreciate it/);
  });
}

test('supported multi-question answer covers location, fee, scope, and tenure', () => {
  const {r} = answer('Are you local? What do you do? What is your fee? How long have you handled short sales?');
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /Atlanta/);
  assert.match(r.reply_text, /paperwork/);
  assert.match(r.reply_text, /5,000/);
  assert.match(r.reply_text, /15 years/);
});

test('email before deciding on call follows approval workflow without call consent', () => {
  const {h, r} = answer('Email me the details at taylor@example.com before I decide whether to schedule a call.');
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /I'll send an overview/);
  assert.match(r.reply_text, /taylor@example.com/);
  assert.doesNotMatch(r.reply_text, /shortly|already sent|Thanks for sending/);
  assert.equal(r.handoff_needed, false);
  assert.deepEqual(JSON.parse(JSON.stringify(h.effects)), [{type: 'email_approval', to: 'taylor@example.com'}]);
  assert.notEqual(h.state.call_booking_status, 'scheduled_callback');
});

test('email missing collects address; future info with provider remains O', () => {
  assert.match(answer('Please send me some information').r.reply_text, /What's the best email for an overview/);
  const {r} = answer('I already have someone helping but email your information for the future', {email: 'future@example.com'});
  assert.equal(r.lead_status, 'O');
  assert.match(r.reply_text, /future@example.com/);
});

test('failed approval enqueue cannot return a false email promise', () => {
  const h = smsHarness({}, {emailFailure: true});
  assert.throws(() => h.incoming('Email me at taylor@example.com'), /approval unavailable/);
  assert.equal(h.state.auto_reply_count, 0);
});

test('verified tenure + unknown recent count gets answer and named handoff', () => {
  const {h, r} = answer("What's your fee and how many short sales have you closed in the last six months?");
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /15 years/);
  assert.match(r.reply_text, /5,000/);
  assert.match(r.reply_text, /(?:don't have|verify|confirm|check).{0,60}(?:count|number|figure)/i);
  assert.equal(r.handoff_needed, true);
  assert.equal(h.effects[0].reason, 'STATS QUESTION');
});

test('fee plus requested call may answer once, but never reopens old handoff', () => {
  const {h, r} = answer('What is your fee? Please call me tomorrow at 3 pm ET.');
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /5,000/);
  assert.equal(r.handoff_needed, true);
  assert.match(h.state.callback_time, /Tomorrow.*3\s*pm.*ET/i);
  assert.equal(h.incoming('And how do you get paid?').should_reply, false);
  const locked = answer('What is your fee?', {human_override: 'TRUE', handoff_flag: 'TRUE', ai_state: 'handoff', conversation_summary: 'max replies reached'});
  assert.equal(locked.r.should_reply, false);
});

test('fee plus present help preserves an owner alert after the answer', () => {
  const {h, r} = answer('I need help. What is your fee?');
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /5,000/);
  assert.equal(r.handoff_needed, true);
  assert.match(h.effects[0].reason, /FEE AND HELP/);
  assert.equal(h.state.human_override, 'TRUE');
});

test('callback keeps the date, timing qualifier, and timezone', () => {
  const {h, r} = answer('What is your fee? Call me tomorrow after 2 PM Eastern.');
  assert.equal(r.handoff_needed, true);
  assert.match(h.state.callback_time, /Tomorrow after 2\s*pm Eastern/i);
});

test('amount plus buyer concern answers both questions', () => {
  const {r} = answer('What is your fee, and what if the buyer cannot afford it?');
  assert.equal(r.should_reply, true);
  assert.equal(r.handoff_needed, false);
  assert.match(r.reply_text, /5,000/);
  assert.match(r.reply_text, /buyer does need to consider that cost/);
});

test('existing first-fee-after-closeout cap exception also covers a compound question', () => {
  const {r} = answer('Are you local? What do you do? What is your fee?', {
    auto_reply_count: 3, mailshake_status: 'O', ai_state: 'done',
    last_outbound_text: 'Ok, no problem. Please keep me in mind.'
  });
  assert.equal(r.should_reply, true);
  assert.equal(r.lead_status, 'O');
  assert.match(r.reply_text, /5,000/);
  assert.match(r.reply_text, /Atlanta/);
  assert.match(r.reply_text, /paperwork/);
});

test('agreement request preserves specific document handoff and existing cap exception', () => {
  const h = smsHarness({auto_reply_count: 3});
  const message = 'Please email me the current agreement and fee schedule at taylor@example.com.';
  const r = h.incoming(message, {deliver: false, messageId: 'documents-new'});
  assert.equal(r.should_reply, true);
  assert.equal(r.handoff_needed, true);
  assert.match(r.reply_text, /agreement and fee schedule/);
  assert.match(h.effects[0].reason, /DOCUMENTS/);
  assert.equal(h.effects.some(effect => effect.type === 'email_approval'), false);
  const outbox = ['', 'queued', 'request-docs', 'documents-new', h.state.phone, r.reply_text, message];
  assert.equal(h.evaluate(`getPendingSmsStaleReason_(${JSON.stringify(outbox)})`), '');
});

test('auction deadline is a review, never callback or generic timeline', () => {
  const {h, r} = answer('The auction is tomorrow at 3 pm. Can you stop it?');
  assert.equal(r.handoff_needed, true);
  assert.notEqual(h.state.call_booking_status, 'scheduled_callback');
  assert.doesNotMatch(r.reply_text || '', /60-90/);
  assert.match(h.effects[0].reason, /DEADLINE|AUCTION/);
});

test('failed prior processor gets acknowledgment and a concrete scope', () => {
  const {r} = answer('The last company took 2% and did nothing. I did all the work. What would you do differently?');
  assert.equal(r.should_reply, true);
  assert.match(r.reply_text, /why you'd be cautious/);
  assert.match(r.reply_text, /paperwork, calls, follow-up, and negotiations/);
});

test('opt out remains silent and immediate', () => {
  const {r} = answer('Please remove me from your database');
  assert.equal(r.should_reply, false);
  assert.equal(r.lead_status, 'R');
});

test('new-handoff answer survives outbox checks only for that exact inbound and destination', () => {
  const h = smsHarness();
  const message = 'What is your fee? Please call me tomorrow at 3 pm ET.';
  const r = h.incoming(message, {deliver: false, messageId: 'handoff-new'});
  assert.equal(r.should_reply, true);
  const row = ['', 'queued', 'request-test', 'handoff-new', h.state.phone, r.reply_text, message];
  const stale = v => h.evaluate(`getPendingSmsStaleReason_(${JSON.stringify(v)})`);
  assert.equal(stale(row), '');
  assert.equal(stale(row.map((v, i) => i === 3 ? 'different-inbound' : v)), 'Human takeover is active');
  assert.equal(stale(row.map((v, i) => i === 5 ? 'different reply' : v)), 'Human takeover is active');
  assert.equal(stale(row.map((v, i) => i === 4 ? '2025550199' : v)), 'CRM row no longer matches destination');
  h.evaluate("applyOverrideCoreV11_('2025550101', 'TRUE')");
  assert.equal(stale(row), 'Human takeover is active');
});

test('an expired new-handoff permission cannot cause a late automatic reply', () => {
  const h = smsHarness();
  const message = 'What is your fee? Please call me.';
  const r = h.incoming(message, {deliver: false, messageId: 'handoff-expired'});
  const key = 'SMS_NEW_HANDOFF_REPLY_V1_2025550101';
  const permit = JSON.parse(h.props.getProperty(key));
  h.props.setProperty(key, JSON.stringify({...permit, expires_at: 1}));
  assert.equal(h.evaluate(`getPendingSmsStaleReason_(${JSON.stringify(['', 'queued', 'r', 'handoff-expired', h.state.phone, r.reply_text, message])})`), 'Human takeover is active');
});

test('a question at the normal reply cap hands off without creating a send permission', () => {
  const {h, r} = answer('What is your fee? Please call me tomorrow at 3 pm ET.', {auto_reply_count: 3});
  assert.equal(r.should_reply, false);
  assert.equal(h.props.getProperty('SMS_NEW_HANDOFF_REPLY_V1_2025550101'), null);
  assert.equal(h.effects[0].type, 'handoff');
});

test('delivered response IDs survive wording differences without relying on queued text', () => {
  const h = smsHarness({last_outbound_text: 'A prior approved version of the explanation.',
    history_json: JSON.stringify([{role: 'assistant', text: 'A prior approved version of the explanation.', response_id: 'self_handling_value', receipt_id: 'delivered'}])});
  assert.equal(h.incoming('I am handling it myself').lead_status, 'R');
  const fee = smsHarness({history_json: JSON.stringify([{role: 'assistant', text: 'Legacy exact price.', response_id: 'fee_specific', receipt_id: 'delivered'}])});
  assert.equal(fee.incoming('How much is the fee?').handoff_needed, true);
});

test('new questions and manual locks still outrank a courteous refusal fragment', () => {
  for (const message of ["I'm handling it myself, but how much is your fee?", "Thanks for reaching out. Who collects the documents?"]) {
    const {r} = answer(message);
    assert.equal(r.should_reply, true);
    assert.doesNotMatch(r.reply_text, /^Ok, no problem/);
    assert.equal(answer(message, {human_override: 'TRUE'}).r.should_reply, false);
  }
});

test('receipt ledger records delivered response ID once, atomically with reply count', () => {
  const h = smsHarness();
  const r = h.incoming('What is your fee?', {deliver: false, messageId: 'fee-ledger'});
  assert.equal(h.state.auto_reply_count, 0);
  assert.equal(JSON.parse(h.state.history_json).some(entry => entry.role === 'assistant'), false);
  h.evaluate(`
    var ledgerHeaders = [...new Set([...Object.values(HEADERS), ...Object.keys(state)])];
    getSheet_ = () => ({
      getLastColumn: () => ledgerHeaders.length,
      getRange: (row) => ({
        getValues: () => [row === 1 ? ledgerHeaders : ledgerHeaders.map(key => state[key])],
        setValues: rows => ledgerHeaders.forEach((key, index) => {state[key] = rows[0][index];})
      })
    });
  `);
  const receipt = {phone: h.state.phone, reply_text: r.reply_text,
    sent_at: '2026-09-12T18:00:00.000Z', message_id: 'fee-ledger',
    request_id: 'verified-request', lease_token: 'verified-lease'};
  const apply = () => h.evaluate(`handleReplySent_(${JSON.stringify(receipt)})`);
  assert.equal(apply().ok, true);
  const delivered = JSON.parse(h.state.history_json).filter(entry => entry.role === 'assistant');
  assert.equal(delivered.length, 1);
  assert.equal(delivered[0].response_id, 'fee_specific');
  assert.ok(delivered[0].receipt_id);
  assert.equal(h.state.auto_reply_count, 1);
  assert.equal(apply().duplicate, true);
  assert.equal(h.state.auto_reply_count, 1);
  assert.equal(JSON.parse(h.state.history_json).filter(entry => entry.role === 'assistant').length, 1);
});
