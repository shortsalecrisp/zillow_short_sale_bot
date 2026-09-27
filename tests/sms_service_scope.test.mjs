import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { smsHarness } from './helpers/sms_core_harness.mjs';

const cases = JSON.parse(fs.readFileSync(new URL('./fixtures/sms_service_scope_cases.json', import.meta.url)));
const priorAccess = [{ role: 'agent', text: cases.property_requests[0] },
  { role: 'assistant', text: cases.unsafe_replies[0] }];
const scope = h => h.evaluate('buildServiceScopeClarificationReply_()');

for (const message of cases.property_requests) {
  test(`property request corrects role without scheduling: ${message}`, () => {
    const h = smsHarness();
    const r = h.incoming(message);
    assert.equal(r.reply_text, scope(h));
    assert.equal(r.should_reply, true);
    assert.equal(r.handoff_needed, false);
    assert.notEqual(h.state.call_booking_status, 'scheduled_callback');
    assert.notEqual(h.state.callback_requested, 'yes');
    assert.equal(h.effects.length, 0);
  });
}
for (const message of cases.context_followups) {
  test(`property thread continuation is not a phone appointment: ${message}`, () => {
    const h = smsHarness({history_json: JSON.stringify(priorAccess)});
    const r = h.incoming(message);
    assert.equal(r.reply_text, scope(h));
    assert.equal(r.should_reply, true);
    assert.equal(h.effects.length, 0);
    assert.notEqual(h.state.callback_requested, 'yes');
  });
}
for (const message of cases.buyers) {
  test(`buyer request gives the processing scope: ${message}`, () => {
    const r = smsHarness().incoming(message);
    assert.equal(r.should_reply, true);
    assert.match(r.reply_text, /don't bring the buyer/);
    assert.match(r.reply_text, /lender paperwork, calls, follow-up/);
  });
}
for (const message of cases.in_scope) {
  test(`in-scope message is not a property task: ${message}`, () => {
    const h = smsHarness({history_json: JSON.stringify(priorAccess)});
    assert.equal(h.evaluate(`isPropertyLogisticsRequest_(${JSON.stringify(message)}, state)`), false);
  });
}
for (const reply of cases.unsafe_replies) {
  test(`last-mile reply guard blocks unsupported promise: ${reply}`, () => {
    const h = smsHarness();
    const d = h.evaluate(`applyReplySanitizers_({reply_text:${JSON.stringify(reply)}}, state)`);
    assert.equal(d.reply_text, scope(h));
    assert.notEqual(d.callback_requested, 'yes');
    assert.notEqual(d.call_booking_status, 'scheduled_callback');
  });
}
for (const reply of cases.safe_replies) {
  test(`output guard allows valid processing statements: ${reply}`, () => {
    const h = smsHarness();
    assert.equal(h.evaluate(`isUnsupportedPropertyServicePromise_(${JSON.stringify(reply)})`), false);
  });
}
test('Gerine transcript remains corrective instead of inheriting the invented visit', () => {
  const history = [...priorAccess,
    {role:'agent',text:cases.context_followups[0]},
    {role:'assistant',text:'Gerine, got it. I can reschedule. What days/times work best for you or the sellers?'},
    {role:'agent',text:cases.context_followups[1]},
    {role:'assistant',text:"Gerine, how about tomorrow at 2:00 PM or Thursday at 10:00 AM? Tell me which one works and I'll note it."}];
  const h = smsHarness({history_json:JSON.stringify(history),auto_reply_count:2});
  const r = h.incoming(cases.context_followups[2]);
  assert.equal(r.reply_text,scope(h));
  assert.equal(r.handoff_needed,false);
});
test('opt-out, owner takeover and reply cap remain authoritative', () => {
  assert.equal(smsHarness().incoming('Remove me. The lockbox is broken.').should_reply,false);
  for (const overrides of [{human_override:'TRUE',ai_state:'handoff',handoff_flag:'TRUE'}, {auto_reply_count:3}]) {
    const h = smsHarness({...overrides,history_json:JSON.stringify(priorAccess)});
    const r = h.incoming('You can reschedule through Showing Time tomorrow at 2');
    assert.equal(r.should_reply,false);
    assert.notEqual(h.state.call_booking_status,'scheduled_callback');
  }
});
test('a real phone call after clarification uses the existing callback flow', () => {
  const h = smsHarness();
  h.incoming(cases.property_requests[0]);
  const r = h.incoming('Can you call me tomorrow at 3 about your short sale service?');
  assert.equal(r.handoff_needed,true);
  assert.equal(h.state.call_booking_status,'scheduled_callback');
});
test('buyer question still answers an explicit fee question in the same message', () => {
  const r = smsHarness().incoming('Do you have buyers, and what is your fee?');
  assert.match(r.reply_text,/don't bring the buyer/);
  assert.match(r.reply_text,/\$5,000/);
});
test('unsafe output with a handoff cannot record a fake appointment', () => {
  const h = smsHarness();
  const d = h.evaluate(`applyReplySanitizers_({reply_text:"I can schedule the appraisal tomorrow.",handoff_needed:true,call_booking_status:'scheduled_callback',callback_time:'tomorrow'}, state)`);
  assert.equal(d.handoff_needed,true);
  assert.equal(d.reply_text,'');
  assert.equal(d.call_booking_status,'');
  assert.equal(d.callback_time,'');
});
test('live intent-contract diagnostics cover the service boundary', () => {
  const h = smsHarness();
  const result = h.evaluate('testSmsIntentContractV3_()');
  const scoped = result.cases.filter(c => c.name.startsWith('service_scope_'));
  assert.equal(scoped.length,6);
  assert.ok(scoped.every(c=>c.passed),JSON.stringify(scoped));
});
