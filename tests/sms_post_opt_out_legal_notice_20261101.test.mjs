import assert from 'node:assert/strict';
import test from 'node:test';
import {smsHarness} from './helpers/sms_core_harness.mjs';

test('STOP clears call state and a later regulatory notice stays silent with a compliance alert', () => {
  const h = smsHarness({
    call_eligible: 'yes',
    call_time_bucket: 'voice_call_1_due',
    call_scheduled_for: '2026-11-02T14:15:00Z',
    callback_requested: 'yes',
    callback_time: 'Tomorrow',
  });

  const stop = h.incoming('STOP', {messageId: 'legal-stop-1'});
  assert.equal(stop.should_reply, false);
  assert.equal(stop.lead_status, 'R');
  assert.equal(h.state.human_override, 'TRUE');
  assert.equal(h.state.call_eligible, '');
  assert.equal(h.state.call_time_bucket, '');
  assert.equal(h.state.call_scheduled_for, '');
  assert.equal(h.state.callback_requested, 'no');
  assert.equal(h.state.callback_time, '');

  const complaint = 'The unsolicited commercial communication violates the Telephone Consumer Protection Act and has been reported to the FCC and FTC.';
  const notice = h.incoming(complaint, {messageId: 'legal-stop-2'});
  assert.equal(notice.should_reply, false);
  assert.equal(notice.handoff_needed, true);
  assert.equal(notice.handoff_type, 'COMPLIANCE / LEGAL NOTICE');
  assert.equal(notice.lead_status, 'R');
  assert.equal(h.state.mailshake_status, 'R');
  assert.equal(h.state.ai_state, 'done');
  assert.equal(h.state.handoff_flag, 'FALSE');
  assert.equal(h.state.human_override, 'TRUE');
  assert.equal(h.state.conversation_summary, 'Opt-out retained; regulatory/legal notice owner alert recorded');
  assert.deepEqual(JSON.parse(JSON.stringify(h.effects)), [
    {type: 'handoff', reason: 'COMPLIANCE / LEGAL NOTICE'},
  ]);

  const duplicate = h.incoming('This TCPA violation was reported to the FCC.', {messageId: 'legal-stop-3'});
  assert.equal(duplicate.should_reply, false);
  assert.equal(duplicate.handoff_needed, false);
  assert.equal(h.effects.length, 1);
});

test('regulatory names without an enforcement claim do not become a legal notice', () => {
  const h = smsHarness({
    mailshake_status: 'R',
    human_override: 'TRUE',
    ai_state: 'done',
    handoff_flag: 'FALSE',
    response_status: 'STOP',
    conversation_summary: 'Opt-out / stop request',
  });
  const result = h.incoming('What does the FCC do?', {messageId: 'legal-control-1'});
  assert.notEqual(result.handoff_type, 'COMPLIANCE / LEGAL NOTICE');
  assert.equal(result.should_reply, false);
});
