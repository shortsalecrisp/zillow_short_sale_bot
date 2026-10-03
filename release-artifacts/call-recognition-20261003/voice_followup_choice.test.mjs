import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

if (!process.env.APPS_SCRIPT_CANDIDATE_PATH) throw new Error('Set APPS_SCRIPT_CANDIDATE_PATH to the guarded live-source candidate');
const source = fs.readFileSync(process.env.APPS_SCRIPT_CANDIDATE_PATH, 'utf8');
const context = {Date, JSON, console,
  PropertiesService: {getScriptProperties: () => ({getProperty: () => ''})},
  Session: {getScriptTimeZone: () => 'America/New_York'},
  Utilities: {formatDate: (date) => new Date(date).toISOString().slice(0, 10)}};
vm.createContext(context);
vm.runInContext(source, context);
const invitation = 'Following up on 123 Main St: I can help with short-sale lender paperwork. Maya, my automated assistant, may call from 217-634-1017. Prefer email or a call with Yoni? For a call, send a day, time and time zone.';
const row = {last_outbound_text: invitation, email: '', history_json: '[]', mailshake_status: ''};
const decide = (text, overrides = {}) => context.applyFastRules_(text, {...row, ...overrides}, new Date('2026-10-03T22:50:00Z'));

test('simple email choice uses existing information approval path, not callback', () => {
  for (const text of ['Email', 'Email please', 'Please email me, no calls']) {
    const result = decide(text);
    assert.equal(result.matched, true, text);
    assert.equal(result.callback_requested, 'no');
    assert.equal(result.handoff_needed, false);
    assert.equal(result.send_info_email, false);
    assert.match(result.reply_text, /best email/i);
  }
  const result = decide('Email please', {email: 'agent@example.com'});
  assert.equal(result.send_info_email, true);
  assert.equal(result.info_email_to, 'agent@example.com');
});

test('human callback preserves exact requested date clock and zone without confirming booking', () => {
  for (const text of ['Yoni Monday at 3:15 PM Pacific', 'Monday 9:30am Eastern', '15:15 Monday']) {
    const result = decide(text);
    assert.equal(result.matched, true, text);
    assert.equal(result.handoff_needed, true);
    assert.equal(result.call_booking_status, 'scheduled_callback');
    assert.equal(result.callback_time, text);
    assert.equal(result.send_reply_before_handoff, true);
    assert.match(result.reply_text, /Yoni to confirm/);
    assert.doesNotMatch(result.reply_text, /scheduled|booked|will call/i);
  }
});

test('human preference is not mistaken for automated-assistant consent', () => {
  const result = decide('Please have Yoni call instead of Maya');
  assert.equal(result.handoff_needed, true);
  assert.equal(result.callback_requested, 'yes');
  assert.equal(result.callback_time, '');
  assert.match(result.reply_text, /day, time and time zone/);
  assert.match(result.reason, /not an automated appointment/);
});

test('written-only and no-robot choices suppress cold calls without full opt-out', () => {
  for (const text of ['No robot calls please', 'Text only please', 'Do not call me']) {
    const result = decide(text);
    assert.equal(result.matched, true, text);
    assert.equal(result.lead_status, 'Y');
    assert.equal(result.callback_requested, 'no');
    assert.equal(result.handoff_needed, false);
    assert.equal(result.call_booking_status, 'interested_no_call');
  }
});

test('stop remains terminal and Maya identity is truthful', () => {
  const stop = decide('Please stop');
  assert.equal(stop.lead_status, 'R');
  assert.equal(stop.conversation_done, true);
  assert.equal(stop.callback_requested, 'no');
  const identity = decide('Who is Maya?');
  assert.match(identity.reply_text, /automated phone assistant/);
  assert.equal(identity.callback_requested, 'no');
});

test('scoped handler cannot reclassify unrelated historical replies or hide fee questions', () => {
  assert.equal(context.buildVoiceFollowupChoiceDecisionV1_('Monday 9:30am', {...row, last_outbound_text: 'Can we arrange a showing?'}, new Date()), null);
  assert.equal(context.buildVoiceFollowupChoiceDecisionV1_('Monday at 3pm, what does it cost?', row, new Date()), null);
  assert.equal(context.buildVoiceFollowupChoiceDecisionV1_('Maybe a call Monday', row, new Date()), null);
  const vague = decide('Yes');
  assert.equal(vague.callback_requested, 'no');
  assert.equal(vague.handoff_needed, false);
  assert.match(vague.reply_text, /prefer email/);
});
