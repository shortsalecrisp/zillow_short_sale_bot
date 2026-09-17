import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

function harness() {
  const sheets = new Map();
  const makeSheet = name => {
    const rows = [['created_at']];
    const sheet = {
      rows,
      getLastRow: () => rows.length,
      appendRow: row => rows.push(row),
      getRange: (start, _column, count) => ({
        getValues: () => rows.slice(start - 1, start - 1 + count),
        setValue: value => { rows[start - 1][1] = value; },
        setValues: values => { values.forEach((value, offset) => { rows[start - 1 + offset] = value; }); },
      }),
    };
    sheets.set(name, sheet);
    return sheet;
  };
  makeSheet('sms_inbound_queue');
  makeSheet('sms_handoff_email_outbox');
  const context = {
    HEADERS: {phone: 'phone', human_override: 'human_override', mailshake_status: 'mailshake_status',
      followup_text_sent: 'follow_up', last_message_id: 'last_message_id', last_inbound_text: 'last_inbound_text'},
    normalizePhone_: value => String(value || '').replace(/\D/g, '').slice(-10),
    normalizeWhitespace_: value => String(value || '').replace(/\s+/g, ' ').trim(),
    getSmsSpreadsheet_: () => ({getSheetByName: name => sheets.get(name)}),
    ensureSmsSheetHeaders_: () => {},
    appendSmsDebugLog_: () => {},
    getSheet_: () => ({}),
    getSheetData_: () => [],
    CacheService: {getScriptCache: () => ({get: () => null, put: () => {}})},
    LockService: {getScriptLock: () => ({tryLock: () => true, releaseLock: () => {}})},
    Utilities: {
      DigestAlgorithm: {SHA_256: 'sha256'},
      computeDigest: (_, value) => [...crypto.createHash('sha256').update(value).digest()],
      base64EncodeWebSafe: value => Buffer.from(value).toString('base64url'),
      getUuid: () => crypto.randomUUID(),
    },
  };
  vm.createContext(context);
  vm.runInContext(fs.readFileSync(new URL('../apps_script/sms_outbox.js', import.meta.url), 'utf8'), context);
  vm.runInContext('ensureSmsSheetHeaders_ = () => {}; ensureSmsOutboxTriggersBestEffortV14_ = () => {}; installSmsOutboxTriggers_ = () => {}; drainHandoffEmailOutboxV11_ = () => {};', context);
  return {context, sheets, run: expression => vm.runInContext(expression, context)};
}

test('scheduled follow-up is not compared to an inbound message ID', () => {
  const h = harness();
  const crm = {phone: '6309367057', human_override: 'FALSE', mailshake_status: '',
    followup_text_sent: '', last_message_id: ''};
  h.context.getSheetData_ = () => [{row: 5673, obj: crm}];
  const send = Array(19).fill('');
  send[3] = 'followup-5673-synthetic-id';
  send[4] = '6309367057';
  send[5] = 'Following up';
  send[6] = '__scheduled_followup__:{"crm_row":5673}';
  h.context.outboxRow = send;
  assert.equal(h.run('getPendingSmsStaleReason_(outboxRow)'), '');

  crm.last_message_id = 'real-agent-inbound';
  assert.equal(h.run('getPendingSmsStaleReason_(outboxRow)'), 'A newer substantive inbound message exists');
  crm.last_message_id = '';
  crm.followup_text_sent = 'x';
  assert.equal(h.run('getPendingSmsStaleReason_(outboxRow)'), 'Scheduled follow-up already recorded');
  crm.followup_text_sent = '';
  crm.mailshake_status = 'R';
  assert.equal(h.run('getPendingSmsStaleReason_(outboxRow)'), 'CRM lead no longer qualifies for follow-up');
});

test('two Tasker paths with different IDs queue one inbound and one owner forward', () => {
  const h = harness();
  h.context.first = {phone: '4135191061', message: 'Please call me', message_id: 'received-text-1', received_at: '9-14-26 15.01'};
  h.context.second = {phone: '4135191061', message: 'Please call me', message_id: 'smsdb-99', received_at: '9-14-26 15.02'};
  const first = h.run('enqueueIncomingSmsV10_(first, "request-1")');
  const second = h.run('enqueueIncomingSmsV10_(second, "request-2")');
  assert.equal(first.queued, true);
  assert.equal(second.queued, false);
  assert.equal(second.duplicate, true);
  assert.equal(h.sheets.get('sms_inbound_queue').rows.length, 2);
});

test('same handoff event with changed history is emailed only once', () => {
  const h = harness();
  h.context.first = {to: 'owner@example.com', subject: 'NEW LEAD - CALL REQUESTED', body: 'First history', event_key: 'same-agent-message'};
  h.context.second = {to: 'owner@example.com', subject: 'NEW LEAD - CALL REQUESTED', body: 'Expanded history', event_key: 'same-agent-message'};
  const first = h.run('queueHandoffEmailV11_(first)');
  const second = h.run('queueHandoffEmailV11_(second)');
  assert.equal(first.queued, true);
  assert.equal(second.queued, false);
  assert.equal(second.duplicate, true);
  assert.equal(h.sheets.get('sms_handoff_email_outbox').rows.length, 2);
});

test('human-owned updates coalesce into one delayed latest-context owner alert', () => {
  const h = harness();
  h.context.first = {to: 'owner@example.com', subject: 'HUMAN HANDOFF UPDATE', body: 'Thursday at 11 AM', event_key: 'date', coalesce_key: '7025550101'};
  h.context.second = {to: 'owner@example.com', subject: 'HUMAN HANDOFF UPDATE', body: 'Video; invite to two addresses', event_key: 'video', coalesce_key: '7025550101'};
  const first = h.run('queueCoalescedHandoffEmailV18_(first)');
  const duplicate = h.run('queueCoalescedHandoffEmailV18_(first)');
  const second = h.run('queueCoalescedHandoffEmailV18_(second)');
  const replay = h.run('queueCoalescedHandoffEmailV18_(first)');
  assert.equal(first.queued, true);
  assert.equal(duplicate.duplicate, true);
  assert.equal(second.coalesced, true);
  assert.equal(replay.duplicate, true);
  const rows = h.sheets.get('sms_handoff_email_outbox').rows;
  assert.equal(rows.length, 2);
  assert.equal(JSON.parse(rows[1][3]).body, 'Video; invite to two addresses');
  assert.ok(new Date(rows[1][8]).getTime() > Date.now());
});
