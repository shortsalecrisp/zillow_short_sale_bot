import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";

const source = fs.readFileSync(path.join(import.meta.dirname, "../apps_script/sms_outbox.js"), "utf8");
const extract = (name, nextName) => source.slice(
  source.indexOf(`function ${name}(`),
  source.indexOf(`function ${nextName}(`),
);

function harness() {
  const rows = [];
  const leadRows = [];
  const sheet = {
    getLastRow: () => rows.length + 1,
    getRange: (_row, _col, count) => ({ getValues: () => rows.slice(0, count) }),
    appendRow: (row) => rows.push(row),
  };
  const context = {
    Date,
    JSON,
    Number,
    String,
    SMS_PENDING_SEND_HEADERS_: Array.from({ length: 19 }),
    normalizePhone_: (value) => String(value).replace(/\D/g, "").replace(/^1(?=\d{10}$)/, ""),
    normalizeWhitespace_: (value) => value.trim().replace(/\s+/g, " "),
    normalizePendingSmsInboundText_: (value) => String(value || "").trim().toLowerCase(),
    installSmsOutboxTriggers_: () => {},
    ensureSmsSheetHeaders_: () => {},
    getSmsSpreadsheet_: () => ({ getSheetByName: (name) => name === "sms_pending_sends" ? sheet : null }),
    LockService: { getScriptLock: () => ({ tryLock: () => true, releaseLock: () => {} }) },
    Utilities: { getUuid: () => "generated-id" },
    getSheet_: () => ({}),
    getSheetData_: () => leadRows,
    HEADERS: {
      phone: "phone",
      human_override: "human_override",
      mailshake_status: "mailshake_status",
      followup_text_sent: "followup_text_sent",
      last_message_id: "last_message_id",
      last_inbound_text: "last_inbound_text",
    },
  };
  vm.createContext(context);
  vm.runInContext(extract("getCrmFollowupSmsStatusV18_", "enqueueFollowupSmsV13_"), context);
  vm.runInContext(extract("enqueueFollowupSmsV13_", "resolveInitialSmsReceiptRowV14_"), context);
  vm.runInContext(extract("getPendingSmsStaleReason_", "findSmsCrmRowByPhone_"), context);
  return { context, rows, leadRows };
}

const identity = {
  crm_prospect_id: "e1f3e796-ff41-4c8c-8991-da6ca179d988",
  phone: "14057723242",
  message: "Approved follow-up",
  message_id: "crm-followup-0123456789abcdef01234567",
  request_id: "render-crm-followup-0123456789abcdef01234567",
};

test("CRM-only approved text enters the existing Tasker outbox and dedupes", () => {
  const { context, rows } = harness();
  const first = context.enqueueFollowupSmsV13_(identity, "webhook-id");
  assert.equal(first.status, "queued");
  assert.equal(rows.length, 1);
  assert.equal(rows[0][17], 0);
  assert.match(rows[0][6], /^__crm_followup__:/);
  assert.equal(context.getPendingSmsStaleReason_(rows[0]), "");

  const duplicate = context.enqueueFollowupSmsV13_(identity, "another-webhook-id");
  assert.equal(duplicate.duplicate, true);
  assert.equal(rows.length, 1);

  rows[0][1] = "sent";
  rows[0][14] = new Date();
  assert.equal(context.getCrmFollowupSmsStatusV18_({ request_id: identity.request_id }).status, "sent");
});

test("CRM-only outbox stops uncertain duplicates and expired sends", () => {
  const { context, rows } = harness();
  context.enqueueFollowupSmsV13_(identity, "webhook-id");
  rows[0][1] = "uncertain";
  const duplicate = context.enqueueFollowupSmsV13_(identity, "another-webhook-id");
  assert.equal(duplicate.ok, false);
  assert.equal(rows.length, 1);

  rows[0][0] = new Date(Date.now() - 31 * 60 * 1000);
  assert.equal(context.getPendingSmsStaleReason_(rows[0]), "CRM follow-up expired before handset send");
});

test("owner-approved sheet follow-up is not mistaken for the scheduled follow-up", () => {
  const { context, rows, leadRows } = harness();
  const requestId = "render-followup-5890-e8df1acc217bc4f4";
  const queued = context.enqueueFollowupSmsV13_({
    row: 5890,
    phone: "4057723241",
    message: "Owner-approved package follow-up",
    message_id: "followup-5890-e8df1acc217bc4f4",
    request_id: requestId,
  }, "webhook-id");
  assert.equal(queued.status, "queued");
  leadRows.push({ row: 5890, obj: {
    phone: "4057723241",
    mailshake_status: "N",
    followup_text_sent: "x",
  } });
  assert.equal(context.getPendingSmsStaleReason_(rows[0]), "");
  rows[0][1] = "sent";
  rows[0][14] = new Date("2026-09-30T18:39:12Z");
  assert.equal(context.getCrmFollowupSmsStatusV18_({ request_id: requestId }).status, "sent");

  rows[0][2] = "scheduler-followup-5890-e8df1acc217bc4f4";
  assert.equal(context.getPendingSmsStaleReason_(rows[0]), "Scheduled follow-up already recorded");
});
