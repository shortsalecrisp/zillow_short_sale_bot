import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = fs.readFileSync(new URL("../apps_script/sms_outbox.js", import.meta.url), "utf8");
const unavailable = () => new Error("Service Spreadsheets failed while accessing document with id sheet-id.");

function fixture() {
  let now = Date.parse("2026-11-03T15:00:00Z"), readFailures = 0, writeFailure = "", writes = 0, releases = 0;
  let sequence = 0, reads = 0;
  const values = {}, sleeps = [], alerts = [], logs = [], debug = [];
  const row = (id, status = "queued") => Object.assign(Array(19).fill(""), {
    0: new Date(now - 60000), 1: status, 2: `request-${id}`, 3: `message-${id}`,
    4: "2125550100", 5: "Existing answer", 8: new Date(now - 1000), 9: 0,
  });
  const rows = [Array(19).fill(""), row(1), row(2)];
  const sheet = {
    getLastRow() { reads++; if (readFailures-- > 0) throw unavailable(); return rows.length; },
    getRange(start, col, count = 1, width = 1) {
      return {
        getValues: () => Array.from({ length: count }, (_, i) =>
          Array.from({ length: width }, (_, j) => rows[start - 1 + i]?.[col - 1 + j] ?? "")),
        setValues(block) {
          writes++;
          if (writeFailure === "before") throw unavailable();
          block.forEach((r, i) => r.forEach((v, j) => { rows[start - 1 + i][col - 1 + j] = v; }));
          if (writeFailure === "after") throw unavailable();
        },
        setValue(value) { writes++; rows[start - 1][col - 1] = value; },
      };
    },
  };
  const lock = { tryLock: () => true, releaseLock: () => { releases++; } };
  const context = {
    Date: class extends Date { static now() { return now; } },
    console: { warn: text => logs.push(text) },
    Utilities: { sleep: ms => sleeps.push(ms), getUuid: () => `lease-${++sequence}` },
    LockService: { getScriptLock: () => lock, getUserLock: () => lock },
    PropertiesService: { getScriptProperties: () => ({
      getProperty: key => values[key] || null, setProperty: (key, value) => { values[key] = value; },
    }) },
    getSmsSpreadsheet_: () => ({ getSheetByName: () => sheet }),
    normalizePhone_: phone => String(phone || "").replace(/\D/g, ""),
    sendSystemAlertEmail_: (subject, body) => alerts.push({ subject, body }),
    appendSmsDebugLog_: (stage, data) => debug.push({ stage, data }),
  };
  vm.runInNewContext(source, context);
  context.ensureSmsSheetHeaders_ = () => {};
  context.getPendingSmsStaleReason_ = () => "";
  return { context, rows, values, sleeps, alerts, logs, debug, sheet, lock,
    failReads: n => { readFailures = n; }, failWrite: mode => { writeFailure = mode; },
    advance: ms => { now += ms; }, counts: () => ({ writes, reads, releases }) };
}

test("one transient getLastRow failure retries only the read and claims once", () => {
  const h = fixture();
  h.failReads(1);
  const result = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  assert.equal(result.should_send, true);
  assert.equal(result.attempts, 1);
  assert.deepEqual(h.sleeps, [250]);
  assert.equal(h.counts().writes, 1);
  assert.equal(h.rows[2][1], "queued");
  assert.equal(h.alerts.length, 0);
  assert.equal(h.debug.length, 0);
  h.context.monitorSmsClaimStorageV36_();
  h.context.monitorSmsClaimStorageV36_();
  assert.equal(h.debug.length, 1);
  assert.equal(h.debug[0].stage, "sms_claim_storage_status");
  assert.equal(h.debug[0].data.status, "ready");
  assert.equal(h.debug[0].data.contract, "claim-storage-v36");
});

test("exhausted reads return explicit no-send retry and durable Sheet-independent diagnostics", () => {
  const h = fixture();
  h.failReads(2);
  const before = JSON.stringify(h.rows);
  const result = h.context.claimPendingSmsSendV10_({ worker_id: "pixel", api_key: "private", reply_text: "body" });
  assert.equal(result.should_send, false);
  assert.equal(result.should_send_text, "false");
  assert.equal(result.retryable, true);
  assert.equal(result.code, "SMS_CLAIM_STORAGE_UNAVAILABLE");
  assert.equal(result.reply_text, undefined);
  assert.equal(JSON.stringify(h.rows), before);
  assert.equal(h.counts().writes, 0);
  assert.equal(JSON.parse(h.values.SMS_CLAIM_STORAGE_HEALTH_V36).failures, 1);
  assert.doesNotMatch(JSON.stringify(h.values), /private|reply_text|lease|212555/);
  assert.equal(h.alerts.length, 0);
  const next = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  assert.equal(next.should_send, true);
  assert.ok(JSON.parse(h.values.SMS_CLAIM_STORAGE_HEALTH_V36).recovered_at_ms);
});

for (const mode of ["before", "after"]) {
  test(`ambiguous claim write (${mode}) is not replayed inline; fresh poll reconciles one lease`, () => {
    const h = fixture();
    h.failWrite(mode);
    const result = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
    assert.equal(result.should_send, false);
    assert.equal(result.retryable, true);
    assert.equal(h.counts().writes, 1);
    assert.deepEqual(h.sleeps, []);
    assert.equal(h.rows[1][13], "");
    assert.equal(h.rows[1][14], "");
    assert.equal(h.rows[2][1], "queued");
    h.failWrite("");
    const next = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
    assert.equal(next.message_id, "message-1");
    assert.equal(next.attempts, 1);
    assert.equal(h.rows[2][1], "queued");
    if (mode === "after") assert.equal(h.counts().writes, 1);
    assert.ok(h.counts().releases >= 2);
  });
}

test("non-transient errors retain normal incident handling instead of being hidden as retryable", () => {
  const h = fixture();
  for (const text of ["Unauthorized", "Sheet missing", "Cannot read properties of undefined", "Service invoked too many times for one day: spreadsheets."]) {
    h.sheet.getLastRow = () => { throw new Error(text); };
    assert.throws(() => h.context.claimPendingSmsSendV10_({}), { message: text });
  }
  assert.deepEqual(h.sleeps, []);
  assert.equal(h.counts().writes, 0);
  assert.equal(Object.keys(h.values).length, 0);
});

test("outages never reopen sent, uncertain or send-started rows", () => {
  for (const status of ["sent", "uncertain", "send_started", "held", "superseded"]) {
    const h = fixture();
    h.rows.splice(2);
    h.rows[1][1] = status;
    h.rows[1][13] = new Date();
    h.failReads(2);
    assert.equal(h.context.claimPendingSmsSendV10_({}).should_send, false);
    assert.equal(h.context.claimPendingSmsSendV10_({}).should_send, false);
    assert.equal(h.rows[1][1], status);
    assert.equal(h.counts().writes, 0);
  }
});

test("sustained failures alert only after ten minutes and are rate limited; recovery ends the incident", () => {
  const h = fixture();
  for (let i = 0; i < 3; i++) h.context.recordSmsClaimStorageOutcomeV36_(false);
  h.context.monitorSmsClaimStorageV36_();
  assert.equal(h.alerts.length, 0);
  h.advance(10 * 60000);
  h.context.monitorSmsClaimStorageV36_();
  h.context.monitorSmsClaimStorageV36_();
  assert.equal(h.alerts.length, 1);
  assert.equal(h.alerts[0].subject, "SMS OUTBOX STORAGE DELAY");
  assert.match(h.alerts[0].body, /not agent interest/);
  h.advance(30 * 60000);
  h.context.monitorSmsClaimStorageV36_();
  assert.equal(h.alerts.length, 2);
  h.context.recordSmsClaimStorageOutcomeV36_(true);
  h.advance(30 * 60000);
  h.context.monitorSmsClaimStorageV36_();
  assert.equal(h.alerts.length, 2);
});

test("failed alert delivery retains retry eligibility and busy diagnostic lock cannot change delivery", () => {
  const h = fixture();
  for (let i = 0; i < 3; i++) h.context.recordSmsClaimStorageOutcomeV36_(false);
  h.advance(10 * 60000);
  h.context.sendSystemAlertEmail_ = () => { throw new Error("mail unavailable"); };
  assert.throws(() => h.context.monitorSmsClaimStorageV36_(), /mail unavailable/);
  assert.equal(JSON.parse(h.values.SMS_CLAIM_STORAGE_HEALTH_V36).last_alert_at_ms, 0);
  h.lock.tryLock = () => false;
  h.context.recordSmsClaimStorageOutcomeV36_(true);
  assert.equal(JSON.parse(h.values.SMS_CLAIM_STORAGE_HEALTH_V36).recovered_at_ms, undefined);
  assert.equal(h.counts().writes, 0);
});

test("failed Sheet diagnostics retain the bounded event without preventing persistent-outage alerts", () => {
  const h = fixture();
  h.context.appendSmsDebugLog_ = () => { throw unavailable(); };
  for (let i = 0; i < 3; i++) h.context.recordSmsClaimStorageOutcomeV36_(false);
  h.advance(10 * 60000);
  h.context.monitorSmsClaimStorageV36_();
  assert.equal(h.alerts.length, 1);
  assert.equal(JSON.parse(h.values.SMS_CLAIM_STORAGE_HEALTH_V36).logged_event_at_ms, undefined);
  assert.equal(h.counts().writes, 0);
});
