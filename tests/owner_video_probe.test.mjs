import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";

const root = path.join(import.meta.dirname, "..");
const sources = ["sms_outbox.js", "zz_unified_post.js"].map((name) =>
  fs.readFileSync(path.join(root, "apps_script", name), "utf8"));
const nonce = "a".repeat(32);
const videoUrl = "https://go.crispshortsales.com/v/" + "b".repeat(32);
const introduction = "Hey Jim, it's Yoni Kutler with Crisp Short Sales. Here's a quick video to put a face to my text. Happy to help if you have questions.";
const expectedMessage = introduction + "\n" + videoUrl;
const legacyMessage = "Owner video test: " + videoUrl;
const input = { phone: "9542053205", probe_nonce: nonce, video_url: videoUrl };

function harness() {
  let now = Date.parse("2026-10-13T16:00:00Z");
  let writes = 0;
  let crmTouches = 0;
  const properties = new Map();
  const systemAlerts = [];
  let uuid = 0;
  let lockAvailable = true;
  class Clock extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }
    static now() { return now; }
  }
  const rows = [Array(19).fill("")];
  const sheet = {
    getLastRow: () => rows.length,
    appendRow: (row) => { writes++; rows.push(row.slice()); },
    getRange: (start, column, count = 1, width = 1) => ({
      getValues: () => Array.from({ length: count }, (_, i) => rows[start - 1 + i].slice(column - 1, column - 1 + width)),
      setValue: (value) => { writes++; rows[start - 1][column - 1] = value; },
      setValues: (values) => { writes++; values.forEach((row, i) => row.forEach((value, j) => { rows[start - 1 + i][column - 1 + j] = value; })); },
    }),
  };
  const noCrm = () => { crmTouches++; throw new Error("CRM must not be touched by an owner probe"); };
  const context = {
    Date: Clock, JSON, String, Number, Object, Array, Math,
    normalizePhone_: (value) => String(value ?? "").replace(/\D/g, "").replace(/^1(?=\d{10}$)/, ""),
    normalizeWhitespace_: (value) => String(value).trim().replace(/\s+/g, " "),
    getSmsSpreadsheet_: () => ({ getSheetByName: (name) => {
      assert.equal(name, "sms_pending_sends");
      return sheet;
    } }),
    getSheet_: noCrm, getSheetData_: noCrm, handleReplySent_: noCrm,
    hasPendingSmsTakeoverV11_: noCrm, sendHandoffEmail_: noCrm,
    PropertiesService: { getScriptProperties: () => ({
      getProperty: (key) => properties.get(key) ?? null,
      setProperty: (key, value) => properties.set(key, String(value)),
      deleteProperty: (key) => properties.delete(key),
    }) },
    LockService: { getScriptLock: () => ({ tryLock: () => lockAvailable, releaseLock: () => {} }) },
    Utilities: { getUuid: () => `lease-${++uuid}` },
    validateToken_: (token) => { if (token !== "test-token") throw new Error("Unauthorized"); },
    parseIncomingRequest_: (event) => JSON.parse(event.postData.contents),
    jsonOutput_: (value) => value,
    safeJsonStringify_: JSON.stringify,
    detectIncomingTransport_: () => "test",
    getRawPostBody_: () => "",
    maskSensitiveDebugText_: () => "",
    sendSystemAlertEmail_: (...args) => systemAlerts.push(args),
  };
  vm.createContext(context);
  sources.forEach((source) => vm.runInContext(source, context));
  context.ensureSmsSheetHeaders_ = () => {};
  context.ensurePendingSmsHeaders_ = () => {};
  context.recordTaskerTransportActivityV12_ = () => {};
  context.appendSmsDebugLog_ = () => {};
  context.getTaskerTransportHealthV12_ = () => ({ healthy: true });
  const post = (action, body = {}) => context.handleUnifiedSmsPost_({
    postData: { contents: JSON.stringify({ action, token: "test-token", ...body }) },
  });
  const queue = (changes = {}) => post("enqueue_owner_video_probe", { ...input, ...changes });
  const claim = () => context.claimPendingSmsSendV10_({ worker_id: "test-handset" });
  const started = () => {
    const body = claim();
    assert.equal(body.should_send, true);
    assert.equal(context.markPendingSmsSendStartedV10_(body).status, "send_started");
    return body;
  };
  return { context, rows, sheet, post, queue, claim, started, systemAlerts,
    advance: (ms) => { now += ms; }, lock: (value) => { lockAvailable = value; },
    counters: () => ({ writes, crmTouches }) };
}

test("owner probe queues and claims only the fixed branded message without CRM access", () => {
  const h = harness();
  const result = h.queue();
  assert.equal(result.queued, true);
  assert.equal(result.message, expectedMessage);
  assert.equal(result.message.charCodeAt(introduction.length), 10);
  assert.equal(result.message_id, "owner-video-probe-" + nonce);
  assert.equal(result.request_id, "owner-video-probe-request-" + nonce);
  assert.equal(h.rows[1][17], "");
  const started = h.started();
  assert.equal(started.phone, "9542053205");
  assert.equal(started.reply_text, expectedMessage);
  assert.equal(h.counters().crmTouches, 0);
});

test("Sue and Christopher keep the exact name through queue, claim and receipt", () => {
  for (const firstName of ["Sue", "Christopher"]) {
    const h = harness();
    const message = expectedMessage.replace("Hey Jim,", `Hey ${firstName},`);
    const queued = h.queue({ first_name: firstName });
    assert.equal(queued.queued, true, firstName);
    assert.equal(queued.message, message);
    const metadata = JSON.parse(h.rows[1][6].replace("__owner_video_probe__:", ""));
    assert.equal(metadata.first_name, firstName);
    assert.equal(Date.parse(metadata.expires_at) - Date.parse(queued.created_at), 30 * 60 * 1000);
    const body = h.started();
    assert.equal(body.phone, "9542053205");
    assert.equal(body.reply_text, message);
    assert.equal(h.post("reply_sent", { ...body, reply_text: expectedMessage }).ok, false);
    assert.equal(h.post("reply_sent", body).status, "sent");
    const readback = h.post("get_owner_video_probe_status", { phone: input.phone, probe_nonce: nonce });
    assert.equal(readback.message, message);
    assert.equal(readback.status, "sent");
    assert.equal(h.counters().crmTouches, 0);
  }
});

test("owner probe first names reject missing-value, multiline, template and full-name injection", () => {
  for (const firstName of ["", " ", null, 1, [], {}, "S", "Sue Smith", " Sue", "Sue ", "Sue\n", "Sue\r\nInjected",
    "Sue123", "<Sue>", "{{name}}", "Sue/Chris", "Sue\\Chris", "Sue--Ann", "A".repeat(33), "unknown", "Agent", "there", "TEST"]) {
    const h = harness();
    assert.equal(h.queue({ first_name: firstName }).ok, false, JSON.stringify(firstName));
    assert.equal(h.rows.length, 1);
    assert.deepEqual(h.counters(), { writes: 0, crmTouches: 0 });
  }
  for (const firstName of ["Anne-Marie", "D'Arcy", "Jose", "\u00c9lodie"]) {
    const h = harness();
    assert.equal(h.queue({ first_name: firstName }).queued, true, firstName);
  }
});

test("a different name cannot reuse a frozen nonce in any state", () => {
  for (const status of ["queued", "claimed", "send_started", "sent", "uncertain", "failed", "expired", "superseded", "manual_sent"]) {
    const h = harness();
    const first = h.queue({ first_name: "Sue" });
    h.rows[1][1] = status;
    h.advance(31 * 60 * 1000);
    const before = h.counters();
    const mismatch = h.queue({ first_name: "Christopher" });
    assert.equal(mismatch.ok, false, status);
    assert.equal(mismatch.duplicate, true);
    assert.equal(mismatch.payload_frozen, true);
    assert.match(mismatch.reason, /first name.*nonce/);
    for (const change of [{ first_name: "Sue" }, {}]) {
      const duplicate = h.queue(change);
      assert.equal(duplicate.duplicate, true);
      assert.equal(duplicate.message, first.message);
      assert.equal(duplicate.expires_at, first.expires_at);
    }
    assert.equal(h.rows.length, 2);
    assert.deepEqual(h.counters(), before);
  }
});

test("old metadata without a first name remains Jim and cannot be rebound to a new name", () => {
  const h = harness();
  h.queue();
  const metadata = JSON.parse(h.rows[1][6].replace("__owner_video_probe__:", ""));
  delete metadata.first_name;
  h.rows[1][6] = "__owner_video_probe__:" + JSON.stringify(metadata);
  assert.equal(h.queue({ first_name: "Sue" }).ok, false);
  assert.equal(h.queue({ first_name: "Jim" }).duplicate, true);
  assert.equal(h.queue().message, expectedMessage);
  const body = h.started();
  assert.equal(body.reply_text, expectedMessage);
  assert.equal(h.post("reply_sent", body).status, "sent");
});

test("name metadata and stored message must agree before claim and receipt", () => {
  for (const firstName of ["Christopher", "", null, "Sue Smith"]) {
    for (const stage of ["claim", "receipt"]) {
      const h = harness();
      h.queue({ first_name: "Sue" });
      const body = stage === "receipt" ? h.started() : null;
      const metadata = JSON.parse(h.rows[1][6].replace("__owner_video_probe__:", ""));
      metadata.first_name = firstName;
      h.rows[1][6] = "__owner_video_probe__:" + JSON.stringify(metadata);
      if (stage === "claim") assert.notEqual(h.claim().should_send, true);
      else assert.equal(h.post("reply_sent", body).ok, false);
      assert.equal(h.counters().crmTouches, 0);
    }
  }
});

test("owner probe rejects every other phone, unsafe URL, message and identity injection", () => {
  const changes = [
    { phone: "9542053206" }, { phone: "evil9542053205" }, { phone: "" },
    { probe_nonce: "A".repeat(32) }, { probe_nonce: "a".repeat(31) },
    { video_url: "http://go.crispshortsales.com/v/" + "b".repeat(32) },
    { video_url: videoUrl + "?next=https://evil.test" }, { video_url: videoUrl + "\nInjected" },
    { video_url: videoUrl.replace("go.", "evil.") }, { video_url: videoUrl.replace("/v/", "/v/../") },
    { video_url: videoUrl.replace("go.", "go.crispshortsales.com@evil.") },
    { message: "Different text " + videoUrl }, { reply_text: "Injected" },
    { message: legacyMessage }, { message: expectedMessage.replace("\n", " ") },
    { message: expectedMessage.replace("\n", "\r\n") }, { allowLegacyMessage: true },
    { legacy_message: true },
    { crm_row: 2 }, { stable_id: "fake" }, { crm_prospect_id: "fake" },
    { request_id: "arbitrary" }, { message_id: "arbitrary" }, { initial_sent_at: "anything" },
  ];
  for (const change of changes) {
    const h = harness();
    assert.equal(h.queue(change).ok, false, JSON.stringify(change));
    assert.equal(h.rows.length, 1);
    assert.deepEqual(h.counters(), { writes: 0, crmTouches: 0 });
  }
});

test("owner probe routes require authentication and status is read only", () => {
  const h = harness();
  assert.equal(h.context.isUnifiedSmsAction_("enqueue_owner_video_probe"), true);
  assert.equal(h.post("enqueue_owner_video_probe", { ...input, token: "wrong" }).ok, false);
  assert.equal(h.rows.length, 1);
  h.queue();
  const before = h.counters();
  const status = h.post("get_owner_video_probe_status", { phone: input.phone, probe_nonce: nonce });
  assert.equal(status.status, "queued");
  assert.equal(status.sent_at, "");
  assert.equal(h.post("get_owner_video_probe_status", { phone: input.phone, probe_nonce: nonce, token: "wrong" }).ok, false);
  assert.equal(h.post("get_owner_video_probe_status", { phone: "9542053206", probe_nonce: nonce }).ok, false);
  assert.deepEqual(h.counters(), before);
});

test("nonce freezes original content and expiry across all queued and terminal states", () => {
  for (const status of ["queued", "claimed", "send_started", "sent", "uncertain", "failed", "expired", "superseded", "manual_sent"]) {
    const h = harness();
    const first = h.queue();
    h.rows[1][1] = status;
    h.advance(31 * 60 * 1000);
    const before = h.counters();
    const second = h.queue({ video_url: videoUrl.replace(/b/g, "c") });
    assert.equal(second.duplicate, true, status);
    assert.equal(second.payload_frozen, true);
    assert.equal(second.message, first.message);
    assert.equal(second.expires_at, first.expires_at);
    assert.equal(h.rows.length, 2);
    assert.deepEqual(h.counters(), before);
  }
});

test("legacy nonce retries and status keep the old body and expiry in every state", () => {
  for (const status of ["queued", "claimed", "send_started", "sent", "uncertain", "failed", "expired", "superseded", "manual_sent"]) {
    const h = harness();
    const first = h.queue();
    h.rows[1][5] = legacyMessage;
    h.rows[1][1] = status;
    h.advance(31 * 60 * 1000);
    const before = h.counters();
    for (const change of [{}, { message: legacyMessage }, { message: expectedMessage }, { video_url: videoUrl.replace(/b/g, "c") }]) {
      const duplicate = h.queue(change);
      assert.equal(duplicate.duplicate, true, status);
      assert.equal(duplicate.payload_frozen, true);
      assert.equal(duplicate.message, legacyMessage);
      assert.equal(duplicate.expires_at, first.expires_at);
    }
    const readback = h.post("get_owner_video_probe_status", { phone: input.phone, probe_nonce: nonce });
    assert.equal(readback.message, legacyMessage);
    assert.equal(h.rows.length, 2);
    assert.deepEqual(h.counters(), before);
  }
});

test("an old-copy retry cannot downgrade a nonce already frozen to the new copy", () => {
  const h = harness();
  assert.equal(h.queue({ message: expectedMessage }).message, expectedMessage);
  const before = h.counters();
  const duplicate = h.queue({ message: legacyMessage });
  assert.equal(duplicate.duplicate, true);
  assert.equal(duplicate.message, expectedMessage);
  assert.deepEqual(h.counters(), before);
});

test("legacy stored body remains claimable and only its exact late receipt succeeds", () => {
  const h = harness(); h.queue(); h.rows[1][5] = legacyMessage;
  const body = h.started();
  assert.equal(body.reply_text, legacyMessage);
  h.advance(31 * 60 * 1000);
  h.context.smsOutboxWatchdog_();
  assert.equal(h.rows[1][1], "uncertain");
  assert.equal(h.post("reply_sent", { ...body, reply_text: expectedMessage }).ok, false);
  assert.equal(h.post("reply_sent", body).status, "sent");
  assert.equal(h.rows[1][5], legacyMessage);
  assert.equal(h.counters().crmTouches, 0);
  assert.equal(h.rows.length, 2);
  assert.deepEqual(h.systemAlerts, []);
});

test("enqueue lock contention never appends or bypasses identity validation", () => {
  const h = harness();
  h.lock(false);
  assert.equal(h.queue().retryable, true);
  assert.equal(h.rows.length, 1);
});

test("expiry blocks queued, active-lease reclaim and send-start paths", () => {
  for (const stage of ["queued", "claimed", "send_start"]) {
    const h = harness();
    h.queue();
    h.advance(29 * 60 * 1000);
    const body = stage !== "queued" ? h.claim() : null;
    h.advance(60 * 1000);
    const result = stage === "send_start" ? h.context.markPendingSmsSendStartedV10_(body) : h.claim();
    assert.notEqual(result.should_send, true, stage);
    assert.equal(h.rows[1][1], "superseded");
    assert.equal(h.counters().crmTouches, 0);
  }
});

test("corrupt metadata, phone and CRM bindings fail closed before claim", () => {
  for (const mutate of [
    (row) => { row[6] = "broken"; }, (row) => { row[4] = "9542053206"; },
    (row) => { row[6] = "__owner_video_probe__:null"; },
    (row) => { row[6] = row[6].replace("__owner_video_probe__:", ""); },
    (row) => { row[17] = 2; }, (row) => { row[16] = "inbound-id"; },
    (row) => { row[5] += " injected"; }, (row) => { row[2] = "other"; },
  ]) {
    const h = harness(); h.queue(); mutate(h.rows[1]);
    assert.notEqual(h.claim().should_send, true);
    assert.equal(h.counters().crmTouches, 0);
  }
});

test("only an exact started handset receipt records sent with no CRM changes", () => {
  const h = harness(); h.queue();
  const claimed = h.claim();
  assert.equal(h.post("reply_sent", claimed).ok, false);
  assert.equal(h.rows[1][1], "claimed");
  h.context.markPendingSmsSendStartedV10_(claimed);
  for (const field of ["request_id", "message_id", "phone", "lease_token", "reply_text"]) {
    const body = { ...claimed }; delete body[field];
    assert.equal(h.post("reply_sent", body).ok, false, field);
    assert.equal(h.rows[1][1], "send_started");
  }
  assert.equal(h.post("reply_sent", { ...claimed, lease_token: "wrong" }).ok, false);
  const result = h.post("reply_sent", claimed);
  assert.equal(result.owner_video_probe, true);
  assert.equal(result.status, "sent");
  assert.equal(h.rows[1][14].toISOString(), "2026-10-13T16:00:00.000Z");
  assert.equal(h.post("reply_sent", claimed).duplicate, true);
  assert.equal(h.counters().crmTouches, 0);
});

test("watchdog preserves exact late receipts but never retries or touches CRM", () => {
  const h = harness(); h.queue(); const body = h.started();
  h.advance(31 * 60 * 1000);
  h.context.smsOutboxWatchdog_();
  assert.equal(h.rows[1][1], "uncertain");
  assert.equal(h.context.buildAutomaticSmsRetryRowV15_(h.rows[1], Date.now()), null);
  assert.equal(h.context.findConfirmedSmsReplyReceiptForPendingRow_(h.rows[1]).confirmed, false);
  assert.equal(h.context.recoverUncertainSmsSendV15_(body).ok, false);
  assert.equal(h.context.alertSmsOutboxProblem_(h.sheet, 2, h.rows[1], "probe failure"), false);
  assert.equal(h.post("reply_sent", body).status, "sent");
  assert.equal(h.counters().crmTouches, 0);
  assert.equal(h.rows.length, 2);
  assert.deepEqual(h.systemAlerts, []);
});

test("expiry watchdog and old pending watchdog stay outbox-only", () => {
  const h = harness(); h.queue(); h.advance(31 * 60 * 1000);
  h.context.smsOutboxWatchdog_();
  assert.equal(h.rows[1][1], "expired");
  h.rows[1][1] = "pending";
  h.context.smsPendingSendWatchdog_();
  assert.equal(h.rows[1][1], "pending");
  assert.equal(h.counters().crmTouches, 0);
  assert.equal(h.rows.length, 2);
  assert.deepEqual(h.systemAlerts, []);
});

test("a reported SMS failure is terminal and cannot create another probe attempt", () => {
  const h = harness(); h.queue(); const body = h.started();
  const result = h.post("sms_send_failed", { ...body, error: "handset failure" });
  assert.equal(result.result.status, "failed");
  assert.equal(result.result.owner_video_probe, true);
  assert.equal(h.claim().should_send, false);
  assert.equal(h.queue().duplicate, true);
  assert.equal(h.rows.length, 2);
  assert.equal(h.counters().crmTouches, 0);
});
