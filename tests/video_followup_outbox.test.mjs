import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";

const root = path.join(import.meta.dirname, "..");
const outboxSource = fs.readFileSync(path.join(root, "apps_script/sms_outbox.js"), "utf8");
const postSource = fs.readFileSync(path.join(root, "apps_script/zz_unified_post.js"), "utf8");
const initial = "2026-10-13T13:00:00.000Z";
const key = "a".repeat(64);
const base = {
  crm_row: 2, stable_id: "listing-123", phone: "9542053205",
  followup_key: key, initial_sent_at: initial, video_variant: "text", video_job_id: "",
  message_id: `followup-video-${key.slice(0, 24)}`,
  request_id: `scheduler-followup-video-${key.slice(0, 24)}`,
  message: "Original follow-up text",
};
const video = (changes = {}) => ({
  ...base, video_variant: "video", video_job_id: "job_123", first_name: "Jim",
  message: "Hey Jim, here's your video: https://example.test/v/opaque", ...changes,
});
const legacy = {
  crm_row: 2, stable_id: base.stable_id, phone: base.phone, message: "Original legacy text",
  message_id: "followup-2-oldhash", request_id: "scheduler-followup-2-oldhash",
};

function parseLocalFixtureDate(text, timeZone, pattern) {
  let parts;
  if (pattern === "yyyy-MM-dd'T'HH:mm:ss.SSS") {
    const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})$/.exec(text);
    if (match) parts = match.slice(1).map(Number);
  } else {
    const seconds = pattern.includes(":ss");
    const twelveHour = pattern.endsWith(" a");
    const regex = new RegExp(`^(\\d{1,2})/(\\d{1,2})/(\\d{4}) (\\d{1,2}):(\\d{2})${seconds ? ":(\\d{2})" : ""}${twelveHour ? " (AM|PM)" : ""}$`);
    const match = regex.exec(text);
    if (match) {
      let hour = Number(match[4]);
      if (twelveHour) hour = hour % 12 + (match.at(-1) === "PM" ? 12 : 0);
      parts = [Number(match[3]), Number(match[1]), Number(match[2]), hour, Number(match[5]), seconds ? Number(match[6]) : 0, 0];
    }
  }
  if (!parts) throw new Error("Fixture format mismatch");
  const wall = Date.UTC(parts[0], parts[1] - 1, ...parts.slice(2));
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone, year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
  });
  const local = Object.fromEntries(formatter.formatToParts(new Date(wall)).map((part) => [part.type, part.value]));
  const rendered = Date.UTC(Number(local.year), Number(local.month) - 1, Number(local.day), Number(local.hour), Number(local.minute), Number(local.second), parts[6]);
  return new Date(wall + wall - rendered);
}

function harness() {
  let writes = 0;
  let triggers = 0;
  let sequence = 0;
  let takeover = false;
  const makeSheet = (data) => ({
    data,
    getLastRow: () => data.length,
    getDataRange: () => ({ getValues: () => data.map((row) => row.slice()) }),
    appendRow: (row) => { writes++; data.push(row.slice()); },
    getRange: (start, column, count = 1, width = 1) => {
      if (typeof start === "string") {
        const match = /^([A-Z]+)(\d+)$/.exec(start);
        column = [...match[1]].reduce((n, char) => n * 26 + char.charCodeAt(0) - 64, 0);
        start = Number(match[2]);
      }
      return {
        getValues: () => Array.from({ length: count }, (_, i) =>
          Array.from({ length: width }, (_, j) => data[start - 1 + i]?.[column - 1 + j] ?? "")),
        getValue: () => data[start - 1]?.[column - 1] ?? "",
        setValue: (value) => { writes++; data[start - 1][column - 1] = value; },
        setValues: (values) => {
          writes++;
          values.forEach((row, i) => row.forEach((value, j) => { data[start - 1 + i][column - 1 + j] = value; }));
        },
      };
    },
  });
  const headers = Array.from({ length: 33 }, (_, i) => `column_${i}`);
  Object.assign(headers, {
    0: "agent_name", 2: "phone", 8: "follow_up", 10: "mailshake_status",
    28: "human_override", 29: "last_message_id", 30: "last_inbound_text", 31: "handoff_flag",
  });
  const row = Array(33).fill("");
  Object.assign(row, { 0: "Jim", 2: base.phone, 10: "N", 22: new Date(initial), 27: base.stable_id });
  const leads = makeSheet([headers, row]);
  const outbox = makeSheet([Array(19).fill("")]);
  const inbound = makeSheet([Array(8).fill("")]);
  const sheets = { sms_pending_sends: outbox, sms_inbound_queue: inbound };
  const context = {
    Date, JSON, String, Number, Object, Array, Math,
    normalizePhone_: (value) => String(value ?? "").replace(/\D/g, "").replace(/^1(?=\d{10}$)/, ""),
    normalizeWhitespace_: (value) => String(value).trim().replace(/\s+/g, " "),
    normalizePendingSmsInboundText_: (value) => String(value ?? "").trim().toLowerCase(),
    normalizePendingSmsReply_: (value) => String(value ?? "").trim().toLowerCase(),
    hasPendingSmsTakeoverV11_: () => takeover,
    installSmsOutboxTriggers_: () => { triggers++; },
    ensureSmsSheetHeaders_: () => {},
    ensurePendingSmsHeaders_: () => {},
    getSmsSpreadsheet_: () => ({
      getSheetByName: (name) => sheets[name] ?? null,
      getSpreadsheetTimeZone: () => "America/New_York",
    }),
    getSheet_: () => leads,
    getSheetData_: () => leads.data.slice(1).map((values, i) => ({
      row: i + 2, obj: Object.fromEntries(headers.map((header, j) => [header, values[j]])),
    })),
    LockService: { getScriptLock: () => ({ tryLock: () => true, releaseLock: () => {} }) },
    Utilities: {
      getUuid: () => `uuid-${++sequence}`,
      formatDate: (date, timeZone) => {
        const parts = new Intl.DateTimeFormat("en-US", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" })
          .formatToParts(date);
        const part = (name) => parts.find((item) => item.type === name).value;
        return `${part("year")}-${part("month")}-${part("day")}`;
      },
      parseDate: parseLocalFixtureDate,
    },
    HEADERS: {
      agent_name: "agent_name", phone: "phone", human_override: "human_override",
      mailshake_status: "mailshake_status", followup_text_sent: "follow_up",
      last_message_id: "last_message_id", last_inbound_text: "last_inbound_text", handoff_flag: "handoff_flag",
    },
  };
  vm.createContext(context);
  vm.runInContext(outboxSource, context);
  // Load the router without replacing the harness's outbox test helpers.
  vm.runInContext(postSource.slice(0, postSource.indexOf("function getUnifiedIgnoredInboundReason_(")), context);
  context.installSmsOutboxTriggers_ = () => { triggers++; };
  context.ensureSmsSheetHeaders_ = () => {};
  return {
    context, row, leads, outbox, inbound, sheets,
    counters: () => ({ writes, triggers }), setTakeover: (value) => { takeover = value; },
  };
}

function enqueue(h, body = base) {
  return h.context.enqueueFollowupSmsV13_(body, "webhook-id");
}

function inboundReply(h, received = "2026-10-13T14:00:00Z", created = received) {
  h.inbound.data.push([new Date(created), "queued", "q1", "dedupe", "m1", base.phone, "Thanks", received]);
}

function initialReceipt(h, sentAt, changes = {}) {
  const row = Array(19).fill("");
  Object.assign(row, {
    0: new Date(sentAt), 1: "sent", 4: base.phone,
    6: `__initial_outreach__:${JSON.stringify({ stable_id: base.stable_id })}`,
    14: new Date(sentAt), 17: 2,
  }, changes);
  h.outbox.data.push(row);
}

function delayedInitialReceipt(h, changes = {}) {
  initialReceipt(h, "2026-10-13T13:00:08.247Z", {
    0: new Date("2026-10-13T12:59:00Z"),
    2: "scheduler-initial-2-0123456789abcdef", 3: "initial-2-0123456789abcdef",
    5: "Original initial outreach text",
    6: `__initial_outreach__:${JSON.stringify({ crm_row: 2, stable_id: base.stable_id })}`,
    13: new Date("2026-10-13T12:59:58Z"), ...changes,
  });
}

function receipt(h, index = h.outbox.data.length - 1) {
  const queued = h.outbox.data[index];
  return h.context.applyScheduledFollowupReceiptV13_({ request_id: queued[2], sent_at: "2026-10-13T20:00:00Z" }, {
    crm_row: queued[17], canonical_phone: queued[4], canonical_reply_text: queued[5], send_metadata: queued[6],
  });
}

test("first text/video choice is immutable under one logical key", () => {
  for (const first of [base, video()]) {
    const h = harness();
    assert.equal(enqueue(h, first).status, "queued");
    const duplicate = enqueue(h, first.video_variant === "text" ? video() : base);
    assert.equal(duplicate.ok, true);
    assert.equal(duplicate.payload_frozen, true);
    assert.equal(duplicate.video_variant, first.video_variant);
    assert.equal(h.outbox.data.length, 2);
    assert.equal(h.outbox.data[1][5], first.message);
  }
});

test("terminal and uncertain attempts never create another payload", () => {
  for (const status of ["receipt_pending", "uncertain", "failed", "superseded", "manual_sent", "sent"]) {
    const h = harness();
    enqueue(h);
    h.outbox.data[1][1] = status;
    const result = enqueue(h, video());
    assert.equal(result.duplicate, true, status);
    assert.equal(result.ok, status === "sent", status);
    assert.equal(h.outbox.data.length, 2);
    assert.equal(h.outbox.data[1][5], base.message);
  }
});

test("logical-key and truncated-ID identity conflicts fail closed", () => {
  for (const changes of [
    { phone: "9542053206" }, { stable_id: "other-listing" },
    { initial_sent_at: "2026-10-13T14:00:00Z" }, { followup_key: "a".repeat(24) + "b".repeat(40) },
  ]) {
    const h = harness();
    enqueue(h);
    assert.equal(enqueue(h, { ...base, ...changes }).ok, false);
    assert.equal(h.outbox.data.length, 2);
  }
});

test("partial or malformed feature bodies cannot silently use the legacy lane", () => {
  for (const changes of [
    { followup_key: "" }, { followup_key: "A".repeat(64) }, { stable_id: "" },
    { initial_sent_at: "10/13/2026" }, { initial_sent_at: "invalid" },
    { video_variant: "other" }, { request_id: "render-followup-2-0123456789abcdef" },
    { message_id: "followup-legacy" }, { video_variant: "video" },
    { video_job_id: "https://not-opaque.test/" },
    { crm_prospect_id: "e1f3e796-ff41-4c8c-8991-da6ca179d988" },
  ]) {
    const h = harness();
    assert.equal(enqueue(h, { ...base, ...changes }).ok, false, JSON.stringify(changes));
    assert.equal(h.outbox.data.length, 1);
  }
});

test("pre-render eligibility requires no body/job and performs no writes", () => {
  const h = harness();
  const body = video({ message: undefined, video_job_id: "" });
  const before = h.counters();
  assert.equal(h.context.checkVideoFollowupEligibilityV19_(body).eligible, true);
  assert.deepEqual(h.counters(), before);
  assert.equal(enqueue(h, body).ok, false);
});

test("authenticated eligibility route bypasses debug and mutation paths", () => {
  const h = harness();
  let validated = false;
  h.context.parseIncomingRequest_ = () => ({ ...video({ video_job_id: "" }), token: "test-token", action: "check_video_followup_eligibility" });
  h.context.validateToken_ = () => { validated = true; };
  h.context.jsonOutput_ = (value) => value;
  h.context.appendSmsDebugLog_ = () => { throw new Error("Unexpected audit mutation"); };
  assert.equal(h.context.isUnifiedSmsAction_("check_video_followup_eligibility"), true);
  const before = h.counters();
  assert.equal(h.context.handleUnifiedSmsPost_({}).eligible, true);
  assert.equal(validated, true);
  assert.deepEqual(h.counters(), before);
});

test("video checks canonical first name but original text does not depend on it", () => {
  const h = harness();
  h.row[0] = " Jane ";
  assert.equal(enqueue(h, video()).ok, false);
  assert.equal(enqueue(h).ok, true);
  const other = harness();
  assert.equal(enqueue(other, video({ first_name: " jim " })).ok, true);
});

test("video first-name identity matches the worker's Unicode and apostrophe normalization", () => {
  for (const [sheetName, canonicalName] of [
    ["D\u2019Andre", "D'Andre"],
    ["O\u2018Neil", "O'Neil"],
    ["O\u02bcNeil", "O'Neil"],
    ["Jose\u0301", "Jos\u00e9"],
    [" \uff2dary ", "Mary"],
  ]) {
    const h = harness();
    h.row[0] = sheetName;
    const body = video({ first_name: canonicalName });
    assert.equal(h.context.checkVideoFollowupEligibilityV19_(body).eligible, true);
    assert.equal(enqueue(h, body).ok, true);

    const reverse = harness();
    reverse.row[0] = canonicalName;
    const rawBody = video({ first_name: sheetName });
    assert.equal(reverse.context.validateVideoFollowupContextV19_(rawBody, true).metadata.first_name, canonicalName);
    assert.equal(reverse.context.checkVideoFollowupEligibilityV19_(rawBody).eligible, true);
  }
});

test("first-name normalization never equates different names, accents, or full names", () => {
  for (const [sheetName, videoName] of [
    ["D\u2019Andre", "D'Angelo"],
    ["Jose", "Joseph"],
    ["Jos\u00e9", "Jose"],
    ["Mary Jane", "Mary"],
    ["Jim Smith", "Jim"],
  ]) {
    const h = harness();
    h.row[0] = sheetName;
    const body = video({ first_name: videoName });
    const result = h.context.checkVideoFollowupEligibilityV19_(body);
    assert.equal(result.eligible, false);
    assert.equal(result.reason, "Personalized first name changed");
    assert.equal(enqueue(h, body).ok, false);
  }
});

test("phone, stable listing, and ambiguous stable identities invalidate feature sends", () => {
  for (const mutate of [
    (h) => { h.row[2] = "9542053206"; },
    (h) => { h.row[27] = "other-listing"; },
    (h) => { h.leads.data.push(h.row.slice()); },
  ]) {
    const h = harness();
    mutate(h);
    assert.equal(enqueue(h).ok, false);
    assert.equal(h.outbox.data.length, 1);
  }
});

test("stable row shifts resolve before enqueue, claim, and standard I/X receipt", () => {
  const h = harness();
  h.leads.data.splice(1, 0, Array(33).fill(""));
  assert.equal(enqueue(h, video()).ok, true);
  assert.equal(h.outbox.data[1][17], 3);
  h.leads.data.splice(1, 0, Array(33).fill(""));
  assert.equal(h.context.getPendingSmsStaleReason_(h.outbox.data[1]), "");
  const result = receipt(h);
  assert.equal(result.row, 4);
  assert.equal(result.owner_approved_followup, false);
  assert.equal(h.leads.data[3][8], "x");
  assert.equal(h.leads.data[3][23].toISOString(), "2026-10-13T20:00:00.000Z");
  assert.equal(h.leads.data[2][8], "");
});

test("a changed initial timestamp rejects enqueue, duplicate, claim, and late CRM receipt", () => {
  const h = harness();
  enqueue(h);
  h.row[22] = new Date("2026-10-13T14:00:00Z");
  assert.equal(enqueue(h).ok, false);
  assert.match(h.context.getPendingSmsStaleReason_(h.outbox.data[1]), /initial-send timestamp/);
  assert.equal(receipt(h).crm_write_skipped, true);
  assert.equal(h.row[8], "");
  assert.equal(h.row[23], "");
});

test("a receipt with incomplete feature identity never writes standard follow-up markers", () => {
  const h = harness();
  enqueue(h);
  const metadata = JSON.parse(h.outbox.data[1][6].replace("__scheduled_followup__:", ""));
  delete metadata.initial_sent_at;
  h.outbox.data[1][6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
  assert.equal(receipt(h).crm_write_skipped, true);
  assert.equal(h.row[8], "");
  assert.equal(h.row[23], "");
});

test("date-only W uses latest exact-phone eligible initial receipt, not the first or wrong identity", () => {
  const h = harness();
  h.row[22] = "10/13/2026";
  initialReceipt(h, "2026-10-13T12:00:00Z");
  initialReceipt(h, initial);
  initialReceipt(h, "2026-10-13T14:00:00Z", { 4: "9542053206" });
  initialReceipt(h, "2026-10-13T15:00:00Z", { 1: "queued" });
  initialReceipt(h, "2026-10-13T16:00:00Z", { 6: '__initial_outreach__:{"stable_id":"another"}' });
  assert.equal(enqueue(h).ok, true);
  initialReceipt(h, "2026-10-13T17:00:00Z");
  assert.match(h.context.getPendingSmsStaleReason_(h.outbox.data[6]), /initial-send timestamp/);
});

test("native W cannot hide a newer same-identity confirmed initial receipt", () => {
  const h = harness();
  initialReceipt(h, "2026-10-13T15:00:00Z");
  assert.equal(enqueue(h).ok, false);
});

test("handset W and a later exact-attempt receipt allow video and text without changing W", () => {
  // Observed receipt-processing delays for the first 18 organic jobs.
  for (const delayMs of [8247, 5516, 3650, 4894, 7542, 6280, 3209, 5517, 4691, 6268, 5321, 4226, 4743, 5601, 6609, 6292, 4938, 4729]) {
    for (const body of [base, video()]) {
      const h = harness();
      delayedInitialReceipt(h, { 14: new Date(Date.parse(initial) + delayMs) });
      h.row[11] = "A newer outbound message must not be used as initial identity";
      const before = h.counters();
      assert.equal(h.context.checkVideoFollowupEligibilityV19_(body).eligible, true, String(delayMs));
      assert.deepEqual(h.counters(), before);
      assert.equal(enqueue(h, body).ok, true);
      assert.equal(h.row[22].toISOString(), initial);
      assert.equal(h.context.getPendingSmsStaleReason_(h.outbox.data[2]), "");
    }
  }
});

test("delayed initial receipt also correlates render IDs after a CRM row shift", () => {
  const h = harness();
  delayedInitialReceipt(h, { 2: "render-initial-2-0123456789abcdef" });
  h.leads.data.splice(1, 0, Array(33).fill(""));
  assert.equal(enqueue(h, video()).ok, true);
  assert.equal(h.outbox.data[2][17], 3);
  assert.equal(h.context.getPendingSmsStaleReason_(h.outbox.data[2]), "");
});

test("receipt processing delay is not a widened time tolerance", () => {
  for (const startedAt of ["2026-10-13T13:00:00.001Z", "2026-10-13T13:00:05Z"]) {
    const h = harness();
    delayedInitialReceipt(h, { 13: new Date(startedAt) });
    assert.equal(enqueue(h, video()).ok, false);
  }
  const h = harness();
  delayedInitialReceipt(h, { 14: new Date("2026-10-13T14:00:00Z") });
  assert.equal(h.context.checkVideoFollowupEligibilityV19_(video()).eligible, true);
});

test("missing, malformed or inconsistent same-attempt evidence still fails closed", () => {
  for (const changes of [
    { 0: "" }, { 0: "invalid" }, { 0: new Date("2026-10-13T13:00:01Z") },
    { 13: "" }, { 13: "invalid" },
    { 2: "" }, { 2: "scheduler-initial-2-ffffffffffffffff" },
    { 2: "other-initial-2-0123456789abcdef" },
    { 3: "" }, { 3: "not-an-initial-id" }, { 5: "" }, { 17: 3 },
    { 6: '__initial_outreach__:{"crm_row":2}' },
    { 6: '__initial_outreach__:{"stable_id":"listing-123"}' },
    { 6: '__initial_outreach__:{"crm_row":3,"stable_id":"listing-123"}' },
  ]) {
    const h = harness();
    delayedInitialReceipt(h, changes);
    assert.equal(enqueue(h, video()).ok, false, JSON.stringify(changes));
    assert.equal(h.outbox.data.length, 2);
  }
});

test("a matching first interval never hides a separate later or overlapping initial attempt", () => {
  for (const startedAt of ["2026-10-13T12:59:59Z", "2026-10-13T13:00:02Z"]) {
    const h = harness();
    delayedInitialReceipt(h);
    delayedInitialReceipt(h, {
      2: "render-initial-2-fedcba9876543210", 3: "initial-2-fedcba9876543210",
      13: new Date(startedAt), 14: new Date("2026-10-13T13:00:09Z"),
    });
    assert.equal(enqueue(h, video()).ok, false);
  }
});

test("duplicate or conflicting initial identifiers cannot prove a unique delayed receipt", () => {
  for (const changes of [{}, { 1: "failed" }, { 4: "9542053206" }, { 3: "initial-2-ffffffffffffffff" }, { 2: "render-initial-2-0123456789abcdef" }]) {
    const h = harness();
    delayedInitialReceipt(h);
    h.outbox.data.push(Object.assign(h.outbox.data[1].slice(), changes));
    assert.equal(enqueue(h, video()).ok, false, JSON.stringify(changes));
  }
});

test("same-attempt exception leaves reply, takeover and existing follow-up guards intact", () => {
  for (const suppress of [
    (h) => inboundReply(h),
    (h) => h.setTakeover(true),
    (h) => { h.row[28] = "TRUE"; },
    (h) => { h.row[31] = "TRUE"; },
    (h) => { h.row[8] = "x"; },
    (h) => { h.row[23] = new Date("2026-10-13T14:00:00Z"); },
    (h) => { h.row[10] = "R"; },
  ]) {
    const h = harness();
    delayedInitialReceipt(h);
    suppress(h);
    assert.equal(enqueue(h, video()).ok, false);
  }
  const h = harness();
  delayedInitialReceipt(h);
  enqueue(h, video());
  assert.equal(enqueue(h).duplicate, true);
  assert.equal(h.outbox.data.length, 3);
});

test("date-only recovery cannot hide a newer initial receipt on a different day", () => {
  const h = harness();
  h.row[22] = "10/13/2026";
  initialReceipt(h, initial);
  initialReceipt(h, "2026-10-14T13:00:00Z");
  assert.equal(enqueue(h).ok, false);
  assert.equal(h.outbox.data.length, 3);
});

test("raw queued replies during rendering suppress enqueue even before CRM processing", () => {
  const h = harness();
  inboundReply(h);
  assert.equal(enqueue(h, video()).ok, false);
  assert.equal(h.outbox.data.length, 1);
});

test("malformed received timestamp falls back to created time and unknown times hold", () => {
  for (const created of ["2026-10-13T14:00:00Z", "invalid"]) {
    const h = harness();
    h.inbound.data.push([created, "failed", "q1", "", "", base.phone, "Thanks", "bad"]);
    assert.equal(enqueue(h).ok, false);
  }
  const h = harness();
  inboundReply(h, "2026-10-12T14:00:00Z");
  assert.equal(enqueue(h).ok, true);
});

test("reply after claim suppresses send_started and reclaimed active lease", () => {
  for (const phase of ["send_started", "claim_retry"]) {
    const h = harness();
    enqueue(h);
    const claimed = h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(claimed.should_send, true);
    inboundReply(h);
    const result = phase === "send_started"
      ? h.context.markPendingSmsSendStartedV10_(claimed)
      : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(result.should_send, false);
    assert.equal(h.outbox.data[1][1], "superseded");
  }
});

test("known processed replies, takeover, opt-out, conversation notes and contact holds suppress", () => {
  for (const mutate of [
    (h) => { h.row[29] = "inbound-id"; }, (h) => { h.row[30] = "Thanks"; },
    (h) => { h.row[28] = "TRUE"; }, (h) => { h.row[31] = "TRUE"; },
    (h) => { h.row[10] = "R"; }, (h) => { h.row[10] = "Y"; },
    (h) => { h.row[9] = "Spoke with agent"; }, (h) => { h.row[24] = "phone_conflict"; },
    (h) => { h.setTakeover(true); },
  ]) {
    const h = harness();
    mutate(h);
    assert.equal(enqueue(h).ok, false);
  }
  for (const note of ["left vm", "no answer on second call", "call start failed before connecting"]) {
    const h = harness();
    h.row[9] = note;
    assert.equal(enqueue(h).ok, true);
  }
});

test("older follow-up marker remains eligible but current or unreadable marker suppresses", () => {
  for (const timestamp of [new Date("2026-10-12T20:00:00Z"), new Date("2026-10-13T20:00:00Z"), "invalid"]) {
    const h = harness();
    h.row[8] = "x";
    h.row[23] = timestamp;
    assert.equal(enqueue(h).ok, timestamp instanceof Date && timestamp < new Date(initial));
  }
});

test("existing legacy follow-up attempts cannot overlap the new key", () => {
  for (const status of ["queued", "claimed", "send_started", "uncertain", "sent", "failed"]) {
    const h = harness();
    const row = Array(19).fill("");
    Object.assign(row, {
      0: new Date("2026-10-13T19:00:00Z"), 1: status, 3: "followup-2-oldhash", 4: base.phone,
      6: '__scheduled_followup__:{"crm_row":2,"stable_id":"listing-123"}',
    });
    h.outbox.data.push(row);
    assert.equal(enqueue(h).ok, false, status);
    assert.equal(h.outbox.data.length, 2);
  }
});

test("feature off/on toggles return the already accepted feature attempt", () => {
  for (const status of ["queued", "claimed", "send_started", "sent", "receipt_pending", "uncertain", "failed", "superseded"]) {
    const h = harness();
    enqueue(h, video());
    h.outbox.data[1][1] = status;
    const disabled = enqueue(h, legacy);
    assert.equal(disabled.duplicate, true, status);
    assert.equal(disabled.payload_frozen, true);
    assert.equal(disabled.message_id, base.message_id);
    assert.equal(disabled.ok, ["queued", "claimed", "send_started", "sent"].includes(status));
    enqueue(h, base);
    assert.equal(h.outbox.data.length, 2);
    assert.equal(h.outbox.data[1][5], video().message);
  }
});

test("feature activation freezes an already queued legacy payload for the same initial", () => {
  for (const status of ["queued", "claimed", "send_started", "sent", "uncertain", "failed"]) {
    const h = harness();
    enqueue(h, legacy);
    h.outbox.data[1][1] = status;
    h.outbox.data[1][0] = new Date("2026-10-13T19:00:00Z");
    const enabled = enqueue(h, video());
    assert.equal(enabled.duplicate, true, status);
    assert.equal(enabled.payload_frozen, true);
    assert.equal(enabled.message_id, legacy.message_id);
    assert.equal(enabled.video_variant, "text");
    assert.equal(h.outbox.data.length, 2);
    assert.equal(h.outbox.data[1][5], legacy.message);
  }
});

test("cross-feature dedupe does not swallow a new initial episode or manual CRM exception", () => {
  const h = harness();
  enqueue(h, video());
  h.outbox.data[1][1] = "sent";
  h.row[22] = new Date("2026-10-14T13:00:00Z");
  assert.equal(enqueue(h, legacy).duplicate, undefined);
  assert.equal(h.outbox.data.length, 3);
  const other = harness();
  enqueue(other, video());
  assert.equal(enqueue(other, { ...legacy, request_id: "render-followup-2-0123456789abcdef" }).duplicate, undefined);
  assert.equal(other.outbox.data.length, 3);
});

test("rollback cannot assume a new initial when existing feature identity is unreadable", () => {
  for (const mutation of [
    (h) => { h.row[22] = "invalid"; },
    (h) => { h.row[22] = ""; },
    (h) => { h.leads.data.push(h.row.slice()); },
  ]) {
    const h = harness();
    enqueue(h, video());
    h.outbox.data[1][1] = "sent";
    mutation(h);
    assert.equal(enqueue(h, legacy).ok, false);
    assert.equal(h.outbox.data.length, 2);
  }
});

test("temporary feature guard failure does not block an unrelated queued legacy send", () => {
  const h = harness();
  enqueue(h, video());
  const another = h.row.slice();
  another[2] = "9542053206";
  another[27] = "another-listing";
  h.leads.data.push(another);
  enqueue(h, { ...legacy, crm_row: 3, phone: another[2], stable_id: another[27], message_id: "followup-3-other", request_id: "scheduler-followup-3-other" });
  delete h.sheets.sms_inbound_queue;
  const claimed = h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
  assert.equal(claimed.should_send, true);
  assert.equal(claimed.phone, another[2]);
  assert.equal(h.outbox.data[1][1], "queued");
});

test("Sheets serial and localized timestamps resolve in the spreadsheet timezone", () => {
  const wallSerial = (Date.UTC(2026, 9, 13, 9) / 86400000) + 25569;
  for (const timestamp of [wallSerial, String(wallSerial), "10/13/2026 9:00:00", "10/13/2026 9:00", "10/13/2026 9:00:00 AM", "10/13/2026 9:00 AM"]) {
    const h = harness();
    h.row[22] = timestamp;
    assert.equal(enqueue(h).ok, true, String(timestamp));
  }
});

test("transient feature guard failures retain queued and claimed work for safe retry", () => {
  for (const phase of ["claim", "active_claim", "send_started"]) {
    const h = harness();
    enqueue(h);
    const claimed = phase === "claim" ? null : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    const originalStatus = h.outbox.data[1][1];
    delete h.sheets.sms_inbound_queue;
    const result = phase === "send_started"
      ? h.context.markPendingSmsSendStartedV10_(claimed)
      : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(result.should_send, false, phase);
    assert.equal(result.retryable, true, phase);
    assert.equal(h.outbox.data[1][1], originalStatus, phase);
  }
});

test("takeover service and send-start ledger errors hold feature work without cancellation", () => {
  const h = harness();
  enqueue(h);
  h.context.hasPendingSmsTakeoverV11_ = () => { throw new Error("Temporary property service failure"); };
  assert.equal(h.context.claimPendingSmsSendV10_({}).retryable, true);
  assert.equal(h.outbox.data[1][1], "queued");
  h.context.findLeasedPendingSmsRow_ = () => { throw new Error("Temporary ledger read failure"); };
  const result = h.context.markPendingSmsSendStartedV10_({ message_id: base.message_id });
  assert.equal(result.retryable, true);
  assert.equal(result.should_send, false);
});

test("legacy first_name remains ordinary non-feature metadata", () => {
  const h = harness();
  assert.equal(enqueue(h, { ...legacy, first_name: "Jim" }).ok, true);
});

test("missing receipt cannot automatically retry feature payload; legacy retry stays unchanged", () => {
  const h = harness();
  enqueue(h);
  const row = h.outbox.data[1];
  row[1] = "send_started";
  row[9] = 1;
  assert.equal(h.context.buildAutomaticSmsRetryRowV15_(row, Date.now()), null);
  row[3] = "followup-2-oldhash";
  assert.equal(h.context.buildAutomaticSmsRetryRowV15_(row, Date.now())[1], "queued");
});

test("guard read failures hold without changing existing legacy enqueue", () => {
  const h = harness();
  delete h.sheets.sms_inbound_queue;
  assert.equal(h.context.checkVideoFollowupEligibilityV19_(base).eligible, false);
  assert.equal(enqueue(h).ok, false);
  assert.equal(enqueue(h, {
    crm_row: 2, stable_id: base.stable_id, phone: base.phone,
    message: "Legacy text", message_id: "followup-2-oldhash", request_id: "scheduler-followup-2-oldhash",
  }).ok, true);
});
