import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import { gunzipSync } from "node:zlib";

const root = path.join(import.meta.dirname, "..");
const outboxSource = fs.readFileSync(path.join(root, "apps_script/sms_outbox.js"), "utf8");
const lexiconSource = fs.readFileSync(path.join(root, "apps_script/video_pronunciation_lexicon.js"), "utf8");
const INITIAL = "2026-10-16T13:00:00.000Z";
const PHONE = "9542053205";
const KEY = "a".repeat(64);
const STABLE = "listing-performance-123";

function digest(value) {
  const sorted = (item) => Array.isArray(item) ? item.map(sorted) : item && typeof item === "object"
    ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, sorted(item[key])])) : item;
  return createHash("sha256").update(JSON.stringify(sorted(value))).digest("hex");
}

function videoBody(name = "Marc", observed = "mark") {
  const raw = {
    accepted: true, policy_version: "spoken-greeting-tiny-en-v4",
    model_id: "Systran/faster-whisper-tiny.en",
    model_revision: "0d3d19a32d3338f10357c0889762bd8d64bbdeba",
    model_manifest_sha256: "9f995ad3147f0799b2babb9e84926a53bca82d888d5fb6558d925f38dd2b78ee",
    config_sha256: "e61b237ccaa3f4cccb369f0a88b1f9d0d4e01838242d9b74e90c8ed4891f03a2",
    source_audio_sha256: "b".repeat(64), expected_name: name,
    normalized_transcript: `hey ${observed}`, confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.01 }],
  };
  const proof = {
    ...raw, media_sha256: "c".repeat(64), greeting_seconds: 1.2,
    verification_method: "planned-aac-lineage-v3",
    verification_config_sha256: "3d70d23288207ff942cc5385ee6a926eb529434e950e4f4345e333f944500e25",
    video_job_id: "job_performance", followup_key: KEY, phone: PHONE,
    initial_sent_at: INITIAL, stable_id: STABLE,
    audio_lineage: {
      accepted: true, lineage_version: "planned-aac-same-take-v1", first_name: name,
      manifest_sha256: "d".repeat(64), planned_audio_sha256: "e".repeat(64),
      final_media_sha256: "c".repeat(64), checked_audio_sha256: "b".repeat(64),
      full_pcm_sha256: "f".repeat(64), greeting_pcm_sha256: "1".repeat(64),
      aac_track_sha256: "2".repeat(64), speech_proof_sha256: digest(raw), greeting_seconds: 1.2,
    },
  };
  return {
    crm_row: 2, stable_id: STABLE, phone: PHONE, followup_key: KEY,
    initial_sent_at: INITIAL, video_variant: "video", video_job_id: "job_performance",
    first_name: name, video_speech_proof: proof,
    message_id: `followup-video-${KEY.slice(0, 24)}`,
    request_id: `scheduler-followup-video-${KEY.slice(0, 24)}`,
    message: `Hey ${name}, here's your video: https://go.crispshortsales.com/v/job_performance`,
  };
}

function pendingRow(body, status = "queued") {
  const row = Array(19).fill("");
  const metadata = Object.fromEntries([
    "crm_row", "stable_id", "followup_key", "initial_sent_at", "video_variant",
    "video_job_id", "first_name", "video_speech_proof",
  ].map((key) => [key, body[key]]));
  Object.assign(row, {
    0: new Date("2026-10-16T19:00:00Z"), 1: status, 2: body.request_id, 3: body.message_id,
    4: body.phone, 5: body.message, 6: `__scheduled_followup__:${JSON.stringify(metadata)}`,
    9: 0, 17: body.crm_row,
  });
  return row;
}

function world(body = videoBody(), status = "queued") {
  const headers = Array.from({ length: 33 }, (_, i) => `column_${i}`);
  Object.assign(headers, {
    0: "agent_name", 2: "phone", 8: "follow_up", 10: "mailshake_status",
    12: "conversation_summary", 13: "ai_state", 28: "human_override",
    29: "last_message_id", 30: "last_inbound_text", 31: "handoff_flag", 32: "history_json",
  });
  const lead = Array(33).fill("");
  Object.assign(lead, { 0: body.first_name, 2: PHONE, 10: "N", 22: new Date(INITIAL), 27: STABLE });
  const initial = Array(19).fill("");
  Object.assign(initial, {
    0: new Date(INITIAL), 1: "sent", 2: "scheduler-initial-2-0123456789abcdef",
    3: "initial-2-0123456789abcdef", 4: PHONE, 5: "Initial outreach",
    6: `__initial_outreach__:${JSON.stringify({ crm_row: 2, stable_id: STABLE })}`,
    14: new Date(INITIAL), 17: 2,
  });
  return {
    body, headers, lead, leads: [headers, lead], inbound: [Array(8).fill("")],
    outbox: [Array(19).fill(""), initial, pendingRow(body, status)],
    now: Date.parse("2026-10-16T19:05:00Z"), lockOwner: null, locks: [], writes: [], loads: [], assetOps: [],
    sequence: 0, takeover: false,
  };
}

function harness(w, id = "a", options = {}) {
  let beforeLock = options.beforeLock;
  let duringInflate = options.duringInflate;
  const sheets = {};
  const makeSheet = (name, data) => ({
    getLastRow: () => data.length,
    getDataRange: () => ({ getValues: () => data.map((row) => row.slice()) }),
    appendRow: (row) => { w.writes.push({ id, name, kind: "append", lock: w.lockOwner }); data.push(row.slice()); },
    getRange: (start, column, count = 1, width = 1) => {
      if (typeof start === "string") {
        const match = /^([A-Z]+)(\d+)$/.exec(start);
        column = [...match[1]].reduce((n, letter) => n * 26 + letter.charCodeAt(0) - 64, 0);
        start = Number(match[2]);
      }
      return {
        getValues: () => {
          if (options.failLockedRead && w.lockOwner === id) throw new Error("synthetic locked sheet read failure");
          return Array.from({ length: count }, (_, i) =>
            Array.from({ length: width }, (_, j) => data[start - 1 + i]?.[column - 1 + j] ?? ""));
        },
        getValue: () => data[start - 1]?.[column - 1] ?? "",
        setValue: (value) => {
          w.writes.push({ id, name, kind: "cell", lock: w.lockOwner });
          data[start - 1][column - 1] = value;
        },
        setValues: (values) => {
          if (options.failWrite) throw new Error("synthetic atomic sheet write failure");
          w.writes.push({ id, name, kind: "range", lock: w.lockOwner });
          values.forEach((row, i) => row.forEach((value, j) => { data[start - 1 + i][column - 1 + j] = value; }));
        },
      };
    },
  });
  sheets.sms_pending_sends = makeSheet("outbox", w.outbox);
  sheets.sms_inbound_queue = makeSheet("inbound", w.inbound);
  const leads = makeSheet("leads", w.leads);
  class ClockDate extends Date {
    constructor(...args) { super(...(args.length ? args : [w.now])); }
    static now() { return w.now; }
  }
  const signed = (bytes) => [...Buffer.from(bytes)].map((value) => value > 127 ? value - 256 : value);
  const blob = (bytes) => ({ getBytes: () => signed(bytes), getDataAsString: () => Buffer.from(bytes).toString("utf8") });
  const largeAssetOperation = (operation, size) => {
    if (size < 500000) return;
    w.assetOps.push({ id, operation, size, lock: w.lockOwner });
    assert.equal(w.lockOwner, null, `large lexical ${operation} must remain outside the claim lock`);
  };
  const context = vm.createContext({
    Date: ClockDate, JSON: {
      stringify: JSON.stringify,
      parse: (text, ...args) => { largeAssetOperation("JSON parse", text.length); return JSON.parse(text, ...args); },
    }, String, Number, Object, Array, Math,
    normalizePhone_: (value) => String(value ?? "").replace(/\D/g, "").replace(/^1(?=\d{10}$)/, ""),
    normalizeWhitespace_: (value) => String(value).trim().replace(/\s+/g, " "),
    normalizePendingSmsInboundText_: (value) => String(value ?? "").trim().toLowerCase(),
    normalizePendingSmsReply_: (value) => String(value ?? "").trim().toLowerCase(),
    hasPendingSmsTakeoverV11_: () => w.takeover,
    installSmsOutboxTriggers_: () => {}, ensureSmsSheetHeaders_: () => {},
    getSmsSpreadsheet_: () => ({ getSheetByName: (name) => sheets[name] ?? null,
      getSpreadsheetTimeZone: () => "America/New_York" }),
    getSheet_: () => leads,
    getSheetData_: () => w.leads.slice(1).map((values, i) => ({ row: i + 2,
      obj: Object.fromEntries(w.headers.map((header, j) => [header, values[j]])) })),
    HEADERS: {
      agent_name: "agent_name", phone: "phone", human_override: "human_override",
      mailshake_status: "mailshake_status", followup_text_sent: "follow_up",
      last_message_id: "last_message_id", last_inbound_text: "last_inbound_text", handoff_flag: "handoff_flag",
      conversation_summary: "conversation_summary", ai_state: "ai_state", history_json: "history_json",
    },
    LockService: { getScriptLock: () => {
      let interval;
      return {
        tryLock: (timeout) => {
          if (beforeLock) { const hook = beforeLock; beforeLock = null; hook(context); }
          if (w.lockOwner !== null) { w.now += timeout; return false; }
          w.lockOwner = id;
          interval = { id, start: w.now, timeout };
          w.locks.push(interval);
          return true;
        },
        releaseLock: () => {
          assert.equal(w.lockOwner, id, "only the lock owner may release it");
          interval.end = w.now;
          w.lockOwner = null;
        },
      };
    } },
    Utilities: {
      DigestAlgorithm: { SHA_256: "sha256" },
      computeDigest: (_algorithm, bytes) => {
        largeAssetOperation("hash", bytes.length);
        return signed(createHash("sha256").update(Buffer.from(bytes)).digest());
      },
      base64Decode: (value) => { largeAssetOperation("base64 decode", value.length); return signed(Buffer.from(value, "base64")); },
      newBlob: (bytes) => blob(Buffer.from(bytes)),
      ungzip: (input) => {
        w.loads.push({ id, lock: w.lockOwner, start: w.now });
        assert.equal(w.lockOwner, null, "cold lexicon inflation must never occur under the shared outbox lock");
        w.now += 5025;
        if (duringInflate) { const hook = duringInflate; duringInflate = null; hook(context); }
        if (options.corrupt) return blob(Buffer.from("corrupt lexical data"));
        return blob(gunzipSync(Buffer.from(input.getBytes())));
      },
      getUuid: () => `uuid-${++w.sequence}`,
      parseDate: () => { throw new Error("Only exact ISO timestamps are used in this fixture"); },
      formatDate: (date) => date.toISOString().slice(0, 10),
    },
    UrlFetchApp: { fetch: () => { throw new Error("network forbidden"); } },
  });
  vm.runInContext(lexiconSource, context);
  vm.runInContext(outboxSource, context);
  context.installSmsOutboxTriggers_ = () => {};
  context.ensureSmsSheetHeaders_ = () => {};
  return { context, claim: (worker = id) => context.claimPendingSmsSendV10_({ worker_id: worker }),
    setBeforeLock: (hook) => { beforeLock = hook; },
    setDuringInflate: (hook) => { duringInflate = hook; } };
}

function noSend(result) { assert.notEqual(result.should_send, true); }
function expectShortLocks(w) {
  assert.ok(w.locks.length > 0);
  for (const interval of w.locks) assert.ok(interval.end - interval.start < 3000, JSON.stringify(interval));
  assert.equal(w.lockOwner, null);
  assert.ok(w.loads.every((load) => load.lock === null));
  assert.ok(w.assetOps.every((operation) => operation.lock === null));
}
function inboundReply(w) {
  w.inbound.push([new Date(w.now), "queued", "q1", "dedupe", "message-1", PHONE, "Thanks", new Date(w.now)]);
}
function addUnrelatedExactEpisode(w) {
  const body = videoBody("Kathy", "kathy");
  Object.assign(body, {
    crm_row: 3, phone: "9542053206", stable_id: "listing-other", followup_key: "9".repeat(64),
    message_id: `followup-video-${"9".repeat(24)}`, request_id: `scheduler-followup-video-${"9".repeat(24)}`,
  });
  Object.assign(body.video_speech_proof, { phone: body.phone, stable_id: body.stable_id, followup_key: body.followup_key });
  const lead = w.lead.slice();
  Object.assign(lead, { 0: "Kathy", 2: body.phone, 27: body.stable_id });
  w.leads.push(lead);
  const initial = w.outbox[1].slice();
  Object.assign(initial, {
    2: "scheduler-initial-3-1123456789abcdef", 3: "initial-3-1123456789abcdef", 4: body.phone,
    6: `__initial_outreach__:${JSON.stringify({ crm_row: 3, stable_id: body.stable_id })}`, 17: 3,
  });
  w.outbox.push(initial);
  const pending = pendingRow(body);
  w.outbox.push(pending);
  return pending;
}

function requireFutureDecisionEvidence(w) {
  w.outbox[1][13] = new Date(INITIAL);
  const metadata = JSON.parse(w.outbox[2][6].replace(/^__scheduled_followup__:/, ""));
  metadata.initial_receipt = {
    schema: "initial-sms-receipt-v1", request_id: w.outbox[1][2], message_id: w.outbox[1][3],
    phone: PHONE, stable_id: STABLE, created_at: INITIAL, send_started_at: INITIAL,
    sent_at: INITIAL, status: "sent",
  };
  metadata.audit_contract = "followup-decision-v1";
  Object.assign(metadata.video_speech_proof, {
    evidence_version: "same-take-evidence-v1", evidence_contract: "same-take-evidence-required-v1",
    evidence_sha256: "7".repeat(64),
  });
  w.outbox[2][6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
}

function futureJournal(w) {
  return JSON.parse(w.outbox[2][6].replace(/^__scheduled_followup__:/, "")).decision_evidence;
}

test("future evidence claim and cold send-start keep all lexical work outside shared locks", () => {
  const w = world();
  requireFutureDecisionEvidence(w);
  const claim = harness(w, "claim").claim();
  assert.equal(claim.should_send, true);
  const result = harness(w, "start").context.markPendingSmsSendStartedV10_(claim);
  assert.equal(result.ok, true);
  assert.equal(result.status, "send_started");
  assert.equal(futureJournal(w).events.length, 2);
  assert.ok(w.writes.every(write => write.lock !== null));
  expectShortLocks(w);
});

test("future overlapping claim callbacks retain only one immutable claim event and lease", () => {
  const w = world();
  requireFutureDecisionEvidence(w);
  const second = harness(w, "second");
  let inner;
  const first = harness(w, "first", { duringInflate: () => { inner = second.claim("same-worker"); } });
  const outer = first.claim("same-worker");
  assert.equal(outer.lease_token, inner.lease_token);
  assert.equal(w.outbox[2][9], 1);
  assert.equal(futureJournal(w).events.length, 1);
  expectShortLocks(w);
});

test("future send-start rechecks replies after cold warming and journals denial", () => {
  const w = world();
  requireFutureDecisionEvidence(w);
  const claim = harness(w, "claim").claim();
  const original = JSON.stringify(futureJournal(w).events[0]);
  const start = harness(w, "start", { duringInflate: () => inboundReply(w) });
  const result = start.context.markPendingSmsSendStartedV10_(claim);
  noSend(result);
  assert.equal(result.status, "superseded");
  assert.equal(JSON.stringify(futureJournal(w).events[0]), original);
  assert.equal(futureJournal(w).events[1].stage, "send_start_blocked");
  assert.equal(futureJournal(w).events[1].decision.blocked_checks.queued_inbound, true);
  expectShortLocks(w);
});

test("future overlapping send-start callbacks append one event and never overwrite it", () => {
  const w = world();
  requireFutureDecisionEvidence(w);
  const claim = harness(w, "claim").claim();
  const second = harness(w, "second");
  let inner;
  const first = harness(w, "first", { beforeLock: () => { inner = second.context.markPendingSmsSendStartedV10_(claim); } });
  const outer = first.context.markPendingSmsSendStartedV10_(claim);
  assert.equal(inner.ok, true);
  assert.equal(outer.duplicate, true);
  assert.equal(futureJournal(w).events.length, 2);
  assert.equal(w.outbox[2][9], 1);
  expectShortLocks(w);
});

test("future expired unstarted lease appends new claim evidence without replacing the prior event", () => {
  const w = world();
  requireFutureDecisionEvidence(w);
  const h = harness(w, "claim");
  const first = h.claim();
  const original = JSON.stringify(futureJournal(w).events[0]);
  w.now += 301000;
  const second = h.claim();
  assert.notEqual(first.lease_token, second.lease_token);
  assert.equal(second.attempts, 2);
  assert.equal(futureJournal(w).events.length, 2);
  assert.equal(JSON.stringify(futureJournal(w).events[0]), original);
  assert.equal(futureJournal(w).events[1].previous_sha256, futureJournal(w).events[0].sha256);
  expectShortLocks(w);
});

test("future send-start lock contention preserves claim and never grants authorization", () => {
  const w = world();
  requireFutureDecisionEvidence(w);
  const claim = harness(w, "claim").claim();
  const original = JSON.stringify(w.outbox[2]);
  const start = harness(w, "start", { beforeLock: () => { w.lockOwner = "other"; } });
  const result = start.context.markPendingSmsSendStartedV10_(claim);
  noSend(result);
  assert.equal(result.retryable, true);
  assert.equal(JSON.stringify(w.outbox[2]), original);
  assert.equal(start.context.VIDEO_PRONUNCIATION_CACHE_ONLY_V24_, false);
});

test("5.025-second cold inflation occurs before the claim lock, with real pinned dictionary validation", () => {
  const w = world();
  const h = harness(w);
  const before = w.now;
  const result = h.claim();
  assert.equal(result.should_send, true);
  assert.equal(result.reply_text, w.body.message);
  assert.equal(w.loads.length, 1);
  assert.equal(w.now - before, 5025);
  assert.equal(w.outbox.length, 3);
  assert.equal(w.outbox[2][9], 1);
  assert.equal(new Date(w.outbox[2][11]).getTime(), w.now + 5 * 60 * 1000, "lease starts from fresh locked time");
  assert.deepEqual(w.assetOps.map((item) => item.operation), ["base64 decode", "hash", "hash", "JSON parse"]);
  expectShortLocks(w);
});

test("two cold claimers interleaving during warm-up issue only one lease", () => {
  const w = world();
  const second = harness(w, "second");
  let inner;
  const first = harness(w, "first", { duringInflate: () => { inner = second.claim(); } });
  const outer = first.claim();
  assert.equal(inner.should_send, true);
  noSend(outer);
  assert.equal(w.outbox[2][10], inner.lease_token);
  assert.equal(w.outbox[2][18], "second");
  assert.equal(w.outbox[2][9], 1);
  assert.equal(w.outbox.length, 3);
  expectShortLocks(w);
});

test("same-worker HTTP replay after concurrent warm-up returns the exact existing lease", () => {
  const w = world();
  const second = harness(w, "second");
  let inner;
  const first = harness(w, "first", { duringInflate: () => { inner = second.claim("phone-worker"); } });
  const replay = first.claim("phone-worker");
  assert.equal(replay.should_send, true);
  assert.equal(replay.lease_token, inner.lease_token);
  assert.equal(replay.pending_row, inner.pending_row);
  assert.equal(replay.attempts, 1);
  assert.equal(w.outbox[2][9], 1);
  expectShortLocks(w);
});

test("standard-text enqueue overlapping cold inflation preserves the existing video episode", () => {
  const w = world();
  const enqueue = harness(w, "enqueue");
  let result;
  const claim = harness(w, "claim", { duringInflate: () => {
    result = enqueue.context.enqueueFollowupSmsV13_({
      crm_row: 2, stable_id: STABLE, phone: PHONE, message: "Original normal text",
      message_id: "followup-2-other", request_id: "scheduler-followup-2-other",
    }, "webhook-overlap");
  } });
  assert.equal(claim.claim().should_send, true);
  assert.equal(result.duplicate, true);
  assert.equal(result.payload_frozen, true);
  assert.equal(w.outbox.length, 3);
  assert.equal(w.outbox[2][5], w.body.message);
  assert.equal(w.outbox[2][9], 1);
  expectShortLocks(w);
});

for (const [label, mutate] of [
  ["current first name", (w) => { w.lead[0] = "Martha"; }],
  ["current phone", (w) => { w.lead[2] = "9542053206"; }],
  ["initial episode", (w) => { w.lead[22] = new Date("2026-10-16T14:00:00Z"); }],
  ["queued reply", inboundReply],
  ["recorded reply", (w) => { w.lead[30] = "Thanks"; }],
  ["takeover", (w) => { w.takeover = true; }],
  ["handoff", (w) => { w.lead[31] = true; }],
  ["opt-out", (w) => { w.lead[10] = "R"; }],
  ["manual conversation", (w) => { w.lead[9] = "Spoke with agent"; }],
  ["already recorded follow-up", (w) => { w.lead[8] = "x"; }],
]) {
  test(`fresh ${label} change during prewarm is checked before claiming`, () => {
    const w = world();
    const h = harness(w, "claim", { duringInflate: () => mutate(w) });
    noSend(h.claim());
    assert.equal(w.outbox[2][9], 0);
    assert.equal(w.outbox[2][10], "");
    assert.equal(w.outbox.length, 3);
    expectShortLocks(w);
  });
}

for (const status of ["sent", "send_started", "receipt_pending", "uncertain", "failed", "superseded"]) {
  test(`terminal or delivery-locked ${status} row appearing during warm-up is never reopened`, () => {
    const w = world();
    const h = harness(w, "claim", { duringInflate: () => { w.outbox[2][1] = status; } });
    noSend(h.claim());
    assert.equal(w.outbox[2][1], status);
    assert.equal(w.outbox[2][9], 0);
    assert.equal(w.outbox[2][10], "");
    assert.equal(w.outbox.length, 3);
    expectShortLocks(w);
  });
}

test("a v4 lexical row arriving after the prewarm snapshot is retryable without cold inflation or mutation", () => {
  const w = world(videoBody("Marc", "marc"));
  const h = harness(w, "claim", { beforeLock: () => { w.outbox[2] = pendingRow(videoBody()); } });
  const result = h.claim();
  noSend(result);
  assert.equal(result.retryable, true);
  assert.equal(w.loads.length, 0);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.outbox[2][9], 0);
  assert.equal(w.writes.length, 0);
  expectShortLocks(w);
});

test("loss of the prewarmed cache before lock acquisition cannot cause cold work under lock", () => {
  const w = world();
  const h = harness(w, "claim", { beforeLock: (context) => { context.VIDEO_PRONUNCIATION_LEXICON_V24_CACHE_ = null; } });
  const result = h.claim();
  noSend(result);
  assert.equal(result.retryable, true);
  assert.equal(w.loads.length, 1);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.writes.length, 0);
  expectShortLocks(w);
});

test("corrupt lexical data keeps work queued and retryable without issuing a lease", () => {
  const w = world();
  const h = harness(w, "claim", { corrupt: true });
  const result = h.claim();
  noSend(result);
  assert.equal(result.retryable, true);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.outbox[2][9], 0);
  assert.equal(w.writes.length, 0);
  assert.equal(w.loads.length, 1);
  if (w.locks.length) expectShortLocks(w);
});

for (const [name, observed] of [["Marc", "marc"], ["CYDNEY", "sydney"], ["Brook", "brooke"]]) {
  test(`exact/reviewed ${name}/${observed} path does not incur dictionary inflation`, () => {
    const w = world(videoBody(name, observed));
    const h = harness(w);
    assert.equal(h.claim().should_send, true);
    assert.equal(w.loads.length, 0);
    expectShortLocks(w);
  });
}

test("future not-before time stays queued rather than being caught up by prewarming", () => {
  const w = world();
  w.outbox[2][8] = new Date(w.now + 3600000);
  const h = harness(w);
  noSend(h.claim());
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.outbox[2][9], 0);
  assert.equal(w.writes.length, 0);
  expectShortLocks(w);
});

test("missing lexical helpers fail closed only for the lexical video, preserving exact proof behavior", () => {
  const w = world();
  const h = harness(w);
  h.context.videoPronunciationLexiconV24_ = undefined;
  h.context.videoPronunciationLexiconManifestV24_ = undefined;
  const result = h.claim();
  noSend(result);
  assert.equal(result.retryable, true);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.writes.length, 0);
  assert.equal(w.loads.length, 0);
});

test("snapshot row reordering never writes the claim into the obsolete row number", () => {
  const w = world();
  const original = w.outbox[2];
  const h = harness(w, "claim", { duringInflate: () => {
    const terminal = pendingRow(w.body, "failed");
    terminal[2] = "unrelated-terminal";
    terminal[3] = "unrelated-terminal";
    terminal[4] = "9542053206";
    terminal[6] = "not a follow-up";
    w.outbox.splice(2, 0, terminal);
  } });
  const result = h.claim();
  assert.equal(result.should_send, true);
  assert.equal(result.pending_row, 4);
  assert.equal(w.outbox[2][1], "failed");
  assert.equal(w.outbox[2][9], 0);
  assert.equal(w.outbox[3][1], "claimed");
  assert.equal(w.outbox[3][2], original[2]);
  expectShortLocks(w);
});

test("a removed candidate is not recreated from the prewarm snapshot", () => {
  const w = world();
  const h = harness(w, "claim", { duringInflate: () => { w.outbox.splice(2, 1); } });
  noSend(h.claim());
  assert.equal(w.outbox.length, 2);
  assert.equal(w.writes.length, 0);
  expectShortLocks(w);
});

test("claim lock contention after warm-up leaves no mode leak or queued-row mutation", () => {
  const w = world();
  const h = harness(w, "claim", { beforeLock: () => { w.lockOwner = "external"; } });
  const result = h.claim();
  noSend(result);
  assert.equal(result.retryable, true);
  assert.equal(w.writes.length, 0);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(h.context.VIDEO_PRONUNCIATION_CACHE_ONLY_V24_, false);
  w.lockOwner = null;
  h.context.VIDEO_PRONUNCIATION_LEXICON_V24_CACHE_ = null;
  assert.equal(h.context.hasValidVideoFollowupSpeechProofV21_(w.body, w.body), true);
  assert.equal(w.loads.length, 2, "ordinary validation outside claim may load normally after contention");
});

test("atomic write exception releases the claim lock and restores normal validation mode", () => {
  const w = world();
  const h = harness(w, "claim", { failWrite: true });
  assert.throws(() => h.claim(), /synthetic atomic sheet write failure/);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.outbox[2][9], 0);
  assert.equal(w.lockOwner, null);
  assert.equal(h.context.VIDEO_PRONUNCIATION_CACHE_ONLY_V24_, false);
  h.context.VIDEO_PRONUNCIATION_LEXICON_V24_CACHE_ = null;
  assert.equal(h.context.hasValidVideoFollowupSpeechProofV21_(w.body, w.body), true);
  assert.equal(w.loads.length, 2);
  expectShortLocks(w);
});

test("same-worker active lease with cache failure stays unchanged and retryable", () => {
  const w = world();
  const h = harness(w, "claim", { corrupt: true });
  Object.assign(w.outbox[2], {
    1: "claimed", 9: 1, 10: "existing-token", 11: new Date(w.now + 60000), 18: "phone-worker",
  });
  const before = JSON.stringify(w.outbox);
  const result = h.claim("phone-worker");
  noSend(result);
  assert.equal(result.retryable, true);
  assert.equal(JSON.stringify(w.outbox), before);
  assert.equal(w.writes.length, 0);
  if (w.locks.length) expectShortLocks(w);
});

test("a lease expiring during warm-up is freshly reclaimed with a new token, once", () => {
  const w = world();
  Object.assign(w.outbox[2], {
    1: "claimed", 9: 1, 10: "old-token", 11: new Date(w.now + 4000), 18: "phone-worker",
  });
  const h = harness(w);
  const result = h.claim("phone-worker");
  assert.equal(result.should_send, true);
  assert.notEqual(result.lease_token, "old-token");
  assert.equal(result.attempts, 2);
  assert.equal(new Date(w.outbox[2][11]).getTime(), w.now + 300000);
  expectShortLocks(w);
});

for (const status of ["queued", "claimed"]) {
  test(`incoming exact proof preserves frozen general ${status} proof and warms outside enqueue lock`, () => {
    const w = world();
    w.outbox[2][1] = status;
    const stored = JSON.stringify(w.outbox[2]);
    const h = harness(w, "enqueue");
    const result = h.context.enqueueFollowupSmsV13_(videoBody("Marc", "marc"), "webhook-duplicate");
    assert.equal(result.duplicate, true);
    assert.equal(result.payload_frozen, true);
    assert.equal(result.ok, true);
    assert.equal(result.status, status);
    assert.equal(JSON.stringify(w.outbox[2]), stored);
    assert.equal(w.outbox.length, 3);
    assert.equal(w.writes.length, 0);
    assert.equal(w.loads.length, 1);
    expectShortLocks(w);
  });
}

test("reply arriving while frozen general proof warms prevents exact-proof duplicate enqueue", () => {
  const w = world();
  const h = harness(w, "enqueue", { duringInflate: () => inboundReply(w) });
  const result = h.context.enqueueFollowupSmsV13_(videoBody("Marc", "marc"), "webhook-duplicate");
  assert.equal(result.duplicate, true);
  assert.equal(result.ok, false);
  assert.equal(result.queued, false);
  assert.match(result.error, /inbound reply/i);
  assert.equal(w.outbox.length, 3);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.writes.length, 0);
  expectShortLocks(w);
});

test("frozen general proof cache loss in enqueue fails retryably without replacement", () => {
  const w = world();
  const h = harness(w, "enqueue", { beforeLock: (context) => { context.VIDEO_PRONUNCIATION_LEXICON_V24_CACHE_ = null; } });
  const before = JSON.stringify(w.outbox);
  const result = h.context.enqueueFollowupSmsV13_(videoBody("Marc", "marc"), "webhook-duplicate");
  assert.equal(result.duplicate, true);
  assert.equal(result.ok, false);
  assert.equal(result.retryable, true);
  assert.equal(JSON.stringify(w.outbox), before);
  assert.equal(w.loads.length, 1);
  assert.equal(w.writes.length, 0);
  expectShortLocks(w);
});

test("a lexical frozen row appearing after enqueue prewarm is held without cold work or appending", () => {
  const exact = videoBody("Marc", "marc");
  const w = world(exact);
  const h = harness(w, "enqueue", { beforeLock: () => { w.outbox[2] = pendingRow(videoBody()); } });
  const result = h.context.enqueueFollowupSmsV13_(exact, "webhook-duplicate");
  assert.equal(result.ok, false);
  assert.equal(result.duplicate, true);
  assert.equal(result.retryable, true);
  assert.equal(w.outbox.length, 3);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.writes.length, 0);
  assert.equal(w.loads.length, 0);
  expectShortLocks(w);
});

for (const operation of ["claim", "enqueue"]) {
  test(`fresh locked read exception restores cache mode and releases the ${operation} lock`, () => {
    const w = world();
    const h = harness(w, operation, { failLockedRead: true });
    const action = operation === "claim" ? () => h.claim()
      : () => h.context.enqueueFollowupSmsV13_(videoBody("Marc", "marc"), "webhook-duplicate");
    assert.throws(action, /synthetic locked sheet read failure/);
    assert.equal(h.context.VIDEO_PRONUNCIATION_CACHE_ONLY_V24_, false);
    assert.equal(w.lockOwner, null);
    assert.equal(w.writes.length, 0);
    assert.equal(w.outbox[2][1], "queued");
    expectShortLocks(w);
  });
}

test("failed lexical warm-up does not starve a separate exact-proof recipient", () => {
  const w = world();
  const exact = addUnrelatedExactEpisode(w);
  const h = harness(w, "claim", { corrupt: true });
  const result = h.claim();
  assert.equal(result.should_send, true);
  assert.equal(result.phone, exact[4]);
  assert.equal(w.outbox[2][1], "queued");
  assert.equal(w.outbox[2][9], 0);
  assert.equal(w.outbox[4][1], "claimed");
  assert.equal(w.outbox[4][9], 1);
  assert.equal(w.loads.length, 1);
  expectShortLocks(w);
});

test("not-before crossing during warm-up uses fresh time without reopening the first candidate", () => {
  const w = world();
  const exact = addUnrelatedExactEpisode(w);
  exact[8] = new Date(w.now + 4000);
  const h = harness(w, "claim", { duringInflate: () => { w.outbox[2][1] = "failed"; } });
  const result = h.claim();
  assert.equal(result.should_send, true);
  assert.equal(result.phone, exact[4]);
  assert.equal(w.outbox[2][1], "failed");
  assert.equal(w.outbox[2][9], 0);
  assert.equal(w.outbox[4][1], "claimed");
  assert.equal(new Date(w.outbox[4][11]).getTime(), w.now + 300000);
  expectShortLocks(w);
});

for (const [label, mutate] of [
  ["wrong spoken name", (proof) => { proof.normalized_transcript = "hey martha"; }],
  ["extra greeting", (proof) => { proof.normalized_transcript = "hey mark hey jim"; }],
  ["low confidence", (proof) => { proof.confidence[0].avg_logprob = -1.01; }],
  ["mixed policy tuple", (proof) => { proof.verification_method = "planned-aac-lineage-v2"; }],
  ["changed final media", (proof) => { proof.media_sha256 = "4".repeat(64); }],
]) {
  test(`fresh ${label} proof corruption during warm-up never uses the warmed acceptance decision`, () => {
    const w = world();
    const h = harness(w, "claim", { duringInflate: () => {
      const metadata = JSON.parse(w.outbox[2][6].replace(/^__scheduled_followup__:/, ""));
      mutate(metadata.video_speech_proof);
      w.outbox[2][6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
    } });
    const result = h.claim();
    noSend(result);
    assert.equal(result.retryable, true);
    assert.equal(w.outbox[2][1], "queued");
    assert.equal(w.outbox[2][9], 0);
    assert.equal(w.writes.length, 0);
    expectShortLocks(w);
  });
}
