import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";

const root = path.join(import.meta.dirname, "..");
const outboxSource = fs.readFileSync(path.join(root, "apps_script/sms_outbox.js"), "utf8");
const postSource = fs.readFileSync(path.join(root, "apps_script/zz_unified_post.js"), "utf8");

function schedulerSerialized(body) {
  // Exercise the real Python request boundary, not a JS copy of its allowlist.
  const result = spawnSync(process.env.PYTHON || "python3", [
    "-I", "-B", path.join(root, "tests/serialize_followup_payload_fixture.py"),
  ], {
    encoding: "utf8", timeout: 10000, maxBuffer: 128 * 1024,
    input: JSON.stringify({
      action: "enqueue_followup_sms", row_idx: body.crm_row, phone_digits: body.phone,
      message: body.message, stable_id: body.stable_id, followup_context: body,
    }),
  });
  assert.equal(result.error, undefined);
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}
const initial = "2026-10-13T13:00:00.000Z";
const key = "a".repeat(64);
const base = {
  crm_row: 2, stable_id: "listing-123", phone: "9542053205",
  followup_key: key, initial_sent_at: initial, video_variant: "text", video_job_id: "",
  message_id: `followup-video-${key.slice(0, 24)}`,
  request_id: `scheduler-followup-video-${key.slice(0, 24)}`,
  message: "Original follow-up text",
};
const speechPolicy = {
  accepted: true, policy_version: "exact-greeting-tiny-en-v1", model_id: "Systran/faster-whisper-tiny.en",
  model_revision: "0d3d19a32d3338f10357c0889762bd8d64bbdeba",
  model_manifest_sha256: "9f995ad3147f0799b2babb9e84926a53bca82d888d5fb6558d925f38dd2b78ee",
  config_sha256: "f1a2b9866f0576c6500f760d0b58b7a29ed8acf9d337aacc0c66c134ad39e9ca",
};
const spokenSpeechPolicy = {
  ...speechPolicy, policy_version: "spoken-greeting-tiny-en-v2",
  config_sha256: "b3a1b5798d53032e5c23da361c467141c1cfd502a8def78e8d3ddeaabde2322e",
};
const canonicalName = (name) => String(name).normalize("NFKC").replace(/[\u2018\u2019\u02bc]/g, "'").trim();
const video = (changes = {}) => {
  const body = { ...base, video_variant: "video", video_job_id: "job_123", first_name: "Jim", ...changes };
  const name = canonicalName(body.first_name);
  if (!Object.hasOwn(changes, "message")) body.message = `Hey ${name}, here's your video: https://go.crispshortsales.com/v/${body.video_job_id}`;
  if (!Object.hasOwn(changes, "video_speech_proof")) body.video_speech_proof = {
    ...speechPolicy, video_job_id: body.video_job_id, followup_key: body.followup_key,
    phone: body.phone, initial_sent_at: body.initial_sent_at, stable_id: body.stable_id,
    source_audio_sha256: "b".repeat(64), media_sha256: "c".repeat(64),
    expected_name: name, normalized_transcript: `hey ${name.toLowerCase()}`,
    greeting_seconds: 1.5, confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.01 }],
  };
  return body;
};
const spokenVideo = (changes = {}, observedName) => {
  const body = video(changes);
  body.video_speech_proof = { ...body.video_speech_proof, ...spokenSpeechPolicy };
  if (observedName !== undefined) body.video_speech_proof.normalized_transcript = `hey ${observedName}`;
  return body;
};
const canonicalDigest = (value) => {
  const sorted = (item) => Array.isArray(item) ? item.map(sorted) : item && typeof item === "object"
    ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, sorted(item[key])])) : item;
  return createHash("sha256").update(JSON.stringify(sorted(value))).digest("hex");
};
const plannedVideo = () => {
  const body = spokenVideo();
  const proof = body.video_speech_proof;
  const rawProof = Object.fromEntries([
    "accepted", "policy_version", "model_id", "model_revision", "model_manifest_sha256", "config_sha256",
    "source_audio_sha256", "expected_name", "normalized_transcript", "confidence",
  ].map((field) => [field, proof[field]]));
  const verification = {
    method: "planned-aac-lineage-v1", lineage_version: "planned-aac-same-take-v1",
    extraction_version: "ffmpeg-pcm-s16le-mono-16k-v1", speech_config_sha256: spokenSpeechPolicy.config_sha256,
    binding: "immutable-planned-aac-full-pcm-and-greeting-v1",
  };
  // Full provider-shaped fixture; media bytes are synthetic, not an ASR-quality test.
  Object.assign(proof, {
    verification_method: verification.method,
    verification_config_sha256: canonicalDigest(verification),
    audio_lineage: {
      accepted: true, lineage_version: verification.lineage_version, first_name: proof.expected_name,
      manifest_sha256: "d".repeat(64), planned_audio_sha256: "e".repeat(64),
      final_media_sha256: proof.media_sha256, checked_audio_sha256: proof.source_audio_sha256,
      full_pcm_sha256: "f".repeat(64), greeting_pcm_sha256: "1".repeat(64),
      aac_track_sha256: "2".repeat(64), speech_proof_sha256: canonicalDigest(rawProof),
      greeting_seconds: proof.greeting_seconds,
    },
  });
  return body;
};
const directedVideo = (name = "Colin", observed = "collin") => {
  const body = plannedVideo();
  body.first_name = name;
  body.message = `Hey ${name}, here's your video: https://go.crispshortsales.com/v/${body.video_job_id}`;
  const proof = body.video_speech_proof;
  Object.assign(proof, {
    policy_version: "spoken-greeting-tiny-en-v5",
    config_sha256: "060ec33e383b742872a129c3652143f658313851fd61594cec243789b5dcb8fd",
    expected_name: name, normalized_transcript: `hey ${observed}`,
    verification_method: "planned-aac-lineage-v4",
    verification_config_sha256: "528159aa9b387b42fbc0a4987953e5e46fd36e808ee098a677b5f8cab5ba7592",
  });
  proof.audio_lineage.first_name = name;
  proof.audio_lineage.speech_proof_sha256 = canonicalDigest(Object.fromEntries([
    "accepted", "policy_version", "model_id", "model_revision", "model_manifest_sha256", "config_sha256",
    "source_audio_sha256", "expected_name", "normalized_transcript", "confidence",
  ].map((field) => [field, proof[field]])));
  return body;
};
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

function harness({ initialProof = true } = {}) {
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
    12: "conversation_summary", 13: "ai_state",
    28: "human_override", 29: "last_message_id", 30: "last_inbound_text", 31: "handoff_flag",
    32: "history_json",
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
      DigestAlgorithm: { SHA_256: "SHA_256" },
      computeDigest: (_, text) => [...createHash("sha256").update(text).digest()].map(value => value > 127 ? value - 256 : value),
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
      conversation_summary: "conversation_summary", ai_state: "ai_state", history_json: "history_json",
    },
  };
  vm.createContext(context);
  vm.runInContext(outboxSource, context);
  // Load the router without replacing the harness's outbox test helpers.
  vm.runInContext(postSource.slice(0, postSource.indexOf("function getUnifiedIgnoredInboundReason_(")), context);
  context.installSmsOutboxTriggers_ = () => { triggers++; };
  context.ensureSmsSheetHeaders_ = () => {};
  if (initialProof) initialReceipt({ outbox }, initial);
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
  const messageId = `initial-2-${h.outbox.data.length.toString(16).padStart(16, "0")}`;
  const row = Array(19).fill("");
  Object.assign(row, {
    0: new Date(sentAt), 1: "sent", 2: `scheduler-${messageId}`, 3: messageId, 4: base.phone,
    5: "Original initial outreach text",
    6: `__initial_outreach__:${JSON.stringify({ crm_row: 2, stable_id: base.stable_id })}`,
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

test("current-phone initial proof is required even when H and W are populated", () => {
  for (const proof of ["none", "old_phone", "old_episode", "wrong_listing", "not_sent"]) {
    const h = harness({ initialProof: false });
    h.row[7] = "x";
    if (proof === "old_phone") initialReceipt(h, initial, { 4: "9542053206" });
    if (proof === "old_episode") initialReceipt(h, "2026-10-12T13:00:00Z");
    if (proof === "wrong_listing") initialReceipt(h, initial, { 6: '__initial_outreach__:{"crm_row":2,"stable_id":"another"}' });
    if (proof === "not_sent") initialReceipt(h, initial, { 1: "send_started" });
    const before = h.counters();
    const result = h.context.checkVideoFollowupEligibilityV19_(video());
    assert.equal(result.eligible, false, proof);
    assert.equal(result.retryable, true, proof);
    assert.match(result.reason, /initial.*current phone.*episode/i);
    assert.deepEqual(h.counters(), before);
    assert.equal(enqueue(h, video()).ok, false, proof);
  }
});

test("current-episode receipt cannot be replaced by an older receipt plus current H and W", () => {
  const h = harness({ initialProof: false });
  initialReceipt(h, "2026-10-12T13:00:00Z");
  const result = h.context.checkVideoFollowupEligibilityV19_(base);
  assert.equal(result.eligible, false);
  assert.equal(result.retryable, true);
});

test("current-phone initial proof rejects duplicate current-episode receipts", () => {
  const h = harness({ initialProof: false });
  initialReceipt(h, initial);
  initialReceipt(h, initial);
  assert.equal(h.context.checkVideoFollowupEligibilityV19_(video()).eligible, false);
});

test("exact-time initial receipt identifiers must be unique across the entire outbox", () => {
  const conflicts = [
    { 1: "failed" }, { 4: "9542053206" }, { 14: new Date("2026-10-12T13:00:00Z") },
    { 6: '__initial_outreach__:{"crm_row":2,"stable_id":"another"}' },
  ];
  for (const changes of conflicts) {
    for (const shared of ["both", "request", "message"]) {
      const h = harness();
      const conflicting = Object.assign(h.outbox.data[1].slice(), changes);
      if (shared === "request") conflicting[3] = "other-message";
      if (shared === "message") conflicting[2] = "other-request";
      h.outbox.data.push(conflicting);
      const label = `${shared}: ${JSON.stringify(changes)}`;
      const result = h.context.checkVideoFollowupEligibilityV19_(video());
      assert.equal(result.eligible, false, label);
      assert.equal(result.retryable, true, label);
      assert.equal(enqueue(h, video()).ok, false, label);
    }
  }
});

test("exact-time initial receipts support legacy opaque IDs but require both identifiers", () => {
  for (const identifiers of [
    ["legacy-initial-request", "legacy-initial-message"],
    ["", "legacy-initial-message"], ["legacy-initial-request", ""], ["", ""],
  ]) {
    const h = harness();
    [h.outbox.data[1][2], h.outbox.data[1][3]] = identifiers;
    const valid = identifiers.every(Boolean);
    const result = h.context.checkVideoFollowupEligibilityV19_(video());
    assert.equal(result.eligible, valid, JSON.stringify(identifiers));
    if (!valid) assert.equal(result.retryable, true);
    assert.equal(enqueue(h, video()).ok, valid);
  }
});

test("a second valid exact-time receipt cannot hide an ambiguous initial receipt", () => {
  for (const conflict of ["duplicate_id", "missing_ids"]) {
    const h = harness();
    if (conflict === "duplicate_id") {
      h.outbox.data.push(Object.assign(h.outbox.data[1].slice(), { 1: "failed" }));
      initialReceipt(h, initial);
    } else {
      initialReceipt(h, initial, { 2: "", 3: "" });
    }
    const result = h.context.checkVideoFollowupEligibilityV19_(video());
    assert.equal(result.eligible, false, conflict);
    assert.equal(result.retryable, true, conflict);
    assert.equal(enqueue(h, video()).ok, false, conflict);
  }
});

test("legacy exact-time receipts require the same current phone and agreeing CRM identity", () => {
  for (const metadata of [{ crm_row: 2 }, { crm_row: 3 }, {}]) {
    const h = harness({ initialProof: false });
    initialReceipt(h, initial, { 6: `__initial_outreach__:${JSON.stringify(metadata)}` });
    assert.equal(h.context.checkVideoFollowupEligibilityV19_(video()).eligible, metadata.crm_row === 2);
  }
});

test("missing initial proof holds claims and send-start without consuming the follow-up", () => {
  for (const phase of ["claim", "active_claim", "send_started"]) {
    const h = harness();
    enqueue(h, video());
    const claim = phase === "claim" ? null : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    h.outbox.data.splice(1, 1);
    const beforeStatus = h.outbox.data[1][1];
    const result = phase === "send_started"
      ? h.context.markPendingSmsSendStartedV10_(claim)
      : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(result.should_send, false, phase);
    assert.equal(result.retryable, true, phase);
    assert.equal(h.outbox.data[1][1], beforeStatus, phase);
  }
});

test("an actual matching initial receipt resolves the retryable proof hold", () => {
  const h = harness({ initialProof: false });
  assert.equal(h.context.checkVideoFollowupEligibilityV19_(video()).retryable, true);
  delayedInitialReceipt(h);
  assert.equal(h.context.checkVideoFollowupEligibilityV19_(video()).eligible, true);
});

test("first text/video choice is immutable under one logical key", () => {
  for (const first of [base, video()]) {
    const h = harness();
    assert.equal(enqueue(h, first).status, "queued");
    const duplicate = enqueue(h, first.video_variant === "text" ? video() : base);
    assert.equal(duplicate.ok, true);
    assert.equal(duplicate.payload_frozen, true);
    assert.equal(duplicate.video_variant, first.video_variant);
    assert.equal(h.outbox.data.length, 3);
    assert.equal(h.outbox.data[2][5], first.message);
  }
});

test("terminal and uncertain attempts never create another payload", () => {
  for (const status of ["receipt_pending", "uncertain", "failed", "superseded", "manual_sent", "sent"]) {
    const h = harness();
    enqueue(h);
    h.outbox.data[2][1] = status;
    const result = enqueue(h, video());
    assert.equal(result.duplicate, true, status);
    assert.equal(result.ok, status === "sent", status);
    assert.equal(h.outbox.data.length, 3);
    assert.equal(h.outbox.data[2][5], base.message);
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
    assert.equal(h.outbox.data.length, 3);
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
    assert.equal(h.outbox.data.length, 2);
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

test("actual videos persist their exact spoken-name proof without changing pre-render eligibility", () => {
  const h = harness();
  const before = h.counters();
  assert.equal(h.context.checkVideoFollowupEligibilityV19_(video({
    message: undefined, video_job_id: "", video_speech_proof: undefined,
  })).eligible, true);
  assert.deepEqual(h.counters(), before);
  const body = video();
  assert.equal(enqueue(h, body).ok, true);
  const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
  assert.deepEqual(metadata.video_speech_proof, body.video_speech_proof);
  assert.equal(h.context.getPendingSmsStaleReason_(h.outbox.data[2]), "");
});

test("new speech policy accepts exact names and only verified spelling pairs through send-start", () => {
  for (const [first_name, observed] of [
    ["Jim", "jim"], ["D'Andre", "d'andre"], ["Kathryn", "kathryn"], ["Nikki", "nikki"],
    ["Kathryn", "katherine"], ["Katherine", "kathryn"], ["Nikki", "nicky"], ["Nicky", "nikki"],
  ]) {
    const h = harness();
    h.row[0] = first_name;
    const body = spokenVideo({ first_name }, observed);
    assert.equal(enqueue(h, body).ok, true, `${first_name}: ${observed}`);
    const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
    assert.deepEqual(metadata.video_speech_proof, body.video_speech_proof);
    assert.equal(metadata.video_speech_proof.normalized_transcript, `hey ${observed}`);
    const claim = h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(claim.should_send, true);
    const started = h.context.markPendingSmsSendStartedV10_(claim);
    assert.equal(started.ok, true);
    assert.equal(started.status, "send_started");
    assert.equal(h.outbox.data.length, 3);
  }
});

test("planned AAC additive proof survives the unchanged Apps Script transport gate", () => {
  const h = harness();
  const body = plannedVideo();
  assert.equal(body.video_speech_proof.verification_config_sha256,
    "9f7fe677874406a34d5969c0c9f3a547225165764685805c5df50c97f3e831d6");
  assert.equal(enqueue(h, body).ok, true);
  const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
  assert.deepEqual(metadata.video_speech_proof, body.video_speech_proof);
  const claim = h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
  assert.equal(claim.should_send, true);
  assert.equal(claim.reply_text, body.message);
  const started = h.context.markPendingSmsSendStartedV10_(claim);
  assert.equal(started.ok, true);
  assert.equal(started.status, "send_started");
  assert.equal(h.outbox.data.length, 3);
});

test("directed v5 variants preserve canonical identity through enqueue, claim and send-start", () => {
  for (const [name, observed] of [["Colin", "collin"], ["Sheikh", "shake"]]) {
    const h = harness();
    h.row[0] = name;
    const body = directedVideo(name, observed);
    assert.equal(enqueue(h, body).ok, true);
    const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
    assert.equal(metadata.first_name, name);
    assert.deepEqual(metadata.video_speech_proof, body.video_speech_proof);
    const claim = h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(claim.should_send, true);
    assert.equal(claim.reply_text, body.message);
    assert.equal(h.context.markPendingSmsSendStartedV10_(claim).status, "send_started");
    assert.equal(h.outbox.data.length, 3);
  }
});

test("v5 cannot replace a frozen text choice or reopen terminal or in-flight episodes", () => {
  for (const status of ["queued", "claimed", "send_started", "receipt_pending", "uncertain",
    "failed", "superseded", "manual_sent", "sent"]) {
    const h = harness();
    h.row[0] = "Colin";
    assert.equal(enqueue(h, base).ok, true);
    h.outbox.data[2][1] = status;
    const before = h.outbox.data[2].slice();
    const result = enqueue(h, directedVideo());
    assert.equal(result.payload_frozen, true, status);
    assert.equal(h.outbox.data.length, 3, status);
    assert.deepEqual(h.outbox.data[2], before, status);
  }
});

test("v5 availability never reinterprets frozen legacy proofs or upgrades their method", () => {
  for (const phase of ["claim", "active_claim", "send_started"]) {
    const h = harness();
    h.row[0] = "Colin";
    assert.equal(enqueue(h, video({ first_name: "Colin" })).ok, true);
    const claim = phase === "claim" ? null : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
    metadata.video_speech_proof.normalized_transcript = "hey collin";
    h.outbox.data[2][6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
    const before = h.outbox.data[2].slice();
    const result = phase === "send_started" ? h.context.markPendingSmsSendStartedV10_(claim)
      : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(result.should_send, false, phase);
    assert.equal(result.retryable, true, phase);
    assert.deepEqual(h.outbox.data[2], before, phase);
    const replacement = enqueue(h, directedVideo());
    assert.equal(replacement.payload_frozen, true);
    assert.equal(replacement.ok, false);
    assert.deepEqual(h.outbox.data[2], before);
    assert.equal(h.outbox.data.length, 3);
  }
});

test("v5 queued replies, manual conversations and takeover still suppress before claim and send-start", () => {
  for (const mutate of [(h) => inboundReply(h), (h) => { h.row[10] = "H"; },
    (h) => { h.row[28] = "TRUE"; }, (h) => { h.row[31] = "TRUE"; },
    (h) => { h.row[12] = "Spoke with agent; callback requested"; },
    (h) => { h.setTakeover(true); }]) {
    for (const phase of ["claim", "active_claim", "send_started"]) {
      const h = harness();
      h.row[0] = "Colin";
      assert.equal(enqueue(h, directedVideo()).ok, true);
      const claim = phase === "claim" ? null : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
      mutate(h);
      const result = phase === "send_started" ? h.context.markPendingSmsSendStartedV10_(claim)
        : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
      assert.equal(result.should_send, false, phase);
      assert.equal(h.outbox.data[2][1], "superseded", phase);
      assert.equal(h.outbox.data.length, 3);
    }
  }
});

test("planned AAC additive fields cannot bypass existing name identity or speech thresholds", () => {
  for (const changes of [
    { expected_name: "Sue" }, { normalized_transcript: "hey sue" },
    { phone: "9542053206" }, { stable_id: "other" }, { followup_key: "d".repeat(64) },
    { initial_sent_at: "2026-10-12T13:00:00Z" }, { video_job_id: "other" },
    { model_id: "Systran/faster-whisper-base.en" }, { model_revision: "other" },
    { model_manifest_sha256: "f".repeat(64) }, { config_sha256: "0".repeat(64) },
    { confidence: [{ avg_logprob: -1.01, no_speech_prob: 0.01 }] },
    { confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.61 }] },
    { media_sha256: "missing" }, { greeting_seconds: 3 },
  ]) {
    const h = harness();
    const body = plannedVideo();
    Object.assign(body.video_speech_proof, changes);
    assert.equal(enqueue(h, body).ok, false, JSON.stringify(changes));
    assert.equal(h.outbox.data.length, 2);
  }
  for (const message of [
    "Hey Jim: https://go.crispshortsales.com/v/another_job",
    "Hey Jim: https://other.example/v/job_123",
    "Hey Jim: https://go.crispshortsales.com/not-video/job_123",
  ]) {
    const h = harness();
    const body = plannedVideo();
    body.message = message;
    assert.equal(enqueue(h, body).ok, false, message);
    assert.equal(h.outbox.data.length, 2);
  }
});

test("new spelling policy never widens legacy proofs or accepts unapproved near-names", () => {
  const invalid = [
    ["Kathryn", "kathleen"], ["Kathryn", "catherine"], ["Kathryn", "karen"],
    ["Kathryn", "nikki"], ["Nikki", "mickey"], ["Nikki", "nick"], ["Nikki", "nicole"],
    ["Jacqueline", "jack lim"], ["Sibelys", "cebellis"], ["Marc", "mark"],
    ["Jerrick", "jim"], ["Nikki", "nicky hey jim"], ["Nikki", "nicky!"],
    ["Nikki", "Nicky"], ["Nikki", " nicky"],
  ];
  for (const [first_name, observed] of invalid) {
    const h = harness();
    h.row[0] = first_name;
    assert.equal(enqueue(h, spokenVideo({ first_name }, observed)).ok, false, `${first_name}: ${observed}`);
    assert.equal(h.outbox.data.length, 2);
  }
  for (const [first_name, observed] of [["Kathryn", "katherine"], ["Katherine", "kathryn"],
    ["Nikki", "nicky"], ["Nicky", "nikki"]]) {
    const h = harness();
    h.row[0] = first_name;
    const body = video({ first_name });
    body.video_speech_proof.normalized_transcript = `hey ${observed}`;
    assert.equal(enqueue(h, body).ok, false, "Legacy policy must remain exact-only");
  }
});

test("speech policies require complete matching model and config tuples", () => {
  for (const makeVideo of [video, spokenVideo]) {
    const original = makeVideo().video_speech_proof;
    for (const changes of [
      { policy_version: "unsupported" }, { policy_version: "spelling-equivalent-greeting-tiny-en-v2" },
      { policy_version: original.policy_version === speechPolicy.policy_version ? spokenSpeechPolicy.policy_version : speechPolicy.policy_version },
      { config_sha256: original.config_sha256 === speechPolicy.config_sha256 ? spokenSpeechPolicy.config_sha256 : speechPolicy.config_sha256 },
      { model_id: "Systran/faster-whisper-base.en" }, { model_revision: "other" },
      { model_manifest_sha256: "f".repeat(64) },
    ]) {
      const h = harness();
      const body = makeVideo();
      Object.assign(body.video_speech_proof, changes);
      assert.equal(enqueue(h, body).ok, false, JSON.stringify(changes));
      assert.equal(h.outbox.data.length, 2);
    }
  }
});

test("new proof policy preserves recipient, media, confidence and frozen-payload guards", () => {
  for (const changes of [
    { phone: "9542053206" }, { expected_name: "Nicky" }, { stable_id: "other" },
    { followup_key: "d".repeat(64) }, { video_job_id: "other" },
    { initial_sent_at: "2026-10-12T13:00:00Z" }, { source_audio_sha256: "bad" },
    { media_sha256: "bad" }, { greeting_seconds: 3 },
    { confidence: [{ avg_logprob: -1.01, no_speech_prob: 0.01 }] },
    { confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.61 }] },
  ]) {
    const h = harness();
    h.row[0] = "Nikki";
    const body = spokenVideo({ first_name: "Nikki" }, "nicky");
    Object.assign(body.video_speech_proof, changes);
    assert.equal(enqueue(h, body).ok, false, JSON.stringify(changes));
    assert.equal(h.outbox.data.length, 2);
  }
  for (const [first, second] of [[video, spokenVideo], [spokenVideo, video]]) {
    const h = harness();
    assert.equal(enqueue(h, first()).ok, true);
    const frozen = h.outbox.data[2].slice();
    assert.equal(enqueue(h, second()).ok, true);
    assert.deepEqual(h.outbox.data[2], frozen);
    assert.equal(h.outbox.data.length, 3);
    const claim = h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(claim.should_send, true);
  }
});

test("actual video enqueue rejects missing, stale or incorrectly bound speech proofs", () => {
  const badProofs = [undefined, null, {}, [],
    ...Object.keys(video().video_speech_proof).map((field) => ({ ...video().video_speech_proof, [field]: undefined })),
    ...Object.entries({ accepted: "true", policy_version: "old", model_id: "other", model_revision: "other",
      model_manifest_sha256: "a".repeat(64), config_sha256: "a".repeat(64), video_job_id: "other_job",
      followup_key: "d".repeat(64), phone: "9542053206", initial_sent_at: "2026-10-12T13:00:00Z",
      stable_id: "other", expected_name: "Joseph", normalized_transcript: "hey jimmy",
      source_audio_sha256: "not-a-hash", media_sha256: "C".repeat(64), greeting_seconds: 3,
      confidence: [{ avg_logprob: -1.1, no_speech_prob: 0.01 }],
    }).map(([field, value]) => ({ ...video().video_speech_proof, [field]: value })),
  ];
  for (const proof of badProofs) {
    const h = harness();
    const body = video({ video_speech_proof: proof });
    assert.equal(h.context.checkVideoFollowupEligibilityV19_(body).eligible, true);
    const result = enqueue(h, body);
    assert.equal(result.ok, false, JSON.stringify(proof));
    assert.equal(result.retryable, true);
    assert.equal(h.outbox.data.length, 2);
  }
});

test("speech proof binds the branded URL and exact job in the actual SMS body", () => {
  for (const message of ["No video link", "https://other.test/v/job_123",
    "https://go.crispshortsales.com/v/other_job", "https://go.crispshortsales.com/v/job_123?other=1",
    "https://go.crispshortsales.com/v/job_123 https://other.test", "https://go.crispshortsales.com/v/job_123 HTTPS://other.test",
    "https://go.crispshortsales.com/v/job_123-extra"]) {
    const h = harness();
    assert.equal(enqueue(h, video({ message })).ok, false, message);
    assert.equal(h.outbox.data.length, 2);
  }
});

test("speech proof rejects names outside the recognizer's supported first-name policy", () => {
  for (const first_name of ["A", "agent", "Unknown", "There", "test", "Jim Smith", "Jim2", "J".repeat(33)]) {
    const h = harness();
    h.row[0] = first_name;
    assert.equal(enqueue(h, video({ first_name })).ok, false, first_name);
    assert.equal(h.outbox.data.length, 2);
  }
});

test("speech proof rejects nonnumeric duration and invalid confidence evidence", () => {
  for (const changes of [{ greeting_seconds: "1.5" }, { greeting_seconds: true }, { greeting_seconds: NaN },
    { greeting_seconds: 0.49 }, { greeting_seconds: 2.51 }, { confidence: [] }, { confidence: [{}] },
    { confidence: [{ avg_logprob: "-0.1", no_speech_prob: 0 }] },
    { confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.61 }] },
    { confidence: [{ avg_logprob: 0.1, no_speech_prob: 0 }] },
    { confidence: [{ avg_logprob: -0.1, no_speech_prob: NaN }] }]) {
    const h = harness();
    const proof = { ...video().video_speech_proof, ...changes };
    assert.equal(enqueue(h, video({ video_speech_proof: proof })).ok, false, JSON.stringify(changes));
  }
});

test("unverified frozen video work is held at claim, reclaim and send-start without replacement", () => {
  for (const phase of ["claim", "active_claim", "send_started"]) {
    const h = harness();
    enqueue(h, video());
    const claim = phase === "claim" ? null : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
    delete metadata.video_speech_proof;
    h.outbox.data[2][6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
    const before = h.outbox.data[2].slice();
    const result = phase === "send_started" ? h.context.markPendingSmsSendStartedV10_(claim)
      : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(result.should_send, false, phase);
    assert.equal(result.retryable, true, phase);
    assert.deepEqual(h.outbox.data[2], before, phase);
    const duplicate = enqueue(h, base);
    assert.equal(duplicate.ok, false, phase);
    assert.equal(duplicate.retryable, true, phase);
    assert.equal(duplicate.payload_frozen, true, phase);
    assert.deepEqual(h.outbox.data[2], before, phase);
    assert.equal(h.outbox.data.length, 3);
  }
});

test("proof revalidation uses the stored payload and preserves started, uncertain and historical sent rows", () => {
  for (const status of ["send_started", "receipt_pending", "uncertain", "sent"]) {
    const h = harness();
    enqueue(h, video());
    const claim = h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    h.outbox.data[2][1] = status;
    const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
    delete metadata.video_speech_proof;
    h.outbox.data[2][6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
    const before = h.outbox.data[2].slice();
    if (status === "send_started") {
      const result = h.context.markPendingSmsSendStartedV10_(claim);
      assert.equal(result.should_send, false);
      assert.equal(result.retryable, true);
    }
    const result = enqueue(h, base);
    assert.equal(result.ok, status === "sent", status);
    assert.equal(result.payload_frozen, true);
    assert.deepEqual(h.outbox.data[2], before);
    assert.equal(h.outbox.data.length, 3);
  }
  const h = harness();
  enqueue(h, video());
  h.outbox.data[2][5] = "https://go.crispshortsales.com/v/different_job";
  assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" }).should_send, false);
  assert.equal(h.outbox.data[2][1], "queued");
});

test("text fallbacks do not require or inherit video speech proof", () => {
  const h = harness();
  assert.equal(enqueue(h, { ...base, video_speech_proof: { accepted: false } }).ok, true);
  const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
  assert.equal(metadata.video_speech_proof, undefined);
  assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" }).should_send, true);
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
    assert.equal(h.outbox.data.length, 2);
  }
});

test("stable row shifts resolve before enqueue, claim, and standard I/X receipt", () => {
  const h = harness();
  h.leads.data.splice(1, 0, Array(33).fill(""));
  assert.equal(enqueue(h, video()).ok, true);
  assert.equal(h.outbox.data[2][17], 3);
  h.leads.data.splice(1, 0, Array(33).fill(""));
  assert.equal(h.context.getPendingSmsStaleReason_(h.outbox.data[2]), "");
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
  assert.match(h.context.getPendingSmsStaleReason_(h.outbox.data[2]), /initial-send timestamp/);
  assert.equal(receipt(h).crm_write_skipped, true);
  assert.equal(h.row[8], "");
  assert.equal(h.row[23], "");
});

test("a receipt with incomplete feature identity never writes standard follow-up markers", () => {
  const h = harness();
  enqueue(h);
  const metadata = JSON.parse(h.outbox.data[2][6].replace("__scheduled_followup__:", ""));
  delete metadata.initial_sent_at;
  h.outbox.data[2][6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
  assert.equal(receipt(h).crm_write_skipped, true);
  assert.equal(h.row[8], "");
  assert.equal(h.row[23], "");
});

test("date-only W uses latest exact-phone eligible initial receipt, not the first or wrong identity", () => {
  const h = harness({ initialProof: false });
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
  const h = harness({ initialProof: false });
  initialReceipt(h, "2026-10-13T15:00:00Z");
  assert.equal(enqueue(h).ok, false);
});

test("handset W and a later exact-attempt receipt allow video and text without changing W", () => {
  // Observed receipt-processing delays for the first 18 organic jobs.
  for (const delayMs of [8247, 5516, 3650, 4894, 7542, 6280, 3209, 5517, 4691, 6268, 5321, 4226, 4743, 5601, 6609, 6292, 4938, 4729]) {
    for (const body of [base, video()]) {
      const h = harness({ initialProof: false });
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
  const h = harness({ initialProof: false });
  delayedInitialReceipt(h, { 2: "render-initial-2-0123456789abcdef" });
  h.leads.data.splice(1, 0, Array(33).fill(""));
  assert.equal(enqueue(h, video()).ok, true);
  assert.equal(h.outbox.data[2][17], 3);
  assert.equal(h.context.getPendingSmsStaleReason_(h.outbox.data[2]), "");
});

test("receipt processing delay is not a widened time tolerance", () => {
  for (const startedAt of ["2026-10-13T13:00:00.001Z", "2026-10-13T13:00:05Z"]) {
    const h = harness({ initialProof: false });
    delayedInitialReceipt(h, { 13: new Date(startedAt) });
    assert.equal(enqueue(h, video()).ok, false);
  }
  const h = harness({ initialProof: false });
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
    const h = harness({ initialProof: false });
    delayedInitialReceipt(h, changes);
    assert.equal(enqueue(h, video()).ok, false, JSON.stringify(changes));
    assert.equal(h.outbox.data.length, 2);
  }
});

test("a matching first interval never hides a separate later or overlapping initial attempt", () => {
  for (const startedAt of ["2026-10-13T12:59:59Z", "2026-10-13T13:00:02Z"]) {
    const h = harness({ initialProof: false });
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
    const h = harness({ initialProof: false });
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
    const h = harness({ initialProof: false });
    delayedInitialReceipt(h);
    suppress(h);
    assert.equal(enqueue(h, video()).ok, false);
  }
  const h = harness({ initialProof: false });
  delayedInitialReceipt(h);
  enqueue(h, video());
  assert.equal(enqueue(h).duplicate, true);
  assert.equal(h.outbox.data.length, 3);
});

test("date-only recovery cannot hide a newer initial receipt on a different day", () => {
  const h = harness({ initialProof: false });
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
  assert.equal(h.outbox.data.length, 2);
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
    assert.equal(h.outbox.data[2][1], "superseded");
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

const existingConversationFixtures = [
  ["already has help", { 10: "H" }],
  ["lowercase already has help", { 10: " h " }],
  ["recorded agent reply", { 32: JSON.stringify([{ role: "agent", text: "Call me at 3", ts: "2026-10-13T14:00:00Z" }]) }],
  ["recorded user reply", { 32: JSON.stringify([{ role: "user", text: "How much do you charge?" }]) }],
  ["recorded content reply", { 32: JSON.stringify([{ role: "agent", content: "Yes, please call" }]) }],
  ["active conversation state", { 13: "active" }],
  ["handoff state", { 13: "handoff" }],
  ["completed state", { 13: "done" }],
  ["manual conversation summary", { 12: "Spoke with agent; callback requested" }],
  ["callback summary", { 12: "Agent requested a callback tomorrow" }],
];

test("H and independently recorded conversations block both personalized video and text fallback", () => {
  for (const [label, changes] of existingConversationFixtures) {
    for (const body of [base, video()]) {
      const h = harness();
      Object.assign(h.row, changes);
      const before = h.counters();
      assert.equal(h.context.checkVideoFollowupEligibilityV19_(body).eligible, false, `${label}: ${body.video_variant}`);
      assert.deepEqual(h.counters(), before, label);
      assert.equal(enqueue(h, body).ok, false, `${label}: ${body.video_variant}`);
      assert.equal(h.outbox.data.length, 2, label);
    }
  }
});

test("new H or conversation evidence cancels a reminder at claim, active-lease replay and send-start", () => {
  for (const [label, changes] of existingConversationFixtures) {
    for (const phase of ["claim", "active_claim", "send_started"]) {
      for (const body of [base, video()]) {
        const h = harness();
        assert.equal(enqueue(h, body).ok, true, label);
        const lease = phase === "claim" ? null : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
        Object.assign(h.row, changes);
        const result = phase === "send_started"
          ? h.context.markPendingSmsSendStartedV10_(lease)
          : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
        assert.equal(result.should_send, false, `${label}: ${phase}: ${body.video_variant}`);
        assert.equal(h.outbox.data[2][1], "superseded", label);
        assert.equal(h.outbox.data.length, 3, label);
      }
    }
  }
});

test("ordinary scheduled text uses the conversation guard without changing owner-approved follow-ups", () => {
  for (const [label, changes] of existingConversationFixtures) {
    for (const phase of ["claim", "send_started"]) {
      const h = harness();
      assert.equal(enqueue(h, legacy).ok, true, label);
      const lease = phase === "claim" ? null : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
      Object.assign(h.row, changes);
      const result = phase === "send_started"
        ? h.context.markPendingSmsSendStartedV10_(lease)
        : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
      assert.equal(result.should_send, false, `${label}: ${phase}`);
      assert.equal(h.outbox.data[2][1], "superseded", label);
    }
    const owner = harness();
    Object.assign(owner.row, changes);
    assert.equal(enqueue(owner, { ...legacy, request_id: "render-followup-2-0123456789abcdef" }).ok, true, label);
    assert.equal(owner.context.claimPendingSmsSendV10_({ worker_id: "owner-test" }).should_send, true, label);
  }
});

test("outbound-only history and operational summaries do not imply an agent conversation", () => {
  for (const [label, changes] of [
    ["assistant-only history", { 32: JSON.stringify([{ role: "assistant", text: "Original initial outreach text" }]) }],
    ["bot-only history", { 32: JSON.stringify([{ role: "bot", text: "Original initial outreach text" }]) }],
    ["system-only history", { 32: JSON.stringify([{ role: "system", text: "Lead verifier: initial SMS receipt confirmed" }]) }],
    ["empty history", { 32: "[]" }],
    ["empty inbound history entry", { 32: JSON.stringify([{ role: "agent", text: "  " }]) }],
    ["malformed history", { 32: "[not valid json" }],
    ["non-array history", { 32: JSON.stringify({ role: "agent", text: "Not a valid history array" }) }],
    ["verifier summary", { 12: "Lead verifier: initial SMS receipt confirmed" }],
    ["operational summary", { 12: "Initial SMS queued; awaiting handset receipt" }],
    ["non-response summary", { 12: "No answer on first call" }],
    ["verifier discussion summary", { 12: "Lead verifier checked whether agent requested a callback" }],
    ["new state", { 13: "new" }],
    ["initial state", { 13: "initial" }],
    ["unknown state", { 13: "unrecognized_state" }],
  ]) {
    for (const body of [base, video()]) {
      const h = harness();
      Object.assign(h.row, changes);
      assert.equal(h.context.checkVideoFollowupEligibilityV19_(body).eligible, true, `${label}: ${body.video_variant}`);
      assert.equal(enqueue(h, body).ok, true, `${label}: ${body.video_variant}`);
      assert.equal(h.context.getPendingSmsStaleReason_(h.outbox.data[2]), "", label);
    }
  }
});

test("malformed history does not bypass other genuine reply or takeover evidence", () => {
  for (const changes of [{ 29: "inbound-id" }, { 30: "Call me at 3" }, { 28: "TRUE" }, { 31: "TRUE" }]) {
    const h = harness();
    Object.assign(h.row, { 32: "[not valid json" }, changes);
    assert.equal(enqueue(h, base).ok, false);
    assert.equal(h.outbox.data.length, 2);
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
    assert.equal(h.outbox.data.length, 3);
  }
});

test("feature off/on toggles return the already accepted feature attempt", () => {
  for (const status of ["queued", "claimed", "send_started", "sent", "receipt_pending", "uncertain", "failed", "superseded"]) {
    const h = harness();
    enqueue(h, video());
    h.outbox.data[2][1] = status;
    const disabled = enqueue(h, legacy);
    assert.equal(disabled.duplicate, true, status);
    assert.equal(disabled.payload_frozen, true);
    assert.equal(disabled.message_id, base.message_id);
    assert.equal(disabled.ok, ["queued", "claimed", "send_started", "sent"].includes(status));
    enqueue(h, base);
    assert.equal(h.outbox.data.length, 3);
    assert.equal(h.outbox.data[2][5], video().message);
  }
});

test("feature activation freezes an already queued legacy payload for the same initial", () => {
  for (const status of ["queued", "claimed", "send_started", "sent", "uncertain", "failed"]) {
    const h = harness();
    enqueue(h, legacy);
    h.outbox.data[2][1] = status;
    h.outbox.data[2][0] = new Date("2026-10-13T19:00:00Z");
    const enabled = enqueue(h, video());
    assert.equal(enabled.duplicate, true, status);
    assert.equal(enabled.payload_frozen, true);
    assert.equal(enabled.message_id, legacy.message_id);
    assert.equal(enabled.video_variant, "text");
    assert.equal(h.outbox.data.length, 3);
    assert.equal(h.outbox.data[2][5], legacy.message);
  }
});

test("cross-feature dedupe does not swallow a new initial episode or manual CRM exception", () => {
  const h = harness();
  enqueue(h, video());
  h.outbox.data[2][1] = "sent";
  h.row[22] = new Date("2026-10-14T13:00:00Z");
  assert.equal(enqueue(h, legacy).duplicate, undefined);
  assert.equal(h.outbox.data.length, 4);
  const other = harness();
  enqueue(other, video());
  assert.equal(enqueue(other, { ...legacy, request_id: "render-followup-2-0123456789abcdef" }).duplicate, undefined);
  assert.equal(other.outbox.data.length, 4);
});

test("rollback cannot assume a new initial when existing feature identity is unreadable", () => {
  for (const mutation of [
    (h) => { h.row[22] = "invalid"; },
    (h) => { h.row[22] = ""; },
    (h) => { h.leads.data.push(h.row.slice()); },
  ]) {
    const h = harness();
    enqueue(h, video());
    h.outbox.data[2][1] = "sent";
    mutation(h);
    assert.equal(enqueue(h, legacy).ok, false);
    assert.equal(h.outbox.data.length, 3);
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
  assert.equal(h.outbox.data[2][1], "queued");
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
    const originalStatus = h.outbox.data[2][1];
    delete h.sheets.sms_inbound_queue;
    const result = phase === "send_started"
      ? h.context.markPendingSmsSendStartedV10_(claimed)
      : h.context.claimPendingSmsSendV10_({ worker_id: "owner-test" });
    assert.equal(result.should_send, false, phase);
    assert.equal(result.retryable, true, phase);
    assert.equal(h.outbox.data[2][1], originalStatus, phase);
  }
});

test("takeover service and send-start ledger errors hold feature work without cancellation", () => {
  const h = harness();
  enqueue(h);
  h.context.hasPendingSmsTakeoverV11_ = () => { throw new Error("Temporary property service failure"); };
  assert.equal(h.context.claimPendingSmsSendV10_({}).retryable, true);
  assert.equal(h.outbox.data[2][1], "queued");
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
  const row = h.outbox.data[2];
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

function futureEvidenceBody(h, asVideo = false) {
  const body = asVideo ? video() : { ...base };
  const result = h.context.checkVideoFollowupEligibilityV19_(body);
  assert.equal(result.eligible, true);
  assert.ok(result.initial_receipt);
  body.initial_receipt = JSON.parse(JSON.stringify(result.initial_receipt));
  if (asVideo) Object.assign(body.video_speech_proof, {
    evidence_version: "same-take-evidence-v1", evidence_contract: "same-take-evidence-required-v1",
    evidence_sha256: "e".repeat(64),
  });
  return body;
}

function futureHarness() {
  const h = harness({ initialProof: false });
  delayedInitialReceipt(h);
  return h;
}

function journal(h) {
  return JSON.parse(h.outbox.data.at(-1)[6].replace(/^__scheduled_followup__:/, "")).decision_evidence;
}

function schedulerReceiptFixture(asVideo = false) {
  const h = futureHarness();
  const phone = "2025550142";
  h.row[2] = phone;
  for (const row of h.outbox.data.slice(1)) row[4] = phone;
  const body = asVideo ? directedVideo("Jim", "jim") : { ...base, first_name: "Jim", video_job_id: "job_123" };
  body.phone = phone;
  if (asVideo) Object.assign(body.video_speech_proof, {
    phone, evidence_version: "same-take-evidence-v1",
    evidence_contract: "same-take-evidence-required-v1", evidence_sha256: "e".repeat(64),
  });
  const resolved = h.context.checkVideoFollowupEligibilityV19_(body);
  assert.equal(resolved.eligible, true);
  body.initial_receipt = JSON.parse(JSON.stringify(resolved.initial_receipt));
  return { h, body };
}

for (const asVideo of [false, true]) {
  test(`Python serializer receipt boundary journals ${asVideo ? "evidence video" : "text fallback"} claim and send-start`, () => {
    const { h, body } = schedulerReceiptFixture(asVideo);
    const before = JSON.stringify(body);
    const payload = schedulerSerialized(body);
    assert.deepEqual(payload.initial_receipt, body.initial_receipt);
    for (const field of ["message_id", "request_id", "followup_key", "video_job_id", "video_variant", "phone"]) {
      assert.equal(payload[field], body[field], field);
    }
    assert.deepEqual(payload.video_speech_proof, body.video_speech_proof);
    assert.equal(JSON.stringify(body), before);
    assert.equal(enqueue(h, payload).ok, true);
    const metadata = JSON.parse(h.outbox.data.at(-1)[6].replace(/^__scheduled_followup__:/, ""));
    assert.equal(metadata.audit_contract, "followup-decision-v1");
    assert.deepEqual(metadata.initial_receipt, body.initial_receipt);
    const claim = h.context.claimPendingSmsSendV10_({ worker_id: "offline-fixture" });
    assert.equal(claim.should_send, true);
    assert.equal(h.context.markPendingSmsSendStartedV10_(claim).ok, true);
    const events = journal(h).events;
    assert.deepEqual(events.map(event => event.stage), ["claim", "send_start"]);
    let prior = "";
    for (const event of events) {
      const { sha256, ...unhashed } = event;
      assert.equal(sha256, canonicalDigest(unhashed));
      assert.equal(event.previous_sha256, prior);
      assert.equal(event.event_id, canonicalDigest([payload.request_id, payload.message_id, event.stage, claim.lease_token]));
      assert.equal(event.decision.eligible, true);
      assert.deepEqual(event.decision.initial_receipt, body.initial_receipt);
      assert.equal(event.speech_evidence_sha256, asVideo ? "e".repeat(64) : "");
      prior = sha256;
    }
    const saved = JSON.stringify(h.outbox.data);
    assert.equal(h.context.markPendingSmsSendStartedV10_(claim).ok, true);
    assert.equal(JSON.stringify(h.outbox.data), saved);
  });
}

test("approved video copy survives Python serialization and existing Apps whitespace normalization with both journals", () => {
  const { h, body } = schedulerReceiptFixture(true);
  const jobId = "3".repeat(32);
  const videoUrl = `https://go.crispshortsales.com/v/${jobId}`;
  body.video_job_id = jobId;
  body.video_speech_proof.video_job_id = jobId;
  body.message = "Hey, just following up on my message from earlier.  "
    + "Heres a quick video to put a face to my text... Happy to help if you have any questions!\n"
    + videoUrl;
  const expected = "Hey, just following up on my message from earlier. "
    + "Heres a quick video to put a face to my text... Happy to help if you have any questions! "
    + videoUrl;
  const payload = schedulerSerialized(body);
  assert.equal(payload.message, body.message);
  assert.equal(payload.reply_text, body.message);
  assert.notEqual(payload.phone, "9542053205");
  assert.deepEqual(payload.initial_receipt, body.initial_receipt);
  assert.deepEqual(payload.video_speech_proof, body.video_speech_proof);
  for (const field of ["message_id", "request_id", "followup_key", "video_job_id", "video_variant", "phone"]) {
    assert.equal(payload[field], body[field], field);
  }
  assert.equal(enqueue(h, payload).ok, true);
  const stored = h.outbox.data.at(-1);
  assert.equal(stored[5], expected);
  assert.equal(stored[2], body.request_id);
  assert.equal(stored[3], body.message_id);
  assert.deepEqual(stored[5].match(/https?:\/\/[^\s<>"']+/g), [videoUrl]);
  const metadata = JSON.parse(stored[6].replace(/^__scheduled_followup__:/, ""));
  assert.equal(metadata.audit_contract, "followup-decision-v1");
  assert.equal(metadata.video_job_id, jobId);
  assert.deepEqual(metadata.initial_receipt, body.initial_receipt);
  const claim = h.context.claimPendingSmsSendV10_({ worker_id: "offline-fixture" });
  assert.equal(claim.should_send, true);
  assert.equal(claim.reply_text, expected);
  assert.equal(claim.request_id, body.request_id);
  assert.equal(claim.message_id, body.message_id);
  assert.equal(h.context.markPendingSmsSendStartedV10_(claim).ok, true);
  assert.equal(h.outbox.data.at(-1)[5], expected);
  const events = journal(h).events;
  assert.deepEqual(events.map(event => event.stage), ["claim", "send_start"]);
  let prior = "";
  for (const event of events) {
    const { sha256, ...unhashed } = event;
    assert.equal(sha256, canonicalDigest(unhashed));
    assert.equal(event.previous_sha256, prior);
    assert.equal(event.event_id, canonicalDigest([body.request_id, body.message_id, event.stage, claim.lease_token]));
    assert.equal(event.decision.eligible, true);
    assert.deepEqual(event.decision.initial_receipt, body.initial_receipt);
    assert.equal(event.speech_evidence_sha256, "e".repeat(64));
    prior = sha256;
  }
});

test("Python serializer receipt boundary preserves legacy absence but rejects stripped evidence video", () => {
  for (const asVideo of [false, true]) {
    const { h, body } = schedulerReceiptFixture(asVideo);
    delete body.initial_receipt;
    const payload = schedulerSerialized(body);
    assert.equal(Object.hasOwn(payload, "initial_receipt"), false);
    assert.equal(Object.hasOwn(payload, "audit_contract"), false);
    assert.equal(enqueue(h, payload).ok, !asVideo);
    if (!asVideo) {
      assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "offline-fixture" }).should_send, true);
      assert.equal(journal(h), undefined);
    } else {
      assert.equal(h.outbox.data.length, 2);
    }
  }
});

test("Python serializer receipt boundary retains invalid-present values for Apps fail-closed validation", () => {
  for (const asVideo of [false, true]) {
    for (const invalid of [null, {}, [], "", { schema: "unknown" }, { status: "sent" }]) {
      const { h, body } = schedulerReceiptFixture(asVideo);
      body.initial_receipt = invalid;
      const payload = schedulerSerialized(body);
      assert.equal(Object.hasOwn(payload, "initial_receipt"), true);
      assert.deepEqual(payload.initial_receipt, invalid);
      const before = JSON.stringify(h.outbox.data);
      assert.equal(enqueue(h, payload).ok, false);
      assert.equal(JSON.stringify(h.outbox.data), before);
    }
  }
});

test("Python serializer receipt boundary cannot change receipt identity or strict speech gates", () => {
  for (const mutate of [
    body => { body.initial_receipt.phone = "2025550143"; },
    body => { body.initial_receipt.stable_id = "different-listing"; },
    body => { body.initial_receipt.sent_at = "2026-10-13T13:00:09.000Z"; },
    body => { body.video_speech_proof.normalized_transcript = "hey cindy"; },
    body => { body.video_speech_proof.confidence[0].avg_logprob = -1.01; },
    body => { body.video_speech_proof.greeting_seconds = 2.51; },
    body => { delete body.video_speech_proof.evidence_contract; },
  ]) {
    const { h, body } = schedulerReceiptFixture(true);
    mutate(body);
    const payload = schedulerSerialized(body);
    assert.deepEqual(payload.initial_receipt, body.initial_receipt);
    assert.deepEqual(payload.video_speech_proof, body.video_speech_proof);
    const before = JSON.stringify(h.outbox.data);
    assert.equal(enqueue(h, payload).ok, false);
    assert.equal(JSON.stringify(h.outbox.data), before);
  }
});

test("Python serializer receipt boundary never replaces frozen text or reopens completed and uncertain rows", () => {
  const text = schedulerSerialized(schedulerReceiptFixture().body);
  const replacement = schedulerSerialized(schedulerReceiptFixture(true).body);
  for (const status of ["queued", "claimed", "send_started", "receipt_pending", "uncertain",
    "failed", "superseded", "manual_sent", "sent"]) {
    const { h } = schedulerReceiptFixture();
    assert.equal(enqueue(h, text).ok, true);
    h.outbox.data.at(-1)[1] = status;
    const before = JSON.stringify(h.outbox.data);
    const result = enqueue(h, replacement);
    assert.equal(result.payload_frozen, true, status);
    assert.equal(JSON.stringify(h.outbox.data), before, status);
    if (["receipt_pending", "uncertain", "failed", "superseded", "manual_sent", "sent"].includes(status)) {
      assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "offline-fixture" }).should_send, false, status);
      assert.equal(JSON.stringify(h.outbox.data), before, status);
    }
  }
});

test("Python serializer receipt boundary never retrofits an existing unmarked text row", () => {
  const legacyBody = schedulerReceiptFixture().body;
  delete legacyBody.initial_receipt;
  const legacyPayload = schedulerSerialized(legacyBody);
  assert.equal(Object.hasOwn(legacyPayload, "initial_receipt"), false);
  const repairedPayloads = [false, true].map(asVideo => schedulerSerialized(schedulerReceiptFixture(asVideo).body));
  for (const status of ["queued", "claimed", "send_started", "receipt_pending", "uncertain",
    "failed", "superseded", "manual_sent", "sent"]) {
    for (const repaired of repairedPayloads) {
      const { h } = schedulerReceiptFixture();
      assert.equal(enqueue(h, legacyPayload).ok, true);
      h.outbox.data.at(-1)[1] = status;
      const before = JSON.stringify(h.outbox.data);
      assert.equal(enqueue(h, repaired).payload_frozen, true, `${status}/${repaired.video_variant}`);
      assert.equal(JSON.stringify(h.outbox.data), before, `${status}/${repaired.video_variant}`);
      const metadata = JSON.parse(h.outbox.data.at(-1)[6].replace(/^__scheduled_followup__:/, ""));
      for (const field of ["initial_receipt", "audit_contract", "decision_evidence"]) {
        assert.equal(Object.hasOwn(metadata, field), false, `${status}/${field}`);
      }
    }
  }
});

test("Python serializer receipt boundary records reply and takeover blocks at claim and send-start", () => {
  for (const asVideo of [false, true]) {
    const payload = schedulerSerialized(schedulerReceiptFixture(asVideo).body);
    for (const phase of ["claim", "send_start"]) {
      for (const control of ["queued_reply", "recorded_reply", "takeover"]) {
        const { h } = schedulerReceiptFixture(asVideo);
        assert.equal(enqueue(h, payload).ok, true);
        const claim = phase === "send_start"
          ? h.context.claimPendingSmsSendV10_({ worker_id: "offline-fixture" }) : null;
        if (claim) assert.equal(claim.should_send, true);
        if (control === "queued_reply") {
          h.inbound.data.push(["2026-10-13T14:00:00Z", "queued", "offline-q", "offline-dedupe",
            "offline-message", payload.phone, "Please stop", "2026-10-13T14:00:00Z"]);
        } else if (control === "recorded_reply") {
          h.row[32] = JSON.stringify([{ role: "user", text: "Please stop" }]);
        } else {
          h.setTakeover(true);
        }
        const result = claim ? h.context.markPendingSmsSendStartedV10_(claim)
          : h.context.claimPendingSmsSendV10_({ worker_id: "offline-fixture" });
        assert.equal(claim ? result.ok : result.should_send, false, `${asVideo}/${phase}/${control}`);
        const events = journal(h).events;
        assert.equal(events.at(-1).stage, `${phase}_blocked`);
        assert.equal(events.at(-1).decision.eligible, false);
        assert.equal(events.some(event => event.stage === "send_start"), false);
      }
    }
  }
});

test("pure eligibility exposes the sole exact immutable initial receipt without writes", () => {
  const h = futureHarness();
  const before = h.counters();
  const result = h.context.checkVideoFollowupEligibilityV19_(base);
  assert.equal(result.eligible, true);
  assert.deepEqual(JSON.parse(JSON.stringify(result.initial_receipt)), {
    schema: "initial-sms-receipt-v1", request_id: "scheduler-initial-2-0123456789abcdef",
    message_id: "initial-2-0123456789abcdef", phone: base.phone, stable_id: base.stable_id,
    created_at: "2026-10-13T12:59:00.000Z", send_started_at: "2026-10-13T12:59:58.000Z",
    sent_at: "2026-10-13T13:00:08.247Z", status: "sent",
  });
  assert.deepEqual(h.counters(), before);
  h.leads.data.splice(1, 0, Array(33).fill(""));
  const shifted = h.context.checkVideoFollowupEligibilityV19_(base);
  assert.deepEqual(shifted.initial_receipt, result.initial_receipt);
  assert.equal(shifted.crm_row, 3);
});

test("legacy eligibility remains unchanged when strict immutable receipt evidence is absent", () => {
  const h = harness();
  const before = h.counters();
  const result = h.context.checkVideoFollowupEligibilityV19_(base);
  assert.equal(result.eligible, true);
  assert.equal(result.initial_receipt, null);
  assert.deepEqual(h.counters(), before);
  assert.equal(enqueue(h).ok, true);
  assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, true);
  assert.equal(journal(h), undefined);
});

test("present but invalid receipt descriptors never fall back to legacy handling", () => {
  for (const mutate of [
    body => { body.initial_receipt = null; },
    body => { body.initial_receipt.schema = "other"; },
    body => { body.initial_receipt.phone = "9542053206"; },
    body => { body.initial_receipt.stable_id = "other"; },
    body => { body.initial_receipt.request_id = "wrong"; },
    body => { body.initial_receipt.message_id = "initial-1-0123456789abcdef"; },
    body => { body.initial_receipt.send_started_at = "2026-10-13T13:00:01.000Z"; },
    body => { body.initial_receipt.created_at = "2026-10-13T13:00:00.000Z"; },
    body => { body.initial_receipt.status = "send_started"; },
    body => { body.audit_contract = "unrecognized"; },
  ]) {
    const h = futureHarness();
    const body = futureEvidenceBody(h);
    mutate(body);
    const before = h.counters();
    assert.equal(h.context.checkVideoFollowupEligibilityV19_(body).eligible, false);
    assert.equal(enqueue(h, body).ok, false);
    assert.deepEqual(h.counters(), before);
  }
  const h = futureHarness();
  assert.equal(enqueue(h, { ...legacy, initial_receipt: {} }).ok, false);
});

test("a supplied receipt is revalidated against current exact outbox identity", () => {
  for (const mutation of [
    row => { row[2] = "render-initial-2-0123456789abcdef"; },
    row => { row[14] = new Date("2026-10-13T13:00:09.000Z"); },
    row => { row[13] = new Date("2026-10-13T12:59:59.000Z"); },
  ]) {
    const h = futureHarness();
    const body = futureEvidenceBody(h);
    mutation(h.outbox.data[1]);
    const result = h.context.checkVideoFollowupEligibilityV19_(body);
    assert.equal(result.eligible, false);
    assert.match(result.reason, /pinned initial receipt/);
    assert.equal(enqueue(h, body).ok, false);
  }
});

test("new video metadata requires the descriptor marker but legacy speech proof is unchanged", () => {
  for (const field of ["evidence_version", "evidence_contract", "evidence_sha256"]) {
    const h = futureHarness();
    const body = futureEvidenceBody(h, true);
    delete body.video_speech_proof[field];
    assert.equal(enqueue(h, body).ok, false, field);
    delete body.initial_receipt;
    for (const marker of ["evidence_version", "evidence_contract", "evidence_sha256"]) delete body.video_speech_proof[marker];
    assert.equal(enqueue(h, body).ok, true, `legacy ${field}`);
  }
});

test("future claim and send-start atomically retain exact decision snapshots and callbacks are idempotent", () => {
  const h = futureHarness();
  const body = futureEvidenceBody(h, true);
  assert.equal(enqueue(h, body).ok, true);
  const claim = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  assert.equal(claim.should_send, true);
  const first = JSON.stringify(journal(h).events[0]);
  assert.equal(journal(h).events[0].stage, "claim");
  assert.equal(journal(h).events[0].speech_evidence_sha256, "e".repeat(64));
  assert.equal(journal(h).events[0].decision.initial_receipt.request_id, body.initial_receipt.request_id);
  assert.equal(Object.values(journal(h).events[0].decision.blocked_checks).some(Boolean), false);
  const writes = h.counters().writes;
  const replay = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  assert.equal(replay.lease_token, claim.lease_token);
  assert.equal(h.counters().writes, writes);
  assert.equal(JSON.stringify(journal(h).events[0]), first);
  const started = h.context.markPendingSmsSendStartedV10_(claim);
  assert.equal(started.ok, true);
  assert.equal(h.outbox.data.at(-1)[1], "send_started");
  assert.equal(journal(h).events.length, 2);
  assert.equal(journal(h).events[1].stage, "send_start");
  assert.equal(journal(h).events[1].previous_sha256, journal(h).events[0].sha256);
  assert.equal(JSON.stringify(journal(h).events[0]), first);
  const afterStart = h.counters().writes;
  const second = h.context.markPendingSmsSendStartedV10_(claim);
  assert.equal(second.duplicate, true);
  assert.equal(h.counters().writes, afterStart);
  assert.equal(journal(h).events.length, 2);
});

test("reply after claim is recorded at send-start denial without altering original claim evidence", () => {
  const h = futureHarness();
  enqueue(h, futureEvidenceBody(h));
  const claim = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  const before = JSON.stringify(journal(h).events[0]);
  inboundReply(h);
  const result = h.context.markPendingSmsSendStartedV10_(claim);
  assert.equal(result.should_send, false);
  assert.equal(result.status, "superseded");
  assert.equal(journal(h).events[1].stage, "send_start_blocked");
  assert.equal(journal(h).events[1].decision.blocked_checks.queued_inbound, true);
  assert.equal(journal(h).events[1].decision.inbound.samples[0].queue_id, "q1");
  assert.equal(JSON.stringify(journal(h).events[0]), before);
});

test("future claim captures each existing CRM/control suppression without changing its enforcement", () => {
  for (const [field, mutate] of [
    ["pending_takeover", h => h.setTakeover(true)],
    ["human_override", h => { h.row[28] = "TRUE"; }],
    ["handoff", h => { h.row[31] = "TRUE"; }],
    ["crm_status", h => { h.row[10] = "H"; }],
    ["recorded_conversation", h => { h.row[32] = '[{"role":"user","text":"Call me"}]'; }],
    ["manual_note", h => { h.row[9] = "Discussed transaction"; }],
    ["crm_inbound", h => { h.row[29] = "received-message"; }],
    ["queued_inbound", h => inboundReply(h)],
  ]) {
    const h = futureHarness();
    enqueue(h, futureEvidenceBody(h));
    mutate(h);
    const result = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
    assert.equal(result.should_send, false, field);
    assert.equal(h.outbox.data.at(-1)[1], "superseded", field);
    assert.equal(journal(h).events[0].stage, "claim_blocked");
    assert.equal(journal(h).events[0].decision.blocked_checks[field], true, field);
  }
});

test("failed evidence persistence cannot authorize a claim or advance attempt ownership", () => {
  const h = futureHarness();
  enqueue(h, futureEvidenceBody(h));
  const prior = JSON.stringify(h.outbox.data.at(-1));
  const getRange = h.outbox.getRange;
  h.outbox.getRange = (...args) => ({ ...getRange(...args), setValues: () => { throw new Error("offline write failure"); } });
  const result = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  assert.equal(result.should_send, false);
  assert.equal(result.retryable, true);
  assert.equal(JSON.stringify(h.outbox.data.at(-1)), prior);
});

test("evidence readback failure retains a single claimed attempt recoverable by same worker", () => {
  const h = futureHarness();
  enqueue(h, futureEvidenceBody(h));
  const getRange = h.outbox.getRange;
  let failReadback = false;
  h.outbox.getRange = (...args) => {
    const range = getRange(...args);
    return { ...range,
      setValues: values => { range.setValues(values); failReadback = true; },
      getValues: () => { if (failReadback) throw new Error("offline readback failure"); return range.getValues(); },
    };
  };
  assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, false);
  assert.equal(h.outbox.data.at(-1)[1], "claimed");
  assert.equal(h.outbox.data.at(-1)[9], 1);
  h.outbox.getRange = getRange;
  assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, true);
  assert.equal(h.outbox.data.at(-1)[9], 1);
  assert.equal(journal(h).events.length, 1);
});

test("failed send-start evidence write leaves the same claim and no send authorization", () => {
  const h = futureHarness();
  enqueue(h, futureEvidenceBody(h));
  const claim = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  const prior = JSON.stringify(h.outbox.data.at(-1));
  const getRange = h.outbox.getRange;
  h.outbox.getRange = (...args) => ({ ...getRange(...args), setValues: () => { throw new Error("offline write failure"); } });
  const result = h.context.markPendingSmsSendStartedV10_(claim);
  assert.equal(result.should_send, false);
  assert.equal(result.retryable, true);
  assert.equal(JSON.stringify(h.outbox.data.at(-1)), prior);
});

test("missing or tampered original journal is never backfilled at send-start", () => {
  for (const tamper of [
    metadata => { delete metadata.decision_evidence; },
    metadata => { metadata.decision_evidence.events[0].decision.eligible = false; },
  ]) {
    const h = futureHarness();
    enqueue(h, futureEvidenceBody(h));
    const claim = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
    const row = h.outbox.data.at(-1);
    const metadata = JSON.parse(row[6].replace(/^__scheduled_followup__:/, ""));
    tamper(metadata);
    row[6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
    const prior = JSON.stringify(row);
    const result = h.context.markPendingSmsSendStartedV10_(claim);
    assert.equal(result.should_send, false);
    assert.equal(JSON.stringify(row), prior);
  }
});

test("future journal overflow holds without truncation, eviction or an untracked send", () => {
  const h = futureHarness();
  enqueue(h, futureEvidenceBody(h));
  const row = h.outbox.data.at(-1);
  const metadata = JSON.parse(row[6].replace(/^__scheduled_followup__:/, ""));
  metadata.reserved_padding = "x".repeat(44000);
  row[6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
  const prior = JSON.stringify(row);
  const result = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  assert.equal(result.should_send, false);
  assert.equal(result.retryable, true);
  assert.equal(JSON.stringify(row), prior);
});

test("future journal event cap holds without eviction or changing the existing queued record", () => {
  const h = futureHarness();
  const body = futureEvidenceBody(h);
  enqueue(h, body);
  const metadata = JSON.parse(h.outbox.data.at(-1)[6].replace(/^__scheduled_followup__:/, ""));
  const result = h.context.checkVideoFollowupEligibilityV19_({ ...body, audit_contract: metadata.audit_contract }, { ignoreOwnKey: true, captureDecision: true });
  let row = h.outbox.data.at(-1);
  for (let i = 0; i < 16; i++) row = h.context.appendVideoDecisionEvidenceV26_(row, "claim", result, `prior-${i}`);
  h.outbox.data[h.outbox.data.length - 1] = row;
  const prior = JSON.stringify(row);
  assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, false);
  assert.equal(JSON.stringify(row), prior);
  assert.equal(journal(h).events.length, 16);
});

test("terminal and already-started future rows are never claimed, replayed or retrofitted", () => {
  for (const status of ["sent", "send_started", "receipt_pending", "uncertain", "failed", "superseded"]) {
    const h = futureHarness();
    enqueue(h, futureEvidenceBody(h));
    h.outbox.data.at(-1)[1] = status;
    const prior = JSON.stringify(h.outbox.data);
    assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, false);
    assert.equal(JSON.stringify(h.outbox.data), prior, status);
  }
});

test("future audit fields cannot bypass unchanged name, confidence, cadence or receipt uniqueness guards", () => {
  for (const mutate of [
    body => { body.video_speech_proof.normalized_transcript = "hey cindy"; },
    body => { body.video_speech_proof.confidence[0].avg_logprob = -1.01; },
    body => { body.video_speech_proof.greeting_seconds = 2.51; },
    body => { body.video_speech_proof.evidence_sha256 = "not-a-hash"; },
  ]) {
    const h = futureHarness();
    const body = futureEvidenceBody(h, true);
    mutate(body);
    assert.equal(enqueue(h, body).ok, false);
  }
  const h = futureHarness();
  const body = futureEvidenceBody(h);
  h.outbox.data.push(h.outbox.data[1].slice());
  assert.equal(h.context.checkVideoFollowupEligibilityV19_(body).eligible, false);
  assert.equal(enqueue(h, body).ok, false);
});

test("future chronology records unknown and created-time fallback honestly", () => {
  for (const [created, received, source] of [
    ["2026-10-13T14:00:00Z", "invalid", "created_at_fallback"],
    ["invalid", "invalid", "unknown"],
  ]) {
    const h = futureHarness();
    enqueue(h, futureEvidenceBody(h));
    h.inbound.data.push([created, "queued", "q-unknown", "dedupe", "m-unknown", base.phone, "hello", received]);
    assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, false);
    const sample = journal(h).events[0].decision.inbound.samples[0];
    assert.equal(sample.timestamp_source, source);
    assert.equal(sample.reported_received_at, null);
    assert.equal(sample.blocked, true);
  }
});

test("partial future marker stripping cannot silently enter the legacy video path", () => {
  for (const strip of [
    body => { delete body.initial_receipt; },
    body => { body.initial_receipt = null; },
    body => { delete body.video_speech_proof.evidence_version; },
    body => { delete body.video_speech_proof.evidence_contract; },
    body => { delete body.video_speech_proof.evidence_sha256; },
  ]) {
    const h = futureHarness();
    const body = futureEvidenceBody(h, true);
    strip(body);
    assert.equal(enqueue(h, body).ok, false);
  }
  for (const strip of [
    metadata => { delete metadata.audit_contract; },
    metadata => { delete metadata.initial_receipt; },
    metadata => { delete metadata.audit_contract; delete metadata.initial_receipt; },
  ]) {
    const h = futureHarness();
    enqueue(h, futureEvidenceBody(h, true));
    const row = h.outbox.data.at(-1);
    const metadata = JSON.parse(row[6].replace(/^__scheduled_followup__:/, ""));
    strip(metadata);
    row[6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
    const before = JSON.stringify(row);
    const claim = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
    assert.equal(claim.should_send, false);
    assert.equal(JSON.stringify(row), before);
  }
});

function enqueueUnrelatedLegacy(h) {
  const another = h.row.slice();
  another[2] = "9542053206";
  another[27] = "another-listing";
  h.leads.data.push(another);
  enqueue(h, { ...legacy, crm_row: 3, phone: another[2], stable_id: another[27],
    message_id: "followup-3-other", request_id: "scheduler-followup-3-other" });
  return another;
}

test("deterministic future journal failures do not starve an unrelated queued SMS", () => {
  for (const mutate of [
    metadata => { metadata.reserved_padding = "x".repeat(44000); },
    metadata => { metadata.decision_evidence = { schema: "wrong", events: [] }; },
  ]) {
    for (const state of ["queued", "claimed"]) {
      const h = futureHarness();
      enqueue(h, futureEvidenceBody(h));
      const row = h.outbox.data[2];
      const metadata = JSON.parse(row[6].replace(/^__scheduled_followup__:/, ""));
      mutate(metadata);
      row[6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
      row[1] = state;
      if (state === "claimed") { row[10] = "expired-lease"; row[11] = new Date(0); row[18] = "other-worker"; }
      const prior = JSON.stringify(row);
      const another = enqueueUnrelatedLegacy(h);
      const result = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
      assert.equal(result.should_send, true);
      assert.equal(result.phone, another[2]);
      assert.equal(JSON.stringify(row), prior);
    }
  }
});

test("uncertain future claim persistence never claims a second queued conversation", () => {
  const h = futureHarness();
  enqueue(h, futureEvidenceBody(h));
  enqueueUnrelatedLegacy(h);
  const getRange = h.outbox.getRange;
  let failReadback = false;
  h.outbox.getRange = (...args) => {
    const range = getRange(...args);
    return { ...range,
      setValues: values => { range.setValues(values); failReadback = true; },
      getValues: () => { if (failReadback) throw new Error("offline readback failure"); return range.getValues(); },
    };
  };
  assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, false);
  assert.equal(h.outbox.data[2][1], "claimed");
  assert.equal(h.outbox.data[3][1], "queued");
  assert.equal(h.outbox.data[3][9], 0);
});

test("nonretryable future identity changes retain existing cancellation with honest incomplete evidence", () => {
  const h = futureHarness();
  enqueue(h, futureEvidenceBody(h));
  h.row[2] = "9542053206";
  assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, false);
  assert.equal(h.outbox.data[2][1], "superseded");
  const event = journal(h).events[0];
  assert.equal(event.stage, "claim_blocked");
  assert.equal(event.decision.identity_resolution_verified, false);
  assert.equal(event.decision.remaining_controls_evaluated, false);
  assert.equal(event.decision.eligible, false);
  assert.equal(event.decision.blocked_checks, undefined);
});

test("partial future text markers cannot bypass decision journaling at claim or send-start", () => {
  for (const state of ["queued", "claimed"]) {
    for (const field of ["audit_contract", "initial_receipt"]) {
      const h = futureHarness();
      enqueue(h, futureEvidenceBody(h));
      const claim = state === "claimed" ? h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }) : null;
      const row = h.outbox.data.at(-1);
      const metadata = JSON.parse(row[6].replace(/^__scheduled_followup__:/, ""));
      delete metadata[field];
      row[6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
      const before = JSON.stringify(row);
      assert.equal(h.context.claimPendingSmsSendV10_({ worker_id: "pixel" }).should_send, false, `${state} ${field}`);
      if (claim) assert.equal(h.context.markPendingSmsSendStartedV10_(claim).should_send, false, field);
      assert.equal(JSON.stringify(row), before);
    }
  }
  const h = futureHarness();
  enqueue(h, futureEvidenceBody(h));
  const claim = h.context.claimPendingSmsSendV10_({ worker_id: "pixel" });
  const row = h.outbox.data.at(-1);
  const metadata = JSON.parse(row[6].replace(/^__scheduled_followup__:/, ""));
  delete metadata.initial_receipt;
  delete metadata.audit_contract;
  row[6] = `__scheduled_followup__:${JSON.stringify(metadata)}`;
  const before = JSON.stringify(row);
  assert.equal(h.context.markPendingSmsSendStartedV10_(claim).should_send, false);
  assert.equal(JSON.stringify(row), before);
});
