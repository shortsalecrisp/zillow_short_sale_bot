import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import { gunzipSync } from "node:zlib";

const root = path.join(import.meta.dirname, "..");
const source = fs.readFileSync(path.join(root, "apps_script/sms_outbox.js"), "utf8");
const lexiconBytes = gunzipSync(fs.readFileSync(path.join(root, "data/video_pronunciation/unique-phones-v1.json.gz")));
const lexiconSha = "e47b99c5c03266ec49f5b5164853abb007d603e324b72c630c3c49ad8e6844df";
assert.equal(createHash("sha256").update(lexiconBytes).digest("hex"), lexiconSha);
const dictionary = Object.freeze(Object.assign(Object.create(null), JSON.parse(lexiconBytes)));
const MODEL = {
  model_id: "Systran/faster-whisper-tiny.en",
  model_revision: "0d3d19a32d3338f10357c0889762bd8d64bbdeba",
  model_manifest_sha256: "9f995ad3147f0799b2babb9e84926a53bca82d888d5fb6558d925f38dd2b78ee",
};
const V4 = { policy_version: "spoken-greeting-tiny-en-v4",
  config_sha256: "e61b237ccaa3f4cccb369f0a88b1f9d0d4e01838242d9b74e90c8ed4891f03a2" };
const V5 = { policy_version: "spoken-greeting-tiny-en-v5",
  config_sha256: "060ec33e383b742872a129c3652143f658313851fd61594cec243789b5dcb8fd" };
const METHOD = "planned-aac-lineage-v4";
const METHOD_SHA = "528159aa9b387b42fbc0a4987953e5e46fd36e808ee098a677b5f8cab5ba7592";

function digest(value) {
  const sorted = (item) => Array.isArray(item) ? item.map(sorted) : item && typeof item === "object"
    ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, sorted(item[key])])) : item;
  return createHash("sha256").update(JSON.stringify(sorted(value))).digest("hex");
}

function harness() {
  let loads = 0;
  let available = true;
  const context = vm.createContext({
    normalizePhone_: (value) => String(value).replace(/\D/g, "").slice(-10),
    videoPronunciationLexiconManifestV24_: () => ({ json_sha256: lexiconSha }),
    videoPronunciationLexiconV24_: () => {
      loads++;
      if (!available) throw new Error("immutable data unavailable");
      return dictionary;
    },
  });
  vm.runInContext(source, context);
  return { context, loads: () => loads, unavailable: () => { available = false; } };
}

function proof(name, observed, changes = {}) {
  const raw = { accepted: true, ...MODEL, ...V5, expected_name: name,
    source_audio_sha256: "a".repeat(64), normalized_transcript: `hey ${observed}`,
    confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.01 }] };
  for (const key of Object.keys(raw)) if (Object.hasOwn(changes, key)) raw[key] = changes[key];
  return { ...raw, media_sha256: "b".repeat(64), greeting_seconds: 1.2,
    verification_method: METHOD, verification_config_sha256: METHOD_SHA,
    audio_lineage: { accepted: true, lineage_version: "planned-aac-same-take-v1", first_name: name,
      manifest_sha256: "c".repeat(64), planned_audio_sha256: "d".repeat(64),
      final_media_sha256: "b".repeat(64), checked_audio_sha256: "a".repeat(64),
      full_pcm_sha256: "e".repeat(64), greeting_pcm_sha256: "f".repeat(64),
      aac_track_sha256: "1".repeat(64), speech_proof_sha256: digest(raw), greeting_seconds: 1.2 },
    ...changes };
}

function item(name, observed, changes = {}) {
  const identity = { video_job_id: "job_123", followup_key: "3".repeat(64), phone: "2125550123",
    initial_sent_at: "2026-10-19T13:00:00.000Z", stable_id: "listing-123" };
  return { body: { phone: identity.phone, message: "https://go.crispshortsales.com/v/job_123" },
    metadata: { video_variant: "video", first_name: name, ...identity,
      video_speech_proof: { ...proof(name, observed, changes), ...identity } } };
}

function accepts(h, expected, observed, policy = V5) {
  return h.context.matchesVideoFollowupSpeechPolicyV22_({ ...MODEL, ...policy,
    normalized_transcript: `hey ${observed}` }, expected);
}

test("v5 config and method hashes are exact additive directed contracts", () => {
  assert.equal(digest({ policy: V5.policy_version, base_general_pronunciation_config_sha256: V4.config_sha256,
    matching_rule: "whole-greeting-canonical-name-directed-reviewed-variants-v1",
    observed_name_variants: { colin: ["collin"], sheikh: ["shake"] } }), V5.config_sha256);
  assert.equal(digest({ method: METHOD, lineage_version: "planned-aac-same-take-v1",
    extraction_version: "ffmpeg-pcm-s16le-mono-16k-v1", speech_config_sha256: V5.config_sha256,
    binding: "immutable-planned-aac-full-pcm-and-greeting-v1" }), METHOD_SHA);
});

test("only v5 accepts the two directed reviewed variants without widening old v4", () => {
  const h = harness();
  for (const [expected, observed] of [["colin", "collin"], ["sheikh", "shake"]]) {
    assert.equal(accepts(h, expected, observed), true);
    assert.equal(accepts(h, observed, expected), false, "Reverse direction is not approved");
    assert.equal(accepts(h, expected, observed, V4), false, "Existing dictionary policy remains unchanged");
  }
  assert.equal(dictionary.colin, "K OW1 L IH0 N");
  assert.equal(dictionary.collin, "K AA1 L IH0 N");
  assert.equal(dictionary.sheikh, "SH IY1 K");
  assert.equal(dictionary.shake, "SH EY1 K");
});

test("v5 retains exact, prior reviewed and full lexical matches while rejecting nearby or partial names", () => {
  const h = harness();
  for (const [expected, observed] of [["colin", "colin"], ["sheikh", "sheikh"],
    ["cydney", "sydney"], ["brook", "brooke"], ["shelly", "shelley"]]) {
    assert.equal(accepts(h, expected, observed), true, `${expected}/${observed}`);
  }
  for (const [expected, observed] of [["colin", "colon"], ["colin", "collins"],
    ["colin", "colleen"], ["colin", "collin collin"], ["colin", "hey collin"],
    ["colin", "collin smith"], ["colin", "Collin"], ["colin", "collin!"],
    ["sheikh", "shakes"], ["sheikh", "shrek"], ["sheikh", "shake shake"],
    ["sheikh", "sheikhs"], ["cydney", "cindy"], ["colin", "shake"], ["sheikh", "collin"]]) {
    assert.equal(accepts(h, expected, observed), false, `${expected}/${observed}`);
  }
});

test("v5 prewarming preserves locked-cache protection and skips data only for pinned matches", () => {
  const h = harness();
  h.unavailable();
  for (const [name, observed] of [["Colin", "collin"], ["Sheikh", "shake"], ["Colin", "colin"], ["Cydney", "sydney"]]) {
    const candidate = item(name, observed);
    assert.equal(h.context.prewarmVideoFollowupPronunciationV24_(candidate.metadata), false);
    assert.equal(h.context.hasValidVideoFollowupSpeechProofV21_(candidate.body, candidate.metadata), true);
  }
  assert.equal(h.loads(), 0);
  const lexical = item("Shelly", "shelley");
  assert.equal(h.context.prewarmVideoFollowupPronunciationV24_(lexical.metadata), true);
  h.context.VIDEO_PRONUNCIATION_CACHE_ONLY_V24_ = true;
  assert.equal(h.context.hasValidVideoFollowupSpeechProofV21_(lexical.body, lexical.metadata), false);
  assert.equal(h.loads(), 1, "No lexical load occurs inside the locked validation path");
});

test("v5 cannot be relabeled or downgraded into any older policy or lineage", () => {
  const h = harness();
  for (const changes of [{ ...V4 }, { config_sha256: V4.config_sha256 },
    { policy_version: V4.policy_version }, { verification_method: "planned-aac-lineage-v3" },
    { verification_config_sha256: "3d70d23288207ff942cc5385ee6a926eb529434e950e4f4345e333f944500e25" },
    { verification_method: undefined }, { verification_config_sha256: undefined }, { audio_lineage: undefined },
    { model_revision: "latest" }, { model_manifest_sha256: "0".repeat(64) }]) {
    const candidate = item("Colin", "collin", changes);
    assert.equal(h.context.hasValidVideoFollowupSpeechProofV21_(candidate.body, candidate.metadata), false, JSON.stringify(changes));
  }
  const oldExact = item("Colin", "colin", { ...V4, verification_method: "planned-aac-lineage-v3",
    verification_config_sha256: "3d70d23288207ff942cc5385ee6a926eb529434e950e4f4345e333f944500e25" });
  assert.equal(h.context.hasValidVideoFollowupSpeechProofV21_(oldExact.body, oldExact.metadata), true);
});

test("v5 preserves strict confidence, cadence, canonical identity, media and episode binding", () => {
  const h = harness();
  for (const mutate of [
    (x) => { x.metadata.video_speech_proof.confidence[0].avg_logprob = -1.0001; },
    (x) => { x.metadata.video_speech_proof.confidence[0].no_speech_prob = 0.6001; },
    (x) => { x.metadata.video_speech_proof.greeting_seconds = 2.5001; },
    (x) => { x.metadata.video_speech_proof.greeting_seconds = 0.4999; },
    (x) => { x.metadata.video_speech_proof.expected_name = "Collin"; },
    (x) => { x.metadata.first_name = "Collin"; },
    (x) => { x.metadata.video_speech_proof.audio_lineage.first_name = "Collin"; },
    (x) => { x.metadata.video_speech_proof.audio_lineage.final_media_sha256 = "9".repeat(64); },
    (x) => { x.metadata.video_speech_proof.audio_lineage.checked_audio_sha256 = "9".repeat(64); },
    (x) => { x.metadata.video_speech_proof.initial_sent_at = "2026-10-19T14:00:00Z"; },
    (x) => { x.metadata.video_speech_proof.stable_id = "other-listing"; },
    (x) => { x.body.phone = "2125550124"; },
    (x) => { x.body.message = "https://go.crispshortsales.com/v/other"; },
  ]) {
    const candidate = item("Colin", "collin");
    mutate(candidate);
    assert.equal(h.context.hasValidVideoFollowupSpeechProofV21_(candidate.body, candidate.metadata), false);
  }
});

test("Apps Script and Python agree on directed v5 full proofs and unchanged old v4 decisions", () => {
  const h = harness();
  const candidates = [["Colin", "colin"], ["Sheikh", "sheikh"], ["Colin", "collin"], ["Sheikh", "shake"], ["Collin", "colin"],
    ["Shake", "sheikh"], ["Colin", "colon"], ["Sheikh", "shakes"], ["Cydney", "sydney"],
    ["Cydney", "cindy"], ["Shelly", "shelley"]].map(([name, observed]) => item(name, observed));
  candidates.push(item("Colin", "collin", { ...V4, verification_method: "planned-aac-lineage-v3",
    verification_config_sha256: "3d70d23288207ff942cc5385ee6a926eb529434e950e4f4345e333f944500e25" }));
  candidates.push(item("Colin", "colin", { ...V4, verification_method: "planned-aac-lineage-v3",
    verification_config_sha256: "3d70d23288207ff942cc5385ee6a926eb529434e950e4f4345e333f944500e25" }));
  candidates.push(item("Colin", "collin", { verification_method: "planned-aac-lineage-v3" }));
  const payload = candidates.map((candidate) => ({ name: candidate.metadata.first_name,
    proof: Object.fromEntries(Object.entries(candidate.metadata.video_speech_proof).filter(([key]) =>
      !["video_job_id", "followup_key", "phone", "initial_sent_at", "stable_id"].includes(key))) }));
  const result = spawnSync("python", ["-c", "import json,sys; from video_followup_speech_guard import validate_speech_proof; data=json.load(sys.stdin); json.dump([validate_speech_proof(x[\"proof\"], first_name=x[\"name\"], media_sha256=x[\"proof\"].get(\"media_sha256\")) for x in data],sys.stdout)"],
    { cwd: root, env: { ...process.env, PYTHONPATH: root }, input: JSON.stringify(payload), encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(candidates.map((candidate) => h.context.hasValidVideoFollowupSpeechProofV21_(candidate.body, candidate.metadata)),
    JSON.parse(result.stdout));
});
