import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";

const root = path.join(import.meta.dirname, "..");
const context = vm.createContext({
  normalizePhone_: (value) => String(value).replace(/\D/g, "").slice(-10),
  Utilities: {
    DigestAlgorithm: { SHA_256: "sha256" },
    computeDigest: (_algorithm, value) => [...createHash("sha256").update(value).digest()]
      .map((byte) => byte > 127 ? byte - 256 : byte),
  },
});
vm.runInContext(fs.readFileSync(path.join(root, "apps_script/sms_outbox.js"), "utf8"), context);

const MODEL = {
  model_id: "Systran/faster-whisper-tiny.en",
  model_revision: "0d3d19a32d3338f10357c0889762bd8d64bbdeba",
  model_manifest_sha256: "9f995ad3147f0799b2babb9e84926a53bca82d888d5fb6558d925f38dd2b78ee",
};
const POLICIES = {
  v1: {
    policy_version: "exact-greeting-tiny-en-v1",
    config_sha256: "f1a2b9866f0576c6500f760d0b58b7a29ed8acf9d337aacc0c66c134ad39e9ca",
  },
  v2: {
    policy_version: "spoken-greeting-tiny-en-v2",
    config_sha256: "b3a1b5798d53032e5c23da361c467141c1cfd502a8def78e8d3ddeaabde2322e",
  },
  v3: {
    policy_version: "spoken-greeting-tiny-en-v3",
    config_sha256: "bb77463c65987cdf07caef001a49d29cb70a93ee7372e7e9edcacd07fde5fea3",
  },
};

function proof(policy, observed) {
  return { ...MODEL, ...POLICIES[policy], normalized_transcript: `hey ${observed}` };
}

function accepts(policy, expected, observed, changes = {}) {
  return context.matchesVideoFollowupSpeechPolicyV22_(
    { ...proof(policy, observed), ...changes },
    expected,
  );
}

function canonicalDigest(value) {
  const sorted = (item) => Array.isArray(item) ? item.map(sorted) : item && typeof item === "object"
    ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, sorted(item[key])])) : item;
  return createHash("sha256").update(JSON.stringify(sorted(value))).digest("hex");
}

function plannedV3(name, observed = name.toLowerCase(), changes = {}) {
  const raw = {
    accepted: true, ...MODEL, ...POLICIES.v3,
    source_audio_sha256: "a".repeat(64), expected_name: name,
    normalized_transcript: `hey ${observed}`,
    confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.01 }],
  };
  const proof = {
    ...raw, media_sha256: "b".repeat(64), greeting_seconds: 1.2,
    verification_method: "planned-aac-lineage-v2",
    verification_config_sha256: "45aa4b4c344179321d82eaf66ab0c36bcaa7cf8f56b48b07f9b63423bc2a6cf5",
    audio_lineage: {
      accepted: true, lineage_version: "planned-aac-same-take-v1", first_name: name,
      manifest_sha256: "c".repeat(64), planned_audio_sha256: "d".repeat(64),
      final_media_sha256: "b".repeat(64), checked_audio_sha256: "a".repeat(64),
      full_pcm_sha256: "e".repeat(64), greeting_pcm_sha256: "f".repeat(64),
      aac_track_sha256: "1".repeat(64), speech_proof_sha256: canonicalDigest(raw),
      greeting_seconds: 1.2,
    },
    video_job_id: "job_123", followup_key: "2".repeat(64), phone: "9542053205",
    initial_sent_at: "2026-10-17T13:00:00.000Z", stable_id: "listing-123",
  };
  Object.assign(proof, changes);
  const rawProof = Object.fromEntries(Object.entries(proof).filter(([key]) => ![
    "video_job_id", "followup_key", "phone", "initial_sent_at", "stable_id",
  ].includes(key)));
  return {
    name,
    raw: rawProof,
    body: { phone: "9542053205", message: "https://go.crispshortsales.com/v/job_123" },
    metadata: {
      video_variant: "video", first_name: name, video_job_id: "job_123",
      followup_key: "2".repeat(64), stable_id: "listing-123",
      initial_sent_at: "2026-10-17T13:00:00.000Z", video_speech_proof: proof,
    },
  };
}

function javascriptFullDecision(item) {
  return context.hasValidVideoFollowupSpeechProofV21_(item.body, item.metadata);
}

function pythonFullDecisions(items) {
  const program = `
import json, sys
from video_followup_speech_guard import validate_speech_proof
items = json.load(sys.stdin)
json.dump([validate_speech_proof(item["raw"], first_name=item["name"],
    media_sha256=item["raw"].get("media_sha256")) for item in items], sys.stdout)
`;
  const result = spawnSync("python", ["-c", program], {
    cwd: root, input: JSON.stringify(items.map(({ name, raw }) => ({ name, raw }))),
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

test("v3 accepts only exact names and explicitly pinned pronunciation groups", () => {
  for (const [expected, observed] of [
    ["cydney", "cydney"], ["cydney", "sydney"], ["sydney", "cydney"],
    ["brook", "brooke"], ["brooke", "brook"],
    ["kathryn", "katherine"], ["nikki", "nicky"],
  ]) {
    assert.equal(accepts("v3", expected, observed), true, `${expected}/${observed}`);
  }

  for (const [expected, observed] of [
    ["cydney", "cindy"], ["cydney", "sidney"], ["sydney", "cindy"],
    ["brook", "brooks"], ["brooke", "brooklyn"],
    ["jim", "gym"], ["marc", "mark"], ["cydney", "sydney smith"],
  ]) {
    assert.equal(accepts("v3", expected, observed), false, `${expected}/${observed}`);
  }
});

test("v1 and v2 behavior remains unchanged", () => {
  assert.equal(accepts("v1", "cydney", "cydney"), true);
  assert.equal(accepts("v1", "cydney", "sydney"), false);
  assert.equal(accepts("v1", "kathryn", "katherine"), false);

  assert.equal(accepts("v2", "kathryn", "katherine"), true);
  assert.equal(accepts("v2", "nikki", "nicky"), true);
  assert.equal(accepts("v2", "cydney", "sydney"), false);
  assert.equal(accepts("v2", "brook", "brooke"), false);
});

test("policy identity tuples cannot be mixed or widened", () => {
  assert.equal(accepts("v3", "cydney", "sydney", { policy_version: POLICIES.v2.policy_version }), false);
  assert.equal(accepts("v2", "cydney", "sydney", { policy_version: POLICIES.v3.policy_version }), false);
  assert.equal(accepts("v3", "cydney", "sydney", { config_sha256: "0".repeat(64) }), false);
  assert.equal(accepts("v3", "cydney", "sydney", { model_revision: "latest" }), false);
});

test("v3 full proofs require the exact planned-aac-lineage-v2 binding", () => {
  const valid = plannedV3("Cydney", "sydney");
  assert.equal(javascriptFullDecision(valid), true);

  for (const changes of [
    { verification_method: undefined },
    { verification_method: "planned-aac-lineage-v1" },
    { verification_config_sha256: undefined },
    { verification_config_sha256: "0".repeat(64) },
    { audio_lineage: undefined },
  ]) {
    assert.equal(javascriptFullDecision(plannedV3("Cydney", "sydney", changes)), false,
      JSON.stringify(changes));
  }
});

test("v3 lineage stays bound to the accepted name, media, audio, duration, and hash shapes", () => {
  const mutations = [
    (lineage) => { lineage.accepted = false; },
    (lineage) => { lineage.lineage_version = "planned-aac-same-take-v2"; },
    (lineage) => { lineage.first_name = "Sydney"; },
    (lineage) => { lineage.final_media_sha256 = "9".repeat(64); },
    (lineage) => { lineage.checked_audio_sha256 = "9".repeat(64); },
    (lineage) => { lineage.greeting_seconds = 1.3; },
    (lineage) => { lineage.speech_proof_sha256 = "invalid"; },
  ];
  for (const mutate of mutations) {
    const item = plannedV3("Cydney", "sydney");
    mutate(item.metadata.video_speech_proof.audio_lineage);
    assert.equal(javascriptFullDecision(item), false);
  }
});

test("v3 keeps the established finite confidence thresholds without reserializing proof numbers", () => {
  assert.equal(javascriptFullDecision(plannedV3("Cydney", "sydney", {
    confidence: [{ avg_logprob: -1.0, no_speech_prob: 1e-7 }],
  })), true);
  assert.equal(javascriptFullDecision(plannedV3("Cydney", "sydney", {
    confidence: [{ avg_logprob: -1.0000001, no_speech_prob: 1e-7 }],
  })), false);
  assert.equal(javascriptFullDecision(plannedV3("Cydney", "sydney", {
    confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.6000001 }],
  })), false);
});

test("Apps Script v3 decisions match the Python full proof validator", () => {
  const cases = [
    plannedV3("Cydney", "cydney"), plannedV3("Cydney", "sydney"),
    plannedV3("Sydney", "cydney"), plannedV3("Brook", "brooke"),
    plannedV3("Brooke", "brook"), plannedV3("Kathryn", "katherine"),
    plannedV3("Nikki", "nicky"), plannedV3("Cydney", "cindy"),
    plannedV3("Cydney", "sidney"), plannedV3("Brook", "brooks"),
    plannedV3("Marc", "mark"),
    plannedV3("Cydney", "sydney", { verification_method: "planned-aac-lineage-v1" }),
    plannedV3("Cydney", "sydney", { verification_config_sha256: "0".repeat(64) }),
    plannedV3("Cydney", "sydney", {
      verification_method: undefined,
      verification_config_sha256: undefined,
      audio_lineage: undefined,
    }),
  ];
  assert.deepEqual(cases.map(javascriptFullDecision), pythonFullDecisions(cases));
});
