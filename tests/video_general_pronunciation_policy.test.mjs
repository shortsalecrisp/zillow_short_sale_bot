import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import { gunzipSync } from "node:zlib";

const root = path.join(import.meta.dirname, "..");
const JSON_SHA = "e47b99c5c03266ec49f5b5164853abb007d603e324b72c630c3c49ad8e6844df";
let lexiconCalls = 0;
let manifestSha = JSON_SHA;
let lexicon = Object.freeze(Object.assign(Object.create(null), {
  cydney: null,
  hay: "HH EY1",
  hey: "HH EY1",
  mary: "M EH1 R IY0",
  ann: "AE1 N",
  maryann: "M EH1 R IY0 AE1 N",
  mark: "M AA1 R K",
  marc: "M AA1 R K",
  sherri: "SH EH1 R IY0",
  cherie: "SH EH1 R IY0",
  shelley: "SH EH1 L IY0",
  shelly: "SH EH1 L IY0",
  stressa: "S T R EH1 S AH0",
  stressb: "S T R EH0 S AH0",
}));

const context = vm.createContext({
  normalizePhone_: (value) => String(value).replace(/\D/g, "").slice(-10),
  videoPronunciationLexiconManifestV24_: () => ({ json_sha256: manifestSha }),
  videoPronunciationLexiconV24_: () => {
    lexiconCalls += 1;
    if (lexicon instanceof Error) throw lexicon;
    return lexicon;
  },
});
vm.runInContext(fs.readFileSync(path.join(root, "apps_script/sms_outbox.js"), "utf8"), context);

const MODEL = {
  model_id: "Systran/faster-whisper-tiny.en",
  model_revision: "0d3d19a32d3338f10357c0889762bd8d64bbdeba",
  model_manifest_sha256: "9f995ad3147f0799b2babb9e84926a53bca82d888d5fb6558d925f38dd2b78ee",
};
const V4 = {
  policy_version: "spoken-greeting-tiny-en-v4",
  config_sha256: "e61b237ccaa3f4cccb369f0a88b1f9d0d4e01838242d9b74e90c8ed4891f03a2",
};

function accepts(expected, observed, changes = {}) {
  return context.matchesVideoFollowupSpeechPolicyV22_({
    ...MODEL, ...V4, normalized_transcript: `hey ${observed}`, ...changes,
  }, expected);
}

function canonicalDigest(value) {
  const sorted = (item) => Array.isArray(item) ? item.map(sorted) : item && typeof item === "object"
    ? Object.fromEntries(Object.keys(item).sort().map((key) => [key, sorted(item[key])])) : item;
  return createHash("sha256").update(JSON.stringify(sorted(value))).digest("hex");
}

function plannedV4(name, observed, changes = {}) {
  const raw = {
    accepted: true, ...MODEL, ...V4,
    source_audio_sha256: "a".repeat(64),
    expected_name: name, normalized_transcript: `hey ${observed}`,
    confidence: [{ avg_logprob: -0.1, no_speech_prob: 0.01 }],
  };
  const proof = {
    ...raw, media_sha256: "b".repeat(64),
    greeting_seconds: 1.2, verification_method: "planned-aac-lineage-v3",
    verification_config_sha256: "3d70d23288207ff942cc5385ee6a926eb529434e950e4f4345e333f944500e25",
    audio_lineage: {
      accepted: true, lineage_version: "planned-aac-same-take-v1", first_name: name,
      manifest_sha256: "c".repeat(64), planned_audio_sha256: "d".repeat(64),
      final_media_sha256: "b".repeat(64), checked_audio_sha256: "a".repeat(64),
      full_pcm_sha256: "e".repeat(64), greeting_pcm_sha256: "f".repeat(64),
      aac_track_sha256: "1".repeat(64), speech_proof_sha256: canonicalDigest(raw),
      greeting_seconds: 1.2,
    },
    video_job_id: "job_123", followup_key: "3".repeat(64), phone: "9542053205",
    initial_sent_at: "2026-10-17T13:00:00.000Z", stable_id: "listing-123",
  };
  Object.assign(proof, changes);
  return {
    body: { phone: "9542053205", message: "https://go.crispshortsales.com/v/job_123" },
    metadata: {
      video_variant: "video", first_name: name, video_job_id: "job_123",
      followup_key: "3".repeat(64), stable_id: "listing-123",
      initial_sent_at: "2026-10-17T13:00:00.000Z", video_speech_proof: proof,
    },
  };
}

function pythonFullDecisions(items) {
  const program = `
import json, sys
from video_followup_speech_guard import validate_speech_proof
items = json.load(sys.stdin)
json.dump([validate_speech_proof(item["proof"], first_name=item["name"],
    media_sha256=item["proof"].get("media_sha256")) for item in items], sys.stdout)
`;
  const payload = items.map((item) => ({
    name: item.metadata.first_name,
    proof: Object.fromEntries(Object.entries(item.metadata.video_speech_proof).filter(([key]) => ![
      "video_job_id", "followup_key", "phone", "initial_sent_at", "stable_id",
    ].includes(key))),
  }));
  const result = spawnSync("python", ["-c", program], {
    cwd: root, env: { ...process.env, PYTHONPATH: root }, input: JSON.stringify(payload), encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr);
  return JSON.parse(result.stdout);
}

test.beforeEach(() => {
  lexiconCalls = 0;
  manifestSha = JSON_SHA;
  lexicon = Object.freeze(Object.assign(Object.create(null), {
    cydney: null,
    hay: "HH EY1", hey: "HH EY1",
    mary: "M EH1 R IY0", ann: "AE1 N", maryann: "M EH1 R IY0 AE1 N",
    mark: "M AA1 R K", marc: "M AA1 R K",
    sherri: "SH EH1 R IY0", cherie: "SH EH1 R IY0",
    shelley: "SH EH1 L IY0", shelly: "SH EH1 L IY0",
    stressa: "S T R EH1 S AH0", stressb: "S T R EH0 S AH0",
  }));
});

test("v4 preserves exact supported names and reviewed v3 groups without loading data", () => {
  assert.equal(accepts("jerrick", "jerrick"), true);
  assert.equal(accepts("cydney", "sydney"), true);
  assert.equal(accepts("brook", "brooke"), true);
  assert.equal(accepts("kathryn", "katherine"), true);
  assert.equal(lexiconCalls, 0);
});

test("v4 compares complete stress-bearing phone sequences from the pinned lookup", () => {
  assert.equal(accepts("shelly", "shelley"), true);
  assert.equal(accepts("marc", "mark"), true);
  assert.equal(accepts("maryann", "mary ann"), true);
  assert.equal(accepts("hay", "hey"), true);
  assert.equal(accepts("stressa", "stressb"), false);
  assert.equal(accepts("cydney", "cindy"), false);
  assert.ok(lexiconCalls >= 5);
});

test("v4 rejects OOV, ambiguous, partial, extra, repeated, and malformed tails", () => {
  assert.equal(accepts("shelly", "unknown"), false);
  assert.equal(accepts("cydney", "cindy"), false);
  assert.equal(accepts("maryann", "mary"), false);
  assert.equal(accepts("maryann", "mary ann ann"), false);
  assert.equal(accepts("maryann", "mary ann ann ann ann"), false);
  assert.equal(accepts("maryann", "hey maryann"), false);
  assert.equal(accepts("maryann", "Mary Ann"), false);
  assert.equal(accepts("maryann", "mary ann!"), false);
});

test("v4 recomputes from immutable data and ignores supplied phone claims", () => {
  assert.equal(accepts("stressa", "stressb", {
    expected_phone_sequences: [["S", "T", "R", "EH1", "S", "AH0"]],
    observed_phone_sequences: [["S", "T", "R", "EH1", "S", "AH0"]],
  }), false);
});

test("v4 fails closed on missing, altered, or unavailable pronunciation data", () => {
  manifestSha = "0".repeat(64);
  assert.equal(accepts("shelly", "shelley"), false);
  assert.equal(lexiconCalls, 0);

  manifestSha = JSON_SHA;
  lexicon = new Error("pronunciation_lexicon_integrity");
  assert.equal(accepts("shelly", "shelley"), false);
  assert.equal(accepts("jerrick", "jerrick"), true);
});

test("missing or corrupt data preserves exact and reviewed proofs but rejects lexical proofs in both runtimes", () => {
  const js = [];
  for (const mode of ["missing", "corrupt"]) {
    manifestSha = mode === "corrupt" ? "0".repeat(64) : JSON_SHA;
    lexicon = new Error(`pronunciation_lexicon_${mode}`);
    js.push(accepts("jerrick", "jerrick"));
    js.push(accepts("cydney", "sydney"));
    js.push(accepts("shelly", "shelley"));
  }

  const program = `
import json, tempfile
from pathlib import Path
import video_followup_pronunciation as pronunciation
import video_followup_speech_guard as guard

def proof(name, observed):
    return {"accepted": True, **guard._policy_identity(guard.GENERAL_PRONUNCIATION_POLICY_VERSION),
        "source_audio_sha256": "a" * 64, "expected_name": name,
        "normalized_transcript": "hey " + observed,
        "confidence": [{"avg_logprob": -0.1, "no_speech_prob": 0.01}]}

source = pronunciation._PATH
decisions = []
with tempfile.TemporaryDirectory() as directory:
    for mode in ("missing", "corrupt"):
        target = Path(directory) / (mode + ".json.gz")
        if mode == "corrupt":
            content = bytearray(source.read_bytes())
            content[len(content) // 2] ^= 1
            target.write_bytes(content)
        pronunciation._PATH = target
        pronunciation._load_lexicon.cache_clear()
        for name, observed in (("Jerrick", "jerrick"), ("Cydney", "sydney"), ("Shelly", "shelley")):
            decisions.append(guard.validate_speech_proof(proof(name, observed), first_name=name))
pronunciation._PATH = source
pronunciation._load_lexicon.cache_clear()
print(json.dumps(decisions))
`;
  const result = spawnSync("python", ["-c", program], {
    cwd: root, env: { ...process.env, PYTHONPATH: root }, encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr);
  const expected = [true, true, false, true, true, false];
  assert.deepEqual(js, expected);
  assert.deepEqual(JSON.parse(result.stdout), expected);
});

test("v4 policy identity cannot be mixed with legacy tuples", () => {
  assert.equal(accepts("shelly", "shelley", { config_sha256: "0".repeat(64) }), false);
  assert.equal(lexiconCalls, 0);
  assert.equal(accepts("shelly", "shelley", { policy_version: "spoken-greeting-tiny-en-v3" }), false);
  assert.equal(accepts("cydney", "sydney", { policy_version: "spoken-greeting-tiny-en-v2" }), false);
  assert.equal(accepts("shelly", "shelley", { model_revision: "latest" }), false);
});

test("full v4 proofs require the exact v3 method and complete final-media lineage", () => {
  assert.equal(context.hasValidVideoFollowupSpeechProofV21_(
    plannedV4("Shelly", "shelley").body, plannedV4("Shelly", "shelley").metadata), true);
  for (const changes of [
    { verification_method: undefined },
    { verification_method: "planned-aac-lineage-v2" },
    { verification_config_sha256: undefined },
    { verification_config_sha256: "0".repeat(64) },
    { audio_lineage: undefined },
  ]) {
    const item = plannedV4("Shelly", "shelley", changes);
    assert.equal(context.hasValidVideoFollowupSpeechProofV21_(item.body, item.metadata), false,
      JSON.stringify(changes));
  }
});

test("v4 retains the existing confidence, recipient, media, and URL bindings", () => {
  for (const mutate of [
    (item) => { item.metadata.video_speech_proof.confidence[0].avg_logprob = -1.0001; },
    (item) => { item.metadata.video_speech_proof.confidence[0].no_speech_prob = 0.6001; },
    (item) => { item.metadata.video_speech_proof.audio_lineage.first_name = "Shelley"; },
    (item) => { item.metadata.video_speech_proof.audio_lineage.final_media_sha256 = "9".repeat(64); },
    (item) => { item.metadata.video_speech_proof.audio_lineage.checked_audio_sha256 = "9".repeat(64); },
    (item) => { item.body.phone = "9542053206"; },
    (item) => { item.body.message = "https://go.crispshortsales.com/v/other"; },
  ]) {
    const item = plannedV4("Shelly", "shelley");
    mutate(item);
    assert.equal(context.hasValidVideoFollowupSpeechProofV21_(item.body, item.metadata), false);
  }
});

test("the generated Apps Script asset independently validates reviewed real dictionary cases", () => {
  function blob(bytes) {
    const data = Buffer.from(bytes.map((value) => (value + 256) % 256));
    return {
      data,
      getBytes: () => [...data],
      getDataAsString: () => data.toString("utf8"),
    };
  }
  const assetContext = vm.createContext({
    normalizePhone_: (value) => String(value).replace(/\D/g, "").slice(-10),
    Utilities: {
      DigestAlgorithm: { SHA_256: "SHA_256" },
      base64Decode: (value) => [...Buffer.from(value, "base64")],
      computeDigest: (_algorithm, bytes) => [...createHash("sha256")
        .update(Buffer.from(bytes.map((value) => (value + 256) % 256))).digest()],
      newBlob: (bytes) => blob(bytes),
      ungzip: (value) => blob([...gunzipSync(value.data)]),
    },
  });
  vm.runInContext(fs.readFileSync(
    path.join(root, "apps_script/video_pronunciation_lexicon.js"), "utf8"), assetContext);
  vm.runInContext(fs.readFileSync(path.join(root, "apps_script/sms_outbox.js"), "utf8"), assetContext);

  const decide = (expected, observed) => assetContext.matchesVideoFollowupSpeechPolicyV22_({
    ...MODEL, ...V4, normalized_transcript: `hey ${observed}`,
  }, expected);
  assert.equal(decide("shelly", "shelley"), true);
  assert.equal(decide("marc", "mark"), true);
  assert.equal(decide("sherri", "cherie"), true);
  assert.equal(decide("sonia", "sonya"), false);
  assert.equal(decide("elise", "a lease"), false);
  assert.equal(decide("cydney", "cindy"), false);
  assert.equal(decide("cydney", "sydney"), true);
  assert.equal(decide("jerrick", "jerrick"), true);
  const loaded = assetContext.videoPronunciationLexiconV24_();
  assert.equal(Object.getPrototypeOf(loaded), null);
  assert.equal(Object.isFrozen(loaded), true);
});

test("Apps Script and Python agree on complete v4 final proofs", () => {
  const items = [
    plannedV4("Jerrick", "jerrick"),
    plannedV4("Cydney", "sydney"),
    plannedV4("Shelly", "shelley"),
    plannedV4("Marc", "mark"),
    plannedV4("Sherri", "cherie"),
    plannedV4("Sonia", "sonya"),
    plannedV4("Cydney", "cindy"),
    plannedV4("Elise", "a lease"),
    plannedV4("Shelly", "shelley", { verification_method: "planned-aac-lineage-v2" }),
    plannedV4("Shelly", "shelley", { verification_config_sha256: "0".repeat(64) }),
  ];
  const js = items.map((item) => context.hasValidVideoFollowupSpeechProofV21_(item.body, item.metadata));
  assert.deepEqual(js, pythonFullDecisions(items));
});

