"""Keep the two independently deployed speech-proof gates in agreement."""
import itertools
import json
from pathlib import Path
import subprocess

from video_followup_speech_guard import (
    CONFIG_SHA256,
    LEGACY_PROOF_IDENTITY,
    MODEL_ID,
    MODEL_MANIFEST_SHA256,
    MODEL_REVISION,
    PACKAGE_VERSIONS,
    POLICY_VERSION,
    SPELLING_EQUIVALENCE_GROUPS,
    validate_speech_proof,
)

ROOT = Path(__file__).resolve().parents[1]


def current_identity():
    return {
        "policy_version": POLICY_VERSION,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "model_manifest_sha256": MODEL_MANIFEST_SHA256,
        "config_sha256": CONFIG_SHA256,
    }


def proof_for(name, identity, observed=None):
    return {**identity, "accepted": True, "expected_name": name,
            "normalized_transcript": "hey " + (observed or name.casefold()),
            "source_audio_sha256": "a" * 64, "media_sha256": "b" * 64,
            "greeting_seconds": 1.2,
            "confidence": [{"avg_logprob": -0.1, "no_speech_prob": 0.01}],
            "video_job_id": "job_123", "followup_key": "c" * 64,
            "phone": "9542053205", "stable_id": "listing-123",
            "initial_sent_at": "2026-10-13T13:00:00.000Z"}


def javascript_decisions(cases):
    program = """
const fs = require('node:fs');
const vm = require('node:vm');
const context = vm.createContext({
  normalizePhone_: (value) => String(value).replace(/\\D/g, '').slice(-10),
});
vm.runInContext(fs.readFileSync('apps_script/sms_outbox.js', 'utf8'), context);
const cases = JSON.parse(fs.readFileSync(0, 'utf8'));
const result = cases.map(({name, proof}) => {
  const metadata = {
    video_variant: 'video', first_name: name, video_job_id: 'job_123',
    followup_key: 'c'.repeat(64), stable_id: 'listing-123',
    initial_sent_at: '2026-10-13T13:00:00.000Z', video_speech_proof: proof,
  };
  return context.hasValidVideoFollowupSpeechProofV21_({
    phone: '9542053205', message: 'https://go.crispshortsales.com/v/job_123',
  }, metadata);
});
process.stdout.write(JSON.stringify(result));
"""
    result = subprocess.run(["node", "-e", program], cwd=ROOT, input=json.dumps(cases),
                            text=True, capture_output=True, check=True, timeout=20)
    return json.loads(result.stdout)


def test_apps_script_preserves_exact_legacy_and_active_proof_semantics():
    cases = []
    for identity in (LEGACY_PROOF_IDENTITY, current_identity()):
        for name in ("Jim", "Kathryn", "Nikki", "D'Andre", "Jos\u00e9", "Stra\u00dfe"):
            cases.append({"name": name, "proof": proof_for(name, identity)})
        for group in SPELLING_EQUIVALENCE_GROUPS:
            for expected, observed in itertools.permutations(group, 2):
                name = expected.title()
                cases.append({"name": name, "proof": proof_for(name, identity, observed)})
        for name, observed in (("Kathryn", "kathleen"), ("Kathryn", "catherine"), ("Kathryn", "karen"),
                               ("Kathryn", "nikki"), ("Nikki", "mickey"),
                               ("Nikki", "nick"), ("Nikki", "nicole"),
                               ("Jacqueline", "jack lim"), ("Sibelys", "cebellis"),
                               ("Jerrick", "jim"), ("Marc", "mark"),
                               ("Nikki", "nicky hey jim")):
            cases.append({"name": name, "proof": proof_for(name, identity, observed)})
    expected = [validate_speech_proof(case["proof"], first_name=case["name"],
                                     media_sha256="b" * 64) for case in cases]
    assert javascript_decisions(cases) == expected
    assert expected == [True] * 6 + [False] * 16 + [True] * 10 + [False] * 12


def test_apps_script_rejects_mixed_policy_tuples_and_changed_evidence():
    cases = []
    for identity, other in ((LEGACY_PROOF_IDENTITY, current_identity()),
                            (current_identity(), LEGACY_PROOF_IDENTITY)):
        for field in identity:
            changes = ["incorrect", None]
            if identity[field] != other[field]:
                changes.append(other[field])
            for value in changes:
                proof = {**proof_for("Jim", identity), field: value}
                cases.append({"name": "Jim", "proof": proof})
        for changes in ({"accepted": "true"}, {"source_audio_sha256": "invalid"},
                        {"media_sha256": "c" * 64}, {"greeting_seconds": 3},
                        {"confidence": [{"avg_logprob": -1.01, "no_speech_prob": 0.01}]}):
            cases.append({"name": "Jim", "proof": {**proof_for("Jim", identity), **changes}})
    expected = [validate_speech_proof(case["proof"], first_name="Jim", media_sha256="b" * 64)
                for case in cases]
    assert not any(expected)
    # Apps Script validates the worker's media attestation, not the actual MP4 bytes.
    compare = [case for case in cases if case["proof"].get("media_sha256") != "c" * 64]
    assert javascript_decisions(compare) == [False] * len(compare)


def test_legacy_proof_identity_stays_pinned_and_intermediate_policy_is_not_accepted():
    assert SPELLING_EQUIVALENCE_GROUPS == (("kathryn", "katherine"), ("nikki", "nicky"))
    assert LEGACY_PROOF_IDENTITY == {
        "policy_version": "exact-greeting-tiny-en-v1",
        "model_id": "Systran/faster-whisper-tiny.en",
        "model_revision": "0d3d19a32d3338f10357c0889762bd8d64bbdeba",
        "model_manifest_sha256": "9f995ad3147f0799b2babb9e84926a53bca82d888d5fb6558d925f38dd2b78ee",
        "config_sha256": "f1a2b9866f0576c6500f760d0b58b7a29ed8acf9d337aacc0c66c134ad39e9ca",
    }
    proof = proof_for("Nikki", {**current_identity(), "policy_version": "spelling-equivalent-greeting-tiny-en-v2"}, "nicky")
    assert not validate_speech_proof(proof, first_name="Nikki")
    assert javascript_decisions([{"name": "Nikki", "proof": proof}]) == [False]


def test_speech_runtime_is_separate_and_version_pinned():
    pinned = {}
    for line in (ROOT / "requirements-video-speech-guard.txt").read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            package, version = line.split("==")
            pinned[package] = version
    assert pinned == PACKAGE_VERSIONS
    assert "faster-whisper" not in (ROOT / "requirements.txt").read_text()
    assert "python scripts/provision_video_speech_guard.py" in (ROOT / "render.yaml").read_text()
    startup = (ROOT / "start.sh").read_text()
    assert "python video_followup_speech_guard.py --self-test" in startup
    for flag in ("ENABLED", "GENERATION_ENABLED", "LIVE_APPROVED"):
        assert f"export VIDEO_FOLLOWUP_{flag}=false" in startup
