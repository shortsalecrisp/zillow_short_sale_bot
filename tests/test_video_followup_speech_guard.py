"""Offline content-gate tests. Never run ASR or contact a model provider."""

import copy
from contextlib import nullcontext
import fcntl
import hashlib
import importlib.metadata
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import resource
import subprocess
import sys
from types import SimpleNamespace
import wave

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import video_followup_speech_guard as guard


@pytest.fixture
def audio():
    points = np.arange(24000) / 24000
    samples = (np.sin(2 * math.pi * 220 * points) * 6000).astype("<i2")
    output = io.BytesIO()
    with wave.open(output, "wb") as stream:
        stream.setnchannels(1)
        stream.setsampwidth(2)
        stream.setframerate(24000)
        stream.writeframes(samples.tobytes())
    return output.getvalue()


@pytest.fixture
def recognized(monkeypatch):
    result = {
        "transcript": "Hey Jerrick!",
        "segments": [{"avg_logprob": -0.10, "no_speech_prob": 0.01}],
        "model_revision": guard.MODEL_REVISION,
        "config_sha256": guard.CONFIG_SHA256,
    }
    calls = []
    monkeypatch.setattr(guard, "preflight_speech_guard", lambda: {
        "python": "/isolated/python", "model_dir": "/offline/tiny.en",
    })

    def recognize(content, python, model_dir):
        calls.append((content, python, model_dir))
        return copy.deepcopy(result)

    monkeypatch.setattr(guard, "_recognize", recognize)
    return result, calls


@pytest.mark.parametrize("transcript", [
    "Hey Jerrick!", "hey jerrick", "HEY JERRICK.", "  Hey, Jerrick!  ", "Hey\nJerrick.",
])
def test_exact_greeting_accepts_only_case_whitespace_punctuation_variations(audio, recognized, transcript):
    result, calls = recognized
    result["transcript"] = transcript
    proof = guard.verify_greeting_words(audio, "Jerrick")
    assert guard.validate_speech_proof(proof, first_name="Jerrick", audio_sha256=hashlib.sha256(audio).hexdigest())
    assert calls == [(audio, "/isolated/python", "/offline/tiny.en")]


@pytest.mark.parametrize("transcript", [
    "Hey Jim!", "Hey Jared!", "Hey Eric!", "Hey Jerrick and Jim!", "Hey Jerrick, it's Yoni.",
    "Hey Jim, it's Yoni Cutler with Chris Short Sales.", "Hey Jerrick 2", "Hey Jerrick 2026",
    "Hey Jerrick one", "Hey Jerrick again", "Hi Jerrick", "Jerrick", "Hey", "", "   ",
    "Hey Jerrick! Hey Jerrick!", "Hey Jerrick@example.com", "Hey Jerrick https://example.com",
])
def test_wrong_name_reference_leakage_extra_words_numbers_or_empty_rejected(audio, recognized, transcript):
    result, calls = recognized
    result["transcript"] = transcript
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_greeting_words(audio, "Jerrick")
    assert len(calls) == 1


@pytest.mark.parametrize("first_name,transcript", [
    ("Sue", "Hey Sue!"), ("Christopher", "Hey Christopher!"), ("Jennifer", "Hey Jennifer!"),
    ("Anne-Marie", "Hey Anne-Marie!"), ("D'Andre", "Hey D'Andre!"),
])
def test_distinct_valid_names_bind_to_the_requested_name(audio, recognized, first_name, transcript):
    result, _ = recognized
    result["transcript"] = transcript
    proof = guard.verify_greeting_words(audio, first_name)
    assert guard.validate_speech_proof(proof, first_name=first_name, audio_sha256=hashlib.sha256(audio).hexdigest())
    assert not guard.validate_speech_proof(proof, first_name="Jim", audio_sha256=hashlib.sha256(audio).hexdigest())


@pytest.mark.parametrize("first_name,transcript,observed", [
    ("Kathryn", "Hey, Katherine.", "hey katherine"),
    ("Katherine", "Hey Kathryn!", "hey kathryn"),
    ("Nikki", "Hey, Nicky!", "hey nicky"),
    ("Nicky", "HEY NIKKI.", "hey nikki"),
])
def test_only_pinned_spelling_groups_get_v2_proof_without_rewriting_asr(
        audio, recognized, first_name, transcript, observed):
    result, calls = recognized
    result["transcript"] = transcript
    proof = guard.verify_greeting_words(audio, first_name)
    assert proof["policy_version"] == guard.POLICY_VERSION
    assert proof["config_sha256"] == guard.CONFIG_SHA256
    assert proof["normalized_transcript"] == observed
    assert proof["expected_name"] == first_name
    assert proof["normalized_transcript"] != "hey " + first_name.casefold()
    assert guard.validate_speech_proof(proof, first_name=first_name,
        audio_sha256=hashlib.sha256(audio).hexdigest())
    assert len(calls) == 1


def test_exact_v1_policy_and_decoder_identity_remain_unchanged(audio, recognized):
    assert guard.LEGACY_PROOF_IDENTITY == {
        "policy_version": "exact-greeting-tiny-en-v1",
        "model_id": "Systran/faster-whisper-tiny.en",
        "model_revision": "0d3d19a32d3338f10357c0889762bd8d64bbdeba",
        "model_manifest_sha256": "9f995ad3147f0799b2babb9e84926a53bca82d888d5fb6558d925f38dd2b78ee",
        "config_sha256": "f1a2b9866f0576c6500f760d0b58b7a29ed8acf9d337aacc0c66c134ad39e9ca",
    }
    proof = guard.verify_greeting_words(audio, "Jerrick")
    assert proof["policy_version"] == guard.POLICY_VERSION
    assert proof["config_sha256"] == guard.CONFIG_SHA256
    assert proof["policy_version"] != guard.LEGACY_PROOF_IDENTITY["policy_version"]
    assert proof["config_sha256"] != guard.LEGACY_PROOF_IDENTITY["config_sha256"]
    proof.update(guard.LEGACY_PROOF_IDENTITY)
    assert guard.validate_speech_proof(proof, first_name="Jerrick",
        audio_sha256=hashlib.sha256(audio).hexdigest())
    assert guard.SPELLING_EQUIVALENCE_GROUPS == (("kathryn", "katherine"), ("nikki", "nicky"))


@pytest.mark.parametrize("first_name,transcript", [
    ("Kathryn", "Hey Kathleen."), ("Kathryn", "Hey Kaitlyn."),
    ("Kathryn", "Hey Catherine."),
    ("Kathryn", "Hey Karen."), ("Nikki", "Hey Mickey."),
    ("Nikki", "Hey Nick."), ("Nikki", "Hey Nicole."),
    ("Nikki", "Hey Katherine."), ("Kathryn", "Hey Nicky."),
    ("Jacqueline", "Hey Jack Lim."), ("Sibelys", "Hey Sibyl."),
    ("Jerrick", "Hey Jim."), ("Marc", "Hey Mark."),
    ("Cydney", "Hey Sydney."), ("Ken", "A Ken."),
    ("Kathryn", "Hey Katherine and Kathryn."),
    ("Nikki", "Hey Nicky, it's Yoni."),
])
def test_spelling_policy_is_not_fuzzy_matching_nicknames_or_extra_words(
        audio, recognized, first_name, transcript):
    result, calls = recognized
    result["transcript"] = transcript
    with pytest.raises(guard.SpeechGuardError, match="speech_guard_name_mismatch"):
        guard.verify_greeting_words(audio, first_name)
    assert len(calls) == 1


@pytest.mark.parametrize("segments", [
    [{"avg_logprob": -1.00001, "no_speech_prob": 0.01}],
    [{"avg_logprob": -0.1, "no_speech_prob": 0.60001}],
    [{"avg_logprob": float("nan"), "no_speech_prob": 0.01}],
])
def test_spelling_equivalence_does_not_relax_confidence(audio, recognized, segments):
    result, _ = recognized
    result.update(transcript="Hey, Katherine.", segments=segments)
    with pytest.raises(guard.SpeechGuardError, match="speech_guard_low_confidence"):
        guard.verify_greeting_words(audio, "Kathryn")


@pytest.mark.parametrize("field,value", [
    ("policy_version", "exact-greeting-tiny-en-v1"),
    ("config_sha256", "f1a2b9866f0576c6500f760d0b58b7a29ed8acf9d337aacc0c66c134ad39e9ca"),
    ("config_sha256", "0" * 64),
    ("expected_name", "Nikki"),
    ("normalized_transcript", "hey kathleen"),
    ("normalized_transcript", "hey nicky"),
    ("normalized_transcript", "Hey, Katherine."),
    ("source_audio_sha256", "0" * 64),
])
def test_spelling_proof_cannot_be_laundered_into_v1_or_another_name(
        audio, recognized, field, value):
    result, _ = recognized
    result["transcript"] = "Hey, Katherine."
    proof = guard.verify_greeting_words(audio, "Kathryn")
    proof[field] = value
    assert not guard.validate_speech_proof(proof, first_name="Kathryn",
        audio_sha256=hashlib.sha256(audio).hexdigest())


def test_v1_proof_cannot_claim_an_equivalent_spelling(audio, recognized):
    result, _ = recognized
    result["transcript"] = "Hey, Kathryn."
    proof = guard.verify_greeting_words(audio, "Kathryn")
    proof.update(guard.LEGACY_PROOF_IDENTITY)
    proof["normalized_transcript"] = "hey katherine"
    assert not guard.validate_speech_proof(proof, first_name="Kathryn",
        audio_sha256=hashlib.sha256(audio).hexdigest())


@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("field,value", [
    ("policy_version", "spelling-equivalent-greeting-tiny-en-v2"),
    ("model_id", "Systran/faster-whisper-base.en"),
    ("model_revision", "3d3d5dee26484f91867d81cb899cfcf72b96be6c"),
    ("model_manifest_sha256", "0" * 64),
])
def test_legacy_and_current_complete_model_policy_tuples_cannot_be_mixed(
        audio, recognized, legacy, field, value):
    proof = guard.verify_greeting_words(audio, "Jerrick")
    if legacy:
        proof.update(guard.LEGACY_PROOF_IDENTITY)
    proof[field] = value
    assert not guard.validate_speech_proof(proof, first_name="Jerrick",
        audio_sha256=hashlib.sha256(audio).hexdigest())


@pytest.mark.parametrize("legacy", [True, False])
def test_legacy_and_current_config_policy_crossovers_rejected(audio, recognized, legacy):
    proof = guard.verify_greeting_words(audio, "Jerrick")
    if legacy:
        proof.update(guard.LEGACY_PROOF_IDENTITY)
        proof["config_sha256"] = guard.CONFIG_SHA256
    else:
        proof["config_sha256"] = guard.LEGACY_PROOF_IDENTITY["config_sha256"]
    assert not guard.validate_speech_proof(proof, first_name="Jerrick",
        audio_sha256=hashlib.sha256(audio).hexdigest())


@pytest.mark.parametrize("first_name", ["Alex", "Missy", "Miguel"])
def test_ready_legacy_proof_needs_no_new_recognition_or_generation(audio, monkeypatch, first_name):
    proof = {"accepted": True, **guard.LEGACY_PROOF_IDENTITY,
        "expected_name": first_name, "normalized_transcript": "hey " + first_name.casefold(),
        "source_audio_sha256": hashlib.sha256(audio).hexdigest(),
        "media_sha256": "a" * 64, "greeting_seconds": 1.2,
        "confidence": [{"avg_logprob": -0.1, "no_speech_prob": 0.01}]}

    def unavailable():
        pytest.fail("Stored proof validation must not load a recognizer or contact a provider")

    monkeypatch.setattr(guard, "preflight_speech_guard", unavailable)
    assert guard.validate_speech_proof(proof, first_name=first_name, media_sha256="a" * 64)


def test_spelling_proof_still_binds_final_media(audio, recognized, monkeypatch):
    result, _ = recognized
    result["transcript"] = "Hey, Nicky!"
    monkeypatch.setattr(guard, "_extract_video_greeting", lambda *_: audio)
    proof = guard.verify_video_greeting_words(b"exact video", "Nikki", 1.0)
    media_sha = hashlib.sha256(b"exact video").hexdigest()
    assert guard.validate_speech_proof(proof, first_name="Nikki", media_sha256=media_sha)
    assert not guard.validate_speech_proof(proof, first_name="Nicky", media_sha256=media_sha)
    assert not guard.validate_speech_proof(proof, first_name="Nikki", media_sha256="0" * 64)


@pytest.mark.parametrize("first_name", ["", " ", "Jim 2", "Jim Smith", "123", None])
def test_invalid_target_name_rejected_before_asr(audio, recognized, first_name):
    _, calls = recognized
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_greeting_words(audio, first_name)
    assert calls == []


@pytest.mark.parametrize("segments", [
    [], None, "not a list", [{}], [None],
    [{"avg_logprob": -5, "no_speech_prob": 0.01}],
    [{"avg_logprob": -0.1, "no_speech_prob": 0.95}],
    [{"avg_logprob": float("nan"), "no_speech_prob": 0.01}],
    [{"avg_logprob": -0.1, "no_speech_prob": float("nan")}],
    [{"avg_logprob": float("inf"), "no_speech_prob": 0.01}],
    [{"avg_logprob": -0.1, "no_speech_prob": -1}],
    [{"avg_logprob": "-0.1", "no_speech_prob": 0.01}],
    [{"avg_logprob": -0.1, "no_speech_prob": "0.01"}],
    [{"avg_logprob": True, "no_speech_prob": False}],
    [{"avg_logprob": -0.1, "no_speech_prob": 0.01}, {"avg_logprob": -5, "no_speech_prob": 0.01}],
])
def test_low_confidence_no_speech_and_malformed_segment_metadata_rejected(audio, recognized, segments):
    result, _ = recognized
    result["segments"] = segments
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_greeting_words(audio, "Jerrick")


def test_documented_confidence_boundaries_are_inclusive(audio, recognized):
    result, _ = recognized
    result["segments"] = [{"avg_logprob": -1.0, "no_speech_prob": 0.6}]
    proof = guard.verify_greeting_words(audio, "Jerrick")
    assert guard.validate_speech_proof(proof, first_name="Jerrick", audio_sha256=hashlib.sha256(audio).hexdigest())


@pytest.mark.parametrize("avg_logprob,no_speech_prob", [(-1.00001, 0.1), (-0.1, 0.60001)])
def test_outside_confidence_boundary_rejected(audio, recognized, avg_logprob, no_speech_prob):
    result, _ = recognized
    result["segments"] = [{"avg_logprob": avg_logprob, "no_speech_prob": no_speech_prob}]
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_greeting_words(audio, "Jerrick")


@pytest.mark.parametrize("field,value", [
    ("transcript", None), ("transcript", ["Hey Jerrick"]),
    ("model_revision", "different-model"), ("model_revision", None),
    ("config_sha256", "different-config"), ("config_sha256", None),
])
def test_untrusted_worker_output_schema_and_model_identity_checked(audio, recognized, field, value):
    result, _ = recognized
    result[field] = value
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_greeting_words(audio, "Jerrick")


def test_proof_contains_audio_name_and_policy_bindings(audio, recognized):
    proof = guard.verify_greeting_words(audio, "Jerrick")
    assert proof["expected_name"] == "Jerrick"
    assert proof["source_audio_sha256"] == hashlib.sha256(audio).hexdigest()
    assert proof["policy_version"] == guard.POLICY_VERSION
    assert proof["model_revision"] == guard.MODEL_REVISION
    assert proof["config_sha256"] == guard.CONFIG_SHA256
    assert proof["model_manifest_sha256"] == guard.MODEL_MANIFEST_SHA256


@pytest.mark.parametrize("field,value", [
    ("expected_name", "Jim"), ("source_audio_sha256", "0" * 64),
    ("policy_version", "old-policy"), ("model_revision", "other-model"),
    ("config_sha256", "0" * 64), ("model_manifest_sha256", "0" * 64),
    ("normalized_transcript", "hey jim"),
])
def test_proof_tampering_rejected(audio, recognized, field, value):
    proof = guard.verify_greeting_words(audio, "Jerrick")
    proof[field] = value
    assert not guard.validate_speech_proof(proof, first_name="Jerrick", audio_sha256=hashlib.sha256(audio).hexdigest())


def test_proof_cannot_be_reused_for_different_audio(audio, recognized):
    proof = guard.verify_greeting_words(audio, "Jerrick")
    assert not guard.validate_speech_proof(proof, first_name="Jerrick", audio_sha256=hashlib.sha256(audio + b"changed").hexdigest())


@pytest.mark.parametrize("proof", [None, {}, [], "verified", {"first_name": "Jerrick"}])
def test_malformed_proof_fails_closed(proof):
    assert not guard.validate_speech_proof(proof, first_name="Jerrick", audio_sha256="a" * 64)


def test_missing_runtime_failure_does_not_invoke_recognition(audio, monkeypatch, recognized):
    _, calls = recognized

    def unavailable():
        raise guard.SpeechGuardError("speech_guard_unavailable")

    monkeypatch.setattr(guard, "preflight_speech_guard", unavailable)
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_greeting_words(audio, "Jerrick")
    assert calls == []


def test_recognition_failure_is_not_retried(audio, monkeypatch, recognized):
    calls = []

    def timeout(*args):
        calls.append(args)
        raise guard.SpeechGuardError("speech_guard_timeout")

    monkeypatch.setattr(guard, "_recognize", timeout)
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_greeting_words(audio, "Jerrick")
    assert len(calls) == 1


@pytest.mark.parametrize("confidence", [[], None, [{}], [{"avg_logprob": -5, "no_speech_prob": 0.01}],
    [{"avg_logprob": -0.1, "no_speech_prob": 0.99}],
    [{"avg_logprob": float("nan"), "no_speech_prob": 0.01}]])
def test_proof_confidence_tampering_rejected(audio, recognized, confidence):
    proof = guard.verify_greeting_words(audio, "Jerrick")
    proof["confidence"] = confidence
    assert not guard.validate_speech_proof(proof, first_name="Jerrick", audio_sha256=hashlib.sha256(audio).hexdigest())


def test_final_video_proof_binds_extracted_greeting_and_full_media(audio, monkeypatch, recognized):
    video = b"owner-approved-video-body"
    extracted = []

    def extract(content, seconds):
        extracted.append((content, seconds))
        return audio

    monkeypatch.setattr(guard, "_extract_video_greeting", extract)
    proof = guard.verify_video_greeting_words(video, "Jerrick", 1.0)
    assert extracted == [(video, 1.0)]
    assert proof["media_sha256"] == hashlib.sha256(video).hexdigest()
    assert proof["source_audio_sha256"] == hashlib.sha256(audio).hexdigest()
    assert guard.validate_speech_proof(proof, first_name="Jerrick", media_sha256=hashlib.sha256(video).hexdigest())
    assert not guard.validate_speech_proof(proof, first_name="Jerrick", media_sha256=hashlib.sha256(video + b"changed").hexdigest())


def test_final_video_wrong_name_rejected(audio, monkeypatch, recognized):
    result, _ = recognized
    result["transcript"] = "Hey Jim!"
    monkeypatch.setattr(guard, "_extract_video_greeting", lambda *_: audio)
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_video_greeting_words(b"video", "Jerrick", 1.0)


@pytest.mark.parametrize("seconds", [0, -1, 0.49, 2.51, float("nan"), float("inf"), "1", True])
def test_invalid_video_greeting_window_rejected_before_extraction(monkeypatch, recognized, seconds):
    calls = []
    monkeypatch.setattr(guard, "_extract_video_greeting", lambda *args: calls.append(args) or b"bad")
    with pytest.raises(guard.SpeechGuardError):
        guard.verify_video_greeting_words(b"video", "Jerrick", seconds)
    assert calls == []


@pytest.fixture
def model_files(tmp_path, monkeypatch):
    model = tmp_path / "model"
    model.mkdir()
    hashes = {}
    for filename in guard.MODEL_HASHES:
        content = ("test model artifact " + filename).encode()
        (model / filename).write_bytes(content)
        hashes[filename] = hashlib.sha256(content).hexdigest()
    manifest = {"model_id": guard.MODEL_ID, "revision": guard.MODEL_REVISION, "files": hashes}
    (model / "manifest.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(guard, "MODEL_HASHES", hashes)
    monkeypatch.setattr(guard, "MODEL_MANIFEST", manifest)
    monkeypatch.setattr(guard, "MODEL_MANIFEST_SHA256", guard._digest_json(manifest))
    return model


def test_pinned_model_files_and_manifest_pass(model_files):
    assert guard.verify_model_assets(model_files) == guard.MODEL_MANIFEST_SHA256


@pytest.mark.parametrize("damage", ["missing", "corrupt", "symlink", "manifest_missing", "manifest_changed", "manifest_oversized", "manifest_invalid_json"])
def test_model_missing_or_hash_corruption_fails_closed(model_files, damage):
    target = model_files / "model.bin"
    manifest = model_files / "manifest.json"
    if damage == "missing":
        target.unlink()
    elif damage == "corrupt":
        target.write_bytes(b"not the pinned model")
    elif damage == "symlink":
        other = model_files / "model-copy.bin"
        target.rename(other)
        target.symlink_to(other)
    elif damage == "manifest_missing":
        manifest.unlink()
    elif damage == "manifest_changed":
        data = json.loads(manifest.read_text())
        data["revision"] = "floating-main"
        manifest.write_text(json.dumps(data))
    elif damage == "manifest_oversized":
        manifest.write_text(" " * (guard.MAX_OUTPUT_BYTES + 1))
    else:
        manifest.write_text("{")
    with pytest.raises(guard.SpeechGuardError) as error:
        guard.verify_model_assets(model_files)
    assert error.value.code == "speech_guard_model_mismatch"


@pytest.fixture
def runtime(tmp_path, model_files, monkeypatch):
    python = tmp_path / "python"
    python.write_text("test executable placeholder")
    python.chmod(0o700)
    monkeypatch.setenv("VIDEO_FOLLOWUP_SPEECH_PYTHON", str(python))
    monkeypatch.setenv("VIDEO_FOLLOWUP_SPEECH_MODEL_DIR", str(model_files))
    calls = []

    def child(content, executable, model, *, check=False):
        calls.append((content, executable, model, check))
        return {"ready": True, "model_revision": guard.MODEL_REVISION, "config_sha256": guard.CONFIG_SHA256}

    monkeypatch.setattr(guard, "_run_child", child)
    return python, model_files, calls


def test_preflight_verifies_model_before_check_child(runtime):
    python, model, calls = runtime
    result = guard.preflight_speech_guard()
    assert result["python"] == str(python)
    assert result["model_dir"] == str(model)
    assert result["model_manifest_sha256"] == guard.MODEL_MANIFEST_SHA256
    assert calls == [(b"", python, model, True)]


@pytest.mark.parametrize("damage", ["python_missing", "python_not_executable", "python_relative", "model_relative", "model_corrupt"])
def test_unavailable_runtime_never_starts_child(runtime, monkeypatch, damage):
    python, model, calls = runtime
    if damage == "python_missing":
        python.unlink()
    elif damage == "python_not_executable":
        python.chmod(0o600)
    elif damage == "python_relative":
        monkeypatch.setenv("VIDEO_FOLLOWUP_SPEECH_PYTHON", "relative/python")
    elif damage == "model_relative":
        monkeypatch.setenv("VIDEO_FOLLOWUP_SPEECH_MODEL_DIR", "relative/model")
    else:
        (model / "model.bin").write_bytes(b"corrupt")
    with pytest.raises(guard.SpeechGuardError):
        guard.preflight_speech_guard()
    assert calls == []


def test_preflight_requires_explicit_child_ready(runtime, monkeypatch):
    monkeypatch.setattr(guard, "_run_child", lambda *args, **kwargs: {"ready": "true"})
    with pytest.raises(guard.SpeechGuardError) as error:
        guard.preflight_speech_guard()
    assert error.value.code == "speech_guard_unavailable"


class FakeChild:
    def __init__(self, command, kwargs, *, stdout, stderr=b"", returncode=0, timeout=False):
        self.command, self.kwargs = command, kwargs
        self.stdout, self.stderr = stdout, stderr
        self.returncode = returncode
        self.timeout, self.killed, self.calls = timeout, False, []

    def communicate(self, input=None, timeout=None):
        self.calls.append((input, timeout))
        if self.timeout and not self.killed:
            raise subprocess.TimeoutExpired(self.command, timeout)
        self.kwargs["stdout"].write(self.stdout)
        self.kwargs["stderr"].write(self.stderr)

    def kill(self):
        self.killed = True


@pytest.fixture
def child_factory(monkeypatch):
    children = []
    settings = {"stdout": json.dumps({"ready": True, "model_revision": guard.MODEL_REVISION,
                                      "config_sha256": guard.CONFIG_SHA256}).encode()}

    def popen(command, **kwargs):
        child = FakeChild(command, kwargs, **settings)
        children.append(child)
        return child

    monkeypatch.setattr(guard, "_single_child", lambda: nullcontext())
    monkeypatch.setattr(guard.subprocess, "Popen", popen)
    return settings, children


def test_child_has_offline_clean_environment_and_single_thread_controls(audio, monkeypatch, child_factory):
    _, children = child_factory
    monkeypatch.setenv("FAL_API_KEY", "must-not-reach-asr")
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-reach-asr")
    monkeypatch.setenv("PYTHONPATH", "/must-not-reach-asr")
    guard._run_child(audio, "/isolated/python", "/offline/model")
    child = children[0]
    assert child.command[:3] == ["/isolated/python", "-I", "-B"]
    assert child.command[-2:] == ["--recognize", "/offline/model"]
    assert child.kwargs["start_new_session"] is True
    assert child.kwargs["stdin"] == subprocess.PIPE
    env = child.kwargs["env"]
    assert not {"FAL_API_KEY", "OPENAI_API_KEY", "PYTHONPATH"}.intersection(env)
    for key in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_HUB_DISABLE_TELEMETRY", "PYTHONNOUSERSITE", "PYTHONDONTWRITEBYTECODE",
                "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS"):
        assert env[key] == "1"
    assert env["TOKENIZERS_PARALLELISM"] == "false"
    assert child.calls == [(audio, 30)]
    assert "Jerrick" not in " ".join(child.command)


def test_child_check_uses_same_sandbox_and_explicit_mode(child_factory):
    _, children = child_factory
    guard._run_child(b"", "/isolated/python", "/offline/model", check=True)
    assert children[0].command[:3] == ["/isolated/python", "-I", "-B"]
    assert children[0].command[-2:] == ["--check", "/offline/model"]


def test_child_timeout_kills_and_reaps_once_without_retry(audio, child_factory):
    settings, children = child_factory
    settings["timeout"] = True
    with pytest.raises(guard.SpeechGuardError) as error:
        guard._run_child(audio, "/isolated/python", "/offline/model")
    assert error.value.code == "speech_guard_timeout"
    assert len(children) == 1
    assert children[0].killed is True
    assert children[0].calls == [(audio, 30), (None, None)]


@pytest.mark.parametrize("damage", ["large_stdout", "large_stderr", "invalid_json", "not_object", "model_mismatch", "config_mismatch"])
def test_child_output_bounded_and_identity_checked(audio, child_factory, damage):
    settings, _ = child_factory
    if damage == "large_stdout":
        settings["stdout"] = b"x" * (guard.MAX_OUTPUT_BYTES + 1)
    elif damage == "large_stderr":
        settings["stderr"] = b"x" * (guard.MAX_OUTPUT_BYTES + 1)
    elif damage == "invalid_json":
        settings["stdout"] = b"{"
    elif damage == "not_object":
        settings["stdout"] = b"[]"
    else:
        payload = json.loads(settings["stdout"])
        payload["model_revision" if damage == "model_mismatch" else "config_sha256"] = "wrong"
        settings["stdout"] = json.dumps(payload).encode()
    with pytest.raises(guard.SpeechGuardError) as error:
        guard._run_child(audio, "/isolated/python", "/offline/model")
    assert error.value.code == "speech_guard_output_invalid"


@pytest.mark.parametrize("code,expected", [("speech_guard_dependency_mismatch", "speech_guard_dependency_mismatch"),
                                           ("secret error with private details", "speech_guard_execution_failed")])
def test_failed_child_error_code_is_sanitized(audio, child_factory, code, expected):
    settings, _ = child_factory
    settings.update(stdout=json.dumps({"error": code}).encode(), returncode=1)
    with pytest.raises(guard.SpeechGuardError) as error:
        guard._run_child(audio, "/isolated/python", "/offline/model")
    assert error.value.code == expected
    assert str(error.value) == expected


def test_missing_child_executable_fails_closed(monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError("do not expose local path")

    monkeypatch.setattr(guard, "_single_child", lambda: nullcontext())
    monkeypatch.setattr(guard.subprocess, "Popen", missing)
    with pytest.raises(guard.SpeechGuardError) as error:
        guard._run_child(b"", "/missing/python", "/model")
    assert error.value.code == "speech_guard_unavailable"


def test_single_child_thread_lock_rejects_overlap_then_recovers(tmp_path, monkeypatch):
    monkeypatch.setattr(guard.tempfile, "gettempdir", lambda: str(tmp_path))
    with guard._single_child():
        with pytest.raises(guard.SpeechGuardError) as error:
            with guard._single_child():
                pytest.fail("overlapping child entered")
        assert error.value.code == "speech_guard_busy"
    with guard._single_child():
        pass


def test_single_child_cross_process_lock_is_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(guard.tempfile, "gettempdir", lambda: str(tmp_path))
    with open(tmp_path / "crisp-video-speech-guard.lock", "w") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(guard.SpeechGuardError) as error:
            with guard._single_child():
                pytest.fail("locked child entered")
        assert error.value.code == "speech_guard_busy"
    with guard._single_child():
        pass


def test_child_decoding_has_no_name_hints_and_one_cpu_worker(audio, monkeypatch):
    calls, limits = {}, []
    monkeypatch.setattr(guard, "verify_model_assets", lambda *_: guard.MODEL_MANIFEST_SHA256)
    monkeypatch.setattr(importlib.metadata, "version", lambda package: guard.PACKAGE_VERSIONS[package])
    monkeypatch.setattr(resource, "setrlimit", lambda kind, values: limits.append((kind, values)))
    monkeypatch.setattr(guard.os, "nice", lambda *_: None)
    monkeypatch.setattr(guard.sys, "argv", ["guard.py", "--recognize", "/offline/model"])
    monkeypatch.setattr(guard.sys, "stdin", SimpleNamespace(buffer=io.BytesIO(audio)))

    class Model:
        def __init__(self, *args, **kwargs):
            calls["model"] = (args, kwargs)

        def transcribe(self, content, **kwargs):
            calls["decode"] = kwargs
            return iter([SimpleNamespace(text="Hey Jerrick!", tokens=[1, 2, 3], avg_logprob=-0.1, no_speech_prob=0.01)]), None

    monkeypatch.setitem(sys.modules, "faster_whisper", SimpleNamespace(WhisperModel=Model))
    result = guard._child_main()
    assert result["transcript"] == "Hey Jerrick!"
    assert calls["model"] == (("/offline/model",), {"device": "cpu", "compute_type": "int8",
        "cpu_threads": 1, "num_workers": 1, "local_files_only": True})
    decoding = calls["decode"]
    assert decoding["initial_prompt"] is None and decoding["prefix"] is None and decoding["hotwords"] is None
    assert decoding["condition_on_previous_text"] is False
    assert decoding["temperature"] == 0
    assert decoding["language"] == "en"
    assert decoding["max_new_tokens"] == 64
    assert (resource.RLIMIT_FSIZE, (guard.MAX_OUTPUT_BYTES, guard.MAX_OUTPUT_BYTES)) in limits
    assert (resource.RLIMIT_CPU, (25, 25)) in limits


def test_self_test_requires_silence_to_be_rejected(recognized):
    result, _ = recognized
    result["transcript"] = "Hey Jim!"
    with pytest.raises(guard.SpeechGuardError) as error:
        guard.self_test()
    assert error.value.code == "speech_guard_output_invalid"


@pytest.mark.parametrize("transcript,segments", [
    ("", []), ("No speech", []),
    ("Hey Jim!", [{"avg_logprob": -4, "no_speech_prob": 0.99}]),
])
def test_self_test_negative_control_checks_real_zero_pcm(recognized, transcript, segments):
    result, calls = recognized
    result.update(transcript=transcript, segments=segments)
    proof = guard.self_test()
    assert proof["ready"] is True
    assert proof["silent_control_rejected"] is True
    assert proof["model_revision"] == guard.MODEL_REVISION
    assert proof["config_sha256"] == guard.CONFIG_SHA256
    assert len(calls) == 1
    with wave.open(io.BytesIO(calls[0][0]), "rb") as audio:
        assert (audio.getnchannels(), audio.getsampwidth(), audio.getframerate(), audio.getnframes()) == (1, 2, 16000, 16000)
        assert audio.readframes(16000) == b"\0" * 32000


@pytest.mark.parametrize("code", ["speech_guard_timeout", "speech_guard_execution_failed", "speech_guard_dependency_mismatch"])
def test_self_test_does_not_turn_worker_failure_into_ready(monkeypatch, recognized, code):
    def failure(*args):
        raise guard.SpeechGuardError(code)

    monkeypatch.setattr(guard, "_recognize", failure)
    with pytest.raises(guard.SpeechGuardError) as error:
        guard.self_test()
    assert error.value.code == code


@pytest.fixture
def provisioner(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location("speech_guard_provisioner_test", ROOT / "scripts/provision_video_speech_guard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    calls = []
    root = tmp_path / "isolated-speech-runtime"
    monkeypatch.setattr(module, "provision_model", lambda directory: calls.append(("model", directory)))

    def builder(**kwargs):
        calls.append(("venv_config", kwargs))
        return SimpleNamespace(create=lambda directory: calls.append(("venv_create", directory)))

    monkeypatch.setattr(module.venv, "EnvBuilder", builder)
    monkeypatch.setattr(module.subprocess, "run", lambda command, **kwargs: calls.append(("run", command, kwargs)))
    monkeypatch.setattr(module.sys, "argv", ["provision_video_speech_guard.py", "--root", str(root)])
    return module, root, calls


def test_full_provisioning_runs_decode_self_test_not_only_preflight(provisioner):
    module, root, calls = provisioner
    module.main()
    assert calls[:3] == [
        ("model", root / "model"),
        ("venv_config", {"with_pip": True}),
        ("venv_create", root / "venv"),
    ]
    commands = [call for call in calls if call[0] == "run"]
    assert len(commands) == 2
    _, install, install_options = commands[0]
    assert install[:4] == [str(root / "venv/bin/python"), "-m", "pip", "install"]
    assert "--only-binary=:all:" in install
    assert install[-2:] == ["-r", str(ROOT / "requirements-video-speech-guard.txt")]
    assert install_options == {"check": True, "timeout": 600}
    _, check, check_options = commands[1]
    assert check == [sys.executable, "-c", "from video_followup_speech_guard import self_test; self_test()"]
    assert "preflight_speech_guard" not in check[-1]
    assert check_options["cwd"] == ROOT
    assert check_options["check"] is True and check_options["timeout"] == 60
    assert check_options["env"]["VIDEO_FOLLOWUP_SPEECH_PYTHON"] == str(root / "venv/bin/python")
    assert check_options["env"]["VIDEO_FOLLOWUP_SPEECH_MODEL_DIR"] == str(root / "model")


def test_model_only_provisioning_does_not_install_or_certify_runtime(provisioner, monkeypatch):
    module, root, calls = provisioner
    monkeypatch.setattr(module.sys, "argv", ["provision_video_speech_guard.py", "--root", str(root), "--model-only"])
    module.main()
    assert calls == [("model", root / "model")]


def test_decode_self_test_failure_aborts_provisioning(provisioner, monkeypatch, capsys):
    module, _, calls = provisioner

    def run(command, **kwargs):
        calls.append(("run", command, kwargs))
        if "-c" in command:
            raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(module.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        module.main()
    assert "assets provisioned" not in capsys.readouterr().out
