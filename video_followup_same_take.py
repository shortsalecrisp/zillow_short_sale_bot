"""At-most-once audio greeting + same-recording lip-sync pipeline.

Only the approved real recording supplies video and the voice reference. Every
paid stage is claimed before submission; unknown outcomes are never resubmitted.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import subprocess
import time
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode, urlsplit

import requests
import video_followup_speech_guard as speech_guard

from video_followup import Conflict, SCRIPT_VERSION, iso, normalize_name, utcnow, valid_name
from video_followup_provider import (
    MAX_API_BYTES, REQUEST_TIMEOUT, SubmissionUncertain, VideoProviderError, _public_addresses, _ffmpeg_executable,
)
from video_followup_audio_guard import (
    GUARD_VERSION, inspect_reference, inspect_greeting, safe_acoustic_failure_diagnostics,
)
from video_followup_greeting_normalization import NORMALIZATION_CONFIG, normalize_greeting_audio
from video_followup_same_take_media import prepare_inputs, compose
from video_followup_speech_guard import (
    SpeechGuardError, preflight_speech_guard, verify_greeting_words,
    verify_video_greeting_words, validate_speech_proof, is_transient_speech_failure,
    safe_speech_failure_diagnostics, preflight_pronunciation_guard,
    LEGACY_VERIFICATION_METHOD, PLANNED_AUDIO_VERIFICATION_METHOD,
    PLANNED_AUDIO_VERIFICATION_CONFIG, PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256,
    PLANNED_VERIFICATION_PROFILES, PRONUNCIATION_POLICY_VERSION, PRONUNCIATION_VERIFICATION_METHOD,
    PLANNED_VERIFICATION_POLICIES, POLICY_VERSION, LEGACY_PROOF_IDENTITY,
    GENERAL_PRONUNCIATION_POLICY_VERSION, GENERAL_PRONUNCIATION_VERIFICATION_METHOD,
    REVIEWED_VARIANT_VERIFICATION_METHOD,
)

MASTER_SHA256 = "01efd28101e8b016d51d571cc87d38d13ea623389663bc7a88cf26cce39bd60b"
MODEL = "fal-ai/kling-video/lipsync/audio-to-video"
QUEUE = "https://queue.fal.run/"
REQUEST_BASE = QUEUE + "fal-ai/kling-video/requests/"
TTS_MODEL = "fal-ai/chatterbox/text-to-speech/turbo"
TTS_REQUEST_BASE = QUEUE + "fal-ai/chatterbox/requests/"
REFERENCE_SHA256 = "5fb066ad56f37021ce47d7ea5d182c2f9f097de4e1d21a3ffd22470f7a5f7387"
REFERENCE_KEY = "assets/voice-references/" + REFERENCE_SHA256 + "/real-recording.wav"
TTS_SEED = 4411
TTS_TEMPERATURE = 0.5
TTS_REQUEST_CENTS = 2
LIPSYNC_REQUEST_CENTS = 7
RESERVATION_CENTS = 10
PREPARATION_MAX_ATTEMPTS = 3
PREPARATION_RETRY_SECONDS = 60
PREPARATION_LEASE_SECONDS = 600
PRICE_READ_MAX_ATTEMPTS = 3
PRICING_ENDPOINT = "https://api.fal.ai/v1/models/pricing"
APPROVED_RATES = {TTS_MODEL: ("1000 characters", Decimal("0.02")),
                  MODEL: ("units", Decimal("0.014"))}
# Legacy paid jobs pin this exact shape/hash; new settings require a new profile.
RENDER_CONFIG = {"tts_model": TTS_MODEL, "seed": TTS_SEED, "temperature": TTS_TEMPERATURE,
                 "reference_sha256": REFERENCE_SHA256, "lipsync_model": MODEL,
                 "greeting_pattern": "Hey {first_name}!", "audio_guard": GUARD_VERSION,
                 "tts_request_cents": TTS_REQUEST_CENTS, "lipsync_request_cents": LIPSYNC_REQUEST_CENTS}
RENDER_CONFIG_SHA256 = hashlib.sha256(json.dumps(RENDER_CONFIG, sort_keys=True).encode()).hexdigest()
LEGACY_GENERATOR_PROFILE = "legacy_v1"
CLEAN_BODY_GENERATOR_PROFILE = "clean_body_v1"
BOUNDED_PAD_GENERATOR_PROFILE = "clean_body_bounded_pad_v1"
CLEAN_BODY_REFERENCE_SHA256 = "b6a19ab1886102f471aa06ccb2e0347120c22498321f852a46b8b49efd707e3d"
CLEAN_BODY_REFERENCE_KEY = "assets/voice-references/" + CLEAN_BODY_REFERENCE_SHA256 + "/name-free-recording.wav"
MAX_BYTES = 25 * 1024 * 1024
EVIDENCE_VERSION = "same-take-evidence-v1"
EVIDENCE_CONTRACT = "same-take-evidence-required-v1"
EVIDENCE_FIELDS = ("evidence_contract", "evidence_version", "evidence_sha256")
_RUNTIME_CACHE = {}
_REQUEST_ID = re.compile(r"[a-zA-Z0-9-]{16,64}")
_PIPELINE_ID = re.compile(r"same-take-[a-f0-9]{32}")
_PREPARATION_FAILURE_CODES = frozenset({
    "master_mismatch", "reference_mismatch", "unapproved_voice_reference", "invalid_voice_reference_duration",
    "unsafe_fal_media", "fal_download_failed", "fal_download_empty", "invalid_narration_audio",
    "clipped_narration_audio", "silent_narration_audio", "quiet_narration_audio",
    "uncertain_narration_pitch", "narration_pitch_outlier", "invalid_greeting_timing",
    "greeting_too_long", "narration_exceeds_budget", "invalid_media_bytes", "unapproved_recording",
    "noncanonical_first_name", "media_processing_failed", "invalid_video_frames", "invalid_source_frames",
    "invalid_audio", "silent_greeting", "invalid_source_audio", "invalid_context_timing",
    "input_budget_mismatch", "input_hash_mismatch", "asset_conflict",
    "speech_guard_unavailable", "speech_guard_model_mismatch", "speech_guard_dependency_mismatch",
    "speech_guard_busy", "speech_guard_timeout", "speech_guard_execution_failed",
    "speech_guard_output_invalid", "speech_guard_audio_invalid", "speech_guard_name_invalid",
    "speech_guard_name_mismatch", "speech_guard_low_confidence", "speech_guard_media_invalid",
    "speech_guard_pronunciation_unresolved", "speech_guard_pronunciation_data_invalid",
    "audio_lineage_invalid", "audio_lineage_codec_changed", "audio_lineage_movie_presentation",
    "audio_lineage_movie_timing", "audio_lineage_track_flags", "audio_lineage_fragmented",
    "audio_lineage_track_count", "audio_lineage_media_container", "audio_lineage_timing",
    "audio_lineage_edit_list", "audio_lineage_sound_presentation", "audio_lineage_external_data",
    "audio_lineage_sample_table", "audio_lineage_source_short", "audio_lineage_identity_mismatch",
    "audio_lineage_speech_proof_invalid", "audio_lineage_track_mismatch", "audio_lineage_decoded_mismatch",
})
_PREPARATION_FAILURE_STAGES = frozenset({
    "master_read", "reference_read", "audio_download", "audio_screen", "audio_duration", "media_prepare",
    "prepared_validation", "raw_speech_screen", "speech_screen", "asset_storage", "asset_signing",
    "audio_plan", "audio_normalization",
})
_DIAGNOSTIC_EXCEPTION_TYPES = frozenset({
    "VideoProviderError", "Conflict", "ValueError", "TypeError", "KeyError", "OSError",
    "RuntimeError", "TimeoutError", "MemoryError", "OverflowError", "HTTPError",
    "ConnectionError", "ReadTimeout", "ConnectTimeout", "Timeout", "SpeechGuardError",
})
_PIPELINE_FAILURE_CODES = frozenset({
    "greeting_preparation_failed", "provider_price_not_approved", "narration_submission_unconfirmed",
    "fal_submission_unconfirmed", "paid_stage_lease_expired", "narration_failed", "fal_render_failed",
    "cancelled_before_lipsync", "failed", "uncertain", "cancelled",
})


class _UnpaidPreflightTransient(VideoProviderError):
    """This invocation stopped before its pipeline or paid request was created."""

    uncertain = False

    def __init__(self, error, first_name, job_id):
        if not is_transient_speech_failure(error):
            raise ValueError("Only a transient speech preflight can be retried")
        self.speech_code, self.first_name, self.job_id = error.code, first_name, job_id
        super().__init__("unpaid_preflight_transient", "Local speech preflight is temporarily unavailable.")


def _digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _speech_configuration(method):
    """Materialize the already-approved digest chain, never a new matching policy."""
    s = speech_guard
    recognition = {"policy": s.POLICY_VERSION, "model": s.MODEL_MANIFEST,
        "packages": s.PACKAGE_VERSIONS, "decoding": s.DECODING, "compute_type": "int8",
        "cpu_threads": 1, "num_workers": 1, "minimum_logprob": -1.0, "maximum_no_speech": 0.6,
        "preprocessing": s.ASR_PREPROCESSING, "matching_rule": "whole-greeting-pinned-spelling-groups-v1",
        "equivalent_name_groups": s.SPELLING_EQUIVALENCE_GROUPS}
    reviewed = {"policy": s.PRONUNCIATION_POLICY_VERSION, "recognition_config_sha256": s.CONFIG_SHA256,
        "matching_rule": "whole-greeting-pinned-pronunciation-groups-v1",
        "equivalent_name_groups": s.PRONUNCIATION_EQUIVALENCE_GROUPS}
    general = {"policy": s.GENERAL_PRONUNCIATION_POLICY_VERSION,
        "recognition_config_sha256": s.CONFIG_SHA256,
        "reviewed_matching_config_sha256": s.PRONUNCIATION_CONFIG_SHA256,
        "lexicon_manifest": s.GENERAL_PRONUNCIATION_LEXICON_MANIFEST,
        "matching_config": s.GENERAL_PRONUNCIATION_MATCHING_CONFIG}
    candidates = [(recognition, s.CONFIG_SHA256), (reviewed, s.PRONUNCIATION_CONFIG_SHA256),
        (general, s.GENERAL_PRONUNCIATION_CONFIG_SHA256),
        (s.REVIEWED_VARIANT_CONFIG, s.REVIEWED_VARIANT_CONFIG_SHA256)]
    policy = PLANNED_VERIFICATION_POLICIES[method]
    chain = []
    for config, expected in candidates:
        if _digest_json(config) != expected:
            raise SpeechGuardError("speech_guard_output_invalid")
        chain.append({"config": config, "sha256": expected})
        if config["policy"] == policy:
            break
    if chain[-1]["config"]["policy"] != policy:
        raise SpeechGuardError("speech_guard_output_invalid")
    return json.loads(json.dumps({"policy_version": policy,
        "config_sha256": chain[-1]["sha256"], "configuration_chain": chain,
        "lexicon_manifest": s.GENERAL_PRONUNCIATION_LEXICON_MANIFEST if len(chain) >= 3 else None}))


def _file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


# This probe imports metadata, not the recognizer or application. Hash actual
# installed files, not RECORD claims; inference remains solely in speech_guard.
_RUNTIME_PROBE = r'''
import hashlib, importlib.metadata, json, pathlib, platform, sys
expected = json.loads(sys.stdin.read())
versions, inventory, guards, total = {}, [], {}, 0
def guard(path):
    st = path.stat()
    guards[str(path)] = [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_mode]
for name in sorted(expected):
    dist = importlib.metadata.distribution(name)
    versions[name] = dist.version
    if dist.version != expected[name] or not dist.files:
        raise ValueError("dependency mismatch")
    for entry in sorted(dist.files, key=str):
        if str(entry).endswith((".pyc", ".pyo")) or "__pycache__" in entry.parts:
            continue
        path = pathlib.Path(dist.locate_file(entry)).resolve(strict=True)
        if not path.is_file():
            raise ValueError("package file missing")
        size = path.stat().st_size
        total += size
        if total > 1073741824 or len(inventory) >= 20000:
            raise ValueError("inventory bound")
        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1048576), b""):
                h.update(chunk)
        inventory.append([name, str(entry), size, h.hexdigest()])
        guard(path)
        guard(path.parent)
python = pathlib.Path(sys.executable).resolve(strict=True)
guard(python)
print(json.dumps({"python": {"path": sys.executable, "realpath": str(python),
    "sha256": hashlib.sha256(python.read_bytes()).hexdigest(), "version": platform.python_version()},
    "platform": {"system": platform.system(), "machine": platform.machine()},
    "packages": versions, "package_files_sha256": hashlib.sha256(json.dumps(inventory,
    sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
    "package_file_count": len(inventory), "package_file_bytes": total, "file_guards": guards}))
'''


def _stat_identity(path):
    st = Path(path).stat()
    return [st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_mode]


def _decoder_linkage(ffmpeg, system):
    if system != "Linux":
        return {"mode": "reported-runtime-versions-only", "linux_parity_claim": False}, {}
    result = subprocess.run(["/usr/bin/ldd", ffmpeg], capture_output=True, timeout=10, check=False)
    if len(result.stdout) + len(result.stderr) > 65536:
        raise ValueError()
    raw = result.stdout + result.stderr
    report = raw.decode("utf-8", errors="strict")
    stable_report = "\n".join(sorted(" ".join(re.sub(r"\(0x[0-9a-fA-F]+\)",
        "(address omitted)", line).split()) for line in report.splitlines() if line.strip()))
    report_sha256 = hashlib.sha256(stable_report.encode()).hexdigest()
    if "not found" in report:
        raise ValueError()
    if report.strip() in {"not a dynamic executable", "statically linked"}:
        return {"mode": "linux-static-executable", "report_sha256": report_sha256}, {}
    paths = sorted(set(re.findall(r"(?:^|\s)(/[^\s]+)", report)))
    if result.returncode or not 1 <= len(paths) <= 256:
        raise ValueError()
    files, guards, total = [], {}, 0
    for path in paths:
        resolved = str(Path(path).resolve(strict=True))
        before = _stat_identity(resolved)
        if not Path(resolved).is_file():
            raise ValueError()
        total += before[2]
        if total > 512 * 1024 * 1024:
            raise ValueError()
        files.append({"path": path, "realpath": resolved, "sha256": _file_sha256(resolved)})
        if _stat_identity(resolved) != before:
            raise ValueError()
        guards[resolved] = before
        guards[path] = _stat_identity(path)
        guards[str(Path(path).parent)] = _stat_identity(Path(path).parent)
    return {"mode": "linux-observed-shared-libraries-v1", "files": files,
            "report_sha256": report_sha256}, guards


def _runtime_identity(paths):
    """Observe the exact selected decoder and isolated speech installation."""
    try:
        ffmpeg = str(Path(_ffmpeg_executable()).resolve(strict=True))
        key = (str(paths["python"]), str(paths["model_dir"]), ffmpeg)
        cached = _RUNTIME_CACHE.get(key)
        if cached and all(_stat_identity(path) == identity for path, identity in cached[1].items()):
            return json.loads(json.dumps(cached[0]))
        speech_guard.verify_model_assets(paths["model_dir"])
        result = subprocess.run([paths["python"], "-I", "-B", "-c", _RUNTIME_PROBE],
            input=json.dumps(speech_guard.PACKAGE_VERSIONS).encode(), capture_output=True,
            timeout=30, check=True, env=speech_guard._environment(paths["model_dir"]))
        if len(result.stdout) > 4 * 1024 * 1024 or len(result.stderr) > 65536:
            raise ValueError()
        recognizer = json.loads(result.stdout)
        if (not isinstance(recognizer, dict) or recognizer.get("packages") != speech_guard.PACKAGE_VERSIONS
                or recognizer["python"]["sha256"] != _file_sha256(paths["python"])
                or not re.fullmatch(r"[a-f0-9]{64}", recognizer["package_files_sha256"])
                or type(recognizer["package_file_count"]) is not int or recognizer["package_file_count"] < 1):
            raise ValueError()
        guards = recognizer.pop("file_guards")
        if not isinstance(guards, dict) or not 1 <= len(guards) <= 40000:
            raise ValueError()
        for path, identity in guards.items():
            if not Path(path).is_absolute() or identity != _stat_identity(path):
                raise ValueError()
        for path in [paths["python"], ffmpeg, str(Path(paths["model_dir"]) / "manifest.json"),
                     *(str(Path(paths["model_dir"]) / name) for name in speech_guard.MODEL_HASHES)]:
            guards[path] = _stat_identity(path)
        linkage, library_guards = _decoder_linkage(ffmpeg, recognizer.get("platform", {}).get("system"))
        guards.update(library_guards)
        version = subprocess.run([ffmpeg, "-version"], capture_output=True, timeout=10, check=True)
        if len(version.stdout) > 32768 or len(version.stderr) > 32768:
            raise ValueError()
        version_text = version.stdout.decode("utf-8", errors="strict")
        if not version_text.startswith("ffmpeg version "):
            raise ValueError()
        observed = {"recognizer": recognizer,
            "model": {"path": str(Path(paths["model_dir"]).resolve(strict=True)),
                "manifest": speech_guard.MODEL_MANIFEST,
                "manifest_sha256": speech_guard.MODEL_MANIFEST_SHA256},
            "decoder": {"path": ffmpeg, "sha256": _file_sha256(ffmpeg),
                "version_output": version_text, "version_sha256": hashlib.sha256(version.stdout).hexdigest(),
                "linkage": linkage,
                "fingerprint_scope": "executable-build-runtime-versions-and-observed-linux-library-bytes"}}
        if not all(_stat_identity(path) == identity for path, identity in guards.items()):
            raise ValueError()
        _RUNTIME_CACHE.clear()
        _RUNTIME_CACHE[key] = (observed, guards)
        return json.loads(json.dumps(observed))
    except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
        raise SpeechGuardError("speech_guard_dependency_mismatch") from None


def _source_identity():
    root = Path(__file__).resolve().parent
    revision = os.getenv("RENDER_GIT_COMMIT", "")
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        try:
            revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True,
                timeout=5, check=True).stdout.decode().strip()
        except (OSError, UnicodeError, subprocess.SubprocessError):
            raise SpeechGuardError("speech_guard_dependency_mismatch") from None
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise SpeechGuardError("speech_guard_dependency_mismatch")
    files = ("video_followup_same_take.py", "video_followup_speech_guard.py",
        "video_followup_pronunciation.py", "video_followup_pronunciation_data.py",
        "video_followup_audio_lineage.py", "video_followup_same_take_media.py",
        "video_followup_provider.py", "video_followup_audio_guard.py", "video_followup_greeting_normalization.py")
    return {"reported_revision": revision, "files_sha256": {name: _file_sha256(root / name) for name in files},
        "revision_scope": "reported-release-revision-with-observed-file-hashes"}


def _capture_evidence_descriptor(state, paths):
    if state["verification_method"] in {GENERAL_PRONUNCIATION_VERIFICATION_METHOD, REVIEWED_VARIANT_VERIFICATION_METHOD}:
        lexicon = Path(__file__).resolve().parent / "data/video_pronunciation/unique-phones-v1.json.gz"
        if _file_sha256(lexicon) != speech_guard.GENERAL_PRONUNCIATION_LEXICON_MANIFEST["gzip_sha256"]:
            raise SpeechGuardError("speech_guard_pronunciation_data_invalid")
    return {"version": EVIDENCE_VERSION,
        "identity": {key: state[key] for key in ("id", "job_key", "first_name", "source_sha256", "script_version")},
        "verification": _saved_verification_profile(state), "generator": _saved_generator_profile(state),
        "speech": _speech_configuration(state["verification_method"]),
        "runtime": _runtime_identity(paths), "source": _source_identity()}


def _evidence_binding(state):
    present = set(EVIDENCE_FIELDS).intersection(state)
    if not present:
        return {}
    if (len(present) != len(EVIDENCE_FIELDS) or state.get("evidence_contract") != EVIDENCE_CONTRACT
            or state.get("evidence_version") != EVIDENCE_VERSION
            or not re.fullmatch(r"[a-f0-9]{64}", str(state.get("evidence_sha256", "")))):
        raise SpeechGuardError("speech_guard_output_invalid")
    return {key: state[key] for key in EVIDENCE_FIELDS}


def _verification_profile(method):
    if method == LEGACY_VERIFICATION_METHOD:
        config = {"method": method}
    elif isinstance(method, str) and method in PLANNED_VERIFICATION_PROFILES:
        config = dict(PLANNED_VERIFICATION_PROFILES[method])
    else:
        raise VideoProviderError("verification_method_invalid", "Audio verification method is not approved.")
    return {"verification_method": method, "verification_config": config,
            "verification_config_sha256": _digest_json(config)}


def _saved_verification_profile(state):
    fields = ("verification_method", "verification_config", "verification_config_sha256")
    if not any(field in state for field in fields):
        if "audio_plan_sha256" in state:
            raise VideoProviderError("pipeline_mismatch", "Legacy pipeline cannot contain a planned audio identity.")
        return _verification_profile(LEGACY_VERIFICATION_METHOD)
    if not all(field in state for field in fields):
        raise VideoProviderError("pipeline_mismatch", "Audio verification identity is incomplete.")
    profile = _verification_profile(state["verification_method"])
    if any(state.get(field) != value for field, value in profile.items()):
        raise VideoProviderError("pipeline_mismatch", "Audio verification identity changed.")
    if "audio_plan_sha256" in state and (profile["verification_method"] not in PLANNED_VERIFICATION_PROFILES
            or not re.fullmatch(r"[a-f0-9]{64}", str(state["audio_plan_sha256"]))):
        raise VideoProviderError("pipeline_mismatch", "Planned audio identity changed.")
    return profile


def _speech_policy_kwargs(state):
    method = _saved_verification_profile(state)["verification_method"]
    policy = PLANNED_VERIFICATION_POLICIES.get(method, POLICY_VERSION)
    return {"policy_version": policy} if policy != POLICY_VERSION else {}


def _validate_pipeline_speech_proof(state, proof, **bindings):
    if not validate_speech_proof(proof, first_name=state["first_name"], **bindings):
        return False
    evidence = _evidence_binding(state)
    if any(proof.get(key) != value for key, value in evidence.items()):
        return False
    if not evidence and any(key in proof for key in EVIDENCE_FIELDS):
        return False
    method = _saved_verification_profile(state)["verification_method"]
    if method in PLANNED_VERIFICATION_PROFILES:
        return proof.get("config_sha256") == PLANNED_VERIFICATION_PROFILES[method]["speech_config_sha256"]
    return proof.get("policy_version") in (LEGACY_PROOF_IDENTITY["policy_version"], POLICY_VERSION)


def _lineage_runtime():
    import video_followup_audio_lineage as lineage
    if (lineage.LINEAGE_VERSION != PLANNED_AUDIO_VERIFICATION_CONFIG["lineage_version"]
            or lineage.EXTRACTION_VERSION != PLANNED_AUDIO_VERIFICATION_CONFIG["extraction_version"]):
        raise SpeechGuardError("speech_guard_output_invalid")
    return lineage


def _generator_profile(name):
    if name == LEGACY_GENERATOR_PROFILE:
        return {"name": name, "config": dict(RENDER_CONFIG),
                "render_config_sha256": RENDER_CONFIG_SHA256,
                "reference_key": REFERENCE_KEY, "reference_sha256": REFERENCE_SHA256}
    if name in (CLEAN_BODY_GENERATOR_PROFILE, BOUNDED_PAD_GENERATOR_PROFILE):
        config = {**RENDER_CONFIG, "generator_profile": name,
                  "reference_sha256": CLEAN_BODY_REFERENCE_SHA256,
                  "acoustic_reference_sha256": REFERENCE_SHA256}
        if name == BOUNDED_PAD_GENERATOR_PROFILE:
            config["greeting_normalization"] = json.loads(json.dumps(NORMALIZATION_CONFIG))
        return {"name": name, "config": config,
                "render_config_sha256": hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest(),
                "reference_key": CLEAN_BODY_REFERENCE_KEY, "reference_sha256": CLEAN_BODY_REFERENCE_SHA256}
    raise VideoProviderError("generator_profile_invalid", "Greeting generator profile is not approved.")


def _saved_generator_profile(state):
    # Missing profile fields belong only to the original, exact legacy contract.
    if "generator_profile" not in state:
        if any(field in state for field in ("generator_config", "tts_reference_key", "tts_reference_sha256")):
            raise VideoProviderError("pipeline_mismatch", "Legacy generator identity changed.")
        profile = _generator_profile(LEGACY_GENERATOR_PROFILE)
    else:
        profile = _generator_profile(state["generator_profile"])
        try:
            config_hash = hashlib.sha256(json.dumps(state.get("generator_config"), sort_keys=True).encode()).hexdigest()
        except (TypeError, ValueError):
            raise VideoProviderError("pipeline_mismatch", "Saved generator configuration is invalid.") from None
        if (state.get("generator_config") != profile["config"]
                or config_hash != profile["render_config_sha256"]
                or state.get("tts_reference_key") != profile["reference_key"]
                or state.get("tts_reference_sha256") != profile["reference_sha256"]):
            raise VideoProviderError("pipeline_mismatch", "Saved generator configuration changed.")
    if state.get("render_config_sha256") != profile["render_config_sha256"]:
        raise VideoProviderError("pipeline_mismatch", "Saved generator profile hash changed.")
    return profile


def _preparation_failure_diagnostics(exc, stage, audio_qa):
    code = getattr(exc, "code", None) if isinstance(exc, (VideoProviderError, SpeechGuardError)) else None
    exception_type = type(exc).__name__
    details = {
        "failure_stage": stage if stage in _PREPARATION_FAILURE_STAGES else "unknown_preparation_stage",
        "underlying_code": code if isinstance(code, str) and code in _PREPARATION_FAILURE_CODES else "unclassified_preparation_failure",
        "exception_type": exception_type if exception_type in _DIAGNOSTIC_EXCEPTION_TYPES else "OtherException",
    }
    if stage == "audio_screen" and audio_qa is None:
        measured = safe_acoustic_failure_diagnostics(exc)
        if measured:
            details["measured_audio_qa"] = measured
    if isinstance(audio_qa, dict):
        quality = audio_qa.get("quality")
        values = {**(quality if isinstance(quality, dict) else {}),
                  "duration_seconds": audio_qa.get("duration_seconds")}
        limits = {"duration_seconds": (0, 10), "clipped_sample_fraction": (0, 1),
                  "active_rms_dbfs": (-160, 0), "median_f0_hz": (70, 400),
                  "reference_median_f0_hz": (70, 400), "pitch_shift_semitones": (-12, 12),
                  "prepared_greeting_seconds": (0.5, 2.5)}
        measured = {name: values[name] for name, (low, high) in limits.items()
                    if type(values.get(name)) in (int, float) and low <= values[name] <= high
                    and math.isfinite(values[name])}
        if measured:
            details["measured_audio_qa"] = measured
    speech = safe_speech_failure_diagnostics(exc)
    if speech:
        details["speech_diagnostics"] = speech
    return details


def _failed_status(state):
    result = {"provider_id": state["id"], "status": "failed",
              "failure_code": state.get("failure_code", state["stage"])}
    if result["failure_code"] == "greeting_preparation_failed":
        for field, allowed, fallback in (
                ("failure_stage", _PREPARATION_FAILURE_STAGES, "unknown_preparation_stage"),
                ("underlying_code", _PREPARATION_FAILURE_CODES, "unclassified_preparation_failure"),
                ("exception_type", _DIAGNOSTIC_EXCEPTION_TYPES, "OtherException")):
            value = state.get(field)
            result[field] = value if isinstance(value, str) and value in allowed else fallback
        speech = safe_speech_failure_diagnostics(SpeechGuardError(
            state.get("underlying_code"), diagnostics=state.get("speech_diagnostics")))
        if speech:
            result["speech_diagnostics"] = speech
    return result


def fal_media_url(url, resolve=None):
    try:
        p = urlsplit(url)
        if not (p.scheme == "https" and p.port in (None, 443) and not p.username
                and not p.password and not p.fragment
                and re.fullmatch(r"(?:v\d+[a-z]?\.)?fal\.media", p.hostname or "")
                and p.path.startswith("/files/") and len(url) < 8192
                and not any(ord(c) < 33 or c == "\\" for c in url)):
            raise ValueError()
        if resolve:
            addresses = resolve(p.hostname)
            if not addresses or any(not ipaddress.ip_address(a).is_global for a in addresses):
                raise ValueError()
    except (ValueError, TypeError, OSError):
        raise VideoProviderError("unsafe_fal_media", "Fal output address is not permitted.") from None
    return url


def tts_request_url(request_id, url=None, *, status=False):
    if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
        raise VideoProviderError("tts_request_mismatch", "Narration request ID did not match.")
    suffix = "/status" if status else ""
    canonical = TTS_REQUEST_BASE + request_id + suffix
    if url is None:
        return canonical
    try:
        p = urlsplit(url)
        expected_path = "/fal-ai/chatterbox/requests/" + request_id + suffix
        if not (p.scheme == "https" and p.hostname == "queue.fal.run"
                and p.port in (None, 443) and not p.username and not p.password
                and not p.query and not p.fragment and p.path == expected_path
                and not any(ord(c) < 33 or c == "\\" for c in url)):
            raise ValueError()
    except (ValueError, TypeError):
        raise VideoProviderError("unsafe_tts_request", "Narration polling address is not permitted.") from None
    return "https://queue.fal.run" + p.path


class SameTakeVideoProvider:
    @staticmethod
    def safe_unpaid_preflight_failure(error, *, first_name, job_id):
        code = getattr(error, "speech_code", None)
        if (type(error) is not _UnpaidPreflightTransient or error.first_name != first_name
                or error.job_id != job_id or not isinstance(code, str)
                or code not in {"speech_guard_busy", "speech_guard_timeout", "speech_guard_unavailable",
                                "speech_guard_execution_failed"}):
            return None
        return {"failure_stage": "prepayment_speech_preflight", "underlying_code": code,
                "exception_type": "SpeechGuardError"}

    @staticmethod
    def safe_failure_diagnostics(status):
        if not isinstance(status, dict) or status.get("status") != "failed":
            return {}
        code = status.get("failure_code")
        if not isinstance(code, str) or code not in _PIPELINE_FAILURE_CODES:
            return {"failure_code": "unclassified_provider_failure"}
        if code != "greeting_preparation_failed":
            return {"failure_code": code}
        sanitized = _failed_status({**status, "id": "", "stage": "failed"})
        return {key: sanitized[key] for key in (
            "failure_code", "failure_stage", "underlying_code", "exception_type", "speech_diagnostics")
            if key in sanitized}

    def __init__(self, fal_key, store, master_asset_key, *, tts_key, reference_asset_key=REFERENCE_KEY, session=None,
                 media_session=None, resolver=None, preparer=prepare_inputs, compositor=compose,
                 generator_profile=LEGACY_GENERATOR_PROFILE, verification_method=LEGACY_VERIFICATION_METHOD):
        if (not fal_key or not tts_key or not master_asset_key.startswith("assets/")
                or reference_asset_key != REFERENCE_KEY
                or TTS_REQUEST_CENTS + LIPSYNC_REQUEST_CENTS > RESERVATION_CENTS):
            raise VideoProviderError("same_take_unconfigured", "Same-recording integration is incomplete.")
        self.store, self.master_key = store, master_asset_key
        self.fal_key, self.tts_key, self.reference_key = fal_key, tts_key, reference_asset_key
        self.generator_profile = _generator_profile(generator_profile)["name"]
        self.verification_method = _verification_profile(verification_method)["verification_method"]
        if (self.generator_profile == BOUNDED_PAD_GENERATOR_PROFILE
                and self.verification_method not in {
                    GENERAL_PRONUNCIATION_VERIFICATION_METHOD, REVIEWED_VARIANT_VERIFICATION_METHOD}):
            raise VideoProviderError("verification_method_invalid", "Normalized greetings require the approved final-audio verifier.")
        self.session, self.media_session = session or requests.Session(), media_session or requests.Session()
        if session is None:
            self.session.trust_env = False
        if media_session is None:
            self.media_session.trust_env = False
        self.resolver = resolver or _public_addresses
        self.preparer, self.compositor = preparer, compositor

    def _master(self):
        content, _ = self.store.objects.read(self.master_key)
        if not content or hashlib.sha256(content).hexdigest() != MASTER_SHA256:
            raise VideoProviderError("master_mismatch", "Approved second-recording master did not match.")
        return content

    def _reference(self):
        content, _ = self.store.objects.read(self.reference_key)
        if not content or hashlib.sha256(content).hexdigest() != REFERENCE_SHA256:
            raise VideoProviderError("reference_mismatch", "Approved real-recording voice reference did not match.")
        inspect_reference(content)
        return content

    def _tts_reference(self, profile):
        content, _ = self.store.objects.read(profile["reference_key"])
        if not content or hashlib.sha256(content).hexdigest() != profile["reference_sha256"]:
            raise VideoProviderError("reference_mismatch", "Approved narration conditioning reference did not match.")
        return content

    def _read(self, pipeline_id):
        if not _PIPELINE_ID.fullmatch(str(pipeline_id)):
            raise VideoProviderError("invalid_pipeline", "Same-recording pipeline ID is invalid.")
        state, version = self.store.read("same-take/" + pipeline_id)
        if not state or state.get("source_sha256") != MASTER_SHA256 or state.get("script_version") != SCRIPT_VERSION:
            raise VideoProviderError("pipeline_mismatch", "Same-recording pipeline identity did not match.")
        expected = "same-take-" + hashlib.sha256((SCRIPT_VERSION + ":" + str(state.get("job_key"))).encode()).hexdigest()[:32]
        if (state.get("id") != pipeline_id or pipeline_id != expected
                or not valid_name(state.get("first_name"))
                or normalize_name(state["first_name"]) != state["first_name"]
                or state.get("reserved_cents") != RESERVATION_CENTS
                or state.get("reference_sha256") != REFERENCE_SHA256
                or state.get("greeting_script") != "Hey " + state["first_name"] + "!"):
            raise VideoProviderError("pipeline_mismatch", "Pipeline identity or budget changed.")
        _saved_generator_profile(state)
        _saved_verification_profile(state)
        if (state.get("generator_profile") == BOUNDED_PAD_GENERATOR_PROFILE
                and state.get("verification_method") not in {
                    GENERAL_PRONUNCIATION_VERIFICATION_METHOD, REVIEWED_VARIANT_VERIFICATION_METHOD}):
            raise VideoProviderError("pipeline_mismatch", "Normalized greeting verification identity changed.")
        self._read_evidence(state)
        return state, version

    def _update(self, pipeline_id, fields, expected_stage=None, *, preparation_attempt=None):
        immutable = {"id", "job_key", "first_name", "source_sha256", "script_version",
                "reference_sha256", "render_config_sha256", "reserved_cents", "greeting_script",
                "generator_profile", "generator_config", "tts_reference_key", "tts_reference_sha256",
                "verification_method", "verification_config", "verification_config_sha256", "audio_plan_sha256",
                *EVIDENCE_FIELDS}
        if set(fields).intersection(immutable):
            raise VideoProviderError("pipeline_mismatch", "Pipeline identity is immutable.")
        saved, _ = self._read(pipeline_id)
        def change(old):
            if not old or any(old.get(field) != saved.get(field) for field in immutable):
                raise VideoProviderError("pipeline_mismatch", "Pipeline identity changed during update.")
            _saved_generator_profile(old)
            _saved_verification_profile(old)
            if preparation_attempt is not None and old.get("preparation_attempts") != preparation_attempt:
                return None
            return {**old, **fields} if expected_stage is None or old.get("stage") == expected_stage else None
        return self.store.mutate("same-take/" + pipeline_id, change)

    def _active(self, state):
        job, _ = self.store.read("jobs/" + state["job_key"])
        return bool(job and job.get("script_version") == SCRIPT_VERSION
                    and job.get("status") in {"submitting", "submitted", "processing"}
                    and normalize_name(job.get("first_name")) == state["first_name"])

    def _json(self, method, url, *, headers, payload=None, paid=False):
        try:
            response = self.session.request(method, url, headers=headers, json=payload,
                                            timeout=REQUEST_TIMEOUT, allow_redirects=False)
        except requests.RequestException:
            if paid:
                raise SubmissionUncertain("same-take", "paid_stage_uncertain") from None
            raise VideoProviderError("status_unavailable", "Provider status is temporarily unavailable.") from None
        try:
            if not 200 <= response.status_code < 300:
                if paid:
                    raise SubmissionUncertain("same-take", "paid_stage_unconfirmed")
                transient = response.status_code in {408, 425, 429} or 500 <= response.status_code < 600
                raise VideoProviderError("status_unavailable" if transient else "status_rejected",
                                         "Provider status request was not accepted.")
            if len(response.content) > MAX_API_BYTES:
                if paid:
                    raise SubmissionUncertain("same-take", "paid_response_invalid")
                raise VideoProviderError("status_invalid", "Provider status was invalid.")
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError()
            return value
        except (ValueError, TypeError):
            if paid:
                raise SubmissionUncertain("same-take", "paid_response_invalid") from None
            raise VideoProviderError("status_invalid", "Provider status was invalid.") from None
        finally:
            response.close()

    def _verified_price(self, model, key):
        unit, maximum = APPROVED_RATES[model]
        value = self._json("GET", PRICING_ENDPOINT + "?" + urlencode({"endpoint_id": model}),
            headers={"Authorization": "Key " + key, "Cache-Control": "no-cache"})
        try:
            prices = value["prices"]
            if (not isinstance(prices, list) or len(prices) != 1
                    or value.get("has_more") is not False or value.get("next_cursor") is not None):
                raise ValueError()
            price = prices[0]
            amount = Decimal(str(price["unit_price"]))
            if (price["endpoint_id"] != model or price["currency"] != "USD" or price["unit"] != unit
                    or not amount.is_finite() or not Decimal("0") < amount <= maximum):
                raise ValueError()
        except (ValueError, TypeError, KeyError, InvalidOperation):
            raise VideoProviderError("unapproved_provider_price", "Provider pricing is unknown or exceeds the approved limit.") from None
        return {"endpoint_id": model, "unit": unit, "currency": "USD", "unit_price": str(amount),
                "maximum_unit_price": str(maximum), "verified_at": iso(utcnow())}

    def _price_before_payment(self, pipeline_id, stage, model, key, field):
        try:
            # These are free GETs inside the existing paid-stage claim. Never
            # restart a submitting stage after a crash or uncertain paid POST.
            for attempt in range(1, PRICE_READ_MAX_ATTEMPTS + 1):
                saved = self._update(pipeline_id, {field + "_read_attempts": attempt}, stage)
                if saved["stage"] != stage:
                    raise VideoProviderError("stale_provider_price", "Provider pricing lease changed.")
                try:
                    rate = self._verified_price(model, key)
                    break
                except VideoProviderError as error:
                    if error.code != "status_unavailable" or attempt == PRICE_READ_MAX_ATTEMPTS:
                        raise
                    time.sleep(attempt)
            saved = self._update(pipeline_id, {field: rate}, stage)
            age = (utcnow() - datetime.fromisoformat(rate["verified_at"].replace("Z", "+00:00"))).total_seconds()
            if saved["stage"] != stage or not 0 <= age <= 60:
                raise VideoProviderError("stale_provider_price", "Fresh provider pricing is required before payment.")
            return rate
        except Exception:
            self._update(pipeline_id, {"stage": "failed", "failure_code": "provider_price_not_approved"}, stage)
            raise

    def submit(self, first_name, job_id):
        first_name = normalize_name(first_name)
        if not valid_name(first_name) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", job_id):
            raise VideoProviderError("invalid_identity", "Verified first name and job identity are required.")
        pipeline_id = "same-take-" + hashlib.sha256((SCRIPT_VERSION + ":" + job_id).encode()).hexdigest()[:32]
        old, _ = self.store.read("same-take/" + pipeline_id)
        if old:
            old, _ = self._read(pipeline_id)
            if old.get("first_name") != first_name or old.get("job_key") != job_id:
                raise VideoProviderError("identity_changed", "Existing greeting belongs to a different name.")
            return pipeline_id
        profile = _generator_profile(self.generator_profile)
        verification = _verification_profile(self.verification_method)
        if self.verification_method in PLANNED_VERIFICATION_PROFILES:
            _lineage_runtime()
        if self.verification_method in {
                GENERAL_PRONUNCIATION_VERIFICATION_METHOD, REVIEWED_VARIANT_VERIFICATION_METHOD}:
            preflight_pronunciation_guard()
        self._master()
        self._reference()
        self._tts_reference(profile)
        # Local verification policy is independent of the paid render identity.
        # Existing requests above are reused; a missing verifier blocks new spend.
        try:
            speech_paths = preflight_speech_guard()
        except SpeechGuardError as error:
            if not is_transient_speech_failure(error):
                raise
            # Another same-key caller may have passed preflight while this one
            # was busy. Reuse its immutable pipeline, never reset or resubmit it.
            concurrent, _ = self.store.read("same-take/" + pipeline_id)
            if concurrent:
                concurrent, _ = self._read(pipeline_id)
                if concurrent.get("first_name") != first_name or concurrent.get("job_key") != job_id:
                    raise VideoProviderError("identity_changed", "Existing greeting belongs to a different name.")
                return pipeline_id
            raise _UnpaidPreflightTransient(error, first_name, job_id) from None
        state = {"id": pipeline_id, "job_key": job_id, "first_name": first_name,
                 "source_sha256": MASTER_SHA256, "script_version": SCRIPT_VERSION,
                 "stage": "narration_submitting", "created_at": iso(utcnow()),
                 "reference_sha256": REFERENCE_SHA256, "render_config_sha256": profile["render_config_sha256"],
                 "generator_profile": profile["name"], "generator_config": profile["config"],
                 "tts_reference_key": profile["reference_key"], "tts_reference_sha256": profile["reference_sha256"],
                 "reserved_cents": RESERVATION_CENTS, "greeting_script": f"Hey {first_name}!", **verification}
        if self.verification_method in PLANNED_VERIFICATION_PROFILES:
            descriptor = _capture_evidence_descriptor(state, speech_paths)
            content = json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
            if len(content) > 64000:
                raise SpeechGuardError("speech_guard_output_invalid")
            self._asset(pipeline_id, "evidence.json", content, "application/json")
            state.update(evidence_contract=EVIDENCE_CONTRACT, evidence_version=EVIDENCE_VERSION,
                         evidence_sha256=hashlib.sha256(content).hexdigest())
        # The observed minimum TTS billing block is 1,000 characters (2 cents).
        if len(state["greeting_script"]) > 1000:
            raise VideoProviderError("tts_exceeds_budget", "Greeting exceeds the approved narration billing block.")
        reference_url = self.store.objects.signed_get_url(profile["reference_key"])
        if not self._active(state):
            raise VideoProviderError("inactive_job", "Video job is no longer active.")
        self.store.write("same-take/" + pipeline_id, state, 0)
        self._price_before_payment(pipeline_id, "narration_submitting", TTS_MODEL, self.tts_key, "tts_price")
        if not self._active(state):
            self._update(pipeline_id, {"stage": "cancelled"}, "narration_submitting")
            return pipeline_id
        try:
            result = self._json("POST", QUEUE + TTS_MODEL, paid=True,
                headers={"Authorization": "Key " + self.tts_key, "X-Fal-No-Retry": "1",
                         "x-app-fal-disable-fallback": "true", "X-Fal-Request-Start-Timeout": "600"},
                payload={"text": state["greeting_script"], "audio_url": reference_url,
                         "seed": profile["config"]["seed"], "temperature": profile["config"]["temperature"]})
            narration_id = result.get("request_id")
            if not isinstance(narration_id, str) or not _REQUEST_ID.fullmatch(narration_id):
                raise SubmissionUncertain(pipeline_id)
            status_url = tts_request_url(narration_id, result.get("status_url"), status=True)
            result_url = tts_request_url(narration_id, result.get("response_url"))
            self._update(pipeline_id, {"stage": "narration_pending", "narration_id": narration_id,
                                      "narration_status_url": status_url, "narration_result_url": result_url},
                         "narration_submitting")
        except Exception:
            self._update(pipeline_id, {"stage": "uncertain", "failure_code": "narration_submission_unconfirmed"}, "narration_submitting")
            raise
        return pipeline_id

    def _asset(self, pipeline_id, kind, content, mime):
        key = f"assets/{pipeline_id}/{kind}"
        try:
            self.store.objects.write(key, content, 0, mime)
        except Conflict:
            old, _ = self.store.objects.read(key)
            if old != content:
                raise VideoProviderError("asset_conflict", "Prepared greeting asset changed.")
        return key

    def _read_evidence(self, state):
        binding = _evidence_binding(state)
        content, _ = self.store.objects.read(f"assets/{state['id']}/evidence.json")
        if content is None and not binding:
            return None
        try:
            if (not binding or not isinstance(content, bytes) or not 0 < len(content) <= 64000
                    or hashlib.sha256(content).hexdigest() != binding["evidence_sha256"]):
                raise ValueError()
            descriptor = json.loads(content)
            expected = {"version": EVIDENCE_VERSION,
                "identity": {key: state[key] for key in ("id", "job_key", "first_name", "source_sha256", "script_version")},
                "verification": _saved_verification_profile(state), "generator": _saved_generator_profile(state),
                "speech": _speech_configuration(state["verification_method"])}
            expected = json.loads(json.dumps(expected))
            if (_digest_json(descriptor) != binding["evidence_sha256"]
                    or set(descriptor) != {*expected, "runtime", "source"}
                    or any(descriptor.get(key) != value for key, value in expected.items())
                    or not isinstance(descriptor["runtime"], dict) or not isinstance(descriptor["source"], dict)):
                raise ValueError()
            return descriptor
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise SpeechGuardError("speech_guard_output_invalid") from None

    def _evidence_runtime(self, state):
        descriptor = self._read_evidence(state)
        if descriptor is None:
            return {}
        actual = _runtime_identity(speech_guard._speech_paths())
        if (actual != descriptor["runtime"]
                or _source_identity().get("files_sha256") != descriptor["source"].get("files_sha256")):
            raise SpeechGuardError("speech_guard_dependency_mismatch")
        return {"ffmpeg": actual["decoder"]["path"]}

    def _read_audio_plan(self, state):
        """Load only create-once, identity-bound assets; never regenerate a plan."""
        key = f"assets/{state['id']}/prepared-plan.json"
        content, _ = self.store.objects.read(key)
        if content is None and "audio_plan_sha256" not in state:
            return None
        try:
            if not isinstance(content, bytes) or not 0 < len(content) <= 128000:
                raise ValueError()
            digest = hashlib.sha256(content).hexdigest()
            if "audio_plan_sha256" in state and digest != state["audio_plan_sha256"]:
                raise ValueError()
            bundle = json.loads(content)
            if (not isinstance(bundle, dict) or _digest_json(bundle) != digest
                    or bundle.get("version") != "frozen-same-take-audio-plan-v1"
                    or bundle.get("pipeline_id") != state["id"]
                    or bundle.get("first_name") != state["first_name"]
                    or bundle.get("verification_config_sha256") != state["verification_config_sha256"]):
                raise ValueError()
            evidence = _evidence_binding(state)
            if (any(bundle.get(key) != value for key, value in evidence.items())
                    or (not evidence and any(key in bundle for key in EVIDENCE_FIELDS))):
                raise ValueError()
            self._read_evidence(state)
            metadata, manifest = bundle["metadata"], bundle["manifest"]
            duration = bundle["narration_duration"]
            if (type(duration) not in {int, float} or not math.isfinite(duration) or not 0 < duration <= 3
                    or not isinstance(bundle["audio_qa"], dict)):
                raise ValueError()
            expected = {"lineage_version": PLANNED_AUDIO_VERIFICATION_CONFIG["lineage_version"],
                "extraction_version": PLANNED_AUDIO_VERIFICATION_CONFIG["extraction_version"],
                "first_name": state["first_name"], "source_sha256": MASTER_SHA256,
                "preparation_sha256": _digest_json(metadata),
                "prepared_greeting_sha256": metadata["greeting_audio_sha256"],
                "greeting_seconds": metadata["greeting_seconds"]}
            if (not isinstance(manifest, dict) or any(manifest.get(k) != v for k, v in expected.items())
                    or bundle["manifest_sha256"] != _digest_json(manifest)
                    or metadata.get("source_sha256") != MASTER_SHA256
                    or metadata.get("first_name") != state["first_name"]
                    or ("metadata" in state and state["metadata"] != metadata)):
                raise ValueError()
            if state.get("generator_profile") == BOUNDED_PAD_GENERATOR_PROFILE:
                raw, _ = self.store.objects.read(f"assets/{state['id']}/raw-narration.wav")
                normalized, _ = self.store.objects.read(f"assets/{state['id']}/normalized-narration.wav")
                try:
                    replay = normalize_greeting_audio(raw)
                except VideoProviderError:
                    raise ValueError() from None
                if (normalized != replay["audio_bytes"]
                        or metadata.get("greeting_normalization") != replay["provenance"]
                        or metadata.get("narration_sha256") != hashlib.sha256(normalized).hexdigest()):
                    raise ValueError()
            for field in ("source_sha256", "preparation_sha256", "prepared_greeting_sha256",
                          "body_pcm_sha256", "joined_pcm_sha256", "planned_audio_sha256", "full_pcm_sha256",
                          "checked_audio_sha256", "greeting_pcm_sha256"):
                if not re.fullmatch(r"[a-f0-9]{64}", str(manifest.get(field, ""))):
                    raise ValueError()
            for field in ("joined_sample_count", "full_sample_count", "greeting_sample_count"):
                if type(manifest.get(field)) is not int or not 0 < manifest[field] <= 48000 * 30:
                    raise ValueError()
            seconds = manifest["greeting_seconds"]
            if (type(seconds) not in {int, float} or not math.isfinite(seconds) or not 0.5 <= seconds <= 2.5
                    or not isinstance(manifest.get("aac_track"), dict)):
                raise ValueError()
            inputs = {"metadata": metadata}
            for field, filename, expected_hash in (
                    ("video_bytes", "input.mp4", metadata["video_sha256"]),
                    ("audio_bytes", "input.wav", metadata["audio_sha256"]),
                    ("greeting_audio_bytes", "greeting.wav", metadata["greeting_audio_sha256"]),
                    ("planned_audio_bytes", "planned.m4a", manifest["planned_audio_sha256"]),
                    ("checked_audio_bytes", "checked-greeting.wav", manifest["checked_audio_sha256"])):
                data, _ = self.store.objects.read(f"assets/{state['id']}/{filename}")
                if (not isinstance(data, bytes) or not 0 < len(data) <= MAX_BYTES
                        or hashlib.sha256(data).hexdigest() != expected_hash):
                    raise ValueError()
                inputs[field] = data
            return {"bundle": bundle, "inputs": inputs, "sha256": digest}
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise SpeechGuardError("speech_guard_audio_invalid") from None

    def _freeze_audio_plan(self, state, inputs, audio_qa, duration):
        plan = _lineage_runtime().prepare_audio_plan(self._master(), inputs["greeting_audio_bytes"],
            inputs["metadata"], first_name=state["first_name"], **self._evidence_runtime(state))
        for filename, content, mime in (
                ("input.mp4", inputs["video_bytes"], "video/mp4"),
                ("input.wav", inputs["audio_bytes"], "audio/wav"),
                ("greeting.wav", inputs["greeting_audio_bytes"], "audio/wav"),
                ("planned.m4a", plan["audio_bytes"], "audio/mp4"),
                ("checked-greeting.wav", plan["greeting_audio_bytes"], "audio/wav")):
            self._asset(state["id"], filename, content, mime)
        bundle = {"version": "frozen-same-take-audio-plan-v1", "pipeline_id": state["id"],
            "first_name": state["first_name"], "verification_config_sha256": state["verification_config_sha256"],
            "metadata": inputs["metadata"], "audio_qa": audio_qa, "narration_duration": duration,
            "manifest": plan["manifest"], "manifest_sha256": plan["manifest_sha256"], **_evidence_binding(state)}
        self._asset(state["id"], "prepared-plan.json",
            json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode(), "application/json")
        return self._read_audio_plan(state)

    def _start_fal(self, state, version, narration):
        pipeline_id = state["id"]
        if not self._active(state):
            return self._update(pipeline_id, {"stage": "cancelled"}, "narration_pending")
        now = utcnow()
        for field in ("preparation_retry_after", "preparation_lease_until"):
            if state.get(field) and now < datetime.fromisoformat(state[field].replace("Z", "+00:00")):
                return state
        attempts = state.get("preparation_attempts", 0)
        if type(attempts) is not int or not 0 <= attempts < PREPARATION_MAX_ATTEMPTS:
            return self._update(pipeline_id, {"stage": "failed",
                "failure_code": "greeting_preparation_failed", "preparation_retry_exhausted": True},
                "narration_pending")
        state = {**state, "preparation_attempts": attempts + 1, "preparation_retry_after": None,
            "preparation_lease_until": iso(now + timedelta(seconds=PREPARATION_LEASE_SECONDS))}
        # A free-work lease prevents concurrent checks from consuming attempts
        # or racing a successful preparation into a terminal failure.
        version = self.store.write("same-take/" + pipeline_id, state, version)
        failure_stage, audio_qa = "master_read", None
        planned = _saved_verification_profile(state)["verification_method"] in PLANNED_VERIFICATION_PROFILES
        raw_speech_proof = None
        try:
            self._evidence_runtime(state)
            cached = self._read_audio_plan(state) if planned else None
            if cached:
                inputs = cached["inputs"]
                audio_qa, duration = cached["bundle"]["audio_qa"], cached["bundle"]["narration_duration"]
            else:
                master = self._master()
                failure_stage = "reference_read"
                reference = self._reference()
                failure_stage = "audio_download"
                audio_source = self._download_fal(narration["media_url"])
                normalization = None
                if state.get("generator_profile") == BOUNDED_PAD_GENERATOR_PROFILE:
                    failure_stage = "audio_normalization"
                    # Retain the original even if a future greeting fails. This
                    # profile never changes an existing pipeline's saved policy.
                    if not isinstance(audio_source, bytes) or not 0 < len(audio_source) <= NORMALIZATION_CONFIG["maximum_wav_bytes"]:
                        raise VideoProviderError("invalid_narration_audio", "Raw greeting exceeds the bounded input size.")
                    self._asset(pipeline_id, "raw-narration.wav", audio_source, "audio/wav")
                    normalized = normalize_greeting_audio(audio_source)
                    audio_source, normalization = normalized["audio_bytes"], normalized["provenance"]
                    self._asset(pipeline_id, "normalized-narration.wav", audio_source, "audio/wav")
                failure_stage = "audio_screen"
                audio_qa = inspect_greeting(audio_source, reference)
                failure_stage = "audio_duration"
                duration = float(audio_qa["duration_seconds"])
                if not math.isfinite(duration) or not 0 < duration <= 3.0:
                    raise VideoProviderError("narration_exceeds_budget", "Narration duration is outside the approved limit.")
                if not planned:
                    failure_stage = "raw_speech_screen"
                    raw_speech_proof = verify_greeting_words(audio_source, state["first_name"])
                    if not _validate_pipeline_speech_proof(state, raw_speech_proof,
                            audio_sha256=hashlib.sha256(audio_source).hexdigest()):
                        raise SpeechGuardError("speech_guard_output_invalid")
                failure_stage = "media_prepare"
                inputs = self.preparer(master, audio_source, first_name=state["first_name"])
                if normalization is not None:
                    if inputs["metadata"].get("narration_sha256") != hashlib.sha256(audio_source).hexdigest():
                        raise VideoProviderError("input_hash_mismatch", "Normalized narration input changed.")
                    inputs["metadata"]["greeting_normalization"] = normalization
            failure_stage = "prepared_validation"
            metadata = inputs["metadata"]
            if (metadata.get("source_sha256") != MASTER_SHA256
                    or metadata.get("first_name") != state["first_name"]
                    or not 2 <= float(metadata.get("context_seconds", 0)) <= 5
                    or TTS_REQUEST_CENTS + LIPSYNC_REQUEST_CENTS > RESERVATION_CENTS):
                raise VideoProviderError("input_budget_mismatch", "Prepared name, source or billable duration did not match.")
            for kind, content_key in (("video", "video_bytes"), ("audio", "audio_bytes"),
                                      ("greeting_audio", "greeting_audio_bytes")):
                if hashlib.sha256(inputs[content_key]).hexdigest() != metadata.get(kind + "_sha256"):
                    raise VideoProviderError("input_hash_mismatch", "Prepared input hash changed.")
            if planned:
                failure_stage = "audio_plan"
                cached = cached or self._freeze_audio_plan(state, inputs, audio_qa, duration)
                inputs = cached["inputs"]
                if "audio_plan_sha256" not in state:
                    # Bind the create-once bundle before ASR: free retries reuse
                    # exactly these AAC bytes instead of re-encoding paid audio.
                    state = {**state, "audio_plan_sha256": cached["sha256"]}
                    version = self.store.write("same-take/" + pipeline_id, state, version)
            failure_stage = "speech_screen"
            self._evidence_runtime(state)
            checked_audio = inputs["checked_audio_bytes"] if planned else inputs["greeting_audio_bytes"]
            speech_proof = verify_greeting_words(checked_audio, state["first_name"], **_speech_policy_kwargs(state))
            self._evidence_runtime(state)
            speech_proof = {**speech_proof, **_evidence_binding(state)}
            if not _validate_pipeline_speech_proof(state, speech_proof,
                    audio_sha256=hashlib.sha256(checked_audio).hexdigest()):
                raise SpeechGuardError("speech_guard_output_invalid")
            failure_stage = "asset_storage"
            video_key = self._asset(pipeline_id, "input.mp4", inputs["video_bytes"], "video/mp4")
            audio_key = self._asset(pipeline_id, "input.wav", inputs["audio_bytes"], "audio/wav")
            greeting_key = self._asset(pipeline_id, "greeting.wav", inputs["greeting_audio_bytes"], "audio/wav")
            failure_stage = "asset_signing"
            video_url = self.store.objects.signed_get_url(video_key)
            audio_url = self.store.objects.signed_get_url(audio_key)
        except Exception as exc:
            retry = is_transient_speech_failure(exc) and state["preparation_attempts"] < PREPARATION_MAX_ATTEMPTS
            active = self._active(state)
            diagnostics = {**_preparation_failure_diagnostics(exc, failure_stage, audio_qa),
                "failure_at": iso(utcnow())}
            fields = {"stage": "narration_pending" if retry and active else "failed" if active else "cancelled",
                "failure_code": "greeting_preparation_retry_pending" if retry and active else "greeting_preparation_failed",
                "preparation_lease_until": None, "preparation_last_failure_at": iso(utcnow()),
                "preparation_retry_after": iso(utcnow() + timedelta(seconds=PREPARATION_RETRY_SECONDS)) if retry and active else None,
                "preparation_retry_exhausted": is_transient_speech_failure(exc) and not retry,
                "last_preparation_failure": diagnostics, **diagnostics}
            self._update(pipeline_id, fields, "narration_pending", preparation_attempt=state["preparation_attempts"])
            # Only durable, identity-checked failure is terminal. A failed write,
            # unreadable state, or competing paid-stage transition still raises.
            latest, _ = self._read(pipeline_id)
            if latest["stage"] == "failed" and latest.get("failure_code") == "greeting_preparation_failed":
                return latest
            if (latest["stage"] == "cancelled" or (latest["stage"] == "narration_pending"
                    and latest.get("preparation_attempts") == state["preparation_attempts"]
                    and latest.get("preparation_retry_after"))):
                return latest
            raise
        # CAS is the paid-stage lease: competing polls cannot pay twice.
        claimed = {**state, "stage": "fal_submitting", "metadata": metadata,
                   "greeting_key": greeting_key, "narration_duration": duration,
                   "estimated_cents": TTS_REQUEST_CENTS + LIPSYNC_REQUEST_CENTS,
                   "audio_qa": audio_qa, "raw_speech_proof": raw_speech_proof,
                   "greeting_speech_proof": speech_proof, "fal_attempt_at": iso(utcnow()),
                   "preparation_lease_until": None, "preparation_retry_after": None,
                   "failure_code": None}
        if planned:
            claimed.pop("raw_speech_proof", None)
        for field in ("failure_code", "failure_stage", "underlying_code", "exception_type", "failure_at",
                      "speech_diagnostics", "measured_audio_qa", "preparation_retry_exhausted"):
            claimed.pop(field, None)
        self.store.write("same-take/" + pipeline_id, claimed, version)
        self._price_before_payment(pipeline_id, "fal_submitting", MODEL, self.fal_key, "lipsync_price")
        if not self._active(claimed):
            return self._update(pipeline_id, {"stage": "cancelled"}, "fal_submitting")
        try:
            result = self._json("POST", QUEUE + MODEL, paid=True,
                headers={"Authorization": "Key " + self.fal_key, "X-Fal-No-Retry": "1",
                         "x-app-fal-disable-fallback": "true", "X-Fal-Request-Start-Timeout": "600"},
                payload={"video_url": video_url, "audio_url": audio_url, "enable_safety_checker": True})
            request_id = result.get("request_id")
            if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
                raise SubmissionUncertain(pipeline_id)
            return self._update(pipeline_id, {"stage": "fal_pending", "fal_request_id": request_id}, "fal_submitting")
        except Exception:
            self._update(pipeline_id, {"stage": "uncertain", "failure_code": "fal_submission_unconfirmed"}, "fal_submitting")
            raise

    def inspect(self, pipeline_id, *, accounting_only=False):
        state, version = self._read(pipeline_id)
        stage = state["stage"]
        if stage in {"narration_submitting", "fal_submitting"}:
            started = datetime.fromisoformat(state.get("fal_attempt_at", state["created_at"]).replace("Z", "+00:00"))
            if (utcnow() - started).total_seconds() < 600:
                return {"provider_id": pipeline_id, "status": "processing"}
            state = self._update(pipeline_id, {"stage": "uncertain", "failure_code": "paid_stage_lease_expired"}, stage)
            return {"provider_id": pipeline_id, "status": "failed" if state["stage"] == "uncertain" else "processing"}
        if stage in {"failed", "uncertain", "cancelled"}:
            return _failed_status(state)
        if stage == "narration_pending":
            if not accounting_only and self._active(state):
                for field in ("preparation_retry_after", "preparation_lease_until"):
                    if state.get(field) and utcnow() < datetime.fromisoformat(state[field].replace("Z", "+00:00")):
                        return {"provider_id": pipeline_id, "status": "processing"}
            headers = {"Authorization": "Key " + self.tts_key}
            narration_status = self._json("GET", tts_request_url(state["narration_id"],
                state.get("narration_status_url"), status=True), headers=headers)
            if narration_status.get("status") in {"IN_QUEUE", "IN_PROGRESS"}:
                return {"provider_id": pipeline_id, "status": "processing"}
            if narration_status.get("status") != "COMPLETED":
                raise VideoProviderError("tts_status_unknown", "Narration returned an unknown state.")
            result = self._json("GET", tts_request_url(state["narration_id"],
                state.get("narration_result_url")), headers=headers)
            audio = result.get("audio")
            narration = {"status": "failed" if result.get("error") or not audio else "completed"}
            if narration["status"] == "completed":
                narration["media_url"] = fal_media_url(audio.get("url") if isinstance(audio, dict) else audio)
            if narration["status"] == "failed":
                state = self._update(pipeline_id, {"stage": "failed", "failure_code": "narration_failed"}, "narration_pending")
            elif narration["status"] == "completed":
                if accounting_only:
                    return {"provider_id": pipeline_id, "status": "failed", "failure_code": "cancelled_before_lipsync"}
                state = self._start_fal(state, version, narration)
            else:
                return {"provider_id": pipeline_id, "status": "processing"}
            return (_failed_status(state) if state["stage"] in {"failed", "cancelled"}
                    else {"provider_id": pipeline_id, "status": "processing"})
        if stage == "fal_pending":
            request_id = state["fal_request_id"]
            if not _REQUEST_ID.fullmatch(request_id):
                raise VideoProviderError("request_mismatch", "Fal request ID did not match.")
            # Provider-returned URLs are never used with Authorization headers.
            headers = {"Authorization": "Key " + self.fal_key}
            result = self._json("GET", REQUEST_BASE + request_id + "/status", headers=headers)
            if result.get("status") in {"IN_QUEUE", "IN_PROGRESS"}:
                return {"provider_id": pipeline_id, "status": "processing"}
            if result.get("status") != "COMPLETED":
                raise VideoProviderError("fal_status_unknown", "Fal returned an unknown state.")
            result = self._json("GET", REQUEST_BASE + request_id, headers=headers)
            if result.get("error") or not isinstance(result.get("video"), dict):
                latest = self._update(pipeline_id, {"stage": "failed", "failure_code": "fal_render_failed"}, "fal_pending")
                return {"provider_id": pipeline_id, "status": "failed" if latest["stage"] == "failed" else "processing"}
            state = self._update(pipeline_id, {"stage": "completed", "media_url": fal_media_url(result["video"].get("url"))}, "fal_pending")
        if state["stage"] == "completed":
            result = {"provider_id": pipeline_id, "status": "completed", "media_url": state["media_url"],
                      "estimated_cents": state["estimated_cents"], "accounting_method": "conservative_reservation"}
            if _saved_verification_profile(state)["verification_method"] in PLANNED_VERIFICATION_PROFILES:
                result.update(verification_method=state["verification_method"],
                    verification_config_sha256=state["verification_config_sha256"])
            return result
        return {"provider_id": pipeline_id, "status": "processing"}

    def _download_fal(self, url):
        fal_media_url(url, self.resolver)
        started, content = time.monotonic(), bytearray()
        try:
            with self.media_session.get(url, stream=True, allow_redirects=False, timeout=REQUEST_TIMEOUT,
                                        headers={"Accept-Encoding": "identity"}) as response:
                if response.status_code != 200:
                    raise ValueError()
                for chunk in response.iter_content(65536):
                    content.extend(chunk)
                    if len(content) > MAX_BYTES or time.monotonic() - started > 90:
                        raise ValueError()
        except (requests.RequestException, ValueError):
            raise VideoProviderError("fal_download_failed", "Fal output download was unavailable or oversized.") from None
        if not content:
            raise VideoProviderError("fal_download_empty", "Fal output was empty.")
        return bytes(content)

    def _planned_final_proof(self, state, media_bytes, cached):
        proof = state.get("greeting_speech_proof")
        manifest = cached["bundle"]["manifest"]
        if (not _validate_pipeline_speech_proof(state, proof,
                audio_sha256=manifest["checked_audio_sha256"])
                or any(key in proof for key in ("verification_method", "audio_lineage", "media_sha256"))):
            raise SpeechGuardError("speech_guard_output_invalid")
        try:
            lineage = _lineage_runtime().verify_final_audio(media_bytes, cached["inputs"]["planned_audio_bytes"],
                manifest, proof, cached["inputs"]["metadata"], first_name=state["first_name"],
                expected_manifest_sha256=cached["bundle"]["manifest_sha256"], **self._evidence_runtime(state))
            self._evidence_runtime(state)
        except VideoProviderError:
            # Deterministic lineage rejection is terminal, never a retryable
            # recognizer outage or permission to publish the provider's audio.
            raise SpeechGuardError("speech_guard_media_invalid") from None
        final = {**proof, "media_sha256": hashlib.sha256(media_bytes).hexdigest(),
            "greeting_seconds": manifest["greeting_seconds"],
            "verification_method": state["verification_method"],
            "verification_config_sha256": state["verification_config_sha256"],
            "audio_lineage": {**lineage, "greeting_seconds": manifest["greeting_seconds"]}}
        if not _validate_pipeline_speech_proof(state, final, media_sha256=final["media_sha256"]):
            raise SpeechGuardError("speech_guard_output_invalid")
        return final

    def prepare_media(self, snapshot, first_name):
        state, _ = self._read(snapshot["provider_id"])
        if state["stage"] != "completed" or normalize_name(first_name) != state["first_name"]:
            raise VideoProviderError("completed_identity_mismatch", "Completed video does not match this agent name.")
        planned = _saved_verification_profile(state)["verification_method"] in PLANNED_VERIFICATION_PROFILES
        if planned:
            if "audio_plan_sha256" not in state:
                raise SpeechGuardError("speech_guard_audio_invalid")
            cached = self._read_audio_plan(state)
            greeting = cached["inputs"]["greeting_audio_bytes"]
            proof = state.get("greeting_speech_proof")
            try:
                self._evidence_runtime(state)
                result = self.compositor(self._master(), self._download_fal(state["media_url"]), greeting,
                    state["metadata"], first_name=state["first_name"],
                    planned_audio_bytes=cached["inputs"]["planned_audio_bytes"])
                self._evidence_runtime(state)
            except VideoProviderError as error:
                if error.code.startswith("audio_lineage_"):
                    raise SpeechGuardError("speech_guard_media_invalid") from None
                raise
            final_proof = self._planned_final_proof(state, result["media_bytes"], cached)
        else:
            greeting, _ = self.store.objects.read(state["greeting_key"])
            greeting_hash = hashlib.sha256(greeting).hexdigest()
            if greeting_hash != state["metadata"].get("greeting_audio_sha256"):
                raise SpeechGuardError("speech_guard_audio_invalid")
            proof = state.get("greeting_speech_proof")
            if not _validate_pipeline_speech_proof(state, proof, audio_sha256=greeting_hash):
                proof = verify_greeting_words(greeting, state["first_name"])
                if not _validate_pipeline_speech_proof(state, proof, audio_sha256=greeting_hash):
                    raise SpeechGuardError("speech_guard_output_invalid")
            result = self.compositor(self._master(), self._download_fal(state["media_url"]), greeting,
                                     state["metadata"], first_name=state["first_name"])
            final_proof = verify_video_greeting_words(result["media_bytes"], state["first_name"],
                                                       state["metadata"].get("greeting_seconds"))
        if not _validate_pipeline_speech_proof(state, final_proof,
                media_sha256=hashlib.sha256(result["media_bytes"]).hexdigest()):
            raise SpeechGuardError("speech_guard_output_invalid")
        result["qa"] = {**result["qa"], "narration_audio": state["audio_qa"],
                        "automated_screen_not_naturalness_guarantee": True}
        self._update(state["id"], {"qa": result["qa"], "greeting_speech_proof": proof,
                                  "final_speech_proof": final_proof}, "completed")
        return {"media_bytes": result["media_bytes"], "poster_bytes": result["poster_bytes"],
                "vtt_bytes": result["captions_bytes"], "duration": result["duration_seconds"],
                "width": 1024, "height": 576, "caption_source": "same_recording_timed", "qa": result["qa"],
                "speech_proof": final_proof}

    def audit_media(self, job, media_bytes):
        state, _ = self._read(job.get("provider_id"))
        if (state["stage"] != "completed" or state["first_name"] != job.get("first_name")
                or state["job_key"] != job.get("followup_key")):
            raise SpeechGuardError("speech_guard_name_mismatch")
        if _saved_verification_profile(state)["verification_method"] in PLANNED_VERIFICATION_PROFILES:
            if "audio_plan_sha256" not in state:
                raise SpeechGuardError("speech_guard_audio_invalid")
            return self._planned_final_proof(state, media_bytes, self._read_audio_plan(state))
        proof = verify_video_greeting_words(media_bytes, state["first_name"],
                                             state["metadata"].get("greeting_seconds"))
        if not _validate_pipeline_speech_proof(state, proof,
                media_sha256=hashlib.sha256(media_bytes).hexdigest()):
            raise SpeechGuardError("speech_guard_output_invalid")
        return proof
