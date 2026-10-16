"""Offline, fail-closed spoken-name screening; never calls a generation API."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import unicodedata
import wave

POLICY_VERSION = "spoken-greeting-tiny-en-v2"
MODEL_ID = "Systran/faster-whisper-tiny.en"
MODEL_REVISION = "0d3d19a32d3338f10357c0889762bd8d64bbdeba"
MODEL_HASHES = {
    "config.json": "14b1b421a90349bc551b881461426b561a874049cb9e4c4864f2ca384f6a7cc5",
    "model.bin": "1a5afae06a4db91c975c9a9d78be5cc110ee4ea022ad57d55492e4550e936b2a",
    "tokenizer.json": "929c5252409436dce1b38a75d1abbcb5e132d170d8e324e4e04ed915fa2d22df",
    "vocabulary.txt": "ff77588746d3a2595d32ab5b69ffd7b95ce2441ac57533cb66fc3eb575a115cf",
}
PACKAGE_VERSIONS = {
    "faster-whisper": "1.2.0", "ctranslate2": "4.8.3", "av": "16.1.0",
    "huggingface-hub": "2.2.0", "tokenizers": "0.23.3", "onnxruntime": "1.31.0",
    "numpy": "2.4.6", "tqdm": "4.70.1", "requests": "2.32.5",
}
MODEL_MANIFEST = {"model_id": MODEL_ID, "revision": MODEL_REVISION, "files": MODEL_HASHES}


def _digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


MODEL_MANIFEST_SHA256 = _digest_json(MODEL_MANIFEST)
# Previously issued proofs remain valid only under their complete original tuple.
LEGACY_PROOF_IDENTITY = {
    "policy_version": "exact-greeting-tiny-en-v1",
    "model_id": "Systran/faster-whisper-tiny.en",
    "model_revision": "0d3d19a32d3338f10357c0889762bd8d64bbdeba",
    "model_manifest_sha256": "9f995ad3147f0799b2babb9e84926a53bca82d888d5fb6558d925f38dd2b78ee",
    "config_sha256": "f1a2b9866f0576c6500f760d0b58b7a29ed8acf9d337aacc0c66c134ad39e9ca",
}
SPELLING_EQUIVALENCE_GROUPS = (("kathryn", "katherine"), ("nikki", "nicky"))
ASR_PREPROCESSING = {"silence_padding_ms": 0}
DECODING = {"language": "en", "task": "transcribe", "beam_size": 5,
            "temperature": 0.0, "condition_on_previous_text": False,
            "initial_prompt": None, "prefix": None, "hotwords": None,
            "vad_filter": False, "word_timestamps": False, "max_new_tokens": 64}
CONFIG_SHA256 = _digest_json({"policy": POLICY_VERSION, "model": MODEL_MANIFEST,
    "packages": PACKAGE_VERSIONS, "decoding": DECODING, "compute_type": "int8",
    "cpu_threads": 1, "num_workers": 1, "minimum_logprob": -1.0, "maximum_no_speech": 0.6,
    "preprocessing": ASR_PREPROCESSING, "matching_rule": "whole-greeting-pinned-spelling-groups-v1",
    "equivalent_name_groups": SPELLING_EQUIVALENCE_GROUPS})
LEGACY_VERIFICATION_METHOD = "direct-final-asr-v1"
PLANNED_AUDIO_VERIFICATION_METHOD = "planned-aac-lineage-v1"
PLANNED_AUDIO_VERIFICATION_CONFIG = {
    "method": PLANNED_AUDIO_VERIFICATION_METHOD,
    "lineage_version": "planned-aac-same-take-v1",
    "extraction_version": "ffmpeg-pcm-s16le-mono-16k-v1",
    "speech_config_sha256": CONFIG_SHA256,
    "binding": "immutable-planned-aac-full-pcm-and-greeting-v1",
}
PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256 = _digest_json(PLANNED_AUDIO_VERIFICATION_CONFIG)
MAX_AUDIO_BYTES = 1200000
MAX_MEDIA_BYTES = 25 * 1024 * 1024
MAX_OUTPUT_BYTES = 16384
TIMEOUT_SECONDS = 30
_THREAD_LOCK = threading.Lock()
_CODES = {"speech_guard_" + code for code in (
    "unavailable", "model_mismatch", "dependency_mismatch", "busy", "timeout",
    "execution_failed", "output_invalid", "audio_invalid", "name_invalid",
    "name_mismatch", "low_confidence", "media_invalid")}


class SpeechGuardError(ValueError):
    def __init__(self, code, *, diagnostics=None):
        self.code = code if code in _CODES else "speech_guard_execution_failed"
        self.uncertain = False
        self.diagnostics = _safe_recognition_diagnostics(diagnostics)
        super().__init__(self.code)


def is_transient_speech_failure(error):
    return isinstance(error, SpeechGuardError) and error.code in {
        "speech_guard_busy", "speech_guard_timeout", "speech_guard_unavailable",
        "speech_guard_execution_failed",
    }


def _safe_recognition_diagnostics(value):
    if not isinstance(value, dict):
        return {}
    identity = {"policy_version": POLICY_VERSION, "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION, "model_manifest_sha256": MODEL_MANIFEST_SHA256,
        "config_sha256": CONFIG_SHA256}
    if any(value.get(key) != expected for key, expected in identity.items()):
        return {}
    result = {"accepted": False, **identity}
    for key in ("source_audio_sha256", "media_sha256"):
        if re.fullmatch(r"[a-f0-9]{64}", str(value.get(key, ""))):
            result[key] = value[key]
    # Retain only short recognized speech, never child logs or arbitrary errors.
    text = value.get("observed_transcript")
    if isinstance(text, str) and len(text) <= 128:
        text = " ".join(unicodedata.normalize("NFKC", text).translate(str.maketrans({
            "\u2018": "'", "\u2019": "'", "\u02bc": "'", "\u201c": '"', "\u201d": '"',
        })).split())
        if all(char.isalpha() or char in " ,.!?'\"-" for char in text):
            result["observed_transcript"] = text
            result["normalized_transcript"] = _normalized_phrase(text)
    segments = value.get("confidence")
    if isinstance(segments, list) and len(segments) <= 8:
        confidence = []
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            clean = {key: segment[key] for key, low, high in (
                ("avg_logprob", -100, 0), ("no_speech_prob", 0, 1))
                if type(segment.get(key)) in {int, float}
                and math.isfinite(segment[key]) and low <= segment[key] <= high}
            if clean:
                confidence.append(clean)
        result["confidence"] = confidence
    return result


def safe_speech_failure_diagnostics(error):
    return (_safe_recognition_diagnostics(error.diagnostics)
            if isinstance(error, SpeechGuardError) else {})


def _name(value):
    if not isinstance(value, str):
        raise SpeechGuardError("speech_guard_name_invalid")
    name = unicodedata.normalize("NFKC", value).translate(
        str.maketrans({"\u2018": "'", "\u2019": "'", "\u02bc": "'"})).strip()
    if (not 1 < len(name) <= 32 or not re.fullmatch(r"[^\W\d_]+(?:[-'][^\W\d_]+){0,2}", name)
            or name.casefold() in {"agent", "unknown", "there", "test"}):
        raise SpeechGuardError("speech_guard_name_invalid")
    return name


def _normalized_phrase(text):
    if not isinstance(text, str) or len(text) > 512:
        return None
    text = unicodedata.normalize("NFKC", text).translate(
        str.maketrans({"\u2018": "'", "\u2019": "'", "\u02bc": "'", "\u201c": '"', "\u201d": '"'})).casefold()
    match = re.fullmatch(r'''[\s"'.,!?]*hey[\s,.:;!?]+([^\W\d_]+(?:[-'][^\W\d_]+){0,2})[\s"'.,!?]*''', text)
    return "hey " + match.group(1) if match else None


def _spelling_equivalent_phrase(name, phrase):
    if not isinstance(phrase, str) or phrase != _normalized_phrase(phrase):
        return False
    observed = phrase[4:]
    expected = name.casefold()
    return observed != expected and any(
        expected in group and observed in group for group in SPELLING_EQUIVALENCE_GROUPS)


def _audio_duration(content):
    if not isinstance(content, bytes) or not 0 < len(content) <= MAX_AUDIO_BYTES:
        raise SpeechGuardError("speech_guard_audio_invalid")
    try:
        with wave.open(io.BytesIO(content), "rb") as audio:
            frames, rate = audio.getnframes(), audio.getframerate()
            if (audio.getcomptype() != "NONE" or audio.getsampwidth() != 2
                    or audio.getnchannels() not in {1, 2} or rate not in {8000, 16000, 22050, 24000, 44100, 48000}
                    or not 0 < frames / rate <= 3):
                raise ValueError()
            if len(audio.readframes(frames + 1)) != frames * audio.getnchannels() * 2:
                raise ValueError()
            return frames / rate
    except (ValueError, EOFError, wave.Error, ZeroDivisionError):
        raise SpeechGuardError("speech_guard_audio_invalid") from None


def _hash_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_model_assets(model_dir):
    directory = Path(model_dir)
    try:
        manifest_path = directory / "manifest.json"
        if manifest_path.stat().st_size > MAX_OUTPUT_BYTES:
            raise ValueError()
        if json.loads(manifest_path.read_text()) != MODEL_MANIFEST:
            raise ValueError()
        for filename, expected in MODEL_HASHES.items():
            path = directory / filename
            if not path.is_file() or path.is_symlink() or _hash_file(path) != expected:
                raise ValueError()
    except (OSError, ValueError, TypeError):
        raise SpeechGuardError("speech_guard_model_mismatch") from None
    return MODEL_MANIFEST_SHA256


def _environment(model_dir):
    return {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1", "HF_HUB_DISABLE_TELEMETRY": "1",
        "HF_HOME": str(model_dir), "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
        "TOKENIZERS_PARALLELISM": "false",
        **{key: "1" for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                               "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS")}}


@contextmanager
def _single_child():
    if not _THREAD_LOCK.acquire(blocking=False):
        raise SpeechGuardError("speech_guard_busy")
    descriptor = None
    try:
        descriptor = os.open(str(Path(tempfile.gettempdir()) / "crisp-video-speech-guard.lock"),
                             os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SpeechGuardError("speech_guard_busy") from None
        yield
    except OSError:
        raise SpeechGuardError("speech_guard_unavailable") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        _THREAD_LOCK.release()


def _run_child(content, python, model_dir, *, check=False):
    # -I ignores PYTHON* environment settings; -B prevents writes under RLIMIT_FSIZE.
    command = [str(python), "-I", "-B", str(Path(__file__).resolve()), "--check" if check else "--recognize", str(model_dir)]
    with _single_child(), tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
        try:
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=output, stderr=errors,
                env=_environment(model_dir), start_new_session=True)
            try:
                process.communicate(input=content, timeout=TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise SpeechGuardError("speech_guard_timeout") from None
        except OSError:
            raise SpeechGuardError("speech_guard_unavailable") from None
        if output.tell() > MAX_OUTPUT_BYTES or errors.tell() > MAX_OUTPUT_BYTES:
            raise SpeechGuardError("speech_guard_output_invalid")
        output.seek(0)
        try:
            result = json.loads(output.read(MAX_OUTPUT_BYTES + 1))
        except (ValueError, UnicodeError):
            raise SpeechGuardError("speech_guard_output_invalid") from None
        if not isinstance(result, dict):
            raise SpeechGuardError("speech_guard_output_invalid")
        if process.returncode:
            raise SpeechGuardError(result.get("error", "speech_guard_execution_failed"))
        if result.get("model_revision") != MODEL_REVISION or result.get("config_sha256") != CONFIG_SHA256:
            raise SpeechGuardError("speech_guard_output_invalid")
        return result


def _speech_paths():
    root = Path(__file__).resolve().parent / ".video-speech-guard"
    python = Path(os.getenv("VIDEO_FOLLOWUP_SPEECH_PYTHON", str(root / "venv/bin/python")))
    model_dir = Path(os.getenv("VIDEO_FOLLOWUP_SPEECH_MODEL_DIR", str(root / "model")))
    if not python.is_absolute() or not model_dir.is_absolute() or not python.is_file() or not os.access(python, os.X_OK):
        raise SpeechGuardError("speech_guard_unavailable")
    return {"python": str(python), "model_dir": str(model_dir), "model_manifest_sha256": MODEL_MANIFEST_SHA256}


def preflight_speech_guard():
    paths = _speech_paths()
    python, model_dir = paths["python"], paths["model_dir"]
    verify_model_assets(model_dir)
    result = _run_child(b"", python, model_dir, check=True)
    if result.get("ready") is not True:
        raise SpeechGuardError("speech_guard_unavailable")
    return paths


def _recognize(content, python, model_dir):
    return _run_child(content, python, model_dir)


def _confidence_ok(segments):
    if not isinstance(segments, list) or not 1 <= len(segments) <= 8:
        return False
    for segment in segments:
        if not isinstance(segment, dict):
            return False
        logprob, silence = segment.get("avg_logprob"), segment.get("no_speech_prob")
        if (type(logprob) not in {int, float} or type(silence) not in {int, float}
                or not math.isfinite(logprob) or not math.isfinite(silence)
                or not -1 <= logprob <= 0 or not 0 <= silence <= 0.6):
            return False
    return True


def verify_greeting_words(audio_bytes, first_name):
    name = _name(first_name)
    _audio_duration(audio_bytes)
    # The recognition child validates the same assets and dependencies before
    # decoding. Do not launch a second model-loading child for each audio clip.
    paths = _speech_paths()
    result = _recognize(audio_bytes, paths["python"], paths["model_dir"])
    if result.get("model_revision") != MODEL_REVISION or result.get("config_sha256") != CONFIG_SHA256:
        raise SpeechGuardError("speech_guard_output_invalid")
    phrase = _normalized_phrase(result.get("transcript"))
    rejected = {"policy_version": POLICY_VERSION, "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION, "model_manifest_sha256": MODEL_MANIFEST_SHA256,
        "config_sha256": CONFIG_SHA256, "source_audio_sha256": hashlib.sha256(audio_bytes).hexdigest(),
        "observed_transcript": result.get("transcript"), "confidence": result.get("segments")}
    if phrase != "hey " + name.casefold() and not _spelling_equivalent_phrase(name, phrase):
        raise SpeechGuardError("speech_guard_name_mismatch", diagnostics=rejected)
    if not _confidence_ok(result.get("segments")):
        raise SpeechGuardError("speech_guard_low_confidence", diagnostics=rejected)
    return {"accepted": True, "policy_version": POLICY_VERSION, "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION, "model_manifest_sha256": MODEL_MANIFEST_SHA256,
        "config_sha256": CONFIG_SHA256, "source_audio_sha256": hashlib.sha256(audio_bytes).hexdigest(),
        "expected_name": name, "normalized_transcript": phrase, "confidence": result["segments"]}


def validate_speech_proof(proof, *, first_name, audio_sha256=None, media_sha256=None):
    try:
        name = _name(first_name)
    except SpeechGuardError:
        return False
    if not isinstance(proof, dict):
        return False
    policy, phrase = proof.get("policy_version"), proof.get("normalized_transcript")
    if policy == LEGACY_PROOF_IDENTITY["policy_version"] and phrase == "hey " + name.casefold():
        identity = LEGACY_PROOF_IDENTITY
    elif policy == POLICY_VERSION and (phrase == "hey " + name.casefold() or _spelling_equivalent_phrase(name, phrase)):
        identity = {"policy_version": POLICY_VERSION, "model_id": MODEL_ID,
            "model_revision": MODEL_REVISION, "model_manifest_sha256": MODEL_MANIFEST_SHA256,
            "config_sha256": CONFIG_SHA256}
    else:
        return False
    expected = {"accepted": True, **identity, "expected_name": name, "normalized_transcript": phrase}
    if any(proof.get(key) != value for key, value in expected.items()) or proof.get("accepted") is not True:
        return False
    if not re.fullmatch(r"[a-f0-9]{64}", str(proof.get("source_audio_sha256", ""))) or not _confidence_ok(proof.get("confidence")):
        return False
    if audio_sha256 is not None and proof["source_audio_sha256"] != audio_sha256:
        return False
    if media_sha256 is not None:
        seconds = proof.get("greeting_seconds")
        if (not re.fullmatch(r"[a-f0-9]{64}", str(media_sha256)) or proof.get("media_sha256") != media_sha256
                or type(seconds) not in {int, float} or not math.isfinite(seconds) or not 0.5 <= seconds <= 2.5):
            return False
    lineage_fields = {"verification_method", "verification_config_sha256", "audio_lineage"}
    if any(key in proof for key in lineage_fields):
        if not lineage_fields.issubset(proof) or not _validate_audio_lineage_proof(proof, name):
            return False
    return True


def _validate_audio_lineage_proof(proof, name):
    if (proof.get("verification_method") != PLANNED_AUDIO_VERIFICATION_METHOD
            or proof.get("verification_config_sha256") != PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256
            or proof.get("policy_version") != POLICY_VERSION):
        return False
    lineage = proof.get("audio_lineage")
    if not isinstance(lineage, dict) or lineage.get("accepted") is not True:
        return False
    expected = {"lineage_version": PLANNED_AUDIO_VERIFICATION_CONFIG["lineage_version"],
        "first_name": name, "final_media_sha256": proof.get("media_sha256"),
        "checked_audio_sha256": proof.get("source_audio_sha256"),
        "greeting_seconds": proof.get("greeting_seconds")}
    if any(lineage.get(key) != value for key, value in expected.items()):
        return False
    for key in ("manifest_sha256", "planned_audio_sha256", "final_media_sha256", "checked_audio_sha256",
                "full_pcm_sha256", "greeting_pcm_sha256", "aac_track_sha256", "speech_proof_sha256"):
        if not re.fullmatch(r"[a-f0-9]{64}", str(lineage.get(key, ""))):
            return False
    seconds = proof.get("greeting_seconds")
    if type(seconds) not in {int, float} or not math.isfinite(seconds) or not 0.5 <= seconds <= 2.5:
        return False
    original = {key: value for key, value in proof.items() if key not in {
        "verification_method", "verification_config_sha256", "audio_lineage", "media_sha256", "greeting_seconds"}}
    try:
        return _digest_json(original) == lineage["speech_proof_sha256"]
    except (ValueError, TypeError):
        return False


def _extract_video_greeting(video_bytes, greeting_seconds):
    if (not isinstance(video_bytes, bytes) or not 0 < len(video_bytes) <= MAX_MEDIA_BYTES
            or type(greeting_seconds) not in {int, float} or not math.isfinite(greeting_seconds)
            or not 0.5 <= greeting_seconds <= 2.5):
        raise SpeechGuardError("speech_guard_media_invalid")
    from video_followup_provider import _ffmpeg_executable
    with tempfile.TemporaryDirectory(prefix="video-speech-") as directory:
        source, target = Path(directory) / "input.mp4", Path(directory) / "greeting.wav"
        source.write_bytes(video_bytes)
        command = [_ffmpeg_executable(), "-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "1",
            "-protocol_whitelist", "file,pipe", "-i", str(source), "-t", str(greeting_seconds),
            "-vn", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", "-fs", str(MAX_AUDIO_BYTES), str(target)]
        try:
            subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=TIMEOUT_SECONDS, check=True)
            if target.stat().st_size > MAX_AUDIO_BYTES:
                raise ValueError()
            return target.read_bytes()
        except (OSError, ValueError, subprocess.SubprocessError):
            raise SpeechGuardError("speech_guard_media_invalid") from None


def verify_video_greeting_words(video_bytes, first_name, greeting_seconds):
    _name(first_name)
    if (not isinstance(video_bytes, bytes) or not 0 < len(video_bytes) <= MAX_MEDIA_BYTES
            or type(greeting_seconds) not in {int, float} or not math.isfinite(greeting_seconds)
            or not 0.5 <= greeting_seconds <= 2.5):
        raise SpeechGuardError("speech_guard_media_invalid")
    audio = _extract_video_greeting(video_bytes, greeting_seconds)
    try:
        proof = verify_greeting_words(audio, first_name)
    except SpeechGuardError as error:
        details = safe_speech_failure_diagnostics(error)
        if details:
            details["media_sha256"] = hashlib.sha256(video_bytes).hexdigest()
        raise SpeechGuardError(error.code, diagnostics=details) from None
    return {**proof, "media_sha256": hashlib.sha256(video_bytes).hexdigest(), "greeting_seconds": greeting_seconds}


def _child_main():
    import importlib.metadata
    import resource
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_OUTPUT_BYTES, MAX_OUTPUT_BYTES))
    resource.setrlimit(resource.RLIMIT_CPU, (25, 25))
    try:
        os.nice(10)
    except OSError:
        pass
    if len(sys.argv) != 3 or sys.argv[1] not in {"--check", "--recognize"}:
        raise SpeechGuardError("speech_guard_execution_failed")
    verify_model_assets(sys.argv[2])
    try:
        if any(importlib.metadata.version(package) != version for package, version in PACKAGE_VERSIONS.items()):
            raise ValueError()
        from faster_whisper import WhisperModel
    except (ImportError, ValueError, importlib.metadata.PackageNotFoundError):
        raise SpeechGuardError("speech_guard_dependency_mismatch") from None
    model = WhisperModel(sys.argv[2], device="cpu", compute_type="int8", cpu_threads=1,
                         num_workers=1, local_files_only=True)
    base = {"model_revision": MODEL_REVISION, "config_sha256": CONFIG_SHA256}
    if sys.argv[1] == "--check":
        return {**base, "ready": True}
    audio = sys.stdin.buffer.read(MAX_AUDIO_BYTES + 1)
    _audio_duration(audio)
    segments, _ = model.transcribe(io.BytesIO(audio), **DECODING)
    segments = list(segments)
    if len(segments) > 8 or sum(len(segment.text) for segment in segments) > 512:
        raise SpeechGuardError("speech_guard_output_invalid")
    # Reject exhausted decoding rather than trusting a potentially truncated prefix.
    if any(len(segment.tokens) >= DECODING["max_new_tokens"] for segment in segments):
        raise SpeechGuardError("speech_guard_output_invalid")
    return {**base, "transcript": " ".join(segment.text.strip() for segment in segments),
        "segments": [{"avg_logprob": segment.avg_logprob, "no_speech_prob": segment.no_speech_prob} for segment in segments]}


def self_test():
    """Offline readiness and a negative control, not a pronunciation benchmark."""
    paths = preflight_speech_guard()
    stream = io.BytesIO()
    with wave.open(stream, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(16000)
        audio.writeframes(b"\0" * 32000)
    result = _recognize(stream.getvalue(), paths["python"], paths["model_dir"])
    if _normalized_phrase(result.get("transcript")) is not None and _confidence_ok(result.get("segments")):
        raise SpeechGuardError("speech_guard_output_invalid")
    return {"ready": True, "silent_control_rejected": True, "policy_version": POLICY_VERSION,
            "model_revision": MODEL_REVISION, "model_manifest_sha256": MODEL_MANIFEST_SHA256,
            "config_sha256": CONFIG_SHA256}


if __name__ == "__main__":
    try:
        answer = self_test() if sys.argv[1:] == ["--self-test"] else _child_main()
    except Exception as error:
        print(json.dumps({"error": getattr(error, "code", "speech_guard_execution_failed")}))
        raise SystemExit(1)
    print(json.dumps(answer, allow_nan=False))
