"""Bounded, local acoustic screening before a paid lip-sync request.

This detects gross silence, clipping, timing and pitch anomalies. It does not
prove speaker identity, pronunciation, exact words or conversational naturalness.
Failure means normal text fallback, not another paid narration attempt.
"""
from __future__ import annotations

import hashlib
import io
import math
import wave

import numpy as np

from video_followup_provider import VideoProviderError


GUARD_VERSION = "crisp-recording-acoustic-v1"
REFERENCE_SHA256 = "5fb066ad56f37021ce47d7ea5d182c2f9f097de4e1d21a3ffd22470f7a5f7387"
REFERENCE_GREETING_SECONDS = 26 / 30
VALID_SAMPLE_RATES = frozenset({16000, 22050, 24000, 32000, 44100, 48000})
MAX_BYTES = 2 * 1024 * 1024
MIN_ACTIVE_RMS = 10 ** (-45 / 20)
MAX_CLIPPED_FRACTION = 0.001
MIN_VOICED_FRAMES = 10
MAX_PITCH_SHIFT_SEMITONES = 3.0


def _bounded_failure_measurements(values):
    if type(values) is not dict:
        return {}
    limits = {"duration_seconds": (0, 10), "clipped_sample_fraction": (0, 1),
              "active_rms_dbfs": (-160, 0), "median_f0_hz": (70, 400),
              "reference_median_f0_hz": (70, 400), "pitch_shift_semitones": (-12, 12),
              "prepared_greeting_seconds": (0.5, 2.5)}
    return {name: values[name] for name, (low, high) in limits.items()
            if type(values.get(name)) in (int, float) and low <= values[name] <= high
            and math.isfinite(values[name])}


def safe_acoustic_failure_diagnostics(exc):
    if type(exc) is not VideoProviderError:
        return {}
    code = getattr(exc, "code", None)
    if type(code) is not str or code != "narration_pitch_outlier":
        return {}
    return _bounded_failure_measurements(getattr(exc, "acoustic_diagnostics", None))


def _fail(code: str, message: str, *, measurements=None):
    error = VideoProviderError(code, message)
    if code == "narration_pitch_outlier" and measurements is not None:
        error.acoustic_diagnostics = _bounded_failure_measurements(measurements)
    raise error


def _decode(content: bytes) -> tuple[np.ndarray, np.ndarray, int]:
    if not isinstance(content, bytes) or not 0 < len(content) <= MAX_BYTES:
        _fail("invalid_narration_audio", "Audio must be a bounded PCM WAV.")
    try:
        with wave.open(io.BytesIO(content), "rb") as stream:
            channels, width, rate, compression, frames = (
                stream.getnchannels(), stream.getsampwidth(), stream.getframerate(),
                stream.getcomptype(), stream.getnframes(),
            )
            if channels not in (1, 2) or width != 2 or rate not in VALID_SAMPLE_RATES or compression != "NONE":
                raise ValueError()
            if not 0 < frames <= 10 * rate:
                raise ValueError()
            raw = stream.readframes(frames)
            if len(raw) != frames * channels * width:
                raise ValueError()
    except (wave.Error, EOFError, ValueError, OverflowError):
        _fail("invalid_narration_audio", "Audio must be complete PCM16 mono or stereo WAV at a supported rate.")
    original = np.frombuffer(raw, dtype="<i2").astype(np.float64).reshape(-1, channels) / 32768
    return original.mean(axis=1), original, rate


def _energy(samples: np.ndarray, original: np.ndarray, rate: int) -> dict:
    # Inspect original channels before downmix or loudnorm can hide clipping.
    clipped = float(np.mean(np.abs(original) >= 0.999))
    if clipped > MAX_CLIPPED_FRACTION:
        _fail("clipped_narration_audio", "Generated speech has excessive raw clipping.")
    block = max(1, round(rate / 100))
    count = math.ceil(len(samples) / block)
    padded = np.pad(samples, (0, count * block - len(samples)))
    rms = np.sqrt(np.mean(padded.reshape(count, block) ** 2, axis=1))
    threshold = max(80 / 32768, float(np.max(rms)) * 0.015)
    active = np.flatnonzero(rms > threshold)
    if not len(active):
        _fail("silent_narration_audio", "Generated speech is silent or too quiet.")
    active_rms = float(np.sqrt(np.mean(rms[active] ** 2)))
    if active_rms < MIN_ACTIVE_RMS:
        _fail("quiet_narration_audio", "Generated speech is too quiet for reliable screening.")
    first = int(active[0]) * block
    last = min(len(samples), (int(active[-1]) + 1) * block)
    return {
        "clipped_sample_fraction": clipped,
        "active_rms_dbfs": float(20 * math.log10(active_rms)),
        "peak_dbfs": float(20 * math.log10(max(float(np.max(np.abs(original))), 1e-12))),
        "first_active_sample": first, "last_active_sample": last,
        "active_span_seconds": (last - first) / rate,
        "active_frame_count": int(len(active)),
    }


def _resample(samples: np.ndarray, rate: int) -> np.ndarray:
    target = 16000
    if rate == target:
        return samples
    if rate > target:
        # A fixed low-pass FIR avoids aliasing when reducing common WAV rates.
        offsets = np.arange(63, dtype=np.float64) - 31
        cutoff = 0.45 * target / rate
        kernel = 2 * cutoff * np.sinc(2 * cutoff * offsets) * np.hamming(63)
        samples = np.convolve(samples, kernel / np.sum(kernel), mode="same")
    points = np.arange(math.floor(len(samples) * target / rate), dtype=np.float64) * rate / target
    return np.interp(points, np.arange(len(samples)), samples)


def _pitch(samples: np.ndarray, rate: int) -> dict:
    """YIN-like periodicity, rejecting uncertain frames instead of guessing F0."""
    audio = _resample(samples, rate)
    target, size, step = 16000, 800, 160
    minimum, maximum = round(target / 400), round(target / 70)
    pitches, periodicities = [], []
    active_frames = 0
    for start in range(0, len(audio) - size + 1, step):
        frame = audio[start:start + size].copy()
        frame -= np.mean(frame)
        if float(np.sqrt(np.mean(frame ** 2))) < 0.004:
            continue
        active_frames += 1
        difference = np.zeros(maximum + 1)
        for lag in range(1, maximum + 1):
            delta = frame[:-lag] - frame[lag:]
            difference[lag] = np.mean(delta ** 2)
        normalized = np.ones_like(difference)
        normalized[1:] = difference[1:] * np.arange(1, maximum + 1) / np.maximum(np.cumsum(difference[1:]), 1e-12)
        candidates = [
            lag for lag in range(minimum + 1, maximum)
            if normalized[lag] < 0.2
            and normalized[lag] < normalized[lag - 1]
            and normalized[lag] <= normalized[lag + 1]
        ]
        if not candidates:
            continue
        lag = candidates[0]
        denominator = normalized[lag - 1] - 2 * normalized[lag] + normalized[lag + 1]
        offset = 0.5 * (normalized[lag - 1] - normalized[lag + 1]) / denominator if denominator else 0.0
        refined = lag + float(np.clip(offset, -0.5, 0.5))
        pitches.append(target / refined)
        periodicities.append(1 - float(normalized[lag]))
    if len(pitches) < MIN_VOICED_FRAMES or len(pitches) / max(active_frames, 1) < 0.15:
        _fail("uncertain_narration_pitch", "Speech has insufficient periodic voice evidence for the acoustic screen.")
    return {
        "median_f0_hz": float(np.median(pitches)),
        "f0_p10_hz": float(np.percentile(pitches, 10)),
        "f0_p90_hz": float(np.percentile(pitches, 90)),
        "voiced_frame_count": len(pitches), "pitch_active_frame_count": active_frames,
        "median_periodicity": float(np.median(periodicities)),
    }


def _quality(samples: np.ndarray, original: np.ndarray, rate: int) -> dict:
    return {"guard_version": GUARD_VERSION, "sample_rate": rate,
            "channels": original.shape[1], "sample_format": "PCM16",
            **_energy(samples, original, rate)}


def inspect_reference(wav_bytes: bytes) -> dict:
    """Require the exact approved real recording; do not accept arbitrary voices."""
    digest = hashlib.sha256(wav_bytes).hexdigest() if isinstance(wav_bytes, bytes) else ""
    if digest != REFERENCE_SHA256:
        _fail("unapproved_voice_reference", "The approved real-recording voice reference changed.")
    samples, original, rate = _decode(wav_bytes)
    duration = len(samples) / rate
    if not 5 <= duration <= 10:
        _fail("invalid_voice_reference_duration", "Voice reference must contain 5 to 10 seconds of the approved recording.")
    quality = _quality(samples, original, rate)
    anchor = samples[:round(REFERENCE_GREETING_SECONDS * rate)]
    quality.update(_pitch(anchor, rate))
    quality.update({"reference_sha256": digest,
                    "pitch_anchor_seconds": REFERENCE_GREETING_SECONDS,
                    "status": "reference_acoustic_screen_passed"})
    return {"duration_seconds": duration, "quality": quality}


def inspect_greeting(wav_bytes: bytes, reference_bytes: bytes) -> dict:
    """Screen before loudnorm/lip-sync; leave name and identity claims to callers."""
    reference = inspect_reference(reference_bytes)
    samples, original, rate = _decode(wav_bytes)
    duration = len(samples) / rate
    if not 0 < duration <= 3:
        _fail("greeting_too_long", "Raw greeting must not exceed three seconds.")
    quality = _quality(samples, original, rate)
    first, last = quality["first_active_sample"], quality["last_active_sample"]
    start = max(0, first - round(0.05 * rate)) if first > 0.2 * rate else 0
    end = min(len(samples), last + round(0.10 * rate)) if len(samples) - last > 0.2 * rate else len(samples)
    # Mirror the compositor's 30 fps padding and four-frame speech tail.
    frames = max(15, math.ceil((end - start) / rate * 30), math.ceil((last - start) / rate * 30) + 4)
    if frames > 75 or quality["active_span_seconds"] < 0.25:
        _fail("invalid_greeting_timing", "Greeting does not fit the validated half-to-two-and-a-half-second window.")
    pitch = _pitch(samples[start:end], rate)
    anchor_hz = reference["quality"]["median_f0_hz"]
    shift = 12 * math.log2(pitch["median_f0_hz"] / anchor_hz)
    if abs(shift) > MAX_PITCH_SHIFT_SEMITONES:
        # The caller never receives audio_qa on rejection; retain only safe measurements.
        _fail("narration_pitch_outlier", "Generated speech differs too much from the recorded greeting's pitch range.",
              measurements={**quality, **pitch, "duration_seconds": duration,
                            "reference_median_f0_hz": anchor_hz, "pitch_shift_semitones": shift,
                            "prepared_greeting_seconds": frames / 30})
    quality.update(pitch)
    quality.update({
        "reference_sha256": REFERENCE_SHA256,
        "reference_median_f0_hz": anchor_hz,
        "pitch_shift_semitones": shift,
        "leading_silence_trim_seconds": start / rate,
        "trailing_silence_trim_seconds": (len(samples) - end) / rate,
        "prepared_greeting_seconds": frames / 30,
        "status": "bounded_acoustic_screen_passed",
        "speaker_identity_verified": False,
        "pronunciation_verified": False,
        "naturalness_verified": False,
    })
    return {"duration_seconds": duration, "quality": quality}
