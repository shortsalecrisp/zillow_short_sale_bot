import hashlib
import io
import math
import wave

import numpy as np
import pytest

import video_followup_audio_guard as guard
from video_followup_provider import VideoProviderError


def wav(samples, rate=24000, channels=1, width=2):
    stream = io.BytesIO()
    with wave.open(stream, "wb") as output:
        output.setnchannels(channels)
        output.setsampwidth(width)
        output.setframerate(rate)
        output.writeframes(samples.astype("<i2").tobytes())
    return stream.getvalue()


def tone(seconds, hz=221, rate=24000, amplitude=8000, leading=0, trailing=0):
    audio = np.sin(np.arange(round(seconds * rate)) * 2 * math.pi * hz / rate) * amplitude
    return np.pad(audio.astype(np.int16), (round(leading * rate), round(trailing * rate)))


@pytest.fixture
def reference(monkeypatch):
    content = wav(tone(6))
    monkeypatch.setattr(guard, "REFERENCE_SHA256", hashlib.sha256(content).hexdigest())
    return content


def raises(code, callback):
    with pytest.raises(VideoProviderError) as error:
        callback()
    assert error.value.code == code


def test_reference_requires_exact_approved_bytes(reference):
    raises("unapproved_voice_reference", lambda: guard.inspect_reference(reference + b"x"))


@pytest.mark.parametrize("seconds", [4.99, 10.01])
def test_reference_duration_bounds(monkeypatch, seconds):
    content = wav(tone(seconds))
    monkeypatch.setattr(guard, "REFERENCE_SHA256", hashlib.sha256(content).hexdigest())
    expected = "invalid_voice_reference_duration" if seconds < 5 else "invalid_narration_audio"
    raises(expected, lambda: guard.inspect_reference(content))


def test_reference_reports_hash_and_pitch(reference):
    result = guard.inspect_reference(reference)
    assert result["duration_seconds"] == 6
    assert result["quality"]["median_f0_hz"] == pytest.approx(221, abs=1)
    assert result["quality"]["reference_sha256"] == hashlib.sha256(reference).hexdigest()


@pytest.mark.parametrize("rate", sorted(guard.VALID_SAMPLE_RATES))
def test_supported_pcm_rates_have_consistent_pitch(reference, rate):
    result = guard.inspect_greeting(wav(tone(0.7, hz=216, rate=rate), rate), reference)
    assert result["quality"]["median_f0_hz"] == pytest.approx(216, abs=1)
    assert result["quality"]["pitch_shift_semitones"] == pytest.approx(-0.396, abs=0.05)
    assert 0.5 <= result["quality"]["prepared_greeting_seconds"] <= 2.5


def test_stereo_pcm_is_accepted(reference):
    samples = tone(0.7)
    result = guard.inspect_greeting(wav(np.column_stack((samples, samples)), channels=2), reference)
    assert result["quality"]["channels"] == 2


def test_stereo_cancellation_is_not_called_valid_speech(reference):
    samples = tone(0.7)
    raises("silent_narration_audio", lambda: guard.inspect_greeting(wav(np.column_stack((samples, -samples)), channels=2), reference))


@pytest.mark.parametrize("hz", [120, 159, 300, 390])
def test_coarse_pitch_outliers_rejected(reference, hz):
    raises("narration_pitch_outlier", lambda: guard.inspect_greeting(wav(tone(0.7, hz=hz)), reference))


def test_approved_benchmark_range_is_not_overly_narrow(reference):
    assert guard.inspect_greeting(wav(tone(0.8, hz=189)), reference)["quality"]["status"] == "bounded_acoustic_screen_passed"


def test_silence_rejected_before_loudness_normalization(reference):
    raises("silent_narration_audio", lambda: guard.inspect_greeting(wav(np.zeros(24000, dtype=np.int16)), reference))


def test_very_quiet_signal_rejected(reference):
    raises("quiet_narration_audio", lambda: guard.inspect_greeting(wav(tone(0.7, amplitude=180)), reference))


def test_noise_has_insufficient_pitch_confidence(reference):
    samples = np.random.default_rng(53).normal(0, 3000, 24000).astype(np.int16)
    raises("uncertain_narration_pitch", lambda: guard.inspect_greeting(wav(samples), reference))


def test_loud_dc_is_not_voice(reference):
    samples = np.full(24000, 8000, dtype=np.int16)
    raises("uncertain_narration_pitch", lambda: guard.inspect_greeting(wav(samples), reference))


def test_brief_pitched_burst_in_noise_is_low_confidence(reference):
    samples = np.random.default_rng(53).normal(0, 3000, 24000).astype(np.int16)
    samples[:1200] = tone(0.05)
    raises("uncertain_narration_pitch", lambda: guard.inspect_greeting(wav(samples), reference))


def test_raw_clipping_rejected(reference):
    samples = tone(0.7)
    samples[:500] = 32767
    raises("clipped_narration_audio", lambda: guard.inspect_greeting(wav(samples), reference))


def test_clipping_is_checked_before_stereo_downmix(reference):
    samples = tone(0.7)
    left = samples.copy()
    left[:500] = 32767
    right = samples.copy()
    right[:500] = -32768
    raises("clipped_narration_audio", lambda: guard.inspect_greeting(wav(np.column_stack((left, right)), channels=2), reference))


def test_long_outer_silence_is_trimmed_and_tail_retained(reference):
    result = guard.inspect_greeting(wav(tone(0.62, leading=0.32, trailing=0.4)), reference)
    quality = result["quality"]
    assert quality["leading_silence_trim_seconds"] == pytest.approx(0.27, abs=0.01)
    assert quality["trailing_silence_trim_seconds"] == pytest.approx(0.3, abs=0.01)
    assert quality["prepared_greeting_seconds"] == 25 / 30


def test_short_outer_pauses_are_not_trimmed(reference):
    result = guard.inspect_greeting(wav(tone(0.5, leading=0.1, trailing=0.15)), reference)
    assert result["quality"]["leading_silence_trim_seconds"] == 0
    assert result["quality"]["trailing_silence_trim_seconds"] == 0


def test_raw_duration_cannot_be_hidden_by_trimming(reference):
    raises("greeting_too_long", lambda: guard.inspect_greeting(wav(tone(0.7, leading=2.4)), reference))


def test_too_long_prepared_greeting_rejected(reference):
    raises("invalid_greeting_timing", lambda: guard.inspect_greeting(wav(tone(2.5)), reference))


def test_too_short_speech_rejected(reference):
    raises("invalid_greeting_timing", lambda: guard.inspect_greeting(wav(tone(0.15, leading=0.2, trailing=0.3)), reference))


@pytest.mark.parametrize("content", [b"", b"not audio", b"RIFF", b"x" * (guard.MAX_BYTES + 1)])
def test_invalid_or_oversized_wav_rejected(reference, content):
    raises("invalid_narration_audio", lambda: guard.inspect_greeting(content, reference))


def test_truncated_pcm_rejected(reference):
    content = wav(tone(0.7))
    raises("invalid_narration_audio", lambda: guard.inspect_greeting(content[:-100], reference))


@pytest.mark.parametrize("rate,channels,width", [(8000, 1, 2), (96000, 1, 2), (24000, 3, 2), (24000, 1, 1), (24000, 1, 4)])
def test_unsupported_pcm_format_rejected(reference, rate, channels, width):
    content = wav(tone(0.7), rate=rate, channels=channels, width=width)
    raises("invalid_narration_audio", lambda: guard.inspect_greeting(content, reference))


def test_guard_does_not_claim_phonetics_or_identity_verification(reference):
    quality = guard.inspect_greeting(wav(tone(0.7)), reference)["quality"]
    assert quality["speaker_identity_verified"] is False
    assert quality["pronunciation_verified"] is False
    assert quality["naturalness_verified"] is False
