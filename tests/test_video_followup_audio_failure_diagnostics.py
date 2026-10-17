"""Failure measurements must not change acoustic acceptance or disclose inputs."""
import hashlib
import io
import json
import math
import wave

import numpy as np
import pytest

import video_followup_audio_guard as guard
import video_followup_same_take as provider
from video_followup_provider import VideoProviderError


MESSAGE = "Generated speech differs too much from the recorded greeting's pitch range."
MEASUREMENTS = {"duration_seconds": 1.2, "clipped_sample_fraction": 0.0,
                "active_rms_dbfs": -19.0, "median_f0_hz": 164.0,
                "reference_median_f0_hz": 221.5, "pitch_shift_semitones": -5.2,
                "prepared_greeting_seconds": 1.3}


def tone_wav(seconds, hz):
    rate = 24000
    samples = (8000 * np.sin(np.arange(round(seconds * rate)) * 2 * math.pi * hz / rate)).astype("<i2")
    output = io.BytesIO()
    with wave.open(output, "wb") as stream:
        stream.setnchannels(1)
        stream.setsampwidth(2)
        stream.setframerate(rate)
        stream.writeframes(samples.tobytes())
    return output.getvalue()


def pitch_error(values=None):
    error = VideoProviderError("narration_pitch_outlier", MESSAGE)
    error.acoustic_diagnostics = dict(MEASUREMENTS) if values is None else values
    return error


@pytest.mark.parametrize("hz", [159, 300])
def test_actual_pitch_rejection_preserves_error_and_computed_measurements(monkeypatch, hz):
    reference = tone_wav(6, 221)
    monkeypatch.setattr(guard, "REFERENCE_SHA256", hashlib.sha256(reference).hexdigest())
    with pytest.raises(VideoProviderError) as caught:
        guard.inspect_greeting(tone_wav(0.7, hz), reference)
    error = caught.value
    assert type(error) is VideoProviderError
    assert error.code == "narration_pitch_outlier"
    assert str(error) == MESSAGE and error.args == (MESSAGE,)
    measured = guard.safe_acoustic_failure_diagnostics(error)
    assert set(measured) == set(MEASUREMENTS)
    assert measured["duration_seconds"] == 0.7
    assert measured["median_f0_hz"] == pytest.approx(hz, abs=1)
    assert measured["reference_median_f0_hz"] == pytest.approx(221, abs=1)
    assert measured["pitch_shift_semitones"] == pytest.approx(12 * math.log2(hz / 221), abs=0.05)
    assert abs(measured["pitch_shift_semitones"]) > guard.MAX_PITCH_SHIFT_SEMITONES == 3.0
    assert all(type(value) in (float, int) and math.isfinite(value) for value in measured.values())
    assert "passed" not in json.dumps(measured)


def test_in_range_audio_still_has_same_success_contract(monkeypatch):
    reference = tone_wav(6, 221)
    monkeypatch.setattr(guard, "REFERENCE_SHA256", hashlib.sha256(reference).hexdigest())
    result = guard.inspect_greeting(tone_wav(0.7, 216), reference)
    assert result["quality"]["status"] == "bounded_acoustic_screen_passed"
    assert result["quality"]["pitch_shift_semitones"] == pytest.approx(-0.396, abs=0.05)
    assert "acoustic_diagnostics" not in result


def test_error_payload_is_whitelisted_before_attaching():
    with pytest.raises(VideoProviderError) as caught:
        guard._fail("narration_pitch_outlier", MESSAGE, measurements={
            **MEASUREMENTS, "audio_bytes": b"private", "url": "https://private/?secret=hidden",
            "guard_version": "untrusted", "status": "passed", "token": "hidden"})
    assert caught.value.acoustic_diagnostics == MEASUREMENTS


@pytest.mark.parametrize("bad", [True, False, "221.5", "https://private/?token=hidden",
    b"audio", None, {}, [], float("nan"), float("inf"), float("-inf"), 10 ** 1000, -(10 ** 1000)])
def test_each_untrusted_value_is_dropped_at_persistence_boundary(bad):
    error = pitch_error({key: bad for key in MEASUREMENTS})
    assert guard.safe_acoustic_failure_diagnostics(error) == {}
    assert "measured_audio_qa" not in provider._preparation_failure_diagnostics(error, "audio_screen", None)


@pytest.mark.parametrize("name,bad", [("duration_seconds", -1), ("duration_seconds", 11),
    ("clipped_sample_fraction", 1.1), ("active_rms_dbfs", 1), ("median_f0_hz", 401),
    ("reference_median_f0_hz", 69), ("pitch_shift_semitones", 13),
    ("prepared_greeting_seconds", 2.6)])
def test_out_of_bounds_measurement_dropped(name, bad):
    values = {**MEASUREMENTS, name: bad}
    result = guard.safe_acoustic_failure_diagnostics(pitch_error(values))
    assert result == {key: value for key, value in MEASUREMENTS.items() if key != name}


def test_payload_is_resanitized_and_copied_not_trusted_by_reference():
    error = pitch_error()
    first = guard.safe_acoustic_failure_diagnostics(error)
    first["secret"] = "must-not-propagate"
    error.acoustic_diagnostics["url"] = "https://private/?secret=hidden"
    error.acoustic_diagnostics["duration_seconds"] = float("nan")
    second = guard.safe_acoustic_failure_diagnostics(error)
    assert second == {key: value for key, value in MEASUREMENTS.items() if key != "duration_seconds"}
    assert "hidden" not in json.dumps(second)


@pytest.mark.parametrize("error", [RuntimeError("private"), VideoProviderError("fal_download_failed", "private"),
    type("InjectedError", (VideoProviderError,), {})("narration_pitch_outlier", "private")])
def test_other_exception_types_or_codes_cannot_supply_acoustic_payload(error):
    error.acoustic_diagnostics = dict(MEASUREMENTS)
    assert guard.safe_acoustic_failure_diagnostics(error) == {}


def test_missing_error_code_cannot_mask_original_failure():
    error = pitch_error()
    del error.code
    assert guard.safe_acoustic_failure_diagnostics(error) == {}
    assert provider._preparation_failure_diagnostics(error, "audio_screen", None) == {
        "failure_stage": "audio_screen", "underlying_code": "unclassified_preparation_failure",
        "exception_type": "VideoProviderError"}


@pytest.mark.parametrize("payload", [None, [], "private", b"audio",
    type("UntrustedDict", (dict,), {})(MEASUREMENTS)])
def test_only_plain_payload_dict_is_used(payload):
    error = pitch_error()
    error.acoustic_diagnostics = payload
    assert guard.safe_acoustic_failure_diagnostics(error) == {}


@pytest.mark.parametrize("stage", sorted(provider._PREPARATION_FAILURE_STAGES - {"audio_screen"}) + ["unknown"])
def test_other_stages_are_not_mislabeled_with_exception_acoustics(stage):
    details = provider._preparation_failure_diagnostics(pitch_error(), stage, None)
    assert "measured_audio_qa" not in details
    assert details["failure_stage"] == (stage if stage in provider._PREPARATION_FAILURE_STAGES else "unknown_preparation_stage")


def test_completed_audio_qa_retains_existing_precedence():
    details = provider._preparation_failure_diagnostics(pitch_error(), "audio_screen", {"duration_seconds": 1.5})
    assert details["measured_audio_qa"] == {"duration_seconds": 1.5}


def test_legacy_bare_error_remains_unchanged():
    details = provider._preparation_failure_diagnostics(
        VideoProviderError("narration_pitch_outlier", MESSAGE), "audio_screen", None)
    assert details == {"failure_stage": "audio_screen", "underlying_code": "narration_pitch_outlier",
                       "exception_type": "VideoProviderError"}
