import hashlib
import io
import json
import struct
import wave

import numpy as np
import pytest

import video_followup_greeting_normalization as normalization
from video_followup_provider import VideoProviderError


RATE = 24000


def wav(samples, rate=RATE, channels=1):
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as output:
        output.setnchannels(channels)
        output.setsampwidth(2)
        output.setframerate(rate)
        output.writeframes(np.asarray(samples, dtype="<i2").tobytes())
    return buffer.getvalue()


def padded(seconds=1, leading=1, trailing=2, rate=RATE, channels=1, value=1000):
    body = np.full((round(seconds * rate), channels), value, dtype="<i2")
    return np.pad(body, ((round(leading * rate), round(trailing * rate)), (0, 0)))


def reject(code, content):
    with pytest.raises(VideoProviderError) as error:
        normalization.normalize_greeting_audio(content)
    assert error.value.code == code


def unpack(content):
    with wave.open(io.BytesIO(content), "rb") as audio:
        return (np.frombuffer(audio.readframes(audio.getnframes()), dtype="<i2")
                .reshape(-1, audio.getnchannels()), audio.getframerate())


@pytest.mark.parametrize("rate", normalization.NORMALIZATION_CONFIG["sample_rates"])
@pytest.mark.parametrize("channels", [1, 2])
def test_exact_zero_edges_preserve_all_original_channels_and_sample_rate(rate, channels):
    original = padded(rate=rate, channels=channels)
    if channels == 2:
        original[:, 1] *= -1
    raw = wav(original, rate, channels)
    result = normalization.normalize_greeting_audio(raw)
    data, actual_rate = unpack(result["audio_bytes"])
    before = (50 * rate + 999) // 1000
    after = (100 * rate + 999) // 1000
    assert data.shape == (rate + before + after, channels)
    assert actual_rate == rate
    np.testing.assert_array_equal(data[before:before + rate], original[rate:2 * rate])
    assert not np.any(data[:before])
    assert not np.any(data[-after:])
    provenance = result["provenance"]
    assert provenance["removed_leading_frames"] == rate - before
    assert provenance["removed_trailing_frames"] == 2 * rate - after
    assert provenance["interior_samples_removed"] == 0
    assert provenance["normalized_pcm_sha256"] == hashlib.sha256(data.astype("<i2").tobytes()).hexdigest()


@pytest.mark.parametrize("frames", [1, RATE // 2, 3 * RATE - 1, 3 * RATE])
@pytest.mark.parametrize("channels", [1, 2])
def test_at_most_three_seconds_is_byte_identical(frames, channels):
    samples = np.zeros((frames, channels), dtype="<i2")
    samples[frames // 2] = 1
    content = wav(samples, channels=channels)
    result = normalization.normalize_greeting_audio(content)
    assert result["audio_bytes"] is content
    assert result["provenance"]["byte_identical_pass_through"] is True
    assert result["provenance"]["raw_wav_sha256"] == result["provenance"]["normalized_wav_sha256"]


def test_short_silence_is_not_presented_as_accepted_speech():
    content = wav(np.zeros(RATE, dtype="<i2"))
    result = normalization.normalize_greeting_audio(content)
    assert result["audio_bytes"] == content
    assert result["provenance"]["speech_or_acoustic_acceptance_claimed"] is False


def test_quiet_samples_above_noise_floor_are_never_removed():
    samples = padded(seconds=1, leading=1, trailing=2)
    samples[0] = 33
    samples[-1] = -33
    reject("greeting_too_long", wav(samples))


def test_stereo_cancelling_edges_are_not_silence():
    samples = padded(seconds=1, leading=1, trailing=2, channels=2)
    samples[:RATE] = [33, -33]
    samples[-2 * RATE:] = [33, -33]
    reject("greeting_too_long", wav(samples, channels=2))


def test_single_channel_nonzero_edges_are_not_removed():
    samples = padded(seconds=1, leading=1, trailing=2, channels=2)
    samples[0, 1] = 33
    samples[-1, 0] = -33
    reject("greeting_too_long", wav(samples, channels=2))


def test_interior_pause_and_both_nonzero_spans_are_preserved():
    samples = padded(seconds=1.8, leading=1, trailing=1)
    samples[round(1.5 * RATE):round(2.3 * RATE)] = 0
    result = normalization.normalize_greeting_audio(wav(samples))
    decoded, _ = unpack(result["audio_bytes"])
    start = result["provenance"]["retained_start_frame"]
    end = result["provenance"]["retained_end_frame_exclusive"]
    np.testing.assert_array_equal(decoded, samples[start:end])
    assert np.count_nonzero(decoded) == np.count_nonzero(samples)


@pytest.mark.parametrize("samples", [
    np.full(3 * RATE + 1, 33, dtype="<i2"),
    padded(seconds=3, leading=1, trailing=1),
    np.concatenate((np.full(RATE // 2, 33), np.zeros(3 * RATE), np.full(RATE // 2, 33))).astype("<i2"),
])
def test_overlong_nonzero_envelope_or_interior_pause_never_cropped(samples):
    reject("greeting_too_long", wav(samples))


def test_normalized_three_second_boundary_is_inclusive():
    for extra, accepted in ((0, True), (1, False)):
        body = np.full(3 * RATE - RATE // 20 - RATE // 10 + extra, 33, dtype="<i2")
        samples = np.pad(body, (RATE, RATE))
        content = wav(samples)
        if accepted:
            result = normalization.normalize_greeting_audio(content)
            assert result["provenance"]["normalized_frames"] == 3 * RATE
        else:
            reject("greeting_too_long", content)


@pytest.mark.parametrize("edge", ["front", "back"])
@pytest.mark.parametrize("extra", [0, 1])
def test_edge_run_must_be_strictly_over_200ms(edge, extra):
    short = RATE // 5 + extra
    before, after = (short, 3 * RATE) if edge == "front" else (3 * RATE, short)
    samples = np.pad(np.full(RATE, 33, dtype="<i2"), (before, after))
    result = normalization.normalize_greeting_audio(wav(samples))
    key = "removed_leading_frames" if edge == "front" else "removed_trailing_frames"
    margin = RATE // 20 if edge == "front" else RATE // 10
    assert result["provenance"][key] == (short - margin if extra else 0)


def test_long_all_zero_input_is_not_recovered():
    reject("silent_narration_audio", wav(np.zeros(4 * RATE, dtype="<i2")))


@pytest.mark.parametrize("frames", [10 * RATE + 1, 0])
def test_empty_or_over_ten_second_input_is_invalid(frames):
    reject("invalid_narration_audio", wav(np.zeros(frames, dtype="<i2")))


def test_ten_second_bounded_input_can_only_remove_outer_zeros():
    result = normalization.normalize_greeting_audio(wav(padded(seconds=1, leading=4, trailing=5)))
    assert result["provenance"]["raw_frames"] == 10 * RATE
    assert result["provenance"]["normalized_frames"] == round(1.15 * RATE)


@pytest.mark.parametrize("channels", [1, 2])
def test_clipped_edges_cannot_be_deleted_or_hidden_by_downmix(channels):
    samples = padded(seconds=1, leading=1, trailing=2, channels=channels)
    samples[:500] = 32767
    if channels == 2:
        samples[:500, 1] = -32768
    reject("clipped_narration_audio", wav(samples, channels=channels))


def test_silence_cannot_dilute_normalized_clipping_fraction():
    samples = padded(seconds=1, leading=4, trailing=5)
    samples[4 * RATE:4 * RATE + 100] = 32767
    reject("clipped_narration_audio", wav(samples))


def test_clipping_threshold_matches_existing_pcm_guard():
    samples = np.full(RATE, 32735, dtype="<i2")
    assert normalization.normalize_greeting_audio(wav(samples))["provenance"]["original_clipped_samples"] == 0
    samples[:] = 32736
    reject("clipped_narration_audio", wav(samples))


def change_u32(content, offset, value):
    return content[:offset] + struct.pack("<I", value) + content[offset + 4:]


@pytest.mark.parametrize("content", [b"", b"RIFF", b"not a wav", b"x" * (2 * 1024 * 1024 + 1), bytearray(b"RIFF")])
def test_invalid_types_headers_and_oversized_input(content):
    reject("invalid_narration_audio", content)


@pytest.mark.parametrize("mutate", [
    lambda b: b[:-1],
    lambda b: b + b"hidden content",
    lambda b: change_u32(b, 4, len(b) - 7),
    lambda b: change_u32(b, 40, len(b) - 43),
    lambda b: change_u32(b, 40, len(b) - 45),
    lambda b: change_u32(b, 16, 18),
    lambda b: b[:20] + struct.pack("<H", 3) + b[22:],
    lambda b: b[:22] + struct.pack("<H", 3) + b[24:],
    lambda b: change_u32(b, 24, 8000),
    lambda b: change_u32(b, 28, 1),
    lambda b: b[:32] + struct.pack("<H", 1) + b[34:],
    lambda b: b[:34] + struct.pack("<H", 24) + b[36:],
    lambda b: b"RIFX" + b[4:],
    lambda b: b[:8] + b"AVI " + b[12:],
])
def test_truncated_malformed_or_inconsistent_wav_is_rejected(mutate):
    reject("invalid_narration_audio", mutate(wav(np.ones(RATE, dtype="<i2"))))


@pytest.mark.parametrize("chunk", [b"data" + struct.pack("<I", 2) + b"\x01\x00",
                                    b"slnt" + struct.pack("<I", 4) + bytes(4),
                                    b"fmt " + struct.pack("<I", 16) + bytes(16)])
def test_extra_playback_chunks_rejected_even_when_riff_length_is_consistent(chunk):
    content = wav(np.ones(RATE, dtype="<i2")) + chunk
    content = change_u32(content, 4, len(content) - 8)
    reject("invalid_narration_audio", content)


def test_incomplete_stereo_frame_rejected_even_with_updated_sizes():
    content = wav(np.ones((RATE, 2), dtype="<i2"), channels=2)[:-2]
    content = change_u32(change_u32(content, 4, len(content) - 8), 40, len(content) - 44)
    reject("invalid_narration_audio", content)


def test_provenance_is_complete_deterministic_and_independent_of_returned_mutation():
    content = wav(padded())
    first = normalization.normalize_greeting_audio(content)
    again = normalization.normalize_greeting_audio(content)
    assert first == again
    provenance = first["provenance"]
    canonical = json.dumps(provenance["config"], sort_keys=True, separators=(",", ":")).encode()
    assert hashlib.sha256(canonical).hexdigest() == normalization.NORMALIZATION_CONFIG_SHA256
    assert provenance["raw_wav_sha256"] == hashlib.sha256(content).hexdigest()
    assert provenance["normalized_wav_sha256"] == hashlib.sha256(first["audio_bytes"]).hexdigest()
    assert provenance["raw_pcm_sha256"] == hashlib.sha256(content[44:]).hexdigest()
    provenance["config"]["sample_rates"].append(8000)
    assert normalization.normalize_greeting_audio(content) == again


def test_mutated_exported_config_fails_closed(monkeypatch):
    monkeypatch.setitem(normalization.NORMALIZATION_CONFIG, "maximum_raw_seconds", 20)
    reject("invalid_narration_audio", wav(padded()))


def chunk(kind, payload, pad=b"\x00"):
    return kind + struct.pack("<I", len(payload)) + payload + (pad if len(payload) % 2 else b"")


def container(chunks):
    body = b"WAVE" + b"".join(chunks)
    return b"RIFF" + struct.pack("<I", len(body)) + body


def metadata_wav(content, fmt18=False):
    fmt = content[20:36] + (b"\x00\x00" if fmt18 else b"")
    return container([
        chunk(b"JUNK", b"pad", pad=b"\x7f"),
        chunk(b"fmt ", fmt),
        chunk(b"LIST", b"INFO" + chunk(b"ISFT", b"Lavf61.7.100\x00") + chunk(b"IART", b"X")),
        chunk(b"data", content[44:]),
        chunk(b"LIST", b"INFO" + chunk(b"ICMT", b"retained\x00")),
        chunk(b"JUNK", b"tail"),
    ])


@pytest.mark.parametrize("fmt18", [False, True])
@pytest.mark.parametrize("seconds", [1, 3])
def test_supported_metadata_and_empty_fmt_extension_short_input_is_byte_identical(fmt18, seconds):
    original = metadata_wav(wav(padded(seconds=seconds, leading=0, trailing=0)), fmt18)
    result = normalization.normalize_greeting_audio(original)
    assert result["audio_bytes"] is original
    assert result["provenance"]["retained_non_audio_chunks_byte_identical"] is True


@pytest.mark.parametrize("fmt18", [False, True])
@pytest.mark.parametrize("channels", [1, 2])
def test_long_metadata_wav_preserves_all_metadata_order_padding_and_pcm(fmt18, channels):
    original = padded(channels=channels)
    plain = wav(original, channels=channels)
    raw = metadata_wav(plain, fmt18)
    result = normalization.normalize_greeting_audio(raw)
    proof = result["provenance"]
    start, end = proof["retained_start_frame"], proof["retained_end_frame_exclusive"]
    expected = metadata_wav(wav(original[start:end], channels=channels), fmt18)
    assert result["audio_bytes"] == expected
    assert proof["raw_pcm_sha256"] == hashlib.sha256(plain[44:]).hexdigest()
    assert proof["raw_pcm_start_byte"] > 44
    assert proof["raw_pcm_end_byte_exclusive"] < len(raw)
    assert proof["normalized_pcm_start_byte"] == proof["raw_pcm_start_byte"]
    np.testing.assert_array_equal(unpack(result["audio_bytes"])[0], original[start:end])


@pytest.mark.parametrize("prefix", [True, False])
@pytest.mark.parametrize("kind", [b"data", b"fmt "])
def test_duplicate_format_or_data_chunks_rejected_in_any_position(prefix, kind):
    plain = wav(np.ones(RATE, dtype="<i2"))
    fmt, data = chunk(b"fmt ", plain[20:36]), chunk(b"data", plain[44:])
    duplicate = fmt if kind == b"fmt " else data
    pieces = [duplicate, fmt, data] if prefix else [fmt, data, duplicate]
    reject("invalid_narration_audio", container(pieces))


@pytest.mark.parametrize("metadata", [
    chunk(b"LIST", b"wavl" + chunk(b"data", b"\x01\x00")),
    chunk(b"LIST", b"INFO" + chunk(b"data", b"\x01\x00")),
    chunk(b"LIST", b"INFO" + b"ISFT"),
    chunk(b"LIST", b"INFO" + b"ISFT" + struct.pack("<I", 100) + b"\x00"),
    chunk(b"LIST", b"INFO" + b"ISFT" + struct.pack("<I", 1) + b"X"),
    chunk(b"LIST", b"INFO" + chunk(b"ISFT", b"x") * 65),
    chunk(b"JUNK", bytes(65536)),
    chunk(b"JUNK", b"") * 63,
    chunk(b"RIFF", b"WAVE"),
    chunk(b"fact", bytes(4)),
])
def test_malformed_unbounded_or_playback_metadata_rejected(metadata):
    plain = wav(np.ones(RATE, dtype="<i2"))
    reject("invalid_narration_audio", container([
        chunk(b"fmt ", plain[20:36]), chunk(b"data", plain[44:]), metadata,
    ]))


def test_odd_metadata_chunk_without_pad_is_rejected():
    plain = wav(np.ones(RATE, dtype="<i2"))
    reject("invalid_narration_audio", container([
        chunk(b"fmt ", plain[20:36]), chunk(b"data", plain[44:]),
        b"JUNK" + struct.pack("<I", 1) + b"X",
    ]))


def test_nonempty_pcm_extension_and_data_before_format_are_rejected():
    plain = wav(np.ones(RATE, dtype="<i2"))
    fmt, data = chunk(b"fmt ", plain[20:36]), chunk(b"data", plain[44:])
    reject("invalid_narration_audio", container([data, fmt]))
    reject("invalid_narration_audio", container([
        chunk(b"fmt ", plain[20:36] + b"\x01\x00"), data,
    ]))


@pytest.mark.parametrize("channels", [1, 2])
@pytest.mark.parametrize("noise", [-32, -19, -1, 0, 1, 19, 32])
def test_only_bounded_original_channel_noise_removed(channels, noise):
    samples = padded(seconds=1, leading=1, trailing=2, channels=channels)
    samples[:RATE] = noise
    samples[-2 * RATE:] = noise
    if channels == 2:
        samples[:, 1] *= -1
    result = normalization.normalize_greeting_audio(wav(samples, channels=channels))
    proof = result["provenance"]
    assert proof["maximum_discarded_absolute_pcm16"] == abs(noise)
    assert proof["all_above_noise_floor_original_channel_samples_retained"] is True
    start, end = proof["retained_start_frame"], proof["retained_end_frame_exclusive"]
    np.testing.assert_array_equal(unpack(result["audio_bytes"])[0], samples[start:end])


@pytest.mark.parametrize("noise", [-32, 19, 32])
def test_long_noise_only_recording_is_not_a_greeting(noise):
    reject("silent_narration_audio", wav(np.full(4 * RATE, noise, dtype="<i2")))


def test_interior_low_level_samples_are_never_removed():
    samples = padded(seconds=1.8, leading=1, trailing=1, channels=2)
    samples[round(1.5 * RATE):round(2.3 * RATE)] = [19, -32]
    result = normalization.normalize_greeting_audio(wav(samples, channels=2))
    proof = result["provenance"]
    start, end = proof["retained_start_frame"], proof["retained_end_frame_exclusive"]
    np.testing.assert_array_equal(unpack(result["audio_bytes"])[0], samples[start:end])
    assert proof["interior_samples_removed"] == 0


@pytest.mark.parametrize("channels", [1, 2])
def test_one_sample_above_noise_floor_at_final_edge_cannot_be_trimmed(channels):
    samples = padded(seconds=1.4, leading=0, trailing=1.62, channels=channels)
    samples[round(1.4 * RATE):] = 19
    samples[-1, channels - 1] = -33
    reject("greeting_too_long", wav(samples, channels=channels))


@pytest.mark.parametrize("before_data", [False, True])
@pytest.mark.parametrize("long", [False, True])
def test_c2pa_is_bound_to_raw_and_dropped_only_after_audio_changes(before_data, long):
    plain = wav(padded() if long else np.full(RATE, 1000, dtype="<i2"))
    fmt, data = chunk(b"fmt ", plain[20:36]), chunk(b"data", plain[44:])
    signature = chunk(b"C2PA", b"opaque original signature", pad=b"\x12")
    chunks = [fmt, signature, data] if before_data else [fmt, data, signature]
    raw = container(chunks)
    result = normalization.normalize_greeting_audio(raw)
    proof = result["provenance"]
    assert proof["raw_c2pa_chunk_sha256"] == hashlib.sha256(signature).hexdigest()
    assert proof["raw_wav_sha256"] == hashlib.sha256(raw).hexdigest()
    if long:
        assert proof["removed_c2pa_chunk_sha256"] == hashlib.sha256(signature).hexdigest()
        assert proof["c2pa_action"] == "removed_after_audio_change"
        assert result["audio_bytes"] == normalization.normalize_greeting_audio(plain)["audio_bytes"]
        assert proof["normalized_pcm_start_byte"] == 44
    else:
        assert result["audio_bytes"] is raw
        assert proof["removed_c2pa_chunk_sha256"] is None
        assert proof["c2pa_action"] == "unchanged"


@pytest.mark.parametrize("signature", [
    chunk(b"C2PA", b"a") + chunk(b"C2PA", b"b"),
    chunk(b"C2PA", bytes(1048577)),
])
def test_duplicate_or_unbounded_c2pa_is_rejected(signature):
    plain = wav(np.full(RATE, 1000, dtype="<i2"))
    reject("invalid_narration_audio", container([
        chunk(b"fmt ", plain[20:36]), chunk(b"data", plain[44:]), signature,
    ]))
