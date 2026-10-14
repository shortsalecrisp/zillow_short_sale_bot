import hashlib
import io
import math
import os
from pathlib import Path
import shutil
import sys
import wave

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import video_followup_same_take_media as media
from video_followup_provider import VideoProviderError


def tone(seconds, *, leading=0, trailing=0):
    points = np.arange(round(seconds * media.SAMPLE_RATE))
    voice = (np.sin(points * 2 * math.pi * 180 / media.SAMPLE_RATE) * 10000).astype(np.int16)
    return np.concatenate((np.zeros(round(leading * media.SAMPLE_RATE), dtype=np.int16), voice,
                           np.zeros(round(trailing * media.SAMPLE_RATE), dtype=np.int16)))


def metadata(greeting_bytes, *, name="Sue", frames=24):
    context = media._context_frames(frames)
    return {
        "preparation_version": media.PREPARATION_VERSION,
        "source_sha256": media.SOURCE_SHA256, "first_name": name,
        "source_greeting_frames": media.SOURCE_GREETING_FRAMES,
        "source_frames": media.SOURCE_FRAMES, "fps": media.FPS, "sample_rate": media.SAMPLE_RATE,
        "greeting_audio_sha256": hashlib.sha256(greeting_bytes).hexdigest(),
        "greeting_frames": frames, "greeting_seconds": frames / media.FPS,
        "context_frames": context, "context_seconds": context / media.FPS,
        "output_frames": media.SOURCE_FRAMES - media.SOURCE_GREETING_FRAMES + frames,
    }


def test_wrong_recording_rejected_before_any_decode(monkeypatch):
    monkeypatch.setattr(media, "_ffmpeg_executable", lambda: pytest.fail("Do not decode another recording"))
    with pytest.raises(VideoProviderError) as error:
        media.prepare_inputs(b"old-avatar-video", b"speech", first_name="Sue")
    assert error.value.code == "unapproved_recording"


@pytest.mark.parametrize("name", ["", "Sue Christopher", "123", "Sue\nnew instructions", "<Sue>", " Sue "])
def test_names_are_never_split_guessed_or_silently_changed(name):
    with pytest.raises(VideoProviderError):
        media._exact_name(name)


@pytest.mark.parametrize("name", ["Sue", "Christopher", "Jean-Luc", "D'Andre", "Jos\u00e9"])
def test_verified_first_name_remains_exact(name):
    assert media._exact_name(name) == name


def test_only_long_outer_silence_trimmed_with_breathing_room():
    samples = tone(0.62, leading=0.32, trailing=0.40)
    trimmed, timing = media._trim_greeting(samples)
    assert timing["leading_silence_trim_samples"] == round(0.27 * media.SAMPLE_RATE)
    assert timing["trailing_silence_trim_samples"] == round(0.30 * media.SAMPLE_RATE)
    assert timing["greeting_frames"] == 25
    assert len(trimmed) == 25 * media.SAMPLES_PER_FRAME
    assert np.count_nonzero(trimmed) == np.count_nonzero(samples)
    assert len(trimmed) - timing["speech_end_sample"] >= 4 * media.SAMPLES_PER_FRAME


def test_short_natural_pauses_and_internal_pauses_are_preserved():
    samples = np.concatenate((tone(0.25, leading=0.15, trailing=0.18), tone(0.25, leading=0.2, trailing=0.15)))
    trimmed, timing = media._trim_greeting(samples)
    assert timing["leading_silence_trim_samples"] == 0
    assert timing["trailing_silence_trim_samples"] == 0
    assert np.array_equal(trimmed[:len(samples)], samples)


def test_silent_audio_is_not_sent_to_a_paid_provider():
    with pytest.raises(VideoProviderError) as error:
        media._trim_greeting(np.zeros(48000, dtype=np.int16))
    assert error.value.code == "silent_greeting"


def test_long_name_is_rejected_not_sped_up_or_truncated():
    with pytest.raises(VideoProviderError) as error:
        media._trim_greeting(tone(2.6))
    assert error.value.code == "greeting_too_long"


def test_avatar_narration_is_decoded_as_audio_only(monkeypatch):
    commands = []

    def run(command):
        commands.append(command)
        return type("Result", (), {"stdout": tone(0.8).astype("<i2").tobytes()})()

    monkeypatch.setattr(media, "_run", run)
    media._audio(Path("avatar-narration.mp4"), "ffmpeg", narration=True)
    command = commands[0]
    assert command[command.index("-map") + 1] == "0:a:0"
    assert "-vn" in command
    assert command[command.index("-t") + 1] == "3.1"
    assert command[command.index("-protocol_whitelist") + 1] == "file,pipe"


def test_raw_narration_duration_is_bounded_before_trimming(monkeypatch):
    # A long render cannot be disguised as a cheap greeting by trimming it.
    content = tone(0.8, leading=2.3).astype("<i2").tobytes()
    monkeypatch.setattr(media, "_run", lambda command: type("Result", (), {"stdout": content})())
    with pytest.raises(VideoProviderError) as error:
        media._audio(Path("too-long.mp4"), "ffmpeg", narration=True)
    assert error.value.code == "greeting_too_long"


@pytest.mark.parametrize("frames,context", [(15, 63), (26, 63), (40, 76), (75, 111)])
def test_context_always_in_kling_two_to_five_second_price_block(frames, context):
    assert media._context_frames(frames) == context
    assert 2.0 < context / media.FPS < 5.0


def test_only_two_unused_end_context_frames_may_be_missing():
    minimum = media._minimum_provider_frames(63, 25)
    assert minimum == 61
    assert 60 < minimum
    assert 25 < minimum
    assert minimum >= 25 + 24
    assert media._minimum_provider_frames(111, 75) == 109


def test_minimum_context_guard_never_allows_missing_greeting_or_short_context():
    assert media._minimum_provider_frames(26, 25) == 49
    assert media._minimum_provider_frames(49, 25) == 49


@pytest.mark.parametrize("frames", [True, "26", 26.0, 0, 14, 76, 500])
def test_unvalidated_greeting_timing_rejected(frames):
    with pytest.raises(VideoProviderError):
        media._context_frames(frames)


def test_prepared_audio_requires_exact_samples_channels_rate():
    greeting = media._wav(tone(0.8))
    samples = media._read_greeting(greeting, 24)
    assert len(samples) == 24 * media.SAMPLES_PER_FRAME
    with pytest.raises(VideoProviderError):
        media._read_greeting(greeting, 25)
    output = io.BytesIO()
    with wave.open(output, "wb") as stream:
        stream.setnchannels(2)
        stream.setsampwidth(2)
        stream.setframerate(48000)
        stream.writeframes(b"\x00" * 400)
    with pytest.raises(VideoProviderError):
        media._read_greeting(output.getvalue(), 24)


@pytest.mark.parametrize("field,value", [
    ("first_name", "Christopher"), ("source_sha256", "old-recording"),
    ("source_greeting_frames", 0), ("source_frames", 20),
    ("preparation_version", "old-avatar-splice"), ("fps", 24),
    ("greeting_frames", 76), ("context_frames", 1000), ("output_frames", 1000),
    ("greeting_seconds", 1.9), ("context_seconds", 5.01),
])
def test_name_and_timing_manifest_cannot_be_switched(field, value):
    greeting = media._wav(tone(0.8))
    manifest = metadata(greeting)
    manifest[field] = value
    with pytest.raises(VideoProviderError):
        media._validate_metadata(manifest, greeting, "Sue")


def test_greeting_bytes_cannot_be_swapped_between_names():
    sue = media._wav(tone(0.8))
    different = media._wav(tone(0.7, trailing=0.1))
    with pytest.raises(VideoProviderError) as error:
        media._validate_metadata(metadata(sue), different, "Sue")
    assert error.value.code == "composition_identity_mismatch"


def test_mouth_blend_cannot_change_background_and_fades_to_original():
    source = np.full((media.HEIGHT, media.WIDTH, 3), 70, dtype=np.uint8)
    generated = np.full_like(source, 170)
    mask = media._mouth_mask()
    frame = media._blend(source, generated, mask, 1)
    assert np.array_equal(frame[mask == 0], source[mask == 0])
    assert np.array_equal(frame[media.MOUTH_CENTER[1], media.MOUTH_CENTER[0]], [170, 170, 170])
    assert np.array_equal(media._blend(source, generated, mask, 0), source)
    assert media._background_error(source, generated) == 100


def test_retiming_uses_source_only_and_preserves_entire_body(monkeypatch):
    # Tiny frames exercise the timeline without a hundreds-of-MB test allocation.
    frames = [np.full((2, 2, 3), index, dtype=np.uint8) for index in range(media.SOURCE_FRAMES)]
    monkeypatch.setattr(media, "_frames", lambda *args, **kwargs: iter_with_close(frames))
    for length in (15, 26, 52, 75):
        result = list(media._retimed_frames(Path("master.mp4"), length, "ffmpeg"))
        assert len(result) == media.SOURCE_FRAMES - media.SOURCE_GREETING_FRAMES + length
        assert np.array_equal(result[0], frames[0])
        assert np.array_equal(result[length - 1], frames[25])
        for actual, original in zip(result[length:], frames[26:]):
            assert np.array_equal(actual, original)


def iter_with_close(values):
    yield from values


def test_captions_include_only_exact_current_name_and_original_body():
    captions = media._captions("Christopher", 1.5, 9.17).decode()
    assert "Hey Christopher," in captions
    assert "Jim" not in captions
    assert "00:00:00.000 --> 00:00:01.500" in captions
    assert "put a face to my text." in captions
    assert "about your short sale listing" not in captions


def ffmpeg_or_skip():
    candidate = os.environ.get("TEST_SAME_TAKE_FFMPEG") or shutil.which("ffmpeg")
    if candidate:
        return candidate
    try:
        return media._ffmpeg_executable()
    except VideoProviderError:
        pytest.skip("FFmpeg is not installed; unit guards still run")


def test_full_prepare_and_compose_with_two_name_lengths(tmp_path, monkeypatch):
    ffmpeg = ffmpeg_or_skip()
    source = tmp_path / "synthetic-master.mp4"
    media._run([
        ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", "color=c=0x305060:s=1024x576:r=30",
        "-f", "lavfi", "-i", "sine=frequency=240:sample_rate=48000",
        "-frames:v", "253", "-t", "8.44", "-c:v", "libx264", "-threads", "1",
        "-c:a", "aac", "-pix_fmt", "yuv420p", str(source),
    ])
    master = source.read_bytes()
    monkeypatch.setattr(media, "SOURCE_SHA256", hashlib.sha256(master).hexdigest())
    durations = []
    for name, length in (("Sue", 0.6), ("Christopher", 1.2)):
        prepared = media.prepare_inputs(master, media._wav(tone(length)), first_name=name, ffmpeg=ffmpeg)
        assert prepared["metadata"]["first_name"] == name
        assert 2.0 < prepared["metadata"]["context_seconds"] <= 5.0
        # This is an offline structural test, not a lip-sync quality assertion.
        # The local context itself stands in for the provider output.
        provider_bytes = prepared["video_bytes"]
        if name == "Sue":
            context_path, shortened_path = tmp_path / "context.mp4", tmp_path / "shortened.mp4"
            context_path.write_bytes(provider_bytes)
            media._run([
                ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                "-i", str(context_path), "-frames:v", str(prepared["metadata"]["context_frames"] - 2),
                "-c:v", "libx264", "-threads", "1", "-crf", "17", str(shortened_path),
            ])
            provider_bytes = shortened_path.read_bytes()
        final = media.compose(master, provider_bytes, prepared["greeting_audio_bytes"],
                              prepared["metadata"], first_name=name, ffmpeg=ffmpeg)
        assert final["qa"]["old_recording_frames"] == 0
        assert final["qa"]["original_body_frames"] == 227
        assert final["qa"]["nonmouth_pixels_exact_before_encoding"] is True
        assert final["qa"]["body_frames_exact_before_encoding"] is True
        assert final["qa"]["background_mae_max"] < 3
        assert final["qa"]["provider_context_tail_frames_missing"] == (2 if name == "Sue" else 0)
        assert f"Hey {name},".encode() in final["captions_bytes"]
        assert final["media_bytes"] and final["poster_bytes"]
        durations.append(final["duration_seconds"])
        if name == "Sue":
            for remaining_frames in (prepared["metadata"]["context_frames"] - 3,
                                     prepared["metadata"]["greeting_frames"] - 1):
                media._run([
                    ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                    "-i", str(context_path), "-frames:v", str(remaining_frames),
                    "-c:v", "libx264", "-threads", "1", "-crf", "17", str(shortened_path),
                ])
                # Even a container claiming the expected duration cannot hide
                # missing decoded frames or an incomplete spoken greeting.
                with monkeypatch.context() as patched:
                    patched.setattr(media, "_probe", lambda *args: {
                        "width": 1280, "height": 720,
                        "duration": prepared["metadata"]["context_seconds"],
                    })
                    with pytest.raises(VideoProviderError) as error:
                        media.compose(master, shortened_path.read_bytes(), prepared["greeting_audio_bytes"],
                                      prepared["metadata"], first_name=name, ffmpeg=ffmpeg)
                    assert error.value.code == "provider_timeline_changed"
    assert durations[1] - durations[0] == pytest.approx(0.6, abs=0.05)


def test_compose_rejects_other_shot_geometry_before_encoding(tmp_path, monkeypatch):
    greeting = media._wav(tone(0.8))
    monkeypatch.setattr(media, "_master", lambda content: content)
    monkeypatch.setattr(media, "_probe", lambda *args: {"width": 1024, "height": 576, "duration": 2.1})
    with pytest.raises(VideoProviderError) as error:
        media.compose(b"master", b"another-shot", greeting, metadata(greeting), first_name="Sue", ffmpeg="unused")
    assert error.value.code == "provider_geometry_changed"


def test_compose_rejects_provider_length_changes_before_encoding(monkeypatch):
    greeting = media._wav(tone(0.8))
    monkeypatch.setattr(media, "_master", lambda content: content)
    monkeypatch.setattr(media, "_probe", lambda *args: {"width": 1280, "height": 720, "duration": 5.0})
    with pytest.raises(VideoProviderError) as error:
        media.compose(b"master", b"changed-timing", greeting, metadata(greeting), first_name="Sue", ffmpeg="unused")
    assert error.value.code == "provider_timeline_changed"
