"""Offline candidate tests. Synthetic tone proofs test binding, never ASR quality."""

import copy
import hashlib
from pathlib import Path
import struct
import sys

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import video_followup_audio_lineage as lineage
import video_followup_same_take_media as media
import video_followup_speech_guard as speech
from video_followup_provider import VideoProviderError


def _run(ffmpeg, *args):
    return lineage._run([ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", *map(str, args)])


def _proof(plan, name="Sue"):
    # Deliberately synthetic: this suite proves cryptographic/media binding,
    # not that a tone contains speech. Production must obtain a genuine proof.
    return {"accepted": True, "policy_version": speech.POLICY_VERSION,
        "model_id": speech.MODEL_ID, "model_revision": speech.MODEL_REVISION,
        "model_manifest_sha256": speech.MODEL_MANIFEST_SHA256, "config_sha256": speech.CONFIG_SHA256,
        "source_audio_sha256": hashlib.sha256(plan["greeting_audio_bytes"]).hexdigest(),
        "expected_name": name, "normalized_transcript": "hey " + name.casefold(),
        "confidence": [{"avg_logprob": -0.2, "no_speech_prob": 0.01}]}


@pytest.fixture(scope="module")
def prepared(tmp_path_factory):
    ffmpeg = lineage._ffmpeg_executable()
    directory = tmp_path_factory.mktemp("audio-lineage")
    master = directory / "master.mp4"
    _run(ffmpeg, "-f", "lavfi", "-i", "color=c=blue:size=160x90:rate=30:duration=8.433333333",
        "-f", "lavfi", "-i", "sine=frequency=520:sample_rate=48000:duration=8.433333333",
        "-map", "0:v:0", "-map", "1:a:0", "-c:v", "libx264", "-threads", "1", "-c:a", "aac", "-ac", "1", master)
    master_bytes = master.read_bytes()
    original_hash = media.SOURCE_SHA256
    media.SOURCE_SHA256 = hashlib.sha256(master_bytes).hexdigest()
    frames = 24
    points = np.arange(frames * media.SAMPLES_PER_FRAME)
    greeting = media._wav((np.sin(points * 2 * np.pi * 220 / media.SAMPLE_RATE) * 9000).astype("<i2"))
    context = media._context_frames(frames)
    metadata = {"preparation_version": media.PREPARATION_VERSION, "source_sha256": media.SOURCE_SHA256,
        "first_name": "Sue", "source_greeting_frames": media.SOURCE_GREETING_FRAMES,
        "source_frames": media.SOURCE_FRAMES, "fps": media.FPS, "sample_rate": media.SAMPLE_RATE,
        "greeting_audio_sha256": hashlib.sha256(greeting).hexdigest(), "greeting_frames": frames,
        "greeting_seconds": frames / media.FPS, "context_frames": context, "context_seconds": context / media.FPS,
        "output_frames": media.SOURCE_FRAMES - media.SOURCE_GREETING_FRAMES + frames}
    plan = lineage.prepare_audio_plan(master_bytes, greeting, metadata, first_name="Sue", ffmpeg=ffmpeg)
    planned = directory / "planned.m4a"
    planned.write_bytes(plan["audio_bytes"])
    silent = directory / "silent.mp4"
    _run(ffmpeg, "-i", master, "-map", "0:v:0", "-an", "-c:v", "copy", silent)
    final = directory / "final.mp4"
    _run(ffmpeg, "-i", silent, "-i", planned, "-map", "0:v:0", "-map", "1:a:0", "-c", "copy",
        "-movie_timescale", "48000", "-movflags", "+faststart", final)
    yield {"root": directory, "ffmpeg": ffmpeg, "master": master, "silent": silent,
        "planned": planned, "final": final, "plan": plan, "metadata": metadata, "proof": _proof(plan)}
    media.SOURCE_SHA256 = original_hash


def _verify(prepared, content=None, **changes):
    plan = prepared["plan"]
    args = {"final_bytes": content or prepared["final"].read_bytes(), "planned_audio_bytes": plan["audio_bytes"],
        "manifest": plan["manifest"], "speech_proof": prepared["proof"], "expected_preparation": prepared["metadata"],
        "first_name": "Sue", "expected_manifest_sha256": plan["manifest_sha256"], "ffmpeg": prepared["ffmpeg"]}
    args.update(changes)
    return lineage.verify_final_audio(**args)


def test_real_aac_stream_copy_proves_full_track_and_exact_checked_bytes(prepared):
    result = _verify(prepared)
    assert result["accepted"] is True
    assert result["first_name"] == "Sue"
    assert result["final_media_sha256"] == hashlib.sha256(prepared["final"].read_bytes()).hexdigest()
    audio, pcm = lineage._decode_audio(prepared["final"].read_bytes(), ffmpeg=prepared["ffmpeg"], greeting_seconds=0.8)
    assert audio == prepared["plan"]["greeting_audio_bytes"]
    assert speech._extract_video_greeting(prepared["final"].read_bytes(), 0.8) == audio
    assert hashlib.sha256(pcm).hexdigest() == prepared["plan"]["manifest"]["greeting_pcm_sha256"]


def test_mux_helper_preserves_exact_proof_and_forbids_preexisting_audio(prepared):
    plan = prepared["plan"]
    def mux(video):
        return lineage.mux_planned_audio(video, plan["audio_bytes"], plan["manifest"], prepared["proof"],
            prepared["metadata"], first_name="Sue", expected_manifest_sha256=plan["manifest_sha256"],
            ffmpeg=prepared["ffmpeg"])
    output = mux(prepared["silent"].read_bytes())
    assert output["lineage_proof"]["accepted"] is True
    assert _verify(prepared, output["media_bytes"])["accepted"] is True
    with pytest.raises(VideoProviderError):
        mux(prepared["final"].read_bytes())


@pytest.mark.parametrize("filter_", ["volume=0", "volume=0.5", "atrim=end=2", "adelay=150|150", "highpass=f=500", "pan=stereo|c0=c0|c1=c0"])
def test_real_audio_mutations_rejected(prepared, tmp_path, filter_):
    output = tmp_path / "changed.mp4"
    _run(prepared["ffmpeg"], "-i", prepared["final"], "-map", "0:v:0", "-map", "0:a:0",
        "-c:v", "copy", "-af", filter_, "-c:a", "aac", "-b:a", "160k", output)
    with pytest.raises(VideoProviderError):
        _verify(prepared, output.read_bytes())


def test_wrong_greeting_replacement_rejected(prepared, tmp_path):
    output = tmp_path / "wrong-name.mp4"
    _run(prepared["ffmpeg"], "-i", prepared["silent"], "-f", "lavfi", "-i", "sine=frequency=920:sample_rate=48000:duration=8.4",
        "-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy", "-c:a", "aac", "-ac", "1", output)
    with pytest.raises(VideoProviderError):
        _verify(prepared, output.read_bytes())


def test_extra_audio_and_alternate_default_tracks_rejected(prepared, tmp_path):
    output = tmp_path / "extra-audio.mp4"
    _run(prepared["ffmpeg"], "-i", prepared["final"], "-i", prepared["planned"], "-map", "0:v:0",
        "-map", "0:a:0", "-map", "1:a:0", "-c", "copy", "-disposition:a:0", "0", "-disposition:a:1", "default", output)
    with pytest.raises(VideoProviderError) as exc:
        _verify(prepared, output.read_bytes())
    assert exc.value.code == "audio_lineage_track_count"


def test_no_video_rejected_even_with_exact_audio(prepared):
    with pytest.raises(VideoProviderError) as exc:
        _verify(prepared, prepared["plan"]["audio_bytes"])
    assert exc.value.code == "audio_lineage_track_count"


def test_shifted_packet_timeline_rejected_even_without_reencoding(prepared, tmp_path):
    output = tmp_path / "shifted.mp4"
    _run(prepared["ffmpeg"], "-i", prepared["silent"], "-itsoffset", "0.1", "-i", prepared["planned"],
        "-map", "0:v:0", "-map", "1:a:0", "-c", "copy", output)
    with pytest.raises(VideoProviderError):
        _verify(prepared, output.read_bytes())


def test_trimmed_tail_stream_copy_rejected(prepared, tmp_path):
    output = tmp_path / "tail-cut.mp4"
    _run(prepared["ffmpeg"], "-i", prepared["final"], "-t", "6", "-map", "0", "-c", "copy", output)
    with pytest.raises(VideoProviderError):
        _verify(prepared, output.read_bytes())


def test_millisecond_movie_clock_is_not_accepted_as_exact_stream_copy(prepared, tmp_path):
    output = tmp_path / "rounding.mp4"
    _run(prepared["ffmpeg"], "-i", prepared["final"], "-map", "0", "-c", "copy",
        "-movie_timescale", "1000", output)
    with pytest.raises(VideoProviderError):
        _verify(prepared, output.read_bytes())


def _audio_box(content, name):
    moov = lineage._one(lineage._boxes(content), b"moov")
    for trak in [box for box in lineage._children(content, moov) if box[0] == b"trak"]:
        track_parts = lineage._children(content, trak)
        mdia = lineage._one(track_parts, b"mdia")
        mdia_parts = lineage._children(content, mdia)
        handler = lineage._payload(content, lineage._one(mdia_parts, b"hdlr"))
        if handler[8:12] != b"soun":
            continue
        if name == b"tkhd":
            return lineage._one(track_parts, name)
        if name == b"elst":
            return lineage._one(lineage._children(content, lineage._one(track_parts, b"edts")), name)
        minf_parts = lineage._children(content, lineage._one(mdia_parts, b"minf"))
        if name == b"smhd":
            return lineage._one(minf_parts, name)
        stbl = lineage._one(minf_parts, b"stbl")
        return lineage._one(lineage._children(content, stbl), name)
    pytest.fail("missing fixture audio")


@pytest.mark.parametrize("box,offset,value", [
    (b"elst", 12, 0), (b"elst", 12, 2048), (b"elst", 16, 131072),
    (b"stts", 8, 2000000), (b"stts", 12, 0), (b"stts", 12, 2048),
    (b"stsz", 8, 2000000), (b"stsz", 12, 2000000),
    (b"stsc", 8, 0), (b"stsc", 12, 0), (b"stsc", 16, 2),
    (b"stco", 8, 0), (b"tkhd", 0, 0),
])
def test_malformed_packet_tables_and_changed_priming_fail_closed(prepared, box, offset, value):
    content = bytearray(prepared["final"].read_bytes())
    target = _audio_box(content, box)
    struct.pack_into(">I", content, target[1] + offset, value)
    with pytest.raises(VideoProviderError):
        _verify(prepared, bytes(content))


def test_duplicate_packet_offset_is_rejected(prepared):
    content = bytearray(prepared["final"].read_bytes())
    target = _audio_box(content, b"stco")
    count = struct.unpack_from(">I", content, target[1] + 4)[0]
    assert count > 1
    content[target[1] + 12:target[1] + 16] = content[target[1] + 8:target[1] + 12]
    with pytest.raises(VideoProviderError):
        _verify(prepared, bytes(content))


@pytest.mark.parametrize("offset,value,size", [
    (20, 32768, 4), (20, 131072, 4), (24, 0, 2), (24, 128, 2),
    (16, 1, 4), (16, 2000000, 4), (36, 131072, 4), (4, 0, 0),
])
def test_movie_presentation_controls_cannot_change_accepted_audio(prepared, offset, value, size):
    content = bytearray(prepared["final"].read_bytes())
    moov = lineage._one(lineage._boxes(content), b"moov")
    mvhd = lineage._one(lineage._children(content, moov), b"mvhd")
    if size:
        struct.pack_into(">I" if size == 4 else ">H", content, mvhd[1] + offset, value)
    else:
        # A version-one header cannot reuse version-zero byte offsets.
        content[mvhd[1]] = 1
    with pytest.raises(VideoProviderError):
        _verify(prepared, bytes(content))


@pytest.mark.parametrize("offset,value", [(0, 1), (0, 16777216), (4, 16777216), (4, 4294901760), (4, 1)])
def test_sound_header_balance_flags_and_reserved_fields_are_canonical(prepared, offset, value):
    content = bytearray(prepared["final"].read_bytes())
    target = _audio_box(content, b"smhd")
    struct.pack_into(">I", content, target[1] + offset, value)
    with pytest.raises(VideoProviderError) as exc:
        _verify(prepared, bytes(content))
    assert exc.value.code == "audio_lineage_sound_presentation"


def test_unknown_audio_presentation_box_rejected(prepared):
    content = bytearray(prepared["final"].read_bytes())
    target = _audio_box(content, b"smhd")
    content[target[1] - 4:target[1]] = b"vmhd"
    with pytest.raises(VideoProviderError) as exc:
        _verify(prepared, bytes(content))
    assert exc.value.code == "audio_lineage_sound_presentation"


@pytest.mark.parametrize("field,value", [("first_name", "Jim"), ("source_sha256", "0" * 64),
    ("body_pcm_sha256", "0" * 64), ("joined_pcm_sha256", "0" * 64), ("joined_sample_count", 1),
    ("full_pcm_sha256", "0" * 64), ("greeting_pcm_sha256", "0" * 64), ("greeting_seconds", 1.0),
    ("checked_audio_sha256", "0" * 64), ("lineage_version", "future-policy")])
def test_misbound_manifest_rejected(prepared, field, value):
    manifest = copy.deepcopy(prepared["plan"]["manifest"])
    manifest[field] = value
    with pytest.raises(VideoProviderError):
        _verify(prepared, manifest=manifest)


@pytest.mark.parametrize("field,value", [("expected_name", "Jim"), ("normalized_transcript", "hey jim"),
    ("source_audio_sha256", "0" * 64), ("accepted", False), ("config_sha256", "0" * 64),
    ("confidence", [{"avg_logprob": -2.0, "no_speech_prob": 0.01}])])
def test_wrong_or_unapproved_speech_proof_rejected(prepared, field, value):
    proof = copy.deepcopy(prepared["proof"])
    proof[field] = value
    with pytest.raises(VideoProviderError) as exc:
        _verify(prepared, speech_proof=proof)
    assert exc.value.code == "audio_lineage_speech_proof_invalid"


def test_different_preparation_episode_rejected(prepared):
    metadata = {**prepared["metadata"], "first_name": "Jim"}
    with pytest.raises(VideoProviderError):
        _verify(prepared, expected_preparation=metadata)


@pytest.mark.parametrize("bad", [b"", b"123", b"\0\0\0\0moov", b"\0\0\0\x07moov", b"\0\0\0\x20moov",
    b"\0\0\0\x01moov", b"\0\0\0\x01moov" + struct.pack(">Q", 2 ** 63),
    struct.pack(">I4s", 8, b"moof"), struct.pack(">I4s", 8, b"moov") * 2])
def test_malformed_and_fragmented_mp4s_fail_closed(bad):
    with pytest.raises(VideoProviderError):
        lineage.inspect_aac_track(bad, video_tracks=0)


def test_unapproved_original_recording_is_rejected_without_decode(monkeypatch):
    monkeypatch.setattr(lineage, "_run", lambda *args: pytest.fail("must not decode"))
    with pytest.raises(VideoProviderError) as exc:
        lineage.prepare_audio_plan(b"other-recording", b"greeting", {}, first_name="Sue")
    assert exc.value.code == "unapproved_recording"


def test_lineage_contract_and_recognizer_are_separate():
    assert lineage.LINEAGE_VERSION == "planned-aac-same-take-v1"
    assert not hasattr(lineage, "verify_greeting_words")
