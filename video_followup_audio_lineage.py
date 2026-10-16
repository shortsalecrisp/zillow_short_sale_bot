"""Bind one pre-encoded AAC track to its checked greeting.

This is not a speech recognizer and cannot approve a name. A caller must run the
unchanged speech guard on ``greeting_audio_bytes`` before paying for lip-sync.
The final video must stream-copy the planned AAC track; re-encoding is rejected.
Only unfragmented, local, single-track AAC-LC/48 kHz/mono MP4 audio is supported.
Both containers use a 48 kHz movie clock: a 1 kHz edit-list clock can round and
change the last AAC packet duration during an otherwise lossless stream copy.
No provider, storage, job, SMS, or network operation exists in this module.
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import struct
import tempfile
import wave

import numpy as np

import video_followup_same_take_media as media
from video_followup_provider import VideoProviderError, _ffmpeg_executable, _run
from video_followup_speech_guard import validate_speech_proof


LINEAGE_VERSION = "planned-aac-same-take-v1"
EXTRACTION_VERSION = "ffmpeg-pcm-s16le-mono-16k-v1"
MAX_PACKETS = 1024
MAX_BOXES = 4096
MAX_PCM_BYTES = 12 * media.SAMPLE_RATE * 2
IDENTITY_MATRIX = struct.pack(">9I", 65536, 0, 0, 0, 65536, 0, 0, 0, 1073741824)


def _fail(code="audio_lineage_invalid"):
    raise VideoProviderError(code, "The planned and final audio lineage did not match.")


def _hash(value):
    return hashlib.sha256(value).hexdigest()


def _digest(value):
    return _hash(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())


def _boxes(content, start=0, end=None):
    end = len(content) if end is None else end
    result = []
    while start < end:
        if end - start < 8 or len(result) >= MAX_BOXES:
            _fail()
        length, kind = struct.unpack_from(">I4s", content, start)
        header = 8
        if length == 1:
            if end - start < 16:
                _fail()
            length = struct.unpack_from(">Q", content, start + 8)[0]
            header = 16
        if length < header or start + length > end:
            _fail()
        result.append((kind, start + header, start + length))
        start += length
    if start != end:
        _fail()
    return result


def _one(boxes, kind, *, optional=False):
    matches = [box for box in boxes if box[0] == kind]
    if not matches and optional:
        return None
    if len(matches) != 1:
        _fail()
    return matches[0]


def _children(content, box):
    return _boxes(content, box[1], box[2])


def _payload(content, box):
    return content[box[1]:box[2]]


def _full_table(content, box, entry_size, maximum=MAX_PACKETS):
    raw = _payload(content, box)
    if len(raw) < 8 or raw[:4] != b"\0" * 4:
        _fail()
    count = struct.unpack_from(">I", raw, 4)[0]
    if not 0 < count <= maximum or len(raw) != 8 + count * entry_size:
        _fail()
    return raw[8:], count


def _descriptor(raw, offset):
    if offset >= len(raw):
        _fail()
    tag, offset, length = raw[offset], offset + 1, 0
    for _ in range(4):
        if offset >= len(raw):
            _fail()
        item, offset = raw[offset], offset + 1
        length = (length << 7) | (item & 127)
        if not item & 128:
            if offset + length > len(raw):
                _fail()
            return tag, raw[offset:offset + length], offset + length
    _fail()


def _aac_config(esds):
    if esds[:4] != b"\0" * 4:
        _fail()
    tag, es, end = _descriptor(esds, 4)
    if tag != 3 or end != len(esds) or len(es) < 3 or es[2] != 0:
        _fail()
    tag, decoder, offset = _descriptor(es, 3)
    # MPEG-4 Audio, audio stream, no upstream flag, reserved bit set.
    if tag != 4 or len(decoder) < 13 or decoder[:2] != b"\x40\x15":
        _fail()
    tag, config, config_end = _descriptor(decoder, 13)
    if tag != 5 or config_end != len(decoder) or len(config) < 2:
        _fail()
    bits = int.from_bytes(config[:2], "big")
    if (bits >> 11, (bits >> 7) & 15, (bits >> 3) & 15) != (2, 3, 1):
        _fail("audio_lineage_codec_changed")
    tag, sl, end = _descriptor(es, offset)
    if tag != 6 or sl != b"\x02" or end != len(es):
        _fail()
    return config


def _movie_clock(content, movie):
    children = _children(content, movie)
    raw = _payload(content, _one(children, b"mvhd"))
    # FFmpeg decoding ignores some QuickTime presentation controls. Reject
    # those controls rather than treating decoded PCM equality as sufficient.
    if (len(raw) != 100 or raw[:4] != b"\0" * 4
            or raw[20:26] != b"\0\x01\0\0\x01\0"
            or raw[26:36] != b"\0" * 10 or raw[36:72] != IDENTITY_MATRIX
            or raw[72:96] != b"\0" * 24):
        _fail("audio_lineage_movie_presentation")
    clock, duration = struct.unpack_from(">II", raw, 12)
    if clock != media.SAMPLE_RATE or not 0 < duration <= 12 * clock:
        _fail("audio_lineage_movie_timing")
    track_durations, track_ids = [], []
    for track in [box for box in children if box[0] == b"trak"]:
        header = _payload(content, _one(_children(content, track), b"tkhd"))
        if len(header) != 84 or header[:4] != b"\0\0\0\x03":
            _fail("audio_lineage_track_flags")
        track_ids.append(struct.unpack_from(">I", header, 12)[0])
        track_durations.append(struct.unpack_from(">I", header, 20)[0])
    if (not track_ids or len(set(track_ids)) != len(track_ids) or min(track_ids) < 1
            or struct.unpack_from(">I", raw, 96)[0] <= max(track_ids)
            or not all(0 < value <= duration for value in track_durations)
            or duration != max(track_durations)):
        _fail("audio_lineage_movie_timing")
    return clock


def inspect_aac_track(content, *, video_tracks):
    """Read packet bytes and exact playback metadata, not ffmpeg's rounded log."""
    media._bounded(content, "Audio lineage media")
    top = _boxes(content)
    if any(box[0] in {b"moof", b"sidx", b"mfra"} for box in top):
        _fail("audio_lineage_fragmented")
    movie = _one(top, b"moov")
    parts = _children(content, movie)
    if any(box[0] in {b"mvex", b"cmov"} for box in parts):
        _fail()
    tracks = []
    for track in [box for box in parts if box[0] == b"trak"]:
        children = _children(content, track)
        mdia = _one(children, b"mdia")
        handler = _payload(content, _one(_children(content, mdia), b"hdlr"))
        if len(handler) < 12 or handler[:4] != b"\0" * 4:
            _fail()
        tracks.append((handler[8:12], track, mdia))
    if [kind for kind, _, _ in tracks].count(b"soun") != 1 or [kind for kind, _, _ in tracks].count(b"vide") != video_tracks or len(tracks) != video_tracks + 1:
        _fail("audio_lineage_track_count")
    _, track, mdia = next(item for item in tracks if item[0] == b"soun")
    children = _children(content, track)
    if any(box[0] not in {b"tkhd", b"edts", b"mdia", b"udta"} for box in children):
        _fail()
    header = _payload(content, _one(children, b"tkhd"))
    if len(header) != 84 or header[:4] != b"\0\0\0\x03":
        _fail("audio_lineage_track_flags")
    # Track ID and creation dates may change at muxing; timing and presentation may not.
    presentation = header[20:]
    if header[36:40] != b"\x01\0\0\0" or header[-8:] != b"\0" * 8:
        _fail()
    mdia_parts = _children(content, mdia)
    if any(box[0] not in {b"mdhd", b"hdlr", b"minf"} for box in mdia_parts):
        _fail("audio_lineage_media_container")
    mdhd = _payload(content, _one(mdia_parts, b"mdhd"))
    if len(mdhd) != 24 or mdhd[:4] != b"\0" * 4:
        _fail()
    sample_rate, duration = struct.unpack_from(">II", mdhd, 12)
    if sample_rate != media.SAMPLE_RATE or not 0 < duration <= 12 * sample_rate:
        _fail("audio_lineage_timing")
    edits = _one(children, b"edts")
    edit_parts = _children(content, edits)
    if len(edit_parts) != 1:
        _fail()
    edit = _payload(content, _one(edit_parts, b"elst"))
    if len(edit) != 20 or edit[:8] != b"\0\0\0\0\0\0\0\x01":
        _fail("audio_lineage_edit_list")
    edit_duration, media_start, rate = struct.unpack_from(">IiI", edit, 8)
    if media_start != 1024 or rate != 65536 or not edit_duration:
        _fail("audio_lineage_edit_list")
    minf = _one(mdia_parts, b"minf")
    minf_parts = _children(content, minf)
    if (any(box[0] not in {b"smhd", b"dinf", b"stbl"} for box in minf_parts)
            or _payload(content, _one(minf_parts, b"smhd")) != b"\0" * 8):
        _fail("audio_lineage_sound_presentation")
    dinf = _one(minf_parts, b"dinf")
    references = _children(content, dinf)
    if len(references) != 1:
        _fail("audio_lineage_external_data")
    data_ref = _payload(content, _one(references, b"dref"))
    if data_ref != b"\0\0\0\0\0\0\0\x01\0\0\0\x0curl\x20\0\0\0\x01":
        _fail("audio_lineage_external_data")
    stbl = _children(content, _one(minf_parts, b"stbl"))
    allowed = {b"stsd", b"stts", b"stsc", b"stsz", b"stco", b"co64", b"sgpd", b"sbgp"}
    if any(box[0] not in allowed for box in stbl):
        _fail("audio_lineage_sample_table")
    stsd = _payload(content, _one(stbl, b"stsd"))
    if len(stsd) < 44 or stsd[:8] != b"\0\0\0\0\0\0\0\x01":
        _fail()
    entries = _boxes(stsd, 8)
    entry = _one(entries, b"mp4a")
    if len(entries) != 1 or entry[2] - entry[1] < 28:
        _fail()
    audio_header = stsd[entry[1]:entry[1] + 28]
    if audio_header[:16] != b"\0" * 6 + b"\0\x01" + b"\0" * 8 or audio_header[18:28] != b"\0\x10\0\0\0\0\xbb\x80\0\0":
        _fail("audio_lineage_codec_changed")
    extension = _boxes(stsd, entry[1] + 28, entry[2])
    if any(box[0] not in {b"esds", b"btrt"} for box in extension):
        _fail()
    config = _aac_config(_payload(stsd, _one(extension, b"esds")))
    stts, count = _full_table(content, _one(stbl, b"stts"), 8)
    durations = []
    for index in range(count):
        repeats, delta = struct.unpack_from(">II", stts, index * 8)
        if not 0 < repeats <= MAX_PACKETS or not 0 < delta <= 1024 or len(durations) + repeats > MAX_PACKETS:
            _fail()
        durations.extend([delta] * repeats)
    sizes_raw = _payload(content, _one(stbl, b"stsz"))
    if len(sizes_raw) < 12 or sizes_raw[:8] != b"\0" * 8:
        _fail()
    packet_count = struct.unpack_from(">I", sizes_raw, 8)[0]
    if packet_count != len(durations) or len(sizes_raw) != 12 + 4 * packet_count or sum(durations) != duration:
        _fail("audio_lineage_timing")
    sizes = struct.unpack_from(f">{packet_count}I", sizes_raw, 12)
    if any(not 0 < size <= 8192 for size in sizes):
        _fail()
    stsc, count = _full_table(content, _one(stbl, b"stsc"), 12)
    layout = [struct.unpack_from(">III", stsc, index * 12) for index in range(count)]
    if layout[0][0] != 1 or any(not 0 < n <= MAX_PACKETS or desc != 1 for _, n, desc in layout) or any(a[0] >= b[0] for a, b in zip(layout, layout[1:])):
        _fail()
    chunks_box = _one(stbl, b"stco", optional=True)
    chunks64 = _one(stbl, b"co64", optional=True)
    if bool(chunks_box) == bool(chunks64):
        _fail()
    chunks, count = _full_table(content, chunks_box or chunks64, 4 if chunks_box else 8)
    offsets = struct.unpack(f">{count}{'I' if chunks_box else 'Q'}", chunks)
    if layout[-1][0] > count:
        _fail()
    ranges = [(box[1], box[2]) for box in top if box[0] == b"mdat"]
    packets, spans, layout_index, packet_index = [], [], 0, 0
    for chunk_index, offset in enumerate(offsets, 1):
        if layout_index + 1 < len(layout) and chunk_index == layout[layout_index + 1][0]:
            layout_index += 1
        for _ in range(layout[layout_index][1]):
            if packet_index >= packet_count:
                _fail()
            end = offset + sizes[packet_index]
            if not any(start <= offset < end <= stop for start, stop in ranges):
                _fail()
            spans.append((offset, end))
            packets.append(_hash(content[offset:end]))
            offset, packet_index = end, packet_index + 1
    if packet_index != packet_count or any(left[1] > right[0] for left, right in zip(sorted(spans), sorted(spans)[1:])):
        _fail()
    groups = {box[0].decode(): _hash(_payload(content, box)) for box in stbl if box[0] in {b"sgpd", b"sbgp"}}
    if len(groups) != len([box for box in stbl if box[0] in {b"sgpd", b"sbgp"}]):
        _fail()
    return {"codec": "aac-lc", "channels": 1, "sample_rate": sample_rate,
        "audio_specific_config_hex": config.hex(), "sample_entry_channels": int.from_bytes(audio_header[16:18], "big"),
        "packet_count": packet_count, "packet_sha256": packets, "packet_samples": durations,
        "media_duration_samples": duration, "movie_timescale": _movie_clock(content, movie),
        "track_presentation_hex": presentation.hex(), "edit_list_hex": edit.hex(),
        "language_hex": mdhd[20:].hex(), "sample_groups": groups}


def _decode_audio(content, *, ffmpeg, greeting_seconds=None):
    with tempfile.TemporaryDirectory(prefix="crisp-audio-lineage-") as directory:
        source, target = Path(directory) / "input.mp4", Path(directory) / "checked.wav"
        source.write_bytes(content)
        command = [ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "1",
            "-protocol_whitelist", "file,pipe", "-i", str(source), "-map", "0:a:0"]
        if greeting_seconds is not None:
            command += ["-t", str(greeting_seconds)]
        command += ["-vn", "-ac", "1", "-ar", "16000" if greeting_seconds is not None else "48000",
            "-c:a", "pcm_s16le", "-fs", str(MAX_PCM_BYTES), str(target)]
        _run(command)
        audio = target.read_bytes()
        with wave.open(io.BytesIO(audio), "rb") as wav:
            rate = 16000 if greeting_seconds is not None else media.SAMPLE_RATE
            if (wav.getnchannels(), wav.getsampwidth(), wav.getframerate(), wav.getcomptype()) != (1, 2, rate, "NONE"):
                _fail()
            pcm = wav.readframes(wav.getnframes())
            if not 0 < len(pcm) < MAX_PCM_BYTES or len(pcm) % 2:
                _fail()
        return audio, pcm


def prepare_audio_plan(master_bytes, greeting_audio_bytes, metadata, *, first_name, ffmpeg=None):
    """Encode the eventual full AAC audio before lip-sync or name recognition."""
    media._master(master_bytes)
    media._validate_metadata(metadata, greeting_audio_bytes, first_name)
    greeting = media._read_greeting(greeting_audio_bytes, metadata["greeting_frames"])
    ffmpeg = ffmpeg or _ffmpeg_executable()
    with tempfile.TemporaryDirectory(prefix="crisp-planned-audio-") as directory:
        root = Path(directory)
        master, source, encoded = root / "master.mp4", root / "full.wav", root / "planned.m4a"
        master.write_bytes(master_bytes)
        original = media._audio(master, ffmpeg)
        body = original[media.SOURCE_GREETING_FRAMES * media.SAMPLES_PER_FRAME:]
        if len(body) < (media.SOURCE_FRAMES - media.SOURCE_GREETING_FRAMES - 1) * media.SAMPLES_PER_FRAME:
            _fail("audio_lineage_source_short")
        joined = np.concatenate((greeting, body))
        source.write_bytes(media._wav(joined))
        _run([ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
            "-protocol_whitelist", "file,pipe", "-i", str(source), "-map", "0:a:0", "-vn",
            "-c:a", "aac", "-b:a", "160k", "-movie_timescale", str(media.SAMPLE_RATE),
            "-movflags", "+faststart", str(encoded)])
        audio_bytes = encoded.read_bytes()
    track = inspect_aac_track(audio_bytes, video_tracks=0)
    _, pcm = _decode_audio(audio_bytes, ffmpeg=ffmpeg)
    checked, greeting_pcm = _decode_audio(audio_bytes, ffmpeg=ffmpeg, greeting_seconds=metadata["greeting_seconds"])
    manifest = {"lineage_version": LINEAGE_VERSION, "extraction_version": EXTRACTION_VERSION,
        "first_name": first_name, "source_sha256": media.SOURCE_SHA256,
        "preparation_sha256": _digest(metadata), "prepared_greeting_sha256": _hash(greeting_audio_bytes),
        "body_pcm_sha256": _hash(body.astype("<i2", copy=False).tobytes()),
        "joined_pcm_sha256": _hash(joined.astype("<i2", copy=False).tobytes()),
        "joined_sample_count": len(joined), "planned_audio_sha256": _hash(audio_bytes),
        "full_pcm_sha256": _hash(pcm), "full_sample_count": len(pcm) // 2,
        "checked_audio_sha256": _hash(checked), "greeting_pcm_sha256": _hash(greeting_pcm),
        "greeting_sample_count": len(greeting_pcm) // 2, "greeting_seconds": metadata["greeting_seconds"],
        "aac_track": track}
    return {"audio_bytes": audio_bytes, "greeting_audio_bytes": checked, "manifest": manifest,
        "manifest_sha256": _digest(manifest)}


def verify_final_audio(final_bytes, planned_audio_bytes, manifest, speech_proof, expected_preparation, *, first_name, expected_manifest_sha256, ffmpeg=None):
    """Prove the final decoded audio is exactly the pre-paid ASR-checked audio."""
    media._exact_name(first_name)
    if not isinstance(manifest, dict) or not isinstance(expected_preparation, dict):
        _fail()
    if _digest(manifest) != expected_manifest_sha256:
        _fail("audio_lineage_identity_mismatch")
    expected = {"lineage_version": LINEAGE_VERSION, "extraction_version": EXTRACTION_VERSION,
        "first_name": first_name, "source_sha256": media.SOURCE_SHA256,
        "preparation_sha256": _digest(expected_preparation),
        "prepared_greeting_sha256": expected_preparation.get("greeting_audio_sha256"),
        "greeting_seconds": expected_preparation.get("greeting_seconds"),
        "planned_audio_sha256": _hash(planned_audio_bytes)}
    if any(manifest.get(key) != value for key, value in expected.items()):
        _fail("audio_lineage_identity_mismatch")
    if not validate_speech_proof(speech_proof, first_name=first_name, audio_sha256=manifest.get("checked_audio_sha256")):
        _fail("audio_lineage_speech_proof_invalid")
    planned = inspect_aac_track(planned_audio_bytes, video_tracks=0)
    final = inspect_aac_track(final_bytes, video_tracks=1)
    if planned != manifest.get("aac_track") or final != planned:
        _fail("audio_lineage_track_mismatch")
    ffmpeg = ffmpeg or _ffmpeg_executable()
    for content in (planned_audio_bytes, final_bytes):
        _, pcm = _decode_audio(content, ffmpeg=ffmpeg)
        checked, greeting_pcm = _decode_audio(content, ffmpeg=ffmpeg, greeting_seconds=manifest["greeting_seconds"])
        if (_hash(pcm) != manifest.get("full_pcm_sha256") or len(pcm) // 2 != manifest.get("full_sample_count")
                or _hash(checked) != manifest.get("checked_audio_sha256")
                or _hash(greeting_pcm) != manifest.get("greeting_pcm_sha256")
                or len(greeting_pcm) // 2 != manifest.get("greeting_sample_count")):
            _fail("audio_lineage_decoded_mismatch")
    return {"accepted": True, "lineage_version": LINEAGE_VERSION, "first_name": first_name,
        "manifest_sha256": _digest(manifest), "planned_audio_sha256": _hash(planned_audio_bytes),
        "final_media_sha256": _hash(final_bytes), "checked_audio_sha256": manifest["checked_audio_sha256"],
        "full_pcm_sha256": manifest["full_pcm_sha256"], "greeting_pcm_sha256": manifest["greeting_pcm_sha256"],
        "aac_track_sha256": _digest(planned), "speech_proof_sha256": _digest(speech_proof)}


def mux_audio_track(silent_video_bytes, planned_audio_bytes, *, ffmpeg=None):
    """Copy a planned AAC track without approving its name or job identity."""
    media._bounded(silent_video_bytes, "Silent compositor video")
    top = _boxes(silent_video_bytes)
    if any(box[0] in {b"moof", b"sidx", b"mfra"} for box in top):
        _fail()
    movie = _one(top, b"moov")
    tracks = [box for box in _children(silent_video_bytes, movie) if box[0] == b"trak"]
    if len(tracks) != 1:
        _fail("audio_lineage_track_count")
    mdia = _one(_children(silent_video_bytes, tracks[0]), b"mdia")
    handler = _payload(silent_video_bytes, _one(_children(silent_video_bytes, mdia), b"hdlr"))
    if len(handler) < 12 or handler[8:12] != b"vide":
        _fail("audio_lineage_track_count")
    planned = inspect_aac_track(planned_audio_bytes, video_tracks=0)
    ffmpeg = ffmpeg or _ffmpeg_executable()
    with tempfile.TemporaryDirectory(prefix="crisp-lineage-mux-") as directory:
        root = Path(directory)
        video, audio, output = root / "silent.mp4", root / "planned.m4a", root / "final.mp4"
        video.write_bytes(silent_video_bytes)
        audio.write_bytes(planned_audio_bytes)
        _run([ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
            "-protocol_whitelist", "file,pipe", "-i", str(video), "-i", str(audio),
            "-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy", "-c:a", "copy",
            "-movie_timescale", str(media.SAMPLE_RATE), "-movflags", "+faststart", str(output)])
        final = output.read_bytes()
    if inspect_aac_track(final, video_tracks=1) != planned:
        _fail("audio_lineage_track_mismatch")
    _, planned_pcm = _decode_audio(planned_audio_bytes, ffmpeg=ffmpeg)
    _, final_pcm = _decode_audio(final, ffmpeg=ffmpeg)
    if planned_pcm != final_pcm:
        _fail("audio_lineage_decoded_mismatch")
    return final


def mux_planned_audio(silent_video_bytes, planned_audio_bytes, manifest, speech_proof, expected_preparation, *, first_name, expected_manifest_sha256, ffmpeg=None):
    """Mux a silent compositor result and validate its exact approved proof."""
    ffmpeg = ffmpeg or _ffmpeg_executable()
    final = mux_audio_track(silent_video_bytes, planned_audio_bytes, ffmpeg=ffmpeg)
    proof = verify_final_audio(final, planned_audio_bytes, manifest, speech_proof, expected_preparation,
        first_name=first_name, expected_manifest_sha256=expected_manifest_sha256, ffmpeg=ffmpeg)
    return {"media_bytes": final, "lineage_proof": proof}
