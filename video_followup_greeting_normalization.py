"""Versioned removal of bounded, very-low-amplitude PCM edge padding only.

This module is not a speech/acoustic validator. Its output must still pass every
existing guard. Nothing here calls a provider or changes an existing pipeline.
"""

from array import array
import hashlib
import json
import struct
import sys

from video_followup_provider import VideoProviderError


NORMALIZATION_VERSION = "bounded-noise-edge-padding-v1"
NORMALIZATION_CONFIG = {
    "version": NORMALIZATION_VERSION,
    "format": "riff-wave-pcm16-fmt16-or-fmt18-cbsize0-one-data-v1",
    "allowed_metadata_chunks": ["JUNK", "LIST/INFO", "C2PA"],
    "maximum_metadata_bytes": 65536,
    "maximum_c2pa_bytes": 1048576,
    "maximum_c2pa_chunks": 1,
    "maximum_chunks": 64,
    "maximum_info_fields": 64,
    "info_field_identifier_rule": "I-followed-by-three-uppercase-ascii-letters",
    "metadata_preservation": "byte-identical-original-order-except-drop-C2PA-on-audio-change",
    "c2pa_removal_proof": "sha256-of-complete-original-chunk-including-padding",
    "maximum_wav_bytes": 2 * 1024 * 1024,
    "sample_rates": [16000, 22050, 24000, 32000, 44100, 48000],
    "channels": [1, 2],
    "maximum_raw_seconds": 10,
    "maximum_normalized_seconds": 3,
    "pass_through_at_or_below_seconds": 3,
    "edge_noise_run_strictly_greater_than_ms": 200,
    "retain_leading_padding_ms": 50,
    "retain_trailing_padding_ms": 100,
    "padding_rounding": "ceil-to-complete-frame",
    "edge_noise_rule": "every-original-channel-absolute-pcm16-sample-at-or-below-32",
    "edge_noise_absolute_threshold_pcm16": 32,
    "interior_sample_removal": False,
    "resample_or_time_stretch": False,
    "clipped_sample_absolute_threshold_pcm16": 32736,
    "maximum_clipped_sample_fraction": "0.001",
}
_CONFIG_BYTES = json.dumps(NORMALIZATION_CONFIG, sort_keys=True, separators=(",", ":")).encode()
NORMALIZATION_CONFIG_SHA256 = hashlib.sha256(_CONFIG_BYTES).hexdigest()
_RATES = frozenset(NORMALIZATION_CONFIG["sample_rates"])


def _fail(code, message):
    raise VideoProviderError(code, message)


def _check_info(payload):
    if payload[:4] != b"INFO":
        _fail("invalid_narration_audio", "Only non-playback LIST/INFO metadata is supported.")
    offset, fields = 4, 0
    while offset < len(payload):
        if len(payload) - offset < 8:
            _fail("invalid_narration_audio", "Audio contains truncated INFO metadata.")
        field = payload[offset:offset + 4]
        size = struct.unpack_from("<I", payload, offset + 4)[0]
        end = offset + 8 + size + size % 2
        # INFO uses I-prefixed text metadata identifiers, not nested audio or
        # RIFF/LIST containers. Never interpret metadata as another PCM stream.
        if (field[:1] != b"I" or not all(65 <= value <= 90 for value in field)
                or end > len(payload) or fields >= 64):
            _fail("invalid_narration_audio", "Audio contains unsupported or incomplete INFO metadata.")
        offset, fields = end, fields + 1


def _parse(content):
    if (type(content) is not bytes or not 44 <= len(content) <= 2 * 1024 * 1024
            or content[:4] != b"RIFF" or content[8:12] != b"WAVE"
            or struct.unpack_from("<I", content, 4)[0] != len(content) - 8):
        _fail("invalid_narration_audio", "Audio must be one complete bounded RIFF/WAVE container.")
    fmt, data, offset, chunks, metadata_bytes = None, None, 12, [], 0
    c2pa = None
    while offset < len(content):
        if len(content) - offset < 8 or len(chunks) >= 64:
            _fail("invalid_narration_audio", "Audio has truncated or excessive RIFF chunks.")
        kind = content[offset:offset + 4]
        size = struct.unpack_from("<I", content, offset + 4)[0]
        payload_start, payload_end = offset + 8, offset + 8 + size
        chunk_end = payload_end + size % 2
        if chunk_end > len(content):
            _fail("invalid_narration_audio", "Audio contains a truncated RIFF chunk or padding.")
        if kind == b"fmt ":
            if fmt is not None or data is not None or size not in (16, 18):
                _fail("invalid_narration_audio", "Audio must contain one PCM format chunk before data.")
            if size == 18 and content[payload_end - 2:payload_end] != b"\x00\x00":
                _fail("invalid_narration_audio", "PCM format extensions must be empty.")
            fmt = struct.unpack_from("<HHIIHH", content, payload_start)
        elif kind == b"data":
            if data is not None or fmt is None:
                _fail("invalid_narration_audio", "Audio must contain exactly one data chunk after its format.")
            data = (offset, payload_start, payload_end, chunk_end)
        elif kind in (b"JUNK", b"LIST"):
            metadata_bytes += chunk_end - offset
            if metadata_bytes > 65536:
                _fail("invalid_narration_audio", "Audio metadata exceeds the bounded size limit.")
            if kind == b"LIST":
                _check_info(content[payload_start:payload_end])
        elif kind == b"C2PA":
            if c2pa is not None or size > 1048576:
                _fail("invalid_narration_audio", "Audio contains duplicate or unbounded C2PA provenance.")
            c2pa = (offset, chunk_end)
        else:
            _fail("invalid_narration_audio", "Audio contains an unsupported playback or metadata chunk.")
        chunks.append((kind, offset, chunk_end))
        offset = chunk_end
    if fmt is None or data is None:
        _fail("invalid_narration_audio", "Audio must contain one format and one data chunk.")
    kind, channels, rate, byte_rate, alignment, bits = fmt
    data_offset, data_start, data_end, data_chunk_end = data
    size = data_end - data_start
    if (kind != 1 or channels not in (1, 2) or rate not in _RATES or bits != 16
            or alignment != channels * 2 or byte_rate != rate * alignment
            or size == 0 or size % alignment):
        _fail("invalid_narration_audio", "Audio must be complete PCM16 mono/stereo with exactly one data chunk.")
    frames = size // alignment
    if frames > 10 * rate:
        _fail("invalid_narration_audio", "Raw audio exceeds the bounded ten-second input limit.")
    samples = array("h")
    samples.frombytes(content[data_start:data_end])
    if sys.byteorder != "little":
        samples.byteswap()
    return samples, channels, rate, frames, data, chunks, c2pa


def _edge_frames(samples, channels, frames):
    first, end = 0, frames
    while first < frames and all(abs(samples[first * channels + channel]) <= 32 for channel in range(channels)):
        first += 1
    while end > first and all(abs(samples[(end - 1) * channels + channel]) <= 32 for channel in range(channels)):
        end -= 1
    return first, end


def _clipped_count(samples):
    return sum(abs(sample) >= 32736 for sample in samples)


def normalize_greeting_audio(content: bytes) -> dict:
    """Return deterministic audio_bytes/provenance, or an existing fail-closed code.

    Every original-channel sample above the bounded floor and every interior
    pause is retained. Only over-three-second inputs are eligible for removal.
    """
    if json.dumps(NORMALIZATION_CONFIG, sort_keys=True, separators=(",", ":")).encode() != _CONFIG_BYTES:
        _fail("invalid_narration_audio", "Greeting normalization configuration changed.")
    samples, channels, rate, frames, data, chunks, c2pa = _parse(content)
    clipped = _clipped_count(samples)
    if clipped * 1000 > len(samples):
        _fail("clipped_narration_audio", "Generated speech has excessive raw clipping.")
    first, end = _edge_frames(samples, channels, frames)
    trailing = frames - end
    start_frame, end_frame = 0, frames
    if frames > 3 * rate:
        if first == frames:
            _fail("silent_narration_audio", "A long silent or noise-only recording is not a greeting.")
        if first * 1000 > 200 * rate:
            start_frame = first - (50 * rate + 999) // 1000
        if trailing * 1000 > 200 * rate:
            end_frame = end + (100 * rate + 999) // 1000
        if end_frame - start_frame > 3 * rate:
            _fail("greeting_too_long", "The complete above-noise-floor greeting envelope exceeds three seconds.")
    normalized_frames = end_frame - start_frame
    if clipped * 1000 > normalized_frames * channels:
        _fail("clipped_narration_audio", "Removing silence cannot dilute normalized-audio clipping.")
    alignment = channels * 2
    data_offset, data_start, data_end, data_chunk_end = data
    pcm = content[data_start + start_frame * alignment:data_start + end_frame * alignment]
    normalized_data_start = data_start
    removed_c2pa_sha256 = None
    if start_frame == 0 and end_frame == frames:
        normalized = content
    else:
        body = bytearray(b"WAVE")
        for kind, offset, end_offset in chunks:
            if kind == b"C2PA":
                # A signature for the raw audio must not appear to authenticate
                # changed audio. Retain it only in the separately stored raw WAV.
                removed_c2pa_sha256 = hashlib.sha256(content[offset:end_offset]).hexdigest()
            elif kind == b"data":
                normalized_data_start = 8 + len(body) + 8
                body.extend(b"data" + struct.pack("<I", len(pcm)) + pcm)
            else:
                body.extend(content[offset:end_offset])
        normalized = b"RIFF" + struct.pack("<I", len(body)) + bytes(body)
    removed = samples[:start_frame * channels] + samples[end_frame * channels:]
    discarded_max = max((abs(sample) for sample in removed), default=0)
    if discarded_max > 32:
        _fail("invalid_narration_audio", "Normalization attempted to remove a sample above its noise bound.")
    provenance = {
        "version": NORMALIZATION_VERSION,
        "config": json.loads(_CONFIG_BYTES),
        "config_sha256": NORMALIZATION_CONFIG_SHA256,
        "raw_wav_sha256": hashlib.sha256(content).hexdigest(),
        "normalized_wav_sha256": hashlib.sha256(normalized).hexdigest(),
        "raw_pcm_sha256": hashlib.sha256(content[data_start:data_end]).hexdigest(),
        "normalized_pcm_sha256": hashlib.sha256(pcm).hexdigest(),
        "raw_wav_bytes": len(content),
        "normalized_wav_bytes": len(normalized),
        "channels": channels,
        "sample_rate": rate,
        "sample_width_bytes": 2,
        "raw_pcm_start_byte": data_start,
        "raw_pcm_end_byte_exclusive": data_end,
        "normalized_pcm_start_byte": normalized_data_start,
        "normalized_pcm_end_byte_exclusive": normalized_data_start + len(pcm),
        "retained_non_audio_chunks_byte_identical": True,
        "raw_c2pa_chunk_sha256": hashlib.sha256(content[c2pa[0]:c2pa[1]]).hexdigest() if c2pa else None,
        "removed_c2pa_chunk_sha256": removed_c2pa_sha256,
        "c2pa_action": "removed_after_audio_change" if removed_c2pa_sha256 else "unchanged",
        "raw_frames": frames,
        "normalized_frames": normalized_frames,
        "raw_duration_seconds": frames / rate,
        "normalized_duration_seconds": normalized_frames / rate,
        "leading_bounded_noise_frames": first,
        "trailing_bounded_noise_frames": trailing,
        "retained_start_frame": start_frame,
        "retained_end_frame_exclusive": end_frame,
        "removed_leading_frames": start_frame,
        "removed_trailing_frames": frames - end_frame,
        "original_clipped_samples": clipped,
        "normalized_clipped_samples": clipped,
        "byte_identical_pass_through": normalized == content,
        "all_above_noise_floor_original_channel_samples_retained": True,
        "maximum_discarded_absolute_pcm16": discarded_max,
        "interior_samples_removed": 0,
        "speech_or_acoustic_acceptance_claimed": False,
    }
    return {"audio_bytes": normalized, "provenance": provenance}
