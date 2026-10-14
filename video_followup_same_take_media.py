"""Local, source-locked greeting edits. This module never calls a paid provider.

Avatar narration is decoded as audio only. Every image comes from the approved
second recording, except the tightly bounded mouth patch returned by lip-sync.
The rest of the original recording is preserved, with a time offset for names
whose greeting takes longer or less time than the original greeting.
"""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import io
import math
from pathlib import Path
import subprocess
import tempfile
import threading
import wave

import numpy as np

from video_followup_provider import (
    VideoProviderError, _ffmpeg_executable, _probe, _run, _timestamp,
    canonical_first_name,
)


SOURCE_SHA256 = "01efd28101e8b016d51d571cc87d38d13ea623389663bc7a88cf26cce39bd60b"
PREPARATION_VERSION = "crisp-second-take-mouth-v1"
WIDTH, HEIGHT, FPS, SAMPLE_RATE = 1024, 576, 30, 48000
SOURCE_FRAMES = 253
SOURCE_GREETING_FRAMES = 26
SOURCE_GREETING_SECONDS = SOURCE_GREETING_FRAMES / FPS
SAMPLES_PER_FRAME = SAMPLE_RATE // FPS
MIN_GREETING_FRAMES, MAX_GREETING_FRAMES = 15, 75
MAX_NARRATION_SECONDS = 3.0
MIN_CONTEXT_FRAMES, MAX_CONTEXT_FRAMES = 63, 150
FRAME_BYTES = WIDTH * HEIGHT * 3
PROCESS_SECONDS = 90
MAX_BYTES = 25 * 1024 * 1024
BACKGROUND_MAE_LIMIT = 18.0
MAX_PROVIDER_TIMELINE_DRIFT = 0.085
MOUTH_CENTER = (505, 281)
MOUTH_AXES = (82, 64)
SCRIPT_BODY = (
    "it's Yoni Kutler with Crisp Short Sales. Just wanted to put a face to my text. "
    "If you have any questions, just text me back. Happy to help."
)


def _fail(code: str, message: str):
    raise VideoProviderError(code, message)


def _hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _bounded(content: bytes, label: str) -> bytes:
    if not isinstance(content, bytes) or not 0 < len(content) <= MAX_BYTES:
        _fail("invalid_media_bytes", f"{label} must be nonempty bounded media bytes.")
    return content


def _master(content: bytes) -> bytes:
    _bounded(content, "Master recording")
    if _hash(content) != SOURCE_SHA256:
        _fail("unapproved_recording", "Only the approved full second recording may be used.")
    return content


def _exact_name(value: str) -> str:
    name = canonical_first_name(value)
    if name != value:
        _fail("noncanonical_first_name", "Resolve the verified first name before preparing media.")
    return name


@contextmanager
def _process(command: list[str], *, direction: str):
    # Bound both blocking pipe reads/writes and decoder failures without exposing
    # provider URLs or command output to a durable job error.
    with tempfile.TemporaryFile() as errors:
        try:
            process = subprocess.Popen(
                command, stdin=subprocess.PIPE if direction == "write" else subprocess.DEVNULL,
                stdout=subprocess.PIPE if direction == "read" else subprocess.DEVNULL,
                stderr=errors,
            )
        except OSError:
            _fail("media_processing_failed", "Unable to start the local media processor.")
        expired = threading.Event()

        def stop():
            expired.set()
            process.kill()

        timer = threading.Timer(PROCESS_SECONDS, stop)
        timer.daemon = True
        timer.start()
        raised = False
        try:
            yield process
        except BaseException:
            raised = True
            raise
        finally:
            if raised:
                process.kill()
            for pipe in (process.stdin, process.stdout):
                if pipe is not None:
                    try:
                        pipe.close()
                    except OSError:
                        pass
            result = process.wait()
            timer.cancel()
            if not raised and (expired.is_set() or result):
                _fail("media_processing_failed", "Local media processing did not complete successfully.")


def _frames(path: Path, ffmpeg: str, *, provider: bool = False):
    filters = f"fps={FPS},scale={WIDTH}:{HEIGHT}:flags=lanczos" if provider else "null"
    command = [
        ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "1",
        "-protocol_whitelist", "file,pipe", "-i", str(path), "-map", "0:v:0",
        "-vf", filters, "-an", "-f", "rawvideo", "-pix_fmt", "rgb24", "-",
    ]
    with _process(command, direction="read") as process:
        count = 0
        while True:
            chunks, size = [], 0
            while size < FRAME_BYTES:
                data = process.stdout.read(FRAME_BYTES - size)
                if not data:
                    break
                chunks.append(data)
                size += len(data)
            if not size:
                break
            if size != FRAME_BYTES or count > SOURCE_FRAMES + MAX_GREETING_FRAMES:
                _fail("invalid_video_frames", "Video has incomplete or too many decoded frames.")
            count += 1
            yield np.frombuffer(b"".join(chunks), dtype=np.uint8).reshape(HEIGHT, WIDTH, 3)


def _retimed_frames(path: Path, greeting_frames: int, ffmpeg: str):
    original = _frames(path, ffmpeg)
    try:
        greeting = []
        for _ in range(SOURCE_GREETING_FRAMES):
            try:
                greeting.append(next(original))
            except StopIteration:
                _fail("invalid_source_frames", "The full approved recording is incomplete.")
        for index in range(greeting_frames):
            # Keep both endpoints so the last edited frame meets the untouched
            # source body at the same physical head/camera position.
            position = index * (SOURCE_GREETING_FRAMES - 1) / (greeting_frames - 1)
            low, high = math.floor(position), math.ceil(position)
            fraction = position - low
            if low == high:
                yield greeting[low]
            else:
                yield np.rint(
                    greeting[low].astype(np.float32) * (1 - fraction)
                    + greeting[high].astype(np.float32) * fraction
                ).astype(np.uint8)
        del greeting
        count = SOURCE_GREETING_FRAMES
        for frame in original:
            count += 1
            yield frame
        if count != SOURCE_FRAMES:
            _fail("invalid_source_frames", "The approved recording frame count changed.")
    finally:
        original.close()


def _encode(frames, path: Path, ffmpeg: str, *, upscale: bool = False):
    command = [
        ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", f"{WIDTH}x{HEIGHT}",
        "-r", str(FPS), "-i", "-", "-an",
    ]
    if upscale:
        command += ["-vf", "scale=1280:720:flags=lanczos"]
    command += [
        "-c:v", "libx264", "-threads", "1", "-crf", "17", "-pix_fmt", "yuv420p",
        "-movflags", "+faststart", str(path),
    ]
    count = 0
    try:
        with _process(command, direction="write") as process:
            for frame in frames:
                if frame.shape != (HEIGHT, WIDTH, 3) or frame.dtype != np.uint8:
                    _fail("invalid_video_frames", "Source video frame geometry changed.")
                process.stdin.write(frame.tobytes())
                count += 1
    except (BrokenPipeError, OSError):
        _fail("media_processing_failed", "Local video encoding failed.")
    finally:
        close = getattr(frames, "close", None)
        if close:
            close()
    return count


def _audio(path: Path, ffmpeg: str, *, narration: bool = False) -> np.ndarray:
    filters = "aresample=async=1:first_pts=0"
    if narration:
        filters += ",highpass=f=70,loudnorm=I=-18:TP=-1.5:LRA=11"
    result = _run([
        ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error",
        "-protocol_whitelist", "file,pipe", "-i", str(path), "-map", "0:a:0", "-vn",
        "-t", "3.1" if narration else "10", "-af", filters,
        "-ac", "1", "-ar", str(SAMPLE_RATE), "-f", "s16le", "-",
    ])
    if not result.stdout or len(result.stdout) % 2:
        _fail("invalid_audio", "Media must contain usable audio.")
    samples = np.frombuffer(result.stdout, dtype="<i2")
    if narration and len(samples) > round(MAX_NARRATION_SECONDS * SAMPLE_RATE):
        _fail("greeting_too_long", "Narration is too long for the approved short greeting.")
    return samples


def _trim_greeting(samples: np.ndarray) -> tuple[np.ndarray, dict]:
    if samples.ndim != 1 or not len(samples):
        _fail("invalid_audio", "A nonempty mono greeting is required.")
    # Only remove pauses longer than 200 ms, retaining breathing room rather
    # than clipping the onset/decay of a short or unusual first name.
    block = SAMPLE_RATE // 100
    blocks = math.ceil(len(samples) / block)
    padded = np.pad(samples.astype(np.float32), (0, blocks * block - len(samples)))
    rms = np.sqrt(np.mean(padded.reshape(blocks, block) ** 2, axis=1))
    threshold = max(80.0, float(np.max(rms)) * 0.015)
    voiced = np.flatnonzero(rms > threshold)
    if not len(voiced):
        _fail("silent_greeting", "The generated greeting is silent.")
    first, last = int(voiced[0]) * block, min(len(samples), (int(voiced[-1]) + 1) * block)
    start = max(0, first - int(0.05 * SAMPLE_RATE)) if first > 0.2 * SAMPLE_RATE else 0
    end = min(len(samples), last + int(0.10 * SAMPLE_RATE)) if len(samples) - last > 0.2 * SAMPLE_RATE else len(samples)
    trimmed = samples[start:end]
    speech_end_sample = last - start
    frames = max(
        MIN_GREETING_FRAMES, math.ceil(len(trimmed) / SAMPLES_PER_FRAME),
        math.ceil(speech_end_sample / SAMPLES_PER_FRAME) + 4,
    )
    if frames > MAX_GREETING_FRAMES:
        _fail("greeting_too_long", "The name needs a longer greeting than this validated template supports.")
    greeting = np.pad(trimmed, (0, frames * SAMPLES_PER_FRAME - len(trimmed)))
    return greeting, {
        "narration_samples": len(samples), "leading_silence_trim_samples": start,
        "trailing_silence_trim_samples": len(samples) - end,
        "greeting_frames": frames, "greeting_seconds": frames / FPS,
        "speech_end_sample": speech_end_sample,
    }


def _wav(samples: np.ndarray) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as stream:
        stream.setnchannels(1)
        stream.setsampwidth(2)
        stream.setframerate(SAMPLE_RATE)
        stream.writeframes(samples.astype("<i2", copy=False).tobytes())
    return output.getvalue()


def _read_greeting(content: bytes, expected_frames: int) -> np.ndarray:
    _bounded(content, "Greeting audio")
    try:
        with wave.open(io.BytesIO(content), "rb") as stream:
            if (stream.getnchannels(), stream.getsampwidth(), stream.getframerate(), stream.getcomptype()) != (1, 2, SAMPLE_RATE, "NONE"):
                raise ValueError()
            samples = np.frombuffer(stream.readframes(stream.getnframes()), dtype="<i2")
    except (wave.Error, EOFError, ValueError):
        _fail("invalid_greeting_audio", "Prepared greeting audio changed format.")
    if len(samples) != expected_frames * SAMPLES_PER_FRAME:
        _fail("invalid_greeting_audio", "Prepared greeting audio changed length.")
    return samples


def _context_frames(greeting_frames: int) -> int:
    if type(greeting_frames) is not int or not MIN_GREETING_FRAMES <= greeting_frames <= MAX_GREETING_FRAMES:
        _fail("invalid_greeting_timing", "Greeting timing is outside the validated template bounds.")
    return max(MIN_CONTEXT_FRAMES, greeting_frames + 36)


def prepare_inputs(master_bytes: bytes, narration_bytes: bytes, *, first_name: str, ffmpeg: str | None = None) -> dict:
    """Prepare a 2.1-3.7 second same-take context; narration images are never used."""
    _master(master_bytes)
    _bounded(narration_bytes, "Narration")
    first_name = _exact_name(first_name)
    ffmpeg = ffmpeg or _ffmpeg_executable()
    with tempfile.TemporaryDirectory(prefix="crisp-same-take-") as directory:
        root = Path(directory)
        master, narration, video = root / "master.mp4", root / "narration.media", root / "context.mp4"
        master.write_bytes(master_bytes)
        narration.write_bytes(narration_bytes)
        greeting, timing = _trim_greeting(_audio(narration, ffmpeg, narration=True))
        greeting_frames = timing["greeting_frames"]
        context_frames = _context_frames(greeting_frames)
        source_audio = _audio(master, ffmpeg)
        tail = source_audio[SOURCE_GREETING_FRAMES * SAMPLES_PER_FRAME:]
        context_audio = np.concatenate((greeting, tail))[:context_frames * SAMPLES_PER_FRAME]
        if len(context_audio) != context_frames * SAMPLES_PER_FRAME:
            _fail("invalid_source_audio", "The master recording has insufficient body audio.")

        def context():
            source = _retimed_frames(master, greeting_frames, ffmpeg)
            try:
                for index, frame in enumerate(source):
                    if index >= context_frames:
                        break
                    yield frame
            finally:
                source.close()

        if _encode(context(), video, ffmpeg, upscale=True) != context_frames:
            _fail("invalid_context_timing", "Prepared source context changed frame count.")
        video_bytes, audio_bytes, greeting_bytes = video.read_bytes(), _wav(context_audio), _wav(greeting)
        metadata = {
            "preparation_version": PREPARATION_VERSION, "source_sha256": SOURCE_SHA256,
            "first_name": first_name, "narration_sha256": _hash(narration_bytes),
            "video_sha256": _hash(video_bytes), "audio_sha256": _hash(audio_bytes),
            "greeting_audio_sha256": _hash(greeting_bytes),
            "source_greeting_frames": SOURCE_GREETING_FRAMES, "source_frames": SOURCE_FRAMES,
            "fps": FPS, "sample_rate": SAMPLE_RATE, "context_frames": context_frames,
            "context_seconds": context_frames / FPS,
            "output_frames": SOURCE_FRAMES - SOURCE_GREETING_FRAMES + greeting_frames,
            **timing,
        }
        return {"video_bytes": video_bytes, "audio_bytes": audio_bytes,
                "greeting_audio_bytes": greeting_bytes, "metadata": metadata}


def _validate_metadata(metadata: dict, greeting_bytes: bytes, first_name: str) -> dict:
    if not isinstance(metadata, dict):
        _fail("invalid_composition_metadata", "Prepared composition metadata is required.")
    fixed = {
        "preparation_version": PREPARATION_VERSION, "source_sha256": SOURCE_SHA256,
        "first_name": _exact_name(first_name), "source_greeting_frames": SOURCE_GREETING_FRAMES,
        "source_frames": SOURCE_FRAMES, "fps": FPS, "sample_rate": SAMPLE_RATE,
        "greeting_audio_sha256": _hash(greeting_bytes),
    }
    if any(metadata.get(key) != value for key, value in fixed.items()):
        _fail("composition_identity_mismatch", "The name, recording or prepared greeting no longer matches.")
    greeting_frames = metadata.get("greeting_frames")
    context_frames = _context_frames(greeting_frames)
    calculated = {
        "context_frames": context_frames, "context_seconds": context_frames / FPS,
        "greeting_seconds": greeting_frames / FPS,
        "output_frames": SOURCE_FRAMES - SOURCE_GREETING_FRAMES + greeting_frames,
    }
    if any(metadata.get(key) != value for key, value in calculated.items()):
        _fail("invalid_composition_timing", "Prepared composition timing has changed.")
    return metadata


def _mouth_mask() -> np.ndarray:
    yy, xx = np.ogrid[:HEIGHT, :WIDTH]
    radius = np.sqrt(((xx - MOUTH_CENTER[0]) / MOUTH_AXES[0]) ** 2 + ((yy - MOUTH_CENTER[1]) / MOUTH_AXES[1]) ** 2)
    return np.clip((1.0 - radius) / 0.24, 0, 1).astype(np.float32)


def _background_error(source: np.ndarray, generated: np.ndarray) -> float:
    # Stationary bookcase/TV edges guard against a different shot, framing or
    # provider timeline. Speech-related face motion is intentionally excluded.
    left = np.abs(source[:, :180].astype(np.int16) - generated[:, :180].astype(np.int16))
    right = np.abs(source[:, 850:].astype(np.int16) - generated[:, 850:].astype(np.int16))
    return (float(left.sum()) + float(right.sum())) / (left.size + right.size)


def _blend(source: np.ndarray, generated: np.ndarray, mask: np.ndarray, fade: float) -> np.ndarray:
    alpha = (mask * fade)[..., None]
    frame = np.rint(source * (1 - alpha) + generated * alpha).astype(np.uint8)
    if not np.array_equal(frame[mask == 0], source[mask == 0]):
        _fail("nonmouth_pixels_changed", "A greeting edit changed pixels outside the mouth area.")
    return frame


def _captions(first_name: str, greeting_seconds: float, duration: float) -> bytes:
    cues = [
        (0, greeting_seconds, f"Hey {first_name},"),
        (greeting_seconds, min(duration, greeting_seconds + 2.25), "it's Yoni Kutler with Crisp Short Sales."),
        (greeting_seconds + 2.25, min(duration, greeting_seconds + 4.35), "Just wanted to put a face to my text."),
        (greeting_seconds + 4.35, duration, "If you have any questions, just text me back. Happy to help."),
    ]
    text = "WEBVTT\n\n" + "\n\n".join(
        f"{_timestamp(start)} --> {_timestamp(end)}\n{body}"
        for start, end, body in cues if start < end
    ) + "\n"
    return text.encode("utf-8")


def compose(master_bytes: bytes, kling_bytes: bytes, greeting_audio_bytes: bytes, metadata: dict, *, first_name: str, ffmpeg: str | None = None) -> dict:
    """Blend only same-take mouth pixels, retaining every original body frame."""
    _master(master_bytes)
    _bounded(kling_bytes, "Lip-sync output")
    metadata = _validate_metadata(metadata, greeting_audio_bytes, first_name)
    greeting_frames, context_frames = metadata["greeting_frames"], metadata["context_frames"]
    greeting = _read_greeting(greeting_audio_bytes, greeting_frames)
    ffmpeg = ffmpeg or _ffmpeg_executable()
    with tempfile.TemporaryDirectory(prefix="crisp-compose-") as directory:
        root = Path(directory)
        master, model = root / "master.mp4", root / "lipsync.mp4"
        silent, wav_path, output, poster = root / "silent.mp4", root / "audio.wav", root / "final.mp4", root / "poster.jpg"
        master.write_bytes(master_bytes)
        model.write_bytes(kling_bytes)
        probe = _probe(model, ffmpeg)
        if (probe["width"], probe["height"]) not in {(1280, 720), (1920, 1080)}:
            _fail("provider_geometry_changed", "Lip-sync output changed the approved 16:9 framing.")
        if abs(probe["duration"] - metadata["context_seconds"]) > MAX_PROVIDER_TIMELINE_DRIFT:
            _fail("provider_timeline_changed", "Lip-sync output changed the prepared clip duration.")
        mask, errors = _mouth_mask(), []

        def edited():
            source_frames, generated_frames = _retimed_frames(master, greeting_frames, ffmpeg), _frames(model, ffmpeg, provider=True)
            generated_count = 0
            try:
                for index, source in enumerate(source_frames):
                    if index < context_frames:
                        try:
                            generated = next(generated_frames)
                        except StopIteration:
                            _fail("provider_timeline_changed", "Lip-sync output ended before its prepared context.")
                        generated_count += 1
                        error = _background_error(source, generated)
                        errors.append(error)
                        if error >= BACKGROUND_MAE_LIMIT:
                            _fail("provider_geometry_changed", "Lip-sync output changed the shot or temporal alignment.")
                    if index < greeting_frames:
                        fade = min(1.0, (greeting_frames - index - 1) / 4.0)
                        yield _blend(source, generated, mask, fade)
                    else:
                        yield source
                # Do not accept clipped or substantially duplicated provider frames.
                for _ in generated_frames:
                    generated_count += 1
                if not context_frames <= generated_count <= context_frames + 2:
                    _fail("provider_timeline_changed", "Lip-sync output frame count changed.")
            finally:
                source_frames.close()
                generated_frames.close()

        count = _encode(edited(), silent, ffmpeg)
        if count != metadata["output_frames"]:
            _fail("invalid_output_timing", "The original recording body was not preserved in full.")
        source_audio = _audio(master, ffmpeg)
        final_audio = np.concatenate((greeting, source_audio[SOURCE_GREETING_FRAMES * SAMPLES_PER_FRAME:]))
        wav_path.write_bytes(_wav(final_audio))
        _run([
            ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
            "-protocol_whitelist", "file,pipe", "-i", str(silent), "-i", str(wav_path),
            "-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy", "-c:a", "aac", "-b:a", "160k",
            "-movflags", "+faststart", str(output),
        ])
        _run([
            ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
            "-protocol_whitelist", "file,pipe", "-ss", str(greeting_frames / FPS + 0.5),
            "-i", str(output), "-frames:v", "1", "-q:v", "2", str(poster),
        ])
        media, poster_bytes = output.read_bytes(), poster.read_bytes()
        _bounded(media, "Final video")
        duration = _probe(output, ffmpeg)["duration"]
        expected_duration = max(count / FPS, len(final_audio) / SAMPLE_RATE)
        if abs(duration - expected_duration) > 0.08:
            _fail("invalid_output_timing", "The final audio/video timeline changed unexpectedly.")
        return {
            "media_bytes": media, "poster_bytes": poster_bytes,
            "captions_bytes": _captions(first_name, greeting_frames / FPS, duration),
            "duration_seconds": duration,
            "qa": {
                "preparation_version": PREPARATION_VERSION, "source_sha256": SOURCE_SHA256,
                "first_name": first_name, "final_sha256": _hash(media), "model_output_sha256": _hash(kling_bytes),
                "output_frames": count, "original_body_frames": SOURCE_FRAMES - SOURCE_GREETING_FRAMES,
                "greeting_frames": greeting_frames, "source_body_start_frame": SOURCE_GREETING_FRAMES,
                "old_recording_frames": 0, "body_frames_exact_before_encoding": True,
                "body_audio_exact_before_encoding": True, "nonmouth_pixels_exact_before_encoding": True,
                "background_mae_max": max(errors), "quality_status": "requires_visual_and_name_pronunciation_review",
            },
        }
