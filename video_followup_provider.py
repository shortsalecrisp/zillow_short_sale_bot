"""Bounded HeyGen rendering and media preparation, without scheduler side effects.

The caller must persist a submission attempt before calling ``submit``. An
uncertain submission is never retried here: reconcile it with HeyGen first.
"""

from __future__ import annotations

import html
import ipaddress
import json
import math
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time
import unicodedata
from urllib.parse import urlsplit

import requests


API_BASE = "https://api.heygen.com/v3"
AVATAR_ID = "baaaebc70e134e96b85209f2a446a03c"
VOICE_ID = "1ca117e4778643189228acc9400a37f3"
SCRIPT_TEMPLATE = (
    "Hey {first_name}, it's Yoni Kutler with Crisp Short Sales. "
    "Just wanted to put a face to my text about your short sale listing. "
    "If you have any questions, text me back. Happy to help."
)
MEDIA_HOSTS = frozenset(
    {"files.heygen.ai", "files2.heygen.ai", "resource.heygen.ai", "resource2.heygen.ai"}
)
MAX_SOURCE_BYTES = 50 * 1024 * 1024
MAX_MEDIA_BYTES = 25 * 1024 * 1024
MAX_POSTER_BYTES = 2 * 1024 * 1024
MAX_CAPTION_BYTES = 256 * 1024
MAX_API_BYTES = 256 * 1024
MAX_DURATION_SECONDS = 30.0
REQUEST_TIMEOUT = (5, 30)
DOWNLOAD_DEADLINE_SECONDS = 90
PROCESS_TIMEOUT_SECONDS = 90
_NAME = re.compile(r"[^\W\d_]+(?:['-][^\W\d_]+)*", re.UNICODE)
_IDENTIFIER = re.compile(r"[A-Za-z0-9_:.-]{1,200}")
_TIMING = re.compile(
    r"^(\d{2}:\d{2}:\d{2}[,.]\d{3})\s+-->\s+"
    r"(\d{2}:\d{2}:\d{2}[,.]\d{3})(?:\s+.*)?$"
)


class VideoProviderError(RuntimeError):
    """A bounded, sanitized error suitable for durable job state."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


class SubmissionUncertain(VideoProviderError):
    """HeyGen may have accepted the request; never blindly create another job."""

    uncertain = True

    def __init__(self, job_id: str, code: str = "submission_uncertain"):
        self.job_id = job_id
        super().__init__(code, "HeyGen submission result is uncertain; reconciliation is required.")


class SubmissionRejected(VideoProviderError):
    """HeyGen definitively rejected this request before returning a video ID."""

    uncertain = False


def canonical_first_name(value: str) -> str:
    value = unicodedata.normalize("NFKC", str(value or "")).strip().replace("\u2019", "'")
    if not value or len(value) > 40 or not _NAME.fullmatch(value):
        raise VideoProviderError("invalid_first_name", "A verified first name is required.")
    return value


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise VideoProviderError("invalid_identifier", "Invalid provider or job identifier.")
    return value


def _public_addresses(host: str) -> list[str]:
    return [item[4][0] for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)]


def validate_media_url(value: str, *, resolve=None) -> str:
    """Allow only provider HTTPS media, including signed query strings."""
    try:
        parsed = urlsplit(value)
        valid = (
            isinstance(value, str)
            and len(value) <= 8192
            and parsed.scheme == "https"
            and parsed.hostname in MEDIA_HOSTS
            and parsed.port in (None, 443)
            and parsed.username is None
            and parsed.password is None
            and not parsed.fragment
            and parsed.path.startswith("/")
            and not any(ord(char) < 33 or char == "\\" for char in value)
        )
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise VideoProviderError("unsafe_media_url", "Provider media URL is not permitted.")
    if resolve is not None:
        try:
            addresses = resolve(parsed.hostname)
            if not addresses or any(not ipaddress.ip_address(addr).is_global for addr in addresses):
                raise ValueError("nonpublic address")
        except (OSError, ValueError, TypeError):
            raise VideoProviderError("unsafe_media_address", "Provider media must resolve publicly.") from None
    return value


def _duration(value) -> float:
    try:
        value = float(value)
    except (ValueError, TypeError):
        value = float("nan")
    if not math.isfinite(value) or not 0 < value <= MAX_DURATION_SECONDS:
        raise VideoProviderError("invalid_duration", "Video duration must be positive and at most 30 seconds.")
    return value


def _ffmpeg_executable() -> str:
    executable = shutil.which("ffmpeg")
    if executable:
        return executable
    try:
        import imageio_ffmpeg

        return imageio_ffmpeg.get_ffmpeg_exe()
    except (ImportError, RuntimeError, OSError):
        raise VideoProviderError("ffmpeg_unavailable", "No system or bundled FFmpeg is available.") from None


def _run(command: list[str], *, allow_failure: bool = False):
    try:
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=PROCESS_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise VideoProviderError("media_processing_failed", "Local media processing did not finish.") from None
    if result.returncode and not allow_failure:
        raise VideoProviderError("media_processing_failed", "Local media processing rejected the video.")
    return result


def _probe(path: Path, ffmpeg: str) -> dict:
    ffprobe = shutil.which("ffprobe")
    if ffprobe:
        result = _run(
            [ffprobe, "-v", "error", "-protocol_whitelist", "file,pipe", "-show_streams",
             "-show_format", "-of", "json", str(path)]
        )
        try:
            data = json.loads(result.stdout)
            streams = data["streams"]
            video = next(stream for stream in streams if stream.get("codec_type") == "video")
            return {
                "width": int(video["width"]), "height": int(video["height"]),
                "duration": _duration(data["format"]["duration"]),
                "has_audio": any(stream.get("codec_type") == "audio" for stream in streams),
            }
        except (KeyError, ValueError, TypeError, StopIteration):
            raise VideoProviderError("invalid_media", "Video metadata is invalid.") from None
    result = _run(
        [ffmpeg, "-nostdin", "-hide_banner", "-protocol_whitelist", "file,pipe", "-i", str(path),
         "-t", "0", "-f", "null", "-"],
        allow_failure=True,
    )
    stderr = result.stderr.decode("utf-8", errors="replace")
    timing = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", stderr)
    video_line = next((line for line in stderr.splitlines() if "Video:" in line), "")
    dimensions = re.search(r"(?:^|[ ,])(\d{2,5})x(\d{2,5})(?:[ ,\[]|$)", video_line)
    if not timing or not dimensions:
        raise VideoProviderError("invalid_media", "Video metadata is invalid.")
    return {
        "width": int(dimensions[1]), "height": int(dimensions[2]),
        "duration": _duration(int(timing[1]) * 3600 + int(timing[2]) * 60 + float(timing[3])),
        "has_audio": any("Audio:" in line for line in stderr.splitlines()),
    }


def _timestamp(seconds: float) -> str:
    milliseconds = round(seconds * 1000)
    hours, milliseconds = divmod(milliseconds, 3600000)
    minutes, milliseconds = divmod(milliseconds, 60000)
    seconds, milliseconds = divmod(milliseconds, 1000)
    return f"{hours:02}:{minutes:02}:{seconds:02}.{milliseconds:03}"


def _seconds(timestamp: str) -> float:
    hours, minutes, seconds = timestamp.replace(",", ".").split(":")
    if int(minutes) >= 60 or float(seconds) >= 60:
        raise ValueError("invalid timestamp")
    return int(hours) * 3600 + int(minutes) * 60 + float(seconds)


def _provider_vtt(content: bytes, duration: float) -> bytes:
    try:
        text = content.decode("utf-8-sig").replace("\r\n", "\n").replace("\r", "\n")
        cues = []
        previous_start = -1.0
        for block in re.split(r"\n\s*\n", text.strip()):
            lines = block.splitlines()
            position = next((i for i, line in enumerate(lines) if _TIMING.fullmatch(line)), None)
            if position is None:
                continue
            timing = _TIMING.fullmatch(lines[position])
            start, end = _seconds(timing[1]), _seconds(timing[2])
            if not 0 <= start < end <= duration + 0.25 or start < previous_start:
                raise ValueError("invalid cue range")
            body = " ".join(lines[position + 1:]).strip()
            if not body or len(body) > 2000:
                raise ValueError("invalid cue text")
            body = html.escape(re.sub(r"<[^>]*>", "", body))
            cues.append(f"{_timestamp(start)} --> {_timestamp(min(end, duration))}\n{body}")
            previous_start = start
        if not cues:
            raise ValueError("missing cues")
        return ("WEBVTT\n\n" + "\n\n".join(cues) + "\n").encode("utf-8")
    except (UnicodeError, ValueError, TypeError):
        raise VideoProviderError("invalid_captions", "Provider captions could not be used.") from None


def approximate_vtt(first_name: str, duration: float) -> bytes:
    """Phrase timing is proportional, not forced alignment or provider timing."""
    first_name, duration = canonical_first_name(first_name), _duration(duration)
    phrases = [
        f"Hey {first_name}, it's Yoni Kutler with Crisp Short Sales.",
        "Just wanted to put a face to my text about your short sale listing.",
        "If you have any questions, text me back. Happy to help.",
    ]
    total_words = sum(len(phrase.split()) for phrase in phrases)
    start, seen = 0.0, 0
    cues = []
    for phrase in phrases:
        seen += len(phrase.split())
        end = duration * seen / total_words
        cues.append(f"{_timestamp(start)} --> {_timestamp(end)}\n{html.escape(phrase)}")
        start = end
    return (
        "WEBVTT\n\nNOTE Approximate phrase timing; not forced-aligned.\n\n"
        + "\n\n".join(cues) + "\n"
    ).encode("utf-8")


class HeyGenVideoProvider:
    def __init__(self, api_key: str, *, session=None, media_session=None, resolver=None):
        if not isinstance(api_key, str) or not api_key.strip():
            raise VideoProviderError("provider_unconfigured", "HeyGen API credentials are unavailable.")
        self._api_key = api_key
        self._session = session or requests.Session()
        self._media_session = media_session or requests.Session()
        if session is None:
            self._session.trust_env = False
        if media_session is None:
            self._media_session.trust_env = False
        self._resolve = resolver or _public_addresses

    @staticmethod
    def script(first_name: str) -> str:
        return SCRIPT_TEMPLATE.format(first_name=canonical_first_name(first_name))

    def submit(self, first_name: str, job_id: str) -> str:
        first_name, job_id = canonical_first_name(first_name), _identifier(job_id)
        payload = {
            "type": "avatar", "avatar_id": AVATAR_ID, "voice_id": VOICE_ID,
            "voice_settings": {"speed": 1.05}, "script": self.script(first_name),
            "engine": {"type": "avatar_iii"}, "resolution": "720p", "aspect_ratio": "16:9",
            "output_format": "mp4", "caption": {"file_format": "srt"},
            "title": f"Crisp follow-up {job_id}", "callback_id": job_id,
        }
        try:
            response = self._session.post(
                f"{API_BASE}/videos", json=payload,
                headers={"X-Api-Key": self._api_key, "Idempotency-Key": job_id},
                timeout=REQUEST_TIMEOUT, allow_redirects=False,
            )
        except requests.RequestException:
            raise SubmissionUncertain(job_id) from None
        try:
            if 400 <= response.status_code < 500 and response.status_code not in (408, 409):
                raise SubmissionRejected("submission_rejected", f"HeyGen rejected submission (HTTP {response.status_code}).")
            if not 200 <= response.status_code < 300:
                raise SubmissionUncertain(job_id)
            if len(response.content) > MAX_API_BYTES:
                raise SubmissionUncertain(job_id)
            try:
                provider_id = response.json()["data"]["video_id"]
                return _identifier(provider_id)
            except (KeyError, ValueError, TypeError, VideoProviderError):
                raise SubmissionUncertain(job_id) from None
        finally:
            response.close()

    def inspect(self, video_id: str) -> dict:
        video_id = _identifier(video_id)
        try:
            response = self._session.get(
                f"{API_BASE}/videos/{video_id}", headers={"X-Api-Key": self._api_key},
                timeout=REQUEST_TIMEOUT, allow_redirects=False,
            )
        except requests.RequestException:
            raise VideoProviderError("inspection_unavailable", "HeyGen status could not be read.") from None
        try:
            if response.status_code != 200 or len(response.content) > MAX_API_BYTES:
                raise VideoProviderError("inspection_unavailable", "HeyGen status could not be read.")
            try:
                data = response.json()["data"]
                status = data["status"]
                if not isinstance(status, str) or data.get("id", video_id) != video_id:
                    raise ValueError("wrong video")
            except (KeyError, ValueError, TypeError):
                raise VideoProviderError("invalid_provider_response", "HeyGen status response was invalid.") from None
            status = {"waiting": "pending", "queued": "pending", "running": "processing"}.get(status, status)
            if status not in {"pending", "processing", "completed", "failed"}:
                raise VideoProviderError("unknown_provider_status", "HeyGen returned an unknown lifecycle state.")
            result = {"provider_id": video_id, "status": status}
            if status == "completed":
                result["duration"] = _duration(data.get("duration"))
                result["media_url"] = validate_media_url(data.get("video_url"))
                for source, target in (("subtitle_url", "subtitle_url"), ("thumbnail_url", "poster_url")):
                    result[target] = validate_media_url(data[source]) if data.get(source) else None
            elif status == "failed":
                result["failure_code"] = str(data.get("failure_code") or "render_failed")[:80]
            return result
        finally:
            response.close()

    def _download(self, url: str, maximum_bytes: int) -> bytes:
        validate_media_url(url, resolve=self._resolve)
        started = time.monotonic()
        try:
            response = self._media_session.get(
                url, timeout=REQUEST_TIMEOUT, allow_redirects=False, stream=True,
                headers={"Accept-Encoding": "identity"},
            )
        except requests.RequestException:
            raise VideoProviderError("media_download_failed", "Provider media could not be downloaded.") from None
        try:
            if response.status_code != 200:
                raise VideoProviderError("media_download_failed", "Provider media could not be downloaded.")
            advertised_size = response.headers.get("Content-Length")
            if advertised_size:
                try:
                    if int(advertised_size) < 0 or int(advertised_size) > maximum_bytes:
                        raise ValueError("oversize")
                except ValueError:
                    raise VideoProviderError("media_too_large", "Provider media exceeds the download limit.") from None
            content = bytearray()
            for chunk in response.iter_content(chunk_size=65536):
                if time.monotonic() - started > DOWNLOAD_DEADLINE_SECONDS:
                    raise VideoProviderError("media_download_timeout", "Provider media exceeded the download deadline.")
                if chunk:
                    if len(content) + len(chunk) > maximum_bytes:
                        raise VideoProviderError("media_too_large", "Provider media exceeds the download limit.")
                    content.extend(chunk)
            if not content:
                raise VideoProviderError("empty_media", "Provider media was empty.")
            return bytes(content)
        except requests.RequestException:
            raise VideoProviderError("media_download_failed", "Provider media could not be downloaded.") from None
        finally:
            response.close()

    def prepare_media(self, snapshot: dict, first_name: str) -> dict:
        first_name = canonical_first_name(first_name)
        if snapshot.get("status") != "completed":
            raise VideoProviderError("media_not_ready", "Only a completed provider video can be prepared.")
        declared_duration = _duration(snapshot.get("duration"))
        source_bytes = self._download(snapshot.get("media_url"), MAX_SOURCE_BYTES)
        ffmpeg = _ffmpeg_executable()
        with tempfile.TemporaryDirectory(prefix="crisp-video-") as directory:
            source = Path(directory) / "source.mp4"
            output = Path(directory) / "video.mp4"
            poster = Path(directory) / "poster.jpg"
            source.write_bytes(source_bytes)
            metadata = _probe(source, ffmpeg)
            if (metadata["width"], metadata["height"]) != (1280, 720):
                raise VideoProviderError("unexpected_source_dimensions", "The approved crop requires a 1280x720 source.")
            if not metadata["has_audio"] or abs(metadata["duration"] - declared_duration) > 0.5:
                raise VideoProviderError("unexpected_source_media", "Source audio or duration did not match the render.")
            _run([
                ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                "-protocol_whitelist", "file,pipe", "-i", str(source),
                "-map", "0:v:0", "-map", "0:a:0", "-vf", "crop=960:540:304:180,setsar=1",
                "-c:v", "libx264", "-preset", "slow", "-crf", "18", "-pix_fmt", "yuv420p",
                "-c:a", "copy", "-movflags", "+faststart", str(output),
            ])
            if not output.is_file() or not 0 < output.stat().st_size <= MAX_MEDIA_BYTES:
                raise VideoProviderError("invalid_prepared_media", "Prepared video is missing or oversized.")
            prepared = _probe(output, ffmpeg)
            if (prepared["width"], prepared["height"]) != (960, 540) or not prepared["has_audio"]:
                raise VideoProviderError("invalid_prepared_media", "Prepared video failed dimension or audio verification.")
            if abs(prepared["duration"] - metadata["duration"]) > 0.1:
                raise VideoProviderError("invalid_prepared_media", "Prepared video did not preserve its duration.")
            _run([
                ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-ss", "0.1",
                "-protocol_whitelist", "file,pipe", "-i", str(output), "-frames:v", "1", "-q:v", "2", str(poster),
            ])
            if not poster.is_file() or not 0 < poster.stat().st_size <= MAX_POSTER_BYTES:
                raise VideoProviderError("invalid_poster", "Prepared poster is missing or oversized.")
            duration = prepared["duration"]
            caption_source = "approximate"
            vtt = approximate_vtt(first_name, duration)
            if snapshot.get("subtitle_url"):
                try:
                    vtt = _provider_vtt(self._download(snapshot["subtitle_url"], MAX_CAPTION_BYTES), duration)
                    caption_source = "provider"
                except VideoProviderError:
                    pass
            return {
                "media_bytes": output.read_bytes(), "poster_bytes": poster.read_bytes(),
                "vtt_bytes": vtt, "duration": duration, "caption_source": caption_source,
                "width": 960, "height": 540,
            }
