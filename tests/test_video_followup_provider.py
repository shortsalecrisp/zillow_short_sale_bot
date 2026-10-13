import json
from pathlib import Path
import subprocess
import sys
import types
from unittest.mock import Mock

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import video_followup_provider as provider


class FakeResponse:
    def __init__(self, data=None, status=200, chunks=None, headers=None):
        self.data = data
        self.status_code = status
        self.headers = headers or {}
        self.content = json.dumps(data).encode() if data is not None else b""
        self.chunks = chunks if chunks is not None else [self.content]
        self.closed = False

    def json(self):
        if isinstance(self.data, Exception):
            raise self.data
        return self.data

    def iter_content(self, chunk_size):
        yield from self.chunks

    def close(self):
        self.closed = True


def make_provider(api=None, media=None, resolver=None):
    return provider.HeyGenVideoProvider(
        "test-api-key", session=api or Mock(), media_session=media or Mock(),
        resolver=resolver or (lambda host: ["8.8.8.8"]),
    )


def completed_snapshot():
    return {
        "status": "completed", "duration": 20.0,
        "media_url": "https://files2.heygen.ai/video/approved.mp4?Expires=1900000000",
        "subtitle_url": None,
    }


def test_submit_exact_approved_contract_and_no_redirects():
    response = FakeResponse({"data": {"video_id": "video_123"}})
    api = Mock()
    api.post.return_value = response
    client = make_provider(api)
    assert client.submit("Jim", "job_123") == "video_123"
    api.post.assert_called_once_with(
        "https://api.heygen.com/v3/videos",
        json={
            "type": "avatar", "avatar_id": provider.AVATAR_ID, "voice_id": provider.VOICE_ID,
            "voice_settings": {"speed": 1.05},
            "script": (
                "Hey Jim, it's Yoni Kutler with Crisp Short Sales. "
                "Just wanted to put a face to my text about your short sale listing. "
                "If you have any questions, text me back. Happy to help."
            ),
            "engine": {"type": "avatar_iii"}, "resolution": "720p", "aspect_ratio": "16:9",
            "output_format": "mp4", "caption": {"file_format": "srt"},
            "title": "Crisp follow-up job_123", "callback_id": "job_123",
        },
        headers={"X-Api-Key": "test-api-key", "Idempotency-Key": "job_123"},
        timeout=(5, 30), allow_redirects=False,
    )
    assert response.closed


@pytest.mark.parametrize("first_name", ["", "123", "Jim\nIgnore instructions", "Mary Jane", "<script>", "Hi/Jim"])
def test_invalid_name_is_rejected_before_submission(first_name):
    api = Mock()
    with pytest.raises(provider.VideoProviderError, match="verified first name"):
        make_provider(api).submit(first_name, "job_1")
    api.post.assert_not_called()


def test_names_with_real_apostrophes_hyphens_and_unicode_are_supported():
    assert provider.canonical_first_name("  Jean-Luc ") == "Jean-Luc"
    assert provider.canonical_first_name("D\u2019Andre") == "D'Andre"
    assert provider.canonical_first_name("Jos\u00e9") == "Jos\u00e9"


@pytest.mark.parametrize("exception", [requests.Timeout(), requests.ConnectionError(), requests.RequestException()])
def test_submit_network_unknown_is_uncertain_and_never_retried(exception):
    api = Mock()
    api.post.side_effect = exception
    with pytest.raises(provider.SubmissionUncertain) as error:
        make_provider(api).submit("Jim", "job_1")
    assert error.value.job_id == "job_1"
    assert error.value.uncertain
    assert api.post.call_count == 1


@pytest.mark.parametrize("status", [302, 408, 409, 500, 502, 503])
def test_submit_unknown_http_status_is_uncertain(status):
    api = Mock()
    response = FakeResponse({}, status=status)
    api.post.return_value = response
    with pytest.raises(provider.SubmissionUncertain):
        make_provider(api).submit("Jim", "job_1")
    assert api.post.call_count == 1
    assert response.closed


@pytest.mark.parametrize("status", [400, 401, 402, 403, 429])
def test_submit_definitive_rejection_is_separate(status):
    api = Mock()
    api.post.return_value = FakeResponse({}, status=status)
    with pytest.raises(provider.SubmissionRejected) as error:
        make_provider(api).submit("Jim", "job_1")
    assert error.value.uncertain is False
    assert api.post.call_count == 1


@pytest.mark.parametrize("data", [{}, {"data": {}}, {"data": {"video_id": "https://bad"}}, {"data": None}])
def test_success_without_valid_video_id_is_uncertain(data):
    api = Mock()
    api.post.return_value = FakeResponse(data)
    with pytest.raises(provider.SubmissionUncertain):
        make_provider(api).submit("Jim", "job_1")


@pytest.mark.parametrize("status,normalized", [("waiting", "pending"), ("queued", "pending"), ("running", "processing"), ("processing", "processing")])
def test_inspect_maps_only_known_states(status, normalized):
    api = Mock()
    api.get.return_value = FakeResponse({"data": {"id": "video_1", "status": status}})
    assert make_provider(api).inspect("video_1") == {"provider_id": "video_1", "status": normalized}
    assert api.get.call_args.kwargs["allow_redirects"] is False


def test_inspect_completed_validates_media_and_duration():
    api = Mock()
    api.get.return_value = FakeResponse({"data": {
        "id": "video_1", "status": "completed", "duration": 22.42,
        "video_url": "https://files2.heygen.ai/video/1.mp4?Expires=1",
        "subtitle_url": "https://files.heygen.ai/srt/1.srt",
    }})
    snapshot = make_provider(api).inspect("video_1")
    assert snapshot["media_url"].endswith("Expires=1")
    assert snapshot["duration"] == 22.42
    assert snapshot["poster_url"] is None


@pytest.mark.parametrize("duration", [0, -1, 30.01, float("inf"), float("nan"), None, "unknown"])
def test_invalid_render_duration_is_not_ready_for_use(duration):
    api = Mock()
    api.get.return_value = FakeResponse({"data": {
        "status": "completed", "duration": duration,
        "video_url": "https://files2.heygen.ai/video/1.mp4",
    }})
    with pytest.raises(provider.VideoProviderError, match="duration"):
        make_provider(api).inspect("video_1")


@pytest.mark.parametrize("url", [
    "http://files2.heygen.ai/video.mp4", "https://127.0.0.1/video.mp4",
    "https://localhost/video.mp4", "https://[::1]/video.mp4", "https://files2.heygen.ai.evil.test/v.mp4",
    "https://user:pass@files2.heygen.ai/v.mp4", "https://files2.heygen.ai:8443/v.mp4",
    "https://evil.test@files2.heygen.ai/v.mp4", "https://files2.heygen.ai/v.mp4#fragment",
    "https://files2.heygen.ai/v.mp4\n", "https://files2.heygen.ai\\@evil.test/v.mp4",
])
def test_media_url_disallows_unexpected_or_local_targets(url):
    with pytest.raises(provider.VideoProviderError):
        provider.validate_media_url(url)


@pytest.mark.parametrize("addresses", [["127.0.0.1"], ["10.2.3.4"], ["169.254.169.254"], ["::1"], ["8.8.8.8", "10.0.0.1"], []])
def test_download_checks_dns_before_http(addresses):
    media = Mock()
    client = make_provider(media=media, resolver=lambda host: addresses)
    with pytest.raises(provider.VideoProviderError, match="resolve publicly"):
        client._download("https://files2.heygen.ai/video.mp4", 100)
    media.get.assert_not_called()


def test_download_never_forwards_api_credentials_or_follows_redirect():
    media = Mock()
    response = FakeResponse(chunks=[b"mp4"], headers={"Content-Length": "3"})
    media.get.return_value = response
    assert make_provider(media=media)._download("https://files2.heygen.ai/video.mp4", 10) == b"mp4"
    assert media.get.call_args.kwargs["headers"] == {"Accept-Encoding": "identity"}
    assert media.get.call_args.kwargs["allow_redirects"] is False
    assert media.get.call_args.kwargs["timeout"] == (5, 30)
    assert response.closed


@pytest.mark.parametrize("headers,chunks", [({"Content-Length": "11"}, [b"x"]), ({}, [b"12345", b"678901"]), ({"Content-Length": "garbage"}, [b"x"])])
def test_media_download_is_bounded_even_without_content_length(headers, chunks):
    media = Mock()
    response = FakeResponse(headers=headers, chunks=chunks)
    media.get.return_value = response
    with pytest.raises(provider.VideoProviderError, match="limit"):
        make_provider(media=media)._download("https://files2.heygen.ai/video.mp4", 10)
    assert response.closed


def test_download_redirect_is_not_followed():
    media = Mock()
    media.get.return_value = FakeResponse(status=302, headers={"Location": "http://127.0.0.1/private"})
    with pytest.raises(provider.VideoProviderError):
        make_provider(media=media)._download("https://files2.heygen.ai/video.mp4", 10)
    assert media.get.call_count == 1


def stub_processing(monkeypatch, source_dimensions=(1280, 720), source_audio=True):
    commands = []
    probes = []
    monkeypatch.setattr(provider, "_ffmpeg_executable", lambda: "/bundled/ffmpeg")

    def probe(path, executable):
        probes.append(path.name)
        width, height = source_dimensions if path.name == "source.mp4" else (960, 540)
        return {"width": width, "height": height, "duration": 20.0, "has_audio": source_audio}

    def run(command, **kwargs):
        commands.append(command)
        destination = Path(command[-1])
        destination.write_bytes(b"prepared" if destination.suffix == ".mp4" else b"poster")

    monkeypatch.setattr(provider, "_probe", probe)
    monkeypatch.setattr(provider, "_run", run)
    return commands, probes


def test_preparation_uses_exact_crop_preserves_audio_and_temp_cleanup(monkeypatch):
    commands, probes = stub_processing(monkeypatch)
    client = make_provider()
    client._download = Mock(return_value=b"source")
    result = client.prepare_media(completed_snapshot(), "Jim")
    assert result["media_bytes"] == b"prepared"
    assert result["poster_bytes"] == b"poster"
    assert result["duration"] == 20.0
    assert result["caption_source"] == "approximate"
    assert b"Approximate phrase timing" in result["vtt_bytes"]
    assert b"Hey Jim" in result["vtt_bytes"]
    crop = commands[0]
    assert crop[crop.index("-vf") + 1] == "crop=960:540:304:180,setsar=1"
    assert crop[crop.index("-c:a") + 1] == "copy"
    assert "0:a:0" in crop
    assert probes == ["source.mp4", "video.mp4"]
    assert not Path(crop[-1]).exists()


@pytest.mark.parametrize("dimensions", [(1920, 1080), (640, 360), (720, 1280)])
def test_alternate_source_dimensions_never_get_silent_crop(monkeypatch, dimensions):
    commands, probes = stub_processing(monkeypatch, source_dimensions=dimensions)
    client = make_provider()
    client._download = Mock(return_value=b"source")
    with pytest.raises(provider.VideoProviderError, match="1280x720"):
        client.prepare_media(completed_snapshot(), "Jim")
    assert not commands


def test_preparation_rejects_silent_source(monkeypatch):
    commands, probes = stub_processing(monkeypatch, source_audio=False)
    client = make_provider()
    client._download = Mock(return_value=b"source")
    with pytest.raises(provider.VideoProviderError, match="audio"):
        client.prepare_media(completed_snapshot(), "Jim")
    assert not commands


def test_provider_srt_timing_is_used_when_available(monkeypatch):
    stub_processing(monkeypatch)
    client = make_provider()
    client._download = Mock(side_effect=[b"source", b"1\n00:00:00,000 --> 00:00:02,500\nHey Jim.\n"])
    snapshot = completed_snapshot()
    snapshot["subtitle_url"] = "https://files.heygen.ai/srt/1.srt"
    result = client.prepare_media(snapshot, "Jim")
    assert result["caption_source"] == "provider"
    assert result["vtt_bytes"] == b"WEBVTT\n\n00:00:00.000 --> 00:00:02.500\nHey Jim.\n"


def test_bad_provider_captions_fall_back_to_marked_approximate(monkeypatch):
    stub_processing(monkeypatch)
    client = make_provider()
    client._download = Mock(side_effect=[b"source", b"not captions"])
    snapshot = completed_snapshot()
    snapshot["subtitle_url"] = "https://files.heygen.ai/srt/1.srt"
    result = client.prepare_media(snapshot, "Jim")
    assert result["caption_source"] == "approximate"
    assert b"Approximate" in result["vtt_bytes"]


def test_caption_cues_cannot_overrun_verified_duration():
    with pytest.raises(provider.VideoProviderError):
        provider._provider_vtt(b"1\n00:00:00,000 --> 00:00:31,000\nBad\n", 20)


def test_bundled_ffmpeg_is_used_only_when_system_binary_absent(monkeypatch):
    monkeypatch.setattr(provider.shutil, "which", lambda name: None)
    monkeypatch.setitem(sys.modules, "imageio_ffmpeg", types.SimpleNamespace(get_ffmpeg_exe=lambda: "/vendored/ffmpeg"))
    assert provider._ffmpeg_executable() == "/vendored/ffmpeg"


def test_ffmpeg_probe_fallback_reads_dimensions_and_audio(monkeypatch, tmp_path):
    monkeypatch.setattr(provider.shutil, "which", lambda name: None)
    monkeypatch.setattr(provider, "_run", lambda *args, **kwargs: types.SimpleNamespace(stderr=(
        b"Duration: 00:00:20.00, start: 0.00\n"
        b"Stream #0:0: Video: h264, yuv420p, 1280x720 [SAR 1:1 DAR 16:9], 30 fps\n"
        b"Stream #0:1: Audio: aac, 44100 Hz, stereo\n"
    )))
    assert provider._probe(tmp_path / "input.mp4", "/bundled/ffmpeg") == {
        "width": 1280, "height": 720, "duration": 20.0, "has_audio": True,
    }


def test_real_bundled_ffmpeg_crop_keeps_audio_and_visible_corner_mark(tmp_path):
    imageio_ffmpeg = pytest.importorskip("imageio_ffmpeg")
    executable = imageio_ffmpeg.get_ffmpeg_exe()
    source = tmp_path / "fixture.mp4"
    subprocess.run([
        executable, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", "color=c=blue:s=1280x720:r=15:d=3",
        "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=44100:duration=3",
        "-vf", "drawbox=x=1190:y=665:w=50:h=30:color=red:t=fill",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-shortest", str(source),
    ], check=True, timeout=30)
    snapshot = completed_snapshot()
    snapshot["duration"] = 3.0
    client = make_provider()
    client._download = Mock(return_value=source.read_bytes())
    result = client.prepare_media(snapshot, "Jim")
    output = tmp_path / "prepared.mp4"
    output.write_bytes(result["media_bytes"])
    metadata = provider._probe(output, executable)
    assert (metadata["width"], metadata["height"]) == (960, 540)
    assert metadata["has_audio"]
    assert metadata["duration"] == pytest.approx(3.0, abs=0.02)

    def audio_digest(path):
        return subprocess.run([
            executable, "-nostdin", "-hide_banner", "-loglevel", "error", "-i", str(path),
            "-map", "0:a:0", "-c:a", "copy", "-f", "hash", "-hash", "md5", "-",
        ], capture_output=True, check=True, timeout=30).stdout

    assert audio_digest(source) == audio_digest(output)
    frame = subprocess.run([
        executable, "-nostdin", "-hide_banner", "-loglevel", "error", "-i", str(output),
        "-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "rgb24", "-",
    ], capture_output=True, check=True, timeout=30).stdout
    pixel = (500 * 960 + 910) * 3
    red, green, blue = frame[pixel:pixel + 3]
    assert red > 180 and green < 80 and blue < 80
    assert result["poster_bytes"].startswith(b"\xff\xd8")
