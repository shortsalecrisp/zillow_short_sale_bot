import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import sys
import types
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import video_followup as core
import video_followup_worker as bootstrap


class Response:
    def __init__(self, status=200, payload=None, content=b""):
        self.status_code = status
        self.payload = payload or {}
        self.content = content

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("storage request rejected")


@pytest.fixture(autouse=True)
def isolated_config(monkeypatch):
    for name in list(core.os.environ):
        if name.startswith("VIDEO_FOLLOWUP_") or name in {
            "HEYGEN_API_KEY", "FAL_API_KEY", "TASKER_TRANSPORT_HEALTH_URL", "SMS_CHATBOT_ALLOWED_TOKEN", "GCP_SERVICE_ACCOUNT_JSON"
        }:
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(core, "_SERVICE", None)
    monkeypatch.setattr(bootstrap, "_thread", None)


def test_read_media_is_bound_to_metadata_generation():
    session = Mock()
    session.get.side_effect = [
        Response(payload={"size": "4", "generation": "27"}),
        Response(content=b"data"),
    ]
    objects = core.GCSObjects("crisp-video-private", session=session)
    assert objects.read("jobs/abc") == (b"data", 27)
    url = "https://storage.googleapis.com/storage/v1/b/crisp-video-private/o/video-followup%2Fjobs%2Fabc"
    assert session.get.call_args_list[0].args == (url,)
    assert session.get.call_args_list[0].kwargs == {"timeout": 20}
    assert session.get.call_args_list[1].kwargs == {
        "params": {"alt": "media", "generation": 27}, "timeout": 30,
    }


def test_missing_object_404_does_not_fetch_media():
    session = Mock()
    session.get.return_value = Response(status=404)
    assert core.GCSObjects("crisp-video-private", session=session).read("jobs/missing") == (None, 0)
    assert session.get.call_count == 1


def test_signed_asset_url_is_short_lived_read_only_and_offline(monkeypatch):
    session, credentials = Mock(), Mock()
    credentials.service_account_email = "video@example.iam.gserviceaccount.com"
    credentials.sign_bytes.return_value = b"signed"
    objects = core.GCSObjects("crisp-video-private", session=session)
    objects._signing_credentials = credentials
    monkeypatch.setattr(core, "utcnow", lambda: datetime(2026, 10, 14, 12, 0, tzinfo=timezone.utc))
    result = urlsplit(objects.signed_get_url("assets/approved master/video.mp4"))
    query = parse_qs(result.query)
    assert result.scheme == "https" and result.netloc == "storage.googleapis.com"
    assert result.path == "/crisp-video-private/video-followup/assets/approved%20master/video.mp4"
    assert query["X-Goog-Date"] == ["20261014T120000Z"]
    assert query["X-Goog-Expires"] == ["1800"]
    assert query["X-Goog-Credential"] == [credentials.service_account_email + "/20261014/auto/storage/goog4_request"]
    assert query["X-Goog-Signature"] == [b"signed".hex()]
    raw_query = result.query.split("&X-Goog-Signature=")[0]
    canonical = "\n".join(("GET", result.path, raw_query, "host:storage.googleapis.com\n", "host", "UNSIGNED-PAYLOAD"))
    assert credentials.sign_bytes.call_args.args == (("GOOG4-RSA-SHA256\n20261014T120000Z\n20261014/auto/storage/goog4_request\n"
                                                     + hashlib.sha256(canonical.encode()).hexdigest()).encode(),)
    session.assert_not_called()
    session.get.assert_not_called()
    session.post.assert_not_called()


@pytest.mark.parametrize("key", ["jobs/private", "assets/../jobs/private", "assets/a\\b", "assets/a\nb", None])
def test_signed_url_refuses_non_media_or_unsafe_paths(key):
    objects = core.GCSObjects("crisp-video-private", session=Mock())
    with pytest.raises(ValueError, match="asset reads"):
        objects.signed_get_url(key)


@pytest.mark.parametrize("ttl", [0, -1, 3601, True, "1800", 1.5])
def test_signed_url_refuses_long_or_invalid_lifetimes(ttl):
    objects = core.GCSObjects("crisp-video-private", session=Mock())
    with pytest.raises(ValueError, match="lifetime"):
        objects.signed_get_url("assets/master/video", ttl)


def test_signed_url_needs_existing_credentials():
    objects = core.GCSObjects("crisp-video-private", session=Mock())
    with pytest.raises(ValueError, match="credentials"):
        objects.signed_get_url("assets/master/video")


@pytest.mark.parametrize("key,size", [("jobs/large", 512 * 1024 + 1), ("assets/id/video", core.MAX_MEDIA_BYTES + 1)])
def test_oversized_storage_metadata_blocks_download(key, size):
    session = Mock()
    session.get.return_value = Response(payload={"size": str(size), "generation": "8"})
    with pytest.raises(ValueError, match="size limit"):
        core.GCSObjects("crisp-video-private", session=session).read(key)
    assert session.get.call_count == 1


@pytest.mark.parametrize("version", [0, 7])
def test_write_uses_exact_generation_precondition(version):
    session = Mock()
    session.post.return_value = Response(payload={"generation": "8"})
    objects = core.GCSObjects("crisp-video-private", session=session)
    assert objects.write("jobs/abc", b"{}", version) == 8
    session.post.assert_called_once_with(
        "https://storage.googleapis.com/upload/storage/v1/b/crisp-video-private/o",
        params={"uploadType": "media", "name": "video-followup/jobs/abc", "ifGenerationMatch": version},
        data=b"{}", headers={"Content-Type": "application/json"}, timeout=30,
    )


def test_write_412_is_conflict_and_is_not_repeated():
    session = Mock()
    session.post.return_value = Response(status=412)
    with pytest.raises(core.Conflict):
        core.GCSObjects("crisp-video-private", session=session).write("jobs/abc", b"{}", 4)
    assert session.post.call_count == 1


@pytest.mark.parametrize("status", [204, 404, 412])
def test_delete_is_generation_bound_and_leaves_newer_generation_alone(status):
    session = Mock()
    session.delete.return_value = Response(status=status)
    objects = core.GCSObjects("crisp-video-private", session=session)
    objects.delete("active/abc", 11)
    session.delete.assert_called_once_with(
        objects.url("active/abc"), params={"ifGenerationMatch": 11}, timeout=20,
    )


def test_storage_pagination_reads_every_page():
    session = Mock()
    session.get.side_effect = [
        Response(payload={"items": [{"name": "video-followup/active/one"}], "nextPageToken": "page2"}),
        Response(payload={"items": [{"name": "video-followup/active/two"}], "nextPageToken": "page3"}),
        Response(payload={"items": [{"name": "video-followup/active/three"}]}),
    ]
    objects = core.GCSObjects("crisp-video-private", session=session)
    assert objects.list("active/", limit=1000) == ["active/one", "active/two", "active/three"]
    assert [call.kwargs["params"] for call in session.get.call_args_list] == [
        {"prefix": "video-followup/active/", "maxResults": 1000},
        {"prefix": "video-followup/active/", "maxResults": 1000, "pageToken": "page2"},
        {"prefix": "video-followup/active/", "maxResults": 1000, "pageToken": "page3"},
    ]
    assert all(call.kwargs["timeout"] == 20 for call in session.get.call_args_list)


def test_feature_off_creates_no_client_storage_or_thread(monkeypatch):
    storage = Mock(side_effect=AssertionError("storage must not initialize"))
    service = Mock(side_effect=AssertionError("service must not initialize"))
    thread = Mock(side_effect=AssertionError("thread must not initialize"))
    monkeypatch.setattr(core, "GCSObjects", storage)
    monkeypatch.setattr(bootstrap, "get_service", service)
    monkeypatch.setattr(bootstrap.threading, "Thread", thread)
    core.prepare_for_followup()
    assert core.select_for_followup("usual text") == ("usual text", {})
    assert bootstrap.create_worker() is None
    bootstrap.start_worker()
    storage.assert_not_called()
    service.assert_not_called()
    thread.assert_not_called()


def test_generation_off_initializes_no_worker_clients(monkeypatch):
    monkeypatch.setenv("VIDEO_FOLLOWUP_ENABLED", "true")
    service = Mock(side_effect=AssertionError("storage must not initialize"))
    monkeypatch.setattr(bootstrap, "get_service", service)
    assert bootstrap.create_worker() is None
    service.assert_not_called()


def configured_environment(monkeypatch):
    env = {
        "VIDEO_FOLLOWUP_ENABLED": "true", "VIDEO_FOLLOWUP_GENERATION_ENABLED": "true",
        "VIDEO_FOLLOWUP_STORAGE_BUCKET": "crisp-video-private",
        "VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "https://go.crispshortsales.com",
        "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T15:00:00Z",
        "VIDEO_FOLLOWUP_TEST_PHONES": "9542053205",
        "SMS_CHATBOT_ALLOWED_TOKEN": "test-token-secret",
        "FAL_API_KEY": "test-fal-secret", "FAL_TTS_API_KEY": "test-tts-secret",
        "VIDEO_FOLLOWUP_SAME_TAKE_MASTER_KEY": "assets/approved-master/video",
        "TASKER_TRANSPORT_HEALTH_URL": "https://script.google.com/macros/s/test/exec",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return env


def fake_constructors(monkeypatch):
    service = Mock(return_value=types.SimpleNamespace(store=object()))
    provider_constructor = Mock(return_value=object())
    eligibility_constructor = Mock(return_value=object())
    worker_constructor = Mock(return_value=object())
    monkeypatch.setattr(bootstrap, "get_service", service)
    monkeypatch.setitem(sys.modules, "video_followup_same_take", types.SimpleNamespace(
        SameTakeVideoProvider=provider_constructor, _generator_profile=Mock()))
    monkeypatch.setattr(bootstrap, "Eligibility", eligibility_constructor)
    monkeypatch.setattr(bootstrap, "Worker", worker_constructor)
    return service, provider_constructor, eligibility_constructor, worker_constructor


@pytest.mark.parametrize("missing", [
    "VIDEO_FOLLOWUP_STORAGE_BUCKET", "VIDEO_FOLLOWUP_PUBLIC_ORIGIN", "VIDEO_FOLLOWUP_INITIALS_AFTER",
    "VIDEO_FOLLOWUP_TEST_PHONES", "SMS_CHATBOT_ALLOWED_TOKEN", "TASKER_TRANSPORT_HEALTH_URL",
    "FAL_API_KEY", "FAL_TTS_API_KEY", "VIDEO_FOLLOWUP_SAME_TAKE_MASTER_KEY",
])
def test_bootstrap_requires_complete_configuration_before_clients(monkeypatch, missing):
    configured_environment(monkeypatch)
    monkeypatch.delenv(missing)
    constructors = fake_constructors(monkeypatch)
    with pytest.raises(ValueError):
        bootstrap.create_worker()
    for constructor in constructors:
        constructor.assert_not_called()


@pytest.mark.parametrize("cutoff", ["garbage", "2026-10-13T15:00:00", " "])
def test_activation_cutoff_requires_real_timestamp_and_timezone(monkeypatch, cutoff):
    configured_environment(monkeypatch)
    monkeypatch.setenv("VIDEO_FOLLOWUP_INITIALS_AFTER", cutoff)
    constructors = fake_constructors(monkeypatch)
    with pytest.raises(ValueError):
        bootstrap.create_worker()
    for constructor in constructors:
        constructor.assert_not_called()


@pytest.mark.parametrize("origin", ["http://go.crispshortsales.com", "https://evil.test", "https://user:secret@go.crispshortsales.com", "https://go.crispshortsales.com/path"])
def test_unapproved_player_origin_rejected_before_clients(monkeypatch, origin):
    configured_environment(monkeypatch)
    monkeypatch.setenv("VIDEO_FOLLOWUP_PUBLIC_ORIGIN", origin)
    constructors = fake_constructors(monkeypatch)
    with pytest.raises(ValueError):
        bootstrap.create_worker()
    for constructor in constructors:
        constructor.assert_not_called()


def test_owner_only_bootstrap_uses_exact_phone_allowlist_and_private_storage(monkeypatch):
    env = configured_environment(monkeypatch)
    service, provider_constructor, eligibility_constructor, worker_constructor = fake_constructors(monkeypatch)
    result = bootstrap.create_worker()
    assert result is worker_constructor.return_value
    provider_constructor.assert_called_once_with("test-fal-secret", service.return_value.store,
                                               "assets/approved-master/video", tts_key="test-tts-secret",
                                               generator_profile="legacy_v1")
    eligibility_constructor.assert_called_once_with(env["TASKER_TRANSPORT_HEALTH_URL"], "test-token-secret")
    config = worker_constructor.call_args.args[2]
    assert config.allowlist == {"9542053205"}
    assert not config.live
    assert config.bucket == "crisp-video-private"
    assert config.reserve_cents == 10


def test_bootstrap_failure_logs_only_exception_type(monkeypatch, caplog):
    secret = "PRIVATE-KEY API-TOKEN CREDENTIAL-JSON"
    monkeypatch.setattr(bootstrap, "create_worker", Mock(side_effect=RuntimeError(secret)))
    with caplog.at_level(logging.ERROR, logger=bootstrap.__name__):
        bootstrap.start_worker()
    assert "RuntimeError" in caplog.text
    assert secret not in caplog.text
    assert "PRIVATE-KEY" not in caplog.text


def test_eligibility_transport_failure_never_logs_token_or_payload(caplog):
    session = Mock()
    secret = "secret-auth-token"
    session.post.side_effect = requests.Timeout("request containing " + secret)
    eligible = core.Eligibility("https://script.google.com/macros/s/test/exec", secret, session=session)
    job = core.snapshot(row=2, stable_id="verified-listing", phone="9542053205", first_name="Jim", initial_sent_at="2026-10-13T15:00:00Z")
    job["id"] = "a" * 32
    with caplog.at_level(logging.WARNING, logger=core.__name__):
        assert eligible(job) is None
    assert secret not in caplog.text
    assert "9542053205" not in caplog.text
    assert "https://script.google.com" not in caplog.text
