import hashlib
import json
from pathlib import Path
import sys
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
from unittest.mock import Mock

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import video_followup as core
import video_followup_same_take as provider


MASTER_KEY = "assets/approved-second-recording/video"
MASTER = b"the approved complete second recording"
NARRATION_ID = "a" * 32
FAL_ID = "12345678-abcd-1234-abcd-123456789012"
MEDIA_URL = "https://v3.fal.media/files/private/lipsync.mp4"
INPUTS = {"video_bytes": b"same-source context", "audio_bytes": b"context audio",
          "greeting_audio_bytes": b"new greeting audio"}


def prepared_inputs(name="Sue"):
    return {**INPUTS, "metadata": {"source_sha256": provider.MASTER_SHA256, "first_name": name,
            "context_seconds": 2.0, "video_sha256": hashlib.sha256(INPUTS["video_bytes"]).hexdigest(),
            "audio_sha256": hashlib.sha256(INPUTS["audio_bytes"]).hexdigest(),
            "greeting_audio_sha256": hashlib.sha256(INPUTS["greeting_audio_bytes"]).hexdigest()}}


class MemoryObjects:
    def __init__(self):
        self.lock = Lock()
        self.data = {}
        self.generation = 0

    def read(self, key):
        with self.lock:
            value = self.data.get(key)
            return (value[0], value[1]) if value else (None, 0)

    def write(self, key, body, version, content_type="application/json"):
        with self.lock:
            old = self.data.get(key)
            if (old[1] if old else 0) != version:
                raise core.Conflict()
            self.generation += 1
            self.data[key] = (body, self.generation)
            return self.generation

    def signed_get_url(self, key, ttl_seconds=1800):
        return "https://storage.googleapis.com/private/video-followup/" + key + "?signed=test"


class Response:
    def __init__(self, payload, status=200):
        self.payload, self.status_code = payload, status
        self.content = json.dumps(payload).encode()
        self.closed = False

    def json(self):
        return self.payload

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    for path in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
        monkeypatch.setattr(path, Mock(side_effect=AssertionError("No external network in provider tests")))


@pytest.fixture
def world(monkeypatch):
    monkeypatch.setattr(provider, "MASTER_SHA256", hashlib.sha256(MASTER).hexdigest())
    objects = MemoryObjects()
    objects.write(MASTER_KEY, MASTER, 0)
    store, session, media_session = core.Store(objects), Mock(), Mock()
    session.request.side_effect = lambda method, url, **kwargs: Response(
        {"data": {"video_id": NARRATION_ID}} if url.endswith("/videos") else
        {"request_id": FAL_ID, "status_url": "https://evil.example/stolen", "response_url": "https://evil.example/also-stolen"})
    prepare, compose = Mock(side_effect=lambda *args, first_name: prepared_inputs(first_name)), Mock(return_value={
        "media_bytes": b"final same-take video", "poster_bytes": b"poster", "captions_bytes": b"WEBVTT",
        "duration_seconds": 8.53, "qa": {"source_continuity": True}})
    service = provider.SameTakeVideoProvider("heygen-test-secret", "fal-test-secret", store, MASTER_KEY,
        session=session, media_session=media_session, resolver=lambda hostname: ["8.8.8.8"],
        preparer=prepare, compositor=compose)
    service.heygen = Mock()
    service.heygen.inspect.return_value = {"status": "completed", "duration": 1.5,
                                           "media_url": "https://resource.heygen.ai/audio-source.mp4"}
    service.heygen._download.return_value = b"narration source"
    return service, store, session, media_session, prepare, compose


def active_job(store, first_name="Sue", job_id="test-job"):
    store.write("jobs/" + job_id, {"script_version": core.SCRIPT_VERSION,
                "status": "submitting", "first_name": first_name}, 0)
    return job_id


def pipeline(world, name="Sue"):
    service, store, *_ = world
    job_id = active_job(store, name)
    return service.submit(name, job_id)


def state(world, pipeline_id):
    return world[1].read("same-take/" + pipeline_id)[0]


def test_approved_master_hash_pins_full_second_recording():
    assert provider.MASTER_SHA256 == "01efd28101e8b016d51d571cc87d38d13ea623389663bc7a88cf26cce39bd60b"


def test_bad_or_missing_master_blocks_payment(world):
    service, store, session, *_ = world
    job_id = active_job(store)
    _, version = store.objects.read(MASTER_KEY)
    store.objects.write(MASTER_KEY, b"old avatar or different source", version)
    with pytest.raises(provider.VideoProviderError, match="master"):
        service.submit("Sue", job_id)
    session.request.assert_not_called()
    service.master_key = "assets/missing/video"
    with pytest.raises(provider.VideoProviderError, match="master"):
        service.submit("Sue", job_id)
    session.request.assert_not_called()


@pytest.mark.parametrize("name", ["Sue", "Christopher", "Anne-Marie", "O'Neil"])
def test_full_verified_name_is_used_without_truncation(world, name):
    service, _, session, *_ = world
    pipeline_id = pipeline(world, name)
    call = session.request.call_args
    assert call.args == ("POST", provider.API_BASE + "/videos")
    assert call.kwargs["json"]["script"] == "Hey " + name + "!"
    assert call.kwargs["json"]["engine"] == {"type": "avatar_iii"}
    assert call.kwargs["headers"]["Idempotency-Key"] == pipeline_id
    assert call.kwargs["allow_redirects"] is False
    assert state(world, pipeline_id)["first_name"] == name
    assert session.request.call_count == 1


@pytest.mark.parametrize("raw,canonical", [(" \uff33\uff55\uff45 ", "Sue"), ("O\u2019Neil", "O'Neil"), ("Jose\u0301", "Jos\u00e9")])
def test_normalization_matches_job_and_provider_script(world, raw, canonical):
    service, store, session, *_ = world
    job_id = active_job(store, raw)
    pipeline_id = service.submit(raw, job_id)
    assert state(world, pipeline_id)["first_name"] == canonical
    assert session.request.call_args.kwargs["json"]["script"] == "Hey " + canonical + "!"


@pytest.mark.parametrize("name", ["Jim Smith", "Mary Jane", "Jim/Jane", "", "unknown", "J", "Jim2"])
def test_uncertain_name_never_pays(world, name):
    service, store, session, *_ = world
    job_id = active_job(store, name)
    with pytest.raises(provider.VideoProviderError):
        service.submit(name, job_id)
    session.request.assert_not_called()


def test_same_job_cannot_change_name(world):
    service, store, session, *_ = world
    pipeline_id = pipeline(world)
    store.mutate("jobs/test-job", lambda old: {**old, "first_name": "Christopher"})
    with pytest.raises(provider.VideoProviderError, match="different name"):
        service.submit("Christopher", "test-job")
    assert session.request.call_count == 1
    assert state(world, pipeline_id)["first_name"] == "Sue"


def test_repeated_submit_never_repeats_narration_payment(world):
    service, _, session, *_ = world
    pipeline_id = pipeline(world)
    assert service.submit("Sue", "test-job") == pipeline_id
    assert service.submit("Sue", "test-job") == pipeline_id
    assert session.request.call_count == 1


def test_concurrent_submissions_pay_for_narration_only_once(world):
    service, store, session, *_ = world
    job_id = active_job(store)
    barrier = Barrier(8)
    def submit(_):
        barrier.wait(timeout=5)
        try:
            return service.submit("Sue", job_id)
        except core.Conflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(8)))
    ids = {result for result in results if result != "conflict"}
    assert len(ids) == 1
    assert session.request.call_count == 1


@pytest.mark.parametrize("failure", [requests.Timeout("private provider details"), Response({}, 500), Response({"data": {}}, 200)])
def test_unknown_narration_outcome_never_resubmits(world, failure):
    service, store, session, *_ = world
    job_id = active_job(store)
    session.request.side_effect = failure if isinstance(failure, Exception) else lambda *args, **kwargs: failure
    with pytest.raises(provider.SubmissionUncertain):
        service.submit("Sue", job_id)
    pipeline_id = service.submit("Sue", job_id)
    assert service.inspect(pipeline_id)["status"] == "failed"
    assert state(world, pipeline_id)["stage"] == "uncertain"
    assert session.request.call_count == 1


def test_unknown_fal_outcome_never_resubmits(world):
    service, _, session, *_ = world
    pipeline_id = pipeline(world)
    session.request.side_effect = requests.Timeout("unknown queue outcome")
    with pytest.raises(provider.SubmissionUncertain):
        service.inspect(pipeline_id)
    assert service.inspect(pipeline_id)["status"] == "failed"
    assert service.submit("Sue", "test-job") == pipeline_id
    assert state(world, pipeline_id)["stage"] == "uncertain"
    assert session.request.call_count == 2


@pytest.mark.parametrize("stage", ["narration_pending", "fal_pending"])
def test_restart_resumes_only_known_provider_request(world, stage):
    service, _, session, *_ = world
    pipeline_id = pipeline(world)
    if stage == "fal_pending":
        service.inspect(pipeline_id)
        session.request.side_effect = lambda *args, **kwargs: Response({"status": "IN_PROGRESS"})
    else:
        service.heygen.inspect.return_value = {"status": "processing"}
    before = session.request.call_count
    for _ in range(3):
        assert service.inspect(pipeline_id)["status"] == "processing"
    assert all(call.args[0] == "GET" for call in session.request.call_args_list[before:])


def test_competing_polls_pay_for_fal_once(world):
    service, _, session, _, prepare, _ = world
    pipeline_id = pipeline(world)
    barrier = Barrier(2)
    def inputs(*args, **kwargs):
        barrier.wait(timeout=5)
        return prepared_inputs()
    prepare.side_effect = inputs
    def poll():
        try:
            return service.inspect(pipeline_id)
        except core.Conflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: poll(), range(2)))
    assert "conflict" in results
    assert state(world, pipeline_id)["stage"] == "fal_pending"
    assert len([call for call in session.request.call_args_list if call.args == ("POST", provider.QUEUE + provider.MODEL)]) == 1


def test_accounting_only_inspection_never_starts_fal(world):
    service, _, session, _, prepare, _ = world
    pipeline_id = pipeline(world)
    assert service.inspect(pipeline_id, accounting_only=True)["status"] == "failed"
    assert session.request.call_count == 1
    prepare.assert_not_called()
    service.heygen._download.assert_not_called()


@pytest.mark.parametrize("status", ["cancelled", "frozen_text", "ready"])
def test_cancelled_or_no_longer_active_job_never_starts_fal(world, status):
    service, store, session, _, prepare, _ = world
    pipeline_id = pipeline(world)
    store.mutate("jobs/test-job", lambda old: {**old, "status": status})
    assert service.inspect(pipeline_id)["status"] == "failed"
    assert session.request.call_count == 1
    prepare.assert_not_called()


def test_reply_during_input_preparation_cancels_before_fal_payment(world):
    service, store, session, _, prepare, _ = world
    pipeline_id = pipeline(world)
    def cancel(*args, **kwargs):
        store.mutate("jobs/test-job", lambda old: {**old, "status": "cancelled"})
        return prepared_inputs()
    prepare.side_effect = cancel
    assert service.inspect(pipeline_id)["status"] == "failed"
    assert session.request.call_count == 1
    assert state(world, pipeline_id)["stage"] == "cancelled"


@pytest.mark.parametrize("duration", [0, -1, 3.01, float("inf"), float("nan")])
def test_overlong_or_invalid_narration_blocks_fal_budget(world, duration):
    service, _, session, _, prepare, _ = world
    pipeline_id = pipeline(world, "Christopher")
    service.heygen.inspect.return_value.update(duration=duration)
    assert service.inspect(pipeline_id)["status"] == "failed"
    assert state(world, pipeline_id)["failure_code"] == "narration_exceeds_budget"
    assert session.request.call_count == 1
    prepare.assert_not_called()


def test_fal_uses_same_recording_inputs_and_only_canonical_authenticated_poll_urls(world):
    service, _, session, _, prepare, _ = world
    pipeline_id = pipeline(world)
    service.inspect(pipeline_id)
    call = session.request.call_args
    assert call.args == ("POST", provider.QUEUE + provider.MODEL)
    assert call.kwargs["headers"]["X-Fal-No-Retry"] == "1"
    assert call.kwargs["json"]["video_url"].startswith("https://storage.googleapis.com/private/video-followup/assets/" + pipeline_id)
    prepare.assert_called_once_with(MASTER, b"narration source", first_name="Sue")
    session.request.side_effect = [Response({"status": "COMPLETED"}), Response({"video": {"url": MEDIA_URL}})]
    completed = service.inspect(pipeline_id)
    assert completed["status"] == "completed"
    assert completed["accounting_method"] == "conservative_reservation"
    assert "billed_cents" not in completed
    calls = session.request.call_args_list[-2:]
    assert [call.args for call in calls] == [
        ("GET", provider.REQUEST_BASE + FAL_ID + "/status"), ("GET", provider.REQUEST_BASE + FAL_ID)]
    assert all(call.kwargs["allow_redirects"] is False for call in calls)
    assert all(call.kwargs["headers"] == {"Authorization": "Key fal-test-secret"} for call in calls)


@pytest.mark.parametrize("url", ["http://v3.fal.media/files/a", "https://evil.example/files/a", "https://v3.fal.media.evil.example/files/a",
    "https://user:pass@v3.fal.media/files/a", "https://v3.fal.media:444/files/a", "https://v3.fal.media/private/a",
    "https://v3.fal.media/files/a#secret", "https://127.0.0.1/files/a", "https://v3.fal.media/files/a\\b", None])
def test_fal_ssrf_urls_rejected_before_download(world, url):
    service, _, _, media, *_ = world
    with pytest.raises(provider.VideoProviderError):
        service._download_fal(url)
    media.get.assert_not_called()


@pytest.mark.parametrize("addresses", [["127.0.0.1"], ["10.0.0.2"], ["169.254.169.254"], ["::1"], ["8.8.8.8", "10.0.0.1"], []])
def test_private_dns_answers_rejected_before_download(world, addresses):
    service, _, _, media, *_ = world
    service.resolver = lambda hostname: addresses
    with pytest.raises(provider.VideoProviderError):
        service._download_fal(MEDIA_URL)
    media.get.assert_not_called()


def test_media_name_must_match_and_original_master_remains_the_source(world):
    service, _, session, _, _, compose = world
    pipeline_id = pipeline(world)
    service.inspect(pipeline_id)
    session.request.side_effect = [Response({"status": "COMPLETED"}), Response({"video": {"url": MEDIA_URL}})]
    completed = service.inspect(pipeline_id)
    service._download_fal = Mock(return_value=b"same-source lip synced context")
    with pytest.raises(provider.VideoProviderError):
        service.prepare_media(completed, "Christopher")
    compose.assert_not_called()
    result = service.prepare_media(completed, "Sue")
    compose.assert_called_once_with(MASTER, b"same-source lip synced context", b"new greeting audio",
                                    prepared_inputs()["metadata"], first_name="Sue")
    assert result["media_bytes"] == b"final same-take video"
    assert result["duration"] == 8.53
    assert state(world, pipeline_id)["qa"] == {"source_continuity": True}


def test_changed_source_after_narration_blocks_fal(world):
    service, store, session, _, prepare, _ = world
    pipeline_id = pipeline(world)
    _, version = store.objects.read(MASTER_KEY)
    store.objects.write(MASTER_KEY, b"replacement take", version)
    with pytest.raises(provider.VideoProviderError):
        service.inspect(pipeline_id)
    assert session.request.call_count == 1
    prepare.assert_not_called()


@pytest.mark.parametrize("field,value", [("source_sha256", "wrong-source"), ("first_name", "Christopher"),
    ("context_seconds", 5.01), ("context_seconds", 1.99), ("context_seconds", float("nan")),
    ("video_sha256", "changed"), ("audio_sha256", "changed"), ("greeting_audio_sha256", "changed")])
def test_prepared_input_identity_and_budget_are_checked_before_fal(world, field, value):
    service, _, session, _, prepare, _ = world
    pipeline_id = pipeline(world)
    payload = prepared_inputs()
    payload["metadata"][field] = value
    prepare.side_effect = None
    prepare.return_value = payload
    with pytest.raises(provider.VideoProviderError):
        service.inspect(pipeline_id)
    assert session.request.call_count == 1
    assert state(world, pipeline_id)["stage"] == "failed"


@pytest.mark.parametrize("field,value", [("id", "same-take-" + "a" * 32), ("job_key", "other-job"),
    ("reserved_cents", 50), ("script_version", "face-to-text-v1"), ("source_sha256", "old-source"),
    ("first_name", "Sue Jones")])
def test_tampered_pipeline_identity_never_polls_or_submits(world, field, value):
    service, store, session, *_ = world
    pipeline_id = pipeline(world)
    store.mutate("same-take/" + pipeline_id, lambda old: {**old, field: value})
    with pytest.raises(provider.VideoProviderError):
        service.inspect(pipeline_id)
    assert session.request.call_count == 1
    service.heygen.inspect.assert_not_called()


@pytest.mark.parametrize("stage", ["narration_submitting", "fal_submitting"])
def test_pending_paid_stage_is_not_failed_early_or_retried_after_expiry(world, monkeypatch, stage):
    service, store, session, *_ = world
    pipeline_id = pipeline(world)
    now = core.utcnow()
    store.mutate("same-take/" + pipeline_id, lambda old: {**old, "stage": stage,
        "created_at": core.iso(now), "fal_attempt_at": core.iso(now)})
    monkeypatch.setattr(provider, "utcnow", lambda: now + timedelta(seconds=599))
    assert service.inspect(pipeline_id)["status"] == "processing"
    assert state(world, pipeline_id)["stage"] == stage
    monkeypatch.setattr(provider, "utcnow", lambda: now + timedelta(seconds=601))
    assert service.inspect(pipeline_id)["status"] == "failed"
    assert state(world, pipeline_id)["stage"] == "uncertain"
    assert service.submit("Sue", "test-job") == pipeline_id
    assert session.request.call_count == 1


def test_expected_stage_guard_does_not_revert_newer_completed_pipeline(world):
    service, store, _, *_ = world
    pipeline_id = pipeline(world)
    store.mutate("same-take/" + pipeline_id, lambda old: {**old, "stage": "completed", "media_url": MEDIA_URL})
    latest = service._update(pipeline_id, {"stage": "failed"}, "narration_pending")
    assert latest["stage"] == "completed"
    assert state(world, pipeline_id)["media_url"] == MEDIA_URL
