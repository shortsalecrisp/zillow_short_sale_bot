"""Owner-only orchestration tests; every provider and storage operation is local."""

import importlib.util
import json
import math
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import video_followup as video
import video_followup_provider as provider_module

SPEC = importlib.util.spec_from_file_location("owner_video_sample_under_test", ROOT / "scripts" / "owner_video_sample.py")
owner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = owner
SPEC.loader.exec_module(owner)

NOW = datetime(2026, 10, 13, 16, 0, tzinfo=timezone.utc)
NONCE = "0123456789abcdef0123456789abcdef"
JOB_KEY = "owner-probe-" + NONCE
PROVIDER_ID = "owner-render-verified-123"
OWNER_PHONE = "9542053205"
SECRET = "private-test-secret-must-not-leak"
MEDIA = {
    "media_bytes": b"prepared owner-only mp4",
    "poster_bytes": b"prepared owner-only jpeg",
    "vtt_bytes": b"WEBVTT\n\n00:00.000 --> 00:12.250\nHey Jim.\n",
    "duration": 12.25,
    "caption_source": "provider",
}


class MemoryObjects:
    def __init__(self):
        self.lock = threading.Lock()
        self.objects = {}
        self.generation = 0
        self.writes = []
        self.before_write = None
        self.read_error = None

    def read(self, key):
        if self.read_error:
            raise self.read_error
        with self.lock:
            value = self.objects.get(key)
            return (value[0], value[1]) if value else (None, 0)

    def write(self, key, body, version, content_type="application/json"):
        assert isinstance(body, bytes)
        if self.before_write:
            self.before_write(key, body, version)
        with self.lock:
            existing = self.objects.get(key)
            if (existing[1] if existing else 0) != version:
                raise video.Conflict()
            self.generation += 1
            self.objects[key] = (body, self.generation, content_type)
            self.writes.append((key, body))
            return self.generation

    def list(self, prefix, limit=50):
        with self.lock:
            return sorted(key for key in self.objects if key.startswith(prefix))


class FakeProvider:
    def __init__(self):
        self.submissions = []
        self.inspections = []
        self.preparations = []
        self.submit_error = None
        self.inspect_error = None
        self.prepare_error = None
        self.on_submit = None
        self.snapshot = {"provider_id": PROVIDER_ID, "status": "pending"}

    def submit(self, first_name, job_key):
        self.submissions.append((first_name, job_key))
        if self.on_submit:
            self.on_submit()
        if self.submit_error:
            raise self.submit_error
        return PROVIDER_ID

    def inspect(self, provider_id):
        self.inspections.append(provider_id)
        if self.inspect_error:
            raise self.inspect_error
        return dict(self.snapshot)

    def prepare_media(self, snapshot, first_name):
        self.preparations.append((dict(snapshot), first_name))
        if self.prepare_error:
            raise self.prepare_error
        return dict(MEDIA)


def env(**overrides):
    values = {
        "VIDEO_FOLLOWUP_ENABLED": "false",
        "VIDEO_FOLLOWUP_GENERATION_ENABLED": "false",
        "VIDEO_FOLLOWUP_LIVE_APPROVED": "false",
        "VIDEO_FOLLOWUP_STORAGE_BUCKET": "private-owner-probe-unit-tests",
        "VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "https://go.crispshortsales.com",
        "HEYGEN_API_KEY": "fake-unit-test-key",
        "GCP_SERVICE_ACCOUNT_JSON": "{}",
    }
    values.update(overrides)
    return values


@pytest.fixture(autouse=True)
def forbid_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Owner sample tests must never use the network")

    for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
        monkeypatch.setattr(target, forbidden)


@pytest.fixture
def harness(tmp_path, monkeypatch):
    objects = MemoryObjects()
    store = video.Store(objects)

    def forbidden(*args, **kwargs):
        raise AssertionError("Owner sample must not enter the lead scheduler")

    monkeypatch.setattr(video.Store, "prepare", forbidden)
    monkeypatch.setattr(video, "snapshot", forbidden)
    monkeypatch.setattr(video.Worker, "run_job", forbidden)
    monkeypatch.setattr(video.Worker, "tick", forbidden)
    monkeypatch.setattr(video.Eligibility, "__call__", forbidden)
    return SimpleNamespace(
        objects=objects, store=store, provider=FakeProvider(),
        config=video.Config(env()), receipt_dir=tmp_path / "receipts",
    )


def run(harness, nonce=NONCE):
    return owner.run_step(
        harness.store, harness.provider, harness.config, nonce,
        now=lambda: NOW, receipt_dir=harness.receipt_dir,
    )


def job(harness):
    return harness.store.read("jobs/" + JOB_KEY)[0]


def reservation(harness):
    ledger, _ = harness.store.read("budget/ledger")
    return ledger["reservations"][JOB_KEY]


def assert_sanitized(result):
    assert isinstance(result, dict)
    assert SECRET not in json.dumps(result)


def receipt_text(harness):
    return "\n".join(path.read_text() for path in harness.receipt_dir.rglob("*") if path.is_file())


def complete(harness):
    harness.provider.snapshot = {"provider_id": PROVIDER_ID, "status": "completed", "duration": MEDIA["duration"]}
    return run(harness)


def test_preflight_accepts_disabled_flags_and_missing_flags_default_false():
    values = env()
    config = owner.validate_preflight(values)
    assert isinstance(config, video.Config)
    assert not config.enabled and not config.generation_enabled and not config.live
    for name in ("VIDEO_FOLLOWUP_ENABLED", "VIDEO_FOLLOWUP_GENERATION_ENABLED", "VIDEO_FOLLOWUP_LIVE_APPROVED"):
        values.pop(name)
    config = owner.validate_preflight(values)
    assert not config.enabled and not config.generation_enabled and not config.live


@pytest.mark.parametrize("flag", ["VIDEO_FOLLOWUP_ENABLED", "VIDEO_FOLLOWUP_GENERATION_ENABLED", "VIDEO_FOLLOWUP_LIVE_APPROVED"])
@pytest.mark.parametrize("value", ["true", "1", "yes", ""])
def test_preflight_rejects_any_flag_not_explicitly_false(flag, value):
    with pytest.raises(ValueError):
        owner.validate_preflight(env(**{flag: value}))


@pytest.mark.parametrize("name", ["VIDEO_FOLLOWUP_STORAGE_BUCKET", "VIDEO_FOLLOWUP_PUBLIC_ORIGIN", "HEYGEN_API_KEY", "GCP_SERVICE_ACCOUNT_JSON"])
def test_preflight_requires_configuration_without_revealing_secrets(name):
    values = env(HEYGEN_API_KEY=SECRET)
    values.pop(name)
    with pytest.raises(ValueError) as error:
        owner.validate_preflight(values)
    assert SECRET not in str(error.value)


@pytest.mark.parametrize("override", [
    {"VIDEO_FOLLOWUP_STORAGE_BUCKET": "invalid/bucket"},
    {"VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "https://unapproved.example.test"},
    {"VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "http://go.crispshortsales.com"},
])
def test_preflight_rejects_unapproved_storage_or_public_origin(override):
    with pytest.raises(ValueError):
        owner.validate_preflight(env(**override))


@pytest.mark.parametrize("constant", ["AVATAR_ID", "VOICE_ID", "SCRIPT_TEMPLATE"])
def test_preflight_rejects_provider_identity_drift(monkeypatch, constant):
    monkeypatch.setattr(provider_module, constant, "unapproved-provider-identity")
    with pytest.raises(ValueError):
        owner.validate_preflight(env())


@pytest.mark.parametrize("flag", ["enabled", "generation_enabled", "live"])
def test_run_rejects_enabled_operational_flags_before_submission(harness, flag):
    setattr(harness.config, flag, True)
    try:
        result = run(harness)
    except ValueError:
        pass
    else:
        assert_sanitized(result)
        assert result["status"] not in {"submitted", "ready"}
    assert harness.provider.submissions == []
    assert harness.objects.writes == []


@pytest.mark.parametrize("nonce", ["", "0" * 31, "0" * 33, "A" * 32, "g" * 32, "../owner", "0" * 32 + "\n"])
def test_invalid_nonce_never_reads_or_writes_remote_state(harness, nonce):
    harness.objects.read_error = AssertionError("Invalid nonce must fail before storage")
    try:
        result = run(harness, nonce)
    except ValueError:
        pass
    else:
        assert_sanitized(result)
        assert result["status"] not in {"submitted", "ready"}
    assert harness.provider.submissions == []
    assert harness.objects.writes == []


def test_first_step_is_one_owner_submission_with_budget_and_cas_before_provider(harness):
    def check_intent():
        record = job(harness)
        assert record["status"] == "submitting"
        assert record["reserved_at"] and record["lease_at"]
        assert not record.get("provider_id")
        assert reservation(harness)["cents"] == 50
        assert reservation(harness)["inflight"] is True

    harness.provider.on_submit = check_intent
    result = run(harness)
    assert result["status"] == "submitted"
    assert result["job_key"] == JOB_KEY
    assert result["provider_id"] == PROVIDER_ID
    assert re.fullmatch(r"[0-9a-f]{32}", result["opaque_id"])
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    record = job(harness)
    assert record["kind"] == "owner_video_sample"
    assert record["first_name"] == "Jim"
    assert record.get("owner_phone", record.get("phone")) == OWNER_PHONE
    assert record["id"] == result["opaque_id"]
    assert record["status"] == "submitted"
    assert record["provider_id"] == PROVIDER_ID
    assert not ({"row", "crm_row", "stable_id", "initial_sent_at", "message_id", "request_id"} & record.keys())
    assert not harness.objects.list("active/")
    assert PROVIDER_ID in receipt_text(harness)
    assert JOB_KEY in receipt_text(harness)


@pytest.mark.parametrize("status", ["pending", "processing"])
def test_resume_inspects_same_provider_id_without_reserving_or_resubmitting(harness, monkeypatch, status):
    first = run(harness)
    harness.provider.snapshot["status"] = status
    monkeypatch.setattr(harness.store, "reserve", lambda *args: pytest.fail("Resume must not reserve again"))
    result = run(harness)
    assert result["opaque_id"] == first["opaque_id"]
    assert result["provider_id"] == PROVIDER_ID
    assert harness.provider.inspections == [PROVIDER_ID]
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert harness.provider.preparations == []
    assert reservation(harness)["inflight"] is True


def test_completed_render_is_settled_published_and_read_back_without_scheduler(harness, monkeypatch):
    first = run(harness)
    monkeypatch.setattr(harness.store, "reserve", lambda *args: pytest.fail("Completion must not reserve again"))
    result = complete(harness)
    assert result["status"] == "ready"
    assert result["opaque_id"] == first["opaque_id"]
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert harness.provider.preparations == [(harness.provider.snapshot, "Jim")]
    assert reservation(harness)["cents"] == math.ceil(MEDIA["duration"])
    assert reservation(harness)["inflight"] is False
    assert job(harness)["status"] == "ready"
    pointer, _ = harness.store.read("public/" + result["opaque_id"])
    assert pointer == {"key": JOB_KEY}
    assert harness.store.get_public_video(result["opaque_id"]) == {"first_name": "Jim", "duration": MEDIA["duration"], "status": "ready"}
    for kind, field in (("video", "media_bytes"), ("poster", "poster_bytes"), ("captions", "vtt_bytes")):
        assert harness.store.get_public_asset(result["opaque_id"], kind)["body"] == MEDIA[field]
    assert not harness.objects.list("active/")
    again = run(harness)
    assert again["status"] == "ready"
    assert harness.provider.submissions == [("Jim", JOB_KEY)]


def test_confirmed_provider_failure_keeps_conservative_cost_and_never_retries(harness):
    run(harness)
    harness.provider.snapshot = {"provider_id": PROVIDER_ID, "status": "failed", "failure_code": SECRET}
    result = run(harness)
    assert result["status"] == "failed"
    assert_sanitized(result)
    assert reservation(harness)["cents"] == 50
    assert reservation(harness)["inflight"] is False
    assert job(harness)["status"] == "failed"
    run(harness)
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert not harness.objects.list("public/")


@pytest.mark.parametrize("error", [provider_module.SubmissionUncertain(JOB_KEY), RuntimeError(SECRET)])
def test_uncertain_submission_holds_budget_and_is_never_repeated(harness, error):
    harness.provider.submit_error = error
    result = run(harness)
    assert_sanitized(result)
    assert job(harness)["status"] in {"submitting", "uncertain"}
    assert reservation(harness)["cents"] == 50
    assert reservation(harness)["inflight"] is True
    harness.provider.submit_error = None
    run(harness)
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert harness.provider.inspections == []
    assert not harness.objects.list("public/")


def test_definite_rejection_settles_conservative_cost_without_retry(harness):
    harness.provider.submit_error = provider_module.SubmissionRejected("submission_rejected", SECRET)
    result = run(harness)
    assert result["status"] == "failed"
    assert_sanitized(result)
    assert reservation(harness)["cents"] == 50
    assert reservation(harness)["inflight"] is False
    run(harness)
    assert harness.provider.submissions == [("Jim", JOB_KEY)]


def test_provider_receipt_is_local_before_cloud_persistence_and_recovers_same_render(harness):
    failed = []

    def fail_receipt_write(key, body, version):
        if key == "owner-submission-receipts/" + NONCE:
            assert PROVIDER_ID in receipt_text(harness)
            assert JOB_KEY in receipt_text(harness)
        if key == "jobs/" + JOB_KEY and json.loads(body).get("provider_id") and not failed:
            assert PROVIDER_ID in receipt_text(harness)
            assert JOB_KEY in receipt_text(harness)
            failed.append(True)
            raise OSError(SECRET)

    harness.objects.before_write = fail_receipt_write
    result = run(harness)
    assert failed
    assert_sanitized(result)
    assert result.get("provider_id") == PROVIDER_ID
    assert job(harness)["status"] in {"submitting", "uncertain"}
    harness.objects.before_write = None
    resumed = run(harness)
    assert resumed.get("provider_id") == PROVIDER_ID
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert harness.provider.inspections == []
    run(harness)
    assert harness.provider.inspections == [PROVIDER_ID]


def test_budget_exhaustion_never_submits(harness):
    harness.config.daily_cents = 0
    result = run(harness)
    assert_sanitized(result)
    assert result["status"] not in {"submitted", "ready"}
    assert harness.provider.submissions == []
    assert not harness.objects.list("public/")


def test_storage_read_failure_never_submits_and_is_sanitized(harness):
    harness.objects.read_error = OSError(SECRET)
    result = run(harness)
    assert_sanitized(result)
    assert harness.provider.submissions == []


def test_inspection_failure_keeps_same_job_and_budget_without_new_submission(harness):
    run(harness)
    harness.provider.inspect_error = RuntimeError(SECRET)
    result = run(harness)
    assert_sanitized(result)
    assert result["provider_id"] == PROVIDER_ID
    assert reservation(harness)["inflight"] is True
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert not harness.objects.list("public/")


def test_media_processing_failure_does_not_publish_or_purchase_another_render(harness):
    run(harness)
    harness.provider.prepare_error = RuntimeError(SECRET)
    result = complete(harness)
    assert_sanitized(result)
    assert result["status"] != "ready"
    assert not harness.objects.list("public/")
    assert reservation(harness)["cents"] == math.ceil(MEDIA["duration"])
    assert reservation(harness)["inflight"] is False
    assert harness.provider.submissions == [("Jim", JOB_KEY)]


def test_concurrent_calls_cannot_submit_twice(harness):
    barrier = threading.Barrier(2)

    def invoke(_):
        barrier.wait(timeout=5)
        return run(harness)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(invoke, range(2)))
    assert all(isinstance(result, dict) for result in results)
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert not harness.objects.list("active/")


def test_unusable_receipt_directory_prevents_purchase(harness):
    harness.receipt_dir.write_text("not a directory")
    try:
        result = run(harness)
    except (ValueError, OSError):
        pass
    else:
        assert_sanitized(result)
        assert result["status"] not in {"submitted", "ready"}
    assert harness.provider.submissions == []


def test_existing_worker_index_blocks_owner_sample_before_purchase(harness):
    harness.store.write("active/" + JOB_KEY, {"key": JOB_KEY}, 0)
    result = run(harness)
    assert result["ok"] is False
    assert harness.provider.submissions == []
    assert not harness.objects.list("budget/")


@pytest.mark.parametrize("field, value", [
    ("first_name", "SomeoneElse"),
    ("owner_phone", "2125550100"),
    ("kind", "lead_followup"),
    ("row", 1234),
    ("script_sha256", "wrong-script"),
])
def test_existing_job_identity_drift_never_calls_provider(harness, field, value):
    run(harness)
    harness.store.mutate("jobs/" + JOB_KEY, lambda old: {**old, field: value})
    result = run(harness)
    assert result["ok"] is False
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert harness.provider.inspections == []


@pytest.mark.parametrize("field, value", [
    ("job_key", "owner-probe-" + "f" * 32),
    ("nonce", "f" * 32),
    ("id", "f" * 32),
    ("attempt_token", "f" * 32),
    ("first_name", "SomeoneElse"),
    ("owner_phone", "2125550100"),
    ("script_sha256", "wrong-script"),
])
def test_local_receipt_must_match_every_bound_identity_field(harness, field, value):
    def fail_remote_receipt(key, body, version):
        if key == "owner-submission-receipts/" + NONCE:
            raise OSError(SECRET)

    harness.objects.before_write = fail_remote_receipt
    result = run(harness)
    assert result.get("provider_id") == PROVIDER_ID
    harness.objects.before_write = None
    path = harness.receipt_dir / (NONCE + ".json")
    receipt = json.loads(path.read_text())
    receipt[field] = value
    path.write_text(json.dumps(receipt))
    result = run(harness)
    assert result["ok"] is False
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert harness.provider.inspections == []
    assert not job(harness).get("provider_id")


@pytest.mark.parametrize("remaining_receipt", ["local", "remote"])
def test_orphan_receipt_blocks_a_new_purchase_when_job_is_missing(harness, remaining_receipt):
    run(harness)
    with harness.objects.lock:
        harness.objects.objects.pop("jobs/" + JOB_KEY)
        if remaining_receipt == "local":
            harness.objects.objects.pop("owner-submission-receipts/" + NONCE)
    if remaining_receipt == "remote":
        (harness.receipt_dir / (NONCE + ".json")).unlink()
    result = run(harness)
    assert result["ok"] is False
    assert harness.provider.submissions == [("Jim", JOB_KEY)]
    assert harness.provider.inspections == []


def test_existing_receipt_blocks_prepared_record_from_being_submitted_again(harness):
    run(harness)

    def reset_to_prepared(old):
        old.pop("provider_id", None)
        old.pop("attempt_token", None)
        return {**old, "status": "prepared"}

    harness.store.mutate("jobs/" + JOB_KEY, reset_to_prepared)
    result = run(harness)
    assert result["ok"] is False
    assert harness.provider.submissions == [("Jim", JOB_KEY)]


@pytest.mark.parametrize("kind", ["video", "poster", "captions"])
def test_conflicting_existing_asset_fails_publication_and_cancels_public_access(harness, kind):
    first = run(harness)
    harness.objects.write(f"assets/{first['opaque_id']}/{kind}", b"wrong preexisting media", 0)
    result = complete(harness)
    assert result["ok"] is False
    assert not result.get("publication_verified")
    assert job(harness)["status"] == "cancelled"
    assert harness.store.get_public_video(first["opaque_id"]) is None
    assert harness.provider.submissions == [("Jim", JOB_KEY)]


def test_conflicting_public_pointer_cannot_be_reported_as_verified(harness):
    first = run(harness)
    harness.store.write("public/" + first["opaque_id"], {"key": "different-owner-probe"}, 0)
    result = complete(harness)
    assert result["ok"] is False
    assert not result.get("publication_verified")
    assert job(harness)["status"] == "cancelled"
    assert harness.provider.submissions == [("Jim", JOB_KEY)]


def test_ready_resume_rechecks_hashes_without_any_provider_calls(harness):
    first = run(harness)
    assert complete(harness)["status"] == "ready"
    before = (list(harness.provider.submissions), list(harness.provider.inspections), list(harness.provider.preparations))
    path = f"assets/{first['opaque_id']}/video"
    _, generation = harness.objects.read(path)
    harness.objects.write(path, b"corrupt replacement", generation)
    result = run(harness)
    assert result["ok"] is False
    assert not result.get("publication_verified")
    assert (harness.provider.submissions, harness.provider.inspections, harness.provider.preparations) == before


def test_inspected_provider_id_mismatch_cannot_publish(harness):
    run(harness)
    harness.provider.snapshot = {"provider_id": "different-render", "status": "completed", "duration": 12.25}
    result = run(harness)
    assert result["ok"] is False
    assert harness.provider.preparations == []
    assert not harness.objects.list("public/")
    assert reservation(harness)["inflight"] is True


@pytest.mark.parametrize("duration", [0, -1, 31, float("nan"), float("inf")])
def test_invalid_completed_duration_never_processes_or_publishes(harness, duration):
    run(harness)
    harness.provider.snapshot = {"provider_id": PROVIDER_ID, "status": "completed", "duration": duration}
    result = run(harness)
    assert result["ok"] is False
    assert harness.provider.preparations == []
    assert not harness.objects.list("public/")
    assert reservation(harness)["inflight"] is True
