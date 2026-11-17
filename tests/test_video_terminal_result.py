import hashlib
import json
from unittest.mock import Mock

import pytest

import test_video_followup_same_take as fixtures
import test_video_followup as core_fixtures
import video_followup_same_take as provider

world = fixtures.world
no_network = fixtures.no_network


def start_future(world):
    service, store, *_ = world
    store.write("jobs/future", {"script_version": provider.SCRIPT_VERSION, "status": "submitting",
        "first_name": "Sue", "provider_result_handling_version": provider.PROVIDER_RESULT_HANDLING_VERSION}, 0)
    return service.submit("Sue", "future")


def at_fal_pending(world, *, future=True):
    identifier = start_future(world) if future else fixtures.start(world)
    assert world[0].inspect(identifier)["status"] == "processing"
    assert fixtures.state(world, identifier)["stage"] == "fal_pending"
    return identifier


def install_result(world, status=504, value=None, *, status_identity=fixtures.FAL_ID, queue_state="COMPLETED"):
    session = world[2]
    original = session.request.side_effect
    body = value if value is not None else {"detail": [{"type": "downstream_service_unavailable",
        "msg": "private detail https://private/?token=hidden", "input": "secret"}]}
    def request(method, url, **kwargs):
        if (method, url) == ("GET", provider.REQUEST_BASE + fixtures.FAL_ID + "/status"):
            return fixtures.Response({"status": queue_state, "request_id": status_identity})
        if (method, url) == ("GET", provider.REQUEST_BASE + fixtures.FAL_ID):
            return fixtures.Response(body, status)
        return original(method, url, **kwargs)
    session.request.side_effect = request
    return body


def test_new_pipeline_pins_handling_marker_and_old_pipeline_does_not(world):
    future = start_future(world)
    legacy = fixtures.start(world)
    assert fixtures.state(world, future)["provider_result_handling_version"] == provider.PROVIDER_RESULT_HANDLING_VERSION
    assert "provider_result_handling_version" not in fixtures.state(world, legacy)


@pytest.mark.parametrize("accounting_only", [False, True])
def test_completed_typed_504_is_terminal_without_paid_retry_or_refund(world, accounting_only):
    identifier = at_fal_pending(world)
    body = install_result(world)
    paid_before = len(fixtures.paid_calls(world))
    result = world[0].inspect(identifier, accounting_only=accounting_only)
    assert result["status"] == "failed" and result["failure_code"] == "fal_render_failed"
    assert result["provider_http_status"] == 504
    assert result["provider_request_id"] == fixtures.FAL_ID
    assert result["underlying_code"] == "downstream_service_unavailable"
    assert result["provider_response_sha256"] == hashlib.sha256(json.dumps(body).encode()).hexdigest()
    assert "secret" not in str(result) and "hidden" not in str(result)
    assert "billed_cents" not in result and "cost_cents" not in result
    assert fixtures.state(world, identifier)["stage"] == "failed"
    assert world[0].inspect(identifier)["status"] == "failed"
    assert len(fixtures.paid_calls(world)) == paid_before


def test_old_completed_error_stays_unmodified_and_is_not_reinterpreted(world):
    identifier = at_fal_pending(world, future=False)
    install_result(world)
    before = world[1].read("same-take/" + identifier)
    with pytest.raises(provider.VideoProviderError, match="not accepted"):
        world[0].inspect(identifier, accounting_only=True)
    assert world[1].read("same-take/" + identifier) == before


@pytest.mark.parametrize("status,value", [
    (504, {"detail": "gateway error"}), (502, {"error_type": "unknown_gateway"}),
    (401, {"error_type": "internal_error"}), (403, {"error_type": "internal_error"}),
    (404, {"error_type": "internal_error"}), (429, {"error_type": "internal_error"}),
    (504, {"error_type": "request_timeout", "request_id": "different-request-id"}),
    (504, {"error_type": "request_timeout", "type": "internal_error"}),
])
def test_unbound_or_untyped_access_and_gateway_failures_remain_uncertain(world, status, value):
    identifier = at_fal_pending(world)
    install_result(world, status, value)
    before = world[1].read("same-take/" + identifier)
    with pytest.raises(provider.VideoProviderError):
        world[0].inspect(identifier)
    assert world[1].read("same-take/" + identifier) == before


@pytest.mark.parametrize("identity", [None, "different-request-id"])
def test_mismatched_completed_status_cannot_release_capacity(world, identity):
    identifier = at_fal_pending(world)
    install_result(world, status_identity=identity)
    before = world[1].read("same-take/" + identifier)
    with pytest.raises(provider.VideoProviderError, match="bind"):
        world[0].inspect(identifier)
    assert world[1].read("same-take/" + identifier) == before


def test_still_processing_request_does_not_fetch_or_classify_terminal_result(world):
    identifier = at_fal_pending(world)
    install_result(world, queue_state="IN_PROGRESS")
    result = world[0].inspect(identifier)
    assert result["status"] == "processing"
    assert fixtures.state(world, identifier)["stage"] == "fal_pending"


def test_successful_future_result_keeps_video_verification_path(world):
    identifier = at_fal_pending(world)
    install_result(world, 200, {"video": {"url": fixtures.FINAL_VIDEO}})
    assert world[0].inspect(identifier)["status"] == "completed"
    assert fixtures.state(world, identifier)["stage"] == "completed"


class CapacityLoggingTests(core_fixtures.unittest.TestCase):
    setUp = core_fixtures.VideoFollowupTests.setUp
    make_config = core_fixtures.VideoFollowupTests.make_config
    make_context = core_fixtures.VideoFollowupTests.make_context
    worker = core_fixtures.VideoFollowupTests.worker

    def test_capacity_hold_is_logged_once_and_keeps_old_debits_and_private_slot(self):
        self.service.prepare(self.context)
        key = self.context["followup_key"]
        self.store.reserve("private-audio-protected", self.config, core_fixtures.NOW)
        self.store.reserve("old-provider-reservation", self.config, core_fixtures.NOW)
        before = self.store.read("budget/ledger")
        self.worker().run_job(key)
        job = self.store.read("jobs/" + key)[0]
        assert job["budget_hold_reason"] == "inflight_limit" and job["budget_hold_inflight"] == 2
        assert self.provider.submissions == []
        count = len(self.objects.writes)
        self.worker().run_job(key)
        assert len(self.objects.writes) == count
        assert self.store.read("budget/ledger") == before
        text, meta = self.service.select(self.context, core_fixtures.FALLBACK)
        assert text == core_fixtures.FALLBACK and meta["selection_reason"] == "video_budget_inflight_limit"

    def test_legacy_jobs_are_not_given_new_logging_markers(self):
        self.service.prepare(self.context)
        key = self.context["followup_key"]
        self.store.mutate("jobs/" + key, lambda job: {k: v for k, v in job.items()
            if k != "provider_result_handling_version"})
        self.store.reserve("private-audio-protected", self.config, core_fixtures.NOW)
        self.store.reserve("old-provider-reservation", self.config, core_fixtures.NOW)
        before = self.store.read("jobs/" + key)
        self.worker().run_job(key)
        assert self.store.read("jobs/" + key) == before

    def test_terminal_future_failure_frees_slot_but_keeps_full_debit_and_no_resubmit(self):
        self.service.prepare(self.context)
        key = self.context["followup_key"]
        worker = self.worker()
        worker.run_job(key)
        self.provider.status = {"status": "failed", "failure_code": "fal_render_failed"}
        worker.run_job(key)
        ledger = self.store.read("budget/ledger")[0]
        assert ledger["reservations"][key]["cents"] == 10
        assert ledger["reservations"][key]["inflight"] is False
        assert self.store.read("jobs/" + key)[0]["status"] == "failed"
        worker.run_job(key)
        assert len(self.provider.submissions) == 1

    def test_daily_and_monthly_denials_retain_exact_existing_limits(self):
        for env, reason in (({"VIDEO_FOLLOWUP_DAILY_CENTS": "0"}, "daily_limit"),
                            ({"VIDEO_FOLLOWUP_MONTHLY_CENTS": "0"}, "monthly_limit")):
            config = core_fixtures.VideoFollowupTests.make_config(self, **env)
            decision = {}
            assert self.store.reserve("new", config, core_fixtures.NOW, decision=decision) is False
            assert decision["reason"] == reason
            assert self.store.read("budget/ledger")[0] is None
