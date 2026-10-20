"""Standard reminder requests must not create a second automatic send path."""

import json
import socket
import sys
import threading
import types
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from test_internal_sms_endpoint import FakeSendResult, FakeWorksheet, _import_webhook_server, _source_sms_template
from test_video_followup import MEDIA, MemoryObjects, initial_receipt, video
from test_video_followup_scheduler import INITIAL_SENT_AT, PHONE, harness, scheduler_handoff


STANDARD = _source_sms_template("SMS_FU_TEMPLATE")
DEFERRED = "deferred_to_scheduled_video_followup"
URL = "/internal/send-followup-sms"
AUTH = {"authorization": "Bearer secret-token"}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("Network and providers are forbidden in standard-reminder tests")
    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(socket.socket, "connect_ex", denied)
    monkeypatch.setattr(socket, "create_connection", denied)


@pytest.fixture
def world(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch, sender_result=FakeSendResult(success=True),
    )
    scheduler = sys.modules["bot_min"]
    scheduler.SMS_FU_TEMPLATE = STANDARD
    scheduler.SMS_ENABLE = True
    scheduler.SMS_TEST_MODE = False
    monkeypatch.setattr(module, "DISABLE_APIFY_SCHEDULER", False)
    monkeypatch.setattr(module, "_scheduler_started", True)
    monkeypatch.setattr(module, "_scheduler_thread", types.SimpleNamespace(is_alive=lambda: True))
    monkeypatch.setattr(module, "_scheduler_stop", threading.Event())
    monkeypatch.setattr(module, "SMS_ENABLE", True, raising=False)
    monkeypatch.setattr(module, "SMS_TEST_MODE", False, raising=False)
    monkeypatch.setattr(module, "TASKER_TRANSPORT_HEALTH_URL", "https://script.example.test/exec")
    monkeypatch.setattr(module, "SMS_CHATBOT_ALLOWED_TOKEN", "unit-test-token")
    scheduler.TASKER_TRANSPORT_HEALTH_URL = module.TASKER_TRANSPORT_HEALTH_URL
    scheduler.SMS_CHATBOT_ALLOWED_TOKEN = module.SMS_CHATBOT_ALLOWED_TOKEN
    sheet.rows[12][0] = "Jim"
    sheet.rows[12][2] = PHONE
    sheet.rows[12][7] = "x"
    sheet.rows[12][22] = INITIAL_SENT_AT
    sheet.rows[12][27] = "listing-12"
    guards = {}
    for name in ("_ensure_scheduler_thread", "_process_pending_queue", "_process_initial_sms_queue"):
        guards[name] = Mock(side_effect=AssertionError("The route must not wake a scheduler or worker"))
        monkeypatch.setattr(module, name, guards[name])
    for name in ("run_hourly_scheduler", "_follow_up_pass", "send_sms"):
        guards[name] = Mock(side_effect=AssertionError("The route must not run an extra scheduler pass"))
        setattr(scheduler, name, guards[name])
    blocked_video = types.ModuleType("video_followup")
    for name in ("get_service", "select_for_followup", "prepare_for_followup"):
        guards[name] = Mock(side_effect=AssertionError("The route must not inspect or generate video jobs"))
        setattr(blocked_video, name, guards[name])
    monkeypatch.setitem(sys.modules, "video_followup", blocked_video)
    return types.SimpleNamespace(
        module=module, sheet=sheet, sender=sender, scheduler=scheduler, guards=guards,
        client=TestClient(module.app), payload={"row": 12, "phone": PHONE, "message": STANDARD},
    )


def assert_no_route_side_effects(world):
    assert world.sender.calls == []
    assert world.sheet.batch_updates == []
    assert world.sheet.appended_rows == []
    for guard in world.guards.values():
        guard.assert_not_called()


def assert_deferred(response):
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == DEFERRED
    assert body["queued"] is False
    assert body["sent"] is False
    assert body["outbound_persisted"] is False
    assert "request_id" not in body
    assert "message_id" not in body
    assert "pending_row" not in body


@pytest.mark.parametrize("message", [
    STANDARD,
    "  " + STANDARD + "\n",
    STANDARD.replace(" ", " \n "),
    STANDARD.replace("\u2014", "-"),
    STANDARD.replace("\u2014", "\u2013"),
    STANDARD.upper(),
])
def test_whole_standard_template_defers_without_claiming_a_send(world, message):
    response = world.client.post(URL, headers=AUTH, json={**world.payload, "message": message})
    assert_deferred(response)
    assert_no_route_side_effects(world)


@pytest.mark.parametrize("state", [
    "missing", "prepared", "ready", "failed", "cancelled", "frozen_text", "frozen_video",
    "queued", "claimed", "send_started", "receipt_pending", "uncertain", "sent", "superseded",
])
def test_standard_request_cannot_reopen_a_video_or_delivery_state(world, state):
    # State supplied by a caller must never authorize this alternate path.
    response = world.client.post(URL, headers=AUTH, json={
        **world.payload, "video_status": state, "outbox_status": state,
        "force_resend": True, "retry": True, "first_name": "WrongName",
    })
    assert_deferred(response)
    assert_no_route_side_effects(world)


@pytest.mark.parametrize("updates", [
    {7: "", 22: ""},
    {22: "2099-10-13T10:00:00Z"},
    {22: "2020-10-13T10:00:00Z"},
    {8: "x", 23: "2026-10-13T18:00:00Z"},
    {10: "R"}, {10: "Y"}, {10: "H"},
    {13: "handoff"}, {19: "TRUE"},
    {17: json.dumps([{"role": "agent", "text": "Yes, let's talk"}])},
])
def test_standard_request_leaves_episode_due_reply_and_duplicate_policy_to_scheduler(world, updates):
    for index, value in updates.items():
        world.sheet.rows[12][index] = value
    before = list(world.sheet.rows[12])
    assert_deferred(world.client.post(URL, headers=AUTH, json=world.payload))
    assert world.sheet.rows[12] == before
    assert_no_route_side_effects(world)


def test_standard_without_sheet_row_fails_closed_even_with_crm_identity(world):
    payload = {key: value for key, value in world.payload.items() if key != "row"}
    payload.update(prospect_id="e1f3e796-ff41-4c8c-8991-da6ca179d988", recommendation_key="approved")
    response = world.client.post(URL, headers=AUTH, json=payload)
    assert response.status_code == 400
    assert_no_route_side_effects(world)


def test_standard_request_requires_authorization_without_triggering_any_work(world):
    response = world.client.post(URL, json=world.payload)
    assert response.status_code == 403
    assert_no_route_side_effects(world)


@pytest.mark.parametrize("phone", ["2125550199", ""])
def test_standard_requires_the_current_sheet_phone(world, phone):
    world.sheet.rows[12][2] = phone
    response = world.client.post(URL, headers=AUTH, json=world.payload)
    assert response.status_code == 409
    assert_no_route_side_effects(world)


@pytest.mark.parametrize("condition", [
    "disabled", "dead", "no_thread", "not_started", "stopping", "no_stop",
    "sms_disabled", "test_mode", "server_no_url", "server_no_token", "bot_no_url", "bot_no_token",
])
def test_unavailable_scheduler_is_explicitly_held_not_queued(world, monkeypatch, condition):
    if condition == "disabled":
        monkeypatch.setattr(world.module, "DISABLE_APIFY_SCHEDULER", True)
    elif condition == "dead":
        monkeypatch.setattr(world.module, "_scheduler_thread", types.SimpleNamespace(is_alive=lambda: False))
    elif condition == "no_thread":
        monkeypatch.setattr(world.module, "_scheduler_thread", None)
    elif condition == "not_started":
        monkeypatch.setattr(world.module, "_scheduler_started", False)
    elif condition == "stopping":
        world.module._scheduler_stop.set()
    elif condition == "no_stop":
        monkeypatch.setattr(world.module, "_scheduler_stop", None)
    elif condition == "sms_disabled":
        monkeypatch.setattr(world.scheduler, "SMS_ENABLE", False)
        monkeypatch.setattr(world.module, "SMS_ENABLE", False)
    elif condition == "test_mode":
        monkeypatch.setattr(world.scheduler, "SMS_TEST_MODE", True)
        monkeypatch.setattr(world.module, "SMS_TEST_MODE", True)
    else:
        name = "TASKER_TRANSPORT_HEALTH_URL" if condition.endswith("no_url") else "SMS_CHATBOT_ALLOWED_TOKEN"
        monkeypatch.setattr(world.module if condition.startswith("server_") else world.scheduler, name, "")
    response = world.client.post(URL, headers=AUTH, json=world.payload)
    assert response.status_code == 503
    assert response.json().get("status") not in {"queued", "sent", DEFERRED}
    assert_no_route_side_effects(world)


@pytest.mark.parametrize("message", [
    "Custom follow-up",
    STANDARD + " Please call me tomorrow.",
    "Quoted text: " + STANDARD,
    "Hey Jim, can we talk tomorrow?",
])
def test_genuine_custom_message_keeps_existing_outbox_behavior_without_video(world, message):
    response = world.client.post(URL, headers=AUTH, json={**world.payload, "message": message})
    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert len(world.sender.calls) == 1
    assert world.sender.calls[0]["message"] == message
    assert world.sender.calls[0]["row_idx"] == 12
    for guard in world.guards.values():
        guard.assert_not_called()


def automatic_path(monkeypatch, state):
    for name, value in {
        "VIDEO_FOLLOWUP_ENABLED": "true", "VIDEO_FOLLOWUP_GENERATION_ENABLED": "false",
        "VIDEO_FOLLOWUP_TEST_PHONES": PHONE, "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T00:00:00Z",
        "VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "https://go.crispshortsales.com",
    }.items():
        monkeypatch.setenv(name, value)
    context = video.snapshot(row=12, stable_id="listing-12", phone=PHONE,
                             first_name="Jim", initial_sent_at=INITIAL_SENT_AT)
    context.update(video.initial_receipt_binding(context, initial_receipt(context)))
    store = video.Store(MemoryObjects())
    if state != "missing":
        store.prepare(context)
        if state == "ready":
            store.mutate("jobs/" + context["followup_key"], lambda job: {
                **job, "status": "processing", "provider_id": "offline-fixture",
            })
            assert store.finish(context["followup_key"], "offline-fixture", MEDIA)
        else:
            store.mutate("jobs/" + context["followup_key"], lambda job: {**job, "status": state})
    monkeypatch.setattr(video, "_SERVICE", video.Service(video.Config(), store))
    monkeypatch.setitem(sys.modules, "video_followup", video)
    auto = harness(real_outbox=True)
    return auto, store, context


@pytest.mark.parametrize("state,variant", [("ready", "video"), ("missing", "text"), ("failed", "text")])
def test_deferral_then_existing_automatic_pipeline_selects_ready_video_or_same_key_fallback(world, monkeypatch, state, variant):
    assert_deferred(world.client.post(URL, headers=AUTH, json=world.payload))
    assert_no_route_side_effects(world)
    auto, store, context = automatic_path(monkeypatch, state)
    scheduler_handoff(auto.namespace, enabled=True)
    assert auto.post.call_count == 1
    body = auto.post.call_args.kwargs["json"]
    assert body["video_variant"] == variant
    assert body["followup_key"] == context["followup_key"]
    assert body["message_id"] == context["message_id"]
    assert body["first_name"] == "Jim"
    assert body["initial_sent_at"] == context["initial_sent_at"]
    if variant == "video":
        assert body["message"].startswith("Hey, just following up on my message from earlier.  Heres a quick video")
        assert "Jim" not in body["message"]
        assert "https://go.crispshortsales.com/v/" in body["message"]
        assert body["video_speech_proof"]["expected_name"] == "Jim"
        assert store.get_public_video(body["video_job_id"]) is not None
    else:
        assert body["message"] == STANDARD
        assert not body.get("video_speech_proof")
    auto.direct_sender.assert_not_called()
    auto.sheet_write.assert_not_called()
    assert world.sender.calls == []


def test_concurrent_standard_requests_cannot_add_an_attempt_beside_existing_scheduler(world, monkeypatch):
    auto, _store, _context = automatic_path(monkeypatch, "ready")
    barrier = threading.Barrier(4)

    def standard_request():
        barrier.wait(timeout=5)
        return world.module._send_followup_sms_from_payload(dict(world.payload))

    def scheduled_pass():
        barrier.wait(timeout=5)
        scheduler_handoff(auto.namespace, enabled=True)

    with ThreadPoolExecutor(max_workers=4) as pool:
        requests = [pool.submit(standard_request) for _ in range(3)]
        scheduled = pool.submit(scheduled_pass)
        scheduled.result(timeout=10)
        for request in requests:
            result = request.result(timeout=10)
            assert result["status"] == DEFERRED
            assert result["queued"] is False
    assert auto.post.call_count == 1
    assert world.sender.calls == []
    assert world.sheet.batch_updates == []


@pytest.mark.parametrize("state", [
    "missing", "prepared", "submitting", "submitted", "processing", "ready", "failed", "cancelled",
    "frozen_text", "frozen_video", "uncertain", "budget_blocked",
])
def test_standard_deferral_does_not_read_or_mutate_existing_real_store_jobs(world, monkeypatch, state):
    auto, store, _context = automatic_path(monkeypatch, state)
    before = dict(store.objects._objects)
    for name in ("get_service", "select_for_followup", "prepare_for_followup"):
        monkeypatch.setattr(video, name, world.guards[name])
    assert_deferred(world.client.post(URL, headers=AUTH, json=world.payload))
    assert_no_route_side_effects(world)
    assert store.objects._objects == before
    auto.post.assert_not_called()


@pytest.mark.parametrize("damage", ["wrong_name", "changed_media", "missing_public_binding"])
def test_existing_scheduler_preserves_name_and_publication_checks_after_deferral(world, monkeypatch, damage):
    assert_deferred(world.client.post(URL, headers=AUTH, json={**world.payload, "first_name": "WrongName"}))
    auto, store, context = automatic_path(monkeypatch, "ready")
    job, _ = store.read("jobs/" + context["followup_key"])
    if damage == "wrong_name":
        store.mutate("jobs/" + context["followup_key"], lambda old: {**old, "first_name": "Sue"})
    elif damage == "changed_media":
        path = f"assets/{job['id']}/video"
        _, version = store.objects.read(path)
        store.objects.write(path, b"changed bytes", version, "video/mp4")
    else:
        path = "public/" + job["id"]
        _, version = store.objects.read(path)
        store.objects.delete(path, version)
    scheduler_handoff(auto.namespace, enabled=True)
    assert auto.post.call_count == 1
    body = auto.post.call_args.kwargs["json"]
    assert body["video_variant"] == "text"
    assert body["first_name"] == "Jim"
    assert body["message"] == STANDARD
    assert body["followup_key"] == context["followup_key"]
    assert "video_speech_proof" not in body
    assert world.sender.calls == []


def test_existing_cancelled_episode_still_cannot_send_after_standard_route_deferral(world, monkeypatch):
    assert_deferred(world.client.post(URL, headers=AUTH, json=world.payload))
    auto, store, context = automatic_path(monkeypatch, "cancelled")
    before, _ = store.read("jobs/" + context["followup_key"])
    scheduler_handoff(auto.namespace, enabled=True)
    after, _ = store.read("jobs/" + context["followup_key"])
    assert after == before
    auto.post.assert_not_called()
    assert world.sender.calls == []


@pytest.mark.parametrize("status", [
    "queued", "pending", "alerted", "claimed", "send_started", "receipt_pending", "uncertain",
    "sent", "failed", "superseded", "cancelled",
])
def test_existing_locked_outbox_episode_is_unchanged_by_repeated_standard_requests(world, status):
    outbox = FakeWorksheet({
        1: ["created_at", "status", "request_id", "message_id", "phone", "reply_text", "inbound_text"],
        2: ["2026-10-13T16:00:00Z", status, "existing-request", "existing-message", PHONE,
            STANDARD, '__scheduled_followup__:{"crm_row":12,"stable_id":"listing-12"}'],
    })
    world.module.gclient.open_by_key("sheet-id").worksheets["sms_pending_sends"] = outbox
    before = json.loads(json.dumps(outbox.rows))
    for _ in range(2):
        assert_deferred(world.client.post(URL, headers=AUTH, json=world.payload))
    assert json.loads(json.dumps(outbox.rows)) == before
    assert outbox.batch_updates == []
    assert outbox.appended_rows == []
    assert_no_route_side_effects(world)
