import copy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import video_followup as core
import video_followup_diagnostics as diagnostics
from test_internal_sms_endpoint import _import_webhook_server, FakeSendResult
from fastapi.testclient import TestClient


KEY = "a" * 64
OTHER = "b" * 64
PIPELINE = "same-take-" + "c" * 32


class ReadOnlyStore:
    def __init__(self):
        self.values = {
            "budget/ledger": ({"reservations": {
                "private-audio-protected": {"cents": 10, "day": "2026-10-23", "inflight": True},
                OTHER: {"cents": 10, "day": "2026-11-13", "inflight": True}}}, 7),
            "jobs/" + KEY: ({"id": "d" * 32, "status": "frozen_text", "variant": "text",
                "reason": "Stored video failed spoken-name verification", "provider_id": PIPELINE,
                "secret": "never exposed", "speech_proof": {"raw": "not exported"}}, 9),
            "jobs/" + OTHER: ({"status": "uncertain", "provider_id": "https://bad/secret?token=hidden"}, 10),
            "same-take/" + PIPELINE: ({"stage": "failed", "failure_code": "pitch_outlier",
                "media_url": "https://private/?token=hidden", "token": "hidden"}, 11)}
        self.reads = []

    def read(self, key):
        self.reads.append(key)
        return copy.deepcopy(self.values.get(key, (None, 0)))


def configuration():
    return SimpleNamespace(bucket="private", daily_cents=300, monthly_cents=7500, reserve_cents=10)


def test_snapshot_only_reads_stored_json_and_retains_unknown_paid_slot():
    store = ReadOnlyStore()
    before = copy.deepcopy(store.values)
    result = diagnostics.snapshot([KEY], store=store, config=configuration(),
        now=datetime(2026, 11, 16, 20, tzinfo=timezone.utc))
    assert result["complete"] is True and result["read_only"] is True
    assert result["provider_calls"] == result["production_writes"] == 0
    assert result["budget"]["inflight_count"] == 2
    assert result["budget"]["accounted_month_cents"] == 10
    assert result["budget"]["accounted_today_cents"] == 0
    assert result["jobs"][0]["pipeline"]["state"]["failure_code"] == "pitch_outlier"
    assert "secret" not in str(result) and "hidden" not in str(result)
    assert store.values == before
    assert all(p == "budget/ledger" or p.startswith(("jobs/", "same-take/")) for p in store.reads)


@pytest.mark.parametrize("value", [None, "", "../private", "A" * 64, KEY + "," + KEY,
    ",".join(format(i, "064x") for i in range(21))])
def test_invalid_keys_rejected_before_storage(value):
    with pytest.raises(ValueError):
        diagnostics.validate_keys(value)


def test_diagnostic_storage_cannot_write_or_delete():
    session = Mock()
    objects = core.GCSObjects("private-video-bucket", session=session, read_only=True)
    with pytest.raises(ValueError, match="read-only"):
        objects.write("budget/ledger", b"{}", 7)
    with pytest.raises(ValueError, match="read-only"):
        objects.delete("jobs/" + KEY, 9)
    session.post.assert_not_called()
    session.delete.assert_not_called()


def test_missing_budget_does_not_report_zero_activity():
    store = ReadOnlyStore()
    store.values.pop("budget/ledger")
    with pytest.raises(ValueError, match="unavailable"):
        diagnostics.snapshot([KEY], store=store, config=configuration())


def test_racing_budget_is_explicitly_incomplete():
    store = ReadOnlyStore()
    original = store.read
    count = 0
    def read(key):
        nonlocal count
        value, version = original(key)
        if key == "budget/ledger":
            count += 1
            if count > 1:
                version += 1
        return value, version
    store.read = read
    result = diagnostics.snapshot([KEY], store=store, config=configuration())
    assert result["complete"] is False
    assert result["production_writes"] == 0


def test_endpoint_requires_auth_exact_keys_and_never_invokes_sender(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    backend = Mock(return_value={"read_only": True, "sent": False})
    monkeypatch.setattr(diagnostics, "snapshot", backend)
    client = TestClient(module.app)
    route = "/internal/video-followup-diagnostics"
    headers = {"Authorization": "Bearer secret-token"}
    assert client.get(route, params={"keys": KEY}).status_code == 403
    assert client.get(route, headers=headers).status_code == 400
    assert client.get(route, params={"keys": "../secrets"}, headers=headers).status_code == 400
    backend.assert_not_called()
    result = client.get(route, params={"keys": KEY}, headers=headers)
    assert result.status_code == 200 and result.json()["sent"] is False
    assert result.headers["cache-control"] == "no-store"
    backend.assert_called_once_with([KEY])
    assert sender.calls == []


def test_endpoint_access_failure_is_not_reported_as_no_jobs(monkeypatch, caplog):
    module, _, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    monkeypatch.setattr(diagnostics, "snapshot", Mock(side_effect=RuntimeError("https://private/?token=hidden")))
    result = TestClient(module.app).get("/internal/video-followup-diagnostics", params={"keys": KEY},
        headers={"Authorization": "Bearer secret-token"})
    assert result.status_code == 503
    assert result.json()["detail"] == "stored_video_diagnostics_unavailable"
    assert "hidden" not in caplog.text and "https://private" not in caplog.text
    assert sender.calls == []
