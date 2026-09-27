import requests
import pytest
import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sms_takeover import create_takeover_router, relay_takeover


UPSTREAM = "https://script.google.com/macros/s/approved-deployment/exec"


def setup(result=None, failure=None, status=200):
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        if failure:
            raise failure

        class Response:
            status_code = status

            def json(self):
                return result

        return Response()

    app = FastAPI()
    app.include_router(create_takeover_router(UPSTREAM, post=post))
    return TestClient(app), calls


def success(**overrides):
    return {"ok": True, "phone": "9089024778", "human_override": "TRUE",
            "pending_send": {"ok": True, "cancelled": 1}, **overrides}


def test_preview_get_never_stops_bot_or_exposes_credentials():
    client, calls = setup()
    response = client.get("/sms-takeover")
    assert response.status_code == 200
    assert calls == []
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["referrer-policy"] == "no-referrer"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert "location.hash" in response.text
    assert "history.replaceState" in response.text
    assert "document.visibilityState!=='visible'" in response.text
    assert "script.google.com" not in response.text


@pytest.mark.parametrize("phone", ["+19089024778", " 19089024778", "908-902-4778"])
def test_takeover_relays_exact_phone_and_confirms_override_and_cancellation(phone):
    client, calls = setup(success())
    response = client.post("/sms-takeover", json={"phone": phone, "token": "owner-secret",
                                                 "action": "incoming_sms", "target": "https://evil.example"})
    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert calls[0][0] == UPSTREAM
    assert calls[0][1]["json"] == {"action": "takeover", "phone": "9089024778", "value": "TRUE", "token": "owner-secret"}
    assert "owner-secret" not in response.text


@pytest.mark.parametrize("result", [
    {}, {"ok": True}, success(phone="9542053205"), success(human_override="FALSE"),
    success(pending_send={"ok": False, "retryable": True}), success(pending_send=None),
    success(ok=False), [],
])
def test_no_false_success_for_partial_or_wrong_destination_results(result):
    client, _ = setup(result)
    response = client.post("/sms-takeover", json={"phone": "9089024778", "token": "test"})
    assert response.status_code == 502
    assert response.json() == {"ok": False, "retryable": True}


def test_pending_control_is_accepted_only_with_queue_cancellation():
    client, _ = setup(success(queued=True))
    response = client.post("/sms-takeover", json={"phone": "9089024778", "token": "test"})
    assert response.json() == {"ok": True, "phone": "9089024778", "queued": True}


def test_auth_failure_does_not_claim_success_or_retry_forever():
    client, _ = setup({"ok": False, "error": "Unauthorized"})
    response = client.post("/sms-takeover", json={"phone": "9089024778", "token": "wrong"})
    assert response.status_code == 403
    assert response.json() == {"ok": False, "retryable": False}


def test_network_failure_is_retryable_without_credential_leak():
    client, _ = setup(failure=requests.Timeout("internal secret"))
    response = client.post("/sms-takeover", json={"phone": "9089024778", "token": "test"})
    assert response.status_code == 502
    assert "secret" not in response.text


@pytest.mark.parametrize("body", [None, [], {}, {"phone": "1", "token": "test"}, {"phone": "9089024778"}])
def test_invalid_request_never_calls_upstream(body):
    client, calls = setup()
    response = client.post("/sms-takeover", json=body)
    assert response.status_code == 400
    assert calls == []


def test_cross_origin_post_is_rejected():
    client, calls = setup()
    response = client.post("/sms-takeover", json={"phone": "9089024778", "token": "test"},
                           headers={"origin": "https://evil.example"})
    assert response.status_code == 403
    assert calls == []


@pytest.mark.parametrize("url", [
    "https://evil.example/exec", "http://script.google.com/macros/s/id/exec",
    "https://script.google.com@evil.example/macros/s/id/exec",
    "https://script.google.com/macros/u/1/s/id/exec",
    "https://script.google.com/macros/s/id/exec?action=other",
])
def test_invalid_upstream_configuration_cannot_relay_credentials(url):
    def forbidden(*args, **kwargs):
        raise AssertionError("No request should be made")
    assert relay_takeover(url, "test", "9089024778", post=forbidden)[0] == 503
