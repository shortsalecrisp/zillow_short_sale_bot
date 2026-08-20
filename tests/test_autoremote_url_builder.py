from types import SimpleNamespace

from sms_providers import SMSGatewayForAndroid, get_sender

DUMMY_AUTOREMOTE_KEY = "test-autoremote-key"


def test_normalize_endpoint_root_strips_sendmessage_path():
    sender = SMSGatewayForAndroid(
        api_key=DUMMY_AUTOREMOTE_KEY,
        endpoint=f"https://autoremotejoaomgcd.appspot.com/sendmessage/{DUMMY_AUTOREMOTE_KEY}",
    )
    assert sender.endpoint_root == "https://autoremotejoaomgcd.appspot.com"


def test_get_sender_has_no_committed_key_fallback(monkeypatch):
    monkeypatch.delenv("AUTOREMOTE_KEY", raising=False)
    monkeypatch.delenv("SMS_GATEWAY_API_KEY", raising=False)
    monkeypatch.delenv("SMS_API_KEY", raising=False)

    sender = get_sender("android_gateway")

    assert sender.api_key == ""


def test_get_sender_prefers_autoremote_key(monkeypatch):
    monkeypatch.setenv("AUTOREMOTE_KEY", "private-autoremote-key")
    monkeypatch.setenv("SMS_GATEWAY_API_KEY", "gateway-key")
    monkeypatch.setenv("SMS_API_KEY", "legacy-key")

    sender = get_sender("android_gateway")

    assert sender.api_key == "private-autoremote-key"


def test_send_with_diagnostics_uses_fcm_sendmessage_endpoint(monkeypatch):
    captured = {}

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        assert timeout == 15
        return SimpleNamespace(status_code=200, text="OK")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(
        api_key=DUMMY_AUTOREMOTE_KEY,
        endpoint="https://autoremotejoaomgcd.appspot.com/sendmessage",
    )
    result = sender.send_with_diagnostics(
        to="+15551234567",
        message="Hello there",
        sms_type="initial",
    )

    assert result.success is True
    assert captured["url"] == "https://autoremotejoaomgcd.appspot.com/sendmessage"
    assert captured["params"] == {
        "key": DUMMY_AUTOREMOTE_KEY,
        "message": "smsbot=:=+15551234567|||Hello there|||initial",
        "target": "+15551234567",
    }


def test_send_command_uses_autoremote_arcomm_separator(monkeypatch):
    captured = {}

    def fake_get(url, params, timeout):
        captured["message"] = params["message"]
        return SimpleNamespace(status_code=200, text="OK")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(api_key=DUMMY_AUTOREMOTE_KEY)
    sender.send_with_diagnostics(
        to="15551234567",
        message="Follow-up text",
        sms_type="followup",
    )

    assert captured["message"] == (
        "smsbot=:=15551234567|||Follow-up text|||followup"
    )


def test_send_commands_are_spaced_for_tasker(monkeypatch):
    starts = iter([100.0, 103.0])
    sleeps = []

    monkeypatch.setattr("sms_providers.time.monotonic", lambda: next(starts))
    monkeypatch.setattr("sms_providers.time.sleep", sleeps.append)
    monkeypatch.setattr(
        "sms_providers.requests.get",
        lambda url, params, timeout: SimpleNamespace(status_code=200, text="OK"),
    )

    sender = SMSGatewayForAndroid(api_key=DUMMY_AUTOREMOTE_KEY)
    sender.send_with_diagnostics("15551234567", "First", sms_type="initial")
    sender.send_with_diagnostics("15557654321", "Second", sms_type="initial")

    assert sleeps == [27.0]


def test_send_with_diagnostics_masks_encoded_key_in_preview(monkeypatch):
    secret = "abc:def/ghi+123"

    def fake_get(url, params, timeout):
        return SimpleNamespace(status_code=200, text="OK")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(api_key=secret)
    result = sender.send_with_diagnostics(
        to="+15551234567",
        message="Hello there",
        sms_type="initial",
    )

    assert result.success is True
    assert secret not in result.payload_preview
    assert "abc%3Adef%2Fghi%2B123" not in result.payload_preview
    assert "abc...23" in result.payload_preview


def test_send_with_diagnostics_accepts_http_200_without_error_body(monkeypatch):
    def fake_get(url, params, timeout):
        return SimpleNamespace(status_code=200, text="queued")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(
        api_key=DUMMY_AUTOREMOTE_KEY,
        endpoint="https://autoremotejoaomgcd.appspot.com/sendmessage",
    )
    result = sender.send_with_diagnostics(
        to="+15551234567",
        message="Hello there",
        sms_type="initial",
    )

    assert result.success is True
    assert result.status_code == 200


def test_send_with_diagnostics_http_200_ok_is_success(monkeypatch):
    def fake_get(url, params, timeout):
        return SimpleNamespace(status_code=200, text=" OK ")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(api_key=DUMMY_AUTOREMOTE_KEY)
    result = sender.send_with_diagnostics(
        to="+15551234567",
        message="Hello there",
        sms_type="initial",
    )

    assert result.success is True
    assert result.status_code == 200


def test_send_with_diagnostics_http_200_token_error_is_failure(monkeypatch):
    def fake_get(url, params, timeout):
        return SimpleNamespace(status_code=200, text="Not a valid FCM registration token")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(api_key=DUMMY_AUTOREMOTE_KEY)
    result = sender.send_with_diagnostics(
        to="+15551234567",
        message="Hello there",
        sms_type="initial",
    )

    assert result.success is False
    assert result.exception_type == "HTTPError"


def test_send_with_diagnostics_http_200_empty_body_is_accepted(monkeypatch):
    def fake_get(url, params, timeout):
        return SimpleNamespace(status_code=200, text="   ")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(api_key=DUMMY_AUTOREMOTE_KEY)
    result = sender.send_with_diagnostics(
        to="+15551234567",
        message="Hello there",
        sms_type="initial",
    )

    assert result.success is True
    assert result.status_code == 200


def test_send_with_diagnostics_non_200_is_failure(monkeypatch):
    def fake_get(url, params, timeout):
        return SimpleNamespace(status_code=500, text="OK")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(api_key=DUMMY_AUTOREMOTE_KEY)
    result = sender.send_with_diagnostics(
        to="+15551234567",
        message="Hello there",
        sms_type="initial",
    )

    assert result.success is False
    assert result.exception_type == "HTTPError"


def test_transport_health_gate_allows_send_when_tasker_is_recent(monkeypatch):
    calls = []

    def fake_post(url, json, timeout):
        calls.append(("health", url, json, timeout))
        return SimpleNamespace(
            status_code=200,
            json=lambda: {
                "ok": True,
                "healthy": True,
                "reason": "Tasker transport is active",
                "age_seconds": 14,
                "last_activity_at": "2026-08-20T12:00:00.000Z",
                "transport_version": "16",
            },
        )

    def fake_get(url, params, timeout):
        calls.append(("send", url, params, timeout))
        return SimpleNamespace(status_code=200, text="OK")

    monkeypatch.setattr("sms_providers.requests.post", fake_post)
    monkeypatch.setattr("sms_providers.requests.get", fake_get)
    sender = SMSGatewayForAndroid(
        api_key=DUMMY_AUTOREMOTE_KEY,
        tasker_health_endpoint="https://example.test/exec",
        tasker_health_token="transport-token",
        require_tasker_health=True,
    )

    result = sender.send_with_diagnostics("15551234567", "Hello", sms_type="initial")

    assert result.success is True
    assert [call[0] for call in calls] == ["health", "send"]
    assert calls[0][2] == {"token": "transport-token", "action": "transport_health"}


def test_transport_health_gate_holds_sms_when_tasker_is_stale(monkeypatch):
    send_calls = []
    monkeypatch.setattr(
        "sms_providers.requests.post",
        lambda url, json, timeout: SimpleNamespace(
            status_code=200,
            json=lambda: {
                "ok": True,
                "healthy": False,
                "reason": "Tasker transport activity is stale",
                "age_seconds": 720,
                "last_activity_at": "2026-08-20T11:00:00.000Z",
                "transport_version": "16",
            },
        ),
    )
    monkeypatch.setattr(
        "sms_providers.requests.get",
        lambda *args, **kwargs: send_calls.append((args, kwargs)),
    )
    sender = SMSGatewayForAndroid(
        api_key=DUMMY_AUTOREMOTE_KEY,
        tasker_health_endpoint="https://example.test/exec",
        tasker_health_token="transport-token",
        require_tasker_health=True,
    )

    result = sender.send_with_diagnostics("15551234567", "Hello", sms_type="initial")

    assert result.success is False
    assert result.exception_type == "TaskerTransportUnavailable"
    assert send_calls == []


def test_transport_health_gate_fails_closed_when_health_endpoint_errors(monkeypatch):
    send_calls = []

    def fail_health(*args, **kwargs):
        raise TimeoutError("health timeout")

    monkeypatch.setattr("sms_providers.requests.post", fail_health)
    monkeypatch.setattr(
        "sms_providers.requests.get",
        lambda *args, **kwargs: send_calls.append((args, kwargs)),
    )
    sender = SMSGatewayForAndroid(
        api_key=DUMMY_AUTOREMOTE_KEY,
        tasker_health_endpoint="https://example.test/exec",
        tasker_health_token="transport-token",
        require_tasker_health=True,
    )

    result = sender.send_with_diagnostics("15551234567", "Hello", sms_type="initial")

    assert result.success is False
    assert result.exception_type == "TaskerTransportUnavailable"
    assert "TimeoutError" in result.exception_message
    assert send_calls == []
