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
