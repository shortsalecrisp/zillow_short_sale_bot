from types import SimpleNamespace

from sms_providers import SMSGatewayForAndroid


def test_normalize_endpoint_root_strips_sendmessage_path():
    sender = SMSGatewayForAndroid(
        api_key="EhobscAL",
        endpoint="https://autoremotejoaomgcd.appspot.com/sendmessage/EhobscAL",
    )
    assert sender.endpoint_root == "https://autoremotejoaomgcd.appspot.com"


def test_send_with_diagnostics_uses_fcm_sendmessage_endpoint(monkeypatch):
    captured = {}

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        assert timeout == 15
        return SimpleNamespace(status_code=200, text="Success!")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(
        api_key="EhobscAL",
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
        "key": "EhobscAL",
        "message": "smsbot=+15551234567|||Hello there|||initial",
        "target": "+15551234567",
    }


def test_send_with_diagnostics_requires_success_confirmation(monkeypatch):
    def fake_get(url, params, timeout):
        return SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr("sms_providers.requests.get", fake_get)

    sender = SMSGatewayForAndroid(
        api_key="EhobscAL",
        endpoint="https://autoremotejoaomgcd.appspot.com/sendmessage",
    )
    result = sender.send_with_diagnostics(
        to="+15551234567",
        message="Hello there",
        sms_type="initial",
    )

    assert result.success is False
    assert result.exception_type == "HTTPError"
