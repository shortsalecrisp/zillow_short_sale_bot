from types import SimpleNamespace

from sms_providers import SMSGatewayForAndroid


def test_normalize_endpoint_root_strips_sendmessage_path():
    sender = SMSGatewayForAndroid(
        api_key="EhobscAL",
        endpoint="https://autoremotejoaomgcd.appspot.com/sendmessage/EhobscAL",
    )
    assert sender.endpoint_root == "https://autoremotejoaomgcd.appspot.com"


def test_send_with_diagnostics_uses_personal_url_format(monkeypatch):
    captured = {}

    def fake_get(url, timeout):
        captured["url"] = url
        assert timeout == 15
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

    assert result.success is True
    assert (
        captured["url"]
        == "https://autoremotejoaomgcd.appspot.com/EhobscAL?message=smsbot%3D%2B15551234567%7C%7C%7CHello+there%7C%7C%7Cinitial"
    )
