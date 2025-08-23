import os
import base64
import re
from typing import Optional

import requests


class SMSGatewayForAndroid:
    """Minimal sender for SMS Gateway for Android."""

    def __init__(self, api_key: str, base_url: str = "https://api.smstext.app"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def send(self, to: str, message: str) -> Optional[str]:
        auth = "Basic " + base64.b64encode(f"apikey:{self.api_key}".encode()).decode()
        headers = {"Authorization": auth}
        payload = [{"mobile": to, "text": message}]
        r = requests.post(f"{self.base_url}/push", json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        # API does not return a message id
        return None


class SMSMobileSender:
    """Sender for SMSMobile API."""

    def __init__(self, api_key: str, from_: str, url: str = "https://api.smsmobileapi.com/sendsms/"):
        self.api_key = api_key
        self.from_ = from_
        self.url = url

    def send(self, to: str, message: str) -> Optional[str]:
        digits = re.sub(r"\D", "", to)
        payload = {
            "apikey": self.api_key,
            "recipients": digits,
            "message": message,
            "sendsms": "1",
        }
        if self.from_:
            payload["from"] = self.from_
        r = requests.post(self.url, timeout=15, data=payload)
        r.raise_for_status()
        data = {}
        try:
            data = r.json().get("result", {})
        except Exception:
            pass
        if str(data.get("error")) != "0":
            raise RuntimeError(f"SMSMobile error: {data}")
        return data.get("message_id")


def get_sender(provider: Optional[str] = None):
    """Return an SMS sender for *provider* (default android_gateway)."""
    prov = (provider or os.getenv("SMS_PROVIDER") or "android_gateway").lower()
    if prov == "smsmobile":
        key = os.getenv("SMSMOBILE_API_KEY", "")
        frm = os.getenv("SMSMOBILE_FROM", "")
        return SMSMobileSender(api_key=key, from_=frm)
    key = os.getenv("SMS_GATEWAY_API_KEY", "")
    return SMSGatewayForAndroid(api_key=key)
