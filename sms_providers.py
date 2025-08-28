import os
import base64
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


def get_sender(provider: Optional[str] = None):
    """Return an SMS sender (currently only SMS Gateway for Android)."""
    # provider parameter is kept for backward compatibility but ignored
    key = os.getenv("SMS_GATEWAY_API_KEY") or os.getenv("SMS_API_KEY", "")
    return SMSGatewayForAndroid(api_key=key)
