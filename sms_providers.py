import logging
import os
from typing import Optional

import requests

LOG = logging.getLogger(__name__)


class SMSGatewayForAndroid:
    """Sender backed by Tasker AutoRemote."""

    def __init__(self, api_key: str, base_url: str = "https://autoremotejoaomgcd.appspot.com"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def send(self, to: str, message: str, sms_type: str = "initial") -> Optional[str]:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        payload = {
            "key": self.api_key,
            "message": f"smsbot={to}|||{message}|||{sms_type}",
        }
        r = requests.post(f"{self.base_url}/sendmessage", data=payload, headers=headers, timeout=15)
        if r.status_code != 200:
            raise requests.HTTPError(
                f"Tasker AutoRemote send failed: status={r.status_code}",
                response=r,
            )
        response_preview = (r.text or "").strip().replace("\n", " ")[:200]
        LOG.debug(
            "Tasker AutoRemote accepted message: status=%s response=%s",
            r.status_code,
            response_preview or "<empty>",
        )
        # API does not return a message id
        return None


def get_sender(provider: Optional[str] = None):
    """Return an SMS sender (currently Tasker AutoRemote)."""
    # provider parameter is kept for backward compatibility but ignored
    key = os.getenv("SMS_GATEWAY_API_KEY") or os.getenv("SMS_API_KEY", "EhobscAL")
    return SMSGatewayForAndroid(api_key=key)
