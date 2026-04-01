import logging
import os
from dataclasses import dataclass
from typing import Optional

import requests

LOG = logging.getLogger(__name__)


@dataclass
class AutoRemoteSendResult:
    success: bool
    status_code: Optional[int]
    response_text: str
    exception_type: str
    exception_message: str
    endpoint: str
    key_present: bool
    key_masked: str
    payload_preview: str


class SMSGatewayForAndroid:
    """Sender backed by Tasker AutoRemote."""

    def __init__(self, api_key: str, base_url: str = "https://autoremotejoaomgcd.appspot.com"):
        self.api_key = api_key or ""
        self.base_url = base_url.rstrip("/")

    @staticmethod
    def _mask_secret(value: str) -> str:
        if not value:
            return "<missing>"
        if len(value) <= 5:
            return "*" * len(value)
        return f"{value[:3]}...{value[-2:]}"

    def _build_result(
        self,
        *,
        success: bool,
        status_code: Optional[int],
        response_text: str,
        exception_type: str = "",
        exception_message: str = "",
        payload_preview: str,
    ) -> AutoRemoteSendResult:
        return AutoRemoteSendResult(
            success=success,
            status_code=status_code,
            response_text=(response_text or "").strip()[:600],
            exception_type=exception_type,
            exception_message=exception_message,
            endpoint=f"{self.base_url}/sendmessage",
            key_present=bool(self.api_key),
            key_masked=self._mask_secret(self.api_key),
            payload_preview=payload_preview,
        )

    def send_with_diagnostics(
        self,
        to: str,
        message: str,
        sms_type: str = "initial",
        *,
        row_idx: Optional[int] = None,
        attempt: Optional[int] = None,
    ) -> AutoRemoteSendResult:
        endpoint = f"{self.base_url}/sendmessage"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        message_payload = f"smsbot={to}|||{message}|||{sms_type}"
        payload = {
            "key": self.api_key,
            "message": message_payload,
        }
        payload_preview = f"key={self._mask_secret(self.api_key)}&message={message_payload[:140]}"

        LOG.info(
            "AUTOREMOTE_CONFIG_CHECK row=%s phone=%s type=%s attempt=%s key_present=%s key_masked=%s endpoint=%s",
            row_idx,
            to,
            sms_type,
            attempt,
            bool(self.api_key),
            self._mask_secret(self.api_key),
            endpoint,
        )
        if not self.api_key:
            LOG.error(
                "AUTOREMOTE_SEND_FAILED row=%s phone=%s type=%s attempt=%s reason=missing_key request_not_sent=true",
                row_idx,
                to,
                sms_type,
                attempt,
            )
            return self._build_result(
                success=False,
                status_code=None,
                response_text="",
                exception_type="MissingConfig",
                exception_message="AutoRemote key missing",
                payload_preview=payload_preview,
            )

        LOG.info(
            "AUTOREMOTE_REQUEST_PREPARED row=%s phone=%s type=%s attempt=%s endpoint=%s payload_preview=%s",
            row_idx,
            to,
            sms_type,
            attempt,
            endpoint,
            payload_preview,
        )

        try:
            LOG.info(
                "AUTOREMOTE_REQUEST_SENT row=%s phone=%s type=%s attempt=%s",
                row_idx,
                to,
                sms_type,
                attempt,
            )
            response = requests.post(endpoint, data=payload, headers=headers, timeout=15)
            response_text = (response.text or "").strip().replace("\n", " ")
            LOG.info(
                "AUTOREMOTE_RESPONSE_RECEIVED row=%s phone=%s type=%s attempt=%s http_status=%s response_body=%s",
                row_idx,
                to,
                sms_type,
                attempt,
                response.status_code,
                response_text[:600] or "<empty>",
            )
            if response.status_code == 200:
                LOG.info(
                    "AUTOREMOTE_SEND_CONFIRMED row=%s phone=%s type=%s attempt=%s http_status=%s",
                    row_idx,
                    to,
                    sms_type,
                    attempt,
                    response.status_code,
                )
                return self._build_result(
                    success=True,
                    status_code=response.status_code,
                    response_text=response_text,
                    payload_preview=payload_preview,
                )

            LOG.error(
                "AUTOREMOTE_SEND_FAILED row=%s phone=%s type=%s attempt=%s http_status=%s response_body=%s",
                row_idx,
                to,
                sms_type,
                attempt,
                response.status_code,
                response_text[:600] or "<empty>",
            )
            return self._build_result(
                success=False,
                status_code=response.status_code,
                response_text=response_text,
                exception_type="HTTPError",
                exception_message=f"Non-200 from AutoRemote: {response.status_code}",
                payload_preview=payload_preview,
            )
        except Exception as exc:
            LOG.exception(
                "AUTOREMOTE_SEND_FAILED row=%s phone=%s type=%s attempt=%s error_type=%s error_message=%s request_failed_before_response=true",
                row_idx,
                to,
                sms_type,
                attempt,
                type(exc).__name__,
                exc,
            )
            return self._build_result(
                success=False,
                status_code=None,
                response_text="",
                exception_type=type(exc).__name__,
                exception_message=str(exc),
                payload_preview=payload_preview,
            )

    def send(self, to: str, message: str, sms_type: str = "initial") -> Optional[str]:
        result = self.send_with_diagnostics(to, message, sms_type=sms_type)
        if not result.success:
            raise requests.HTTPError(
                "Tasker AutoRemote send failed",
                response=None,
            )
        # API does not return a message id
        return None


def get_sender(provider: Optional[str] = None):
    """Return an SMS sender (currently Tasker AutoRemote)."""
    # provider parameter is kept for backward compatibility but ignored
    key = os.getenv("SMS_GATEWAY_API_KEY") or os.getenv("SMS_API_KEY", "EhobscAL")
    return SMSGatewayForAndroid(api_key=key)
