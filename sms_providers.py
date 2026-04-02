import logging
import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import quote_plus, urlsplit

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

    ERROR_SIGNALS = (
        "not a valid fcm registration token",
        "invalid",
        "error",
        "failed",
    )
    DEFAULT_ENDPOINT = "https://autoremotejoaomgcd.appspot.com"

    def __init__(
        self,
        api_key: str,
        endpoint: str = DEFAULT_ENDPOINT,
    ):
        self.api_key = api_key or ""
        self.endpoint = (endpoint or "").strip() or self.DEFAULT_ENDPOINT
        self.endpoint_root = self._normalize_endpoint_root(self.endpoint)

    @classmethod
    def _normalize_endpoint_root(cls, endpoint: str) -> str:
        candidate = (endpoint or "").strip() or cls.DEFAULT_ENDPOINT
        parsed = urlsplit(candidate)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        if "://" not in candidate:
            parsed = urlsplit(f"https://{candidate}")
            if parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        return cls.DEFAULT_ENDPOINT

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
            endpoint=self.endpoint,
            key_present=bool(self.api_key),
            key_masked=self._mask_secret(self.api_key),
            payload_preview=payload_preview,
        )

    def _body_has_error_signal(self, response_text: str) -> bool:
        body = (response_text or "").lower()
        return any(signal in body for signal in self.ERROR_SIGNALS)

    def send_with_diagnostics(
        self,
        to: str,
        message: str,
        sms_type: str = "initial",
        *,
        row_idx: Optional[int] = None,
        attempt: Optional[int] = None,
    ) -> AutoRemoteSendResult:
        message_payload = f"smsbot={to}|||{message}|||{sms_type}"
        encoded_message = quote_plus(message_payload)
        request_url = f"{self.endpoint_root}/{self.api_key}?message={encoded_message}"
        payload_preview = request_url.replace(f"/{self.api_key}?", f"/{self._mask_secret(self.api_key)}?")[:220]

        LOG.info(
            "AUTOREMOTE_CONFIG_CHECK row=%s phone=%s type=%s attempt=%s key_present=%s key_masked=%s key_length=%s endpoint=%s",
            row_idx,
            to,
            sms_type,
            attempt,
            bool(self.api_key),
            self._mask_secret(self.api_key),
            len(self.api_key),
            self.endpoint_root,
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
            "AUTOREMOTE_REQUEST_PREPARED row=%s phone=%s type=%s attempt=%s final_url=%s payload_preview=%s",
            row_idx,
            to,
            sms_type,
            attempt,
            request_url,
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
            response = requests.get(request_url, timeout=15)
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
            if response.status_code == 200 and not self._body_has_error_signal(response_text):
                LOG.info(
                    "AUTOREMOTE_SEND_CONFIRMED row=%s phone=%s type=%s attempt=%s http_status=%s response_body=%s",
                    row_idx,
                    to,
                    sms_type,
                    attempt,
                    response.status_code,
                    response_text[:600] or "<empty>",
                )
                return self._build_result(
                    success=True,
                    status_code=response.status_code,
                    response_text=response_text,
                    payload_preview=payload_preview,
                )

            if response.status_code == 200 and self._body_has_error_signal(response_text):
                LOG.error(
                    "AUTOREMOTE_RESPONSE_BODY_ERROR row=%s phone=%s type=%s attempt=%s http_status=%s response_body=%s",
                    row_idx,
                    to,
                    sms_type,
                    attempt,
                    response.status_code,
                    response_text[:600] or "<empty>",
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
                exception_message=(
                    "AutoRemote HTTP 200 body indicates failure"
                    if response.status_code == 200
                    else f"Non-200 from AutoRemote: {response.status_code}"
                ),
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
    key = os.getenv("AUTOREMOTE_KEY") or os.getenv("SMS_GATEWAY_API_KEY") or os.getenv("SMS_API_KEY", "")
    endpoint = os.getenv("AUTOREMOTE_ENDPOINT", "https://autoremotejoaomgcd.appspot.com")
    return SMSGatewayForAndroid(api_key=key, endpoint=endpoint)
