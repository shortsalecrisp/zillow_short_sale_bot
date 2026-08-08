from __future__ import annotations

import hashlib
import json
import re
from typing import Any
from urllib.parse import urlparse


EXTERNAL_LINK_REPLACEMENT = "[external-link-redacted]"

_PROTOCOL_URL_RE = re.compile(r"\bhttps?://[^\s<>\"']+", re.IGNORECASE)
_ESCAPED_PROTOCOL_URL_RE = re.compile(r"\bhttps?:\\?/\\?/[^\s<>\"']+", re.IGNORECASE)
_WWW_RE = re.compile(r"\bwww\.", re.IGNORECASE)

_EXTERNAL_URL_FIELD_KEYS = {
    "url",
    "href",
    "detailurl",
    "detail_url",
    "detailURL",
    "propertyurl",
    "property_url",
    "propertyURL",
    "listingurl",
    "listing_url",
    "listingURL",
    "zillowurl",
    "zillow_url",
    "zillowURL",
    "hdpurl",
    "hdp_url",
    "hdpURL",
    "sourceurl",
    "source_url",
    "bestphonesourceurl",
    "best_phone_source_url",
    "bestemailsourceurl",
    "best_email_source_url",
    "phoneemailsourceurl",
    "phone_email_source_url",
}
_NORMALIZED_EXTERNAL_URL_FIELD_KEYS = {
    re.sub(r"[^a-z0-9]+", "", key.lower()) for key in _EXTERNAL_URL_FIELD_KEYS
}


def _field_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def is_external_url_field(field_name: Any) -> bool:
    key = _field_key(field_name)
    return key in _NORMALIZED_EXTERNAL_URL_FIELD_KEYS


def sanitize_external_links_for_sheet(value: Any) -> str:
    """Return a sheet-safe string with clickable external URLs removed."""
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple)):
        value = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    text = str(value)
    if not text:
        return ""
    text = _ESCAPED_PROTOCOL_URL_RE.sub(EXTERNAL_LINK_REPLACEMENT, text)
    text = _PROTOCOL_URL_RE.sub(EXTERNAL_LINK_REPLACEMENT, text)
    return _WWW_RE.sub("www[dot]", text)


def safe_source_reference(url: Any) -> str:
    """Represent a source URL without storing the clickable URL itself."""
    raw = str(url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    domain = parsed.netloc or raw.split("/", 1)[0]
    domain = re.sub(r"^www\.", "", domain.lower()).strip()
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    if domain:
        return f"source_domain={domain}; source_ref={digest}"
    return f"source_ref={digest}"


def sanitize_payload_for_sheet(value: Any, *, drop_url_fields: bool = True) -> Any:
    """Recursively sanitize a JSON-like payload before writing it to Sheets."""
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            if drop_url_fields and is_external_url_field(key):
                continue
            sanitized[str(key)] = sanitize_payload_for_sheet(item, drop_url_fields=drop_url_fields)
        return sanitized
    if isinstance(value, list):
        return [sanitize_payload_for_sheet(item, drop_url_fields=drop_url_fields) for item in value]
    if isinstance(value, tuple):
        return [sanitize_payload_for_sheet(item, drop_url_fields=drop_url_fields) for item in value]
    if isinstance(value, str):
        return sanitize_external_links_for_sheet(value)
    return value


def sanitize_payload_for_sheet_json(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized = sanitize_payload_for_sheet(payload, drop_url_fields=True)
    if isinstance(sanitized, dict):
        return sanitized
    return {}
