"""Bounded private operational logs, never stored in the transactional workbook."""
import json
import re
from datetime import datetime, timezone


def _text(value, limit=2000):
    value = str(value)
    value = re.sub(r"(?i)((?:token|api[_-]?key|secret|password|signature)=)[^&\s]+", r"\1[redacted]", value)
    value = re.sub(r"(?i)Bearer\s+[^\s\"']+", "Bearer [redacted]", value)
    value = re.sub(r'(?i)("(?:token|api[_-]?key|secret|password|authorization|signature)"\s*:\s*")[^"]*(")',
                   r"\1[redacted]\2", value)
    return value[:limit] + (" [truncated]" if len(value) > limit else "")


def _safe(value, depth=0):
    if depth > 5:
        return "[depth limit]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        if value.lstrip().startswith(("{", "[")):
            try:
                return _safe(json.loads(value), depth + 1)
            except (ValueError, RecursionError):
                pass
        return _text(value)
    if isinstance(value, dict):
        return {_text(k, 120): "[redacted]" if re.search(
            r"token|secret|password|authorization|api[_-]?key|signature|cookie", str(k), re.I)
            else _safe(v, depth + 1) for k, v in list(value.items())[:40]}
    if isinstance(value, (tuple, list)):
        return [_safe(v, depth + 1) for v in value[:25]]
    return _text(value)


def emit_sms_diagnostic(logger, stage, details=None):
    details = details or {}
    record = {"schema": "sms-diagnostic-v37", "logged_at": datetime.now(timezone.utc).isoformat(),
              "stage": _text(stage, 120), "data": _safe(details)}
    encoded = json.dumps(record, ensure_ascii=True, allow_nan=False)
    if len(encoded) > 16000:
        record["data"] = {"request_id": _text(details.get("request_id", ""), 200),
                          "message_id": _text(details.get("message_id", ""), 200),
                          "reason": _text(details.get("reason", ""), 500), "truncated": True}
        encoded = json.dumps(record, ensure_ascii=True, allow_nan=False)
    logger.info("SMS_DIAGNOSTIC %s", encoded)
