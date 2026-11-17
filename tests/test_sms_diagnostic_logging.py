import ast
import json
from pathlib import Path
from unittest.mock import Mock

from sms_diagnostics import emit_sms_diagnostic


def test_logs_redact_nested_and_embedded_secrets_and_keep_request():
    logger = Mock()
    emit_sms_diagnostic(logger, "queued", {"request_id": "exact", "lease_token": "lease-secret",
        "raw_body": json.dumps({"token": "body-secret", "password": "pw-secret"}),
        "url": "https://example.invalid/?token=url-secret&action=status"})
    text = logger.info.call_args.args[1]
    assert "exact" in text
    assert all(secret not in text for secret in ("lease-secret", "body-secret", "pw-secret", "url-secret"))


def test_oversized_payload_is_bounded_with_identity_preserved():
    logger = Mock()
    emit_sms_diagnostic(logger, "stage", {"request_id": "exact", "message_id": "message",
        **{str(i): "\U0001f600" * 100000 for i in range(35)}})
    text = logger.info.call_args.args[1]
    assert len(text) < 16000
    assert json.loads(text)["data"] == {"request_id": "exact", "message_id": "message", "reason": "", "truncated": True}


def test_server_debug_wrapper_has_no_spreadsheet_or_sender_calls():
    source = Path(__file__).resolve().parents[1].joinpath("webhook_server.py").read_text()
    tree = ast.parse(source)
    func = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_sms_append_debug")
    text = ast.get_source_segment(source, func)
    assert "emit_sms_diagnostic" in text
    assert all(word not in text for word in ("_sms_get_or_create_ws", "append_row", "send_with_diagnostics", "sheets_service"))
