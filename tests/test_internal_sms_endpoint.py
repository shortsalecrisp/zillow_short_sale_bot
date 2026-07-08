import importlib
import json
import sys
import types
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class FakeSendResult:
    def __init__(self, success=True, status_code=200, response_text="OK"):
        self.success = success
        self.status_code = status_code
        self.response_text = response_text
        self.exception_type = "" if success else "HTTPError"
        self.exception_message = "" if success else "gateway failed"


class FakeSender:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def send_with_diagnostics(self, to, message, sms_type, row_idx=None, attempt=None):
        self.calls.append(
            {
                "to": to,
                "message": message,
                "sms_type": sms_type,
                "row_idx": row_idx,
                "attempt": attempt,
            }
        )
        return self.result


class FakeWorksheet:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.batch_updates = []
        self.appended_rows = []

    def row_values(self, row):
        return list(self.rows.get(row, []))

    def batch_update(self, data, value_input_option=None):
        self.batch_updates.append(
            {"data": data, "value_input_option": value_input_option}
        )
        for item in data:
            rng = item["range"]
            value = item["values"][0][0]
            letters = "".join(ch for ch in rng if ch.isalpha())
            digits = "".join(ch for ch in rng if ch.isdigit())
            if not digits:
                continue
            row = int(digits)
            col = _col_to_index(letters)
            existing = self.rows.setdefault(row, [])
            while len(existing) < col:
                existing.append("")
            existing[col - 1] = value

    def update(self, range_name, values=None, **_kwargs):
        if not values:
            return None
        letters = "".join(ch for ch in str(range_name).split(":")[0] if ch.isalpha())
        digits = "".join(ch for ch in str(range_name).split(":")[0] if ch.isdigit())
        row = int(digits or "1")
        start_col = _col_to_index(letters or "A")
        existing = self.rows.setdefault(row, [])
        written = values[0]
        while len(existing) < start_col - 1:
            existing.append("")
        for offset, value in enumerate(written):
            col = start_col + offset
            while len(existing) < col:
                existing.append("")
            existing[col - 1] = value
        return None

    def append_row(self, row, **_kwargs):
        self.appended_rows.append(row)
        next_row = max(self.rows.keys(), default=0) + 1
        self.rows[next_row] = list(row)

    def get_all_values(self):
        if not self.rows:
            return [["zpid", "address", "source", "created_at", "status"]]
        max_row = max(self.rows.keys())
        return [list(self.rows.get(idx, [])) for idx in range(1, max_row + 1)]


class FakeWorkbook:
    def __init__(self, worksheets):
        self.worksheets = worksheets

    def worksheet(self, name):
        return self.worksheets[name]

    def add_worksheet(self, title, rows, cols):
        ws = FakeWorksheet()
        self.worksheets[title] = ws
        return ws


def _col_to_index(letters):
    value = 0
    for char in letters:
        value = value * 26 + (ord(char.upper()) - ord("A") + 1)
    return value


def _row(
    *,
    phone="555-111-2222",
    sent="",
    init_ts="",
    verified="",
    first="Alex",
    address="123 Main",
):
    values = [""] * 43
    values[0] = first
    values[2] = phone
    values[4] = address
    values[7] = sent
    values[22] = init_ts
    values[42] = verified
    return values


CHATBOT_HEADERS = [
    "agent_name",
    "last_name",
    "phone",
    "email",
    "listing_address",
    "city",
    "state",
    "initial_text_sent",
    "followup_text_sent",
    "response_status",
    "mailshake_status",
    "last_outbound_text",
    "conversation_summary",
    "ai_state",
    "last_contact_time",
    "call_booking_status",
    "handoff_flag",
    "history_json",
    "auto_reply_count",
    "human_override",
    "last_message_id",
] + [f"extra_{idx}" for idx in range(21, 43)]


def _chatbot_row(
    *,
    first="Andi",
    last="Gamble",
    phone="954-235-7723",
    email="Andrea.gamble@lptrealty.com",
    address="2266 Red Gate Rd",
    status="N",
):
    values = [""] * len(CHATBOT_HEADERS)
    values[0] = first
    values[1] = last
    values[2] = phone
    values[3] = email
    values[4] = address
    values[5] = "Orlando"
    values[6] = "FL"
    values[10] = status
    values[17] = "[]"
    values[18] = "0"
    values[19] = "FALSE"
    return values


def _import_webhook_server(monkeypatch, *, sender_result):
    fake_sender = FakeSender(sender_result)
    sheet1 = FakeWorksheet(
        {
            1: CHATBOT_HEADERS,
            2: _chatbot_row(),
            3: _chatbot_row(
                first="Alex",
                last="Agent",
                phone="555-222-3333",
                address="123 Main St",
            ),
            12: _row(phone="555-111-2222", sent="", verified=""),
            13: _row(phone="555-111-2222", sent="x", init_ts="2026-05-22T08:00:00-04:00", verified="x"),
            14: _row(phone="555-111-2222", sent="x", verified=""),
            15: _row(phone="", sent="", verified=""),
            16: _row(phone="555-111-2222", sent="", init_ts="", verified="x"),
        }
    )
    workbook = FakeWorkbook(
        {
            "Sheet1": sheet1,
            "Replies": FakeWorksheet(),
            "sms_debug_log": FakeWorksheet(),
            "sms_send_guard": FakeWorksheet(),
            "PendingQueue": FakeWorksheet(),
        }
    )

    monkeypatch.setenv("GSHEET_ID", "sheet-id")
    monkeypatch.setenv("GCP_SERVICE_ACCOUNT_JSON", json.dumps({}))
    monkeypatch.setenv("SMS_GATEWAY_API_KEY", "gateway-key")
    monkeypatch.setenv("CODEX_AUTOMATION_TOKEN", "secret-token")
    monkeypatch.setenv("DISABLE_APIFY_SCHEDULER", "true")
    monkeypatch.setenv("RENDER_APIFY_TRIGGER_DISABLED", "true")

    fake_bot_min = types.ModuleType("bot_min")
    fake_bot_min.INITIAL_SMS_END = 21
    fake_bot_min.TZ = ZoneInfo("America/New_York")
    fake_bot_min.WORK_START = 8
    fake_bot_min.WORK_END = 20
    fake_bot_min.SCHEDULER_TZ = ZoneInfo("America/New_York")
    fake_bot_min.SMS_TEMPLATE = (
        "Hey {first}, this is Yoni Kutler with Crisp Short Sales. "
        "I saw your short sale at {address}."
    )
    fake_bot_min.append_seen_zpids = lambda *args, **kwargs: None
    fake_bot_min.dedupe_rows_by_zpid = lambda rows: rows
    fake_bot_min.fetch_contact_page = lambda *args, **kwargs: ("", "")
    fake_bot_min.load_seen_zpids = lambda: set()
    fake_bot_min.log_headless_status = lambda logger: None
    fake_bot_min.process_rows = lambda *args, **kwargs: None
    fake_bot_min.run_hourly_scheduler = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "bot_min", fake_bot_min)

    fake_gspread = types.ModuleType("gspread")
    fake_gspread.WorksheetNotFound = KeyError
    fake_gspread.exceptions = types.SimpleNamespace(APIError=RuntimeError)
    fake_gspread.authorize = lambda _creds: types.SimpleNamespace(
        open_by_key=lambda _key: workbook
    )
    monkeypatch.setitem(sys.modules, "gspread", fake_gspread)

    fake_sms = types.ModuleType("sms_providers")
    fake_sms.get_sender = lambda _provider=None: fake_sender
    monkeypatch.setitem(sys.modules, "sms_providers", fake_sms)

    service_account = types.ModuleType("google.oauth2.service_account")
    service_account.Credentials = types.SimpleNamespace(
        from_service_account_info=lambda *_args, **_kwargs: object()
    )
    google_module = types.ModuleType("google")
    oauth2_module = types.ModuleType("google.oauth2")
    oauth2_module.service_account = service_account
    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.oauth2", oauth2_module)
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", service_account)

    sys.modules.pop("webhook_server", None)
    module = importlib.import_module("webhook_server")
    return module, sheet1, fake_sender


def test_internal_initial_sms_requires_token(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        json={"row": 12, "phone": "555-111-2222"},
    )

    assert response.status_code == 403
    assert sender.calls == []


def test_internal_initial_sms_sends_and_marks_sheet_after_gateway_ok(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True, status_code=200, response_text="OK"),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={
            "row": 12,
            "phone": "555-111-2222",
            "first": "Alex",
            "address": "123 Main",
            "mark_codex_verified": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "sent"
    assert body["gateway_status"] == 200
    assert sender.calls == [
        {
            "to": "15551112222",
            "message": (
                "Hey Alex, this is Yoni Kutler with Crisp Short Sales. "
                "I saw your short sale at 123 Main."
            ),
            "sms_type": "initial",
            "row_idx": 12,
            "attempt": 1,
        }
    ]
    assert sheet.rows[12][7] == "x"
    assert datetime.fromisoformat(sheet.rows[12][22]).tzinfo is not None
    assert sheet.rows[12][42] == "x"


def test_internal_initial_sms_uses_street_only_payload_address(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )

    message = module._format_initial_message(
        {"first": "Alex", "address": "123 Main St, Honolulu, HI 96813"},
        _row(first="Alex", address="Fallback Address"),
    )

    assert "at 123 Main St." in message
    assert "Honolulu" not in message


def test_internal_initial_sms_does_not_mark_sheet_when_gateway_fails(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(
            success=False,
            status_code=200,
            response_text="unexpected",
        ),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 12, "phone": "555-111-2222"},
    )

    assert response.status_code == 502
    assert sender.calls
    assert sheet.rows[12][7] == ""
    assert sheet.rows[12][22] == ""
    assert sheet.rows[12][42] == ""


def test_internal_initial_sms_rejects_already_sent_without_force(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 14, "phone": "555-111-2222"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "initial_sms_already_marked"
    assert sender.calls == []


def test_internal_initial_sms_rejects_missing_row_phone(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 15, "phone": "555-111-2222"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "row_phone_missing"
    assert sender.calls == []


def test_internal_initial_sms_rejects_row_phone_mismatch(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 12, "phone": "555-333-4444"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "row_phone_mismatch"
    assert sender.calls == []


def test_internal_initial_sms_force_resend_allows_already_sent_row(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={
            "row": 14,
            "phone": "555-111-2222",
            "force_resend": True,
            "mark_codex_verified": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "sent"
    assert sender.calls[0]["row_idx"] == 14
    assert sheet.rows[14][7] == "x"
    assert sheet.rows[14][42] == "x"


def test_internal_initial_sms_returns_already_verified_without_sending(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 13, "phone": "555-111-2222"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "already_verified"
    assert sender.calls == []


def test_internal_initial_sms_sends_when_verified_but_not_marked_sent(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 16, "phone": "555-111-2222"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "sent"
    assert sender.calls[0]["row_idx"] == 16
    assert sheet.rows[16][7] == "x"
    assert sheet.rows[16][22]
    assert sheet.rows[16][42] == "x"


def test_internal_followup_sms_requires_token(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-followup-sms",
        json={"phone": "555-111-2222", "message": "Custom follow-up"},
    )

    assert response.status_code == 403
    assert sender.calls == []


def test_internal_followup_sms_sends_custom_message_without_marking_sheet(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True, status_code=200, response_text="OK"),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-followup-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 12, "phone": "555-111-2222", "message": "Custom follow-up"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "sent"
    assert body["row"] == 12
    assert body["gateway_status"] == 200
    assert sender.calls == [
        {
            "to": "15551112222",
            "message": "Custom follow-up",
            "sms_type": "followup",
            "row_idx": 12,
            "attempt": 1,
        }
    ]
    assert sheet.rows[12][7] == ""
    assert sheet.rows[12][22] == ""
    assert sheet.rows[12][42] == ""


def test_internal_followup_sms_rejects_empty_message(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-followup-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"phone": "555-111-2222", "message": "   "},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "empty_message"
    assert sender.calls == []


def test_sms_chatbot_records_hot_handoff_without_apps_script_mail(monkeypatch):
    module, sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": (
                "Hi Yoni. I will call you about this today. "
                "I'm dealing with Shellpoint mtg but am beyond busy and can use the help."
            ),
            "received_at": "7-8-26 08.07",
            "message_id": "+19542357723-1783512429065",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["should_reply"] is False
    assert body["handoff_needed"] is True
    assert body["lead_status"] == "Y"
    assert sheet.rows[2][9].startswith("Hi Yoni.")
    assert sheet.rows[2][10] == "Y"
    assert sheet.rows[2][13] == "handoff"
    assert sheet.rows[2][16] == "TRUE"
    assert sheet.rows[2][19] == "TRUE"
    assert "Shellpoint" in sheet.rows[2][17]


def test_sms_chatbot_reply_and_reply_sent_writeback(monkeypatch):
    module, sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "555-222-3333",
            "message": "How can you help?",
            "received_at": "2026-07-08 09:10",
            "message_id": "msg-help-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["ok"] is True
    assert body["should_reply"] is True
    assert body["should_reply_text"] == "true"
    assert body["reply_to_phone"] == "5552223333"
    assert "short sale process" in body["reply_text"]

    response = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "reply_sent",
            "phone": "555-222-3333",
            "reply_text": body["reply_text"],
            "sent_at": "2026-07-08T09:11:00-04:00",
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert sheet.rows[3][11] == body["reply_text"]
    assert sheet.rows[3][18] == "1"
    assert "assistant" in sheet.rows[3][17]
