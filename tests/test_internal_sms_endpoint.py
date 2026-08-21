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

    def delete_rows(self, start_index, end_index=None):
        end_index = end_index or start_index
        count = end_index - start_index + 1
        for row_idx in range(start_index, end_index + 1):
            self.rows.pop(row_idx, None)
        shifted = {}
        for row_idx, row in self.rows.items():
            shifted[row_idx - count if row_idx > end_index else row_idx] = row
        self.rows = shifted

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
] + [f"extra_{idx}" for idx in range(21, 45)]
CHATBOT_HEADERS[37] = "callback_requested"
CHATBOT_HEADERS[38] = "callback_time"
CHATBOT_HEADERS[43] = "last_inbound_text"
CHATBOT_HEADERS[44] = "last_inbound_at"


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
            12: _row(phone="555-111-2212", sent="", verified=""),
            13: _row(phone="555-111-2213", sent="x", init_ts="2026-05-22T08:00:00-04:00", verified="x"),
            14: _row(phone="555-111-2214", sent="x", verified=""),
            15: _row(phone="", sent="", verified=""),
            16: _row(phone="555-111-2216", sent="", init_ts="", verified="x"),
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
    def fake_enqueue_initial_sms(*, row_idx, phone, message, mark_codex_verified):
        fake_sender.calls.append(
            {
                "to": phone,
                "message": message,
                "sms_type": "initial",
                "row_idx": row_idx,
                "attempt": 1,
            }
        )
        if not fake_sender.result.success:
            raise module.HTTPException(status_code=502, detail="tasker_outbox_enqueue_failed")
        return {
            "ok": True,
            "queued": True,
            "request_id": f"render-initial-{row_idx}",
            "message_id": f"initial-{row_idx}-test",
            "pending_row": 10 + row_idx,
        }
    module._enqueue_initial_sms_via_tasker_outbox = fake_enqueue_initial_sms
    return module, sheet1, fake_sender


def test_internal_initial_sms_requires_token(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        json={"row": 12, "phone": "555-111-2212"},
    )

    assert response.status_code == 403
    assert sender.calls == []


def test_internal_initial_sms_queues_and_waits_for_tasker_receipt_before_marking_sheet(monkeypatch):
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
            "phone": "555-111-2212",
            "first": "Alex",
            "address": "123 Main",
            "mark_codex_verified": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "queued"
    assert body["request_id"] == "render-initial-12"
    assert sender.calls == [
        {
            "to": "15551112212",
            "message": (
                "Hey Alex, this is Yoni Kutler with Crisp Short Sales. "
                "I saw your short sale at 123 Main."
            ),
            "sms_type": "initial",
            "row_idx": 12,
            "attempt": 1,
        }
    ]
    assert sheet.rows[12][7] == ""
    assert sheet.rows[12][11] == ""
    assert sheet.rows[12][14] == ""
    assert sheet.rows[12][22] == ""
    assert sheet.rows[12][42] == ""


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
        json={"row": 12, "phone": "555-111-2212"},
    )

    assert response.status_code == 502
    assert sender.calls
    assert sheet.rows[12][7] == ""
    assert sheet.rows[12][11] == ""
    assert sheet.rows[12][14] == ""
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
        json={"row": 14, "phone": "555-111-2214"},
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
            "phone": "555-111-2214",
            "force_resend": True,
            "mark_codex_verified": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert sender.calls[0]["row_idx"] == 14
    assert sheet.rows[14][7] == "x"
    assert sheet.rows[14][42] == ""


def test_internal_initial_sms_returns_already_verified_without_sending(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 13, "phone": "555-111-2213"},
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
        json={"row": 16, "phone": "555-111-2216"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert sender.calls[0]["row_idx"] == 16
    assert sheet.rows[16][7] == ""
    assert sheet.rows[16][22] == ""
    assert sheet.rows[16][42] == "x"


def test_internal_initial_sms_suppresses_duplicate_phone_elsewhere(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    prior = _row(
        phone="555-222-1717",
        sent="x",
        init_ts="2026-07-02T18:01:56-04:00",
        verified="x",
        first="Julia",
        address="116 Highland Ave",
    )
    prior[9] = "I do my own short sales."
    prior[10] = "R"
    sheet.rows[5] = prior
    sheet.rows[17] = _row(
        phone="555-222-1717",
        sent="",
        verified="",
        first="Julia",
        address="340 S 3rd Street #2",
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={
            "row": 17,
            "phone": "555-222-1717",
            "first": "Julia",
            "address": "340 S 3rd Street #2",
            "force_resend": True,
            "mark_codex_verified": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "already_contacted_phone"
    assert body["existing_row"] == 5
    assert body["deleted_row"] == 17
    assert body["row_deleted"] is True
    assert sender.calls == []
    assert 17 not in sheet.rows


def test_internal_initial_sms_deletes_later_duplicate_suppression_marker(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True, status_code=200, response_text="OK"),
    )
    later_duplicate = _row(
        phone="555-111-2212",
        sent="",
        init_ts="",
        verified="x",
        first="Alex",
        address="456 Later Duplicate",
    )
    later_duplicate[24] = "duplicate_phone_suppressed"
    later_duplicate[25] = (
        "2026-07-26T11:00:00-04:00: duplicate phone suppressed; "
        "same phone exists on Sheet1 row 12"
    )
    sheet.rows[17] = later_duplicate
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-initial-sms",
        headers={"authorization": "Bearer secret-token"},
        json={
            "row": 12,
            "phone": "555-111-2212",
            "first": "Alex",
            "address": "123 Main",
            "mark_codex_verified": True,
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "queued"
    assert body["deleted_duplicate_rows"] == [17]
    assert sender.calls[0]["row_idx"] == 12
    assert sheet.rows[12][7] == ""
    assert sheet.rows[12][42] == ""
    assert 17 not in sheet.rows


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


def test_internal_followup_sms_persists_confirmed_outbound_without_marking_initial(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True, status_code=200, response_text="OK"),
    )
    client = TestClient(module.app)

    response = client.post(
        "/internal/send-followup-sms",
        headers={"authorization": "Bearer secret-token"},
        json={"row": 12, "phone": "555-111-2212", "message": "Custom follow-up"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "sent"
    assert body["row"] == 12
    assert body["gateway_status"] == 200
    assert body["outbound_persisted"] is True
    assert sender.calls == [
        {
            "to": "15551112212",
            "message": "Custom follow-up",
            "sms_type": "followup",
            "row_idx": 12,
            "attempt": 1,
        }
    ]
    assert sheet.rows[12][7] == ""
    assert sheet.rows[12][11] == "Custom follow-up"
    assert datetime.fromisoformat(sheet.rows[12][14]).tzinfo is not None
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


def test_sms_name_and_number_request_gets_public_contact_without_handoff(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
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
            "message": "Can you send me your name and number?",
            "message_id": "name-number-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["should_reply"] is True
    assert body["reply_text"] == "Yoni Kutler - 404-300-9526. You can call or text anytime."
    assert body["handoff_needed"] is False
    assert body["needs_review"] is False


def test_sms_name_and_number_rule_does_not_match_agent_sending_buyer_contact(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )

    assert module._sms_is_yoni_name_and_number_request(
        "I can send you the buyer's name and number tomorrow"
    ) is False


def test_sms_weekday_callback_is_handed_off_and_persisted_as_scheduled(monkeypatch):
    module, sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)
    inbound = "Feel free to reach out to me Monday. Today isn't a good day"

    response = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": inbound,
            "message_id": "weekday-callback-1",
            "received_at": "2026-08-08T14:12:00-04:00",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert module._sms_is_scheduled_callback(inbound) is True
    assert module._sms_extract_scheduled_callback_reference(inbound) == "Monday"
    assert body["should_reply"] is False
    assert body["reply_text"] == ""
    assert body["handoff_needed"] is True
    assert body["call_booking_status"] == "scheduled_callback"
    assert body["callback_time"] == "Monday"
    assert sheet.rows[2][15] == "scheduled_callback"
    assert sheet.rows[2][16] == "TRUE"
    assert sheet.rows[2][19] == "TRUE"
    assert sheet.rows[2][37] == "yes"
    assert sheet.rows[2][38] == "Monday"

    assert module._sms_is_scheduled_callback("Please don't call me Monday") is False
    assert module._sms_is_scheduled_callback("I have an open house Monday") is False
    assert module._sms_is_scheduled_callback("Call the lender Monday") is False
    assert module._sms_extract_scheduled_callback_reference("Not tomorrow, please call me Monday") == "Monday"


def test_sms_post_handoff_callback_update_persists_without_reply(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    sheet.rows[2][13] = "handoff"
    sheet.rows[2][15] = "interested_no_call"
    sheet.rows[2][16] = "TRUE"
    sheet.rows[2][19] = "TRUE"
    sheet.rows[2][10] = "O"
    client = TestClient(module.app)
    inbound = "Afternoon on Monday would work better"

    response = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": inbound,
            "message_id": "post-handoff-callback-update-1",
            "received_at": "2026-08-11T13:05:00-04:00",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["should_reply"] is False
    assert body["handoff_needed"] is True
    assert body["reason"] == "Callback updated after human handoff"
    assert body["lead_status"] == "O"
    assert body["callback_updated"] is True
    assert body["alert_needed"] is False
    assert body["handoff_type"] == ""
    assert sender.calls == []
    assert sheet.rows[2][10] == "O"
    assert sheet.rows[2][13] == "handoff"
    assert sheet.rows[2][15] == "scheduled_callback"
    assert sheet.rows[2][16] == "TRUE"
    assert sheet.rows[2][19] == "TRUE"
    assert sheet.rows[2][37] == "yes"
    assert sheet.rows[2][38] == "Monday Afternoon"


def test_sms_post_handoff_repeated_callback_time_does_not_request_another_alert(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult(success=True))
    sheet.rows[2][13] = "handoff"
    sheet.rows[2][15] = "scheduled_callback"
    sheet.rows[2][16] = "TRUE"
    sheet.rows[2][19] = "TRUE"
    sheet.rows[2][38] = "Monday Afternoon"

    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": "Afternoon on Monday would work better",
            "message_id": "post-handoff-callback-repeat-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["should_reply"] is False
    assert body["reason"] == "Callback timing repeated after human handoff"
    assert body["callback_updated"] is False
    assert body["alert_needed"] is False
    assert body["handoff_type"] == ""
    assert sender.calls == []


def test_sms_old_confirmed_closeout_replay_with_new_message_id_is_suppressed(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult(success=True))
    inbound = "I have someone thank you"
    sheet.rows[2][9] = inbound
    sheet.rows[2][13] = "done"
    sheet.rows[2][17] = json.dumps(
        [
            {"role": "agent", "text": inbound, "ts": "2026-08-11T10:05:00-04:00"},
            {"role": "assistant", "text": "Understood - thanks for letting me know.", "ts": "2026-08-11T10:07:00-04:00"},
        ]
    )
    sheet.rows[2][20] = "first-message-id"
    sheet.rows[2][43] = "i have someone thank you"
    sheet.rows[2][44] = "2026-08-11T10:05:00-04:00"

    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": "  I HAVE someone thank you  ",
            "message_id": "second-message-id",
            "received_at": "2026-08-12T10:18:00-04:00",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body.get("duplicate") is True
    assert body["should_reply"] is False
    assert sender.calls == []
    assert sheet.rows[2][20] == "first-message-id"
    assert sheet.rows[2][44] == "2026-08-11T10:05:00-04:00"


def test_sms_old_confirmed_not_short_sale_replay_with_new_message_id_is_suppressed(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult(success=True))
    inbound = (
        "That was an input error and has been corrected - this is not a short sale - "
        "but appreciate your text."
    )
    sheet.rows[2][9] = inbound
    sheet.rows[2][13] = "done"
    sheet.rows[2][17] = json.dumps(
        [
            {"role": "agent", "text": inbound, "ts": "2026-08-14T17:14:00-04:00"},
            {
                "role": "assistant",
                "text": "Ahh, ok... thanks for letting me know. Good luck with your listing!",
                "ts": "2026-08-14T17:16:00-04:00",
            },
        ]
    )
    sheet.rows[2][20] = "randy-first-message-id"
    sheet.rows[2][43] = inbound

    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": inbound,
            "message_id": "randy-replay-message-id",
            "received_at": "2026-08-15T08:07:00-04:00",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body.get("duplicate") is True
    assert body["should_reply"] is False
    assert sender.calls == []
    assert sheet.rows[2][20] == "randy-first-message-id"


def test_sms_unconfirmed_old_duplicate_is_reprocessed(monkeypatch):
    module, sheet, _sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult(success=True))
    inbound = "I have someone thank you"
    sheet.rows[2][9] = inbound
    sheet.rows[2][13] = "active"
    sheet.rows[2][17] = json.dumps(
        [{"role": "agent", "text": inbound, "ts": "2026-08-11T10:05:00-04:00"}]
    )
    sheet.rows[2][20] = "first-message-id"
    sheet.rows[2][43] = inbound
    sheet.rows[2][44] = "2026-08-11T10:05:00-04:00"

    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": inbound,
            "message_id": "second-message-id",
            "received_at": "2026-08-12T10:18:00-04:00",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body.get("duplicate") is not True
    assert sheet.rows[2][20] == "second-message-id"
    assert sheet.rows[2][44] == "2026-08-12T10:18:00-04:00"


def test_sms_callback_repeat_bypasses_durable_duplicate_guard(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult(success=True))
    inbound = "Afternoon on Monday would work better"
    sheet.rows[2][9] = inbound
    sheet.rows[2][13] = "handoff"
    sheet.rows[2][15] = "scheduled_callback"
    sheet.rows[2][16] = "TRUE"
    sheet.rows[2][17] = json.dumps(
        [
            {"role": "agent", "text": inbound, "ts": "2026-08-11T10:05:00-04:00"},
            {"role": "assistant", "text": "Monday afternoon works.", "ts": "2026-08-11T10:06:00-04:00"},
        ]
    )
    sheet.rows[2][19] = "TRUE"
    sheet.rows[2][20] = "first-message-id"
    sheet.rows[2][38] = "Monday Afternoon"
    sheet.rows[2][43] = inbound.lower()
    sheet.rows[2][44] = "2026-08-11T10:05:00-04:00"

    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": inbound,
            "message_id": "second-message-id",
            "received_at": "2026-08-12T10:18:00-04:00",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body.get("duplicate") is not True
    assert body["reason"] == "Callback timing repeated after human handoff"
    assert body["alert_needed"] is False
    assert sheet.rows[2][20] == "second-message-id"
    assert sender.calls == []


def test_sms_post_handoff_non_scheduling_day_reference_stays_under_human_override(monkeypatch):
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult(success=True))
    sheet.rows[2][13] = "handoff"
    sheet.rows[2][15] = "interested_no_call"
    sheet.rows[2][16] = "TRUE"
    sheet.rows[2][19] = "TRUE"

    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": "I have an open house Monday",
            "message_id": "post-handoff-non-scheduling-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["should_reply"] is False
    assert body["reason"] == "Human override enabled - inbound recorded only"
    assert body["callback_updated"] is False
    assert body["alert_needed"] is False
    assert body["handoff_type"] == ""
    assert sheet.rows[2][37] == ""
    assert sheet.rows[2][38] == ""
    assert sender.calls == []


def test_sms_reaction_to_latest_outbound_is_suppressed_before_processing(monkeypatch):
    module, sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    outbound = "Thanks for letting me know. Good luck with the listing!"
    sheet.rows[2][11] = outbound
    client = TestClient(module.app)

    response = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": f"Liked “{outbound}”",
            "message_id": "reaction-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["reaction"] is True
    assert body["should_reply"] is False
    assert sheet.rows[2][17] == "[]"


def test_sms_reaction_matches_when_transport_drops_internal_apostrophe(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )

    outbound = "Ok, no problem. If anything changes, I'll be glad to help."
    inbound = "to “Ok, no problem. If anything changes, Ill be glad to help.”"

    assert module._sms_is_reaction_to_last_outbound(
        inbound,
        {"last_outbound_text": outbound},
    ) is True
    assert module._sms_is_reaction_to_last_outbound(
        "to schedule a call tomorrow",
        {"last_outbound_text": outbound},
    ) is False


def test_sms_compound_opt_outs_are_suppressed_without_false_positive(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )

    for inbound in ["Stop. Already have a company.", "Please remove my info"]:
        decision = module._sms_fast_decision({}, inbound)
        assert decision["lead_status"] == "R"
        assert decision["conversation_done"] is True
        assert decision["block_reply"] is True
        assert decision["reply_text"] == ""

    assert module._sms_is_opt_out("Please stop by the office tomorrow") is False


def test_sms_client_consultation_stays_active_and_gets_acknowledgement(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = (
        "Let me chat with my client because I think it's best that somebody handled that "
        "on her behalf I will get back to you."
    )

    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_is_client_consultation_interest(inbound) is True
    assert decision["lead_status"] == "Y"
    assert decision["conversation_done"] is False
    assert decision["handoff_needed"] is False
    assert decision["block_reply"] is False
    assert decision["reply_text"] == module._sms_client_consultation_reply()
    assert module._sms_is_client_consultation_interest(
        "Let me ask my client, but no thanks, we already have someone"
    ) is False


def test_sms_timeline_question_gets_approved_60_to_90_day_reply(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )

    decision = module._sms_fast_decision({}, "What is the minimum time to stop foreclosure?")

    assert module._sms_is_short_sale_timeline_question("What is the minimum time to stop foreclosure?") is True
    assert decision["reply_text"] == module.SHORT_SALE_TIMELINE_REPLY
    assert decision["lead_status"] == "Y"
    assert decision["handoff_needed"] is False
    assert decision["block_reply"] is False


def test_sms_mixed_timeline_and_unsupported_stats_still_hands_off(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )

    inbound = "What is your success rate and average closing timeline?"
    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_is_short_sale_timeline_question(inbound) is True
    assert module._sms_is_unsupported_performance_stats_question(inbound) is True
    assert decision["reply_text"] == ""
    assert decision["handoff_needed"] is True
    assert decision["block_reply"] is True


def test_sms_existing_crisp_client_exits_marketing_for_handoff(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "I already have an active Crisp portal and am set up with Yoni."

    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_is_existing_crisp_relationship(inbound) is True
    assert decision["lead_status"] == "R"
    assert decision["handoff_needed"] is True
    assert decision["block_reply"] is True
    assert decision["reply_text"] == ""
    assert module._sms_is_existing_crisp_relationship(
        "I already have someone handling it"
    ) is False
    assert module._sms_is_existing_crisp_relationship(
        "What company are you with?"
    ) is False

    generic_current_help = (
        "Hi Yoni, thank you for following up! I currently have someone assisting me with the short sale process "
        "for this property, but I appreciate you reaching out. I'll definitely keep your information for future "
        "short sale opportunities."
    )
    generic_decision = module._sms_fast_decision({}, generic_current_help)
    assert module._sms_is_existing_crisp_relationship(generic_current_help) is False
    assert generic_decision["lead_status"] == "O"
    assert generic_decision["conversation_done"] is True
    assert generic_decision["handoff_needed"] is False
    assert generic_decision["block_reply"] is False
    assert generic_decision["reply_text"].startswith("Thanks, I appreciate it.")

    assert module._sms_is_existing_crisp_relationship(
        "Hi Yoni, I am currently working with you on this short sale."
    ) is True


def test_sms_not_short_sale_closeout_suppresses_same_topic_continuations(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    row = {
        "ai_state": "done",
        "mailshake_status": "R",
        "conversation_summary": "Not actually a short sale / changed listing",
    }

    for inbound in [
        "I have cloned the listing it must have carried over",
        "It's a probate.",
        "Thanks for bringing it to my attention",
    ]:
        decision = module._sms_fast_decision(row, inbound)
        assert decision["lead_status"] == "R"
        assert decision["conversation_done"] is True
        assert decision["block_reply"] is True
        assert decision["reply_text"] == ""

    assert module._sms_is_post_closeout_not_short_sale_continuation(
        "Can you help with probate?", row
    ) is False
    assert module._sms_is_post_closeout_not_short_sale_continuation(
        "Please send me your contact information", row
    ) is False


def test_sms_covered_but_relationship_open_closes_as_non_hot_without_handoff(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    row = {
        "conversation_summary": "Already represented / handled",
        "response_status": "I already have a negotiator",
        "history_json": json.dumps([{"role": "agent", "text": "I already have a negotiator"}]),
    }

    keep_info = module._sms_fast_decision(row, "Great to know. I'll keep your info")
    assert keep_info["lead_status"] == "O"
    assert keep_info["conversation_done"] is True
    assert keep_info["handoff_needed"] is False
    assert keep_info["block_reply"] is False

    apostrophe_stripped = module._sms_fast_decision(
        row,
        "Ill definitely keep your information for future short sale opportunities",
    )
    assert apostrophe_stripped["lead_status"] == "O"
    assert apostrophe_stripped["conversation_done"] is True
    assert apostrophe_stripped["handoff_needed"] is False

    reciprocal = module._sms_fast_decision(
        row,
        "If you have clients looking for a great agent, keep me in mind as well!",
    )
    assert reciprocal["lead_status"] == "O"
    assert reciprocal["conversation_done"] is True
    assert reciprocal["handoff_needed"] is False
    assert reciprocal["reply_text"] == "Absolutely - thanks. I'll keep you in mind, too."

    assert module._sms_is_relationship_only_after_existing_coverage(
        "Can you call me about the next short sale?", row
    ) is False


def test_sms_future_buyer_recontact_closes_warm_without_takeover(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "So let you know when I eventually get a buyer?"

    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_is_future_buyer_recontact(inbound) is True
    assert decision["lead_status"] == "O"
    assert decision["conversation_done"] is True
    assert decision["handoff_needed"] is False
    assert decision["block_reply"] is False
    assert decision["call_booking_status"] == "warm_future_interest"
    assert decision["reply_text"] == module._sms_future_buyer_recontact_reply()
    assert module._sms_is_future_buyer_recontact(
        "Let you know when I get a buyer; can you call me tomorrow?"
    ) is False


def test_sms_natural_tomorrow_callback_and_third_party_negative(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "Let's set up a time to talk tomorrow. Let me know what time works best for you."

    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_is_scheduled_callback(inbound) is True
    assert module._sms_extract_scheduled_callback_reference(inbound) == "Tomorrow"
    assert decision["handoff_needed"] is True
    assert decision["block_reply"] is False
    assert decision["reply_text"] == "Perfect, thanks."
    assert decision["call_booking_status"] == "scheduled_callback"
    assert decision["callback_time"] == "Tomorrow"
    assert module._sms_is_scheduled_callback("Let's talk to the lender tomorrow") is False


def test_sms_call_interest_reopens_closed_conversation_for_handoff(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    row = {
        "ai_state": "done",
        "mailshake_status": "R",
        "call_booking_status": "closed_no_interest",
        "human_override": "TRUE",
    }
    inbound = "I would be interested to have a call to see how your services differ from theirs."

    decision = module._sms_fast_decision(row, inbound)

    assert module._sms_is_phone_call_interest(inbound) is True
    assert decision["lead_status"] == "R"
    assert decision["conversation_done"] is False
    assert decision["handoff_needed"] is False
    assert decision["block_reply"] is True
    assert decision["reason"] == "Human override enabled - inbound recorded only"

    service_interest = module._sms_fast_decision(
        row,
        "I am interested in your services and would like to learn more.",
    )
    assert module._sms_is_present_service_interest(
        "I am interested in your services and would like to learn more."
    ) is True
    assert service_interest["handoff_needed"] is False
    assert service_interest["block_reply"] is True
    assert service_interest["reason"] == "Human override enabled - inbound recorded only"
    assert module._sms_is_present_service_interest(
        "No thanks, but I will keep your information in mind for the future."
    ) is False


def test_sms_company_question_outranks_existing_coverage_closeout(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "I already have an attorney. What company are you so I can let my attorney know?"

    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_is_company_identity_question(inbound) is True
    assert module._sms_has_existing_coverage(inbound) is True
    assert decision["lead_status"] == "O"
    assert decision["conversation_done"] is True
    assert decision["handoff_needed"] is False
    assert decision["reply_text"].startswith("I'm with Crisp Short Sales.")
    assert decision["reason"] == "Answered company question before generic coverage language"


def test_sms_decline_or_not_short_sale_plus_question_is_warm_o(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )

    website = module._sms_fast_decision({}, "No thanks, but what is your website?")
    company = module._sms_fast_decision({}, "This is not a short sale; what company are you with?")
    email = module._sms_fast_decision({}, "Not interested, but email me information")

    for decision in (website, company):
        assert decision["lead_status"] == "O"
        assert decision["conversation_done"] is True
    assert email["lead_status"] == "O"
    assert email["conversation_done"] is False


def test_sms_rate_question_outranks_existing_coverage_closeout(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "We already have a negotiator. What's your rate?"

    decision = module._sms_fast_decision({}, inbound)

    assert decision["lead_status"] == "O"
    assert decision["conversation_done"] is True
    assert decision["handoff_needed"] is False
    assert "flat fee to the buyer" in decision["reply_text"]
    assert decision["reason"] == "Asked about charge, fee, percentage, or how Crisp gets paid"


def test_sms_differentiation_question_gets_deterministic_reply(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "How are you different from them with communication and documentation?"

    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_is_differentiation_question(inbound) is True
    assert decision["lead_status"] == "Y"
    assert decision["handoff_needed"] is True
    assert decision["reply_text"] == (
        "I handle the lender-side work and keep agents updated throughout the process. "
        "If that sounds useful, I'm happy to talk through your listing."
    )
    assert decision["alert_needed"] is True
    assert decision["handoff_type"] == "HOT LEAD - DIFFERENTIATION QUESTION"
    assert decision["reason"] == "Answered differentiation question before generic coverage language"


def test_sms_testimonials_request_uses_reviews_reply_without_email_prompt(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "Can you send testimonials?"

    decision = module._sms_fast_decision({}, inbound)

    assert decision["lead_status"] == "Y"
    assert decision["handoff_needed"] is False
    assert "crispshortsales.com" in decision["reply_text"]
    assert "reviews" in decision["reply_text"].lower()
    assert "what is your email" not in decision["reply_text"].lower()
    assert decision["reason"] == "Answered website question before generic coverage language"


def test_sms_automated_alternate_number_notice_does_not_handoff(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "You've reached Redfin, but we actually use a different number for texting - (214) 427-8372. We'll send you a message from that number!"

    decision = module._sms_fast_decision({"mailshake_status": "R"}, inbound)

    assert module._sms_is_automated_routing_notice(inbound) is True
    assert decision["lead_status"] == "R"
    assert decision["handoff_needed"] is False
    assert decision["block_reply"] is True
    assert decision["preserve_existing_state"] is True
    assert decision["reply_text"] == ""
    assert decision["reason"] == "Automated routing or alternate-number notice ignored"


def test_sms_automated_alternate_number_notice_preserves_existing_handoff_state(monkeypatch):
    module, sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    sheet.rows[2][10] = "Y"
    sheet.rows[2][13] = "handoff"
    sheet.rows[2][15] = "interested_no_call"
    sheet.rows[2][16] = "TRUE"
    sheet.rows[2][19] = "TRUE"
    inbound = "You've reached Redfin, but we actually use a different number for texting - (214) 427-8372. We'll send you a message from that number!"

    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": inbound,
            "message_id": "automated-routing-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["should_reply"] is False
    assert body["handoff_needed"] is False
    assert body["alert_needed"] is False
    assert sender.calls == []
    assert sheet.rows[2][10] == "Y"
    assert sheet.rows[2][13] == "handoff"
    assert sheet.rows[2][15] == "interested_no_call"
    assert sheet.rows[2][16] == "TRUE"
    assert sheet.rows[2][19] == "TRUE"


def test_sms_differentiation_reply_preserves_paragraphs_and_requests_hot_lead_alert(monkeypatch):
    module, _sheet, sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "How are you different from them with communication and documentation?"

    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": inbound,
            "message_id": "differentiation-alert-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["should_reply"] is False
    assert body["reply_text"] == ""
    assert body["handoff_needed"] is True
    assert body["alert_needed"] is True
    assert body["handoff_type"] == "HOT LEAD - DIFFERENTIATION QUESTION"
    assert sender.calls == []


def test_sms_substantive_question_that_would_repeat_answer_routes_to_handoff(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    repeated = "There is no cost to you or the seller. We get paid by the buyer at closing, and charge a flat fee for our service."
    decision = module._sms_apply_repeat_guard(
        module._sms_decision(reply_text=repeated, lead_status="Y"),
        {"last_outbound_text": repeated},
        "How is the buyer going to pay if they are losing money?",
    )

    assert decision["handoff_needed"] is True
    assert decision["block_reply"] is True
    assert decision["reply_text"] == ""
    assert decision["reason"] == "Agent asked a new substantive question after a similar prior answer"


def test_sms_relationship_only_disposition_persists_warm_closed_state(monkeypatch):
    module, sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    sheet.rows[2][9] = "I already have a negotiator"
    sheet.rows[2][12] = "Already represented / handled"
    sheet.rows[2][13] = "done"
    sheet.rows[2][17] = json.dumps([{"role": "agent", "text": "I already have a negotiator"}])
    sheet.rows[2][19] = "FALSE"
    client = TestClient(module.app)

    response = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": "Great to know. I'll keep your info",
            "message_id": "relationship-only-1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["lead_status"] == "O"
    assert body["conversation_done"] is True
    assert body["handoff_needed"] is False
    assert body["should_reply"] is True
    assert sheet.rows[2][10] == "O"
    assert sheet.rows[2][13] == "done"
    assert sheet.rows[2][15] == "warm_future_interest"
    assert sheet.rows[2][16] == "FALSE"


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
    assert body["reply_text"] == ""
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
    assert "lender side of the short sale" in body["reply_text"]

    response = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "reply_sent",
            "phone": "555-222-3333",
            "reply_text": body["reply_text"],
            "sent_at": "2026-07-08T09:11:00-04:00",
            "request_id": "reply-request-1",
            "message_id": "msg-help-1",
            "lease_token": "lease-help-1",
        },
    )

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert sheet.rows[3][11] == body["reply_text"]
    assert sheet.rows[3][18] == "1"
    assert "assistant" in sheet.rows[3][17]

    duplicate = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "reply_sent",
            "phone": "555-222-3333",
            "reply_text": body["reply_text"],
            "sent_at": "2026-07-08T09:11:04-04:00",
            "request_id": "reply-request-1",
            "message_id": "msg-help-1",
            "lease_token": "lease-help-1",
        },
    )
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True
    assert sheet.rows[3][18] == "1"
    assert json.loads(sheet.rows[3][17])[-1]["receipt_id"]


def test_sms_contract_fee_questions_follow_the_required_three_step_flow(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    first = module._sms_fast_decision({}, "What do you charge?")
    assert first["handoff_needed"] is False
    assert "flat fee to the buyer" in first["reply_text"]
    assert "$5,000" not in first["reply_text"]

    row = {"history_json": json.dumps([{"role": "assistant", "text": first["reply_text"]}])}
    second = module._sms_fast_decision(row, "Right, but how much is the fee exactly?")
    assert second["handoff_needed"] is False
    assert "$5,000" in second["reply_text"]

    row["history_json"] = json.dumps(
        [
            {"role": "assistant", "text": first["reply_text"]},
            {"role": "assistant", "text": second["reply_text"]},
        ]
    )
    third = module._sms_fast_decision(row, "Why is the fee that much?")
    assert third["handoff_needed"] is True
    assert third["block_reply"] is True
    assert third["reply_text"] == ""


def test_sms_contract_fee_negotiation_hands_off_without_repeating_price(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    decision = module._sms_fast_decision({}, "My company charges $3,995. Can you match that fee?")
    assert decision["handoff_needed"] is True
    assert decision["block_reply"] is True
    assert decision["handoff_type"] == "FEE NEGOTIATION"
    assert decision["reply_text"] == ""


def test_sms_contract_safe_three_part_question_gets_bounded_answer(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "I already have someone. What do you do, where are you located, and what is your fee?"
    decision = module._sms_fast_decision({}, inbound)
    assert decision["lead_status"] == "O"
    assert decision["handoff_needed"] is False
    assert decision["block_reply"] is False
    assert "Atlanta" in decision["reply_text"]
    assert "lender-side" in decision["reply_text"]
    assert "flat fee" in decision["reply_text"]


def test_sms_contract_self_handler_who_wants_details_is_not_closed_out(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "I am handling that part myself, but could you explain some more details? I am interested in hearing."
    decision = module._sms_fast_decision({}, inbound)
    assert decision["lead_status"] == "Y"
    assert decision["conversation_done"] is False
    assert "lender side of the short sale" in decision["reply_text"]
    assert "Ok, no problem" not in decision["reply_text"]


def test_sms_contract_direct_question_after_closeout_gets_answered_as_future_opportunity(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    row = {
        "ai_state": "done",
        "mailshake_status": "R",
        "call_booking_status": "closed_no_interest",
    }
    decision = module._sms_fast_decision(row, "Actually, are you local and what company are you with?")
    assert decision["lead_status"] == "O"
    assert "Atlanta" in decision["reply_text"]
    assert "Crisp Short Sales" in decision["reply_text"]


def test_sms_contract_courtesy_after_closeout_never_reopens(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    row = {"ai_state": "done", "mailshake_status": "R", "call_booking_status": "closed_no_interest"}
    decision = module._sms_fast_decision(row, "Thank you, I appreciate it")
    assert decision["block_reply"] is True
    assert decision["reply_text"] == ""
    assert decision["lead_status"] == "R"


def test_sms_contract_present_help_and_call_requests_are_terminal_handoffs(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    for inbound in ("I need help", "Let's talk", "Call me after 12 tomorrow"):
        decision = module._sms_fast_decision({}, inbound)
        assert decision["lead_status"] == "Y"
        assert decision["handoff_needed"] is True
        assert decision["block_reply"] is False
        assert decision["reply_text"]


def test_sms_contract_not_short_sale_and_source_question_are_distinct(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    closeout = module._sms_fast_decision({}, "This is definitely not a short sale. It is new construction.")
    assert closeout["lead_status"] == "R"
    assert closeout["reply_text"] == "Ahh, ok... thanks for letting me know. Good luck with your listing!"

    source = module._sms_fast_decision({}, "Why did you think it was a short sale?")
    assert source["lead_status"] == "R"
    assert source["reply_text"] == "I thought I saw it marked online as a short sale. My mistake if I misread it. Thanks."


def test_sms_contract_recent_duplicate_is_suppressed_but_old_repeat_is_not(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    row = {
        "last_inbound_text": "same message",
        "last_inbound_at": "2026-08-20T10:00:00-04:00",
    }
    assert module._sms_is_recent_duplicate_inbound(row, "same message", "2026-08-20T10:03:00-04:00") is True
    assert module._sms_is_recent_duplicate_inbound(row, "same message", "2026-08-20T10:06:00-04:00") is False


def test_sms_contract_send_information_request_uses_email_workflow(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    ask = module._sms_fast_decision({}, "I already have help, but please send me more information about your services.")
    assert ask["lead_status"] == "O"
    assert ask["reply_text"] == "Absolutely, I'd be happy to email you more information. What's the best email?"

    provided = module._sms_fast_decision({}, "Please email the info to agent@example.com")
    assert provided["reply_text"] == (
        "Absolutely, I'll email you more information shortly. Thanks for sending your email."
    )


def test_sms_contract_no_current_help_gets_conversational_service_explanation(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "Hi there, actually I do not have anyone helping starting process"

    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_has_no_current_short_sale_help(inbound) is True
    assert decision["lead_status"] == "Y"
    assert decision["handoff_needed"] is False
    assert decision["block_reply"] is False
    assert "lender paperwork, calls, follow-up, and negotiations through approval" in decision["reply_text"]
    assert decision["reply_text"].endswith("Would you like to go over everything briefly by phone?")
    assert module._sms_has_no_current_short_sale_help("I don't need help") is False
    assert module._sms_has_no_current_short_sale_help("I already have someone helping") is False


def test_sms_contract_title_company_confusion_gets_role_clarification(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    decision = module._sms_fast_decision(
        {},
        "I already have a title company I'm working for if that's what you mean",
    )
    assert decision["lead_status"] == "Y"
    assert decision["conversation_done"] is False
    assert decision["handoff_needed"] is False
    assert "Crisp isn't a title company" in decision["reply_text"]
    assert "lender-side short-sale" in decision["reply_text"]


def test_sms_contract_service_info_request_is_answered_before_email_followup(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    decision = module._sms_fast_decision(
        {"email": "agent@example.com"},
        "We are working with a HUD Housing Counselor, but I will take more info on your services just in case",
    )
    assert decision["lead_status"] == "O"
    assert decision["conversation_done"] is True
    assert decision["reply_text"] == (
        "Absolutely, I'll email you more information shortly. Thanks for sending your email."
    )


def test_sms_contract_regulatory_license_question_hands_off_without_ai_reply(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    decision = module._sms_fast_decision(
        {},
        "We are licensed debt adjusters, which is required by statute in New Hampshire. Are you licensed?",
    )
    assert decision["lead_status"] == "Y"
    assert decision["handoff_needed"] is True
    assert decision["block_reply"] is True
    assert decision["reply_text"] == ""
    assert decision["handoff_type"] == "COMPLIANCE / LICENSING QUESTION"

    attorney = module._sms_fast_decision({}, "Are you licensed as an attorney?")
    assert attorney["handoff_needed"] is False
    assert "not an attorney" in attorney["reply_text"]


def test_sms_contract_historical_compound_regressions_route_correctly(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )

    future = module._sms_fast_decision(
        {},
        "I have these handled, but future short sales are stacking up and I am interested in what you have to say.",
    )
    assert future["lead_status"] in {"O", "Y"}
    assert "marked online" not in future["reply_text"]

    review = module._sms_fast_decision(
        {},
        "I am handling it myself, but I am willing to review what you offer.",
    )
    assert "crispshortsales.com" not in review["reply_text"]
    assert "lender" in review["reply_text"].lower()

    percentage = module._sms_fast_decision({}, "What percentage do you collect? My current company takes 1%.")
    assert percentage["handoff_type"] != "STATS QUESTION"
    assert "flat fee" in percentage["reply_text"].lower()


def test_sms_contract_existing_crisp_and_fee_stays_r_handoff(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    decision = module._sms_fast_decision({}, "We already work with Yoni at Crisp. What is your fee?")
    assert decision["lead_status"] == "R"
    assert decision["handoff_needed"] is True
    assert decision["block_reply"] is True
    assert decision["reply_text"] == ""


def test_sms_contract_fee_with_present_help_or_call_answers_then_freezes(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    for inbound in (
        "Call me tomorrow at 10. What's your fee?",
        "I am handling it myself but would love help getting it approved quicker. How much do you charge?",
    ):
        decision = module._sms_fast_decision({}, inbound)
        assert decision["lead_status"] == "Y"
        assert decision["handoff_needed"] is True
        assert decision["block_reply"] is False
        assert "flat fee" in decision["reply_text"].lower()


def test_sms_contract_language_and_fee_compound_answers_both(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    decision = module._sms_fast_decision({}, "Do you speak Spanish and what is your fee?")
    assert decision["handoff_needed"] is False
    assert "don't speak Spanish" in decision["reply_text"]
    assert "flat fee" in decision["reply_text"].lower()


def test_sms_contract_ordinary_closeout_does_not_block_later_question(monkeypatch):
    module, sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    client = TestClient(module.app)
    first = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": "I already have help, thank you.",
            "message_id": "closeout-reopen-1",
        },
    ).json()
    assert first["lead_status"] == "R"
    assert sheet.rows[2][19] == "FALSE"

    second = client.post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": "Actually, what is your fee?",
            "message_id": "closeout-reopen-2",
        },
    ).json()
    assert second["should_reply"] is True
    assert second["lead_status"] == "O"
    assert "flat fee" in second["reply_text"].lower()


def test_sms_contract_manual_reply_is_terminal_takeover(monkeypatch):
    module, sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    response = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "manual_reply_sent",
            "phone": "+19542357723",
            "reply_text": "I can call you this afternoon.",
        },
    )
    assert response.status_code == 200
    assert response.json()["manual_takeover"] is True
    assert sheet.rows[2][13] == "handoff"
    assert sheet.rows[2][16] == "TRUE"
    assert sheet.rows[2][19] == "TRUE"


def test_sms_contract_reply_cap_always_hands_off_substantive_inbound(monkeypatch):
    module, sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    sheet.rows[2][18] = "3"
    body = TestClient(module.app).post(
        "/sms-chatbot",
        data={
            "token": "secret-token",
            "action": "incoming_sms",
            "phone": "+19542357723",
            "message": "What is your fee?",
            "message_id": "cap-handoff-1",
        },
    ).json()
    assert body["should_reply"] is False
    assert body["handoff_needed"] is True
    assert body["handoff_type"] == "MAX REPLIES REACHED"
    assert sheet.rows[2][19] == "TRUE"


def test_sms_contract_source_contains_retry_and_truthfulness_guards():
    chatbot = (ROOT / "apps_script" / "sms_chatbot.js").read_text()
    outbox = (ROOT / "apps_script" / "sms_outbox.js").read_text()
    unified = (ROOT / "apps_script" / "zz_unified_post.js").read_text()
    assert "currentCount >= 3" in chatbot
    assert "SMS_CONVERSATION_LEASE_" in chatbot
    assert "You are not a chatbot. You are a real person texting." not in chatbot
    assert "never claim that a physical action" in chatbot
    assert "if (retryable) break;" in outbox
    register_pending = outbox.split("function registerPendingSmsSendV10_", 1)[1].split(
        "function claimPendingSmsSendV10_", 1
    )[0]
    assert "tryLock(3000)" in register_pending
    assert "SMS_PENDING_REGISTRATION_BUSY" in register_pending
    assert "saveSmsInboundDecisionSnapshotV10_" in outbox
    assert "waitLock(10000)" not in register_pending
    claim_pending = outbox.split("function claimPendingSmsSendV10_", 1)[1].split(
        "function markPendingSmsSendStartedV10_", 1
    )[0]
    assert "tryLock(3000)" in claim_pending
    assert "waitLock" not in claim_pending
    incoming_branch = unified.split('if (action === "incoming_sms")', 1)[1].split(
        'if (action === "reply_sent")', 1
    )[0]
    assert "enqueueIncomingSmsV10_(body, requestId)" in incoming_branch
    assert "handleIncomingSms_(body)" not in incoming_branch
    matched_branch = chatbot.split("if (ruleResult.matched)", 1)[1].split(
        "let decision = getAiDecision_", 1
    )[0]
    assert matched_branch.index("sendHandoffEmail_(") < matched_branch.index("updateRowFields_(sheet, row, updates)")
    assert "intent_contract_v3" in unified
    assert "function testSmsIntentContractV3_" in chatbot
    suppressor = unified.split("function shouldSuppressUnifiedDuplicateInbound_", 1)[1].split(
        "function markUnifiedInboundProcessed_", 1
    )[0]
    assert "cache.put" not in suppressor
