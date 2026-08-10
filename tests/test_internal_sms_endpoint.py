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
] + [f"extra_{idx}" for idx in range(21, 43)]
CHATBOT_HEADERS[37] = "callback_requested"
CHATBOT_HEADERS[38] = "callback_time"


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
            "phone": "555-111-2212",
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
    assert sheet.rows[12][7] == "x"
    assert sheet.rows[12][11] == (
        "Hey Alex, this is Yoni Kutler with Crisp Short Sales. "
        "I saw your short sale at 123 Main."
    )
    assert datetime.fromisoformat(sheet.rows[12][14]).tzinfo is not None
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
    assert response.json()["status"] == "sent"
    assert sender.calls[0]["row_idx"] == 16
    assert sheet.rows[16][7] == "x"
    assert sheet.rows[16][22]
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


def test_internal_initial_sms_ignores_later_duplicate_suppression_marker(monkeypatch):
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
    assert response.json()["status"] == "sent"
    assert sender.calls[0]["row_idx"] == 12
    assert sheet.rows[12][7] == "x"
    assert sheet.rows[12][42] == "x"


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


def test_sms_existing_crisp_client_exits_marketing_for_handoff(monkeypatch):
    module, _sheet, _sender = _import_webhook_server(
        monkeypatch,
        sender_result=FakeSendResult(success=True),
    )
    inbound = "I already have an active Crisp portal and am set up with Yoni."

    decision = module._sms_fast_decision({}, inbound)

    assert module._sms_is_existing_crisp_relationship(inbound) is True
    assert decision["lead_status"] == "Y"
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
    assert generic_decision["lead_status"] == "R"
    assert generic_decision["conversation_done"] is True
    assert generic_decision["handoff_needed"] is False
    assert generic_decision["block_reply"] is False
    assert generic_decision["reply_text"].startswith("Ok, no problem.")

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
