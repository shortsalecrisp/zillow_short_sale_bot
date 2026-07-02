import importlib.machinery
import os
import sys
import types
from pathlib import Path
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("GOOGLE_API_KEY", "test")
os.environ.setdefault("GOOGLE_CX", "test")
os.environ.setdefault("GSHEET_ID", "test_sheet")
os.environ.setdefault("GCP_SERVICE_ACCOUNT_JSON", "{}")
os.environ.setdefault("SMS_GATEWAY_API_KEY", "dummy")


dummy_sheet = types.SimpleNamespace(col_values=lambda idx: [], row_count=2)
dummy_workbook = types.SimpleNamespace(sheet1=dummy_sheet, worksheet=lambda name: dummy_sheet)
dummy_client = types.SimpleNamespace(open_by_key=lambda key: dummy_workbook)

sys.modules["gspread"] = types.SimpleNamespace(authorize=lambda creds: dummy_client)

fake_openai = types.SimpleNamespace(__spec__=importlib.machinery.ModuleSpec("openai", None))
sys.modules["openai"] = fake_openai


class _DummyCreds:
    @staticmethod
    def from_service_account_info(info, scopes=None):
        return _DummyCreds()


discovery_module = types.ModuleType("googleapiclient.discovery")
discovery_module.build = lambda *args, **kwargs: object()

googleapiclient_module = types.ModuleType("googleapiclient")
googleapiclient_module.discovery = discovery_module
sys.modules["googleapiclient"] = googleapiclient_module
sys.modules["googleapiclient.discovery"] = discovery_module

service_account_module = types.ModuleType("google.oauth2.service_account")
service_account_module.Credentials = _DummyCreds

sys.modules.setdefault("google", types.ModuleType("google"))
oauth2_module = sys.modules.setdefault("google.oauth2", types.ModuleType("google.oauth2"))
setattr(oauth2_module, "service_account", service_account_module)
sys.modules["google.oauth2.service_account"] = service_account_module

import bot_min


class _FakeRequest:
    def __init__(self, payload):
        self._payload = payload

    def execute(self):
        return self._payload


class _FailingRequest:
    def __init__(self, exc):
        self._exc = exc

    def execute(self):
        raise self._exc


class _FakeValuesAPI:
    def __init__(self):
        self._ranges = []
        self._batch_updates = []

    def batchGet(self, spreadsheetId, ranges, majorDimension, valueRenderOption):
        self._ranges.extend(ranges)
        if majorDimension == "COLUMNS":
            return _FakeRequest(
                {
                    "valueRanges": [
                        {"values": [["2024-01-01T10:00:00-05:00"]]},
                        {"values": [[]]},
                    ]
                }
            )
        if majorDimension == "ROWS":
            row = [""] * bot_min.MIN_COLS
            row[bot_min.COL_FIRST] = "Sam"
            row[bot_min.COL_PHONE] = "5550001111"
            row[bot_min.COL_STREET] = "123 Main St"
            row[bot_min.COL_INIT_TS] = "2024-01-01T10:00:00-05:00"
            return _FakeRequest({"valueRanges": [{"values": [row]}]})
        raise AssertionError(f"Unexpected batchGet: {ranges}")

    def get(self, spreadsheetId, range, majorDimension, valueRenderOption):
        self._ranges.append(range)
        if range.endswith("!C2:C2"):
            return _FakeRequest({"values": [["5559998888"]]})
        raise AssertionError(f"Unexpected range: {range}")

    def batchGet(self, spreadsheetId, ranges, majorDimension, valueRenderOption):
        self._ranges.extend(ranges)
        init_col = bot_min._col_index_to_letter(bot_min.COL_INIT_TS)
        fu_flag_col = bot_min._col_index_to_letter(bot_min.COL_REPLY_FLAG)
        row_range = f"{bot_min.GSHEET_TAB}!A2:{bot_min.FOLLOWUP_READ_END_COL}2"

        if ranges == [
            f"{bot_min.GSHEET_TAB}!{init_col}2:{init_col}2",
            f"{bot_min.GSHEET_TAB}!{fu_flag_col}2:{fu_flag_col}2",
        ]:
            return _FakeRequest({
                "valueRanges": [
                    {"values": [["2024-01-01T10:00:00-05:00"]]},
                    {"values": [[""]]},
                ]
            })

        if ranges == [row_range]:
            row = [""] * bot_min.MIN_COLS
            row[bot_min.COL_FIRST] = "Sam"
            row[bot_min.COL_PHONE] = "5550001111"
            row[bot_min.COL_STREET] = "123 Main St"
            row[bot_min.COL_INIT_TS] = "2024-01-01T10:00:00-05:00"
            return _FakeRequest({"valueRanges": [{"values": [row]}]})

        raise AssertionError(f"Unexpected ranges: {ranges}")

    def batchUpdate(self, spreadsheetId, body):
        self._batch_updates.append(body)
        return _FakeRequest({"updated": True})


class _FakeSheetsService:
    def __init__(self):
        self.values_api = _FakeValuesAPI()

    def spreadsheets(self):
        return self

    def values(self):
        return self.values_api


def test_follow_up_uses_latest_sheet_phone(monkeypatch):
    fake_service = _FakeSheetsService()
    sent = {}

    monkeypatch.setattr(bot_min, "sheets_service", fake_service)
    monkeypatch.setattr(bot_min, "ws", types.SimpleNamespace(row_count=2))
    monkeypatch.setattr(bot_min, "check_reply", lambda *args, **kwargs: False)
    monkeypatch.setattr(bot_min, "business_hours_elapsed", lambda *args, **kwargs: bot_min.FU_HOURS)

    def _capture_send(phone, first, address, row_idx, follow_up=False):
        sent["phone"] = phone
        sent["row_idx"] = row_idx
        sent["follow_up"] = follow_up

    monkeypatch.setattr(bot_min, "send_sms", _capture_send)

    bot_min._follow_up_pass()

    assert sent["phone"] == "5559998888"
    assert sent["row_idx"] == 2
    assert sent["follow_up"] is True


class _MailshakeValuesAPI:
    def __init__(self, rows):
        self.rows = rows
        self.batch_updates = []

    def batchGet(self, spreadsheetId, ranges, majorDimension, valueRenderOption):
        value_ranges = []
        for col_range in ranges:
            col = col_range.split("!")[1].split("2:")[0]
            values = []
            for row in self.rows:
                values.append(row.get(col, ""))
            value_ranges.append({"values": [values]})
        return _FakeRequest({"valueRanges": value_ranges})

    def batchUpdate(self, spreadsheetId, body):
        self.batch_updates.append(body)
        return _FakeRequest({"updated": True})


class _MailshakeSheetsService:
    def __init__(self, rows):
        self.values_api = _MailshakeValuesAPI(rows)

    def spreadsheets(self):
        return self

    def values(self):
        return self.values_api


def test_mailshake_release_marks_due_followup_after_two_hour_grace(monkeypatch):
    now = bot_min.SCHEDULER_TZ.localize(datetime(2026, 7, 2, 16, 0, 0))
    due_ts = (now - timedelta(hours=2, minutes=5)).isoformat()
    recent_ts = (now - timedelta(hours=1, minutes=59)).isoformat()
    service = _MailshakeSheetsService([
        {"I": "x", "C": "5550001111", "J": "", "K": "", "X": due_ts},
        {"I": "x", "C": "5550002222", "J": "", "K": "", "X": recent_ts},
        {"I": "x", "C": "5550003333", "J": "", "K": "Y", "X": due_ts},
        {"I": "x", "C": "5550004444", "J": "manual", "K": "", "X": due_ts},
    ])

    monkeypatch.setattr(bot_min, "sheets_service", service)
    monkeypatch.setattr(bot_min, "ws", types.SimpleNamespace(row_count=5))
    monkeypatch.setattr(bot_min, "check_reply", lambda *args, **kwargs: False)

    assert bot_min.release_due_followups_to_mailshake(now) == 1

    updates = service.values_api.batch_updates[0]["data"]
    assert updates == [{"range": f"{bot_min.GSHEET_TAB}!K2", "values": [["N"]]}]


def test_mailshake_release_skips_rows_with_sms_reply(monkeypatch):
    now = bot_min.SCHEDULER_TZ.localize(datetime(2026, 7, 2, 16, 0, 0))
    due_ts = (now - timedelta(hours=3)).isoformat()
    service = _MailshakeSheetsService([
        {"I": "x", "C": "5550001111", "J": "", "K": "", "X": due_ts},
    ])
    replied = []

    monkeypatch.setattr(bot_min, "sheets_service", service)
    monkeypatch.setattr(bot_min, "ws", types.SimpleNamespace(row_count=2))
    monkeypatch.setattr(bot_min, "check_reply", lambda *args, **kwargs: True)
    monkeypatch.setattr(bot_min, "mark_reply", lambda row_idx: replied.append(row_idx))

    assert bot_min.release_due_followups_to_mailshake(now) == 0
    assert replied == [2]
    assert service.values_api.batch_updates == []


def test_resolve_timestamp_columns_rejects_reserved_and_colliding_columns():
    init_idx, fu_idx, warnings = bot_min._resolve_timestamp_columns(
        bot_min.COL_ZPID,
        bot_min.COL_ZPID,
    )
    assert init_idx == bot_min.DEFAULT_COL_INIT_TS
    assert fu_idx == bot_min.DEFAULT_COL_FU_TS
    assert warnings


def test_parse_configured_col_index_rejects_non_a1_labels():
    idx, warning = bot_min._parse_configured_col_index(
        "TIMESTAMP",
        default_index=bot_min.DEFAULT_COL_INIT_TS,
        env_key="GSHEET_INIT_TS_COL",
        max_index=bot_min.MAX_CONFIGURABLE_TIMESTAMP_COL,
    )
    assert idx == bot_min.DEFAULT_COL_INIT_TS
    assert warning


def test_mark_sent_retries_transient_sheet_failure(monkeypatch):
    class _RetryValuesAPI:
        def __init__(self):
            self.calls = []

        def batchUpdate(self, spreadsheetId, body):
            self.calls.append((spreadsheetId, body))
            if len(self.calls) == 1:
                return _FailingRequest(RuntimeError("temporary sheet error"))
            return _FakeRequest({})

    class _RetrySheetsService:
        def __init__(self):
            self.values_api = _RetryValuesAPI()

        def spreadsheets(self):
            return self

        def values(self):
            return self.values_api

    fake_service = _RetrySheetsService()
    sleeps = []
    monkeypatch.setattr(bot_min, "sheets_service", fake_service)
    monkeypatch.setattr(bot_min.time, "sleep", lambda secs: sleeps.append(secs))

    assert bot_min.mark_sent(42, "msg-123") is True
    assert len(fake_service.values_api.calls) == 2
    assert sleeps == [1]

    body = fake_service.values_api.calls[-1][1]
    assert body["data"][0]["range"] == "Sheet1!H42"
    assert body["data"][0]["values"] == [["x"]]
    assert body["data"][1]["range"] == "Sheet1!W42"
    assert body["data"][2]["range"] == "Sheet1!L42"
    assert body["data"][2]["values"] == [["msg-123"]]
