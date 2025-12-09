import os
import sys
import types
import sqlite3

import pytest

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# --- stub external dependencies before importing process_rows ---
os.environ.setdefault("OPENAI_API_KEY", "test")
os.environ.setdefault("APIFY_API_TOKEN", "test")

# gspread + oauth2client shims
_dummy_sheet = types.SimpleNamespace(append_row=lambda row: None)
_dummy_workbook = types.SimpleNamespace(sheet1=_dummy_sheet)
_dummy_client = types.SimpleNamespace(open_by_key=lambda key: _dummy_workbook)
sys.modules["gspread"] = types.SimpleNamespace(authorize=lambda creds: _dummy_client)

class _DummyCreds:
    @staticmethod
    def from_json_keyfile_name(*args, **kwargs):
        return object()

service_account_module = types.ModuleType("oauth2client.service_account")
service_account_module.ServiceAccountCredentials = _DummyCreds
oauth2client_module = types.ModuleType("oauth2client")
oauth2client_module.service_account = service_account_module
sys.modules["oauth2client"] = oauth2client_module
sys.modules["oauth2client.service_account"] = service_account_module

# SMS + OpenAI shims
sms_module = types.ModuleType("sms_providers")
sms_module.get_sender = lambda provider: types.SimpleNamespace(send=lambda to, body: None)
sys.modules["sms_providers"] = sms_module

openai_module = types.SimpleNamespace(
    chat=types.SimpleNamespace(
        completions=types.SimpleNamespace(
            create=lambda **kwargs: types.SimpleNamespace(
                choices=[types.SimpleNamespace(message=types.SimpleNamespace(content="{}"))]
            )
        )
    )
)
sys.modules["openai"] = openai_module

from importlib.machinery import SourceFileLoader

process_rows = SourceFileLoader("process_rows", str(ROOT / "process_rows")).load_module()


def _cache_conn():
    conn = sqlite3.connect(":memory:")
    process_rows._init_cache(conn)
    return conn


def test_dynamic_whitelist_from_detail_url(monkeypatch):
    queries = []

    def fake_scrape(q):
        queries.append(q)
        return ["https://vivorealty.com/contact"]

    class FakeResp:
        def __init__(self, text):
            self.text = text

    monkeypatch.setattr(process_rows, "_scrape_google", fake_scrape)
    monkeypatch.setattr(process_rows, "_parse_detail_contact", lambda url: (None, None, False))
    monkeypatch.setattr(
        process_rows.requests,
        "get",
        lambda url, timeout=None, headers=None: FakeResp("<html>Cell 555-101-2020</html>"),
    )

    cache = _cache_conn()
    phone, email = process_rows.find_contact(
        {
            "agentName": "Alex Agent",
            "address": "123 Road, Dallas TX",
            "detailUrl": "https://listings.vivorealty.com/property/1",
        },
        cache,
    )

    assert phone == "555-101-2020"
    assert email is None
    assert any("site:vivorealty.com" in q for q in queries)


def test_brokerage_domain_preferred_over_portal(monkeypatch):
    class FakeResp:
        def __init__(self, text):
            self.text = text

    def fake_scrape(q):
        return [
            "https://vivorealty.com/agent",
            "https://homes.com/listing",
        ]

    def fake_get(url, timeout=None, headers=None):
        if "vivorealty.com" in url:
            return FakeResp("<html>Call Cell 555-222-3333</html>")
        return FakeResp("<html>Phone 555-000-9999</html>")

    monkeypatch.setattr(process_rows, "_scrape_google", fake_scrape)
    monkeypatch.setattr(process_rows, "_parse_detail_contact", lambda url: (None, None, False))
    monkeypatch.setattr(process_rows.requests, "get", fake_get)

    cache = _cache_conn()
    phone, _ = process_rows.find_contact(
        {
            "agentName": "Alex Agent",
            "address": "123 Road, Dallas TX",
            "detailUrl": "https://listings.vivorealty.com/property/1",
        },
        cache,
    )

    assert phone == "555-222-3333"


def test_office_number_deprioritized(monkeypatch):
    class FakeResp:
        def __init__(self, text):
            self.text = text

    def fake_scrape(q):
        return [
            "https://broker.com/office",
            "https://broker.com/mobile",
        ]

    def fake_get(url, timeout=None, headers=None):
        if "office" in url:
            return FakeResp("<html>Office 555-111-2222</html>")
        return FakeResp("<html>Cell 555-333-4444</html>")

    monkeypatch.setattr(process_rows, "_scrape_google", fake_scrape)
    monkeypatch.setattr(process_rows, "_parse_detail_contact", lambda url: (None, None, False))
    monkeypatch.setattr(process_rows.requests, "get", fake_get)

    cache = _cache_conn()
    phone, _ = process_rows.find_contact(
        {
            "agentName": "Alex Agent",
            "address": "123 Road, Dallas TX",
            "detailUrl": "https://listings.broker.com/property/1",
        },
        cache,
    )

    assert phone == "555-333-4444"
