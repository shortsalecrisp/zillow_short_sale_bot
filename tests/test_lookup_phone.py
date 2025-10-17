import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("GOOGLE_API_KEY", "test")
os.environ.setdefault("GOOGLE_CX", "test")
os.environ.setdefault("GSHEET_ID", "test_sheet")
os.environ.setdefault("GCP_SERVICE_ACCOUNT_JSON", "{}")
os.environ.setdefault("SMS_GATEWAY_API_KEY", "dummy")

dummy_sheet = types.SimpleNamespace(col_values=lambda idx: [])
dummy_workbook = types.SimpleNamespace(sheet1=dummy_sheet)
dummy_client = types.SimpleNamespace(open_by_key=lambda key: dummy_workbook)

sys.modules.setdefault("gspread", types.SimpleNamespace(authorize=lambda creds: dummy_client))


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


def test_realtor_office_label_cloudmersive_override(monkeypatch):
    """Ensure realtor.com style Office label stays when Cloudmersive marks mobile."""
    test_number = "555-867-5309"

    def fake_rapid_property(zpid):
        return {
            "contact_recipients": [
                {
                    "display_name": "Jane Agent",
                    "label": "Office:",
                    "phones": [
                        {"number": test_number},
                    ],
                }
            ]
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Jane Agent",
        "CA",
        {"zpid": "12345", "contact_recipients": []},
    )

    assert result["number"] == test_number
    assert result["confidence"] == "low"
    assert result["source"] == "rapid_contact"
    assert result["score"] >= bot_min.CONTACT_PHONE_LOW_CONF


def test_lookup_phone_allows_nickname_in_page_guard(monkeypatch):
    page_html = """
    <html>
        <body>
            <h1>Joshua "Josh" Sparber</h1>
            <p>Cell: (555) 010-0000</p>
            <p>Office: (555) 999-0000</p>
            <a href="tel:5550100000">Call Josh</a>
        </body>
    </html>
    """

    def fake_fetch(url):
        return page_html, "text/html"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: ["query"])
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://example.com/profile"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])
    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: "010-0000" in number)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Joshua M Sparber",
        "MN",
        {"zpid": "", "contact_recipients": []},
    )

    assert result["number"] == "555-010-0000"
    assert result["source"] == "agent_card_dom"


def test_lookup_email_allows_first_name_variants(monkeypatch):
    page_html = """
    <html>
        <body>
            <div>Meet Mike Johnson, your trusted agent.</div>
            <a href="mailto:mike@homes.com">Email Mike Johnson</a>
        </body>
    </html>
    """

    def fake_fetch(url):
        return page_html, "text/html"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(
        bot_min,
        "build_q_email",
        lambda agent, state, brokerage, domain_hint, mls_id: ["query"],
    )
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://example.com/profile"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])
    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)

    bot_min.cache_e.clear()

    result = bot_min.lookup_email(
        "Michael Johnson",
        "KY",
        {"zpid": "", "contact_recipients": []},
    )

    assert result["email"] == "mike@homes.com"
    assert result["source"] == "mailto"
