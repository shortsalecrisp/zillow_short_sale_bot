import json
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


def test_lookup_email_accepts_generic_team_when_only_option(monkeypatch):
    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_email", lambda *args, **kwargs: ["query"])
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://team.example"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])

    def fake_fetch(url):
        return (
            """
            <html><body>
            <h1>Jon McCall</h1>
            <p>Hudson, FL short sale experts</p>
            <a href="mailto:team@jonmccallteam.com">Email the team</a>
            </body></html>
            """,
            "text/html",
        )

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)

    bot_min.cache_e.clear()

    result = bot_min.lookup_email(
        "Jon McCall",
        "FL",
        {"zpid": "1", "city": "Hudson", "state": "FL"},
    )

    assert result["email"] == "team@jonmccallteam.com"
    assert result["confidence"] in {"low", "high"}


def test_rapid_email_used_when_personal_missing(monkeypatch):
    contact_email = "patmurray@dwellchicago.com"

    def fake_rapid_property(zpid):
        return {
            "contact_recipients": [
                {
                    "display_name": "Pat M.",
                    "emails": [contact_email],
                }
            ]
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_email", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))

    bot_min.cache_e.clear()

    result = bot_min.lookup_email(
        "Pat Murray",
        "IL",
        {
            "zpid": "12345",
            "city": "Naperville",
            "state": "IL",
            "brokerage": "Dwell Chicago",
        },
    )

    assert result["email"] == contact_email
    assert result["confidence"] in {"low", "high"}
    assert result["source"] in {"rapid_contact", "rapid_listed_by"}


def test_unrelated_generic_email_still_withheld(monkeypatch):
    generic_email = "info@randommail.com"

    def fake_rapid_property(zpid):
        return {
            "contact_recipients": [
                {
                    "display_name": "Unknown Team",
                    "emails": [generic_email],
                }
            ]
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_email", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))

    bot_min.cache_e.clear()

    result = bot_min.lookup_email(
        "Jane Agent",
        "TX",
        {"zpid": "999", "city": "Austin", "state": "TX", "brokerage": "Sun Homes"},
    )

    assert result["email"] == ""
    assert result["confidence"] == ""
    assert result["reason"] in {"withheld_low_conf_mix", "no_personal_email"}


def test_build_q_email_includes_locality_tokens():
    queries = bot_min.build_q_email(
        "Antonio Flores",
        "TX",
        brokerage="Flores Realty Group",
        city="Seguin",
        postal_code="78155",
    )

    assert queries[0].startswith('"Antonio Flores" Seguin TX 78155')
    assert any("Seguin" in q for q in queries)
    assert any("Flores Realty Group" in q for q in queries)


def test_lookup_email_uses_override(monkeypatch):
    override_payload = {"jane agent|CA": {"email": "jane@example.com"}}
    monkeypatch.setenv("CONTACT_OVERRIDE_JSON", json.dumps(override_payload))

    bot_min.cache_e.clear()
    bot_min.cache_p.clear()
    bot_min._contact_override_cache = {"raw": None, "map": {}}

    def fail(*args, **kwargs):
        raise AssertionError("should short-circuit before scraping")

    monkeypatch.setattr(bot_min, "rapid_property", fail)
    monkeypatch.setattr(bot_min, "build_q_email", fail)
    monkeypatch.setattr(bot_min, "google_items", fail)
    monkeypatch.setattr(bot_min, "pmap", fail)
    monkeypatch.setattr(bot_min, "fetch_contact_page", fail)

    result = bot_min.lookup_email(
        "Jane Agent",
        "CA",
        {"zpid": "123", "city": "Los Angeles", "state": "CA"},
    )

    assert result["email"] == "jane@example.com"
    assert result["confidence"] == "high"
    assert result["source"] == "override"


def test_is_generic_email_flags_placeholder():
    assert bot_min._is_generic_email("name@yoursite.com")


def test_is_generic_email_allows_real_agent_email():
    assert not bot_min._is_generic_email("jane.doe@realtyworld.com")
