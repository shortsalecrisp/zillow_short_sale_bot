import importlib.machinery
import json
import logging
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

bot_min.jina_cached_search = lambda *args, **kwargs: []
bot_min._contact_enrichment = lambda *args, **kwargs: {}


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

    original_hints = bot_min.PROFILE_HINTS.copy()
    bot_min.PROFILE_HINTS = {"jon mccall|fl": ["https://team.example/contact"]}
    try:
        result = bot_min.lookup_email(
            "Jon McCall",
            "FL",
            {"zpid": "1", "city": "Hudson", "state": "FL"},
        )
    finally:
        bot_min.PROFILE_HINTS = original_hints

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
    assert result["source"] in {"rapid_contact", "rapid_listed_by", "rapid_email_authoritative"}


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

    assert queries[0].startswith('"Antonio Flores" "Real Estate Agent" email')
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


def test_fallback_contact_query_adds_contact_tokens():
    query = bot_min._fallback_contact_query(
        "Mary Agent",
        "FL",
        {"city": "Tampa", "brokerage": "Bright Homes"},
    )
    assert "mobile" in query
    assert "brighthomes.com" in query


def test_portal_jsonld_email_extracted(monkeypatch):
    portal_html = Path("tests/fixtures/portal_realtor.html").read_text()

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_email", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: (portal_html, "text/html"))

    bot_min.cache_e.clear()
    original_hints = bot_min.PROFILE_HINTS.copy()
    bot_min.PROFILE_HINTS = {"mary agent|fl": ["https://www.realtor.com/agent/mary-agent"]}

    try:
        result = bot_min.lookup_email(
            "Mary Agent",
            "FL",
            {"zpid": "0", "city": "Tampa", "state": "FL"},
        )
    finally:
        bot_min.PROFILE_HINTS = original_hints

    assert result["email"] == "mary.agent@kw.com"
    assert result["source"] in {"jsonld_person", "portal_struct", "mailto"}


def test_synthetic_email_created_when_search_empty(monkeypatch):
    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_email", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))
    monkeypatch.setattr(bot_min, "search_round_robin", lambda *args, **kwargs: [])

    bot_min.cache_e.clear()
    old_flag = bot_min.ENABLE_SYNTH_EMAIL_FALLBACK
    bot_min.ENABLE_SYNTH_EMAIL_FALLBACK = True

    try:
        result = bot_min.lookup_email(
            "John Doe",
            "TX",
            {"zpid": "", "city": "Austin", "state": "TX", "brokerage": "Bright Homes"},
        )
    finally:
        bot_min.ENABLE_SYNTH_EMAIL_FALLBACK = old_flag

    assert result["email"].startswith("john.doe@brighthomes.com")
    assert result["source"] in {"synthetic_pattern", "pattern"}


def test_enrich_contact_allows_brokerage_domains_without_hint(monkeypatch):
    monkeypatch.setattr(bot_min, "_contact_enrichment", bot_min.enrich_contact)
    monkeypatch.setattr(bot_min, "_rapid_from_payload", lambda payload: {})
    monkeypatch.setattr(bot_min, "_rapid_profile_urls", lambda rapid: [])
    monkeypatch.setattr(
        bot_min,
        "jina_cached_search",
        lambda *args, **kwargs: ["https://remax.com/agents/jane-agent"],
    )

    def fake_fetch(url):
        html = """
        <html><body>
        <h1>Jane Agent</h1>
        <a href="mailto:jane.agent@remax.com">Email Jane</a>
        </body></html>
        """
        return html, True

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(
        bot_min,
        "fetch_text_cached",
        lambda url: {"extracted_text": "", "final_url": url},
    )

    bot_min.cache_e.clear()

    result = bot_min.enrich_contact(
        "Jane Agent",
        "FL",
        {"zpid": "", "city": "Tampa", "state": "FL", "brokerageName": "RE/MAX Anchor Realty"},
    )

    assert result["best_email"] == "jane.agent@remax.com"


def test_lookup_email_logs_diagnostics_on_empty(monkeypatch, caplog):
    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_email", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))
    monkeypatch.setattr(bot_min, "_contact_search_urls", lambda *args, **kwargs: ([], True, "blocked"))

    bot_min.cache_e.clear()
    bot_min._cse_last_state = "blocked"
    old_flag = bot_min.ENABLE_SYNTH_EMAIL_FALLBACK
    bot_min.ENABLE_SYNTH_EMAIL_FALLBACK = False

    with caplog.at_level(logging.WARNING):
        result = bot_min.lookup_email(
            "Sam Agent",
            "TX",
            {"zpid": "0", "city": "Austin", "state": "TX"},
        )
    bot_min.ENABLE_SYNTH_EMAIL_FALLBACK = old_flag

    assert result["email"] == ""
    assert any("cse_state=blocked" in rec.message for rec in caplog.records)
    assert any("search_empty=True" in rec.message for rec in caplog.records)


def test_enrich_contact_uses_js_rendered_contact_page(monkeypatch):
    monkeypatch.setattr(bot_min, "_contact_enrichment", bot_min.enrich_contact)
    monkeypatch.setattr(bot_min, "_rapid_from_payload", lambda payload: {})
    monkeypatch.setattr(bot_min, "_rapid_profile_urls", lambda rapid: [])
    captured = []

    def fake_fetch(url):
        captured.append(url)
        html = """
        <html><body>
        <h1>Alex Agent</h1>
        <a href="mailto:alex.agent@dynamic.com">Email</a>
        <a href="tel:5558889999">Call</a>
        </body></html>
        """
        return html, True

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(
        bot_min,
        "fetch_text_cached",
        lambda url: {"extracted_text": "", "final_url": url},
    )
    monkeypatch.setattr(
        bot_min,
        "jina_cached_search",
        lambda *args, **kwargs: ["https://dynamic.example/contact"],
    )

    bot_min.cache_e.clear()

    result = bot_min.enrich_contact(
        "Alex Agent",
        "WA",
        {"zpid": "", "city": "Seattle", "state": "WA"},
    )

    assert captured == ["https://dynamic.example/contact"]
    assert result["best_email"] == "alex.agent@dynamic.com"
    assert result["best_phone"] == "555-888-9999"


def test_ok_email_rejects_social_handles_and_needs_domain():
    assert not bot_min.ok_email("@j.ziegelbaum")
    assert not bot_min.ok_email("agent@instagram")
    assert bot_min.ok_email("agent@example.com")


def test_select_top_5_prioritizes_non_social_and_filters_low_signal(monkeypatch):
    items = [
        {"link": "https://www.instagram.com/reel/abc123"},
        {"link": "https://www.facebook.com/search/top?q=josh"},
        {"link": "https://agent-site.com/profile/josh-agent"},
        {"link": "https://broker.com/team/josh"},
        {"link": "https://homes.example.com/agents/josh"},
        {"link": "https://team.example.com/contact"},
        {"link": "https://www.facebook.com/josh.agent"},
    ]

    def fake_fetch(url, ttl_days=7):
        return {"extracted_text": "profile page", "http_status": 200, "final_url": url}

    monkeypatch.setattr(bot_min, "fetch_text_cached", fake_fetch)

    selected, rejected = bot_min.select_top_5_urls(
        items,
        fetch_check=True,
        property_state="",
        property_city="",
        limit=10,
    )

    assert len(selected) == 5
    assert not any("instagram.com/reel" in url for url in selected)
    assert not any("facebook.com/search" in url for url in selected)
    assert selected[:4] and all(
        not bot_min._is_social_root(bot_min._domain(url) or "") for url in selected[:4]
    )
