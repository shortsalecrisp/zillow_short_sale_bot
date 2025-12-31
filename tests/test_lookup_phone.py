import importlib.machinery
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

import pytest

import bot_min

bot_min.jina_cached_search = lambda *args, **kwargs: []
bot_min.search_round_robin = lambda *args, **kwargs: []
bot_min._contact_enrichment = lambda *args, **kwargs: {}


def test_build_q_phone_prefers_locality_tokens():
    queries = bot_min.build_q_phone(
        "Antonio Flores",
        "TX",
        city="Seguin",
        postal_code="78155",
        brokerage="Flores Realty Group",
    )

    assert queries[0].startswith('"Antonio Flores" "Real Estate Agent" "Mobile" TX')
    assert any("Flores Realty Group" in q for q in queries)
    assert any("Seguin" in q for q in queries)


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
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(
        bot_min,
        "_looks_direct",
        lambda number, agent, state, tries=2: number == test_number,
    )

    bot_min.cache_p.clear()
    bot_min._line_type_cache.clear()
    bot_min._line_type_verified.clear()
    bot_min._line_type_cache.clear()
    bot_min._line_type_verified.clear()

    result = bot_min.lookup_phone(
        "Jane Agent",
        "CA",
        {"zpid": "12345", "contact_recipients": []},
    )

    assert result["number"] == test_number
    assert result["confidence"] in {"low", "high"}
    assert result["source"].startswith("rapid_contact")
    assert result["score"] >= bot_min.CONTACT_PHONE_LOW_CONF


def test_rapid_mobile_recovers_from_office_demote(monkeypatch):
    mobile_number = "555-321-9999"

    def fake_rapid_property(zpid):
        return {
            "listed_by": {
                "display_name": "Main Office",
                "label": "Office",
                "phones": [{"number": mobile_number}],
            }
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: None)

    bot_min.cache_p.clear()
    bot_min._line_type_cache.clear()
    bot_min._line_type_verified.clear()

    result = bot_min.lookup_phone(
        "Pat Murray",
        "IL",
        {"zpid": "999", "contact_recipients": []},
    )

    assert result["number"] == mobile_number
    assert result["confidence"] in {"low", "high"}
    assert result["source"].startswith("rapid_listed_by")
    assert result["score"] >= bot_min.CONTACT_PHONE_LOW_CONF


def test_rapid_likely_mobile_kept(monkeypatch):
    rapid_number = "555-303-9090"
    cm_info = {
        "valid": True,
        "mobile": True,
        "mobile_verified": False,
        "type": "FixedLineOrMobile",
        "ambiguous_mobile": True,
    }

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {"contact_recipients": [{"phones": [{"number": rapid_number}]}]})
    monkeypatch.setattr(bot_min, "get_line_info", lambda phone: cm_info)

    bot_min._rapid_contact_cache.clear()
    bot_min._rapid_logged.clear()
    bot_min._rapid_cache.clear()

    snapshot = bot_min._rapid_contact_snapshot("Jane Agent", {"zpid": "abc"})

    assert snapshot["selected_phone"] == rapid_number
    assert snapshot["phone_reason"] == "rapid_cloudmersive_likely_mobile"


def test_rapid_candidate_not_dropped_on_invalid(monkeypatch):
    valid_number = "479-305-2241"
    invalid_number = "1563-118-1910"

    def fake_line_info(phone):
        if phone == valid_number:
            return {
                "valid": True,
                "mobile": False,
                "mobile_verified": False,
                "type": "FixedLineOrMobile",
                "ambiguous_mobile": True,
            }
        return {
            "valid": False,
            "mobile": False,
            "mobile_verified": False,
            "type": "Unknown",
            "ambiguous_mobile": False,
        }

    monkeypatch.setattr(bot_min, "get_line_info", fake_line_info)

    phone, reason = bot_min._rapid_select_phone(
        "Patricia Padilla",
        [
            {"value": valid_number},
            {"value": invalid_number},
        ],
    )

    assert phone == valid_number
    assert reason.startswith("rapid_cloudmersive_candidate_mobile")


def test_lookup_phone_prefers_non_office_mobile(monkeypatch):
    office_number = "555-000-1111"
    mobile_number = "555-222-3333"

    def fake_rapid_property(zpid):
        return {
            "contact_recipients": [
                {
                    "display_name": "Jane Agent",
                    "label": "Cell",
                    "phones": [
                        {"number": mobile_number},
                    ],
                }
            ]
        }

    monkeypatch.setattr(
        bot_min,
        "rapid_property",
        fake_rapid_property,
    )
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))

    def fake_is_mobile(number):
        return number in {office_number, mobile_number}

    monkeypatch.setattr(bot_min, "is_mobile_number", fake_is_mobile)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()
    bot_min._line_type_cache.clear()
    bot_min._line_type_verified.clear()

    result = bot_min.lookup_phone(
        "Jane Agent",
        "CA",
        {
            "zpid": "12345",
            "contact_recipients": [
                {
                    "display_name": "Jane Agent",
                    "label": "Office",
                    "phones": [
                        {"number": office_number},
                    ],
                }
            ],
            "city": "Los Angeles",
            "state": "CA",
        },
    )

    assert result["number"] == mobile_number
    assert result["source"].startswith("rapid_contact")


def test_trusted_domain_office_number_demoted(monkeypatch):
    office_number = "555-111-2222"
    mobile_number = "555-333-4444"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: ["query", "query2"])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])
    monkeypatch.setattr(
        bot_min,
        "google_items",
        lambda query: [
            {
                "link": "https://pavelmartynenko.com/contact",
            }
        ],
    )

    def fake_fetch(url):
        return "<html><body>Call us</body></html>", False

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)

    def fake_extract(page):
        return [], [], [], {"tel": [], "title": "Contact"}

    monkeypatch.setattr(bot_min, "extract_struct", fake_extract)

    def fake_proximity_scan(text, first, last):
        return {
            office_number: {"snippets": ["Office"], "score": 1.0, "office": True},
            mobile_number: {"snippets": ["Cell"], "score": 1.0, "office": False},
        }

    monkeypatch.setattr(bot_min, "proximity_scan", fake_proximity_scan)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()
    bot_min._line_type_cache.clear()
    bot_min._line_type_verified.clear()

    original_hints = bot_min.PROFILE_HINTS.copy()
    bot_min.PROFILE_HINTS = {"pavel martynenko|fl": ["https://pavelmartynenko.com/contact"]}
    try:
        result = bot_min.lookup_phone(
            "Pavel Martynenko",
            "FL",
            {"zpid": "1", "city": "Miami", "state": "FL"},
        )
    finally:
        bot_min.PROFILE_HINTS = original_hints

    assert result["number"] == mobile_number
    assert result["source"] == "agent_card_dom"


def test_cloudmersive_boost_applied_to_mobile(monkeypatch):
    mobile_number = "555-999-0000"

    def fake_rapid_property(zpid):
        return {
            "contact_recipients": [
                {
                    "display_name": "Main Office",
                    "label": "Cell",
                    "phones": [
                        {"number": mobile_number},
                    ],
                }
            ]
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))
    monkeypatch.setattr(bot_min, "_names_match", lambda *args, **kwargs: False)

    def fake_is_mobile(number):
        bot_min._line_type_verified[number] = True
        return True

    monkeypatch.setattr(bot_min, "is_mobile_number", fake_is_mobile)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Jane Agent",
        "CA",
        {"zpid": "12345", "contact_recipients": []},
    )

    expected = (
        bot_min.PHONE_SOURCE_BASE["rapid_contact"]
        - 0.7
        + bot_min.CLOUDMERSIVE_MOBILE_BOOST
    )
    assert result["number"] == mobile_number
    assert result["score"] >= bot_min.CONTACT_PHONE_LOW_CONF


def test_trusted_contact_pages_not_penalized(monkeypatch):
    mobile_number = "555-101-2020"

    def fake_rapid_property(zpid):
        return {}

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: ["query", "query2"])

    monkeypatch.setattr(
        bot_min,
        "google_items",
        lambda query: [{"link": "https://trusted.test/contact"}],
    )

    def fake_fetch(url):
        return "<html><body>Jane Agent - CA <a href=\"tel:5551012020\">Call</a></body></html>", False

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])

    def fake_extract(page):
        return (
            [],
            [],
            [],
            {
                "tel": [{"phone": mobile_number, "context": "Call"}],
                "title": "Contact Us",
            },
        )

    monkeypatch.setattr(bot_min, "extract_struct", fake_extract)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    original_trusted = bot_min.TRUSTED_CONTACT_DOMAINS.copy()
    bot_min.TRUSTED_CONTACT_DOMAINS.add("trusted.test")

    bot_min.cache_p.clear()

    try:
        result = bot_min.lookup_phone(
            "Jane Agent",
            "CA",
            {"zpid": "abc", "city": "Los Angeles", "state": "CA"},
        )
    finally:
        bot_min.TRUSTED_CONTACT_DOMAINS = original_trusted

    assert result["number"] == mobile_number
    assert result["score"] >= bot_min.PHONE_SOURCE_BASE["agent_card_dom"]


def test_lookup_phone_rejects_invalid_cloudmersive_numbers(monkeypatch):
    bad_number = "040-135-5597"

    def fake_rapid_property(zpid):
        return {}

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])

    def fake_fetch(url):
        return f"<html><body>Jane Agent NY <a href=\"tel:{bad_number}\">Call</a></body></html>", False

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)

    def fake_extract(page):
        return (
            [],
            [],
            [],
            {"tel": [{"phone": bad_number, "context": "Office"}], "title": "Profile"},
        )

    monkeypatch.setattr(bot_min, "extract_struct", fake_extract)
    monkeypatch.setattr(bot_min, "get_line_info", lambda number: {"valid": False, "mobile": False, "country": "US"})
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()
    bot_min._line_info_cache.clear()

    result = bot_min.lookup_phone(
        "Jane Agent",
        "NY",
        {"zpid": "abc", "city": "Bronx", "state": "NY"},
    )

    assert result["number"] == ""
    assert result["score"] == 0.0


def test_cloudmersive_error_falls_back_to_local_validation(monkeypatch):
    phone = "216-403-9603"

    class DummyResp:
        status_code = 429

        def json(self):
            return {"Message": "Rate limit"}

    old_key = bot_min.CLOUDMERSIVE_KEY
    monkeypatch.setattr(bot_min, "CLOUDMERSIVE_KEY", "dummy")
    monkeypatch.setattr(bot_min.requests, "post", lambda *args, **kwargs: DummyResp())

    bot_min._line_info_cache.clear()

    try:
        info = bot_min.get_line_info(phone)
    finally:
        bot_min.CLOUDMERSIVE_KEY = old_key

    assert info["valid"] is True
    assert info["mobile"] is True
    assert info["country"] == "US"


def test_profile_hint_urls_are_used(monkeypatch):
    mobile_number = "555-303-4040"

    def fake_rapid_property(zpid):
        return {}

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])

    def fake_fetch(url):
        return "<html><body>Jane Agent - CA <a href=\"tel:5553034040\">Text</a></body></html>", False

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)

    def fake_extract(page):
        return (
            [],
            [],
            [],
            {
                "tel": [{"phone": mobile_number, "context": "Cell"}],
                "title": "Agent Profile",
            },
        )

    monkeypatch.setattr(bot_min, "extract_struct", fake_extract)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    original_hints = bot_min.PROFILE_HINTS.copy()
    bot_min.PROFILE_HINTS = {
        "jane agent|ca": ["https://hint.test/profile"],
    }

    original_trusted = bot_min.TRUSTED_CONTACT_DOMAINS.copy()
    bot_min.TRUSTED_CONTACT_DOMAINS.add("hint.test")
    bot_min.cache_p.clear()
    bot_min._line_type_cache.clear()
    bot_min._line_type_verified.clear()

    try:
        result = bot_min.lookup_phone(
            "Jane Agent",
            "CA",
            {"zpid": "abc", "city": "Los Angeles", "state": "CA"},
        )
    finally:
        bot_min.PROFILE_HINTS = original_hints
        bot_min.TRUSTED_CONTACT_DOMAINS = original_trusted

    assert result["number"] == mobile_number


def test_lookup_phone_mismatched_rapid_does_not_override(monkeypatch):
    office_number = "555-010-1010"

    def fake_rapid_property(zpid):
        return {
            "contact_recipients": [
                {
                    "display_name": "Main Office",
                    "label": "Office",
                    "phones": [
                        {"number": office_number},
                    ],
                }
            ]
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: False)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Faith Corbett",
        "NY",
        {"zpid": "30768362", "contact_recipients": []},
    )

    assert result["number"] == ""
    assert result["reason"] == "withheld_low_conf_mix"


def test_lookup_phone_continues_search_after_nonproductive_page(monkeypatch):
    office_number = "555-111-2222"
    mobile_number = "555-333-4444"

    def fake_rapid_property(zpid):
        return {}

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: ["query", "query2"])

    google_results = [
        [],
        [
            {"link": "https://independent-broker.test/office"},
            {"link": "https://independent-broker.test/mobile"},
        ],
    ]

    monkeypatch.setattr(bot_min, "google_items", lambda query: google_results.pop(0))
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])
    monkeypatch.setattr(
        bot_min,
        "_contact_search_urls",
        lambda *args, **kwargs: (
            ["https://independent-broker.test/office", "https://independent-broker.test/mobile"],
            False,
            "ok",
        ),
    )

    calls = []

    def fake_fetch(url):
        calls.append(url)
        if url.endswith("/office"):
            page = """
            <html><body><h1>Jane Agent</h1><p>Meet our team.</p><p>Serving Seattle, WA.</p></body></html>
            """
            return page, "text/html"
        page = """
        <html><body><h1>Jane Agent</h1><p>Cell: (555) 333-4444</p><p>Based in Seattle, WA.</p></body></html>
        """
        return page, "text/html"

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: number == mobile_number)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Jane Agent",
        "WA",
        {"zpid": "12345", "contact_recipients": []},
    )

    assert calls == [
        "https://independent-broker.test/office",
        "https://independent-broker.test/mobile",
    ]
    assert result["number"] == mobile_number
    assert result["source"] == "agent_card_dom"


def test_lookup_phone_requires_location_cue(monkeypatch):
    wrong_number_raw = "(555) 101-2020"
    right_number_raw = "(555) 999-8888"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: ["query"])
    monkeypatch.setattr(bot_min, "google_items", lambda query: [
        {"link": "https://wrong.example"},
        {"link": "https://right.example"},
    ])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])

    pages = {
        "https://wrong.example": (
            """
            <html><body>
            <h1>Lisa Dean</h1>
            <p>Serving Austin, TX buyers.</p>
            <p>Call {wrong}</p>
            </body></html>
            """.format(wrong=wrong_number_raw),
            "text/html",
        ),
        "https://right.example": (
            """
            <html><body>
            <h1>Lisa Dean</h1>
            <p>Tampa FL short sale specialist.</p>
            <p>Call {right}</p>
            <a href="tel:5559998888">Call Lisa Dean</a>
            </body></html>
            """.format(right=right_number_raw),
            "text/html",
        ),
    }

    fetched = []

    def fake_fetch(url):
        fetched.append(url)
        return pages[url]

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Lisa Dean",
        "FL",
        {"zpid": "1", "city": "Tampa", "state": "FL"},
    )

    assert fetched == ["https://wrong.example", "https://right.example"]
    assert result["number"] == bot_min.fmt_phone(right_number_raw)


def test_lookup_phone_unlabeled_number_with_full_name(monkeypatch):
    target_number = "708-407-4942"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: ["query"])
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://name-only.example"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])

    def fake_fetch(url):
        return (
            """
            <html><body>
            <h1>Ola Sanni</h1>
            <p>Ola Sanni 708-407-4942</p>
            <p>Serving Chicago, IL short sale owners.</p>
            </body></html>
            """,
            "text/html",
        )

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Ola Sanni",
        "IL",
        {"zpid": "1", "city": "Chicago", "state": "IL"},
    )

    assert result["number"] == target_number
    assert result["source"] == "agent_card_dom"


def test_lookup_phone_team_context_not_demoted(monkeypatch):
    office_number = "555-000-1111"
    mobile_number = "555-222-3333"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: ["query"])
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://team.example"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])

    def fake_fetch(url):
        return (
            """
            <html><body>
            <h1>Kristina Bartlett</h1>
            <p>Rachel Holland Team direct line: {mobile}</p>
            <p>Main Office: {office}</p>
            <p>Serving Spokane, WA homeowners.</p>
            </body></html>
            """.format(mobile=mobile_number, office=office_number),
            "text/html",
        )

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Kristina Bartlett",
        "WA",
        {"zpid": "1", "city": "Spokane", "state": "WA"},
    )

    assert result["number"] == mobile_number


def test_lookup_phone_penalizes_template_number(monkeypatch):
    template_number = "214-748-3641"
    direct_number = "352-725-7206"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: ["query"])
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://contact.example"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])

    def fake_fetch(url):
        return (
            """
            <html><body>
            <h1>Jon McCall</h1>
            <p>Hudson, FL short sale listings</p>
            <p>Office: {template}</p>
            <p>Cell: {direct}</p>
            <a href="tel:3527257206">Call Jon</a>
            </body></html>
            """.format(template=template_number, direct=direct_number),
            "text/html",
        )

    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(
        bot_min,
        "_looks_direct",
        lambda number, agent, state: number != template_number,
    )

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Jon McCall",
        "FL",
        {"zpid": "1", "city": "Hudson", "state": "FL"},
    )

    assert result["number"] == direct_number


def test_lookup_phone_uses_lower_mobile_override_threshold(monkeypatch):
    office_number = "555-555-0100"
    mobile_number = "555-777-8888"

    def fake_rapid_property(zpid):
        return {
            "contact_recipients": [
                {
                    "display_name": "Jane Agent",
                    "label": "Cell",
                    "phones": [
                        {"number": mobile_number},
                    ],
                }
            ]
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))

    def fake_is_mobile(number):
        return True

    monkeypatch.setattr(bot_min, "is_mobile_number", fake_is_mobile)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()

    payload = {
        "zpid": "12345",
        "contact_recipients": [
            {
                "display_name": "Jane Agent",
                "label": "Office",
                "phones": [
                    {"number": office_number},
                ],
            }
        ],
        "city": "Seattle",
        "state": "WA",
    }

    result = bot_min.lookup_phone("Jane Agent", "WA", payload)

    assert result["number"] == mobile_number
    assert result["source"].startswith("rapid_contact")


def test_lookup_phone_prefers_mobile_over_high_scoring_office(monkeypatch):
    office_number = "555-222-0101"
    mobile_number = "555-222-0202"

    def fake_rapid_property(zpid):
        return {
            "contact_recipients": [
                {
                    "display_name": "Jane Agent",
                    "label": "Cell",
                    "phones": [
                        {"number": mobile_number},
                    ],
                }
            ]
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))

    def fake_is_mobile(number):
        return number == mobile_number

    monkeypatch.setattr(bot_min, "is_mobile_number", fake_is_mobile)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()

    payload = {
        "zpid": "12345",
        "contact_recipients": [
            {
                "display_name": "Jane Agent",
                "label": "Office",
                "phones": [
                    {"number": office_number},
                ],
            }
        ],
        "city": "Seattle",
        "state": "WA",
    }

    result = bot_min.lookup_phone("Jane Agent", "WA", payload)

    assert result["number"] == mobile_number
    assert result["source"].startswith("rapid_contact")

def test_lookup_phone_allows_nickname_in_page_guard(monkeypatch):
    page_html = """
    <html>
        <body>
            <h1>Joshua "Josh" Sparber</h1>
            <p>Cell: (555) 010-0000</p>
            <p>Office: (555) 999-0000</p>
            <p>Serving Minneapolis, MN short sale clients.</p>
            <a href="tel:5550100000">Call Josh</a>
        </body>
    </html>
    """

    def fake_fetch(url):
        return page_html, "text/html"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state, **kwargs: ["query"])
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://example.com/profile"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])
    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: "010-0000" in number)

    bot_min.cache_p.clear()

    result = bot_min.lookup_phone(
        "Joshua M Sparber",
        "MN",
        {"zpid": "", "contact_recipients": [], "city": "Minneapolis", "state": "MN"},
    )

    assert result["number"] == "555-010-0000"
    assert result["source"] == "agent_card_dom"


def test_lookup_email_allows_first_name_variants(monkeypatch):
    page_html = """
    <html>
        <body>
            <div>Meet Mike Johnson, your trusted agent.</div>
            <p>Louisville, KY short sale specialist.</p>
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
        lambda agent, state, brokerage, domain_hint, mls_id, **kwargs: ["query"],
    )
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://example.com/profile"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])
    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)

    bot_min.cache_e.clear()

    original_hints = bot_min.PROFILE_HINTS.copy()
    bot_min.PROFILE_HINTS = {"michael johnson|ky": ["https://example.com/profile"]}
    try:
        result = bot_min.lookup_email(
            "Michael Johnson",
            "KY",
            {"zpid": "", "contact_recipients": [], "city": "Louisville", "state": "KY"},
        )
    finally:
        bot_min.PROFILE_HINTS = original_hints

    assert result["email"] == "mike@homes.com"
    assert result["source"] == "mailto"


def test_lookup_email_fallback_accepts_agent_match(monkeypatch):
    fake_email = "priscilla.perez-mcguire@remax.com"

    def fake_rapid_property(zpid):
        return {
            "listed_by": {
                "display_name": "Priscilla Perez-McGuire",
                "brokerageName": "RE/MAX Anchor Realty",
                "emails": [fake_email],
            }
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_email", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))
    monkeypatch.setattr(bot_min, "CONTACT_EMAIL_MIN_SCORE", 2.5)

    bot_min.cache_e.clear()

    result = bot_min.lookup_email(
        "Priscilla Perez-McGuire",
        "FL",
        {"zpid": "12345", "contact_recipients": []},
    )

    assert result["email"] == fake_email
    assert result["confidence"] in {"low", "high"}
    assert result["source"] in {"rapid_listed_by", "rapid_email_authoritative"}


def test_lookup_phone_uses_override(monkeypatch):
    override_payload = {"jane agent|CA": {"phone": "555-101-2020"}}
    monkeypatch.setenv("CONTACT_OVERRIDE_JSON", json.dumps(override_payload))

    bot_min.cache_p.clear()
    bot_min.cache_e.clear()
    bot_min._contact_override_cache = {"raw": None, "map": {}}

    def fail(*args, **kwargs):
        raise AssertionError("should short-circuit before scraping")

    monkeypatch.setattr(bot_min, "rapid_property", fail)
    monkeypatch.setattr(bot_min, "_rapid_from_payload", lambda row: {})
    monkeypatch.setattr(bot_min, "build_q_phone", fail)
    monkeypatch.setattr(bot_min, "pmap", fail)

    result = bot_min.lookup_phone("Jane Agent", "CA", {"zpid": "12345"})

    assert result["number"] == "555-101-2020"
    assert result["confidence"] == "high"
    assert result["source"] == "override"


def test_process_rows_surfaces_override(monkeypatch):
    override_payload = {"jane agent|CA": {"phone": "555-444-3333", "email": "jane@example.com"}}
    monkeypatch.setenv("CONTACT_OVERRIDE_JSON", json.dumps(override_payload))

    bot_min.cache_p.clear()
    bot_min.cache_e.clear()
    bot_min._contact_override_cache = {"raw": None, "map": {}}

    monkeypatch.setattr(bot_min, "is_short_sale", lambda *_: True)
    monkeypatch.setattr(bot_min, "is_active_listing", lambda *_: True)
    monkeypatch.setattr(bot_min, "phone_exists", lambda *_: False)

    def fail(*args, **kwargs):
        raise AssertionError("should short-circuit before scraping")

    monkeypatch.setattr(bot_min, "rapid_property", fail)
    monkeypatch.setattr(bot_min, "_rapid_from_payload", lambda row: {})
    monkeypatch.setattr(bot_min, "build_q_phone", fail)
    monkeypatch.setattr(bot_min, "build_q_email", fail)
    monkeypatch.setattr(bot_min, "google_items", fail)
    monkeypatch.setattr(bot_min, "pmap", fail)

    captured = {}

    def fake_append_row(row_vals):
        captured["row"] = row_vals
        return 42

    monkeypatch.setattr(bot_min, "append_row", fake_append_row)
    monkeypatch.setattr(bot_min, "send_sms", lambda *args, **kwargs: None)

    bot_min.process_rows(
        [
            {
                "description": "short sale listing", 
                "agentName": "Jane Agent",
                "state": "CA",
                "street": "123 Elm St",
                "city": "Los Angeles",
                "zpid": "abc",
            }
        ]
    )

    assert captured["row"][bot_min.COL_PHONE] == "555-444-3333"
    assert captured["row"][bot_min.COL_EMAIL] == "jane@example.com"
    assert captured["row"][bot_min.COL_PHONE_CONF] == "high"
    assert captured["row"][bot_min.COL_EMAIL_CONF] == "high"


def test_portal_mobile_number_extracted(monkeypatch):
    portal_html = Path("tests/fixtures/portal_exprealty.html").read_text()

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: (portal_html, "text/html"))
    monkeypatch.setattr(bot_min, "is_mobile_number", lambda number: True)
    monkeypatch.setattr(bot_min, "_looks_direct", lambda *args, **kwargs: True)

    bot_min.cache_p.clear()
    bot_min._line_type_cache.clear()
    bot_min._line_type_verified.clear()

    original_hints = bot_min.PROFILE_HINTS.copy()
    bot_min.PROFILE_HINTS = {
        "sam stone|fl": ["https://www.exprealty.com/agent/sam-stone"],
    }

    try:
        result = bot_min.lookup_phone(
            "Sam Stone",
            "FL",
            {"zpid": "", "contact_recipients": [], "city": "Orlando", "state": "FL"},
        )
    finally:
        bot_min.PROFILE_HINTS = original_hints

    assert result["number"] == bot_min.fmt_phone("555-333-2222")
    assert result["source"] in {"jsonld_person", "portal_struct", "agent_card_dom"}


def test_contact_search_urls_skip_portals(monkeypatch):
    captured_queries = []

    def fake_search_round_robin(queries, **kwargs):
        captured_queries.extend(list(queries))
        return [
            [
                (
                    "google_cse",
                    [
                        {"link": "https://independent.example"},
                        {"link": "https://www.zillow.com/profile"},
                    ],
                )
            ]
        ]

    monkeypatch.setattr(bot_min, "search_round_robin", fake_search_round_robin)

    urls, search_empty, _ = bot_min._contact_search_urls(
        "Sam Stone",
        "FL",
        {"city": "Orlando", "state": "FL", "brokerage": "Bright Homes"},
        domain_hint="samstone.com",
        brokerage="Bright Homes",
        limit=3,
    )

    assert captured_queries
    assert urls == ["https://independent.example"]
    assert search_empty is False


def test_select_top5_relaxes_when_empty(monkeypatch):
    urls = [
        "https://example.com/about",
        "https://example.gov/agent",
    ]

    def fake_fetch_cached(url, ttl_days=7):
        return {
            "final_url": url,
            "http_status": 200,
            "extracted_text": "ok",
            "retry_needed": False,
        }

    monkeypatch.setattr(bot_min, "fetch_text_cached", fake_fetch_cached)

    filtered, rejected = bot_min.select_top_5_urls(urls, fetch_check=True, relaxed=False)

    assert any("example.com/about" in url for url in filtered)
    assert all("example.gov" not in url for url in filtered)
    assert any(reason == "gov_edu" for _, reason in rejected)
