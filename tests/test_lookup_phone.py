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
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))

    def fake_is_mobile(number):
        return number in {office_number, mobile_number}

    monkeypatch.setattr(bot_min, "is_mobile_number", fake_is_mobile)

    bot_min.cache_p.clear()

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
    assert result["source"] == "rapid_contact"


def test_lookup_phone_continues_search_after_nonproductive_page(monkeypatch):
    office_number = "555-111-2222"
    mobile_number = "555-333-4444"

    def fake_rapid_property(zpid):
        return {
            "listed_by": {
                "display_name": "Jane Agent",
                "phones": [
                    {"number": office_number},
                ],
            }
        }

    monkeypatch.setattr(bot_min, "rapid_property", fake_rapid_property)
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: ["query"])

    google_results = [
        [],
        [
            {"link": "https://independent-broker.test/office"},
            {"link": "https://independent-broker.test/mobile"},
        ],
    ]

    monkeypatch.setattr(bot_min, "google_items", lambda query: google_results.pop(0))
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])

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
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: ["query"])
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


def test_lookup_phone_team_context_not_demoted(monkeypatch):
    office_number = "555-000-1111"
    mobile_number = "555-222-3333"

    monkeypatch.setattr(bot_min, "rapid_property", lambda zpid: {})
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: ["query"])
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
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: ["query"])
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
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: [])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [])
    monkeypatch.setattr(bot_min, "google_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bot_min, "fetch_contact_page", lambda url: ("", ""))

    def fake_is_mobile(number):
        return True

    monkeypatch.setattr(bot_min, "is_mobile_number", fake_is_mobile)

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
    assert result["source"] == "rapid_contact"

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
    monkeypatch.setattr(bot_min, "build_q_phone", lambda name, state: ["query"])
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
        lambda agent, state, brokerage, domain_hint, mls_id: ["query"],
    )
    monkeypatch.setattr(bot_min, "google_items", lambda query: [{"link": "https://example.com/profile"}])
    monkeypatch.setattr(bot_min, "pmap", lambda fn, iterable: [fn(item) for item in iterable])
    monkeypatch.setattr(bot_min, "fetch_contact_page", fake_fetch)

    bot_min.cache_e.clear()

    result = bot_min.lookup_email(
        "Michael Johnson",
        "KY",
        {"zpid": "", "contact_recipients": [], "city": "Louisville", "state": "KY"},
    )

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
    assert result["confidence"] == "low"
    assert result["source"] == "rapid_listed_by"
