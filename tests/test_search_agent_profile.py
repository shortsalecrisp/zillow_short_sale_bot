import json
import sys
import types
from pathlib import Path

import pytest


def load_bot():
    root = Path(__file__).resolve().parents[1]
    cfg_path = root / "config.json"
    original = cfg_path.read_text()
    data = json.loads(original)
    data.setdefault("openai_api_key", "")
    data.setdefault("google_api_key", "")
    data.setdefault("google_cx", "")
    data.setdefault("google_sheet_name", "dummy")
    cfg_path.write_text(json.dumps(data))
    try:
        class DummyUA:
            def __init__(self, *args, **kwargs):
                self.random = "test-agent"

        sys.modules.setdefault(
            "fake_useragent", types.SimpleNamespace(UserAgent=DummyUA)
        )

        class DummyCreds: ...

        sys.modules.setdefault(
            "oauth2client.service_account",
            types.SimpleNamespace(
                ServiceAccountCredentials=types.SimpleNamespace(
                    from_json_keyfile_name=lambda *a, **k: DummyCreds()
                )
            ),
        )

        class DummySheet:
            def col_values(self, n):
                return []

        class DummyGSpread:
            def authorize(self, creds):
                class DummyOpen:
                    def open(self, name):
                        return types.SimpleNamespace(sheet1=DummySheet())

                return DummyOpen()

        sys.modules.setdefault("gspread", DummyGSpread())
        sys.modules.setdefault("openai", types.SimpleNamespace(api_key=""))

        sys.path.insert(0, str(root))
        code = (root / "bot.py").read_text().split("\nwhile True:")[0]
        module = types.ModuleType("bot")
        module.__dict__["__name__"] = "bot"
        exec(code, module.__dict__)
    finally:
        cfg_path.write_text(original)
    return module


def test_search_agent_profile_structured_data(monkeypatch):
    bot = load_bot()

    class DummyUA:
        random = "test-agent"

    monkeypatch.setattr(bot, "ua", DummyUA())

    def fake_google_search_links(query):
        return [{
            "link": "https://example.com/agent",
            "pagemap": {
                "contactpoint": [{"telephone": "555-444-3333", "contacttype": "mobile"}],
                "metatags": [{"email": "agent@example.com"}]
            }
        }]

    called = {}

    def fake_get(*args, **kwargs):
        called['called'] = True
        raise AssertionError("requests.get should not be called when structured data is present")

    monkeypatch.setattr(bot, "google_search_links", fake_google_search_links)
    monkeypatch.setattr(bot.requests, "get", fake_get)

    phone, email = bot.search_agent_profile("John Doe", "CA")

    assert phone == "555-444-3333"
    assert email == "agent@example.com"
    assert 'called' not in called
