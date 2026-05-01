import asyncio
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


class _WorksheetNotFound(Exception):
    pass


class _DummySheet:
    def col_values(self, idx):
        return []

    def row_values(self, idx):
        return [
            "zpid",
            "address",
            "source",
            "created_at",
            "status",
            "claimed_at",
            "processed_at",
            "result",
            "error",
            "listing_json",
        ]

    def get_all_values(self):
        return [self.row_values(1)]

    def append_row(self, values):
        return None

    def update(self, *args, **kwargs):
        return None


dummy_sheet = _DummySheet()
dummy_workbook = types.SimpleNamespace(
    sheet1=dummy_sheet,
    worksheet=lambda name: dummy_sheet,
    add_worksheet=lambda **kwargs: dummy_sheet,
)
dummy_client = types.SimpleNamespace(open_by_key=lambda key: dummy_workbook)

gspread_module = types.SimpleNamespace(
    authorize=lambda creds: dummy_client,
    WorksheetNotFound=_WorksheetNotFound,
    exceptions=types.SimpleNamespace(APIError=Exception, WorksheetNotFound=_WorksheetNotFound),
)
sys.modules["gspread"] = gspread_module

fake_openai = types.SimpleNamespace(__spec__=importlib.machinery.ModuleSpec("openai", None))
sys.modules["openai"] = fake_openai


class _DummyFastAPI:
    def on_event(self, *args, **kwargs):
        return lambda func: func

    def post(self, *args, **kwargs):
        return lambda func: func

    def get(self, *args, **kwargs):
        return lambda func: func

    def head(self, *args, **kwargs):
        return lambda func: func


fastapi_module = types.ModuleType("fastapi")
fastapi_module.FastAPI = lambda *args, **kwargs: _DummyFastAPI()
fastapi_module.HTTPException = Exception
fastapi_module.Request = object
fastapi_module.Response = lambda *args, **kwargs: types.SimpleNamespace(
    status_code=kwargs.get("status_code")
)
sys.modules["fastapi"] = fastapi_module

starlette_requests_module = types.ModuleType("starlette.requests")


class _ClientDisconnect(Exception):
    pass


starlette_requests_module.ClientDisconnect = _ClientDisconnect
sys.modules.setdefault("starlette", types.ModuleType("starlette"))
sys.modules["starlette.requests"] = starlette_requests_module


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

import webhook_server


class _FakeRequest:
    headers = {"content-type": "application/json", "content-length": "1"}
    query_params = {}

    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")

    async def body(self):
        return self._body


def _listing(zpid, state="FL", source="apify"):
    return {
        "zpid": zpid,
        "address": f"{zpid} Main St, Testville, {state}",
        "street": f"{zpid} Main St",
        "city": "Testville",
        "state": state,
        "agentName": "Test Agent",
        "description": "Short sale subject to lender approval.",
        "search_source": source,
    }


def test_payload_webhook_enqueues_extra_state_rows(monkeypatch):
    enqueued = []

    monkeypatch.setattr(webhook_server, "load_seen_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_fetch_extra_state_rows", lambda: [_listing("mi-1", "MI", "mi")])
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(webhook_server, "_process_pending_queue", lambda *args, **kwargs: 0)
    monkeypatch.setattr(webhook_server, "append_seen_zpids", lambda zpids: None)
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 0)
    webhook_server.EXPORTED_ZPIDS.clear()

    def fake_enqueue(rows, source):
        enqueued.extend(str(row.get("zpid")) for row in rows)
        return len(rows)

    monkeypatch.setattr(webhook_server, "_enqueue_pending_rows", fake_enqueue)

    payload = {"listings": [_listing("main-1")], "upstreamDatasetId": "dataset-1"}
    result = asyncio.run(webhook_server.apify_hook(_FakeRequest(payload)))

    assert result["status"] == "processed"
    assert "main-1" in enqueued
    assert "mi-1" in enqueued


def test_original_cap_does_not_consume_state_search_cap(monkeypatch):
    enqueued = []
    seen_batches = []

    original_rows = [_listing(f"main-{idx}") for idx in range(5)]
    state_rows = [
        _listing("mi-1", "MI", "mi"),
        _listing("ak-1", "AK", "ak"),
        _listing("hi-1", "HI", "hi"),
    ]

    monkeypatch.setattr(webhook_server, "load_seen_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_fetch_extra_state_rows", lambda: state_rows)
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(webhook_server, "_process_pending_queue", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        webhook_server,
        "append_seen_zpids",
        lambda zpids: seen_batches.append([str(zpid) for zpid in zpids]),
    )
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 5)
    webhook_server.EXPORTED_ZPIDS.clear()

    def fake_enqueue(rows, source):
        enqueued.extend(str(row.get("zpid")) for row in rows)
        return len(rows)

    monkeypatch.setattr(webhook_server, "_enqueue_pending_rows", fake_enqueue)

    result = asyncio.run(
        webhook_server.apify_hook(
            _FakeRequest({"listings": original_rows, "upstreamDatasetId": "dataset-1"})
        )
    )

    seen_zpids = {zpid for batch in seen_batches for zpid in batch}

    assert result["status"] == "processed"
    assert {f"main-{idx}" for idx in range(5)} <= set(enqueued)
    assert {"mi-1", "ak-1", "hi-1"} <= set(enqueued)
    assert {f"main-{idx}" for idx in range(5)} <= seen_zpids
    assert {"mi-1", "ak-1", "hi-1"} <= seen_zpids


def test_state_searches_have_separate_combined_cap(monkeypatch):
    enqueued_by_source = []

    original_rows = [_listing(f"main-{idx}") for idx in range(5)]
    state_rows = [_listing(f"mi-{idx}", "MI", "mi") for idx in range(7)]

    monkeypatch.setattr(webhook_server, "load_seen_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_fetch_extra_state_rows", lambda: state_rows)
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(webhook_server, "_process_pending_queue", lambda *args, **kwargs: 0)
    monkeypatch.setattr(webhook_server, "append_seen_zpids", lambda zpids: None)
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 5)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_LIMIT", 5)
    webhook_server.EXPORTED_ZPIDS.clear()

    def fake_enqueue(rows, source):
        enqueued_by_source.extend((source, str(row.get("zpid"))) for row in rows)
        return len(rows)

    monkeypatch.setattr(webhook_server, "_enqueue_pending_rows", fake_enqueue)

    result = asyncio.run(
        webhook_server.apify_hook(
            _FakeRequest({"listings": original_rows, "upstreamDatasetId": "dataset-1"})
        )
    )

    original_enqueued = [zpid for source, zpid in enqueued_by_source if source == "payload.listings"]
    state_enqueued = [zpid for source, zpid in enqueued_by_source if source == "state-search"]

    assert result["status"] == "processed"
    assert original_enqueued == [f"main-{idx}" for idx in range(5)]
    assert state_enqueued == [f"mi-{idx}" for idx in range(5)]
