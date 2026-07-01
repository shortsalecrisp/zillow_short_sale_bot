import asyncio
import importlib.machinery
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
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
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", lambda rows: rows, raising=False)
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(webhook_server, "_process_pending_queue", lambda *args, **kwargs: 0)
    monkeypatch.setattr(webhook_server, "append_seen_zpids", lambda zpids: None)
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 0)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_BACKGROUND", False)
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


def test_startup_queue_recovery_is_backgrounded(monkeypatch):
    calls = []
    scheduled = []

    def fake_process_pending_queue(*args, **kwargs):
        calls.append((args, kwargs))
        return 0

    def fake_create_task(coro):
        scheduled.append(coro)
        return types.SimpleNamespace()

    monkeypatch.setattr(webhook_server, "_process_pending_queue", fake_process_pending_queue)
    monkeypatch.setattr(webhook_server.asyncio, "create_task", fake_create_task)

    asyncio.run(webhook_server._recover_pending_queue())

    assert calls == []
    assert len(scheduled) == 1
    scheduled[0].close()


def test_scheduler_startup_work_defaults_off(monkeypatch):
    monkeypatch.delenv("FOLLOWUP_RUN_ON_STARTUP", raising=False)
    monkeypatch.delenv("SCHEDULER_RUN_IMMEDIATELY", raising=False)

    assert webhook_server.FREE_SOURCE_PILOT_STARTUP_CATCHUP is False
    assert webhook_server._should_run_immediately() is False


def test_scheduler_startup_work_can_be_enabled(monkeypatch):
    monkeypatch.setenv("FOLLOWUP_RUN_ON_STARTUP", "true")
    monkeypatch.delenv("SCHEDULER_RUN_IMMEDIATELY", raising=False)

    assert webhook_server._should_run_immediately() is True


def test_extra_state_searches_exclude_mi(monkeypatch):
    sources = {cfg["source"] for cfg in webhook_server.EXTRA_STATE_SEARCHES}

    assert sources == {"ak", "hi"}


def test_free_source_pilot_defaults_exclude_mi():
    assert "MI" not in webhook_server.FREE_SOURCE_PILOT_STATES
    assert len(webhook_server.FREE_SOURCE_PILOT_STATES) == 49


def test_state_search_uses_shared_default_fetch_limit(monkeypatch):
    monkeypatch.delenv("APIFY_STATE_SEARCH_FETCH_LIMIT", raising=False)
    monkeypatch.delenv("APIFY_STATE_SEARCH_FETCH_LIMIT_MI", raising=False)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_FETCH_LIMIT", 25)

    assert webhook_server._state_search_fetch_limit("hi") == 25


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
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", lambda rows: rows, raising=False)
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(webhook_server, "_process_pending_queue", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        webhook_server,
        "append_seen_zpids",
        lambda zpids: seen_batches.append([str(zpid) for zpid in zpids]),
    )
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 5)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_BACKGROUND", False)
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
    assert {"mi-1", "ak-1", "hi-1"}.isdisjoint(seen_zpids)


def test_state_searches_have_separate_combined_cap(monkeypatch):
    enqueued_by_source = []

    original_rows = [_listing(f"main-{idx}") for idx in range(5)]
    state_rows = [_listing(f"mi-{idx}", "MI", "mi") for idx in range(7)]

    monkeypatch.setattr(webhook_server, "load_seen_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_fetch_extra_state_rows", lambda: state_rows)
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", lambda rows: rows, raising=False)
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(webhook_server, "_process_pending_queue", lambda *args, **kwargs: 0)
    monkeypatch.setattr(webhook_server, "append_seen_zpids", lambda zpids: None)
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 5)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_LIMIT", 5)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_BACKGROUND", False)
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


def test_state_search_prioritizes_low_volume_states_before_mi(monkeypatch):
    enqueued_by_source = []
    state_rows = (
        [_listing(f"mi-{idx}", "MI", "mi") for idx in range(5)]
        + [_listing(f"ak-{idx}", "AK", "ak") for idx in range(2)]
        + [_listing("hi-1", "HI", "hi")]
    )

    monkeypatch.setattr(webhook_server, "_fetch_extra_state_rows", lambda: state_rows)
    monkeypatch.setattr(webhook_server, "_pending_queue_state_skip_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", lambda rows: rows, raising=False)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_LIMIT", 5)

    def fake_enqueue(rows, source):
        enqueued_by_source.extend((source, str(row.get("zpid"))) for row in rows)
        return len(rows)

    monkeypatch.setattr(webhook_server, "_enqueue_pending_rows", fake_enqueue)

    count = webhook_server._enqueue_extra_state_rows({})

    state_enqueued = [zpid for source, zpid in enqueued_by_source if source == "state-search"]
    assert count == 5
    assert state_enqueued == ["ak-0", "ak-1", "hi-1", "mi-0", "mi-1"]


def test_state_search_cap_is_applied_after_queue_skip(monkeypatch):
    enqueued_by_source = []

    original_rows = [_listing("main-1")]
    state_rows = [_listing(f"mi-{idx}", "MI", "mi") for idx in range(7)]

    monkeypatch.setattr(webhook_server, "load_seen_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_pending_queue_state_skip_zpids", lambda: {f"mi-{idx}" for idx in range(5)})
    monkeypatch.setattr(webhook_server, "_fetch_extra_state_rows", lambda: state_rows)
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", lambda rows: rows, raising=False)
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(webhook_server, "_process_pending_queue", lambda *args, **kwargs: 0)
    monkeypatch.setattr(webhook_server, "append_seen_zpids", lambda zpids: None)
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 5)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_LIMIT", 5)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_BACKGROUND", False)
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

    state_enqueued = [zpid for source, zpid in enqueued_by_source if source == "state-search"]

    assert result["status"] == "processed"
    assert state_enqueued == ["mi-5", "mi-6"]


def test_state_search_runs_search_task_without_detail_wrapper(monkeypatch):
    captured = {}

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {
                    "zpid": "hi-1",
                    "detailUrl": "https://www.zillow.com/homedetails/hi-1_zpid/",
                    "address": "1 Ocean Ave, Pahoa, HI 96778",
                    "street": "1 Ocean Ave",
                    "city": "Pahoa",
                    "state": "HI",
                    "agentName": "State Agent",
                    "description": "Short sale subject to lender approval.",
                }
            ]

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return _Response()

    def fake_post(*args, **kwargs):
        raise AssertionError("state search should not call the detail wrapper before filtering")

    monkeypatch.setattr(webhook_server, "APIFY_TOKEN", "token")
    monkeypatch.setattr(webhook_server.requests, "get", fake_get)
    monkeypatch.setattr(webhook_server.requests, "post", fake_post)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_LIMIT", 5)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_FETCH_LIMIT", 7)

    rows = webhook_server._run_state_task_sync_dataset_items("state-task", "hi")

    assert rows[0]["zpid"] == "hi-1"
    assert rows[0]["agentName"] == "State Agent"
    assert rows[0]["search_source"] == "hi"
    assert captured["url"].endswith("/actor-tasks/state-task/run-sync-get-dataset-items")
    assert captured["params"]["limit"] == 7
    assert captured["params"]["maxItems"] == 7


def test_state_search_queue_payload_uses_street_only_sms_address():
    row = {
        "zpid": "hi-1",
        "detailUrl": "https://www.zillow.com/homedetails/hi-1_zpid/",
        "address": "1 Ocean Ave, Pahoa, HI 96778",
        "street": "1 Ocean Ave, Pahoa, HI 96778",
        "city": "Pahoa",
        "state": "HI",
        "zip": "96778",
        "agentName": "State Agent",
        "description": "Short sale subject to lender approval.",
        "search_source": "hi",
    }

    payload = webhook_server._compact_queue_resume_payload(row, "state-search")

    assert payload["address"] == "1 Ocean Ave"
    assert payload["street"] == "1 Ocean Ave"
    assert payload["full_address"] == "1 Ocean Ave, Pahoa, HI 96778"


def test_state_search_queue_payload_preserves_special_listing_conditions():
    row = {
        "zpid": "mi-1",
        "detailUrl": "https://www.zillow.com/homedetails/mi-1_zpid/",
        "address": "1 Main St, Detroit, MI 48201",
        "street": "1 Main St",
        "city": "Detroit",
        "state": "MI",
        "agentName": "State Agent",
        "description": "Clean house with updated kitchen.",
        "search_source": "mi",
        "resoFacts": {"specialListingConditions": "Short Sale,Standard"},
    }

    payload = webhook_server._compact_queue_resume_payload(row, "state-search")

    assert payload["specialListingConditions"] == "Short Sale,Standard"
    assert "Short Sale,Standard" in payload["listingText"]


def test_state_detail_task_uses_listing_urls(monkeypatch):
    captured = {}

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {
                    "zpid": "mi-1",
                    "detailUrl": "https://www.zillow.com/homedetails/mi-1_zpid/",
                    "address": "1 Main St, Detroit, MI 48201",
                    "street": "1 Main St",
                    "city": "Detroit",
                    "state": "MI",
                    "agentName": "State Agent",
                    "description": "Short sale subject to lender approval.",
                }
            ]

    def fake_post(url, params, json, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["json"] = json
        captured["timeout"] = timeout
        return _Response()

    monkeypatch.setattr(webhook_server, "APIFY_TOKEN", "token")
    monkeypatch.setattr(webhook_server, "APIFY_STATE_DETAIL_TASK_ID", "detail-task")
    monkeypatch.setattr(webhook_server.requests, "post", fake_post)

    rows = webhook_server._run_state_detail_task_for_rows(
        [
            {
                "zpid": "mi-1",
                "detailUrl": "https://www.zillow.com/homedetails/mi-1_zpid/",
                "address": "1 Main St, Detroit, MI 48201",
                "agentName": "State Agent",
                "search_source": "mi",
            }
        ]
    )

    assert rows[0]["description"] == "Short sale subject to lender approval."
    assert captured["url"].endswith("/actor-tasks/detail-task/run-sync-get-dataset-items")
    assert captured["json"]["startUrls"] == [
        {"url": "https://www.zillow.com/homedetails/mi-1_zpid/"}
    ]
    assert "zpids" not in captured["json"]


def test_state_detail_task_failure_does_not_return_search_only_rows(monkeypatch):
    class _Response:
        def raise_for_status(self):
            raise webhook_server.requests.RequestException("bad request")

        def json(self):
            return []

    monkeypatch.setattr(webhook_server, "APIFY_TOKEN", "token")
    monkeypatch.setattr(webhook_server, "APIFY_STATE_DETAIL_TASK_ID", "detail-task")
    monkeypatch.setattr(webhook_server.requests, "post", lambda *args, **kwargs: _Response())

    rows = webhook_server._run_state_detail_task_for_rows(
        [
            {
                "zpid": "mi-1",
                "detailUrl": "https://www.zillow.com/homedetails/mi-1_zpid/",
                "address": "1 Main St, Detroit, MI 48201",
                "agentName": "State Agent",
                "search_source": "mi",
            }
        ]
    )

    assert rows == []


def test_state_search_details_only_selected_unseen_rows(monkeypatch):
    detail_calls = []
    enqueued = []

    state_rows = [
        {
            "zpid": f"mi-{idx}",
            "address": f"{idx} Main St, Detroit, MI",
            "agentName": "State Agent",
            "search_source": "mi",
        }
        for idx in range(7)
    ]

    def fake_detail(rows):
        detail_calls.extend(str(row.get("zpid")) for row in rows)
        return [
            {
                **row,
                "description": "Detailed short sale subject to lender approval.",
            }
            for row in rows
        ]

    monkeypatch.setattr(webhook_server, "_fetch_extra_state_rows", lambda: state_rows)
    monkeypatch.setattr(webhook_server, "_pending_queue_state_skip_zpids", lambda: {"mi-0", "mi-1"})
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", fake_detail, raising=False)
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_LIMIT", 3)

    def fake_enqueue(rows, source):
        enqueued.extend(str(row.get("zpid")) for row in rows)
        assert all(row.get("description", "").startswith("Detailed short sale") for row in rows)
        return len(rows)

    monkeypatch.setattr(webhook_server, "_enqueue_pending_rows", fake_enqueue)

    count = webhook_server._enqueue_extra_state_rows({})

    assert count == 3
    assert detail_calls == ["mi-2", "mi-3", "mi-4"]
    assert enqueued == ["mi-2", "mi-3", "mi-4"]


def test_coverage_backstop_has_separate_main_and_state_caps(monkeypatch):
    detail_calls = []
    enqueued_by_source = []

    main_rows = [_listing(f"main-{idx}", "FL", "main") for idx in range(4)]
    state_rows = [
        _listing("ak-1", "AK", "ak"),
        _listing("mi-0", "MI", "mi"),
        _listing("mi-1", "MI", "mi"),
        _listing("mi-2", "MI", "mi"),
    ]

    monkeypatch.setattr(webhook_server, "_fetch_apify_backstop_main_rows", lambda: main_rows)
    monkeypatch.setattr(webhook_server, "_fetch_apify_backstop_state_rows", lambda: state_rows)
    monkeypatch.setattr(webhook_server, "_coverage_backstop_skip_zpids", lambda: {"main-0", "mi-0"})
    monkeypatch.setattr(webhook_server, "APIFY_BACKSTOP_MAIN_LIMIT", 2)
    monkeypatch.setattr(webhook_server, "APIFY_BACKSTOP_STATE_LIMIT", 2)

    def fake_detail(rows):
        detail_calls.append([str(row.get("zpid")) for row in rows])
        return [
            {
                **row,
                "description": "Detailed short sale subject to lender approval.",
            }
            for row in rows
        ]

    def fake_enqueue(rows, source):
        enqueued_by_source.append((source, [str(row.get("zpid")) for row in rows]))
        return len(rows)

    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", fake_detail)
    monkeypatch.setattr(webhook_server, "_enqueue_pending_rows", fake_enqueue)

    count = webhook_server._enqueue_apify_coverage_backstop()

    assert count == 4
    assert detail_calls == [["main-1", "main-2"], ["ak-1", "mi-1"]]
    assert enqueued_by_source == [
        ("coverage-backstop-main", ["main-1", "main-2"]),
        ("coverage-backstop-state", ["ak-1", "mi-1"]),
    ]


def test_coverage_backstop_search_fetch_does_not_call_detail_before_filter(monkeypatch):
    captured = {}

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [
                {
                    "zpid": "main-1",
                    "detailUrl": "https://www.zillow.com/homedetails/main-1_zpid/",
                    "address": "1 Main St, Tampa, FL 33602",
                    "street": "1 Main St",
                    "city": "Tampa",
                    "state": "FL",
                    "agentName": "Main Agent",
                    "description": "Short sale subject to lender approval.",
                }
            ]

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return _Response()

    def fake_post(*args, **kwargs):
        raise AssertionError("coverage backstop search fetch should not detail before filtering")

    monkeypatch.setattr(webhook_server, "APIFY_TOKEN", "token")
    monkeypatch.setattr(webhook_server.requests, "get", fake_get)
    monkeypatch.setattr(webhook_server.requests, "post", fake_post)

    rows = webhook_server._run_apify_search_task_sync_dataset_items(
        "main-task",
        "main",
        fetch_limit=37,
        log_prefix="coverage-backstop",
    )

    assert rows[0]["zpid"] == "main-1"
    assert rows[0]["search_source"] == "main"
    assert captured["url"].endswith("/actor-tasks/main-task/run-sync-get-dataset-items")
    assert captured["params"]["limit"] == 37
    assert captured["params"]["maxItems"] == 37


def test_coverage_backstop_day_lock_prevents_duplicate_runs(monkeypatch, tmp_path):
    monkeypatch.setattr(webhook_server, "APIFY_BACKSTOP_LOCK_PATH", str(tmp_path / "coverage-backstop"))

    run_time = datetime(2026, 6, 7, 18, tzinfo=timezone.utc)

    assert webhook_server._acquire_apify_backstop_day(run_time) is True
    assert webhook_server._acquire_apify_backstop_day(run_time) is False
    assert webhook_server._acquire_apify_backstop_day(run_time + timedelta(days=1)) is True
