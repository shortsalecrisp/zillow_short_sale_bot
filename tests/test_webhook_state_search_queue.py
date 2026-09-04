import asyncio
import importlib.machinery
import io
import json
import os
import sys
import threading
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


try:
    import fastapi as _fastapi  # noqa: F401
except Exception:
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

try:
    from starlette.requests import ClientDisconnect as _ClientDisconnect  # noqa: F401
except Exception:
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

import bot_min
import webhook_server


def test_free_source_pilot_post_verifier_audit_due_at_configured_hour():
    before = datetime(2026, 8, 1, 14, 4, tzinfo=timezone.utc)  # 10:04 New York
    due = datetime(2026, 8, 1, 14, 5, tzinfo=timezone.utc)  # 10:05 New York

    assert webhook_server._free_source_pilot_post_verifier_audit_due(before) is False
    assert webhook_server._free_source_pilot_post_verifier_audit_due(due) is True


def test_next_free_source_pilot_post_verifier_audit_is_independent_daily_slot():
    before = datetime(2026, 8, 1, 12, 30, tzinfo=timezone.utc)  # 08:30 New York
    after = datetime(2026, 8, 1, 14, 6, tzinfo=timezone.utc)  # 10:06 New York

    assert webhook_server._next_free_source_pilot_post_verifier_audit(before) == datetime(
        2026, 8, 1, 14, 5, tzinfo=timezone.utc
    )
    assert webhook_server._next_free_source_pilot_post_verifier_audit(after) == datetime(
        2026, 8, 2, 14, 5, tzinfo=timezone.utc
    )


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


def test_scheduler_restart_recovery_defaults_to_scheduled_window(monkeypatch):
    monkeypatch.delenv("FOLLOWUP_RESTART_RECOVERY_ENABLED", raising=False)
    monkeypatch.delenv("FOLLOWUP_RUN_ON_STARTUP", raising=False)
    monkeypatch.delenv("SCHEDULER_RUN_IMMEDIATELY", raising=False)

    assert webhook_server.FREE_SOURCE_PILOT_STARTUP_CATCHUP is True
    assert webhook_server._should_run_immediately() is False


def test_scheduler_restart_recovery_can_be_disabled(monkeypatch):
    monkeypatch.setenv("FOLLOWUP_RESTART_RECOVERY_ENABLED", "false")
    monkeypatch.setenv("FOLLOWUP_RUN_ON_STARTUP", "false")
    monkeypatch.setenv("SCHEDULER_RUN_IMMEDIATELY", "false")

    assert webhook_server._should_run_immediately() is False


def test_scheduler_startup_work_can_be_enabled(monkeypatch):
    monkeypatch.setenv("FOLLOWUP_RESTART_RECOVERY_ENABLED", "false")
    monkeypatch.setenv("FOLLOWUP_RUN_ON_STARTUP", "true")
    monkeypatch.delenv("SCHEDULER_RUN_IMMEDIATELY", raising=False)

    assert webhook_server._should_run_immediately() is True


def test_startup_queue_recovery_only_requeues_stale_items(monkeypatch):
    calls = []

    monkeypatch.setattr(
        webhook_server,
        "_requeue_stale_in_progress_items",
        lambda *, startup=False: calls.append(startup) or 2,
    )
    monkeypatch.setattr(
        webhook_server,
        "_process_pending_queue",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("must not drain pending queue")),
    )

    assert webhook_server._recover_stale_queue_for_scheduled_window() == 2
    assert calls == [True]


def test_scheduler_startup_catchup_signals_completion(monkeypatch):
    completed = threading.Event()
    stop = threading.Event()
    stop.set()
    cycles = []

    monkeypatch.setattr(
        bot_min,
        "_within_scheduler_hours",
        lambda _slot: True,
    )
    monkeypatch.setattr(
        bot_min,
        "_run_hourly_cycle",
        lambda run_time, callbacks, skip_callbacks=False: cycles.append(
            (run_time, callbacks, skip_callbacks)
        ),
    )

    bot_min.run_hourly_scheduler(
        stop_event=stop,
        hourly_callbacks=[lambda _run_time: None],
        run_immediately=True,
        initial_callbacks=False,
        initial_run_complete_event=completed,
    )

    assert completed.is_set()
    assert len(cycles) == 1
    assert cycles[0][2] is True


def test_extra_state_searches_exclude_mi(monkeypatch):
    sources = {cfg["source"] for cfg in webhook_server.EXTRA_STATE_SEARCHES}

    assert sources == {"ak", "hi"}


def test_free_source_pilot_defaults_include_all_states():
    assert "MI" in webhook_server.FREE_SOURCE_PILOT_STATES
    assert len(webhook_server.FREE_SOURCE_PILOT_STATES) == 50


def test_free_source_pilot_next_run_uses_daily_7am_slot():
    now = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 7, 1, 21, 12, 29))

    next_run = webhook_server._next_free_source_pilot_run(now)

    assert next_run == webhook_server.SCHEDULER_TZ.localize(datetime(2026, 7, 2, 7, 0, 0))


def test_free_source_pilot_next_run_returns_today_before_7am():
    now = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 7, 1, 6, 12, 29))

    next_run = webhook_server._next_free_source_pilot_run(now)

    assert next_run == webhook_server.SCHEDULER_TZ.localize(datetime(2026, 7, 1, 7, 0, 0))


def test_free_source_pilot_due_only_at_configured_daily_slot():
    due = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 7, 1, 7, 0, 0))
    not_due = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 7, 1, 8, 0, 0))

    assert webhook_server._free_source_pilot_due(due)
    assert not webhook_server._free_source_pilot_due(not_due)


def test_bounded_startup_catchup_recovers_source_and_audit_after_slots():
    source_now = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 9, 1, 0))
    audit_now = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 10, 6, 0))

    assert webhook_server._bounded_startup_catchup_slot(source_now, 9, 0) == webhook_server.SCHEDULER_TZ.localize(
        datetime(2026, 8, 21, 9, 0, 0)
    )
    assert webhook_server._bounded_startup_catchup_slot(audit_now, 10, 5) == webhook_server.SCHEDULER_TZ.localize(
        datetime(2026, 8, 21, 10, 5, 0)
    )


def test_bounded_startup_catchup_does_not_run_before_launch_date(monkeypatch):
    monkeypatch.setattr(webhook_server, "FREE_SOURCE_PILOT_STARTUP_CATCHUP_START_DATE", "2026-08-21")
    before_launch = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 20, 10, 30, 0))

    assert webhook_server._bounded_startup_catchup_slot(before_launch, 9, 0) is None


def test_audit_startup_catchup_recovers_previous_day_after_timer_loss(monkeypatch):
    monkeypatch.setattr(webhook_server, "FREE_SOURCE_PILOT_STARTUP_CATCHUP_START_DATE", "2026-08-21")
    after_restart = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 22, 8, 0, 0))

    slot = webhook_server._bounded_startup_catchup_slot(
        after_restart,
        10,
        5,
        max_hours=30,
        allow_previous_day=True,
    )

    assert slot == webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 10, 5, 0))


def test_audit_startup_catchup_dispatches_prior_and_current_missing_slots(monkeypatch):
    monkeypatch.setattr(webhook_server, "FREE_SOURCE_PILOT_STARTUP_CATCHUP_START_DATE", "2026-08-21")
    monkeypatch.setattr(webhook_server, "FREE_SOURCE_PILOT_AUDIT_STARTUP_CATCHUP_MAX_HOURS", 30)
    after_current_slot = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 22, 11, 0, 0))

    slots = webhook_server._bounded_audit_startup_catchup_slots(after_current_slot)

    assert slots == [
        webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 10, 5, 0)),
        webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 22, 10, 5, 0)),
    ]


def test_audit_startup_catchup_never_reaches_before_launch_date(monkeypatch):
    monkeypatch.setattr(webhook_server, "FREE_SOURCE_PILOT_STARTUP_CATCHUP_START_DATE", "2026-08-21")
    launch_morning = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 8, 0, 0))

    slot = webhook_server._bounded_startup_catchup_slot(
        launch_morning,
        10,
        5,
        max_hours=30,
        allow_previous_day=True,
    )

    assert slot is None


def test_startup_catchup_dispatches_every_startup_for_durable_child_claim(monkeypatch):
    slot = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 9, 0, 0))
    triggered = []
    monkeypatch.setattr(webhook_server, "FREE_SOURCE_PILOT_STARTUP_CATCHUP", True)
    monkeypatch.setattr(webhook_server, "_bounded_startup_catchup_slot", lambda *args: slot)
    monkeypatch.setattr(webhook_server, "_process_free_source_pilot_callback", triggered.append)

    webhook_server._start_free_source_pilot_startup_catchup()

    assert triggered == [slot]


def test_late_source_completion_schedules_post_verifier_audit_after_grace(monkeypatch):
    captured = {}

    class _Process:
        pid = 12348
        stdout = io.StringIO("")
        stderr = io.StringIO("")
        returncode = 0

        def wait(self, timeout=None):
            return 0

    audit_slot = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 10, 5, 0))
    monkeypatch.setattr(webhook_server.subprocess, "Popen", lambda *args, **kwargs: _Process())
    monkeypatch.setattr(webhook_server, "_log_free_source_pilot_wrapper_terminal", lambda **kwargs: None)
    monkeypatch.setattr(webhook_server, "_audit_slot_after_late_source_completion", lambda run_time: audit_slot)
    monkeypatch.setattr(webhook_server, "_schedule_post_source_audit", lambda slot: captured.setdefault("slot", slot))
    run_time = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 7, 0, 0))

    webhook_server._run_free_source_pilot(run_time)

    assert captured["slot"] == audit_slot


def test_post_source_audit_delay_is_later_of_slot_or_thirty_minute_grace(monkeypatch):
    monkeypatch.setattr(webhook_server, "FREE_SOURCE_PILOT_POST_SOURCE_AUDIT_GRACE_MINUTES", 30)
    audit_slot = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 10, 5, 0))
    early_completion = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 9, 0, 0))
    late_completion = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 9, 50, 0))

    assert webhook_server._post_source_audit_delay_seconds(audit_slot, early_completion) == 65 * 60
    assert webhook_server._post_source_audit_delay_seconds(audit_slot, late_completion) == 30 * 60


def test_source_scheduler_passes_correlated_receipt_and_logs_completion(monkeypatch):
    captured = {}
    terminal = []

    class _Process:
        pid = 12345
        stdout = io.StringIO("")
        stderr = io.StringIO("")
        returncode = 0

        def wait(self, timeout=None):
            return 0

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _Process()

    monkeypatch.setattr(webhook_server.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        webhook_server,
        "_log_free_source_pilot_wrapper_terminal",
        lambda **details: terminal.append(details),
    )
    run_time = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 7, 0, 0))

    webhook_server._run_free_source_pilot(run_time)

    assert "--scheduled-run" in captured["cmd"]
    receipt_index = captured["cmd"].index("--run-receipt-id") + 1
    assert captured["cmd"][receipt_index] == terminal[0]["run_receipt_id"]
    slot_index = captured["cmd"].index("--schedule-slot-id") + 1
    assert captured["cmd"][slot_index] == "source:2026-08-21"
    assert captured["kwargs"]["start_new_session"] is True
    assert terminal[0]["status"] == "process_exited_zero_child_receipt_required"


def test_source_scheduler_timeout_terminates_group_and_logs_failure(monkeypatch):
    terminated = []
    terminal = []

    class _Process:
        pid = 12346
        stdout = io.StringIO("")
        stderr = io.StringIO("")

        def wait(self, timeout=None):
            raise webhook_server.subprocess.TimeoutExpired("pilot", timeout)

    monkeypatch.setattr(webhook_server.subprocess, "Popen", lambda *args, **kwargs: _Process())
    monkeypatch.setattr(
        webhook_server, "_terminate_pilot_process_group", lambda process: terminated.append(process.pid)
    )
    monkeypatch.setattr(
        webhook_server,
        "_log_free_source_pilot_wrapper_terminal",
        lambda **details: terminal.append(details),
    )
    run_time = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 7, 0, 0))

    webhook_server._run_free_source_pilot(run_time)

    assert terminated == [12346]
    assert terminal[0]["status"] == "failed_timeout"


def test_source_scheduler_timeout_logs_terminal_even_when_cleanup_fails(monkeypatch):
    terminal = []

    class _Process:
        pid = 12347
        stdout = io.StringIO("")
        stderr = io.StringIO("")

        def wait(self, timeout=None):
            raise webhook_server.subprocess.TimeoutExpired("pilot", timeout)

    monkeypatch.setattr(webhook_server.subprocess, "Popen", lambda *args, **kwargs: _Process())
    monkeypatch.setattr(
        webhook_server,
        "_terminate_pilot_process_group",
        lambda process: (_ for _ in ()).throw(PermissionError("kill denied")),
    )
    monkeypatch.setattr(
        webhook_server,
        "_log_free_source_pilot_wrapper_terminal",
        lambda **details: terminal.append(details),
    )
    run_time = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 7, 0, 0))

    webhook_server._run_free_source_pilot(run_time)

    assert terminal[0]["status"] == "failed_timeout"
    assert "cleanup_error=PermissionError" in terminal[0]["error"]


def test_post_verifier_audit_uses_correlated_receipt_and_logs_completion(monkeypatch):
    captured = {}
    terminal = []

    class _ImmediateThread:
        def __init__(self, target, **kwargs):
            self.target = target

        def start(self):
            self.target()

    class _Process:
        returncode = 0

        def communicate(self, timeout=None):
            return "", ""

    def fake_popen(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _Process()

    monkeypatch.setattr(webhook_server.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(webhook_server.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        webhook_server,
        "_log_free_source_pilot_wrapper_terminal",
        lambda **details: terminal.append(details),
    )
    run_time = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 10, 5, 0))

    webhook_server._process_free_source_pilot_post_verifier_audit(run_time)

    receipt_index = captured["cmd"].index("--run-receipt-id") + 1
    assert captured["cmd"][receipt_index] == terminal[0]["run_receipt_id"]
    slot_index = captured["cmd"].index("--schedule-slot-id") + 1
    assert captured["cmd"][slot_index] == "post_verifier_audit:2026-08-21"
    assert "--scheduled-run" in captured["cmd"]
    assert captured["kwargs"]["start_new_session"] is True
    assert terminal[0]["status"] == "audit_process_exited_zero_child_receipt_required"


def test_post_verifier_audit_spawn_failure_has_structured_terminal(monkeypatch):
    terminal = []

    class _ImmediateThread:
        def __init__(self, target, **kwargs):
            self.target = target

        def start(self):
            self.target()

    monkeypatch.setattr(webhook_server.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        webhook_server.subprocess, "Popen", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("spawn failed"))
    )
    monkeypatch.setattr(
        webhook_server,
        "_log_free_source_pilot_wrapper_terminal",
        lambda **details: terminal.append(details),
    )
    run_time = webhook_server.SCHEDULER_TZ.localize(datetime(2026, 8, 21, 10, 5, 0))

    webhook_server._process_free_source_pilot_post_verifier_audit(run_time)

    assert terminal[0]["status"] == "audit_failed_spawn_or_wrapper"
    assert terminal[0]["run_receipt_id"]


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


def test_state_search_normalizes_curated_search_fields():
    normalized, drop_reason = webhook_server._normalize_extra_state_row(
        {
            "propertyId": "ak-100",
            "addressStreet": "100 Glacier Rd",
            "addressCity": "Anchorage",
            "addressState": "AK",
            "addressZipcode": "99501",
            "propertyUrl": "https://www.zillow.com/homedetails/ak-100_zpid/",
            "listingStatus": "FOR_SALE",
            "brokerName": "North Broker",
        },
        "ak",
    )

    assert drop_reason is None
    assert normalized["zpid"] == "ak-100"
    assert normalized["street"] == "100 Glacier Rd"
    assert normalized["city"] == "Anchorage"
    assert normalized["state"] == "AK"
    assert normalized["zip"] == "99501"
    assert normalized["detailUrl"] == "https://www.zillow.com/homedetails/ak-100_zpid/"
    assert normalized["homeStatus"] == "FOR_SALE"
    assert normalized["search_source"] == "ak"


def test_apify_normalizer_accepts_curated_detail_fields():
    normalized = webhook_server._normalize_apify_row(
        {
            "propertyId": "hi-200",
            "propertyUrl": "https://www.zillow.com/homedetails/hi-200_zpid/",
            "property": {
                "address": {
                    "streetAddress": "200 Lava Ln",
                    "city": "Pahoa",
                    "state": "HI",
                    "zipcode": "96778",
                },
                "publicRemarks": "Short sale subject to lender approval. Buyer to verify all details.",
                "status": "FOR_SALE",
            },
            "listedBy": [{"name": "Curated Agent"}],
            "brokerageName": "Island Broker",
        }
    )

    payload = webhook_server._compact_queue_resume_payload(normalized, "state-search")

    assert normalized["zpid"] == "hi-200"
    assert normalized["agentName"] == "Curated Agent"
    assert normalized["street"] == "200 Lava Ln"
    assert normalized["city"] == "Pahoa"
    assert normalized["state"] == "HI"
    assert normalized["homeStatus"] == "FOR_SALE"
    assert "Short sale subject to lender approval" in normalized["listing_description"]
    assert payload["listingText"].startswith("Short sale subject to lender approval")
    assert payload["agentName"] == "Curated Agent"


def test_apify_normalizer_accepts_current_listing_address_schema():
    normalized = webhook_server._normalize_apify_row(
        {
            "zpid": "2057678947",
            "propertyUrl": "https://www.zillow.com/homedetails/2057678947_zpid/",
            "listingAddress": {
                "street": "22547 STORYBOOK CABIN WAY",
                "city": "Land O Lakes",
                "state": "FL",
                "zipCode": "34637",
                "full": "22547 STORYBOOK CABIN WAY, Land O Lakes, FL 34637",
            },
            "agent": {"name": "Ana Henriquez", "phoneNumber": "786-878-4474"},
            "broker": {"name": "KELLER WILLIAMS TAMPA PROPERTIES"},
            "description": "Virtually Staged. Short Sale. Welcome to spacious, modern living.",
            "listingStatus": "forSale",
        }
    )

    payload = webhook_server._compact_queue_resume_payload(normalized, "payload.listings")

    assert normalized["street"] == "22547 STORYBOOK CABIN WAY"
    assert normalized["city"] == "Land O Lakes"
    assert normalized["state"] == "FL"
    assert normalized["zip"] == "34637"
    assert normalized["agentName"] == "Ana Henriquez"
    assert normalized["brokerName"] == "KELLER WILLIAMS TAMPA PROPERTIES"
    assert payload["address"] == "22547 STORYBOOK CABIN WAY"
    assert payload["agentName"] == "Ana Henriquez"
    assert payload["brokerName"] == "KELLER WILLIAMS TAMPA PROPERTIES"


def test_apify_normalizer_splits_current_full_address_string_schema():
    normalized = webhook_server._normalize_apify_row(
        {
            "zpid": "448492982",
            "address": "671 NE 195th St APT 227-E, Miami, FL, 33179",
            "agentName": "Micheli Melo Giordano",
            "description": "Short sale subject to lender approval.",
            "listingStatus": "FOR_SALE",
        }
    )

    payload = webhook_server._compact_queue_resume_payload(normalized, "payload.listings")

    assert normalized["street"] == "671 NE 195th St APT 227-E"
    assert normalized["city"] == "Miami"
    assert normalized["state"] == "FL"
    assert normalized["zip"] == "33179"
    assert payload["address"] == "671 NE 195th St APT 227-E"
    assert payload["city"] == "Miami"
    assert payload["state"] == "FL"


def test_payload_listing_selection_normalizes_current_schema_before_enqueue(monkeypatch):
    monkeypatch.setattr(webhook_server, "load_seen_zpids", lambda: set())

    selection = webhook_server._select_payload_listings(
        {
            "listings": [
                {
                    "zpid": "2057678947",
                    "propertyUrl": "https://www.zillow.com/homedetails/2057678947_zpid/",
                    "listingAddress": {
                        "street": "22547 STORYBOOK CABIN WAY",
                        "city": "Land O Lakes",
                        "state": "FL",
                        "zipCode": "34637",
                    },
                    "agent": {"name": "Ana Henriquez"},
                    "description": "Short Sale. Welcome to spacious, modern living.",
                    "listingStatus": "forSale",
                }
            ]
        }
    )

    assert selection["selected"] == 1
    selected = selection["rows"][0]
    assert selected["street"] == "22547 STORYBOOK CABIN WAY"
    assert selected["state"] == "FL"
    assert selected["agentName"] == "Ana Henriquez"
    assert selected["listing_description"].startswith("Short Sale")


def test_payload_webhook_enriches_search_only_rows_before_queue_and_seen(monkeypatch):
    detail_calls = []
    enqueued = []
    seen_batches = []

    search_only_row = {
        "zpid": "main-search-only",
        "detailUrl": "https://www.zillow.com/homedetails/main-search-only_zpid/",
        "address": "38 Jackson Avenue, North Plainfield, NJ 07060",
        "street": "38 Jackson Avenue",
        "city": "North Plainfield",
        "state": "NJ",
        "listingStatus": "forSale",
    }

    def fake_detail(rows, *, source="state-search"):
        detail_calls.append((source, [str(row.get("zpid")) for row in rows]))
        return [
            {
                **row,
                "agentName": "Detail Agent",
                "description": "Short sale subject to lender approval.",
            }
            for row in rows
        ]

    def fake_enqueue(rows, source):
        enqueued.extend((source, dict(row)) for row in rows)
        return len(rows)

    monkeypatch.setattr(webhook_server, "load_seen_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", fake_detail)
    monkeypatch.setattr(webhook_server, "_start_extra_state_rows", lambda payload: 0)
    monkeypatch.setattr(webhook_server, "_start_apify_coverage_backstop", lambda run_time: None)
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(webhook_server, "_process_pending_queue", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        webhook_server,
        "append_seen_zpids",
        lambda zpids: seen_batches.append([str(zpid) for zpid in zpids]),
    )
    monkeypatch.setattr(webhook_server, "_enqueue_pending_rows", fake_enqueue)
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 5)
    webhook_server.EXPORTED_ZPIDS.clear()

    result = asyncio.run(
        webhook_server.apify_hook(
            _FakeRequest({"listings": [search_only_row], "upstreamDatasetId": "dataset-1"})
        )
    )

    assert result["status"] == "processed"
    assert detail_calls == [("payload.listings", ["main-search-only"])]
    assert enqueued[0][0] == "payload.listings"
    assert enqueued[0][1]["description"] == "Short sale subject to lender approval."
    assert seen_batches == [["main-search-only"]]


def test_payload_webhook_holds_rows_when_detail_text_is_unavailable(monkeypatch):
    enqueued = []
    seen_batches = []

    search_only_row = {
        "zpid": "main-missing-detail",
        "detailUrl": "https://www.zillow.com/homedetails/main-missing-detail_zpid/",
        "address": "544 Pine Grove Rd, Gardners, PA 17324",
        "street": "544 Pine Grove Rd",
        "city": "Gardners",
        "state": "PA",
        "listingStatus": "forSale",
    }

    monkeypatch.setattr(webhook_server, "load_seen_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", lambda rows, **kwargs: [])
    monkeypatch.setattr(webhook_server, "_start_extra_state_rows", lambda payload: 0)
    monkeypatch.setattr(webhook_server, "_start_apify_coverage_backstop", lambda run_time: None)
    monkeypatch.setattr(webhook_server, "_within_initial_hours", lambda now: True)
    monkeypatch.setattr(webhook_server, "_drain_deferred_rows", lambda: [])
    monkeypatch.setattr(
        webhook_server,
        "_process_pending_queue",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("held rows should not process")),
    )
    monkeypatch.setattr(
        webhook_server,
        "append_seen_zpids",
        lambda zpids: seen_batches.append([str(zpid) for zpid in zpids]),
    )
    monkeypatch.setattr(webhook_server, "_enqueue_pending_rows", lambda rows, source: enqueued.extend(rows))
    monkeypatch.setattr(webhook_server, "APIFY_MAX_ITEMS", 5)
    webhook_server.EXPORTED_ZPIDS.clear()

    result = asyncio.run(
        webhook_server.apify_hook(
            _FakeRequest({"listings": [search_only_row], "upstreamDatasetId": "dataset-1"})
        )
    )

    assert result == {"status": "held_missing_listing_text", "rows": 0, "held_missing_text": 1}
    assert enqueued == []
    assert seen_batches == []


def test_coverage_backstop_does_not_enqueue_rows_without_detail_text(monkeypatch):
    enqueued = []
    search_only_row = {
        "zpid": "backstop-search-only",
        "detailUrl": "https://www.zillow.com/homedetails/backstop-search-only_zpid/",
        "address": "650 Jefferson Avenue NE, Salem, OR 97301",
        "street": "650 Jefferson Avenue NE",
        "city": "Salem",
        "state": "OR",
        "listingStatus": "forSale",
    }

    monkeypatch.setattr(webhook_server, "_coverage_backstop_skip_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", lambda rows: rows)
    monkeypatch.setattr(
        webhook_server,
        "_enqueue_pending_rows",
        lambda rows, source: enqueued.extend(rows) or len(rows),
    )

    count = webhook_server._enqueue_apify_backstop_rows(
        [search_only_row],
        source="coverage-backstop-main",
        max_rows=1,
    )

    assert count == 0
    assert enqueued == []


def test_state_search_does_not_enqueue_rows_without_detail_text(monkeypatch):
    enqueued = []
    state_row = {
        "zpid": "ak-search-only",
        "detailUrl": "https://www.zillow.com/homedetails/ak-search-only_zpid/",
        "address": "1521 N Plateau Ave, Palmer, AK 99645",
        "street": "1521 N Plateau Ave",
        "city": "Palmer",
        "state": "AK",
        "listingStatus": "forSale",
        "search_source": "ak",
    }

    monkeypatch.setattr(webhook_server, "_fetch_extra_state_rows", lambda: [state_row])
    monkeypatch.setattr(webhook_server, "_pending_queue_state_skip_zpids", lambda: set())
    monkeypatch.setattr(webhook_server, "_run_state_detail_task_for_rows", lambda rows: rows)
    monkeypatch.setattr(
        webhook_server,
        "_enqueue_pending_rows",
        lambda rows, source: enqueued.extend(rows) or len(rows),
    )
    monkeypatch.setattr(webhook_server, "APIFY_STATE_SEARCH_LIMIT", 5)

    count = webhook_server._enqueue_extra_state_rows({})

    assert count == 0
    assert enqueued == []


def test_queue_worker_fails_rows_without_listing_text_before_processing(monkeypatch):
    completions = []

    item = {
        "_row_num": 42,
        "zpid": "queued-search-only",
        "address": "808 Russet Valley Dr",
        "source": "coverage-backstop-main",
        "listing_json": json.dumps(
            {
                "zpid": "queued-search-only",
                "address": "808 Russet Valley Dr",
                "source": "coverage-backstop-main",
            }
        ),
    }

    monkeypatch.setattr(
        webhook_server,
        "process_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("missing text should not be classified")),
    )
    monkeypatch.setattr(
        webhook_server,
        "_complete_queue_item",
        lambda item, status, result="", error="": completions.append((status, result, error)),
    )

    webhook_server._process_claimed_queue_item(item)

    assert completions == [("failed", "", "missing_listing_text")]


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
    assert "source_ref=" in payload["sourceReference"]
    assert "http" not in json.dumps(payload).lower()


def test_pending_queue_serialization_removes_clickable_urls():
    payload = {
        "zpid": "hi-1",
        "source": "state-search",
        "address": "1 Ocean Ave",
        "detailUrl": "https://www.zillow.com/homedetails/hi-1_zpid/",
        "propertyUrl": "https://www.zillow.com/homedetails/hi-1_zpid/",
        "listing_description": "Short sale subject to lender approval. See https://example.com/details",
    }

    serialized = webhook_server._serialize_queue_payload(payload, "hi-1")
    parsed = json.loads(serialized)

    assert "detailUrl" not in parsed
    assert "propertyUrl" not in parsed
    assert "http" not in serialized.lower()
    assert "Short sale subject to lender approval" in parsed["listing_description"]


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
