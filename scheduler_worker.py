"""Standalone worker to run the hourly follow-up scheduler in a long-lived process.

This worker is useful for deployments that do not start the FastAPI webhook server
(e.g. cron jobs or direct calls into ``bot_min.process_rows``). It simply imports
``bot_min`` and keeps the hourly scheduler running so follow-up passes and other
scheduled jobs continue to execute.
"""

import hashlib
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from types import FrameType
from typing import Optional

from bot_min import SCHEDULER_TZ, run_hourly_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("scheduler_worker")


_stop_event = threading.Event()
_free_source_pilot_lock = threading.Lock()

ALL_50_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]


def _should_run_immediately() -> bool:
    return os.getenv("SCHEDULER_RUN_IMMEDIATELY", "false").lower() == "true"


def _handle_sigterm(signum: int, frame: Optional[FrameType]) -> None:
    logger.info("Received signal %s – stopping hourly scheduler", signum)
    _stop_event.set()


def _free_source_pilot_enabled() -> bool:
    return os.getenv("FREE_SOURCE_PILOT_ENABLED", "true").lower() == "true"


def _free_source_pilot_run_hour() -> int:
    return int(os.getenv("FREE_SOURCE_PILOT_RUN_HOUR", "9"))


def _free_source_pilot_run_minute() -> int:
    return int(os.getenv("FREE_SOURCE_PILOT_RUN_MINUTE", "0"))


def _free_source_pilot_states() -> list[str]:
    configured = [
        state.strip().upper()
        for state in os.getenv("FREE_SOURCE_PILOT_STATES", "").split(",")
        if state.strip()
    ]
    force_all_states = os.getenv("FREE_SOURCE_PILOT_FORCE_ALL_STATES", "false").lower() == "true"
    if force_all_states or not configured:
        return list(ALL_50_STATES)
    return configured


def _log_subprocess_lines(pipe, level: int, prefix: str) -> None:
    if pipe is None:
        return
    try:
        for line in iter(pipe.readline, ""):
            cleaned = line.rstrip()
            if cleaned:
                logger.log(level, "%s%s", prefix, cleaned)
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def _free_source_pilot_run_receipt_id(run_time: datetime) -> str:
    local_time = run_time.astimezone(SCHEDULER_TZ)
    return hashlib.sha256(
        f"free-source-pilot|source|{local_time.isoformat()}|{os.getpid()}|{time.time_ns()}".encode("utf-8")
    ).hexdigest()[:16]


def _log_pilot_wrapper_terminal(run_time: datetime, run_receipt_id: str, status: str, returncode=None, error="") -> None:
    logger.info(
        "free-source-pilot: wrapper %s",
        json.dumps(
            {
                "event": "pilot_scheduler_terminal",
                "run_receipt_id": run_receipt_id,
                "run_date": run_time.astimezone(SCHEDULER_TZ).date().isoformat(),
                "observed_at": datetime.now(tz=SCHEDULER_TZ).isoformat(),
                "status": status,
                "returncode": returncode,
                "error": str(error)[:500],
            },
            sort_keys=True,
        ),
    )


def _terminate_pilot_process_group(process: subprocess.Popen, grace_seconds: float = 10.0) -> None:
    process_group_id = process.pid
    try:
        os.killpg(process_group_id, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + grace_seconds
    group_alive = True
    while time.monotonic() < deadline:
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            group_alive = False
            break
        time.sleep(0.1)
    try:
        if group_alive:
            os.killpg(process_group_id, signal.SIGKILL)
    except ProcessLookupError:
        pass
    if process.poll() is None:
        process.wait(timeout=5)
    else:
        process.wait()


def _free_source_pilot_callback(run_time: datetime) -> None:
    if not _free_source_pilot_enabled():
        return
    local_dt = run_time.astimezone(SCHEDULER_TZ)
    if local_dt.hour != _free_source_pilot_run_hour() or local_dt.minute != _free_source_pilot_run_minute():
        return
    run_receipt_id = _free_source_pilot_run_receipt_id(run_time)
    schedule_slot_id = f"source:{local_dt.date().isoformat()}"
    if not _free_source_pilot_lock.acquire(blocking=False):
        logger.info("free-source-pilot: skipped overlapping worker run")
        _log_pilot_wrapper_terminal(run_time, run_receipt_id, "skipped_overlap")
        return

    def _runner() -> None:
        process = None
        try:
            states = _free_source_pilot_states()
            if not states:
                logger.info("free-source-pilot: skipped no states configured")
                _log_pilot_wrapper_terminal(run_time, run_receipt_id, "skipped_no_states")
                return
            script_path = os.path.join(os.path.dirname(__file__), "scripts", "free_short_sale_source_pilot.py")
            cmd = [
                sys.executable,
                script_path,
                "--spreadsheet-id",
                os.getenv("GSHEET_ID", "12UzsoQCo4W0WB_lNl3BjKpQ_wXNhEH7xegkFRVu2M70"),
                "--main-tab",
                os.getenv("GSHEET_TAB", "Sheet1"),
                "--pilot-tab",
                os.getenv("FREE_SOURCE_PILOT_TAB", "Lead Source Pilot"),
                "--states",
                *states,
                "--results-per-query",
                os.getenv("FREE_SOURCE_PILOT_RESULTS_PER_QUERY", "10"),
                "--sleep-seconds",
                os.getenv("FREE_SOURCE_PILOT_SLEEP_SECONDS", "1.0"),
                "--run-date",
                local_dt.date().isoformat(),
                "--scheduled-run",
                "--run-receipt-id",
                run_receipt_id,
                "--schedule-slot-id",
                schedule_slot_id,
            ]
            process = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(__file__),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
            stdout_thread = threading.Thread(
                target=_log_subprocess_lines,
                args=(process.stdout, logging.INFO, "free-source-pilot: stdout "),
                name="free-source-pilot-stdout",
                daemon=True,
            )
            stderr_thread = threading.Thread(
                target=_log_subprocess_lines,
                args=(process.stderr, logging.WARNING, "free-source-pilot: stderr "),
                name="free-source-pilot-stderr",
                daemon=True,
            )
            stdout_thread.start()
            stderr_thread.start()
            try:
                returncode = process.wait(timeout=50 * 60)
            except subprocess.TimeoutExpired as exc:
                cleanup_error = ""
                try:
                    _terminate_pilot_process_group(process)
                except Exception as cleanup_exc:
                    cleanup_error = f"; cleanup_error={type(cleanup_exc).__name__}: {cleanup_exc}"
                    logger.exception("free-source-pilot: timeout cleanup failed")
                stdout_thread.join(timeout=5)
                stderr_thread.join(timeout=5)
                logger.error("free-source-pilot: timed out after %.0fs", exc.timeout)
                _log_pilot_wrapper_terminal(
                    run_time,
                    run_receipt_id,
                    "failed_timeout",
                    error=f"timeout after {exc.timeout:.0f}s{cleanup_error}",
                )
                return
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)
            if returncode:
                logger.error("free-source-pilot: failed returncode=%s", returncode)
                _log_pilot_wrapper_terminal(run_time, run_receipt_id, "failed_nonzero", returncode=returncode)
            else:
                logger.info("free-source-pilot: completed returncode=%s", returncode)
                _log_pilot_wrapper_terminal(
                    run_time, run_receipt_id, "process_exited_zero_child_receipt_required", returncode=returncode
                )
        except Exception as exc:
            logger.exception("free-source-pilot: crashed")
            _log_pilot_wrapper_terminal(run_time, run_receipt_id, "failed_spawn_or_wrapper", error=exc)
        finally:
            _free_source_pilot_lock.release()

    threading.Thread(target=_runner, name="free-source-pilot", daemon=True).start()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


if __name__ == "__main__":
    logger.info("Starting standalone hourly scheduler worker")
    hourly_callbacks = [_free_source_pilot_callback] if _free_source_pilot_enabled() else None
    run_hourly_scheduler(
        stop_event=_stop_event,
        hourly_callbacks=hourly_callbacks,
        run_immediately=_should_run_immediately(),
        initial_callbacks=False,
    )
    logger.info("Scheduler worker exiting")
