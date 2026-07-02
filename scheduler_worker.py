"""Standalone worker to run the hourly follow-up scheduler in a long-lived process.

This worker is useful for deployments that do not start the FastAPI webhook server
(e.g. cron jobs or direct calls into ``bot_min.process_rows``). It simply imports
``bot_min`` and keeps the hourly scheduler running so follow-up passes and other
scheduled jobs continue to execute.
"""

import logging
import os
import signal
import subprocess
import sys
import threading
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


def _free_source_pilot_callback(run_time: datetime) -> None:
    if not _free_source_pilot_enabled():
        return
    local_dt = run_time.astimezone(SCHEDULER_TZ)
    if local_dt.hour != _free_source_pilot_run_hour() or local_dt.minute != _free_source_pilot_run_minute():
        return
    if not _free_source_pilot_lock.acquire(blocking=False):
        logger.info("free-source-pilot: skipped overlapping worker run")
        return

    def _runner() -> None:
        try:
            states = _free_source_pilot_states()
            if not states:
                logger.info("free-source-pilot: skipped no states configured")
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
            ]
            process = subprocess.Popen(
                cmd,
                cwd=os.path.dirname(__file__),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
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
                process.kill()
                logger.error("free-source-pilot: timed out after %.0fs", exc.timeout)
                return
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)
            if returncode:
                logger.error("free-source-pilot: failed returncode=%s", returncode)
            else:
                logger.info("free-source-pilot: completed returncode=%s", returncode)
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
