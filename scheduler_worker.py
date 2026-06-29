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

from bot_min import SCHEDULER_TZ, WORK_END, WORK_START, run_hourly_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("scheduler_worker")


_stop_event = threading.Event()
_free_source_pilot_lock = threading.Lock()


def _should_run_immediately() -> bool:
    return os.getenv("SCHEDULER_RUN_IMMEDIATELY", "false").lower() == "true"


def _handle_sigterm(signum: int, frame: Optional[FrameType]) -> None:
    logger.info("Received signal %s – stopping hourly scheduler", signum)
    _stop_event.set()


def _free_source_pilot_enabled() -> bool:
    return os.getenv("FREE_SOURCE_PILOT_ENABLED", "true").lower() == "true"


def _free_source_pilot_callback(run_time: datetime) -> None:
    if not _free_source_pilot_enabled():
        return
    local_dt = run_time.astimezone(SCHEDULER_TZ)
    if local_dt.hour < WORK_START or local_dt.hour > WORK_END:
        return
    if not _free_source_pilot_lock.acquire(blocking=False):
        logger.info("free-source-pilot: skipped overlapping worker run")
        return

    def _runner() -> None:
        try:
            states = [
                state.strip().upper()
                for state in os.getenv("FREE_SOURCE_PILOT_STATES", "FL,CA,TX,WA,PA,HI,GA,MI").split(",")
                if state.strip()
            ]
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
            completed = subprocess.run(
                cmd,
                cwd=os.path.dirname(__file__),
                capture_output=True,
                text=True,
                timeout=50 * 60,
                check=False,
            )
            if completed.returncode:
                logger.error(
                    "free-source-pilot: failed returncode=%s stdout=%s stderr=%s",
                    completed.returncode,
                    completed.stdout[-4000:],
                    completed.stderr[-4000:],
                )
            else:
                logger.info("free-source-pilot: completed stdout=%s", completed.stdout[-4000:])
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
