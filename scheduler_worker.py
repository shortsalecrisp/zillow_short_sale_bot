"""Standalone worker to run the hourly follow-up scheduler in a long-lived process.

This worker is useful for deployments that do not start the FastAPI webhook server
(e.g. cron jobs or direct calls into ``bot_min.process_rows``). It simply imports
``bot_min`` and keeps the hourly scheduler running so follow-up passes and other
scheduled jobs continue to execute.
"""

import logging
import os
import signal
import threading
from types import FrameType
from typing import Callable, Optional

from bot_min import run_hourly_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("scheduler_worker")


ENABLE_APIFY_HOURLY = os.getenv("ENABLE_APIFY_HOURLY", "false").lower() == "true"
_stop_event = threading.Event()


def _should_run_immediately() -> bool:
    return os.getenv("SCHEDULER_RUN_IMMEDIATELY", "false").lower() == "true"


def _handle_sigterm(signum: int, frame: Optional[FrameType]) -> None:
    logger.info("Received signal %s – stopping hourly scheduler", signum)
    _stop_event.set()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


def _load_apify_hourly_callback() -> Optional[Callable]:
    if not ENABLE_APIFY_HOURLY:
        logger.info("ENABLE_APIFY_HOURLY disabled; running follow-up scheduler only")
        return None

    try:
        from webhook_server import _apify_hourly_task  # type: ignore
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warning(
            "ENABLE_APIFY_HOURLY set but Apify task import failed; "
            "continuing without Apify hourly runs: %s",
            exc,
        )
        return None

    logger.info("Apify hourly task enabled via ENABLE_APIFY_HOURLY")
    return _apify_hourly_task


if __name__ == "__main__":
    logger.info("Starting standalone hourly scheduler worker")
    apify_cb = _load_apify_hourly_callback()
    callbacks = [apify_cb] if apify_cb else None
    run_hourly_scheduler(
        stop_event=_stop_event,
        hourly_callbacks=callbacks,
        run_immediately=_should_run_immediately(),
        initial_callbacks=bool(callbacks),
    )
    logger.info("Scheduler worker exiting")
