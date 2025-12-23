"""Standalone worker to run the hourly follow-up scheduler in a long-lived process.

This worker is useful for deployments that do not start the FastAPI webhook server
(e.g. cron jobs or direct calls into ``bot_min.process_rows``). It simply imports
``bot_min`` and keeps the hourly scheduler running so follow-up passes and other
scheduled jobs continue to execute.
"""

import logging
import signal
import threading
from types import FrameType
from typing import Optional

from bot_min import run_hourly_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("scheduler_worker")


_stop_event = threading.Event()


def _handle_sigterm(signum: int, frame: Optional[FrameType]) -> None:
    logger.info("Received signal %s – stopping hourly scheduler", signum)
    _stop_event.set()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


if __name__ == "__main__":
    logger.info("Starting standalone hourly scheduler worker")
    run_hourly_scheduler(stop_event=_stop_event)
    logger.info("Scheduler worker exiting")
