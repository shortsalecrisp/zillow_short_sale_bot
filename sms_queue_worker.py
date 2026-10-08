"""Dedicated Render worker for the verifier's initial-SMS queue.

The web process remains responsible for HTTP traffic and listing ingestion. This
worker claims the Sheet-backed queue independently so scraper/browser work cannot
starve SMS delivery or create a second claimant in the web scheduler.
"""

import logging
import os
import signal
import threading

from webhook_server import (
    _alert_stale_initial_sms_queue_items,
    _process_initial_sms_queue,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("sms_queue_worker")

POLL_SECONDS = max(15, int(os.getenv("SMS_QUEUE_WORKER_POLL_SECONDS", "30")))
BATCH_SIZE = max(1, min(100, int(os.getenv("SMS_QUEUE_WORKER_BATCH_SIZE", "10"))))
_stop_event = threading.Event()


def _stop(signum, _frame) -> None:
    logger.info("Received signal %s; stopping SMS queue worker", signum)
    _stop_event.set()


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def run() -> None:
    logger.info(
        "Initial SMS queue worker starting poll_seconds=%s batch_size=%s",
        POLL_SECONDS,
        BATCH_SIZE,
    )
    while not _stop_event.is_set():
        try:
            processed = _process_initial_sms_queue(max_items=BATCH_SIZE)
            if processed:
                logger.info("Initial SMS queue worker processed=%s", processed)
        except Exception:
            logger.exception("Initial SMS queue worker cycle failed")
            _alert_stale_initial_sms_queue_items()
        _stop_event.wait(POLL_SECONDS)
    logger.info("Initial SMS queue worker stopped")


if __name__ == "__main__":
    run()
