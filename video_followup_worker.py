"""Opt-in bounded video worker. This module never sends SMS."""

import logging
import os
import threading

from video_followup import Config, Eligibility, Worker, get_service

LOG = logging.getLogger(__name__)
_stop = threading.Event()
_thread = None


def create_worker():
    config = Config()
    if not config.enabled or not config.generation_enabled:
        return None
    if not config.origin or not config.bucket or not config.not_before:
        raise ValueError("Video hosting and activation cutoff must be configured before generation")
    if not config.live and not config.allowlist:
        raise ValueError("Owner-only generation requires the exact test phone allowlist")
    api_key = os.getenv("HEYGEN_API_KEY", "").strip()
    url = os.getenv("TASKER_TRANSPORT_HEALTH_URL", "").strip()
    token = os.getenv("SMS_CHATBOT_ALLOWED_TOKEN", "").strip()
    if not api_key or not url or not token:
        raise ValueError("Video API access and eligibility checking must be configured")
    from video_followup_provider import HeyGenVideoProvider
    return Worker(get_service().store, HeyGenVideoProvider(api_key), config, Eligibility(url, token))


def start_worker():
    global _thread
    if _thread and _thread.is_alive():
        return
    # No provider or storage clients are initialized when this feature is off.
    try:
        worker = create_worker()
    except Exception as exc:
        LOG.error("VIDEO_WORKER_NOT_STARTED error_type=%s; existing SMS remains unchanged", type(exc).__name__)
        return
    if worker is None:
        return
    _stop.clear()

    def run():
        while not _stop.is_set():
            try:
                worker.tick()
            except Exception as exc:
                LOG.warning("VIDEO_WORKER_RETRY error_type=%s", type(exc).__name__)
            _stop.wait(60)
    _thread = threading.Thread(target=run, name="video-preparation", daemon=True)
    _thread.start()


def stop_worker():
    _stop.set()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = create_worker()
    if worker is None:
        raise SystemExit("Video generation is disabled; no work performed")
    worker.tick()
