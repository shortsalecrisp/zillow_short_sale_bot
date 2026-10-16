"""Opt-in bounded video worker. This module never sends SMS."""

import logging
import os
import threading

from video_followup import SCRIPT_VERSION, Config, Eligibility, Worker, get_service

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
    fal_key = os.getenv("FAL_API_KEY", "").strip()
    tts_key = os.getenv("FAL_TTS_API_KEY", "").strip()
    master_asset_key = os.getenv("VIDEO_FOLLOWUP_SAME_TAKE_MASTER_KEY", "").strip()
    url = os.getenv("TASKER_TRANSPORT_HEALTH_URL", "").strip()
    token = os.getenv("SMS_CHATBOT_ALLOWED_TOKEN", "").strip()
    if not fal_key or not tts_key or not master_asset_key or not url or not token:
        raise ValueError("Video API access and eligibility checking must be configured")
    from video_followup_same_take import SameTakeVideoProvider, _generator_profile
    from video_followup_speech_guard import LEGACY_VERIFICATION_METHOD, PLANNED_AUDIO_VERIFICATION_METHOD
    generation_profile = os.getenv("VIDEO_FOLLOWUP_GENERATION_PROFILE", "legacy_v1").strip()
    _generator_profile(generation_profile)
    verification_method = os.getenv("VIDEO_FOLLOWUP_AUDIO_VERIFICATION_METHOD", LEGACY_VERIFICATION_METHOD).strip()
    if verification_method not in {LEGACY_VERIFICATION_METHOD, PLANNED_AUDIO_VERIFICATION_METHOD}:
        raise ValueError("Audio verification method is not approved")
    provider_options = {"tts_key": tts_key, "generator_profile": generation_profile}
    if verification_method == PLANNED_AUDIO_VERIFICATION_METHOD:
        provider_options["verification_method"] = verification_method
    store = get_service().store
    provider = SameTakeVideoProvider(fal_key, store, master_asset_key, **provider_options)
    return Worker(store, provider, config, Eligibility(url, token))


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
    LOG.info("VIDEO_WORKER_STARTED script_version=%s poll_interval_seconds=60", SCRIPT_VERSION)


def stop_worker():
    _stop.set()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = create_worker()
    if worker is None:
        raise SystemExit("Video generation is disabled; no work performed")
    worker.tick()
