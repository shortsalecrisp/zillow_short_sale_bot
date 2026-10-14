"""At-most-once audio greeting + same-recording lip-sync pipeline.

Only the approved real recording supplies video and the voice reference. Every
paid stage is claimed before submission; unknown outcomes are never resubmitted.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode, urlsplit

import requests

from video_followup import Conflict, SCRIPT_VERSION, iso, normalize_name, utcnow, valid_name
from video_followup_provider import (
    MAX_API_BYTES, REQUEST_TIMEOUT, SubmissionUncertain, VideoProviderError, _public_addresses,
)
from video_followup_audio_guard import GUARD_VERSION, inspect_reference, inspect_greeting
from video_followup_same_take_media import prepare_inputs, compose

MASTER_SHA256 = "01efd28101e8b016d51d571cc87d38d13ea623389663bc7a88cf26cce39bd60b"
MODEL = "fal-ai/kling-video/lipsync/audio-to-video"
QUEUE = "https://queue.fal.run/"
REQUEST_BASE = QUEUE + "fal-ai/kling-video/requests/"
TTS_MODEL = "fal-ai/chatterbox/text-to-speech/turbo"
TTS_REQUEST_BASE = QUEUE + "fal-ai/chatterbox/requests/"
REFERENCE_SHA256 = "5fb066ad56f37021ce47d7ea5d182c2f9f097de4e1d21a3ffd22470f7a5f7387"
REFERENCE_KEY = "assets/voice-references/" + REFERENCE_SHA256 + "/real-recording.wav"
TTS_SEED = 4411
TTS_TEMPERATURE = 0.5
TTS_REQUEST_CENTS = 2
LIPSYNC_REQUEST_CENTS = 7
RESERVATION_CENTS = 10
PRICING_ENDPOINT = "https://api.fal.ai/v1/models/pricing"
APPROVED_RATES = {TTS_MODEL: ("1000 characters", Decimal("0.02")),
                  MODEL: ("units", Decimal("0.014"))}
RENDER_CONFIG = {"tts_model": TTS_MODEL, "seed": TTS_SEED, "temperature": TTS_TEMPERATURE,
                 "reference_sha256": REFERENCE_SHA256, "lipsync_model": MODEL,
                 "greeting_pattern": "Hey {first_name}!", "audio_guard": GUARD_VERSION,
                 "tts_request_cents": TTS_REQUEST_CENTS, "lipsync_request_cents": LIPSYNC_REQUEST_CENTS}
RENDER_CONFIG_SHA256 = hashlib.sha256(json.dumps(RENDER_CONFIG, sort_keys=True).encode()).hexdigest()
MAX_BYTES = 25 * 1024 * 1024
_REQUEST_ID = re.compile(r"[a-zA-Z0-9-]{16,64}")
_PIPELINE_ID = re.compile(r"same-take-[a-f0-9]{32}")
_PREPARATION_FAILURE_CODES = frozenset({
    "reference_mismatch", "unapproved_voice_reference", "invalid_voice_reference_duration",
    "unsafe_fal_media", "fal_download_failed", "fal_download_empty", "invalid_narration_audio",
    "clipped_narration_audio", "silent_narration_audio", "quiet_narration_audio",
    "uncertain_narration_pitch", "narration_pitch_outlier", "invalid_greeting_timing",
    "greeting_too_long", "narration_exceeds_budget", "invalid_media_bytes", "unapproved_recording",
    "noncanonical_first_name", "media_processing_failed", "invalid_video_frames", "invalid_source_frames",
    "invalid_audio", "silent_greeting", "invalid_source_audio", "invalid_context_timing",
    "input_budget_mismatch", "input_hash_mismatch", "asset_conflict",
})
_PREPARATION_FAILURE_STAGES = frozenset({
    "reference_read", "audio_download", "audio_screen", "audio_duration", "media_prepare",
    "prepared_validation", "asset_storage", "asset_signing",
})
_DIAGNOSTIC_EXCEPTION_TYPES = frozenset({
    "VideoProviderError", "Conflict", "ValueError", "TypeError", "KeyError", "OSError",
    "RuntimeError", "TimeoutError", "MemoryError", "OverflowError", "HTTPError",
    "ConnectionError", "ReadTimeout", "ConnectTimeout", "Timeout",
})


def _preparation_failure_diagnostics(exc, stage, audio_qa):
    code = getattr(exc, "code", None) if isinstance(exc, VideoProviderError) else None
    exception_type = type(exc).__name__
    details = {
        "failure_stage": stage if stage in _PREPARATION_FAILURE_STAGES else "unknown_preparation_stage",
        "underlying_code": code if isinstance(code, str) and code in _PREPARATION_FAILURE_CODES else "unclassified_preparation_failure",
        "exception_type": exception_type if exception_type in _DIAGNOSTIC_EXCEPTION_TYPES else "OtherException",
    }
    if isinstance(audio_qa, dict):
        quality = audio_qa.get("quality")
        values = {**(quality if isinstance(quality, dict) else {}),
                  "duration_seconds": audio_qa.get("duration_seconds")}
        limits = {"duration_seconds": (0, 10), "clipped_sample_fraction": (0, 1),
                  "active_rms_dbfs": (-160, 0), "median_f0_hz": (70, 400),
                  "reference_median_f0_hz": (70, 400), "pitch_shift_semitones": (-12, 12),
                  "prepared_greeting_seconds": (0.5, 2.5)}
        measured = {name: values[name] for name, (low, high) in limits.items()
                    if type(values.get(name)) in (int, float) and low <= values[name] <= high
                    and math.isfinite(values[name])}
        if measured:
            details["measured_audio_qa"] = measured
    return details


def _failed_status(state):
    result = {"provider_id": state["id"], "status": "failed",
              "failure_code": state.get("failure_code", state["stage"])}
    if result["failure_code"] == "greeting_preparation_failed":
        for field, allowed, fallback in (
                ("failure_stage", _PREPARATION_FAILURE_STAGES, "unknown_preparation_stage"),
                ("underlying_code", _PREPARATION_FAILURE_CODES, "unclassified_preparation_failure"),
                ("exception_type", _DIAGNOSTIC_EXCEPTION_TYPES, "OtherException")):
            value = state.get(field)
            result[field] = value if isinstance(value, str) and value in allowed else fallback
    return result


def fal_media_url(url, resolve=None):
    try:
        p = urlsplit(url)
        if not (p.scheme == "https" and p.port in (None, 443) and not p.username
                and not p.password and not p.fragment
                and re.fullmatch(r"(?:v\d+[a-z]?\.)?fal\.media", p.hostname or "")
                and p.path.startswith("/files/") and len(url) < 8192
                and not any(ord(c) < 33 or c == "\\" for c in url)):
            raise ValueError()
        if resolve:
            addresses = resolve(p.hostname)
            if not addresses or any(not ipaddress.ip_address(a).is_global for a in addresses):
                raise ValueError()
    except (ValueError, TypeError, OSError):
        raise VideoProviderError("unsafe_fal_media", "Fal output address is not permitted.") from None
    return url


def tts_request_url(request_id, url=None, *, status=False):
    if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
        raise VideoProviderError("tts_request_mismatch", "Narration request ID did not match.")
    suffix = "/status" if status else ""
    canonical = TTS_REQUEST_BASE + request_id + suffix
    if url is None:
        return canonical
    try:
        p = urlsplit(url)
        expected_path = "/fal-ai/chatterbox/requests/" + request_id + suffix
        if not (p.scheme == "https" and p.hostname == "queue.fal.run"
                and p.port in (None, 443) and not p.username and not p.password
                and not p.query and not p.fragment and p.path == expected_path
                and not any(ord(c) < 33 or c == "\\" for c in url)):
            raise ValueError()
    except (ValueError, TypeError):
        raise VideoProviderError("unsafe_tts_request", "Narration polling address is not permitted.") from None
    return "https://queue.fal.run" + p.path


class SameTakeVideoProvider:
    def __init__(self, fal_key, store, master_asset_key, *, tts_key, reference_asset_key=REFERENCE_KEY, session=None,
                 media_session=None, resolver=None, preparer=prepare_inputs, compositor=compose):
        if (not fal_key or not tts_key or not master_asset_key.startswith("assets/")
                or reference_asset_key != REFERENCE_KEY
                or TTS_REQUEST_CENTS + LIPSYNC_REQUEST_CENTS > RESERVATION_CENTS):
            raise VideoProviderError("same_take_unconfigured", "Same-recording integration is incomplete.")
        self.store, self.master_key = store, master_asset_key
        self.fal_key, self.tts_key, self.reference_key = fal_key, tts_key, reference_asset_key
        self.session, self.media_session = session or requests.Session(), media_session or requests.Session()
        if session is None:
            self.session.trust_env = False
        if media_session is None:
            self.media_session.trust_env = False
        self.resolver = resolver or _public_addresses
        self.preparer, self.compositor = preparer, compositor

    def _master(self):
        content, _ = self.store.objects.read(self.master_key)
        if not content or hashlib.sha256(content).hexdigest() != MASTER_SHA256:
            raise VideoProviderError("master_mismatch", "Approved second-recording master did not match.")
        return content

    def _reference(self):
        content, _ = self.store.objects.read(self.reference_key)
        if not content or hashlib.sha256(content).hexdigest() != REFERENCE_SHA256:
            raise VideoProviderError("reference_mismatch", "Approved real-recording voice reference did not match.")
        inspect_reference(content)
        return content

    def _read(self, pipeline_id):
        if not _PIPELINE_ID.fullmatch(str(pipeline_id)):
            raise VideoProviderError("invalid_pipeline", "Same-recording pipeline ID is invalid.")
        state, version = self.store.read("same-take/" + pipeline_id)
        if not state or state.get("source_sha256") != MASTER_SHA256 or state.get("script_version") != SCRIPT_VERSION:
            raise VideoProviderError("pipeline_mismatch", "Same-recording pipeline identity did not match.")
        expected = "same-take-" + hashlib.sha256((SCRIPT_VERSION + ":" + str(state.get("job_key"))).encode()).hexdigest()[:32]
        if (state.get("id") != pipeline_id or pipeline_id != expected
                or not valid_name(state.get("first_name"))
                or normalize_name(state["first_name"]) != state["first_name"]
                or state.get("reserved_cents") != RESERVATION_CENTS
                or state.get("reference_sha256") != REFERENCE_SHA256
                or state.get("render_config_sha256") != RENDER_CONFIG_SHA256
                or state.get("greeting_script") != "Hey " + state["first_name"] + "!"):
            raise VideoProviderError("pipeline_mismatch", "Pipeline identity or budget changed.")
        return state, version

    def _update(self, pipeline_id, fields, expected_stage=None):
        self._read(pipeline_id)
        return self.store.mutate("same-take/" + pipeline_id, lambda old:
            {**old, **fields} if expected_stage is None or old.get("stage") == expected_stage else None)

    def _active(self, state):
        job, _ = self.store.read("jobs/" + state["job_key"])
        return bool(job and job.get("script_version") == SCRIPT_VERSION
                    and job.get("status") in {"submitting", "submitted", "processing"}
                    and normalize_name(job.get("first_name")) == state["first_name"])

    def _json(self, method, url, *, headers, payload=None, paid=False):
        try:
            response = self.session.request(method, url, headers=headers, json=payload,
                                            timeout=REQUEST_TIMEOUT, allow_redirects=False)
        except requests.RequestException:
            if paid:
                raise SubmissionUncertain("same-take", "paid_stage_uncertain") from None
            raise VideoProviderError("status_unavailable", "Provider status is temporarily unavailable.") from None
        try:
            if not 200 <= response.status_code < 300 or len(response.content) > MAX_API_BYTES:
                if paid:
                    raise SubmissionUncertain("same-take", "paid_stage_unconfirmed")
                raise VideoProviderError("status_unavailable", "Provider status is temporarily unavailable.")
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError()
            return value
        except (ValueError, TypeError):
            if paid:
                raise SubmissionUncertain("same-take", "paid_response_invalid") from None
            raise VideoProviderError("status_invalid", "Provider status was invalid.") from None
        finally:
            response.close()

    def _verified_price(self, model, key):
        unit, maximum = APPROVED_RATES[model]
        value = self._json("GET", PRICING_ENDPOINT + "?" + urlencode({"endpoint_id": model}),
            headers={"Authorization": "Key " + key, "Cache-Control": "no-cache"})
        try:
            prices = value["prices"]
            if (not isinstance(prices, list) or len(prices) != 1
                    or value.get("has_more") is not False or value.get("next_cursor") is not None):
                raise ValueError()
            price = prices[0]
            amount = Decimal(str(price["unit_price"]))
            if (price["endpoint_id"] != model or price["currency"] != "USD" or price["unit"] != unit
                    or not amount.is_finite() or not Decimal("0") < amount <= maximum):
                raise ValueError()
        except (ValueError, TypeError, KeyError, InvalidOperation):
            raise VideoProviderError("unapproved_provider_price", "Provider pricing is unknown or exceeds the approved limit.") from None
        return {"endpoint_id": model, "unit": unit, "currency": "USD", "unit_price": str(amount),
                "maximum_unit_price": str(maximum), "verified_at": iso(utcnow())}

    def _price_before_payment(self, pipeline_id, stage, model, key, field):
        try:
            rate = self._verified_price(model, key)
            saved = self._update(pipeline_id, {field: rate}, stage)
            age = (utcnow() - datetime.fromisoformat(rate["verified_at"].replace("Z", "+00:00"))).total_seconds()
            if saved["stage"] != stage or not 0 <= age <= 60:
                raise VideoProviderError("stale_provider_price", "Fresh provider pricing is required before payment.")
            return rate
        except Exception:
            self._update(pipeline_id, {"stage": "failed", "failure_code": "provider_price_not_approved"}, stage)
            raise

    def submit(self, first_name, job_id):
        first_name = normalize_name(first_name)
        if not valid_name(first_name) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", job_id):
            raise VideoProviderError("invalid_identity", "Verified first name and job identity are required.")
        self._master()
        self._reference()
        pipeline_id = "same-take-" + hashlib.sha256((SCRIPT_VERSION + ":" + job_id).encode()).hexdigest()[:32]
        old, _ = self.store.read("same-take/" + pipeline_id)
        if old:
            old, _ = self._read(pipeline_id)
            if old.get("first_name") != first_name or old.get("job_key") != job_id:
                raise VideoProviderError("identity_changed", "Existing greeting belongs to a different name.")
            return pipeline_id
        state = {"id": pipeline_id, "job_key": job_id, "first_name": first_name,
                 "source_sha256": MASTER_SHA256, "script_version": SCRIPT_VERSION,
                 "stage": "narration_submitting", "created_at": iso(utcnow()),
                 "reference_sha256": REFERENCE_SHA256, "render_config_sha256": RENDER_CONFIG_SHA256,
                 "reserved_cents": RESERVATION_CENTS, "greeting_script": f"Hey {first_name}!"}
        # The observed minimum TTS billing block is 1,000 characters (2 cents).
        if len(state["greeting_script"]) > 1000:
            raise VideoProviderError("tts_exceeds_budget", "Greeting exceeds the approved narration billing block.")
        reference_url = self.store.objects.signed_get_url(self.reference_key)
        if not self._active(state):
            raise VideoProviderError("inactive_job", "Video job is no longer active.")
        self.store.write("same-take/" + pipeline_id, state, 0)
        self._price_before_payment(pipeline_id, "narration_submitting", TTS_MODEL, self.tts_key, "tts_price")
        if not self._active(state):
            self._update(pipeline_id, {"stage": "cancelled"}, "narration_submitting")
            return pipeline_id
        try:
            result = self._json("POST", QUEUE + TTS_MODEL, paid=True,
                headers={"Authorization": "Key " + self.tts_key, "X-Fal-No-Retry": "1",
                         "X-Fal-Request-Start-Timeout": "600"},
                payload={"text": state["greeting_script"], "audio_url": reference_url,
                         "seed": TTS_SEED, "temperature": TTS_TEMPERATURE})
            narration_id = result.get("request_id")
            if not isinstance(narration_id, str) or not _REQUEST_ID.fullmatch(narration_id):
                raise SubmissionUncertain(pipeline_id)
            status_url = tts_request_url(narration_id, result.get("status_url"), status=True)
            result_url = tts_request_url(narration_id, result.get("response_url"))
            self._update(pipeline_id, {"stage": "narration_pending", "narration_id": narration_id,
                                      "narration_status_url": status_url, "narration_result_url": result_url},
                         "narration_submitting")
        except Exception:
            self._update(pipeline_id, {"stage": "uncertain", "failure_code": "narration_submission_unconfirmed"}, "narration_submitting")
            raise
        return pipeline_id

    def _asset(self, pipeline_id, kind, content, mime):
        key = f"assets/{pipeline_id}/{kind}"
        try:
            self.store.objects.write(key, content, 0, mime)
        except Conflict:
            old, _ = self.store.objects.read(key)
            if old != content:
                raise VideoProviderError("asset_conflict", "Prepared greeting asset changed.")
        return key

    def _start_fal(self, state, version, narration):
        pipeline_id = state["id"]
        if not self._active(state):
            return self._update(pipeline_id, {"stage": "cancelled"}, "narration_pending")
        master = self._master()
        failure_stage, audio_qa = "reference_read", None
        try:
            reference = self._reference()
            failure_stage = "audio_download"
            audio_source = self._download_fal(narration["media_url"])
            failure_stage = "audio_screen"
            audio_qa = inspect_greeting(audio_source, reference)
            failure_stage = "audio_duration"
            duration = float(audio_qa["duration_seconds"])
            if not math.isfinite(duration) or not 0 < duration <= 3.0:
                raise VideoProviderError("narration_exceeds_budget", "Narration duration is outside the approved limit.")
            failure_stage = "media_prepare"
            inputs = self.preparer(master, audio_source, first_name=state["first_name"])
            failure_stage = "prepared_validation"
            metadata = inputs["metadata"]
            if (metadata.get("source_sha256") != MASTER_SHA256
                    or metadata.get("first_name") != state["first_name"]
                    or not 2 <= float(metadata.get("context_seconds", 0)) <= 5
                    or TTS_REQUEST_CENTS + LIPSYNC_REQUEST_CENTS > RESERVATION_CENTS):
                raise VideoProviderError("input_budget_mismatch", "Prepared name, source or billable duration did not match.")
            for kind, content_key in (("video", "video_bytes"), ("audio", "audio_bytes"),
                                      ("greeting_audio", "greeting_audio_bytes")):
                if hashlib.sha256(inputs[content_key]).hexdigest() != metadata.get(kind + "_sha256"):
                    raise VideoProviderError("input_hash_mismatch", "Prepared input hash changed.")
            failure_stage = "asset_storage"
            video_key = self._asset(pipeline_id, "input.mp4", inputs["video_bytes"], "video/mp4")
            audio_key = self._asset(pipeline_id, "input.wav", inputs["audio_bytes"], "audio/wav")
            greeting_key = self._asset(pipeline_id, "greeting.wav", inputs["greeting_audio_bytes"], "audio/wav")
            failure_stage = "asset_signing"
            video_url = self.store.objects.signed_get_url(video_key)
            audio_url = self.store.objects.signed_get_url(audio_key)
        except Exception as exc:
            self._update(pipeline_id, {"stage": "failed", "failure_code": "greeting_preparation_failed",
                **_preparation_failure_diagnostics(exc, failure_stage, audio_qa)}, "narration_pending")
            # Only durable, identity-checked failure is terminal. A failed write,
            # unreadable state, or competing paid-stage transition still raises.
            latest, _ = self._read(pipeline_id)
            if latest["stage"] == "failed" and latest.get("failure_code") == "greeting_preparation_failed":
                return latest
            raise
        # CAS is the paid-stage lease: competing polls cannot pay twice.
        claimed = {**state, "stage": "fal_submitting", "metadata": metadata,
                   "greeting_key": greeting_key, "narration_duration": duration,
                   "estimated_cents": TTS_REQUEST_CENTS + LIPSYNC_REQUEST_CENTS,
                   "audio_qa": audio_qa, "fal_attempt_at": iso(utcnow())}
        self.store.write("same-take/" + pipeline_id, claimed, version)
        self._price_before_payment(pipeline_id, "fal_submitting", MODEL, self.fal_key, "lipsync_price")
        if not self._active(claimed):
            return self._update(pipeline_id, {"stage": "cancelled"}, "fal_submitting")
        try:
            result = self._json("POST", QUEUE + MODEL, paid=True,
                headers={"Authorization": "Key " + self.fal_key, "X-Fal-No-Retry": "1",
                         "X-Fal-Request-Start-Timeout": "600"},
                payload={"video_url": video_url, "audio_url": audio_url, "enable_safety_checker": True})
            request_id = result.get("request_id")
            if not isinstance(request_id, str) or not _REQUEST_ID.fullmatch(request_id):
                raise SubmissionUncertain(pipeline_id)
            return self._update(pipeline_id, {"stage": "fal_pending", "fal_request_id": request_id}, "fal_submitting")
        except Exception:
            self._update(pipeline_id, {"stage": "uncertain", "failure_code": "fal_submission_unconfirmed"}, "fal_submitting")
            raise

    def inspect(self, pipeline_id, *, accounting_only=False):
        state, version = self._read(pipeline_id)
        stage = state["stage"]
        if stage in {"narration_submitting", "fal_submitting"}:
            started = datetime.fromisoformat(state.get("fal_attempt_at", state["created_at"]).replace("Z", "+00:00"))
            if (utcnow() - started).total_seconds() < 600:
                return {"provider_id": pipeline_id, "status": "processing"}
            state = self._update(pipeline_id, {"stage": "uncertain", "failure_code": "paid_stage_lease_expired"}, stage)
            return {"provider_id": pipeline_id, "status": "failed" if state["stage"] == "uncertain" else "processing"}
        if stage in {"failed", "uncertain", "cancelled"}:
            return _failed_status(state)
        if stage == "narration_pending":
            headers = {"Authorization": "Key " + self.tts_key}
            narration_status = self._json("GET", tts_request_url(state["narration_id"],
                state.get("narration_status_url"), status=True), headers=headers)
            if narration_status.get("status") in {"IN_QUEUE", "IN_PROGRESS"}:
                return {"provider_id": pipeline_id, "status": "processing"}
            if narration_status.get("status") != "COMPLETED":
                raise VideoProviderError("tts_status_unknown", "Narration returned an unknown state.")
            result = self._json("GET", tts_request_url(state["narration_id"],
                state.get("narration_result_url")), headers=headers)
            audio = result.get("audio")
            narration = {"status": "failed" if result.get("error") or not audio else "completed"}
            if narration["status"] == "completed":
                narration["media_url"] = fal_media_url(audio.get("url") if isinstance(audio, dict) else audio)
            if narration["status"] == "failed":
                state = self._update(pipeline_id, {"stage": "failed", "failure_code": "narration_failed"}, "narration_pending")
            elif narration["status"] == "completed":
                if accounting_only:
                    return {"provider_id": pipeline_id, "status": "failed", "failure_code": "cancelled_before_lipsync"}
                state = self._start_fal(state, version, narration)
            else:
                return {"provider_id": pipeline_id, "status": "processing"}
            return (_failed_status(state) if state["stage"] in {"failed", "cancelled"}
                    else {"provider_id": pipeline_id, "status": "processing"})
        if stage == "fal_pending":
            request_id = state["fal_request_id"]
            if not _REQUEST_ID.fullmatch(request_id):
                raise VideoProviderError("request_mismatch", "Fal request ID did not match.")
            # Provider-returned URLs are never used with Authorization headers.
            headers = {"Authorization": "Key " + self.fal_key}
            result = self._json("GET", REQUEST_BASE + request_id + "/status", headers=headers)
            if result.get("status") in {"IN_QUEUE", "IN_PROGRESS"}:
                return {"provider_id": pipeline_id, "status": "processing"}
            if result.get("status") != "COMPLETED":
                raise VideoProviderError("fal_status_unknown", "Fal returned an unknown state.")
            result = self._json("GET", REQUEST_BASE + request_id, headers=headers)
            if result.get("error") or not isinstance(result.get("video"), dict):
                latest = self._update(pipeline_id, {"stage": "failed", "failure_code": "fal_render_failed"}, "fal_pending")
                return {"provider_id": pipeline_id, "status": "failed" if latest["stage"] == "failed" else "processing"}
            state = self._update(pipeline_id, {"stage": "completed", "media_url": fal_media_url(result["video"].get("url"))}, "fal_pending")
        if state["stage"] == "completed":
            return {"provider_id": pipeline_id, "status": "completed", "media_url": state["media_url"],
                    "estimated_cents": state["estimated_cents"], "accounting_method": "conservative_reservation"}
        return {"provider_id": pipeline_id, "status": "processing"}

    def _download_fal(self, url):
        fal_media_url(url, self.resolver)
        started, content = time.monotonic(), bytearray()
        try:
            with self.media_session.get(url, stream=True, allow_redirects=False, timeout=REQUEST_TIMEOUT,
                                        headers={"Accept-Encoding": "identity"}) as response:
                if response.status_code != 200:
                    raise ValueError()
                for chunk in response.iter_content(65536):
                    content.extend(chunk)
                    if len(content) > MAX_BYTES or time.monotonic() - started > 90:
                        raise ValueError()
        except (requests.RequestException, ValueError):
            raise VideoProviderError("fal_download_failed", "Fal output download was unavailable or oversized.") from None
        if not content:
            raise VideoProviderError("fal_download_empty", "Fal output was empty.")
        return bytes(content)

    def prepare_media(self, snapshot, first_name):
        state, _ = self._read(snapshot["provider_id"])
        if state["stage"] != "completed" or normalize_name(first_name) != state["first_name"]:
            raise VideoProviderError("completed_identity_mismatch", "Completed video does not match this agent name.")
        greeting, _ = self.store.objects.read(state["greeting_key"])
        result = self.compositor(self._master(), self._download_fal(state["media_url"]), greeting,
                                 state["metadata"], first_name=state["first_name"])
        result["qa"] = {**result["qa"], "narration_audio": state["audio_qa"],
                        "automated_screen_not_naturalness_guarantee": True}
        self._update(state["id"], {"qa": result["qa"]})
        return {"media_bytes": result["media_bytes"], "poster_bytes": result["poster_bytes"],
                "vtt_bytes": result["captions_bytes"], "duration": result["duration_seconds"],
                "width": 1024, "height": 576, "caption_source": "same_recording_timed", "qa": result["qa"]}
