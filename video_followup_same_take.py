"""At-most-once audio greeting + same-recording lip-sync pipeline.

HeyGen visuals are never used. Every paid stage is claimed in private storage
before submission; an unknown outcome is held, never automatically resubmitted.
"""
from __future__ import annotations

import hashlib
import ipaddress
import math
import re
import time
from datetime import datetime
from urllib.parse import urlsplit

import requests

from video_followup import Conflict, SCRIPT_VERSION, iso, normalize_name, utcnow, valid_name
from video_followup_provider import (
    API_BASE, AVATAR_ID, VOICE_ID, MAX_API_BYTES, REQUEST_TIMEOUT,
    HeyGenVideoProvider, SubmissionUncertain, VideoProviderError, _public_addresses,
)
from video_followup_same_take_media import prepare_inputs, compose

MASTER_SHA256 = "01efd28101e8b016d51d571cc87d38d13ea623389663bc7a88cf26cce39bd60b"
MODEL = "fal-ai/kling-video/lipsync/audio-to-video"
QUEUE = "https://queue.fal.run/"
REQUEST_BASE = QUEUE + "fal-ai/kling-video/requests/"
MAX_BYTES = 25 * 1024 * 1024
_REQUEST_ID = re.compile(r"[a-zA-Z0-9-]{16,64}")
_PIPELINE_ID = re.compile(r"same-take-[a-f0-9]{32}")


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


class SameTakeVideoProvider:
    def __init__(self, api_key, fal_key, store, master_asset_key, *, session=None,
                 media_session=None, resolver=None, preparer=prepare_inputs, compositor=compose):
        if not api_key or not fal_key or not master_asset_key.startswith("assets/"):
            raise VideoProviderError("same_take_unconfigured", "Same-recording integration is incomplete.")
        self.store, self.master_key = store, master_asset_key
        self.api_key, self.fal_key = api_key, fal_key
        self.session, self.media_session = session or requests.Session(), media_session or requests.Session()
        if session is None:
            self.session.trust_env = False
        if media_session is None:
            self.media_session.trust_env = False
        self.resolver = resolver or _public_addresses
        self.heygen = HeyGenVideoProvider(api_key, session=self.session,
                                        media_session=self.media_session, resolver=self.resolver)
        self.preparer, self.compositor = preparer, compositor

    def _master(self):
        content, _ = self.store.objects.read(self.master_key)
        if not content or hashlib.sha256(content).hexdigest() != MASTER_SHA256:
            raise VideoProviderError("master_mismatch", "Approved second-recording master did not match.")
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
                or state.get("reserved_cents") != 10):
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

    def submit(self, first_name, job_id):
        first_name = normalize_name(first_name)
        if not valid_name(first_name) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", job_id):
            raise VideoProviderError("invalid_identity", "Verified first name and job identity are required.")
        self._master()
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
                 "reserved_cents": 10, "greeting_script": f"Hey {first_name}!"}
        if not self._active(state):
            raise VideoProviderError("inactive_job", "Video job is no longer active.")
        self.store.write("same-take/" + pipeline_id, state, 0)
        try:
            result = self._json("POST", API_BASE + "/videos", paid=True,
                headers={"X-Api-Key": self.api_key, "Idempotency-Key": pipeline_id},
                payload={"type": "avatar", "avatar_id": AVATAR_ID, "voice_id": VOICE_ID,
                    "voice_settings": {"speed": 1.05}, "script": state["greeting_script"],
                    "engine": {"type": "avatar_iii"}, "resolution": "720p", "aspect_ratio": "16:9",
                    "output_format": "mp4", "caption": {"file_format": "srt"},
                    "title": "Crisp greeting audio " + pipeline_id, "callback_id": pipeline_id})
            narration_id = result.get("data", {}).get("video_id")
            if not isinstance(narration_id, str) or not re.fullmatch(r"[a-f0-9]{32}", narration_id):
                raise SubmissionUncertain(pipeline_id)
            self._update(pipeline_id, {"stage": "narration_pending", "narration_id": narration_id}, "narration_submitting")
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
        duration = float(narration["duration"])
        if not math.isfinite(duration) or not 0 < duration <= 3.0:
            return self._update(pipeline_id, {"stage": "failed", "failure_code": "narration_exceeds_budget"}, "narration_pending")
        master = self._master()
        audio_source = self.heygen._download(narration["media_url"], MAX_BYTES)
        try:
            inputs = self.preparer(master, audio_source, first_name=state["first_name"])
            metadata = inputs["metadata"]
            if (metadata.get("source_sha256") != MASTER_SHA256
                    or metadata.get("first_name") != state["first_name"]
                    or not 2 <= float(metadata.get("context_seconds", 0)) <= 5
                    or 7 + math.ceil(duration) > 10):
                raise VideoProviderError("input_budget_mismatch", "Prepared name, source or billable duration did not match.")
            for kind, content_key in (("video", "video_bytes"), ("audio", "audio_bytes"),
                                      ("greeting_audio", "greeting_audio_bytes")):
                if hashlib.sha256(inputs[content_key]).hexdigest() != metadata.get(kind + "_sha256"):
                    raise VideoProviderError("input_hash_mismatch", "Prepared input hash changed.")
            video_key = self._asset(pipeline_id, "input.mp4", inputs["video_bytes"], "video/mp4")
            audio_key = self._asset(pipeline_id, "input.wav", inputs["audio_bytes"], "audio/wav")
            greeting_key = self._asset(pipeline_id, "greeting.wav", inputs["greeting_audio_bytes"], "audio/wav")
            video_url = self.store.objects.signed_get_url(video_key)
            audio_url = self.store.objects.signed_get_url(audio_key)
        except Exception:
            self._update(pipeline_id, {"stage": "failed", "failure_code": "greeting_preparation_failed"}, "narration_pending")
            raise
        # CAS is the paid-stage lease: competing polls cannot pay twice.
        claimed = {**state, "stage": "fal_submitting", "metadata": metadata,
                   "greeting_key": greeting_key, "narration_duration": duration,
                   "estimated_cents": 7 + math.ceil(duration), "fal_attempt_at": iso(utcnow())}
        self.store.write("same-take/" + pipeline_id, claimed, version)
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
            return {"provider_id": pipeline_id, "status": "failed", "failure_code": state.get("failure_code", stage)}
        if stage == "narration_pending":
            narration = self.heygen.inspect(state["narration_id"])
            if narration["status"] == "failed":
                state = self._update(pipeline_id, {"stage": "failed", "failure_code": "narration_failed"}, "narration_pending")
            elif narration["status"] == "completed":
                if accounting_only:
                    return {"provider_id": pipeline_id, "status": "failed", "failure_code": "cancelled_before_lipsync"}
                state = self._start_fal(state, version, narration)
            else:
                return {"provider_id": pipeline_id, "status": "processing"}
            return {"provider_id": pipeline_id, "status": "failed" if state["stage"] in {"failed", "cancelled"} else "processing"}
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
        self._update(state["id"], {"qa": result["qa"]})
        return {"media_bytes": result["media_bytes"], "poster_bytes": result["poster_bytes"],
                "vtt_bytes": result["captions_bytes"], "duration": result["duration_seconds"],
                "width": 1024, "height": 576, "caption_source": "same_recording_timed", "qa": result["qa"]}
