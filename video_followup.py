"""Opt-in video preparation; the existing scheduler remains the only SMS sender.

GCS generation preconditions protect the durable job and budget ledgers across
process restarts. Apps Script is the final authority for freezing SMS content.
"""

import hashlib
import json
import logging
import math
import os
import re
import secrets
import threading
import unicodedata
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

from video_followup_speech_guard import (
    PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256, PLANNED_AUDIO_VERIFICATION_METHOD,
    PLANNED_VERIFICATION_PROFILES,
    SpeechGuardError, is_transient_speech_failure, safe_speech_failure_diagnostics,
    validate_speech_proof,
)

LOG = logging.getLogger(__name__)
SCRIPT_VERSION = "face-to-text-same-take-turbo-v3"
MAX_MEDIA_BYTES = 30 * 1024 * 1024
LOCAL_VERIFICATION_MAX_ATTEMPTS = 3
LOCAL_VERIFICATION_RETRY_SECONDS = 60
_VERIFICATION_IDENTITY_FIELDS = ("verification_method", "verification_config_sha256")
_JOB_IDENTITY_FIELDS = ("id", "followup_key", "provider_id", "first_name", "phone",
                        "stable_id", "initial_sent_at", "script_version") + _VERIFICATION_IDENTITY_FIELDS
_ACTIVE_VERIFICATION_FAILURE_FIELDS = (
    "failure_stage", "underlying_code", "exception_type", "failure_at", "reason", "speech_diagnostics",
    "finalization_retry_at", "ready_audit_retry_at", "preflight_retry_at", "verification_retry_exhausted",
)
_SERVICE = None
_SERVICE_LOCK = threading.Lock()


class Conflict(Exception):
    pass


def iso(value):
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("An explicit timestamp timezone is required")
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def utcnow():
    return datetime.now(timezone.utc)


def canonical_phone(value):
    digits = re.sub(r"\D", "", str(value))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        raise ValueError("Invalid recipient")
    return digits


def normalize_name(value):
    return unicodedata.normalize("NFKC", str(value or "")).translate(
        str.maketrans({"\u2018": "'", "\u2019": "'", "\u02bc": "'"})).strip()


def valid_name(value):
    value = normalize_name(value)
    return (bool(re.fullmatch(r"[^\W\d_]+(?:[-'][^\W\d_]+){0,2}", value, re.UNICODE))
            and 1 < len(value) <= 32 and value.casefold() not in {"unknown", "agent", "there", "test"})


def _same_job_identity(current, expected):
    return (isinstance(current, dict)
            and all((field in current) == (field in expected) for field in _VERIFICATION_IDENTITY_FIELDS)
            and all(current.get(field) == expected.get(field) for field in _JOB_IDENTITY_FIELDS))


def _same_job_attempt(current, expected, *attempt_fields):
    return (_same_job_identity(current, expected) and not current.get("variant")
            and all(current.get(field) == expected.get(field)
                    for field in ("status",) + attempt_fields))


def _attempt_count(job, field):
    count = job.get(field, 0)
    return count if type(count) is int and 0 <= count <= LOCAL_VERIFICATION_MAX_ATTEMPTS else None


def _verification_binding(value):
    if not any(field in value for field in _VERIFICATION_IDENTITY_FIELDS):
        return {}
    method = value.get("verification_method")
    config = PLANNED_VERIFICATION_PROFILES.get(method) if isinstance(method, str) else None
    if config is None:
        raise SpeechGuardError("speech_guard_output_invalid")
    expected = {"verification_method": method, "verification_config_sha256": hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    if any(value.get(field) != item for field, item in expected.items()):
        raise SpeechGuardError("speech_guard_output_invalid")
    return expected


def snapshot(*, row, stable_id, phone, first_name, initial_sent_at):
    stable_id = str(stable_id or "").strip()
    if not stable_id or int(row) < 2:
        raise ValueError("A verified stable listing identity is required")
    phone = canonical_phone(phone)
    initial = iso(initial_sent_at)
    # The row may move. Copy or script changes must not create a second follow-up.
    key = hashlib.sha256(json.dumps([stable_id, phone, initial], separators=(",", ":")).encode()).hexdigest()
    return dict(row=int(row), crm_row=int(row), stable_id=stable_id, phone=phone,
                first_name=normalize_name(first_name), initial_sent_at=initial,
                followup_key=key, message_id="followup-video-" + key[:24],
                request_id="scheduler-followup-video-" + key[:24])


class Config:
    def __init__(self, env=None):
        env = os.environ if env is None else env
        self.enabled = env.get("VIDEO_FOLLOWUP_ENABLED", "false").lower() == "true"
        self.generation_enabled = env.get("VIDEO_FOLLOWUP_GENERATION_ENABLED", "false").lower() == "true"
        self.live = env.get("VIDEO_FOLLOWUP_LIVE_APPROVED", "false").lower() == "true"
        self.bucket = env.get("VIDEO_FOLLOWUP_STORAGE_BUCKET", "").strip()
        self.origin = env.get("VIDEO_FOLLOWUP_PUBLIC_ORIGIN", "").rstrip("/")
        self.not_before = env.get("VIDEO_FOLLOWUP_INITIALS_AFTER", "").strip()
        if self.not_before:
            self.not_before = iso(self.not_before)
        self.allowlist = {canonical_phone(p) for p in env.get("VIDEO_FOLLOWUP_TEST_PHONES", "").split(",") if p.strip()}
        self.daily_cents = int(env.get("VIDEO_FOLLOWUP_DAILY_CENTS", "300"))
        self.monthly_cents = int(env.get("VIDEO_FOLLOWUP_MONTHLY_CENTS", "7500"))
        # Conservative per-render reservation, never released on uncertain calls.
        self.reserve_cents = 10
        if self.daily_cents < 0 or self.monthly_cents < 0 or self.daily_cents > 300 or self.monthly_cents > 7500:
            raise ValueError("Video budget exceeds the approved $3/day or $75/month")
        if self.origin:
            u = urlsplit(self.origin)
            if u.scheme != "https" or u.hostname not in {"go.crispshortsales.com", "www.crispshortsales.com", "crispshortsales.com"} or u.path or u.query or u.fragment or u.username or u.port:
                raise ValueError("Video links require an approved branded HTTPS origin")

    def accepts(self, context):
        if not self.enabled or not self.not_before:
            return False
        if iso(context["initial_sent_at"]) < iso(self.not_before):
            return False
        return self.live or context["phone"] in self.allowlist


class GCSObjects:
    """Private object storage with compare-and-swap; no bucket creation or ACL edits."""
    def __init__(self, bucket, credentials_json=None, session=None):
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]", bucket):
            raise ValueError("A private video storage bucket must be configured")
        self.bucket = bucket
        self._signing_credentials = None
        if session is None:
            from google.oauth2 import service_account
            from google.auth.transport.requests import AuthorizedSession
            info = json.loads(credentials_json or os.environ["GCP_SERVICE_ACCOUNT_JSON"])
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/devstorage.read_write"])
            session = AuthorizedSession(credentials)
            self._signing_credentials = credentials
        self.session = session

    def url(self, key):
        return "https://storage.googleapis.com/storage/v1/b/" + self.bucket + "/o/" + quote("video-followup/" + key, safe="")

    def signed_get_url(self, key, ttl_seconds=1800):
        if (not isinstance(key, str) or not key.startswith("assets/") or ".." in key.split("/")
                or "\\" in key or any(ord(char) < 32 for char in key)):
            raise ValueError("Only video asset reads may be signed")
        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int) or not 1 <= ttl_seconds <= 3600:
            raise ValueError("Asset read URL lifetime must be between 1 and 3600 seconds")
        credentials = self._signing_credentials
        if credentials is None:
            raise ValueError("Video storage signing credentials are unavailable")
        now = utcnow()
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        scope = now.strftime("%Y%m%d") + "/auto/storage/goog4_request"
        uri = "/" + self.bucket + "/" + quote("video-followup/" + key, safe="/~")
        query = {
            "X-Goog-Algorithm": "GOOG4-RSA-SHA256",
            "X-Goog-Credential": credentials.service_account_email + "/" + scope,
            "X-Goog-Date": stamp, "X-Goog-Expires": str(ttl_seconds), "X-Goog-SignedHeaders": "host",
        }
        query_string = "&".join(quote(k, safe="") + "=" + quote(v, safe="") for k, v in sorted(query.items()))
        canonical = "\n".join(("GET", uri, query_string, "host:storage.googleapis.com\n", "host", "UNSIGNED-PAYLOAD"))
        to_sign = "\n".join(("GOOG4-RSA-SHA256", stamp, scope, hashlib.sha256(canonical.encode()).hexdigest()))
        signature = credentials.sign_bytes(to_sign.encode()).hex()
        return "https://storage.googleapis.com" + uri + "?" + query_string + "&X-Goog-Signature=" + signature

    def read(self, key):
        response = self.session.get(self.url(key), timeout=20)
        if response.status_code == 404:
            return None, 0
        response.raise_for_status()
        meta = response.json()
        if int(meta["size"]) > (MAX_MEDIA_BYTES if key.startswith("assets/") else 512 * 1024):
            raise ValueError("Stored object exceeds size limit")
        version = int(meta["generation"])
        data = self.session.get(self.url(key), params={"alt": "media", "generation": version}, timeout=30)
        data.raise_for_status()
        return data.content, version

    def write(self, key, body, version, content_type="application/json"):
        response = self.session.post(
            "https://storage.googleapis.com/upload/storage/v1/b/" + self.bucket + "/o",
            params={"uploadType": "media", "name": "video-followup/" + key, "ifGenerationMatch": version},
            data=body, headers={"Content-Type": content_type}, timeout=30)
        if response.status_code == 412:
            raise Conflict()
        response.raise_for_status()
        return int(response.json()["generation"])

    def delete(self, key, version):
        response = self.session.delete(self.url(key), params={"ifGenerationMatch": version}, timeout=20)
        if response.status_code not in {204, 404, 412}:
            response.raise_for_status()

    def list(self, prefix, limit=50):
        names, page_token = [], None
        while True:
            params = {"prefix": "video-followup/" + prefix, "maxResults": limit}
            if page_token:
                params["pageToken"] = page_token
            response = self.session.get("https://storage.googleapis.com/storage/v1/b/" + self.bucket + "/o",
                                        params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
            names.extend(o["name"].removeprefix("video-followup/") for o in payload.get("items", []))
            page_token = payload.get("nextPageToken")
            if not page_token:
                return names


class Store:
    def __init__(self, objects):
        self.objects = objects

    def read(self, key):
        body, version = self.objects.read(key)
        return (json.loads(body) if body is not None else None), version

    def write(self, key, value, version):
        return self.objects.write(key, json.dumps(value, sort_keys=True, separators=(",", ":")).encode(), version)

    def mutate(self, key, fn):
        for _ in range(8):
            value, version = self.read(key)
            updated = fn(value)
            if updated is None:
                return value
            try:
                self.write(key, updated, version)
                return updated
            except Conflict:
                continue
        raise Conflict("Storage contention; retry later")

    @staticmethod
    def has_speech_proof(job):
        try:
            binding = _verification_binding(job)
        except SpeechGuardError:
            return False
        proof = job.get("speech_proof")
        return (isinstance(proof, dict) and isinstance(proof.get("media_sha256"), str)
                and all(proof.get(field) == value for field, value in binding.items())
                and validate_speech_proof(proof, first_name=job.get("first_name"),
                                          media_sha256=proof["media_sha256"]))

    def verified_video(self, job):
        if not self.has_speech_proof(job):
            return False
        content, _ = self.objects.read(f"assets/{job['id']}/video")
        return (isinstance(content, bytes) and 0 < len(content) <= MAX_MEDIA_BYTES
                and validate_speech_proof(job["speech_proof"], first_name=job.get("first_name"),
                                          media_sha256=hashlib.sha256(content).hexdigest()))

    def publication_ready(self, job):
        if not self.verified_video(job):
            return False
        pointer, _ = self.read("public/" + job["id"])
        if not isinstance(pointer, dict) or pointer.get("key") != job.get("followup_key"):
            return False
        for kind in ("poster", "captions"):
            content, _ = self.objects.read(f"assets/{job['id']}/{kind}")
            if not isinstance(content, bytes) or not 0 < len(content) <= MAX_MEDIA_BYTES:
                return False
        return True

    def prepare(self, context):
        context = {**context, "first_name": normalize_name(context["first_name"])}
        key = context["followup_key"]
        job = self.mutate("jobs/" + key, lambda old: None if old else {
            **context, "id": secrets.token_hex(16), "status": "prepared",
            "script_version": SCRIPT_VERSION, "created_at": iso(utcnow())})
        if (job["status"] in {"prepared", "submitted", "submitting", "processing"}
                or (job["status"] == "ready" and not job.get("variant") and not self.has_speech_proof(job))):
            # Legacy ready jobs need only a free audit, never another render.
            self.mutate("active/" + key, lambda old: None if old else {"key": key})
        return job

    def reserve(self, key, config, now):
        # One global CAS ledger means concurrent workers cannot each spend the
        # last budget slot. Crashed/failed requests keep their reservation.
        local_day = now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
        month, day = local_day[:7], local_day
        accepted = [False]
        def update(old):
            ledger = old or {"reservations": {}}
            entries = ledger["reservations"]
            cutoff = (now - timedelta(days=90)).date().isoformat()
            entries = {k: v for k, v in entries.items() if v["day"] >= cutoff or v.get("inflight")}
            ledger["reservations"] = entries
            accepted[0] = False
            # A prepared job may have crashed between reservation and its
            # submission lease. Re-budget it for the actual submission day.
            others = {k: v for k, v in entries.items() if k != key}
            if sum(bool(item.get("inflight")) for item in others.values()) >= 2:
                return None
            total = sum(item["cents"] for item in others.values() if item["day"][:7] == month)
            today = sum(item["cents"] for item in others.values() if item["day"] == day)
            if today + config.reserve_cents > config.daily_cents or total + config.reserve_cents > config.monthly_cents:
                return None
            accepted[0] = True
            entry = {"day": day, "cents": config.reserve_cents, "inflight": True}
            if entries.get(key) == entry:
                return None
            entries[key] = entry
            return ledger
        self.mutate("budget/ledger", update)
        return accepted[0]

    def settle(self, key, reserved_at, billed_cents=None, accounting_method=None):
        verified = accounting_method in {"provider_verified", "provider_usage"}
        if billed_cents is not None and verified:
            if isinstance(billed_cents, bool) or not isinstance(billed_cents, (int, float)) or not math.isfinite(billed_cents) or billed_cents < 0:
                raise ValueError("Invalid verified provider charge")
        def update(old):
            if not old or key not in old["reservations"]:
                return None
            entry = old["reservations"][key]
            # Full video duration is unrelated to the cost of replacing its
            # greeting. Estimates/unknown failures retain the reserved amount.
            if billed_cents is not None and verified:
                entry["cents"] = math.ceil(billed_cents)
                entry["accounting_method"] = accounting_method
            entry["inflight"] = False
            return old
        self.mutate("budget/ledger", update)

    def freeze(self, context, text, origin):
        context = {**context, "first_name": normalize_name(context["first_name"])}
        job = self.prepare(context)
        def update(old):
            if old.get("variant") or old["status"] == "cancelled":
                return None
            ready = (old["status"] == "ready" and old.get("script_version") == SCRIPT_VERSION
                     and old["first_name"] == context["first_name"] and valid_name(context["first_name"])
                     and self.publication_ready(old))
            variant = "video" if ready and origin else "text"
            payload = (f"Hey {context['first_name']}, it's Yoni Kutler with Crisp Short Sales. "
                       "Here's a quick video to put a face to my text. Happy to help if you have questions.\n"
                       f"{origin}/v/{old['id']}") if variant == "video" else text
            return {**old, "status": "frozen_" + variant, "variant": variant, "message": payload,
                    "frozen_at": iso(utcnow())}
        job = self.mutate("jobs/" + context["followup_key"], update)
        if job["status"] == "cancelled":
            return None
        return job

    def cancel(self, key, reason):
        return self.mutate("jobs/" + key, lambda old: {**old, "status": "cancelled", "reason": reason,
                           "cancelled_at": iso(utcnow())} if old else None)

    def finish(self, key, provider_id, media, *, expected_job=None):
        job, _ = self.read("jobs/" + key)
        if not job or job["status"] not in {"submitted", "processing"} or job.get("provider_id") != provider_id:
            return False
        if expected_job is not None and not _same_job_attempt(job, expected_job, "lease_at", "finalization_attempts"):
            return False
        if (not isinstance(media, dict) or any(not isinstance(media.get(field), bytes)
                or not 0 < len(media[field]) <= MAX_MEDIA_BYTES
                for field in ("media_bytes", "poster_bytes", "vtt_bytes"))
                or type(media.get("duration")) not in (int, float)
                or not math.isfinite(media["duration"]) or media["duration"] <= 0):
            raise SpeechGuardError("speech_guard_media_invalid")
        proof = media.get("speech_proof")
        if (not self.has_speech_proof({**job, "speech_proof": proof})
                or not validate_speech_proof(proof, first_name=job["first_name"],
                                            media_sha256=hashlib.sha256(media["media_bytes"]).hexdigest())):
            raise SpeechGuardError("speech_guard_output_invalid")
        for kind, value, mime in [("video", media["media_bytes"], "video/mp4"),
                                  ("poster", media["poster_bytes"], "image/jpeg"),
                                  ("captions", media["vtt_bytes"], "text/vtt; charset=utf-8")]:
            try:
                self.objects.write(f"assets/{job['id']}/{kind}", value, 0, mime)
            except Conflict:
                stored, _ = self.objects.read(f"assets/{job['id']}/{kind}")
                if stored != value:
                    raise SpeechGuardError("speech_guard_media_invalid") from None
        if not self.verified_video({**job, "speech_proof": proof}):
            raise SpeechGuardError("speech_guard_media_invalid")
        pointer = self.mutate("public/" + job["id"], lambda old: {"key": key} if old is None else None)
        if not isinstance(pointer, dict) or pointer.get("key") != key:
            raise SpeechGuardError("speech_guard_media_invalid")
        def ready(old):
            if not _same_job_attempt(old, job, "lease_at", "finalization_attempts"):
                return None
            updated = {**old, "status": "ready", "duration": media["duration"], "ready_at": iso(utcnow()),
                       "speech_proof": proof, "verification_retry_pending": False,
                       "speech_verified_at": iso(utcnow())}
            for field in _ACTIVE_VERIFICATION_FAILURE_FIELDS:
                updated.pop(field, None)
            return updated
        return self.mutate("jobs/" + key, ready)["status"] == "ready"

    def get_public_video(self, opaque_id):
        if not re.fullmatch(r"[a-f0-9]{32}", opaque_id):
            return None
        pointer, _ = self.read("public/" + opaque_id)
        if not pointer:
            return None
        job, _ = self.read("jobs/" + pointer["key"])
        if not job or job["id"] != opaque_id or job["status"] not in {"ready", "frozen_video"}:
            return None
        return {k: job[k] for k in ("first_name", "duration", "status")}

    def get_public_asset(self, opaque_id, kind):
        if kind not in {"video", "poster", "captions"} or not self.get_public_video(opaque_id):
            return None
        body, version = self.objects.read(f"assets/{opaque_id}/{kind}")
        if body is None:
            return None
        return {"body": body, "etag": str(version), "content_type": {
            "video": "video/mp4", "poster": "image/jpeg", "captions": "text/vtt; charset=utf-8"}[kind]}


class Eligibility:
    def __init__(self, url, token, session=None):
        import requests
        self.url, self.token, self.session = url, token, session or requests.Session()

    def __call__(self, job):
        if not self.url or not self.token:
            return None
        try:
            body = {k: job[k] for k in ("crm_row", "row", "phone", "stable_id", "first_name", "initial_sent_at", "followup_key", "message_id", "request_id")}
            body.update(action="check_video_followup_eligibility", token=self.token, video_variant="video", video_job_id=job["id"])
            response = self.session.post(self.url, json=body, timeout=20)
            response.raise_for_status()
            result = response.json()
            if result.get("ok") is not True or result.get("retryable"):
                return None
            return result.get("eligible") is True
        except Exception:
            LOG.warning("Video eligibility check unavailable; no paid render started")
            return None


class Worker:
    def __init__(self, store, provider, config, eligible, now=utcnow):
        self.store, self.provider, self.config, self.eligible, self.now = store, provider, config, eligible, now

    def _retry_due(self, job, field):
        return not job.get(field) or self.now() >= datetime.fromisoformat(job[field].replace("Z", "+00:00"))

    def _cancel_attempt(self, key, job, reason):
        self.store.mutate("jobs/" + key, lambda old: {
            **old, "status": "cancelled", "reason": reason, "cancelled_at": iso(self.now())}
            if _same_job_attempt(old, job, "lease_at", "preflight_attempts", "finalization_attempts") else None)

    def _invalid_verification_state(self, key, job, counter, stage, code="invalid_verification_attempt_count"):
        lease = "ready_audit_token" if counter == "ready_audit_attempts" else "lease_at"
        self.store.mutate("jobs/" + key, lambda old: {
            **old, "status": "failed", "failure_stage": stage, "underlying_code": code,
            "exception_type": "InvalidVerificationState", "failure_at": iso(self.now()),
            "verification_retry_pending": False, "reason": "Local verification state failed safety validation"}
            if _same_job_attempt(old, job, lease, counter) else None)

    def _has_unpaid_reservation(self, key, job):
        if job.get("provider_id") or job.get("budget_settled"):
            return False
        ledger, _ = self.store.read("budget/ledger")
        entry = (ledger or {}).get("reservations", {}).get(key)
        return (isinstance(entry, dict) and entry.get("inflight") is True
                and type(entry.get("cents")) is int and entry["cents"] == self.config.reserve_cents)

    def _exhaust_interrupted_verification(self, key, job, *, audit=False):
        counter = "ready_audit_attempts" if audit else "finalization_attempts"
        lease = "ready_audit_token" if audit else "lease_at"
        diagnostics = {"failure_stage": "ready_audit" if audit else "final_media_prepare",
                       "underlying_code": "local_verification_attempt_limit",
                       "exception_type": "InterruptedVerification", "failure_at": iso(self.now())}
        self.store.mutate("jobs/" + key, lambda old: {
            **old, **diagnostics, "status": "failed", "verification_retry_pending": False,
            "verification_retry_exhausted": True,
            "reason": "Local verification attempt limit reached after interrupted processing"}
            if _same_job_attempt(old, job, lease, counter) else None)

    def _verification_failure(self, key, job, error, stage, *, audit=False, preflight=False, budget_settled=False):
        counter = "preflight_attempts" if preflight else "ready_audit_attempts" if audit else "finalization_attempts"
        retry_at = "preflight_retry_at" if preflight else "ready_audit_retry_at" if audit else "finalization_retry_at"
        lease = "ready_audit_token" if audit else "lease_at"
        transient = is_transient_speech_failure(error)
        retry = transient and job.get(counter, 0) < LOCAL_VERIFICATION_MAX_ATTEMPTS
        diagnostics = {
            "failure_stage": stage, "underlying_code": error.code,
            "exception_type": "SpeechGuardError", "failure_at": iso(self.now()),
        }
        speech = safe_speech_failure_diagnostics(error)
        if speech:
            diagnostics["speech_diagnostics"] = speech
        def update(old):
            if not _same_job_attempt(old, job, lease, counter, "budget_settled"):
                return None
            updated = {**old, **diagnostics,
                       ("last_preflight_failure" if preflight else "last_verification_failure"): diagnostics,
                       "verification_retry_pending": retry,
                       "verification_retry_exhausted": transient and not retry,
                       "status": ("prepared" if preflight else "ready" if audit else "submitted") if retry else "failed",
                       "reason": "Local video verification unavailable; retry deferred" if retry
                           else "Prepayment speech preflight remained unavailable" if preflight
                           else "Stored video failed spoken-name verification" if audit
                           else "Final video failed spoken-name verification"}
            if preflight:
                updated["preflight_confirmed_unpaid"] = True
            if budget_settled:
                updated["budget_settled"] = True
            updated.pop(retry_at, None)
            if retry:
                updated[retry_at] = iso(self.now() + timedelta(seconds=LOCAL_VERIFICATION_RETRY_SECONDS))
            if audit:
                updated.pop("ready_audit_token", None)
                updated.pop("ready_audit_lease_at", None)
            return updated
        self.store.mutate("jobs/" + key, update)

    def _capacity_is_terminal(self, job):
        if not job:
            return False
        if job.get("status") in {"failed", "uncertain"}:
            return True
        if job.get("status") not in {"cancelled", "frozen_text"} or job.get("provider_id"):
            return False
        # A concurrent cancellation can precede the submit response. Preserve
        # its existing lease before retiring an orphan with no request ID.
        lease = job.get("lease_at")
        return not lease or (self.now() - datetime.fromisoformat(lease.replace("Z", "+00:00"))).total_seconds() >= 600

    def _release_terminal_capacity(self):
        ledger, _ = self.store.read("budget/ledger")
        for key, entry in (ledger or {}).get("reservations", {}).items():
            if not entry.get("inflight"):
                continue
            job, _ = self.store.read("jobs/" + key)
            if not self._capacity_is_terminal(job):
                continue
            # Terminal jobs are never submitted again. Keep their full debit,
            # but do not let a lost response occupy worker capacity forever.
            # The ledger also covers a crash before reserved_at reached the job.
            self.store.settle(key, job.get("reserved_at"))
            self.store.mutate("jobs/" + key, lambda old: {**old, "budget_settled": True}
                if self._capacity_is_terminal(old) else None)

    def audit_ready(self, key):
        """Free, explicit audit for an unfrozen ready job, even with generation off."""
        job, _ = self.store.read("jobs/" + key)
        if (not job or job.get("status") != "ready" or job.get("variant")
                or job.get("script_version") != SCRIPT_VERSION):
            return False
        if self.store.verified_video(job):
            return True
        if _attempt_count(job, "ready_audit_attempts") is None:
            self._invalid_verification_state(key, job, "ready_audit_attempts", "ready_audit")
            return False
        if not self._retry_due(job, "ready_audit_retry_at"):
            return False
        if (job.get("ready_audit_lease_at") and (self.now() - datetime.fromisoformat(
                job["ready_audit_lease_at"].replace("Z", "+00:00"))).total_seconds() < 600):
            return False
        if self.eligible(job) is not True:
            return False
        if job.get("ready_audit_attempts", 0) >= LOCAL_VERIFICATION_MAX_ATTEMPTS:
            self._exhaust_interrupted_verification(key, job, audit=True)
            return False
        token = secrets.token_hex(16)
        def claim(old):
            if not _same_job_attempt(old, job, "ready_audit_token", "ready_audit_attempts"):
                return None
            return {**old, "ready_audit_attempts": old.get("ready_audit_attempts", 0) + 1,
                    "ready_audit_token": token, "ready_audit_lease_at": iso(self.now()),
                    "verification_retry_pending": True}
        job = self.store.mutate("jobs/" + key, claim)
        if not job or job.get("ready_audit_token") != token:
            return False
        content, _ = self.store.objects.read(f"assets/{job['id']}/video")
        try:
            proof = self.provider.audit_media(job, content)
            if not self.store.verified_video({**job, "speech_proof": proof}):
                raise SpeechGuardError("speech_guard_media_invalid")
        except SpeechGuardError as error:
            self._verification_failure(key, job, error, "ready_audit", audit=True)
            return False
        def verified(old):
            if (not _same_job_attempt(old, job, "ready_audit_token", "ready_audit_attempts")
                    or not self.store.verified_video({**old, "speech_proof": proof})):
                return None
            updated = {**old, "speech_proof": proof, "verification_retry_pending": False,
                       "speech_verified_at": iso(self.now())}
            for field in _ACTIVE_VERIFICATION_FAILURE_FIELDS + ("ready_audit_token", "ready_audit_lease_at"):
                updated.pop(field, None)
            return updated
        audited = self.store.mutate("jobs/" + key, verified)
        return bool(audited and audited.get("status") == "ready" and self.store.verified_video(audited))

    def run_job(self, key):
        job, version = self.store.read("jobs/" + key)
        if not job:
            return
        if job.get("script_version") != SCRIPT_VERSION:
            # Old provider IDs must never enter the new same-recording pipeline.
            if job.get("reserved_at") and not job.get("budget_settled"):
                self.store.settle(key, job["reserved_at"])
            self.store.mutate("jobs/" + key, lambda old: {**old,
                "status": "failed" if old["status"] in {"prepared", "submitting", "submitted", "processing"} else old["status"],
                "reason": "Video source version retired; use normal text", "budget_settled": True}
                if not old.get("budget_settled") or old["status"] in {"prepared", "submitting", "submitted", "processing"} else None)
            return
        if job["status"] in {"cancelled", "frozen_text"}:
            # Finish accounting for a render already purchased, but never
            # publish it or send it after cancellation/the text deadline.
            if job.get("provider_id") and not job.get("budget_settled"):
                status = self.provider.inspect(job["provider_id"], accounting_only=True)
                if status["status"] in {"completed", "failed"}:
                    self.store.settle(key, job["reserved_at"], status.get("billed_cents", status.get("cost_cents")), status.get("accounting_method"))
                    self.store.mutate("jobs/" + key, lambda old: {**old, "budget_settled": True})
            return
        if job["status"] == "ready":
            self.audit_ready(key)
            return
        if job["status"] in {"frozen_video", "failed", "uncertain", "budget_blocked"}:
            return
        if not self.config.accepts(job) or not self.config.generation_enabled:
            return
        now = self.now()
        if job["status"] in {"prepared", "submitted"}:
            counter = "preflight_attempts" if job["status"] == "prepared" else "finalization_attempts"
            stage = "prepayment_speech_preflight" if job["status"] == "prepared" else "final_media_prepare"
            if _attempt_count(job, counter) is None:
                self._invalid_verification_state(key, job, counter, stage)
                return
        if job["status"] == "prepared" and not self._retry_due(job, "preflight_retry_at"):
            return
        if job["status"] == "submitted" and not self._retry_due(job, "finalization_retry_at"):
            return
        if job["status"] in {"submitting", "processing"}:
            elapsed = (now - datetime.fromisoformat(job["lease_at"].replace("Z", "+00:00"))).total_seconds()
            if elapsed < 600:
                return
            # Unknown provider submission is never repeated. Polling an existing
            # render ID, in contrast, can be safely resumed after a crash.
            job["status"] = "submitted" if job.get("provider_id") else "uncertain"
            self.store.write("jobs/" + key, job, version)
            return
        eligible = self.eligible(job)
        if eligible is False:
            self._cancel_attempt(key, job, "Current recipient is no longer eligible")
            return
        if eligible is not True:
            return
        if job["status"] == "prepared":
            preflight_attempts = _attempt_count(job, "preflight_attempts")
            if preflight_attempts:
                if (preflight_attempts >= LOCAL_VERIFICATION_MAX_ATTEMPTS
                        or job.get("preflight_confirmed_unpaid") is not True):
                    self._invalid_verification_state(key, job, "preflight_attempts", "prepayment_speech_preflight")
                    return
                if not self._has_unpaid_reservation(key, job):
                    self._invalid_verification_state(key, job, "preflight_attempts", "prepayment_speech_preflight",
                                                     "unpaid_preflight_reservation_invalid")
                    return
            if not valid_name(job["first_name"]):
                self.store.mutate("jobs/" + key, lambda old: {**old, "status": "failed", "reason": "Uncertain canonical first name"} if old["status"] == "prepared" else None)
                return
            if not self.store.reserve(key, self.config, now):
                # Budget/capacity can recover; the due-time text freeze wins if
                # it has not recovered before the unchanged scheduled follow-up.
                return
            job.update(status="submitting", lease_at=iso(now), reserved_at=iso(now),
                       preflight_attempts=preflight_attempts + 1)
            try:
                self.store.write("jobs/" + key, job, version)
            except Conflict:
                latest, _ = self.store.read("jobs/" + key)
                if latest and latest["status"] in {"cancelled", "frozen_text"} and not latest.get("lease_at"):
                    self.store.settle(key, iso(now))
                raise
            try:
                provider_id = self.provider.submit(job["first_name"], key)
            except Exception as exc:
                classifier = getattr(self.provider, "safe_unpaid_preflight_failure", None)
                unpaid = classifier(exc, first_name=job["first_name"], job_id=key) if callable(classifier) else None
                if isinstance(unpaid, dict) and self._has_unpaid_reservation(key, job):
                    error = SpeechGuardError(unpaid.get("underlying_code"))
                    if error.code == unpaid.get("underlying_code") and is_transient_speech_failure(error):
                        self._verification_failure(key, job, error, "prepayment_speech_preflight", preflight=True)
                        return
                uncertain = getattr(exc, "uncertain", True)
                failed = self.store.mutate("jobs/" + key, lambda old: {**old, "status": "uncertain" if uncertain else "failed",
                    "reason": "Provider submit did not return a confirmed ID"}
                    if _same_job_attempt(old, job, "lease_at", "preflight_attempts") else None)
                if not uncertain and _same_job_attempt(failed, {**job, "status": "failed"}, "lease_at", "preflight_attempts"):
                    self.store.settle(key, job["reserved_at"])
                LOG.warning("Video provider submission not confirmed job=%s; no automatic resubmit", key[:12])
                return
            def submitted(old):
                if (not _same_job_identity(old, job) or any(old.get(field) != job.get(field)
                        for field in ("lease_at", "preflight_attempts"))):
                    return None
                updated = {**old, "status": "submitted" if old["status"] == "submitting" else old["status"],
                           "provider_id": provider_id}
                if _same_job_attempt(old, job, "lease_at", "preflight_attempts"):
                    updated["verification_retry_pending"] = False
                    for field in _ACTIVE_VERIFICATION_FAILURE_FIELDS + ("preflight_confirmed_unpaid",):
                        updated.pop(field, None)
                return updated
            self.store.mutate("jobs/" + key, submitted)
            # A concurrent cancellation tick may have removed the active index
            # before the provider returned. Retain polling for accounting only.
            self.store.mutate("active/" + key, lambda old: None if old else {"key": key})
            return
        if job["status"] == "submitted":
            if job.get("finalization_attempts", 0) >= LOCAL_VERIFICATION_MAX_ATTEMPTS:
                self._exhaust_interrupted_verification(key, job)
                return
            status = self.provider.inspect(job["provider_id"])
            if status["status"] == "failed":
                self.store.settle(key, job["reserved_at"])
                sanitizer = getattr(self.provider, "safe_failure_diagnostics", None)
                diagnostics = sanitizer(status) if callable(sanitizer) else {}
                self.store.mutate("jobs/" + key, lambda old: {**old, **diagnostics, "status": "failed",
                    "budget_settled": True, "failure_at": iso(self.now())}
                    if _same_job_attempt(old, job, "lease_at", "finalization_attempts") else None)
                return
            if status["status"] != "completed":
                return
            self.store.settle(key, job["reserved_at"], status.get("billed_cents", status.get("cost_cents")), status.get("accounting_method"))
            try:
                binding = _verification_binding(status)
                previous_binding = _verification_binding(job)
                if previous_binding and binding != previous_binding:
                    raise SpeechGuardError("speech_guard_output_invalid")
            except SpeechGuardError as error:
                self._verification_failure(key, job, error, "final_verification_identity", budget_settled=True)
                return
            verified_cost = status.get("billed_cents", status.get("cost_cents"))
            if verified_cost is not None and status.get("accounting_method") in {"provider_verified", "provider_usage"} and verified_cost > self.config.reserve_cents:
                self.store.mutate("jobs/" + key, lambda old: {**old, "status": "failed", "budget_settled": True,
                    "reason": "Verified provider cost exceeded per-video ceiling"} if old["status"] == "submitted" else None)
                return
            job.update(status="processing", lease_at=iso(now), budget_settled=True,
                       finalization_attempts=job.get("finalization_attempts", 0) + 1, **binding)
            self.store.write("jobs/" + key, job, version)
            try:
                media = self.provider.prepare_media(status, job["first_name"])
            except SpeechGuardError as error:
                self._verification_failure(key, job, error, "final_media_prepare")
                return
            eligible = self.eligible(job)
            if eligible is False:
                self._cancel_attempt(key, job, "Recipient replied while video was rendering")
                return
            if eligible is not True:
                return
            try:
                self.store.finish(key, job["provider_id"], media, expected_job=job)
            except SpeechGuardError as error:
                self._verification_failure(key, job, error, "publication")

    def tick(self):
        if not self.config.enabled or not self.config.generation_enabled:
            return
        # Includes orphaned terminal jobs whose active index was already removed.
        self._release_terminal_capacity()
        for path in self.store.objects.list("active/", limit=1000):
            key = path.split("/")[-1]
            try:
                self.run_job(key)
                job, _ = self.store.read("jobs/" + key)
                accounting_pending = job and job.get("provider_id") and not job.get("budget_settled") and job["status"] in {"cancelled", "frozen_text"}
                audit_pending = (job and job["status"] == "ready" and not job.get("variant")
                                 and job.get("verification_retry_pending"))
                if job and not accounting_pending and not audit_pending and job["status"] not in {"prepared", "submitted", "submitting", "processing"}:
                    _, version = self.store.objects.read(path)
                    if version:
                        self.store.objects.delete(path, version)
            except Conflict:
                continue
            except Exception as exc:
                LOG.warning("Video job will retry safely job=%s error_type=%s", key[:12], type(exc).__name__)


class Service:
    def __init__(self, config, store, audit_ready=None):
        self.config, self.store, self.audit_ready = config, store, audit_ready

    def prepare(self, context):
        context = {**context, "first_name": normalize_name(context["first_name"])}
        if self.config.accepts(context) and valid_name(context["first_name"]):
            return self.store.prepare(context)

    def select(self, context, fallback):
        context = {**context, "first_name": normalize_name(context["first_name"])}
        if not self.config.accepts(context):
            return fallback, {}
        # If storage is unavailable, still use the same stable outbox identity.
        # Apps Script atomically freezes the first accepted variant under lock.
        metadata = {**context, "video_variant": "text", "video_job_id": ""}
        try:
            job, _ = self.store.read("jobs/" + context["followup_key"])
            if (self.audit_ready and job and job.get("status") == "ready" and not job.get("variant")
                    and not self.store.verified_video(job)):
                try:
                    self.audit_ready(context["followup_key"])
                except Exception as exc:
                    LOG.warning("Free video audit unavailable; use text error_type=%s", type(exc).__name__)
            job = self.store.freeze(context, fallback, self.config.origin)
            if job is None:
                return None, metadata
            if job["variant"] == "video" and (job["first_name"] != context["first_name"]
                    or job.get("script_version") != SCRIPT_VERSION or not valid_name(context["first_name"])):
                # Never label old-name media with the corrected recipient name.
                # The outbox still decides whether an earlier payload is frozen.
                return fallback, metadata
            if job["variant"] == "video" and not self.store.publication_ready(job):
                # Preserve any previously frozen payload. The outbox decides
                # whether this same-key fallback can create a new text entry.
                return fallback, metadata
            metadata.update(video_variant=job["variant"], video_job_id=job["id"])
            if job["variant"] == "video":
                metadata["video_speech_proof"] = {**job["speech_proof"], "video_job_id": job["id"],
                    **{field: context[field] for field in ("followup_key", "phone", "initial_sent_at", "stable_id")}}
            return job["message"], metadata
        except Exception as exc:
            LOG.warning("Video unavailable; same-identity text fallback error_type=%s", type(exc).__name__)
            return fallback, metadata


def get_service():
    global _SERVICE
    if _SERVICE is None:
        with _SERVICE_LOCK:
            if _SERVICE is None:
                config = Config()
                _SERVICE = Service(config, Store(GCSObjects(config.bucket)), audit_ready=_audit_ready_job)
    return _SERVICE


def _audit_ready_job(key):
    from video_followup_worker import create_worker
    worker = create_worker()
    return worker.audit_ready(key) if worker else False


def prepare_for_followup(**kwargs):
    if os.getenv("VIDEO_FOLLOWUP_ENABLED", "false").lower() != "true":
        return
    try:
        get_service().prepare(snapshot(**kwargs))
    except Exception as exc:
        LOG.warning("Video preparation skipped; normal follow-up remains scheduled error_type=%s", type(exc).__name__)


def select_for_followup(fallback, **kwargs):
    if os.getenv("VIDEO_FOLLOWUP_ENABLED", "false").lower() != "true":
        return fallback, {}
    context = None
    try:
        context = snapshot(**kwargs)
        config = Config()
        if not config.accepts(context):
            return fallback, {}
        return get_service().select(context, fallback)
    except Exception as exc:
        LOG.warning("Video configuration unavailable; same-identity fallback error_type=%s", type(exc).__name__)
        return fallback, ({**context, "video_variant": "text", "video_job_id": ""} if context else {})
