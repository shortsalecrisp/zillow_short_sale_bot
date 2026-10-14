"""Retired full-avatar sample command; read existing sample status only."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import video_followup_provider as provider_module
from video_followup import Config, Conflict, GCSObjects, Store, iso, utcnow

OWNER_PHONE = "9542053205"
FIRST_NAME = "Jim"
ORIGIN = "https://go.crispshortsales.com"
APPROVED_AVATAR_ID = "baaaebc70e134e96b85209f2a446a03c"
APPROVED_VOICE_ID = "1ca117e4778643189228acc9400a37f3"
APPROVED_SCRIPT = (
    "Hey Jim, it's Yoni Kutler with Crisp Short Sales. "
    "Just wanted to put a face to my text about your short sale listing. "
    "If you have any questions, text me back. Happy to help."
)
SCRIPT_HASH = hashlib.sha256(APPROVED_SCRIPT.encode()).hexdigest()
FLAGS = ("VIDEO_FOLLOWUP_ENABLED", "VIDEO_FOLLOWUP_GENERATION_ENABLED", "VIDEO_FOLLOWUP_LIVE_APPROVED")
_HEX = re.compile(r"[0-9a-f]{32}\Z")
_PROVIDER_ID = re.compile(r"[A-Za-z0-9_:.-]{1,200}\Z")
_FORBIDDEN_CONTEXT = {"row", "crm_row", "stable_id", "phone", "initial_sent_at", "followup_key", "message_id", "request_id"}
_MEDIA = {"video": "media_bytes", "poster": "poster_bytes", "captions": "vtt_bytes"}


class OwnerSampleError(ValueError):
    def __init__(self, kind):
        self.kind = kind
        super().__init__(kind)


def _approved_assets():
    if (provider_module.AVATAR_ID != APPROVED_AVATAR_ID or
            provider_module.VOICE_ID != APPROVED_VOICE_ID or
            provider_module.HeyGenVideoProvider.script(FIRST_NAME) != APPROVED_SCRIPT):
        raise OwnerSampleError("approved_assets_changed")


def _require_off(config):
    if config.enabled or config.generation_enabled or config.live:
        raise OwnerSampleError("feature_flags_must_be_false")


def validate_preflight(env):
    """Offline checks only: presence/configuration is not API or billing proof."""
    if any(str(env.get(flag, "false")).strip().lower() != "false" for flag in FLAGS):
        raise OwnerSampleError("feature_flags_must_be_false")
    config = Config(env)
    _require_off(config)
    _approved_assets()
    if config.origin != ORIGIN:
        raise OwnerSampleError("approved_origin_required")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]", config.bucket):
        raise OwnerSampleError("private_bucket_required")
    if not str(env.get("HEYGEN_API_KEY", "")).strip() or not str(env.get("GCP_SERVICE_ACCOUNT_JSON", "")).strip():
        raise OwnerSampleError("credentials_not_configured")
    return config


def _identity(nonce):
    return {
        "kind": "owner_video_sample", "nonce": nonce, "job_key": "owner-probe-" + nonce,
        "owner_phone": OWNER_PHONE, "first_name": FIRST_NAME,
        "avatar_id": APPROVED_AVATAR_ID, "voice_id": APPROVED_VOICE_ID,
        "script_sha256": SCRIPT_HASH, "script_version": "face-to-text-v1",
    }


def _validate_job(job, nonce):
    if (not isinstance(job, dict) or any(job.get(k) != v for k, v in _identity(nonce).items()) or
            _FORBIDDEN_CONTEXT.intersection(job) or not _HEX.fullmatch(str(job.get("id", "")))):
        raise OwnerSampleError("owner_job_identity_mismatch")


def _result(job, *, ok=True, kind=None, **extra):
    result = {"ok": ok, "status": job["status"], "job_key": job["job_key"], "opaque_id": job["id"]}
    if job.get("provider_id"):
        result["provider_id"] = job["provider_id"]
    if kind:
        result["kind"] = kind
    result.update(extra)
    return result


def _error_kind(exc, default):
    kind = getattr(exc, "kind", None) or getattr(exc, "code", None)
    return kind if isinstance(kind, str) and re.fullmatch(r"[a-z0-9_]{1,80}", kind) else default


def _receipt(job, provider_id):
    return {
        **_identity(job["nonce"]), "id": job["id"], "attempt_token": job["attempt_token"],
        "provider_id": provider_id,
    }


def _validate_receipt(value, job):
    provider_id = value.get("provider_id") if isinstance(value, dict) else None
    if (not isinstance(provider_id, str) or not _PROVIDER_ID.fullmatch(provider_id) or
            value != _receipt(job, provider_id)):
        raise OwnerSampleError("submission_receipt_identity_mismatch")
    return provider_id


def _local_receipt_path(nonce, receipt_dir):
    return Path(receipt_dir or Path(tempfile.gettempdir()) / "crisp-owner-video-samples") / (nonce + ".json")


def _check_receipt_directory(path):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise OwnerSampleError("receipt_directory_unavailable")
    with tempfile.TemporaryFile(dir=path.parent):
        pass


def _save_local_receipt(path, value):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.is_symlink() or json.loads(path.read_text()) != value:
            raise OwnerSampleError("submission_receipt_identity_mismatch")
        return
    with os.fdopen(fd, "w") as output:
        json.dump(value, output, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())


def _save_remote_receipt(store, job, value):
    key = "owner-submission-receipts/" + job["nonce"]
    try:
        store.write(key, value, 0)
    except Conflict:
        existing, _ = store.read(key)
        if existing != value:
            raise OwnerSampleError("submission_receipt_identity_mismatch")


def _attach_provider(store, job, provider_id):
    def update(old):
        _validate_job(old, job["nonce"])
        if old.get("attempt_token") != job["attempt_token"]:
            raise OwnerSampleError("submission_attempt_mismatch")
        if old.get("provider_id"):
            if old["provider_id"] != provider_id:
                raise OwnerSampleError("submission_receipt_identity_mismatch")
            return None
        if old["status"] not in {"submitting", "uncertain"}:
            raise OwnerSampleError("submission_state_mismatch")
        return {**old, "status": "submitted", "provider_id": provider_id}
    return store.mutate("jobs/" + job["job_key"], update)


def _settle(store, job, duration=None):
    if duration is not None and (isinstance(duration, bool) or not isinstance(duration, (int, float))
                                 or not math.isfinite(duration) or not 0 < duration <= 30):
        raise OwnerSampleError("invalid_completed_duration")
    if not job.get("budget_settled"):
        # Historical render duration is not verified billing evidence. Retain
        # the reservation; only the new same-recording pipeline may generate.
        store.settle(job["job_key"], job["reserved_at"])
        job = store.mutate("jobs/" + job["job_key"], lambda old: {**old, "budget_settled": True})
    return job


def _verify_publication(store, job):
    pointer, _ = store.read("public/" + job["id"])
    expected = job.get("asset_sha256", {})
    public = store.get_public_video(job["id"])
    if (pointer != {"key": job["job_key"]} or set(expected) != set(_MEDIA) or
            not public or public.get("first_name") != FIRST_NAME or public.get("duration") != job.get("duration") or
            public.get("status") != "ready"):
        raise OwnerSampleError("publication_readback_failed")
    for kind in _MEDIA:
        asset = store.get_public_asset(job["id"], kind)
        if not asset or hashlib.sha256(asset["body"]).hexdigest() != expected[kind]:
            raise OwnerSampleError("publication_readback_failed")
    return _result(job, url=ORIGIN + "/v/" + job["id"], publication_verified=True)


def _verified_result(store, job):
    try:
        return _verify_publication(store, job)
    except OwnerSampleError:
        cancelled = store.cancel(job["job_key"], "Owner sample publication readback failed")
        return _result(cancelled, ok=False, kind="publication_readback_failed")


def run_step(store, provider, config, nonce, *, now=utcnow, receipt_dir=None):
    """Archived orchestration test helper; the retired real provider is refused."""
    if not isinstance(nonce, str) or not _HEX.fullmatch(nonce):
        raise OwnerSampleError("explicit_32_hex_nonce_required")
    _require_off(config)
    if isinstance(provider, provider_module.HeyGenVideoProvider):
        raise OwnerSampleError("legacy_avatar_generation_retired")
    _approved_assets()
    key = "owner-probe-" + nonce
    job = None
    try:
        if store.read("active/" + key)[0] is not None:
            raise OwnerSampleError("worker_index_present")
        job, version = store.read("jobs/" + key)
        path = _local_receipt_path(nonce, receipt_dir)
        if job is None:
            if path.exists() or store.read("owner-submission-receipts/" + nonce)[0] is not None:
                raise OwnerSampleError("orphaned_submission_receipt")
            job = {**_identity(nonce), "id": secrets.token_hex(16), "status": "prepared", "created_at": iso(now())}
            try:
                version = store.write("jobs/" + key, job, 0)
            except Conflict:
                job, version = store.read("jobs/" + key)
        _validate_job(job, nonce)
        if job["status"] == "ready":
            return _verified_result(store, job)
        if job["status"] in {"failed", "cancelled"}:
            if job["status"] == "failed":
                job = _settle(store, job)
            return _result(job, ok=False, kind=job.get("error_kind", "terminal_sample"))
        if job["status"] in {"submitting", "uncertain"} and not job.get("provider_id"):
            receipt, _ = store.read("owner-submission-receipts/" + nonce)
            if receipt is None and path.exists():
                if path.is_symlink():
                    raise OwnerSampleError("submission_receipt_identity_mismatch")
                receipt = json.loads(path.read_text())
            if receipt is None:
                return _result(job, ok=False, kind="submission_unknown_no_retry")
            provider_id = _validate_receipt(receipt, job)
            _save_remote_receipt(store, job, receipt)
            return _result(_attach_provider(store, job, provider_id), recovered_provider_id=True)
        if job["status"] == "prepared":
            if job.get("provider_id") or job.get("attempt_token"):
                raise OwnerSampleError("submission_state_mismatch")
            if path.exists() or store.read("owner-submission-receipts/" + nonce)[0] is not None:
                raise OwnerSampleError("orphaned_submission_receipt")
            _check_receipt_directory(path)
            instant = now()
            timestamp = iso(instant)
            submitting = {**job, "status": "submitting", "reserved_at": timestamp,
                          "lease_at": timestamp, "attempt_token": secrets.token_hex(16)}
            # Claim before reserving: a stale reader must not reset settled accounting.
            store.write("jobs/" + key, submitting, version)
            job = submitting
            if not store.reserve(key, config, instant):
                def release_unspent(old):
                    if old.get("attempt_token") != submitting["attempt_token"] or old["status"] != "submitting":
                        raise OwnerSampleError("submission_attempt_mismatch")
                    return {k: v for k, v in {**old, "status": "prepared"}.items()
                            if k not in {"attempt_token", "reserved_at", "lease_at"}}
                job = store.mutate("jobs/" + key, release_unspent)
                return _result(job, ok=False, kind="budget_blocked")
            latest, _ = store.read("jobs/" + key)
            if latest.get("attempt_token") != job["attempt_token"] or latest["status"] != "submitting":
                store.settle(key, timestamp)
                raise OwnerSampleError("submission_attempt_mismatch")
            try:
                provider_id = provider.submit(FIRST_NAME, key)
            except Exception as exc:
                uncertain = getattr(exc, "uncertain", True)
                status = "uncertain" if uncertain else "failed"
                kind = _error_kind(exc, "submission_uncertain" if uncertain else "submission_rejected")
                job = store.mutate("jobs/" + key, lambda old: {**old, "status": status, "error_kind": kind})
                if not uncertain:
                    job = _settle(store, job)
                return _result(job, ok=False, kind=kind)
            receipt = _receipt(job, provider_id)
            _validate_receipt(receipt, job)
            local_saved = False
            try:
                _save_local_receipt(_local_receipt_path(nonce, receipt_dir), receipt)
                local_saved = True
            except (OSError, ValueError):
                pass
            try:
                _save_remote_receipt(store, job, receipt)
                job = _attach_provider(store, job, provider_id)
            except Exception:
                return _result(job, ok=False, kind="provider_id_persistence_pending", provider_id=provider_id,
                               local_receipt_saved=local_saved)
            return _result(job)
        if job["status"] not in {"submitted", "processing"} or not _PROVIDER_ID.fullmatch(str(job.get("provider_id", ""))):
            raise OwnerSampleError("submission_state_mismatch")
        status = provider.inspect(job["provider_id"])
        if status.get("provider_id") != job["provider_id"]:
            raise OwnerSampleError("provider_identity_mismatch")
        if status["status"] == "failed":
            job = store.mutate("jobs/" + key, lambda old: {**old, "status": "failed", "error_kind": "render_failed"})
            return _result(_settle(store, job), ok=False, kind="render_failed")
        if status["status"] != "completed":
            return _result(job, provider_status=status["status"])
        job = _settle(store, job, status["duration"])
        media = provider.prepare_media(status, FIRST_NAME)
        hashes = {kind: hashlib.sha256(media[field]).hexdigest() for kind, field in _MEDIA.items()}
        if job.get("asset_sha256") and job["asset_sha256"] != hashes:
            raise OwnerSampleError("prepared_media_changed")
        job = store.mutate("jobs/" + key, lambda old: {**old, "asset_sha256": hashes,
                                                     "caption_source": media.get("caption_source", "unknown")})
        store.finish(key, job["provider_id"], media)
        job, _ = store.read("jobs/" + key)
        return _verified_result(store, job)
    except Exception as exc:
        result = {"ok": False, "kind": _error_kind(exc, "owner_step_unavailable"), "job_key": key}
        if isinstance(job, dict):
            result["status"] = job.get("status", "unknown")
            if _HEX.fullmatch(str(job.get("id", ""))):
                result["opaque_id"] = job["id"]
            if _PROVIDER_ID.fullmatch(str(job.get("provider_id", ""))):
                result["provider_id"] = job["provider_id"]
        return result


def read_status(store, nonce):
    if not isinstance(nonce, str) or not _HEX.fullmatch(nonce):
        raise OwnerSampleError("explicit_32_hex_nonce_required")
    job, _ = store.read("jobs/owner-probe-" + nonce)
    if job is None:
        return {"ok": False, "kind": "owner_sample_not_found", "read_only": True}
    _validate_job(job, nonce)
    result = _result(job, read_only=True, generation_retired=True)
    if job["status"] == "ready":
        result.update(_verify_publication(store, job))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nonce", help="Persist and reuse one explicit 32-character lowercase hex nonce.")
    parser.add_argument("--check", action="store_true", help="Report this generator as retired, without accessing credentials.")
    parser.add_argument("--status", action="store_true", help="Read stored status only; never render, settle, or publish.")
    args = parser.parse_args(argv)
    if args.status and (args.check or not args.nonce):
        parser.error("--status requires --nonce and cannot be combined with --check")
    if not args.status:
        print(json.dumps({"ok": False, "kind": "legacy_avatar_generation_retired", "generation_enabled": False}, sort_keys=True))
        return 2
    try:
        config = validate_preflight(os.environ)
        store = Store(GCSObjects(config.bucket))
        result = read_status(store, args.nonce)
    except Exception as exc:
        result = {"ok": False, "kind": _error_kind(exc, "owner_step_unavailable")}
    print(json.dumps(result, sort_keys=True))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
