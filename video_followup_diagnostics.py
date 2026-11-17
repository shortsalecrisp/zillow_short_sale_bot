"""Bounded stored-state diagnostics: no providers, workers, eligibility or writes."""
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from video_followup import Config, GCSObjects, Store, iso

MAX_KEYS = 20
KEY = re.compile(r"[a-f0-9]{64}")
PIPELINE = re.compile(r"same-take-[a-f0-9]{32}")
JOB_FIELDS = ("id", "followup_key", "status", "variant", "created_at", "initial_sent_at",
              "first_name", "stable_id", "phone", "provider_id", "reserved_at", "budget_settled",
              "failure_stage", "underlying_code", "exception_type", "failure_at", "reason",
              "preflight_confirmed_unpaid", "verification_retry_pending", "verification_retry_exhausted",
              "provider_result_handling_version", "provider_http_status", "provider_request_id",
              "provider_error_type", "provider_response_sha256", "budget_hold_reason", "budget_blocked_at",
              "budget_hold_inflight", "budget_hold_today_cents", "budget_hold_month_cents")
PIPELINE_FIELDS = ("id", "stage", "created_at", "updated_at", "job_key", "failure_code",
                   "failure_stage", "underlying_code", "exception_type", "reserved_cents",
                   "provider_result_handling_version", "provider_http_status", "provider_request_id",
                   "provider_error_type", "provider_response_sha256")


def validate_keys(raw):
    keys = raw.split(",") if isinstance(raw, str) else []
    if not 1 <= len(keys) <= MAX_KEYS or len(set(keys)) != len(keys) or any(not KEY.fullmatch(k) for k in keys):
        raise ValueError("Bounded exact keys required")
    return keys


def _public_fields(value, fields):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in fields:
        item = value.get(key)
        if isinstance(item, str):
            if len(item) > 256 or any(x in item.lower() for x in ("http", "bearer ", "token=", "\n", "\r")):
                continue
        elif item is not None and type(item) not in (int, float, bool):
            continue
        result[key] = item
    return result


def snapshot(keys, *, store=None, config=None, now=None):
    validate_keys(",".join(keys))
    config = config or Config()
    store = store or Store(GCSObjects(config.bucket, read_only=True))
    now = now or datetime.now(timezone.utc)
    ledger, generation = store.read("budget/ledger")
    if not isinstance(ledger, dict) or not generation:
        raise ValueError("Budget evidence is unavailable")
    entries = (ledger or {}).get("reservations", {})
    if not isinstance(entries, dict) or len(entries) > 10000:
        raise ValueError("Invalid bounded budget ledger")
    inflight = [key for key, entry in entries.items() if entry.get("inflight")]
    if len(inflight) > 20:
        raise ValueError("In-flight inventory exceeds diagnostic bound")
    selected = list(dict.fromkeys(keys + [key for key in inflight if KEY.fullmatch(key)]))
    jobs = []
    for key in selected:
        job, job_generation = store.read("jobs/" + key)
        item = {"key": key, "generation": job_generation, "found": isinstance(job, dict),
                "job": _public_fields(job, JOB_FIELDS)}
        provider = (job or {}).get("provider_id")
        if isinstance(provider, str) and PIPELINE.fullmatch(provider):
            pipeline, pipeline_generation = store.read("same-take/" + provider)
            item["pipeline"] = {"generation": pipeline_generation,
                                "state": _public_fields(pipeline, PIPELINE_FIELDS)}
        jobs.append(item)
    end_ledger, end_generation = store.read("budget/ledger")
    day = now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    return {"schema": "video-followup-diagnostics-v1", "observed_at": iso(now),
        "read_only": True, "complete": generation == end_generation and ledger == end_ledger,
        "scope": "Exact selected stored jobs/pipelines and bounded budget, not a complete media or delivery audit",
        "jobs": jobs, "budget": {"generation": generation, "inflight_count": len(inflight),
            "inflight": [{"key": key, **_public_fields(entries[key], ("day", "cents", "inflight", "accounting_method"))}
                         for key in inflight],
            "accounted_today_cents": sum(e["cents"] for e in entries.values() if e["day"] == day),
            "accounted_month_cents": sum(e["cents"] for e in entries.values() if e["day"][:7] == day[:7]),
            "daily_cap_cents": config.daily_cents, "monthly_cap_cents": config.monthly_cents,
            "per_video_cap_cents": config.reserve_cents, "inflight_cap": 2},
        "provider_calls": 0, "production_writes": 0, "sent": False}
