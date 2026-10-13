"""Report configuration readiness without contacting providers or sending SMS."""
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from video_followup import Config


def readiness():
    required = ("HEYGEN_API_KEY", "VIDEO_FOLLOWUP_STORAGE_BUCKET", "VIDEO_FOLLOWUP_PUBLIC_ORIGIN",
                "GCP_SERVICE_ACCOUNT_JSON", "TASKER_TRANSPORT_HEALTH_URL", "SMS_CHATBOT_ALLOWED_TOKEN",
                "VIDEO_FOLLOWUP_INITIALS_AFTER")
    missing = [key for key in required if not os.getenv(key, "").strip()]
    try:
        config = Config()
    except (ValueError, TypeError):
        return {"configuration_valid": False, "missing": missing,
                "ready_for_live": False, "reason": "Invalid video configuration; values withheld"}
    return {
        "configuration_valid": True,
        "feature_enabled": config.enabled,
        "generation_enabled": config.generation_enabled,
        "live_approved_flag": config.live,
        "owner_allowlist_count": len(config.allowlist),
        "daily_limit_cents": config.daily_cents,
        "monthly_limit_cents": config.monthly_cents,
        "missing": missing,
        "ready_for_live": False,
        "reason": "This offline check cannot prove API funding, hosting, deployed parity, or phone delivery",
        "required_evidence": ["private bucket read/write/CAS", "branded HTTPS playback",
                              "current Apps Script eligibility contract", "funded HeyGen API access",
                              "owner-only Tasker receipt, playback and reply cancellation",
                              "explicit production activation approval"],
    }


if __name__ == "__main__":
    print(json.dumps(readiness(), indent=2))
