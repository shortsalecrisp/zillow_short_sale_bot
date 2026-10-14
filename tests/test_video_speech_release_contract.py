"""Keep the two independently deployed speech-proof gates in agreement."""
from pathlib import Path

from video_followup_speech_guard import (
    CONFIG_SHA256,
    MODEL_ID,
    MODEL_MANIFEST_SHA256,
    MODEL_REVISION,
    PACKAGE_VERSIONS,
    POLICY_VERSION,
)

ROOT = Path(__file__).resolve().parents[1]


def test_apps_script_uses_the_exact_python_proof_policy():
    source = (ROOT / "apps_script/sms_outbox.js").read_text()
    for field, value in {
        "policy_version": POLICY_VERSION,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "model_manifest_sha256": MODEL_MANIFEST_SHA256,
        "config_sha256": CONFIG_SHA256,
    }.items():
        assert f'proof.{field} !== "{value}"' in source


def test_speech_runtime_is_separate_and_version_pinned():
    pinned = {}
    for line in (ROOT / "requirements-video-speech-guard.txt").read_text().splitlines():
        if line.strip() and not line.startswith("#"):
            package, version = line.split("==")
            pinned[package] = version
    assert pinned == PACKAGE_VERSIONS
    assert "faster-whisper" not in (ROOT / "requirements.txt").read_text()
    assert "python scripts/provision_video_speech_guard.py" in (ROOT / "render.yaml").read_text()
    startup = (ROOT / "start.sh").read_text()
    assert "python video_followup_speech_guard.py --self-test" in startup
    for flag in ("ENABLED", "GENERATION_ENABLED", "LIVE_APPROVED"):
        assert f"export VIDEO_FOLLOWUP_{flag}=false" in startup
