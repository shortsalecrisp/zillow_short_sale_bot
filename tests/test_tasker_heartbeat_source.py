from pathlib import Path


SOURCE = (
    Path(__file__).resolve().parents[1]
    / "apps_script"
    / "zz_unified_post.js"
).read_text(encoding="utf-8")


def test_tasker_heartbeat_is_routed_and_logged_without_crm_processing():
    assert "tasker_heartbeat: true" in SOURCE
    assert 'if (action === "tasker_heartbeat")' in SOURCE
    assert 'appendSmsDebugLog_("tasker_heartbeat"' in SOURCE
    heartbeat = SOURCE.split('if (action === "tasker_heartbeat")', 1)[1].split(
        'if (action === "codex_probe")', 1
    )[0]
    assert "handleIncomingSms_" not in heartbeat
    assert "findOrCreateRowByPhone_" not in heartbeat


def test_tasker_transport_health_is_persisted_and_exposed():
    outbox = (
        Path(__file__).resolve().parents[1]
        / "apps_script"
        / "sms_outbox.js"
    ).read_text(encoding="utf-8")
    assert "transport_health: true" in SOURCE
    assert 'if (action === "transport_health")' in SOURCE
    assert 'recordTaskerTransportActivityV12_("heartbeat", body)' in SOURCE
    assert 'recordTaskerTransportActivityV12_("claim", body)' in SOURCE
    assert "function recordTaskerTransportActivityV12_" in outbox
    assert "function getTaskerTransportHealthV12_" in outbox
    assert "SMS_TASKER_LAST_ACTIVITY_MS" in outbox
    assert "monitorTaskerTransportHealthV12_: 5" in outbox
    assert "outbound SMS is being held instead of marked sent" in outbox
