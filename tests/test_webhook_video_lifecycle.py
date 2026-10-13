from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from test_internal_sms_endpoint import FakeSendResult, _import_webhook_server


def _import_isolated_app(monkeypatch):
    monkeypatch.setenv("VIDEO_FOLLOWUP_ENABLED", "false")
    monkeypatch.setenv("VIDEO_FOLLOWUP_GENERATION_ENABLED", "false")
    monkeypatch.setenv("VIDEO_FOLLOWUP_LIVE_APPROVED", "false")
    monkeypatch.setenv("VIDEO_FOLLOWUP_INITIALS_AFTER", "")
    monkeypatch.setenv("VIDEO_FOLLOWUP_TEST_PHONES", "")
    monkeypatch.setattr("requests.sessions.Session.request", Mock(side_effect=AssertionError("Network is forbidden")))
    module, sheet, sender = _import_webhook_server(monkeypatch, sender_result=FakeSendResult())
    legacy_start = Mock()
    monkeypatch.setattr(module, "_ensure_pilot_verifier_queue_thread", legacy_start)
    monkeypatch.setattr(module, "_recover_stale_queue_for_scheduled_window", lambda: 0)
    monkeypatch.setattr(module, "log_headless_status", lambda _logger: None)
    return module, sheet, sender, legacy_start


def test_real_app_import_and_lifecycle_preserve_video_and_existing_handlers(monkeypatch):
    module, _sheet, sender, legacy_start = _import_isolated_app(monkeypatch)
    start_video = Mock()
    stop_video = Mock()
    monkeypatch.setattr(module, "start_video_worker", start_video)
    monkeypatch.setattr(module, "stop_video_worker", stop_video)
    startup_names = [handler.__name__ for handler in module.app.router.on_startup]
    shutdown_names = [handler.__name__ for handler in module.app.router.on_shutdown]
    assert startup_names == [
        "_start_video_preparation", "_log_headless_status", "_recover_pending_queue", "_start_scheduler",
    ]
    assert shutdown_names == ["_stop_video_preparation", "_stop_scheduler"]
    start_video.assert_not_called()
    stop_video.assert_not_called()
    module._scheduler_started = True

    with TestClient(module.app) as client:
        assert client.get("/healthz").status_code == 200
        start_video.assert_called_once_with()
        legacy_start.assert_called_once_with()
        stop_video.assert_not_called()

    stop_video.assert_called_once_with()
    assert module._scheduler_started is False
    assert sender.calls == []


@pytest.mark.parametrize("enabled,generation", [("false", "false"), ("true", "false"), ("false", "true")])
def test_real_app_startup_keeps_disabled_video_worker_inert(monkeypatch, enabled, generation):
    module, _sheet, sender, legacy_start = _import_isolated_app(monkeypatch)
    import video_followup_worker as worker

    monkeypatch.setenv("VIDEO_FOLLOWUP_ENABLED", enabled)
    monkeypatch.setenv("VIDEO_FOLLOWUP_GENERATION_ENABLED", generation)
    monkeypatch.setattr(worker, "_thread", None)
    service = Mock(side_effect=AssertionError("Disabled video must not initialize storage"))
    monkeypatch.setattr(worker, "get_service", service)
    with TestClient(module.app) as client:
        assert client.get("/healthz").json()["status"] == "ok"
        assert worker._thread is None
        service.assert_not_called()
        legacy_start.assert_called_once_with()
    assert worker._stop.is_set()
    assert sender.calls == []
