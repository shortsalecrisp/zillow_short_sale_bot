from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_web_process_does_not_claim_initial_sms_queue_when_worker_is_enabled():
    source = (ROOT / "webhook_server.py").read_text()
    callback_start = source.index("def _process_initial_sms_queue_callback")
    callback_end = source.index("\ndef _ensure_scheduler_thread", callback_start)
    callback = source[callback_start:callback_end]

    assert "INITIAL_SMS_QUEUE_WORKER_ENABLED" in callback
    assert "if not INITIAL_SMS_QUEUE_WORKER_ENABLED:" in callback
    assert "return" in callback.split("if not INITIAL_SMS_QUEUE_WORKER_ENABLED:", 1)[1].split("if not _within_initial_hours", 1)[0]


def test_render_declares_a_dedicated_sms_queue_worker_and_health_probe():
    render = (ROOT / "render.yaml").read_text()

    assert "healthCheckPath: /healthz" in render
    assert "name: zillow-short-sale-sms-queue" in render
    assert "startCommand: python sms_queue_worker.py" in render
    assert "INITIAL_SMS_QUEUE_WORKER_ENABLED" in render


def test_queue_worker_uses_bounded_polling_and_batch_size():
    source = (ROOT / "sms_queue_worker.py").read_text()

    assert "POLL_SECONDS = max(15" in source
    assert "BATCH_SIZE = max(1, min(100" in source
    assert "_process_initial_sms_queue(max_items=BATCH_SIZE)" in source
