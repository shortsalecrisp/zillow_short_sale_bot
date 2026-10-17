"""Same-take provider tests with Fal reference narration and no external calls."""
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier, Lock
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

import video_followup as core
import video_followup_same_take as provider
import video_followup_speech_guard as speech


MASTER = b"approved second recording"
REFERENCE = b"approved real voice reference"
MASTER_KEY = "assets/master/video"
TTS_ID = "01a13a1c-3763-7773-9576-f96095790e79"
FAL_ID = "12345678-abcd-1234-abcd-123456789012"
TTS_AUDIO = "https://v3b.fal.media/files/b/private/greeting.wav"
FINAL_VIDEO = "https://v3.fal.media/files/private/video.mp4"
INPUTS = {"video_bytes": b"same-take context", "audio_bytes": b"context PCM",
          "greeting_audio_bytes": b"greeting PCM"}


def speech_proof(name, audio=INPUTS["greeting_audio_bytes"], media=None):
    result = {"accepted": True, "policy_version": speech.POLICY_VERSION, "model_id": speech.MODEL_ID,
        "model_revision": speech.MODEL_REVISION, "model_manifest_sha256": speech.MODEL_MANIFEST_SHA256,
        "config_sha256": speech.CONFIG_SHA256, "source_audio_sha256": hashlib.sha256(audio).hexdigest(),
        "expected_name": name, "normalized_transcript": "hey " + name.casefold(),
        "confidence": [{"avg_logprob": -0.1, "no_speech_prob": 0.01}]}
    if media is not None:
        result.update(media_sha256=hashlib.sha256(media).hexdigest(), greeting_seconds=1.5)
    return result


class Objects:
    def __init__(self):
        self.lock, self.values, self.version = Lock(), {}, 0

    def read(self, key):
        with self.lock:
            return self.values.get(key, (None, 0))

    def write(self, key, value, version, content_type="application/json"):
        with self.lock:
            if self.values.get(key, (None, 0))[1] != version:
                raise core.Conflict()
            self.version += 1
            self.values[key] = value, self.version
            return self.version

    def signed_get_url(self, key, ttl_seconds=1800):
        return "https://storage.googleapis.com/private/" + key + "?signed=yes"

    def list(self, prefix, limit=1000):
        with self.lock:
            return [key for key in self.values if key.startswith(prefix)][:limit]

    def delete(self, key, version):
        with self.lock:
            if self.values.get(key, (None, 0))[1] != version:
                raise core.Conflict()
            self.values.pop(key, None)


class Response:
    def __init__(self, value, status=200):
        self.value, self.status_code = value, status
        self.content = json.dumps(value).encode()

    def json(self):
        return self.value

    def close(self):
        pass


def prepared(name):
    return {**INPUTS, "metadata": {"source_sha256": provider.MASTER_SHA256,
        "first_name": name, "context_seconds": 2.1, "greeting_seconds": 1.5,
        **{kind + "_sha256": hashlib.sha256(INPUTS[key]).hexdigest()
           for kind, key in (("video", "video_bytes"), ("audio", "audio_bytes"),
                             ("greeting_audio", "greeting_audio_bytes"))}}}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    for path in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
        monkeypatch.setattr(path, Mock(side_effect=AssertionError("No network in staged candidate tests")))


@pytest.fixture
def world(monkeypatch):
    monkeypatch.setattr(provider, "MASTER_SHA256", hashlib.sha256(MASTER).hexdigest())
    monkeypatch.setattr(provider, "REFERENCE_SHA256", hashlib.sha256(REFERENCE).hexdigest())
    monkeypatch.setattr(provider, "inspect_reference", Mock(return_value={"duration_seconds": 8.44}))
    monkeypatch.setattr(provider, "inspect_greeting", Mock(return_value={"duration_seconds": 1.5,
        "coarse_pitch_match": True, "identity_guaranteed": False}))
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(return_value={"ready": True}))
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=lambda audio, name: speech_proof(name, audio)))
    monkeypatch.setattr(provider, "verify_video_greeting_words", Mock(side_effect=
        lambda media, name, seconds: speech_proof(name, media=media)))
    objects = Objects()
    objects.write(MASTER_KEY, MASTER, 0)
    objects.write(provider.REFERENCE_KEY, REFERENCE, 0)
    store = core.Store(objects)
    settings = {"tts_status": "COMPLETED", "tts_result": {"audio": {"url": TTS_AUDIO}},
                "fal_status": "COMPLETED", "fal_result": {"video": {"url": FINAL_VIDEO}},
                "failure": None, "pricing_failure": None,
                "prices": {model: {"prices": [{"endpoint_id": model, "unit_price": float(maximum),
                            "unit": unit, "currency": "USD"}], "has_more": False, "next_cursor": None}
                           for model, (unit, maximum) in provider.APPROVED_RATES.items()}}
    def request(method, url, **kwargs):
        if method == "GET" and url.startswith(provider.PRICING_ENDPOINT + "?"):
            model = parse_qs(urlsplit(url).query)["endpoint_id"][0]
            if settings["pricing_failure"] == model:
                raise requests.Timeout("pricing unavailable")
            return Response(settings["prices"][model])
        if method == "POST" and settings["failure"] == url:
            raise requests.Timeout("uncertain provider outcome")
        if (method, url) == ("POST", provider.QUEUE + provider.TTS_MODEL):
            return Response({"request_id": TTS_ID,
                "status_url": provider.TTS_REQUEST_BASE + TTS_ID + "/status",
                "response_url": provider.TTS_REQUEST_BASE + TTS_ID})
        if (method, url) == ("GET", provider.TTS_REQUEST_BASE + TTS_ID + "/status"):
            return Response({"status": settings["tts_status"]})
        if (method, url) == ("GET", provider.TTS_REQUEST_BASE + TTS_ID):
            return Response(settings["tts_result"])
        if (method, url) == ("POST", provider.QUEUE + provider.MODEL):
            return Response({"request_id": FAL_ID})
        if (method, url) == ("GET", provider.REQUEST_BASE + FAL_ID + "/status"):
            return Response({"status": settings["fal_status"]})
        if (method, url) == ("GET", provider.REQUEST_BASE + FAL_ID):
            return Response(settings["fal_result"])
        raise AssertionError((method, url))
    session = Mock()
    session.request.side_effect = request
    preparer = Mock(side_effect=lambda *args, first_name: prepared(first_name))
    service = provider.SameTakeVideoProvider("kling-only-test-secret", store, MASTER_KEY, tts_key="tts-only-test-secret",
        session=session, media_session=Mock(), resolver=lambda _: ["8.8.8.8"], preparer=preparer,
        compositor=Mock(return_value={"media_bytes": b"output", "poster_bytes": b"poster",
            "captions_bytes": b"WEBVTT", "duration_seconds": 8.6, "qa": {"mouth_only": True}}))
    service._download_fal = Mock(return_value=b"WAV audio")
    return service, store, session, preparer, settings


def start(world, name="Sue", job_id="job"):
    service, store, *_ = world
    store.write("jobs/" + job_id, {"script_version": core.SCRIPT_VERSION,
                "status": "submitting", "first_name": name}, 0)
    return service.submit(name, job_id)


def state(world, identifier):
    return world[1].read("same-take/" + identifier)[0]


def paid_calls(world, model=None):
    return [call for call in world[2].request.call_args_list if call.args[0] == "POST"
            and (model is None or call.args[1] == provider.QUEUE + model)]


@pytest.mark.parametrize("name", ["Sue", "Christopher", "Jennifer", "Anne-Marie", "O'Neil"])
def test_exact_name_and_real_reference_without_preset_or_heygen(world, name):
    identifier = start(world, name)
    call = paid_calls(world)[0]
    assert call.args[1] == provider.QUEUE + provider.TTS_MODEL
    assert call.kwargs["json"] == {"text": "Hey " + name + "!", "seed": 4411,
        "temperature": 0.5, "audio_url": world[1].objects.signed_get_url(provider.REFERENCE_KEY)}
    assert "voice" not in call.kwargs["json"]
    assert call.kwargs["headers"]["X-Fal-No-Retry"] == "1"
    assert call.kwargs["allow_redirects"] is False
    assert not hasattr(world[0], "heygen")
    assert state(world, identifier)["reference_sha256"] == provider.REFERENCE_SHA256


@pytest.mark.parametrize("raw,expected", [(" \uff33\uff55\uff45 ", "Sue"), ("O\u2019Neil", "O'Neil")])
def test_normalized_name_is_exact_and_shared_with_metadata(world, raw, expected):
    identifier = start(world, raw)
    assert state(world, identifier)["first_name"] == expected
    assert paid_calls(world)[0].kwargs["json"]["text"] == "Hey " + expected + "!"


@pytest.mark.parametrize("name", ["Jim Smith", "Mary Jane", "", "unknown", "J", "Jim2"])
def test_ambiguous_name_never_pays(world, name):
    with pytest.raises(provider.VideoProviderError):
        start(world, name)
    assert not paid_calls(world)


@pytest.mark.parametrize("key", [MASTER_KEY, provider.REFERENCE_KEY])
def test_wrong_master_or_reference_blocks_all_payment(world, key):
    _, version = world[1].objects.read(key)
    world[1].objects.write(key, b"unapproved content", version)
    with pytest.raises(provider.VideoProviderError):
        start(world)
    assert not paid_calls(world)


def test_bad_reference_audio_blocks_payment(world, monkeypatch):
    monkeypatch.setattr(provider, "inspect_reference", Mock(side_effect=provider.VideoProviderError("bad", "bad WAV")))
    with pytest.raises(provider.VideoProviderError):
        start(world)
    assert not paid_calls(world)


def test_repeated_submit_and_status_never_repeat_paid_stages(world):
    service = world[0]
    identifier = start(world)
    assert service.submit("Sue", "job") == identifier
    assert service.inspect(identifier)["status"] == "processing"
    assert service.inspect(identifier)["status"] == "completed"
    assert service.inspect(identifier)["status"] == "completed"
    assert service.submit("Sue", "job") == identifier
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1
    assert state(world, identifier)["estimated_cents"] == 9
    assert state(world, identifier)["reserved_cents"] == 10
    assert "billed_cents" not in service.inspect(identifier)


def test_concurrent_submit_pays_tts_once(world):
    service, store, *_ = world
    store.write("jobs/job", {"script_version": core.SCRIPT_VERSION, "status": "submitting", "first_name": "Sue"}, 0)
    barrier = Barrier(8)
    def submit(_):
        barrier.wait(timeout=5)
        try:
            return service.submit("Sue", "job")
        except core.Conflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=8) as executor:
        result = list(executor.map(submit, range(8)))
    assert len({value for value in result if value != "conflict"}) == 1
    assert len(paid_calls(world)) == 1


def test_competing_polls_pay_lipsync_once(world):
    identifier = start(world)
    barrier = Barrier(2)
    def poll(_):
        barrier.wait(timeout=5)
        try:
            return world[0].inspect(identifier)
        except core.Conflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as executor:
        result = list(executor.map(poll, range(2)))
    assert all(value == "conflict" or value["status"] in {"processing", "completed"} for value in result)
    assert world[3].call_count == 1
    assert len(paid_calls(world, provider.MODEL)) == 1


@pytest.mark.parametrize("model", [provider.TTS_MODEL, provider.MODEL])
def test_uncertain_paid_stage_never_resubmits(world, model):
    world[4]["failure"] = provider.QUEUE + model
    if model == provider.TTS_MODEL:
        with pytest.raises(provider.SubmissionUncertain):
            start(world)
        identifier = world[0].submit("Sue", "job")
    else:
        identifier = start(world)
        with pytest.raises(provider.SubmissionUncertain):
            world[0].inspect(identifier)
    assert world[0].inspect(identifier)["status"] == "failed"
    assert world[0].submit("Sue", "job") == identifier
    assert len(paid_calls(world, model)) == 1


@pytest.mark.parametrize("mode", ["cancelled", "frozen_text", "ready", "accounting_only"])
def test_cancelled_or_accounting_only_never_starts_lipsync(world, mode):
    identifier = start(world)
    if mode != "accounting_only":
        world[1].mutate("jobs/job", lambda old: {**old, "status": mode})
    assert world[0].inspect(identifier, accounting_only=mode == "accounting_only")["status"] == "failed"
    assert not paid_calls(world, provider.MODEL)
    world[3].assert_not_called()
    world[0]._download_fal.assert_not_called()


def test_reply_during_preparation_cancels_before_lipsync_payment(world):
    identifier = start(world)
    def prepare(*args, first_name):
        world[1].mutate("jobs/job", lambda old: {**old, "status": "cancelled"})
        return prepared(first_name)
    world[3].side_effect = prepare
    assert world[0].inspect(identifier)["status"] == "failed"
    assert not paid_calls(world, provider.MODEL)


def test_reply_after_narration_lease_cancels_before_tts_payment(world):
    service = world[0]
    service._active = Mock(side_effect=[True, False])
    identifier = start(world)
    assert state(world, identifier)["stage"] == "cancelled"
    assert not paid_calls(world)


@pytest.mark.parametrize("duration", [0, -1, 3.01, float("nan"), float("inf")])
def test_decoded_not_reported_duration_blocks_lipsync(world, monkeypatch, duration):
    identifier = start(world)
    world[4]["tts_result"]["duration"] = 1.0
    monkeypatch.setattr(provider, "inspect_greeting", Mock(return_value={"duration_seconds": duration}))
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["stage"] == "failed"
    assert not paid_calls(world, provider.MODEL)


def test_failed_audio_guard_stops_without_paid_retry(world, monkeypatch):
    identifier = start(world)
    monkeypatch.setattr(provider, "inspect_greeting", Mock(side_effect=provider.VideoProviderError("voice_anomaly", "Review audio")))
    assert world[0].inspect(identifier)["status"] == "failed"
    assert world[0].inspect(identifier)["status"] == "failed"
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("phase,code", [
    ("audio_download", "fal_download_failed"),
    ("audio_screen", "narration_pitch_outlier"),
    ("media_prepare", "media_processing_failed"),
])
def test_preparation_failure_records_safe_stage_and_code_without_retry(world, monkeypatch, phase, code):
    identifier = start(world)
    sensitive = "https://private.example/media?token=never-store-this"
    error = provider.VideoProviderError(code, sensitive)
    if phase == "audio_download":
        world[0]._download_fal.side_effect = error
    elif phase == "audio_screen":
        monkeypatch.setattr(provider, "inspect_greeting", Mock(side_effect=error))
    else:
        world[3].side_effect = error
    result = world[0].inspect(identifier)
    assert result == {"provider_id": identifier, "status": "failed",
        "failure_code": "greeting_preparation_failed", "failure_stage": phase,
        "underlying_code": code, "exception_type": "VideoProviderError"}
    failed = state(world, identifier)
    assert failed["stage"] == "failed"
    assert failed["failure_code"] == "greeting_preparation_failed"
    assert failed["failure_stage"] == phase
    assert failed["underlying_code"] == code
    assert failed["exception_type"] == "VideoProviderError"
    assert sensitive not in json.dumps(failed)
    if phase == "media_prepare":
        assert failed["measured_audio_qa"] == {"duration_seconds": 1.5}
    else:
        assert "measured_audio_qa" not in failed
    assert world[0].submit("Sue", "job") == identifier
    assert world[0].inspect(identifier)["status"] == "failed"
    assert len(paid_calls(world)) == 1
    assert not paid_calls(world, provider.MODEL)


def test_preparation_diagnostics_only_keep_bounded_numeric_completed_qa(world, monkeypatch):
    identifier = start(world)
    monkeypatch.setattr(provider, "inspect_greeting", Mock(return_value={
        "duration_seconds": 1.2, "quality": {
            "median_f0_hz": 212.0, "reference_median_f0_hz": 221.0,
            "pitch_shift_semitones": -0.7, "clipped_sample_fraction": float("nan"),
            "active_rms_dbfs": float("inf"), "prepared_greeting_seconds": True,
            "media_url": "https://private.example/secret", "secret": "do-not-store",
        },
    }))
    world[3].side_effect = RuntimeError("https://private.example/key=do-not-store")
    assert world[0].inspect(identifier)["status"] == "failed"
    failed = state(world, identifier)
    assert failed["failure_stage"] == "media_prepare"
    assert failed["underlying_code"] == "unclassified_preparation_failure"
    assert failed["exception_type"] == "RuntimeError"
    assert failed["measured_audio_qa"] == {
        "duration_seconds": 1.2, "median_f0_hz": 212.0,
        "reference_median_f0_hz": 221.0, "pitch_shift_semitones": -0.7,
    }
    assert "do-not-store" not in json.dumps(failed)
    assert "private.example" not in json.dumps(failed)
    assert len(paid_calls(world)) == 1


def test_preparation_diagnostics_never_copy_untrusted_code_type_or_exception_fields(world, monkeypatch):
    identifier = start(world)
    error_type = type("secret_injected_exception_name", (provider.VideoProviderError,), {})
    error = error_type("secret_token_that_looks_like_a_code", "private error details")
    error.audio_qa = {"duration_seconds": 1.0}
    monkeypatch.setattr(provider, "inspect_greeting", Mock(side_effect=error))
    result = world[0].inspect(identifier)
    assert result["status"] == "failed"
    assert result["underlying_code"] == "unclassified_preparation_failure"
    assert result["exception_type"] == "OtherException"
    failed = state(world, identifier)
    assert failed["underlying_code"] == "unclassified_preparation_failure"
    assert failed["exception_type"] == "OtherException"
    assert "measured_audio_qa" not in failed
    assert "secret_" not in json.dumps(failed)
    assert "private error" not in json.dumps(failed)
    assert len(paid_calls(world)) == 1


def test_pitch_rejection_measurements_are_durable_without_lipsync_or_retry(world, monkeypatch):
    identifier = start(world)
    measured = {"duration_seconds": 1.66, "median_f0_hz": 163.8807,
                "reference_median_f0_hz": 221.5251, "pitch_shift_semitones": -5.2179}
    error = provider.VideoProviderError("narration_pitch_outlier", "private error details")
    error.acoustic_diagnostics = {**measured, "url": "https://private/?token=hidden",
        "raw_audio": b"private", "active_rms_dbfs": float("inf"), "prepared_greeting_seconds": True}
    screen = Mock(side_effect=error)
    monkeypatch.setattr(provider, "inspect_greeting", screen)
    expected = {"provider_id": identifier, "status": "failed", "failure_code": "greeting_preparation_failed",
                "failure_stage": "audio_screen", "underlying_code": "narration_pitch_outlier",
                "exception_type": "VideoProviderError"}
    assert world[0].inspect(identifier) == expected
    screen.assert_called_once()
    failed = state(world, identifier)
    assert failed["measured_audio_qa"] == measured
    assert failed["last_preparation_failure"]["measured_audio_qa"] == measured
    assert failed["preparation_attempts"] == 1 and failed["preparation_retry_after"] is None
    assert "private" not in json.dumps(failed) and "hidden" not in json.dumps(failed)
    world[3].assert_not_called()
    assert world[0].inspect(identifier) == expected
    assert world[0].submit("Sue", "job") == identifier
    screen.assert_called_once()
    assert len(paid_calls(world)) == 1 and not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("stage,code", [
    ("fal_submitting", None), ("fal_pending", None), ("completed", None),
    ("failed", "provider_price_not_approved"),
])
def test_competing_transition_cannot_be_reported_as_preparation_failure(world, stage, code):
    identifier = start(world)
    def prepare(*args, **kwargs):
        world[1].mutate("same-take/" + identifier, lambda old: {**old, "stage": stage,
            "failure_code": code})
        raise provider.VideoProviderError("media_processing_failed", "Concurrent preparation failed")
    world[3].side_effect = prepare
    with pytest.raises(provider.VideoProviderError, match="Concurrent preparation failed"):
        world[0].inspect(identifier)
    assert state(world, identifier)["stage"] == stage
    assert state(world, identifier).get("failure_code") == code
    assert len(paid_calls(world)) == 1
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("failure", [core.Conflict(), OSError("Storage unavailable")])
def test_preparation_failure_write_uncertainty_propagates(world, monkeypatch, failure):
    identifier = start(world)
    world[3].side_effect = provider.VideoProviderError("media_processing_failed", "Preparation failed")
    monkeypatch.setattr(world[0], "_update", Mock(side_effect=failure))
    with pytest.raises(type(failure)):
        world[0].inspect(identifier)
    assert state(world, identifier)["stage"] == "narration_pending"
    assert len(paid_calls(world)) == 1
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("read_failure", ["storage", "identity"])
def test_preparation_failure_requires_fresh_identity_validated_readback(world, monkeypatch, read_failure):
    identifier = start(world)
    world[3].side_effect = provider.VideoProviderError("media_processing_failed", "Preparation failed")
    update = world[0]._update
    def update_then_break_read(*args, **kwargs):
        value = update(*args, **kwargs)
        if read_failure == "storage":
            monkeypatch.setattr(world[0], "_read", Mock(side_effect=OSError("Readback unavailable")))
        else:
            world[1].mutate("same-take/" + identifier, lambda old: {**old, "first_name": "Christopher"})
        return value
    monkeypatch.setattr(world[0], "_update", update_then_break_read)
    with pytest.raises(OSError if read_failure == "storage" else provider.VideoProviderError):
        world[0].inspect(identifier)
    assert state(world, identifier)["stage"] == "failed"
    assert len(paid_calls(world)) == 1
    assert not paid_calls(world, provider.MODEL)


def test_worker_terminalizes_preparation_rejection_immediately_without_retry(world, monkeypatch, caplog):
    service, store, *_ = world
    context = core.snapshot(row=2, stable_id="listing", phone="9542053205", first_name="Sue",
                            initial_sent_at="2026-10-14T12:00:00Z")
    config = core.Config({"VIDEO_FOLLOWUP_ENABLED": "true", "VIDEO_FOLLOWUP_GENERATION_ENABLED": "true",
        "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-14T00:00:00Z", "VIDEO_FOLLOWUP_TEST_PHONES": "9542053205"})
    store.prepare(context)
    key = context["followup_key"]
    worker = core.Worker(store, service, config, lambda _: True)
    worker.tick()
    submitted, _ = store.read("jobs/" + key)
    assert submitted["status"] == "submitted"
    monkeypatch.setattr(provider, "inspect_greeting", Mock(side_effect=
        provider.VideoProviderError("narration_pitch_outlier", "Pitch guard rejected audio")))
    worker.tick()
    failed, _ = store.read("jobs/" + key)
    ledger, _ = store.read("budget/ledger")
    assert failed["status"] == "failed"
    assert ledger["reservations"][key]["inflight"] is False
    assert ledger["reservations"][key]["cents"] == 10
    assert store.objects.read("active/" + key)[0] is None
    assert "will retry safely" not in caplog.text
    worker.tick()
    assert len(paid_calls(world)) == 1
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("audio", [TTS_AUDIO, {"url": TTS_AUDIO}])
def test_tts_audio_output_shapes_both_use_guarded_download(world, audio):
    identifier = start(world)
    world[4]["tts_result"] = {"audio": audio}
    world[0].inspect(identifier)
    world[0]._download_fal.assert_called_once_with(TTS_AUDIO)
    provider.inspect_greeting.assert_called_once_with(b"WAV audio", REFERENCE)


@pytest.mark.parametrize("url", ["https://evil.example/steal", "http://queue.fal.run/fal-ai/chatterbox/requests/" + TTS_ID,
    "https://queue.fal.run.evil.example/fal-ai/chatterbox/requests/" + TTS_ID,
    "https://queue.fal.run/fal-ai/chatterbox/requests/other",
    "https://queue.fal.run/fal-ai/chatterbox/requests/" + TTS_ID + "?forward=evil",
    "https://secret@queue.fal.run/fal-ai/chatterbox/requests/" + TTS_ID])
def test_authenticated_tts_request_urls_are_restricted(url):
    with pytest.raises(provider.VideoProviderError):
        provider.tts_request_url(TTS_ID, url)


def test_documented_canonical_tts_paths_accept_only_matching_request():
    base = provider.TTS_REQUEST_BASE
    assert provider.tts_request_url(TTS_ID, base + TTS_ID) == base + TTS_ID
    assert provider.tts_request_url(TTS_ID, base + TTS_ID + "/status", status=True) == base + TTS_ID + "/status"


def test_full_model_tts_poll_path_is_not_the_canonical_queue_route():
    with pytest.raises(provider.VideoProviderError):
        provider.tts_request_url(TTS_ID, provider.QUEUE + provider.TTS_MODEL + "/requests/" + TTS_ID)


def test_tampered_poll_url_is_rejected_before_authenticated_call(world):
    identifier = start(world)
    world[1].mutate("same-take/" + identifier, lambda old: {**old, "narration_status_url": "https://evil.example/"})
    before = world[2].request.call_count
    with pytest.raises(provider.VideoProviderError):
        world[0].inspect(identifier)
    assert world[2].request.call_count == before


@pytest.mark.parametrize("field,value", [("first_name", "Christopher"), ("reference_sha256", "0" * 64),
    ("render_config_sha256", "0" * 64), ("reserved_cents", 11), ("script_version", "face-to-text-same-take-v2")])
def test_pipeline_identity_or_price_change_blocks_processing(world, field, value):
    identifier = start(world)
    world[1].mutate("same-take/" + identifier, lambda old: {**old, field: value})
    with pytest.raises(provider.VideoProviderError):
        world[0].inspect(identifier)
    assert not paid_calls(world, provider.MODEL)


def test_changed_name_cannot_be_reused_for_same_job(world):
    start(world)
    world[1].mutate("jobs/job", lambda old: {**old, "first_name": "Christopher"})
    with pytest.raises(provider.VideoProviderError):
        world[0].submit("Christopher", "job")
    assert len(paid_calls(world)) == 1


def test_rate_estimate_above_cap_blocks_configuration(world, monkeypatch):
    monkeypatch.setattr(provider, "TTS_REQUEST_CENTS", 4)
    with pytest.raises(provider.VideoProviderError):
        provider.SameTakeVideoProvider("secret", world[1], MASTER_KEY, tts_key="tts-secret")


def test_new_pipeline_version_quarantines_old_heygen_jobs(world):
    assert core.SCRIPT_VERSION == "face-to-text-same-take-turbo-v3"
    world[1].write("jobs/old", {"script_version": "face-to-text-same-take-v2",
                              "status": "submitting", "first_name": "Sue"}, 0)
    with pytest.raises(provider.VideoProviderError):
        world[0].submit("Sue", "old")
    assert not paid_calls(world)


def test_tts_and_kling_credentials_are_routed_separately(world):
    identifier = start(world)
    world[0].inspect(identifier)
    world[0].inspect(identifier)
    for call in world[2].request.call_args_list:
        expected = "tts-only-test-secret" if "chatterbox" in call.args[1] else "kling-only-test-secret"
        assert call.kwargs["headers"]["Authorization"] == "Key " + expected


def test_missing_tts_key_cannot_fall_back_to_kling_key(world):
    with pytest.raises(provider.VideoProviderError):
        provider.SameTakeVideoProvider("kling-key", world[1], MASTER_KEY, tts_key="")
    assert not paid_calls(world)


def test_full_path_uses_one_tts_one_lipsync_and_retains_original_master(world):
    identifier = start(world, "Christopher")
    world[0].inspect(identifier)
    completed = world[0].inspect(identifier)
    result = world[0].prepare_media(completed, "Christopher")
    assert result["media_bytes"] == b"output"
    assert world[0].compositor.call_args.args[0] == MASTER
    assert world[0].compositor.call_args.kwargs["first_name"] == "Christopher"
    assert len(paid_calls(world)) == 2


@pytest.mark.parametrize("url", ["http://v3.fal.media/files/a", "https://evil.example/files/a",
    "https://v3.fal.media.evil.example/files/a", "https://user:pass@v3.fal.media/files/a",
    "https://v3.fal.media:444/files/a", "https://v3.fal.media/private/a",
    "https://v3.fal.media/files/a#secret", "https://127.0.0.1/files/a",
    "https://v3.fal.media/files/a\\b", None])
def test_fal_media_ssrf_is_rejected_before_network(world, url):
    with pytest.raises(provider.VideoProviderError):
        provider.SameTakeVideoProvider._download_fal(world[0], url)
    world[0].media_session.get.assert_not_called()


@pytest.mark.parametrize("addresses", [["127.0.0.1"], ["10.0.0.2"], ["169.254.169.254"],
    ["::1"], ["8.8.8.8", "10.0.0.1"], []])
def test_private_dns_answers_block_all_fal_downloads(world, addresses):
    world[0].resolver = lambda hostname: addresses
    with pytest.raises(provider.VideoProviderError):
        provider.SameTakeVideoProvider._download_fal(world[0], TTS_AUDIO)
    world[0].media_session.get.assert_not_called()


@pytest.mark.parametrize("field,value", [("source_sha256", "wrong-source"), ("first_name", "Christopher"),
    ("context_seconds", 5.01), ("context_seconds", 1.99), ("context_seconds", float("nan")),
    ("video_sha256", "changed"), ("audio_sha256", "changed"), ("greeting_audio_sha256", "changed")])
def test_prepared_input_identity_and_price_block_are_verified(world, field, value):
    identifier = start(world)
    inputs = prepared("Sue")
    inputs["metadata"][field] = value
    world[3].side_effect = None
    world[3].return_value = inputs
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["stage"] == "failed"
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("stage", ["narration_submitting", "fal_submitting"])
def test_paid_lease_expiry_never_restarts_payment(world, monkeypatch, stage):
    identifier = start(world)
    now = core.utcnow()
    world[1].mutate("same-take/" + identifier, lambda old: {**old, "stage": stage,
        "created_at": core.iso(now), "fal_attempt_at": core.iso(now)})
    monkeypatch.setattr(provider, "utcnow", lambda: now + timedelta(seconds=599))
    assert world[0].inspect(identifier)["status"] == "processing"
    monkeypatch.setattr(provider, "utcnow", lambda: now + timedelta(seconds=601))
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["stage"] == "uncertain"
    assert world[0].submit("Sue", "job") == identifier
    assert len(paid_calls(world)) == 1


def test_expected_stage_guard_cannot_revert_newer_completed_pipeline(world):
    identifier = start(world)
    world[1].mutate("same-take/" + identifier, lambda old: {**old, "stage": "completed", "media_url": FINAL_VIDEO})
    latest = world[0]._update(identifier, {"stage": "failed"}, "narration_pending")
    assert latest["stage"] == "completed"


@pytest.mark.parametrize("key", [MASTER_KEY, provider.REFERENCE_KEY])
def test_changed_master_or_reference_between_stages_blocks_lipsync(world, key):
    identifier = start(world)
    _, version = world[1].objects.read(key)
    world[1].objects.write(key, b"changed after narration", version)
    assert world[0].inspect(identifier)["status"] == "failed"
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("model", [provider.TTS_MODEL, provider.MODEL])
@pytest.mark.parametrize("field,value", [("unit_price", 0.03), ("unit_price", 0), ("unit_price", -1),
    ("unit_price", "NaN"), ("unit_price", "Infinity"), ("unit_price", None), ("unit_price", True),
    ("endpoint_id", "other-model"), ("currency", "EUR"), ("unit", "seconds")])
def test_unapproved_live_rate_stops_before_that_paid_stage(world, model, field, value):
    if model == provider.MODEL:
        identifier = start(world)
    world[4]["prices"][model]["prices"][0][field] = value
    with pytest.raises(provider.VideoProviderError):
        if model == provider.TTS_MODEL:
            start(world)
        else:
            world[0].inspect(identifier)
    assert not paid_calls(world, model)


@pytest.mark.parametrize("model", [provider.TTS_MODEL, provider.MODEL])
@pytest.mark.parametrize("payload", [{}, {"prices": []}, {"prices": None},
    {"prices": [{"unit_price": 0.01}]}, {"has_more": True, "prices": []}])
def test_missing_or_ambiguous_price_stops_before_payment(world, model, payload):
    if model == provider.MODEL:
        identifier = start(world)
    world[4]["prices"][model] = payload
    with pytest.raises(provider.VideoProviderError):
        if model == provider.TTS_MODEL:
            start(world)
        else:
            world[0].inspect(identifier)
    assert not paid_calls(world, model)


@pytest.mark.parametrize("model", [provider.TTS_MODEL, provider.MODEL])
def test_pricing_downtime_fails_closed_without_paid_retry(world, model):
    if model == provider.MODEL:
        identifier = start(world)
    world[4]["pricing_failure"] = model
    with pytest.raises(provider.VideoProviderError):
        if model == provider.TTS_MODEL:
            start(world)
        else:
            world[0].inspect(identifier)
    identifier = world[0].submit("Sue", "job")
    assert world[0].inspect(identifier)["status"] == "failed"
    assert not paid_calls(world, model)
    assert state(world, identifier)["reserved_cents"] == 10


def test_each_paid_stage_has_own_fresh_rate_readback_and_audit(world):
    identifier = start(world)
    world[0].inspect(identifier)
    saved = state(world, identifier)
    for field, model in (("tts_price", provider.TTS_MODEL), ("lipsync_price", provider.MODEL)):
        assert saved[field]["endpoint_id"] == model
        assert saved[field]["currency"] == "USD"
        assert saved[field]["verified_at"]
    calls = world[2].request.call_args_list
    for position, call in enumerate(calls):
        if call.args[0] == "POST":
            assert calls[position - 1].args[0] == "GET"
            assert calls[position - 1].args[1].startswith(provider.PRICING_ENDPOINT)
            assert calls[position - 1].kwargs["headers"]["Cache-Control"] == "no-cache"


def test_rate_verification_that_becomes_stale_before_payment_is_rejected(world):
    service = world[0]
    service._verified_price = Mock(return_value={"endpoint_id": provider.TTS_MODEL,
        "unit_price": "0.02", "unit": "1000 characters", "currency": "USD",
        "verified_at": core.iso(core.utcnow() - timedelta(seconds=61))})
    with pytest.raises(provider.VideoProviderError):
        start(world)
    assert not paid_calls(world)


@pytest.mark.parametrize("model,price", [(provider.TTS_MODEL, 0.020001), (provider.MODEL, 0.014001)])
def test_even_small_increase_above_approved_rate_is_rejected(world, model, price):
    if model == provider.MODEL:
        identifier = start(world)
    world[4]["prices"][model]["prices"][0]["unit_price"] = price
    with pytest.raises(provider.VideoProviderError):
        if model == provider.TTS_MODEL:
            start(world)
        else:
            world[0].inspect(identifier)
    assert not paid_calls(world, model)


def test_speech_model_preflight_blocks_new_paid_tts_but_not_existing_request(world, monkeypatch):
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(side_effect=
        speech.SpeechGuardError("speech_guard_unavailable")))
    with pytest.raises(provider._UnpaidPreflightTransient):
        start(world)
    assert not paid_calls(world)
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(return_value={"ready": True}))
    identifier = world[0].submit("Sue", "job")
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(side_effect=
        speech.SpeechGuardError("speech_guard_unavailable")))
    assert world[0].submit("Sue", "job") == identifier
    assert len(paid_calls(world)) == 1


@pytest.mark.parametrize("code", ["speech_guard_busy", "speech_guard_timeout", "speech_guard_unavailable",
                                   "speech_guard_execution_failed"])
def test_only_known_unpaid_preflight_transients_expose_safe_retry_contract(world, monkeypatch, code):
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(side_effect=speech.SpeechGuardError(code)))
    with pytest.raises(provider._UnpaidPreflightTransient) as caught:
        start(world)
    assert world[0].safe_unpaid_preflight_failure(caught.value, first_name="Sue", job_id="job") == {
        "failure_stage": "prepayment_speech_preflight", "underlying_code": code, "exception_type": "SpeechGuardError"}
    assert world[0].safe_unpaid_preflight_failure(caught.value, first_name="Jim", job_id="job") is None
    assert world[0].safe_unpaid_preflight_failure(caught.value, first_name="Sue", job_id="other") is None
    assert not paid_calls(world)
    assert not world[1].objects.list("same-take/")
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(return_value={"ready": True}))
    identifier = world[0].submit("Sue", "job")
    assert world[0].submit("Sue", "job") == identifier
    assert len(paid_calls(world)) == 1


@pytest.mark.parametrize("code", ["speech_guard_model_mismatch", "speech_guard_dependency_mismatch",
    "speech_guard_name_mismatch", "speech_guard_output_invalid", "speech_guard_low_confidence"])
def test_preflight_integrity_failure_is_not_retryable(world, monkeypatch, code):
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(side_effect=speech.SpeechGuardError(code)))
    with pytest.raises(speech.SpeechGuardError) as caught:
        start(world)
    assert world[0].safe_unpaid_preflight_failure(caught.value, first_name="Sue", job_id="job") is None
    assert not paid_calls(world)


@pytest.mark.parametrize("error", [speech.SpeechGuardError("speech_guard_busy"),
    provider.VideoProviderError("unpaid_preflight_transient", "forged"), provider.SubmissionUncertain("job")])
def test_generic_or_paid_submission_error_cannot_claim_unpaid_preflight_retry(world, error):
    assert world[0].safe_unpaid_preflight_failure(error, first_name="Sue", job_id="job") is None


def test_concurrent_pipeline_created_during_failed_preflight_is_reused_without_payment(world, monkeypatch):
    original = provider.preflight_speech_guard
    entered = [False]
    def preflight():
        if entered[0]:
            return original()
        entered[0] = True
        world[0].submit("Sue", "job")
        raise speech.SpeechGuardError("speech_guard_busy")
    monkeypatch.setattr(provider, "preflight_speech_guard", preflight)
    identifier = start(world)
    assert state(world, identifier)["stage"] == "narration_pending"
    assert len(paid_calls(world)) == 1
    assert world[0].submit("Sue", "job") == identifier
    assert len(paid_calls(world)) == 1


@pytest.mark.parametrize("code", ["speech_guard_name_mismatch", "speech_guard_low_confidence", "speech_guard_output_invalid"])
def test_spoken_name_rejection_blocks_lipsync_and_remains_terminal(world, monkeypatch, code):
    identifier = start(world, "Jamie")
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=speech.SpeechGuardError(code)))
    result = world[0].inspect(identifier)
    assert result["status"] == "failed"
    assert result["failure_stage"] == "raw_speech_screen"
    assert result["underlying_code"] == code
    assert world[0].submit("Jamie", "job") == identifier
    assert world[0].inspect(identifier)["status"] == "failed"
    assert len(paid_calls(world)) == 1
    assert not paid_calls(world, provider.MODEL)


def test_prepared_greeting_proof_binds_actual_audio_before_kling(world):
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "processing"
    assert [call.args for call in provider.verify_greeting_words.call_args_list] == [
        (b"WAV audio", "Sue"), (INPUTS["greeting_audio_bytes"], "Sue")]
    saved = state(world, identifier)
    assert speech.validate_speech_proof(saved["raw_speech_proof"], first_name="Sue",
        audio_sha256=hashlib.sha256(b"WAV audio").hexdigest())
    assert speech.validate_speech_proof(saved["greeting_speech_proof"], first_name="Sue",
        audio_sha256=hashlib.sha256(INPUTS["greeting_audio_bytes"]).hexdigest())
    assert len(paid_calls(world, provider.MODEL)) == 1


@pytest.mark.parametrize("code", ["speech_guard_busy", "speech_guard_timeout", "speech_guard_unavailable",
                                   "speech_guard_execution_failed"])
@pytest.mark.parametrize("phase", ["raw", "prepared"])
def test_free_preparation_retry_reuses_paid_narration_after_durable_backoff(world, monkeypatch, code, phase):
    identifier = start(world)
    now = core.utcnow()
    clock = [now]
    monkeypatch.setattr(provider, "utcnow", lambda: clock[0])
    failed = [False]
    def verify(audio, name):
        target = b"WAV audio" if phase == "raw" else INPUTS["greeting_audio_bytes"]
        if not failed[0] and audio == target:
            failed[0] = True
            raise speech.SpeechGuardError(code)
        return speech_proof(name, audio)
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=verify))
    assert world[0].inspect(identifier)["status"] == "processing"
    pending = state(world, identifier)
    assert pending["stage"] == "narration_pending"
    assert pending["narration_id"] == TTS_ID
    assert pending["preparation_attempts"] == 1
    assert pending["preparation_retry_after"] == core.iso(now + timedelta(seconds=60))
    calls = world[2].request.call_count
    assert world[0].inspect(identifier)["status"] == "processing"
    assert world[2].request.call_count == calls
    assert world[0].submit("Sue", "job") == identifier
    clock[0] += timedelta(seconds=61)
    assert world[0].inspect(identifier)["status"] == "processing"
    assert state(world, identifier)["preparation_attempts"] == 2
    assert state(world, identifier)["stage"] == "fal_pending"
    assert state(world, identifier)["last_preparation_failure"]["underlying_code"] == code
    assert "failure_stage" not in state(world, identifier)
    assert "underlying_code" not in state(world, identifier)
    assert "failure_code" not in state(world, identifier)
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1


def test_free_preparation_retry_exhaustion_is_bounded_and_never_pays_lipsync(world, monkeypatch):
    identifier = start(world)
    clock = [core.utcnow()]
    monkeypatch.setattr(provider, "utcnow", lambda: clock[0])
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=speech.SpeechGuardError("speech_guard_busy")))
    for attempt in range(3):
        result = world[0].inspect(identifier)
        assert result["status"] == ("failed" if attempt == 2 else "processing")
        clock[0] += timedelta(seconds=61)
    assert state(world, identifier)["preparation_attempts"] == 3
    assert state(world, identifier)["preparation_retry_exhausted"] is True
    assert world[0].inspect(identifier)["status"] == "failed"
    assert len(paid_calls(world)) == 1


@pytest.mark.parametrize("mode", ["cancelled", "frozen_text", "frozen_video", "ready", "accounting_only"])
def test_free_preparation_retry_never_bypasses_inactive_or_accounting_only_job(world, monkeypatch, mode):
    identifier = start(world)
    clock = [core.utcnow()]
    monkeypatch.setattr(provider, "utcnow", lambda: clock[0])
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=speech.SpeechGuardError("speech_guard_busy")))
    world[0].inspect(identifier)
    if mode != "accounting_only":
        world[1].mutate("jobs/job", lambda old: {**old, "status": mode})
    clock[0] += timedelta(seconds=61)
    assert world[0].inspect(identifier, accounting_only=mode == "accounting_only")["status"] == "failed"
    assert state(world, identifier)["preparation_attempts"] == 1
    assert len(paid_calls(world)) == 1


def test_interrupted_free_preparation_lease_can_resume_but_never_repeat_paid_narration(world, monkeypatch):
    identifier = start(world)
    now = core.utcnow()
    world[1].mutate("same-take/" + identifier, lambda old: {**old, "preparation_attempts": 1,
        "preparation_lease_until": core.iso(now + timedelta(seconds=600))})
    monkeypatch.setattr(provider, "utcnow", lambda: now + timedelta(seconds=599))
    assert world[0].inspect(identifier)["status"] == "processing"
    world[3].assert_not_called()
    monkeypatch.setattr(provider, "utcnow", lambda: now + timedelta(seconds=601))
    assert world[0].inspect(identifier)["status"] == "processing"
    assert state(world, identifier)["stage"] == "fal_pending"
    assert state(world, identifier)["preparation_attempts"] == 2
    assert len(paid_calls(world)) == 2


@pytest.mark.parametrize("model", [provider.TTS_MODEL, provider.MODEL])
def test_transient_price_read_recovery_never_repeats_paid_submit(world, monkeypatch, model):
    if model == provider.MODEL:
        identifier = start(world)
    verify = world[0]._verified_price
    failures = [0]
    def flaky(request_model, key):
        if request_model == model and failures[0] < 2:
            failures[0] += 1
            raise provider.VideoProviderError("status_unavailable", "private details")
        return verify(request_model, key)
    monkeypatch.setattr(world[0], "_verified_price", flaky)
    monkeypatch.setattr(provider.time, "sleep", Mock())
    if model == provider.TTS_MODEL:
        identifier = start(world)
    else:
        world[0].inspect(identifier)
    assert failures == [2]
    assert len(paid_calls(world, model)) == 1
    field = "tts_price_read_attempts" if model == provider.TTS_MODEL else "lipsync_price_read_attempts"
    assert state(world, identifier)[field] == 3
    assert "private details" not in json.dumps(state(world, identifier))


@pytest.mark.parametrize("count", [-1, True, 3, 999, "1", None])
def test_invalid_or_exhausted_preparation_counter_fails_closed(world, count):
    identifier = start(world)
    world[1].mutate("same-take/" + identifier, lambda old: {**old, "preparation_attempts": count})
    assert world[0].inspect(identifier)["status"] == "failed"
    world[3].assert_not_called()
    assert len(paid_calls(world)) == 1


def test_rejected_recognition_diagnostic_is_durable_and_not_a_success_proof(world, monkeypatch):
    identifier = start(world)
    details = {**speech_proof("Sue", b"WAV audio"), "observed_transcript": "Hey Jim!"}
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=speech.SpeechGuardError(
        "speech_guard_name_mismatch", diagnostics=details)))
    status = world[0].inspect(identifier)
    assert status["speech_diagnostics"]["observed_transcript"] == "Hey Jim!"
    assert status["speech_diagnostics"]["accepted"] is False
    assert state(world, identifier)["speech_diagnostics"] == status["speech_diagnostics"]
    assert "raw_speech_proof" not in state(world, identifier)
    assert len(paid_calls(world)) == 1


def test_failed_status_diagnostic_adapter_is_pure_and_allowlisted(world, monkeypatch):
    details = {**speech_proof("Sue"), "observed_transcript": "Hey Jim!"}
    raw = {"status": "failed", "failure_code": "greeting_preparation_failed",
        "failure_stage": "raw_speech_screen", "underlying_code": "speech_guard_name_mismatch",
        "exception_type": "SpeechGuardError", "speech_diagnostics": details,
        "provider_id": "secret-provider-id", "media_url": "https://private.example/secret",
        "message": "secret message"}
    monkeypatch.setattr(world[0], "_read", Mock(side_effect=AssertionError("Adapter must not read or mutate")))
    clean = world[0].safe_failure_diagnostics(raw)
    assert set(clean) == {"failure_code", "failure_stage", "underlying_code", "exception_type", "speech_diagnostics"}
    assert clean["speech_diagnostics"]["accepted"] is False
    assert clean["speech_diagnostics"]["observed_transcript"] == "Hey Jim!"
    assert "secret" not in json.dumps(clean)
    assert not world[2].request.called


@pytest.mark.parametrize("status", [None, [], {}, {"status": "processing", "failure_code": "greeting_preparation_failed"}])
def test_failed_status_adapter_ignores_nonfailure_values(world, status):
    assert world[0].safe_failure_diagnostics(status) == {}


def test_failed_status_adapter_sanitizes_unknown_codes_and_untrusted_fields(world):
    service = world[0]
    assert service.safe_failure_diagnostics({"status": "failed", "failure_code": ["secret"]}) == {
        "failure_code": "unclassified_provider_failure"}
    value = service.safe_failure_diagnostics({"status": "failed", "failure_code": "greeting_preparation_failed",
        "failure_stage": "secret", "underlying_code": "secret", "exception_type": "secret",
        "speech_diagnostics": {"secret": "secret"}})
    assert value == {"failure_code": "greeting_preparation_failed",
        "failure_stage": "unknown_preparation_stage", "underlying_code": "unclassified_preparation_failure",
        "exception_type": "OtherException"}
    assert service.safe_failure_diagnostics({"status": "failed", "failure_code": "provider_price_not_approved",
        "failure_stage": "secret"}) == {"failure_code": "provider_price_not_approved"}


@pytest.mark.parametrize("status,attempts", [(401, 1), (403, 1), (404, 1), (429, 3), (503, 3)])
def test_only_transient_price_http_errors_get_bounded_free_reads(world, monkeypatch, status, attempts):
    request = world[2].request.side_effect
    reads = []
    def failed_price(method, url, **kwargs):
        if url.startswith(provider.PRICING_ENDPOINT):
            reads.append(url)
            return Response({}, status=status)
        return request(method, url, **kwargs)
    world[2].request.side_effect = failed_price
    monkeypatch.setattr(provider.time, "sleep", Mock())
    with pytest.raises(provider.VideoProviderError):
        start(world)
    assert len(reads) == attempts
    assert not paid_calls(world)


@pytest.mark.parametrize("tamper", ["name", "hash", "policy"])
def test_invalid_greeting_proof_cannot_start_lipsync(world, monkeypatch, tamper):
    identifier = start(world)
    proof = speech_proof("Sue")
    proof[{"name": "expected_name", "hash": "source_audio_sha256", "policy": "policy_version"}[tamper]] = "wrong"
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=[speech_proof("Sue", b"WAV audio"), proof]))
    assert world[0].inspect(identifier)["status"] == "failed"
    assert not paid_calls(world, provider.MODEL)


def test_legacy_completed_paid_result_is_audited_without_regeneration(world, monkeypatch):
    identifier = start(world)
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    world[1].mutate("same-take/" + identifier, lambda old: {key: value for key, value in old.items()
        if key != "greeting_speech_proof"})
    provider.verify_greeting_words.reset_mock()
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(side_effect=AssertionError("No new TTS")))
    media = world[0].prepare_media(snapshot, "Sue")
    provider.verify_greeting_words.assert_called_once_with(INPUTS["greeting_audio_bytes"], "Sue")
    provider.verify_video_greeting_words.assert_called_once_with(b"output", "Sue", 1.5)
    assert speech.validate_speech_proof(media["speech_proof"], first_name="Sue",
        media_sha256=hashlib.sha256(b"output").hexdigest())
    assert len(paid_calls(world)) == 2


def test_final_encoded_greeting_mismatch_never_returns_publishable_media(world, monkeypatch):
    identifier = start(world)
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    monkeypatch.setattr(provider, "verify_video_greeting_words", Mock(side_effect=
        speech.SpeechGuardError("speech_guard_name_mismatch")))
    with pytest.raises(speech.SpeechGuardError):
        world[0].prepare_media(snapshot, "Sue")
    assert "final_speech_proof" not in state(world, identifier)
    assert len(paid_calls(world)) == 2


def test_legacy_media_audit_only_verifies_exact_existing_bytes_and_name(world):
    identifier = start(world)
    world[0].inspect(identifier)
    world[0].inspect(identifier)
    job = {"provider_id": identifier, "first_name": "Sue", "followup_key": "job"}
    proof = world[0].audit_media(job, b"existing final video")
    assert proof["media_sha256"] == hashlib.sha256(b"existing final video").hexdigest()
    with pytest.raises(speech.SpeechGuardError):
        world[0].audit_media({**job, "first_name": "Christopher"}, b"existing final video")
    assert len(paid_calls(world)) == 2


PROFILE_FIELDS = ("generator_profile", "generator_config", "tts_reference_key", "tts_reference_sha256")
CLEAN_REFERENCE = b"approved name-free section of the same recording"


def profile_service(world, name):
    original = world[0]
    result = provider.SameTakeVideoProvider(original.fal_key, world[1], MASTER_KEY,
        tts_key=original.tts_key, session=world[2], media_session=Mock(),
        resolver=original.resolver, preparer=world[3], compositor=original.compositor,
        generator_profile=name)
    result._download_fal = original._download_fal
    return result


@pytest.fixture
def clean_reference(world, monkeypatch):
    monkeypatch.setattr(provider, "CLEAN_BODY_REFERENCE_SHA256", hashlib.sha256(CLEAN_REFERENCE).hexdigest())
    world[1].objects.write(provider.CLEAN_BODY_REFERENCE_KEY, CLEAN_REFERENCE, 0)
    return CLEAN_REFERENCE


def remove_profile_fields(world, identifier):
    return world[1].mutate("same-take/" + identifier, lambda old:
        {key: value for key, value in old.items() if key not in PROFILE_FIELDS})


def test_unpatched_approved_generator_hashes_are_fixed():
    assert provider.RENDER_CONFIG_SHA256 == "45ec6161cfa049ac202c9c3012f76fd04fd0826371c3037258432ee29691b837"
    assert provider._generator_profile("legacy_v1")["render_config_sha256"] == provider.RENDER_CONFIG_SHA256
    assert provider._generator_profile("clean_body_v1")["render_config_sha256"] == (
        "004e3565b1c42c4a477d448a1bdf4fe1b3ee0e4d656c29e227b6e48aef7e5a93")


def test_generator_default_preserves_exact_historical_contract(world):
    identifier = start(world)
    saved = state(world, identifier)
    assert world[0].generator_profile == provider.LEGACY_GENERATOR_PROFILE == "legacy_v1"
    assert provider.RENDER_CONFIG_SHA256 == "45ec6161cfa049ac202c9c3012f76fd04fd0826371c3037258432ee29691b837"
    assert saved["render_config_sha256"] == provider.RENDER_CONFIG_SHA256
    assert saved["generator_config"] == provider.RENDER_CONFIG
    assert saved["generator_profile"] == "legacy_v1"
    assert saved["tts_reference_key"] == provider.REFERENCE_KEY
    assert saved["tts_reference_sha256"] == saved["reference_sha256"] == provider.REFERENCE_SHA256
    assert paid_calls(world)[0].kwargs["json"]["audio_url"] == world[1].objects.signed_get_url(provider.REFERENCE_KEY)


def test_clean_profile_changes_only_conditioning_not_master_or_acoustic_baseline(world, clean_reference):
    service = profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE)
    world[1].write("jobs/job", {"script_version": core.SCRIPT_VERSION, "status": "submitting", "first_name": "Sue"}, 0)
    identifier = service.submit("Sue", "job")
    saved = state(world, identifier)
    profile = provider._generator_profile(provider.CLEAN_BODY_GENERATOR_PROFILE)
    assert saved["generator_profile"] == "clean_body_v1"
    assert saved["generator_config"] == profile["config"]
    assert saved["render_config_sha256"] == profile["render_config_sha256"] != provider.RENDER_CONFIG_SHA256
    assert saved["tts_reference_key"] == provider.CLEAN_BODY_REFERENCE_KEY
    assert saved["tts_reference_sha256"] == hashlib.sha256(clean_reference).hexdigest()
    assert saved["reference_sha256"] == provider.REFERENCE_SHA256
    assert saved["source_sha256"] == provider.MASTER_SHA256
    assert profile["config"] == {**provider.RENDER_CONFIG, "generator_profile": "clean_body_v1",
        "reference_sha256": provider.CLEAN_BODY_REFERENCE_SHA256,
        "acoustic_reference_sha256": provider.REFERENCE_SHA256}
    assert paid_calls(world)[0].kwargs["json"] == {"text": "Hey Sue!", "seed": 4411,
        "temperature": 0.5, "audio_url": world[1].objects.signed_get_url(provider.CLEAN_BODY_REFERENCE_KEY)}
    assert service.inspect(identifier)["status"] == "processing"
    provider.inspect_greeting.assert_called_once_with(b"WAV audio", REFERENCE)
    world[3].assert_called_once_with(MASTER, b"WAV audio", first_name="Sue")
    result = service.prepare_media(service.inspect(identifier), "Sue")
    service.compositor.assert_called_once_with(MASTER, b"WAV audio", INPUTS["greeting_audio_bytes"],
        prepared("Sue")["metadata"], first_name="Sue")
    assert speech.validate_speech_proof(result["speech_proof"], first_name="Sue",
        media_sha256=hashlib.sha256(b"output").hexdigest())
    assert len(paid_calls(world)) == 2


@pytest.mark.parametrize("stage", ["narration_submitting", "narration_pending", "fal_submitting",
    "fal_pending", "failed", "uncertain", "cancelled", "completed"])
def test_new_profile_reuses_every_old_job_before_assets_preflight_or_network(world, monkeypatch, stage):
    identifier = start(world)
    remove_profile_fields(world, identifier)
    world[1].mutate("same-take/" + identifier, lambda old: {**old, "stage": stage})
    original = state(world, identifier)
    service = profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE)
    for name in ("_master", "_reference", "_tts_reference"):
        setattr(service, name, Mock(side_effect=AssertionError("Existing request must not fetch assets")))
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(side_effect=AssertionError("No new speech preflight")))
    world[2].request.reset_mock()
    assert service.submit("Sue", "job") == identifier
    assert state(world, identifier) == original
    world[2].request.assert_not_called()
    provider.preflight_speech_guard.assert_not_called()


def test_pending_legacy_request_resumes_without_clean_reference_and_preserves_identity(world):
    identifier = start(world)
    remove_profile_fields(world, identifier)
    service = profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE)
    assert service.submit("Sue", "job") == identifier
    assert service.inspect(identifier)["status"] == "processing"
    assert service.inspect(identifier)["status"] == "completed"
    saved = state(world, identifier)
    assert not set(PROFILE_FIELDS).intersection(saved)
    assert saved["render_config_sha256"] == provider.RENDER_CONFIG_SHA256
    provider.inspect_greeting.assert_called_once_with(b"WAV audio", REFERENCE)
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1


@pytest.mark.parametrize("stage", ["narration_pending", "fal_pending", "completed"])
def test_legacy_accounting_read_never_spends_under_new_profile(world, stage):
    identifier = start(world)
    if stage != "narration_pending":
        world[0].inspect(identifier)
    if stage == "completed":
        world[0].inspect(identifier)
    remove_profile_fields(world, identifier)
    before = len(paid_calls(world))
    service = profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE)
    result = service.inspect(identifier, accounting_only=True)
    assert result["status"] == ("failed" if stage == "narration_pending" else "completed")
    assert len(paid_calls(world)) == before
    assert not set(PROFILE_FIELDS).intersection(state(world, identifier))


@pytest.mark.parametrize("field", PROFILE_FIELDS)
def test_partial_explicit_profile_is_rejected_before_network(world, field):
    identifier = start(world)
    world[1].mutate("same-take/" + identifier, lambda old: {key: value for key, value in old.items() if key != field})
    world[2].request.reset_mock()
    with pytest.raises(provider.VideoProviderError):
        world[0].submit("Sue", "job")
    with pytest.raises(provider.VideoProviderError):
        world[0].inspect(identifier)
    world[2].request.assert_not_called()


@pytest.mark.parametrize("field", PROFILE_FIELDS[1:])
def test_orphan_profile_field_cannot_be_treated_as_legacy(world, field):
    identifier = start(world)
    original = state(world, identifier)[field]
    remove_profile_fields(world, identifier)
    world[1].mutate("same-take/" + identifier, lambda old: {**old, field: original})
    world[2].request.reset_mock()
    with pytest.raises(provider.VideoProviderError):
        world[0].submit("Sue", "job")
    world[2].request.assert_not_called()


@pytest.mark.parametrize("profile_name", ["legacy_v1", "clean_body_v1"])
@pytest.mark.parametrize("field,value", [("generator_profile", "unapproved_v2"),
    ("generator_config", {"seed": 123}), ("generator_config", None),
    ("tts_reference_key", "assets/another-reference.wav"), ("tts_reference_sha256", "0" * 64),
    ("render_config_sha256", "0" * 64)])
def test_saved_generator_identity_tampering_blocks_even_read_only_polling(world, clean_reference, profile_name, field, value):
    service = profile_service(world, profile_name)
    world[1].write("jobs/job", {"script_version": core.SCRIPT_VERSION, "status": "submitting", "first_name": "Sue"}, 0)
    identifier = service.submit("Sue", "job")
    world[1].mutate("same-take/" + identifier, lambda old: {**old, field: value})
    world[2].request.reset_mock()
    with pytest.raises(provider.VideoProviderError):
        service.inspect(identifier, accounting_only=True)
    with pytest.raises(provider.VideoProviderError):
        service.submit("Sue", "job")
    world[2].request.assert_not_called()


def test_old_no_field_profile_requires_exact_historical_render_hash(world):
    identifier = start(world)
    remove_profile_fields(world, identifier)
    world[1].mutate("same-take/" + identifier, lambda old: {**old, "render_config_sha256": "0" * 64})
    world[2].request.reset_mock()
    with pytest.raises(provider.VideoProviderError):
        profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE).submit("Sue", "job")
    world[2].request.assert_not_called()


@pytest.mark.parametrize("profile_name", [None, "", "CLEAN_BODY_V1", "clean_body_v2", "legacy"])
def test_unknown_profile_cannot_create_provider(world, profile_name):
    with pytest.raises(provider.VideoProviderError) as error:
        profile_service(world, profile_name)
    assert error.value.code == "generator_profile_invalid"
    world[2].request.assert_not_called()


@pytest.mark.parametrize("content", [None, b"", b"another reference"])
def test_new_clean_profile_requires_exact_reference_before_price_or_payment(world, content):
    if content is not None:
        world[1].objects.write(provider.CLEAN_BODY_REFERENCE_KEY, content, 0)
    service = profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE)
    world[1].write("jobs/job", {"script_version": core.SCRIPT_VERSION, "status": "submitting", "first_name": "Sue"}, 0)
    with pytest.raises(provider.VideoProviderError) as error:
        service.submit("Sue", "job")
    assert error.value.code == "reference_mismatch"
    world[2].request.assert_not_called()
    provider.preflight_speech_guard.assert_not_called()
    assert world[1].objects.list("same-take/") == []


@pytest.mark.parametrize("key", [MASTER_KEY, provider.REFERENCE_KEY])
def test_clean_conditioning_still_requires_original_master_and_acoustic_reference(world, clean_reference, key):
    _, version = world[1].objects.read(key)
    world[1].objects.write(key, b"changed original recording", version)
    service = profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE)
    world[1].write("jobs/job", {"script_version": core.SCRIPT_VERSION, "status": "submitting", "first_name": "Sue"}, 0)
    with pytest.raises(provider.VideoProviderError):
        service.submit("Sue", "job")
    world[2].request.assert_not_called()


def test_completed_old_profile_can_prepare_media_with_new_default_without_regeneration(world):
    identifier = start(world)
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    remove_profile_fields(world, identifier)
    world[2].request.reset_mock()
    service = profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE)
    result = service.prepare_media(snapshot, "Sue")
    assert speech.validate_speech_proof(result["speech_proof"], first_name="Sue",
        media_sha256=hashlib.sha256(b"output").hexdigest())
    assert not set(PROFILE_FIELDS).intersection(state(world, identifier))
    world[2].request.assert_not_called()


@pytest.mark.parametrize("field", ["id", "job_key", "first_name", "source_sha256", "script_version",
    "reference_sha256", "render_config_sha256", "reserved_cents", "greeting_script", *PROFILE_FIELDS])
def test_update_cannot_rewrite_even_an_unchanged_identity_field(world, field):
    identifier = start(world)
    original = state(world, identifier)
    world[2].request.reset_mock()
    with pytest.raises(provider.VideoProviderError):
        world[0]._update(identifier, {field: original[field]}, "narration_pending")
    assert state(world, identifier) == original
    world[2].request.assert_not_called()


def test_competing_generator_choices_claim_one_paid_pipeline(world, clean_reference, monkeypatch):
    store = world[1]
    services = [world[0], profile_service(world, provider.CLEAN_BODY_GENERATOR_PROFILE)]
    store.write("jobs/job", {"script_version": core.SCRIPT_VERSION, "status": "submitting", "first_name": "Sue"}, 0)
    write = store.write
    barrier = Barrier(2)
    def competing_write(key, value, version):
        if key.startswith("same-take/") and version == 0:
            barrier.wait(timeout=5)
        return write(key, value, version)
    monkeypatch.setattr(store, "write", competing_write)
    def submit(service):
        try:
            return service.submit("Sue", "job")
        except core.Conflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(submit, services))
    assert results.count("conflict") == 1
    identifier = next(value for value in results if value != "conflict")
    saved = state(world, identifier)
    assert len(paid_calls(world)) == 1
    assert paid_calls(world)[0].kwargs["json"]["audio_url"] == store.objects.signed_get_url(saved["tts_reference_key"])
    for service in services:
        assert service.submit("Sue", "job") == identifier
    assert services[1].inspect(identifier)["status"] == "processing"
    assert services[0].inspect(identifier)["status"] == "completed"
    assert state(world, identifier)["generator_profile"] == saved["generator_profile"]
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1


def test_identity_swap_between_read_and_mutate_blocks_first_paid_request(world, clean_reference, monkeypatch):
    store = world[1]
    mutate = store.mutate
    swapped = False
    def swap_then_update(key, update):
        nonlocal swapped
        if key.startswith("same-take/") and not swapped:
            swapped = True
            profile = provider._generator_profile(provider.CLEAN_BODY_GENERATOR_PROFILE)
            mutate(key, lambda old: {**old, "generator_profile": profile["name"],
                "generator_config": profile["config"], "render_config_sha256": profile["render_config_sha256"],
                "tts_reference_key": profile["reference_key"], "tts_reference_sha256": profile["reference_sha256"]})
        return mutate(key, update)
    monkeypatch.setattr(store, "mutate", swap_then_update)
    with pytest.raises(provider.VideoProviderError) as error:
        start(world)
    assert error.value.code == "pipeline_mismatch"
    assert swapped
    assert not paid_calls(world)
    pipeline_id = world[0].submit("Sue", "job")
    assert state(world, pipeline_id)["stage"] != "narration_pending"
    assert "narration_id" not in state(world, pipeline_id)


def test_cas_retry_rechecks_immutable_profile_before_stage_advance(world, clean_reference, monkeypatch):
    identifier = start(world)
    world[2].request.reset_mock()
    store = world[1]
    write = store.write
    swapped = False
    def swap_during_write(key, value, version):
        nonlocal swapped
        if key == "same-take/" + identifier and not swapped:
            swapped = True
            old, latest_version = store.read(key)
            profile = provider._generator_profile(provider.CLEAN_BODY_GENERATOR_PROFILE)
            write(key, {**old, "generator_profile": profile["name"],
                "generator_config": profile["config"], "render_config_sha256": profile["render_config_sha256"],
                "tts_reference_key": profile["reference_key"], "tts_reference_sha256": profile["reference_sha256"]}, latest_version)
            raise core.Conflict()
        return write(key, value, version)
    monkeypatch.setattr(store, "write", swap_during_write)
    with pytest.raises(provider.VideoProviderError) as error:
        world[0]._update(identifier, {"stage": "fal_submitting"}, "narration_pending")
    assert error.value.code == "pipeline_mismatch"
    assert swapped
    assert state(world, identifier)["stage"] == "narration_pending"
    world[2].request.assert_not_called()


@pytest.mark.parametrize("profile_name", ["legacy_v1", "clean_body_v1"])
def test_both_paid_stages_disable_provider_fallback_and_retries(world, clean_reference, profile_name):
    service = profile_service(world, profile_name)
    world[1].write("jobs/job", {"script_version": core.SCRIPT_VERSION, "status": "submitting", "first_name": "Sue"}, 0)
    identifier = service.submit("Sue", "job")
    service.inspect(identifier)
    assert len(paid_calls(world)) == 2
    for call in paid_calls(world):
        assert call.kwargs["headers"]["x-app-fal-disable-fallback"] == "true"
        assert call.kwargs["headers"]["X-Fal-No-Retry"] == "1"
        assert call.kwargs["allow_redirects"] is False


@pytest.fixture
def planned(world, monkeypatch):
    import video_followup_audio_lineage as lineage
    def plan(master, greeting, metadata, *, first_name):
        manifest = {"lineage_version": lineage.LINEAGE_VERSION, "extraction_version": lineage.EXTRACTION_VERSION,
            "first_name": first_name, "source_sha256": provider.MASTER_SHA256,
            "preparation_sha256": provider._digest_json(metadata),
            "prepared_greeting_sha256": hashlib.sha256(greeting).hexdigest(),
            "greeting_seconds": metadata["greeting_seconds"],
            "planned_audio_sha256": hashlib.sha256(b"frozen AAC").hexdigest(),
            "checked_audio_sha256": hashlib.sha256(b"decoded AAC greeting").hexdigest(),
            "body_pcm_sha256": "1" * 64, "joined_pcm_sha256": "2" * 64,
            "full_pcm_sha256": "3" * 64, "greeting_pcm_sha256": "4" * 64,
            "joined_sample_count": 400000, "full_sample_count": 400000,
            "greeting_sample_count": 24000, "aac_track": {"codec": "aac-lc"}}
        return {"audio_bytes": b"frozen AAC", "greeting_audio_bytes": b"decoded AAC greeting",
            "manifest": manifest, "manifest_sha256": provider._digest_json(manifest)}
    def verify(media, audio, manifest, proof, metadata, *, first_name, expected_manifest_sha256):
        assert audio == b"frozen AAC"
        assert provider._digest_json(manifest) == expected_manifest_sha256
        assert speech.validate_speech_proof(proof, first_name=first_name, audio_sha256=manifest["checked_audio_sha256"])
        return {"accepted": True, "lineage_version": lineage.LINEAGE_VERSION, "first_name": first_name,
            "manifest_sha256": expected_manifest_sha256, "planned_audio_sha256": manifest["planned_audio_sha256"],
            "final_media_sha256": hashlib.sha256(media).hexdigest(),
            "checked_audio_sha256": manifest["checked_audio_sha256"], "full_pcm_sha256": "1" * 64,
            "greeting_pcm_sha256": "2" * 64, "aac_track_sha256": "3" * 64,
            "speech_proof_sha256": provider._digest_json(proof)}
    prepare_mock, verify_mock = Mock(side_effect=plan), Mock(side_effect=verify)
    monkeypatch.setattr(lineage, "prepare_audio_plan", prepare_mock)
    monkeypatch.setattr(lineage, "verify_final_audio", verify_mock)
    world[0].verification_method = provider.PLANNED_AUDIO_VERIFICATION_METHOD
    return prepare_mock, verify_mock


def test_planned_method_default_is_legacy_and_unknown_method_rejected(world):
    assert world[0].verification_method == provider.LEGACY_VERIFICATION_METHOD
    with pytest.raises(provider.VideoProviderError, match="not approved"):
        provider.SameTakeVideoProvider("key", world[1], MASTER_KEY, tts_key="key", verification_method="unknown")


def test_planned_audio_is_checked_once_before_kling_and_bound_after_lineage(world, planned):
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "processing"
    provider.verify_greeting_words.assert_called_once_with(b"decoded AAC greeting", "Sue")
    snapshot = world[0].inspect(identifier)
    assert snapshot["verification_method"] == provider.PLANNED_AUDIO_VERIFICATION_METHOD
    assert snapshot["verification_config_sha256"] == provider.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256
    result = world[0].prepare_media(snapshot, "Sue")
    assert world[0].compositor.call_args.kwargs["planned_audio_bytes"] == b"frozen AAC"
    assert planned[1].call_count == 1
    provider.verify_video_greeting_words.assert_not_called()
    assert provider.verify_greeting_words.call_count == 1
    assert speech.validate_speech_proof(result["speech_proof"], first_name="Sue",
        media_sha256=hashlib.sha256(b"output").hexdigest())
    audit = world[0].audit_media({"provider_id": identifier, "first_name": "Sue", "followup_key": "job"}, b"output")
    assert audit == result["speech_proof"]
    assert planned[1].call_count == 2
    assert provider.verify_greeting_words.call_count == 1
    assert len(paid_calls(world)) == 2
    assert state(world, identifier)["estimated_cents"] == 9
    assert "raw_speech_proof" not in state(world, identifier)


def test_planned_busy_retry_reuses_frozen_audio_and_never_repays_narration(world, planned, monkeypatch):
    identifier = start(world)
    original = provider.verify_greeting_words.side_effect
    provider.verify_greeting_words.side_effect = speech.SpeechGuardError("speech_guard_busy")
    assert world[0].inspect(identifier)["status"] == "processing"
    saved = state(world, identifier)
    assert saved["audio_plan_sha256"]
    assert saved["stage"] == "narration_pending"
    assert len(paid_calls(world)) == 1
    downloads, preparation = world[0]._download_fal.call_count, world[3].call_count
    now = core.utcnow() + timedelta(seconds=61)
    monkeypatch.setattr(provider, "utcnow", lambda: now)
    provider.verify_greeting_words.side_effect = original
    assert world[0].inspect(identifier)["status"] == "processing"
    assert planned[0].call_count == 1
    assert world[0]._download_fal.call_count == downloads
    assert world[3].call_count == preparation
    assert state(world, identifier)["audio_plan_sha256"] == saved["audio_plan_sha256"]
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1


@pytest.mark.parametrize("code", ["speech_guard_name_mismatch", "speech_guard_low_confidence", "speech_guard_output_invalid"])
def test_planned_wrong_greeting_is_terminal_before_lipsync(world, planned, code):
    identifier = start(world)
    provider.verify_greeting_words.side_effect = speech.SpeechGuardError(code)
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["underlying_code"] == code
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("filename", ["prepared-plan.json", "planned.m4a", "checked-greeting.wav", "greeting.wav", "input.mp4", "input.wav"])
def test_planned_asset_tamper_blocks_final_publication(world, planned, filename):
    identifier = start(world)
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    key = f"assets/{identifier}/{filename}"
    _, version = world[1].objects.read(key)
    world[1].objects.write(key, b"changed", version)
    with pytest.raises(speech.SpeechGuardError):
        world[0].prepare_media(snapshot, "Sue")
    world[0].compositor.assert_not_called()
    assert "final_speech_proof" not in state(world, identifier)
    assert len(paid_calls(world)) == 2


def test_planned_lineage_rejection_never_binds_final_name_proof(world, planned):
    identifier = start(world)
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    planned[1].side_effect = provider.VideoProviderError("audio_lineage_decoded_mismatch", "not the checked audio")
    with pytest.raises(speech.SpeechGuardError) as error:
        world[0].prepare_media(snapshot, "Sue")
    assert error.value.code == "speech_guard_media_invalid"
    assert "final_speech_proof" not in state(world, identifier)
    provider.verify_video_greeting_words.assert_not_called()


@pytest.mark.parametrize("field", ["verification_method", "verification_config", "verification_config_sha256"])
def test_partial_verification_identity_fails_closed(world, planned, field):
    identifier = start(world)
    key = "same-take/" + identifier
    old, version = world[1].read(key)
    old.pop(field)
    world[1].write(key, old, version)
    with pytest.raises(provider.VideoProviderError):
        world[0].inspect(identifier)
    assert len(paid_calls(world)) == 1


@pytest.mark.parametrize("field", ["verification_method", "verification_config", "verification_config_sha256", "audio_plan_sha256"])
def test_verification_identity_cannot_be_updated(world, planned, field):
    identifier = start(world)
    with pytest.raises(provider.VideoProviderError):
        world[0]._update(identifier, {field: "changed"})
    assert len(paid_calls(world)) == 1


def test_old_pipeline_keeps_original_path_after_constructor_switch(world, planned):
    world[0].verification_method = provider.LEGACY_VERIFICATION_METHOD
    identifier = start(world)
    key = "same-take/" + identifier
    old, version = world[1].read(key)
    for field in ("verification_method", "verification_config", "verification_config_sha256"):
        old.pop(field)
    world[1].write(key, old, version)
    world[0].verification_method = provider.PLANNED_AUDIO_VERIFICATION_METHOD
    assert world[0].submit("Sue", "job") == identifier
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    assert "verification_method" not in snapshot
    world[0].prepare_media(snapshot, "Sue")
    assert provider.verify_greeting_words.call_count == 2
    provider.verify_video_greeting_words.assert_called_once()
    planned[0].assert_not_called()
    planned[1].assert_not_called()
    assert "planned_audio_bytes" not in world[0].compositor.call_args.kwargs


def test_existing_planned_pipeline_remains_planned_if_factory_returns_to_legacy(world, planned):
    identifier = start(world)
    world[0].verification_method = provider.LEGACY_VERIFICATION_METHOD
    assert world[0].submit("Sue", "job") == identifier
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    assert snapshot["verification_method"] == provider.PLANNED_AUDIO_VERIFICATION_METHOD
    world[0].prepare_media(snapshot, "Sue")
    assert planned[0].call_count == 1
    assert planned[1].call_count == 1
    provider.verify_greeting_words.assert_called_once()
    provider.verify_video_greeting_words.assert_not_called()
    assert len(paid_calls(world)) == 2


def test_unknown_lineage_runtime_blocks_new_payment(world, planned, monkeypatch):
    import video_followup_audio_lineage as lineage
    monkeypatch.setattr(lineage, "LINEAGE_VERSION", "not-pinned")
    with pytest.raises(speech.SpeechGuardError):
        start(world)
    assert not paid_calls(world)


def test_planned_audio_preparation_failure_keeps_sanitized_diagnostic(world, planned):
    identifier = start(world)
    planned[0].side_effect = provider.VideoProviderError("audio_lineage_source_short", "private URL must not persist")
    status = world[0].inspect(identifier)
    assert status["status"] == "failed"
    assert status["failure_stage"] == "audio_plan"
    assert status["underlying_code"] == "audio_lineage_source_short"
    assert "private URL" not in json.dumps(state(world, identifier))
    assert not paid_calls(world, provider.MODEL)


def test_planned_retry_still_cancels_if_job_became_inactive(world, planned, monkeypatch):
    identifier = start(world)
    provider.verify_greeting_words.side_effect = speech.SpeechGuardError("speech_guard_busy")
    world[0].inspect(identifier)
    world[1].mutate("jobs/job", lambda old: {**old, "status": "cancelled"})
    monkeypatch.setattr(provider, "utcnow", lambda: core.utcnow() + timedelta(seconds=61))
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["stage"] == "cancelled"
    assert planned[0].call_count == 1
    assert not paid_calls(world, provider.MODEL)


def test_planned_corrupted_bundle_during_backoff_does_not_get_regenerated(world, planned, monkeypatch):
    identifier = start(world)
    provider.verify_greeting_words.side_effect = speech.SpeechGuardError("speech_guard_busy")
    world[0].inspect(identifier)
    key = f"assets/{identifier}/prepared-plan.json"
    _, version = world[1].objects.read(key)
    world[1].objects.write(key, b"changed", version)
    monkeypatch.setattr(provider, "utcnow", lambda: core.utcnow() + timedelta(seconds=61))
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["underlying_code"] == "speech_guard_audio_invalid"
    assert planned[0].call_count == 1
    assert not paid_calls(world, provider.MODEL)
