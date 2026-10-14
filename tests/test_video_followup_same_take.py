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
    def prepare(*args, first_name):
        barrier.wait(timeout=5)
        return prepared(first_name)
    world[3].side_effect = prepare
    def poll(_):
        try:
            return world[0].inspect(identifier)
        except core.Conflict:
            return "conflict"
    with ThreadPoolExecutor(max_workers=2) as executor:
        result = list(executor.map(poll, range(2)))
    assert "conflict" in result
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
    if key == MASTER_KEY:
        with pytest.raises(provider.VideoProviderError):
            world[0].inspect(identifier)
    else:
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
    with pytest.raises(speech.SpeechGuardError):
        start(world)
    assert not paid_calls(world)
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(return_value={"ready": True}))
    identifier = world[0].submit("Sue", "job")
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(side_effect=
        speech.SpeechGuardError("speech_guard_unavailable")))
    assert world[0].submit("Sue", "job") == identifier
    assert len(paid_calls(world)) == 1


@pytest.mark.parametrize("code", ["speech_guard_name_mismatch", "speech_guard_low_confidence", "speech_guard_timeout"])
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
