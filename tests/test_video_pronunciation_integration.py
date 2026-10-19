"""Offline policy rollout integration; no model, provider, or transport calls."""
import hashlib
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import video_followup as core
import video_followup_same_take as provider
import video_followup_speech_guard as speech
import video_followup_worker as bootstrap
from test_video_followup import initial_receipt
from test_video_followup_same_take import (
    Objects, paid_calls, planned, speech_proof, start, state, world,
)


OLD = speech.PLANNED_AUDIO_VERIFICATION_METHOD
NEW = speech.PRONUNCIATION_VERIFICATION_METHOD
NOW = datetime(2026, 10, 17, 16, tzinfo=timezone.utc)
MEDIA = {"media_bytes": b"final video", "poster_bytes": b"poster",
         "vtt_bytes": b"WEBVTT", "duration": 8.6}


@pytest.fixture(autouse=True)
def no_external_calls(monkeypatch):
    for path in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
        monkeypatch.setattr(path, Mock(side_effect=AssertionError("offline integration only")))


@pytest.fixture
def recognition(world, planned, monkeypatch):
    observed = {"text": "Hey Sue!", "logprob": -0.1}
    monkeypatch.setattr(speech, "_audio_duration", lambda content: 1.5)
    monkeypatch.setattr(speech, "_speech_paths", lambda: {"python": "unused", "model_dir": "unused"})
    monkeypatch.setattr(speech, "_recognize", Mock(side_effect=lambda *args: {
        "transcript": observed["text"], "model_revision": speech.MODEL_REVISION,
        "config_sha256": speech.CONFIG_SHA256,
        "segments": [{"avg_logprob": observed["logprob"], "no_speech_prob": 0.01}]}))
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(wraps=speech.verify_greeting_words))
    return observed


def binding(method):
    profile = provider._verification_profile(method)
    return {key: profile[key] for key in ("verification_method", "verification_config_sha256")}


def final_proof(method, name="Sue", transcript=None):
    config = speech.PLANNED_VERIFICATION_PROFILES[method]
    original = speech_proof(name, b"checked greeting")
    if method == NEW:
        original.update(policy_version=speech.PRONUNCIATION_POLICY_VERSION,
                        config_sha256=speech.PRONUNCIATION_CONFIG_SHA256)
    if transcript is not None:
        original["normalized_transcript"] = transcript
    media_hash = hashlib.sha256(MEDIA["media_bytes"]).hexdigest()
    lineage = {"accepted": True, "lineage_version": config["lineage_version"],
        "first_name": name, "final_media_sha256": media_hash,
        "checked_audio_sha256": original["source_audio_sha256"], "greeting_seconds": 1.5,
        "speech_proof_sha256": provider._digest_json(original),
        **{key: hashlib.sha256(key.encode()).hexdigest() for key in (
            "manifest_sha256", "planned_audio_sha256", "full_pcm_sha256", "greeting_pcm_sha256", "aac_track_sha256")}}
    return {**original, **binding(method), "media_sha256": media_hash,
            "greeting_seconds": 1.5, "audio_lineage": lineage}


def submitted_job(method, *, name="Sue"):
    store = core.Store(Objects())
    context = core.snapshot(row=12, stable_id="offline-policy-integration", phone="2125550100",
                            first_name=name, initial_sent_at="2026-10-17T10:00:00Z")
    context.update(core.initial_receipt_binding(context, initial_receipt(context)))
    config = core.Config({"VIDEO_FOLLOWUP_ENABLED": "true", "VIDEO_FOLLOWUP_GENERATION_ENABLED": "true",
        "VIDEO_FOLLOWUP_TEST_PHONES": "2125550100", "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T00:00:00Z",
        "VIDEO_FOLLOWUP_PUBLIC_ORIGIN": "https://go.crispshortsales.com"})
    store.prepare(context)
    key = context["followup_key"]
    assert store.reserve(key, config, NOW)
    store.mutate("jobs/" + key, lambda old: {**old, "status": "submitted", "provider_id": "paid-existing",
        "reserved_at": core.iso(NOW), **binding(method)})
    return store, config, key


@pytest.mark.parametrize("method", [OLD, NEW])
def test_registered_bindings_require_exact_matching_configuration(method):
    profile = provider._verification_profile(method)
    assert profile["verification_config"] == speech.PLANNED_VERIFICATION_PROFILES[method]
    assert provider._saved_verification_profile(profile) == profile
    assert core._verification_binding(profile) == binding(method)
    other = NEW if method == OLD else OLD
    with pytest.raises(provider.VideoProviderError):
        provider._saved_verification_profile({**profile, "verification_config_sha256": binding(other)["verification_config_sha256"]})
    with pytest.raises(speech.SpeechGuardError):
        core._verification_binding({**binding(method), "verification_config_sha256": binding(other)["verification_config_sha256"]})


@pytest.mark.parametrize("name,observed", [("CYDNEY", "Hey Sydney!"), ("Brook", "Hey Brooke!")])
def test_future_pipeline_accepts_only_registered_pronunciation_before_one_lipsync(world, recognition, name, observed):
    world[0].verification_method = NEW
    recognition["text"] = observed
    identifier = start(world, name)
    assert world[0].inspect(identifier)["status"] == "processing"
    saved = state(world, identifier)
    assert saved["verification_method"] == NEW
    assert saved["greeting_speech_proof"]["policy_version"] == speech.PRONUNCIATION_POLICY_VERSION
    assert saved["greeting_speech_proof"]["expected_name"] == name
    provider.verify_greeting_words.assert_called_once_with(b"decoded AAC greeting", name,
                                                         policy_version=speech.PRONUNCIATION_POLICY_VERSION)
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1
    assert saved["estimated_cents"] == 9


@pytest.mark.parametrize("observed,logprob,code", [
    ("Hey Cindy!", -0.1, "speech_guard_name_mismatch"),
    ("Hey Sydney and Cindy!", -0.1, "speech_guard_name_mismatch"),
    ("Hey Sydney!", -1.01, "speech_guard_low_confidence"),
])
def test_new_pronunciation_does_not_accept_wrong_name_extra_content_or_low_confidence(world, recognition, observed, logprob, code):
    world[0].verification_method = NEW
    recognition.update(text=observed, logprob=logprob)
    identifier = start(world, "CYDNEY")
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["underlying_code"] == code
    assert not paid_calls(world, provider.MODEL)


def test_existing_old_pipeline_does_not_gain_pronunciation_equivalence_after_switch(world, recognition):
    identifier = start(world, "CYDNEY")
    world[0].verification_method = NEW
    recognition["text"] = "Hey Sydney!"
    assert world[0].submit("CYDNEY", "job") == identifier
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["verification_method"] == OLD
    provider.verify_greeting_words.assert_called_once_with(b"decoded AAC greeting", "CYDNEY")
    assert not paid_calls(world, provider.MODEL)
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1


@pytest.mark.parametrize("saved_method,current_method", [(OLD, NEW), (NEW, OLD)])
def test_saved_method_bundle_and_proof_remain_immutable_across_factory_switch(world, recognition, saved_method, current_method):
    world[0].verification_method = saved_method
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "processing"
    before = state(world, identifier)
    bundle_key = f"assets/{identifier}/prepared-plan.json"
    bundle_before = world[1].objects.read(bundle_key)
    world[0].verification_method = current_method
    assert world[0].submit("Sue", "job") == identifier
    completed = world[0].inspect(identifier)
    result = world[0].prepare_media(completed, "Sue")
    assert completed["verification_method"] == saved_method
    assert result["speech_proof"]["verification_method"] == saved_method
    assert state(world, identifier)["greeting_speech_proof"] == before["greeting_speech_proof"]
    assert state(world, identifier)["audio_plan_sha256"] == before["audio_plan_sha256"]
    assert world[1].objects.read(bundle_key) == bundle_before
    assert provider.verify_greeting_words.call_count == 1
    assert len(paid_calls(world)) == 2


def test_new_pipeline_rejects_valid_old_name_proof_before_lipsync(world, recognition, monkeypatch):
    world[0].verification_method = NEW
    old_proof = speech_proof("Sue", b"decoded AAC greeting")
    assert speech.validate_speech_proof(old_proof, first_name="Sue")
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(return_value=old_proof))
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["underlying_code"] == "speech_guard_output_invalid"
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("field", ["verification_method", "verification_config", "verification_config_sha256"])
def test_existing_verification_identity_cannot_be_rewritten(world, recognition, field):
    identifier = start(world)
    with pytest.raises(provider.VideoProviderError):
        world[0]._update(identifier, {field: provider._verification_profile(NEW)[field]})
    assert state(world, identifier)["verification_method"] == OLD
    assert len(paid_calls(world)) == 1


@pytest.mark.parametrize("method", [OLD, NEW])
def test_worker_binds_registered_method_before_publication(method, monkeypatch):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    name = "CYDNEY" if method == NEW else "Sue"
    store, config, key = submitted_job(method, name=name)
    engine = Mock()
    engine.inspect.return_value = {"status": "completed", "provider_id": "paid-existing", **binding(method)}
    def media(status, name):
        job, _ = store.read("jobs/" + key)
        assert job["status"] == "processing"
        assert core._verification_binding(job) == binding(method)
        return {**MEDIA, "speech_proof": final_proof(method, name, "hey sydney" if method == NEW else None)}
    engine.prepare_media.side_effect = media
    core.Worker(store, engine, config, lambda job: True, now=lambda: NOW).tick()
    job, _ = store.read("jobs/" + key)
    assert job["status"] == "ready"
    assert store.publication_ready(job)
    assert job["first_name"] == name
    engine.submit.assert_not_called()


@pytest.mark.parametrize("method,other", [(OLD, NEW), (NEW, OLD)])
def test_store_rejects_cross_policy_proof_without_publishing_assets(method, other):
    store, _, key = submitted_job(method)
    proof = final_proof(other)
    assert speech.validate_speech_proof(proof, first_name="Sue", media_sha256=proof["media_sha256"])
    with pytest.raises(speech.SpeechGuardError):
        store.finish(key, "paid-existing", {**MEDIA, "speech_proof": proof})
    assert store.objects.list("assets/") == []
    assert store.objects.list("public/") == []


@pytest.mark.parametrize("method,other", [(OLD, NEW), (NEW, OLD)])
def test_changed_ready_proof_is_not_deliverable_or_silently_upgraded(method, other):
    store, config, key = submitted_job(method)
    assert store.finish(key, "paid-existing", {**MEDIA, "speech_proof": final_proof(method)})
    store.mutate("jobs/" + key, lambda old: {**old, "speech_proof": final_proof(other)})
    job, _ = store.read("jobs/" + key)
    assert not store.verified_video(job)
    assert not store.publication_ready(job)
    message, metadata = core.Service(config, store).select(job, "unchanged text fallback")
    assert message == "unchanged text fallback"
    assert metadata["video_variant"] == "text"
    frozen, _ = store.read("jobs/" + key)
    assert frozen["status"] == "frozen_text"
    assert core._verification_binding(frozen) == binding(method)


@pytest.mark.parametrize("method,other", [(OLD, NEW), (NEW, OLD)])
def test_worker_rejects_changed_completed_method_before_composition(method, other, monkeypatch):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    store, config, key = submitted_job(method)
    engine = Mock()
    engine.inspect.return_value = {"status": "completed", "provider_id": "paid-existing", **binding(other)}
    core.Worker(store, engine, config, lambda job: True, now=lambda: NOW).tick()
    job, _ = store.read("jobs/" + key)
    assert job["status"] == "failed"
    assert job["underlying_code"] == "speech_guard_output_invalid"
    engine.submit.assert_not_called()
    engine.prepare_media.assert_not_called()
    assert store.objects.list("assets/") == []


@pytest.mark.parametrize("status", ["failed", "cancelled", "frozen_text", "sent"])
def test_terminal_job_never_restarts_under_new_policy(status, monkeypatch):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    store, config, key = submitted_job(OLD)
    store.mutate("jobs/" + key, lambda old: {**old, "status": status, "budget_settled": True})
    before = store.read("jobs/" + key)[0]
    engine = Mock(verification_method=NEW)
    core.Worker(store, engine, config, lambda job: True, now=lambda: NOW).tick()
    assert store.read("jobs/" + key)[0] == before
    assert store.objects.list("assets/") == []
    assert store.objects.list("public/") == []
    engine.submit.assert_not_called()
    engine.inspect.assert_not_called()
    engine.prepare_media.assert_not_called()


@pytest.mark.parametrize("stage", ["failed", "cancelled", "uncertain"])
def test_terminal_pipeline_does_not_retry_when_provider_default_changes(world, recognition, stage):
    identifier = start(world)
    world[0]._update(identifier, {"stage": stage})
    before = state(world, identifier)
    world[0].verification_method = NEW
    assert world[0].submit("Sue", "job") == identifier
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier) == before
    assert len(paid_calls(world)) == 1
    provider.verify_greeting_words.assert_not_called()


def test_factory_selects_new_method_without_changing_generator_or_transport(monkeypatch):
    config = SimpleNamespace(enabled=True, generation_enabled=True, origin="https://video.example.invalid",
        bucket="private", not_before="2026-10-14", live=False, allowlist={"9542053205"})
    monkeypatch.setattr(bootstrap, "Config", lambda: config)
    values = {"FAL_API_KEY": "offline-kling", "FAL_TTS_API_KEY": "offline-tts",
        "VIDEO_FOLLOWUP_SAME_TAKE_MASTER_KEY": "assets/master/video", "TASKER_TRANSPORT_HEALTH_URL": "https://script.google.com/offline",
        "SMS_CHATBOT_ALLOWED_TOKEN": "offline-token", "VIDEO_FOLLOWUP_GENERATION_PROFILE": "clean_body_v1",
        "VIDEO_FOLLOWUP_AUDIO_VERIFICATION_METHOD": NEW}
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    store = object()
    monkeypatch.setattr(bootstrap, "get_service", Mock(return_value=SimpleNamespace(store=store)))
    constructor, worker = Mock(return_value=object()), Mock()
    monkeypatch.setattr(provider, "SameTakeVideoProvider", constructor)
    monkeypatch.setattr(bootstrap, "Worker", worker)
    assert bootstrap.create_worker() is worker.return_value
    constructor.assert_called_once_with("offline-kling", store, "assets/master/video", tts_key="offline-tts",
        generator_profile="clean_body_v1", verification_method=NEW)
    assert worker.call_args.args[2] is config
