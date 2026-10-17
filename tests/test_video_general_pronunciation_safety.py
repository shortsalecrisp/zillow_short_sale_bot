"""Offline rollout safety, not actual recognition, generation, or send evidence."""

import copy
import hashlib
from unittest.mock import Mock

import pytest

import video_followup as core
import video_followup_pronunciation as lexicon
import video_followup_same_take as provider
import video_followup_speech_guard as speech
import video_followup_worker as bootstrap
from test_video_followup_same_take import paid_calls, planned, speech_proof, world
from test_video_pronunciation_integration import MEDIA, NOW, binding, submitted_job


OLD_METHODS = (speech.PLANNED_AUDIO_VERIFICATION_METHOD, speech.PRONUNCIATION_VERIFICATION_METHOD)
NEW = speech.GENERAL_PRONUNCIATION_VERIFICATION_METHOD


@pytest.fixture(autouse=True)
def isolated_offline_run(monkeypatch):
    for path in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
        monkeypatch.setattr(path, Mock(side_effect=AssertionError("No network in rollout safety tests")))
    monkeypatch.setattr(speech, "_run_child", Mock(side_effect=AssertionError("No ASR in rollout safety tests")))
    lexicon._load_lexicon.cache_clear()
    yield
    lexicon._load_lexicon.cache_clear()


def final_proof(method, name="Marc", observed="mark"):
    profile = speech.PLANNED_VERIFICATION_PROFILES[method]
    original = speech_proof(name, b"offline checked greeting")
    original.update(policy_version=speech.PLANNED_VERIFICATION_POLICIES[method],
                    config_sha256=profile["speech_config_sha256"], normalized_transcript="hey " + observed)
    media_sha = hashlib.sha256(MEDIA["media_bytes"]).hexdigest()
    return {**original, **binding(method), "media_sha256": media_sha, "greeting_seconds": 1.5,
        "audio_lineage": {"accepted": True, "lineage_version": profile["lineage_version"],
            "first_name": name, "final_media_sha256": media_sha,
            "checked_audio_sha256": original["source_audio_sha256"], "greeting_seconds": 1.5,
            "speech_proof_sha256": provider._digest_json(original),
            **{field: hashlib.sha256(field.encode()).hexdigest() for field in (
                "manifest_sha256", "planned_audio_sha256", "full_pcm_sha256",
                "greeting_pcm_sha256", "aac_track_sha256")}}}


def damage_lexicon(monkeypatch, tmp_path, kind):
    path = tmp_path / "wrong-or-missing-lexicon.gz"
    if kind == "corrupt":
        content = bytearray(lexicon._PATH.read_bytes())
        content[len(content) // 2] ^= 1
        path.write_bytes(content)
    monkeypatch.setattr(lexicon, "_PATH", path)
    lexicon._load_lexicon.cache_clear()


@pytest.mark.parametrize("kind", ["missing", "corrupt"])
def test_actual_lexicon_integrity_failure_blocks_pipeline_creation_and_all_requests(world, planned, monkeypatch, tmp_path, kind):
    world[0].verification_method = NEW
    damage_lexicon(monkeypatch, tmp_path, kind)
    before = copy.deepcopy(world[1].objects.values)
    with pytest.raises(speech.SpeechGuardError, match="speech_guard_pronunciation_data_invalid"):
        world[0].submit("Marc", "new-private-offline-job")
    assert world[1].objects.values == before
    assert world[1].objects.list("same-take/") == []
    world[2].request.assert_not_called()
    provider.preflight_speech_guard.assert_not_called()
    assert not paid_calls(world)


@pytest.mark.parametrize("kind", ["missing", "corrupt"])
def test_worker_lexicon_failure_is_unpaid_terminal_and_not_reopened_after_data_recovers(
        world, planned, monkeypatch, tmp_path, kind):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    service, store, *_ = world
    service.verification_method = NEW
    context = core.snapshot(row=12, stable_id="offline-unpaid-lexicon", phone="2125550100",
        first_name="Marc", initial_sent_at="2026-10-17T10:00:00Z")
    config = core.Config({"VIDEO_FOLLOWUP_ENABLED": "true", "VIDEO_FOLLOWUP_GENERATION_ENABLED": "true",
        "VIDEO_FOLLOWUP_TEST_PHONES": "2125550100", "VIDEO_FOLLOWUP_INITIALS_AFTER": "2026-10-13T00:00:00Z"})
    store.prepare(context)
    original_path = lexicon._PATH
    damage_lexicon(monkeypatch, tmp_path, kind)
    worker = core.Worker(store, service, config, lambda job: True, now=lambda: NOW)
    worker.tick()
    failed, _ = store.read("jobs/" + context["followup_key"])
    assert failed["status"] == "failed"
    assert failed["preflight_attempts"] == 1
    assert not failed.get("provider_id")
    assert store.objects.list("same-take/") == []
    world[2].request.assert_not_called()
    monkeypatch.setattr(lexicon, "_PATH", original_path)
    lexicon._load_lexicon.cache_clear()
    assert speech.preflight_pronunciation_guard() == speech.GENERAL_PRONUNCIATION_LEXICON_MANIFEST
    assert store.prepare(context) == failed
    worker.tick()
    assert store.read("jobs/" + context["followup_key"])[0] == failed
    world[2].request.assert_not_called()
    assert store.objects.list("same-take/") == []


@pytest.mark.parametrize("saved_method", [*OLD_METHODS, NEW])
@pytest.mark.parametrize("status", ["failed", "uncertain", "cancelled", "budget_blocked", "frozen_text", "frozen_video", "sent"])
def test_terminal_or_frozen_worker_episode_never_reopens_when_new_policy_available(saved_method, status, monkeypatch):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    store, config, key = submitted_job(saved_method)
    store.mutate("jobs/" + key, lambda old: {**old, "status": status, "budget_settled": True})
    before = copy.deepcopy(store.read("jobs/" + key)[0])
    assert store.prepare(before) == before
    engine = Mock(verification_method=NEW)
    core.Worker(store, engine, config, lambda job: True, now=lambda: NOW).tick()
    assert store.read("jobs/" + key)[0] == before
    assert core._verification_binding(before) == binding(saved_method)
    assert store.objects.list("assets/") == []
    assert store.objects.list("public/") == []
    engine.submit.assert_not_called()
    engine.inspect.assert_not_called()
    engine.prepare_media.assert_not_called()


@pytest.mark.parametrize("saved_method,proof_method", [
    (OLD_METHODS[0], NEW), (OLD_METHODS[1], NEW), (NEW, OLD_METHODS[0]), (NEW, OLD_METHODS[1])])
def test_store_never_publishes_cross_version_proof_even_when_each_proof_is_valid(saved_method, proof_method):
    store, _, key = submitted_job(saved_method)
    proof = final_proof(proof_method, name="Sue", observed="sue")
    assert speech.validate_speech_proof(proof, first_name="Sue", media_sha256=proof["media_sha256"])
    before = copy.deepcopy(store.read("jobs/" + key)[0])
    with pytest.raises(speech.SpeechGuardError):
        store.finish(key, "paid-existing", {**MEDIA, "speech_proof": proof})
    assert store.read("jobs/" + key)[0] == before
    assert store.objects.list("assets/") == []
    assert store.objects.list("public/") == []


@pytest.mark.parametrize("field,value", [
    ("verification_method", speech.PRONUNCIATION_VERIFICATION_METHOD),
    ("verification_config_sha256", speech.PRONUNCIATION_VERIFICATION_CONFIG_SHA256),
    ("policy_version", speech.PRONUNCIATION_POLICY_VERSION),
    ("config_sha256", speech.PRONUNCIATION_CONFIG_SHA256),
    ("expected_name", "Mark"), ("normalized_transcript", "hey mick"),
])
def test_changed_v4_ready_proof_freezes_text_without_rebinding_the_job(field, value):
    store, config, key = submitted_job(NEW, name="Marc")
    assert store.finish(key, "paid-existing", {**MEDIA, "speech_proof": final_proof(NEW)})
    store.mutate("jobs/" + key, lambda old: {**old, "speech_proof": {**old["speech_proof"], field: value}})
    job = store.read("jobs/" + key)[0]
    assert not store.verified_video(job)
    assert not store.publication_ready(job)
    message, metadata = core.Service(config, store).select(job, "same normal fallback")
    assert message == "same normal fallback"
    assert metadata["video_variant"] == "text"
    frozen = store.read("jobs/" + key)[0]
    assert frozen["status"] == "frozen_text"
    assert core._verification_binding(frozen) == binding(NEW)


def test_valid_v4_publication_freezes_exact_one_branded_video_variant():
    store, config, key = submitted_job(NEW, name="Marc")
    proof = final_proof(NEW)
    assert store.finish(key, "paid-existing", {**MEDIA, "speech_proof": proof})
    ready = store.read("jobs/" + key)[0]
    assert store.publication_ready(ready)
    service = core.Service(config, store)
    message, metadata = service.select(ready, "same normal fallback")
    assert "https://go.crispshortsales.com/v/" + ready["id"] in message
    assert "same normal fallback" not in message
    assert metadata["video_variant"] == "video"
    assert metadata["video_speech_proof"]["expected_name"] == "Marc"
    assert metadata["video_speech_proof"]["normalized_transcript"] == "hey mark"
    frozen = store.read("jobs/" + key)[0]
    assert frozen["status"] == "frozen_video"
    assert service.select(ready, "changed normal fallback") == (message, metadata)
    assert store.read("jobs/" + key)[0] == frozen


@pytest.mark.parametrize("saved_method,reported_method", [(OLD_METHODS[0], NEW), (OLD_METHODS[1], NEW),
    (NEW, OLD_METHODS[0]), (NEW, OLD_METHODS[1])])
def test_worker_cannot_upgrade_or_downgrade_existing_completed_pipeline(saved_method, reported_method, monkeypatch):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    store, config, key = submitted_job(saved_method)
    engine = Mock(verification_method=NEW)
    engine.inspect.return_value = {"status": "completed", "provider_id": "paid-existing", **binding(reported_method)}
    core.Worker(store, engine, config, lambda job: True, now=lambda: NOW).tick()
    job = store.read("jobs/" + key)[0]
    assert job["status"] == "failed"
    assert job["underlying_code"] == "speech_guard_output_invalid"
    assert core._verification_binding(job) == binding(saved_method)
    engine.submit.assert_not_called()
    engine.prepare_media.assert_not_called()
    assert store.objects.list("assets/") == []
    assert store.objects.list("public/") == []


def test_adding_v4_does_not_enable_generation_or_change_provider_defaults_and_budgets(world, monkeypatch):
    monkeypatch.setattr(bootstrap, "Config", lambda: core.Config({}))
    factory = Mock(side_effect=AssertionError("Disabled worker may not instantiate storage"))
    monkeypatch.setattr(bootstrap, "get_service", factory)
    assert bootstrap.create_worker() is None
    factory.assert_not_called()
    assert world[0].verification_method == speech.LEGACY_VERIFICATION_METHOD
    assert world[0].generator_profile == "legacy_v1"
    defaults = core.Config({})
    assert (defaults.enabled, defaults.generation_enabled, defaults.live) == (False, False, False)
    assert (defaults.daily_cents, defaults.monthly_cents, defaults.reserve_cents) == (300, 7500, 10)
    assert (provider.TTS_REQUEST_CENTS, provider.LIPSYNC_REQUEST_CENTS, provider.RESERVATION_CENTS) == (2, 7, 10)
    assert provider.TTS_REQUEST_CENTS + provider.LIPSYNC_REQUEST_CENTS == 9
    assert (provider.TTS_SEED, provider.TTS_TEMPERATURE) == (4411, 0.5)
    world[2].request.assert_not_called()


@pytest.mark.parametrize("limits", [{"VIDEO_FOLLOWUP_DAILY_CENTS": "301"},
    {"VIDEO_FOLLOWUP_MONTHLY_CENTS": "7501"}])
def test_general_policy_cannot_expand_existing_budget_limits(limits):
    with pytest.raises(ValueError, match="budget exceeds"):
        core.Config(limits)
