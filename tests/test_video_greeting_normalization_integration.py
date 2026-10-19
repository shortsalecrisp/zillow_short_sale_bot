"""Future-only greeting preparation safety; no generation, ASR, or transport."""

import copy
import hashlib
import io
import json
import math
import struct
import wave
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import video_followup as core
import video_followup_audio_guard as acoustic
import video_followup_audio_lineage as lineage
import video_followup_greeting_normalization as normalization
import video_followup_same_take as provider
import video_followup_same_take_media as media
import video_followup_speech_guard as speech
import video_followup_worker as bootstrap
from test_video_followup_same_take import (
    MASTER, MASTER_KEY, REFERENCE, clean_reference, paid_calls, planned, prepared,
    speech_proof, start, state, world,
)
from test_video_pronunciation_integration import MEDIA, NOW, submitted_job
from test_video_followup_same_take_media import ffmpeg_or_skip


NEW = "clean_body_bounded_pad_v1"
METHOD = speech.GENERAL_PRONUNCIATION_VERIFICATION_METHOD
LEGACY_HASH = "45ec6161cfa049ac202c9c3012f76fd04fd0826371c3037258432ee29691b837"
CLEAN_HASH = "004e3565b1c42c4a477d448a1bdf4fe1b3ee0e4d656c29e227b6e48aef7e5a93"


def wav_audio(*, leading=2.4, duration=0.7, trailing=0.3, frequency=180, rate=24000, padding_sample=0):
    frames = [padding_sample] * round(leading * rate)
    frames.extend(round(8000 * math.sin(2 * math.pi * frequency * index / rate))
                  for index in range(round(duration * rate)))
    frames.extend([padding_sample] * round(trailing * rate))
    target = io.BytesIO()
    with wave.open(target, "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(rate)
        output.writeframes(struct.pack("<" + "h" * len(frames), *frames))
    return target.getvalue()


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    for path in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
        monkeypatch.setattr(path, Mock(side_effect=AssertionError("Offline greeting integration only")))
    monkeypatch.setattr(speech, "_run_child", Mock(side_effect=AssertionError("No model work in integration tests")))


def new_service(world, profile=NEW, method=METHOD):
    original = world[0]
    result = provider.SameTakeVideoProvider(
        original.fal_key, world[1], MASTER_KEY, tts_key=original.tts_key,
        session=world[2], media_session=Mock(), resolver=original.resolver,
        preparer=world[3], compositor=original.compositor,
        generator_profile=profile, verification_method=method)
    result._download_fal = original._download_fal
    return result


@pytest.fixture
def normalized_world(world, planned, clean_reference, monkeypatch):
    service = new_service(world)
    current = (service, *world[1:])
    raw = wav_audio()
    service._download_fal.return_value = raw

    def prepare(master, audio, *, first_name):
        assert master == MASTER
        inputs = prepared(first_name)
        inputs["metadata"]["narration_sha256"] = hashlib.sha256(audio).hexdigest()
        return inputs

    def proof(audio, name, **kwargs):
        assert kwargs == {"policy_version": speech.GENERAL_PRONUNCIATION_POLICY_VERSION}
        result = speech_proof(name, audio)
        result.update(policy_version=speech.GENERAL_PRONUNCIATION_POLICY_VERSION,
                      config_sha256=speech.GENERAL_PRONUNCIATION_CONFIG_SHA256)
        return result

    world[3].side_effect = prepare
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=proof))
    return current, raw, planned


def test_new_profile_pins_only_preparation_and_keeps_historical_contracts():
    assert provider._generator_profile("legacy_v1")["render_config_sha256"] == LEGACY_HASH
    assert provider._generator_profile("clean_body_v1")["render_config_sha256"] == CLEAN_HASH
    old = provider._generator_profile("clean_body_v1")
    new = provider._generator_profile(NEW)
    assert new["config"] == {
        **old["config"], "generator_profile": NEW,
        "greeting_normalization": normalization.NORMALIZATION_CONFIG,
    }
    assert new["render_config_sha256"] not in {LEGACY_HASH, CLEAN_HASH}
    assert new["reference_key"] == old["reference_key"]
    assert new["reference_sha256"] == old["reference_sha256"]
    assert new["config"]["audio_guard"] == acoustic.GUARD_VERSION == "crisp-recording-acoustic-v1"
    assert new["config"]["greeting_pattern"] == "Hey {first_name}!"
    assert (new["config"]["seed"], new["config"]["temperature"]) == (4411, 0.5)
    assert new["config"]["tts_model"] == provider.TTS_MODEL
    assert new["config"]["lipsync_model"] == provider.MODEL
    assert (provider.TTS_REQUEST_CENTS, provider.LIPSYNC_REQUEST_CENTS, provider.RESERVATION_CENTS) == (2, 7, 10)
    defaults = core.Config({})
    assert (defaults.daily_cents, defaults.monthly_cents, defaults.reserve_cents) == (300, 7500, 10)


@pytest.mark.parametrize("method", [provider.LEGACY_VERIFICATION_METHOD,
    provider.PLANNED_AUDIO_VERIFICATION_METHOD, provider.PRONUNCIATION_VERIFICATION_METHOD])
def test_new_profile_rejects_weaker_verification_before_any_request(world, method):
    with pytest.raises(provider.VideoProviderError) as error:
        new_service(world, method=method)
    assert error.value.code == "verification_method_invalid"
    world[2].request.assert_not_called()


def test_new_profile_preserves_original_tts_and_freezes_normalized_bytes(normalized_world):
    world, raw, plan = normalized_world
    expected = normalization.normalize_greeting_audio(raw)
    identifier = start(world, name="Oscar", job_id="new-private-future-episode")
    assert paid_calls(world)[0].kwargs["json"] == {
        "text": "Hey Oscar!", "seed": 4411, "temperature": 0.5,
        "audio_url": world[1].objects.signed_get_url(provider.CLEAN_BODY_REFERENCE_KEY),
    }
    assert world[0].inspect(identifier)["status"] == "processing"
    saved = state(world, identifier)
    provider.inspect_greeting.assert_called_once_with(expected["audio_bytes"], REFERENCE)
    world[3].assert_called_once_with(MASTER, expected["audio_bytes"], first_name="Oscar")
    assert world[1].objects.read(f"assets/{identifier}/raw-narration.wav")[0] == raw
    assert world[1].objects.read(f"assets/{identifier}/normalized-narration.wav")[0] == expected["audio_bytes"]
    assert saved["metadata"]["greeting_normalization"] == expected["provenance"]
    assert saved["metadata"]["narration_sha256"] == hashlib.sha256(expected["audio_bytes"]).hexdigest()
    bundle = json.loads(world[1].objects.read(f"assets/{identifier}/prepared-plan.json")[0])
    assert bundle["metadata"]["greeting_normalization"] == expected["provenance"]
    assert bundle["manifest"]["preparation_sha256"] == provider._digest_json(bundle["metadata"])
    assert saved["generator_profile"] == NEW
    assert saved["verification_method"] == METHOD
    assert saved["estimated_cents"] == 9
    assert saved["reserved_cents"] == 10
    assert plan[0].call_count == 1
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1
    assert provider.verify_greeting_words.call_args.kwargs == {
        "policy_version": speech.GENERAL_PRONUNCIATION_POLICY_VERSION,
    }
    for call in paid_calls(world):
        assert call.kwargs["headers"]["X-Fal-No-Retry"] == "1"
        assert call.kwargs["headers"]["x-app-fal-disable-fallback"] == "true"


def test_pending_old_profile_keeps_raw_length_rejection_after_factory_change(world, planned, clean_reference, monkeypatch):
    old = new_service(world, profile=provider.CLEAN_BODY_GENERATOR_PROFILE)
    legacy = (old, *world[1:])
    raw = wav_audio()
    old._download_fal.return_value = raw
    identifier = start(legacy, name="Oscar")
    before = state(legacy, identifier)
    active = new_service(legacy)
    monkeypatch.setattr(provider, "normalize_greeting_audio", Mock(side_effect=AssertionError("Old paid request must not normalize")))
    provider.inspect_greeting.side_effect = provider.VideoProviderError("greeting_too_long", "Raw greeting exceeds three seconds")
    assert active.submit("Oscar", "job") == identifier
    assert active.inspect(identifier)["status"] == "failed"
    after = state(legacy, identifier)
    assert after["underlying_code"] == "greeting_too_long"
    assert after["generator_profile"] == before["generator_profile"] == "clean_body_v1"
    assert after["generator_config"] == before["generator_config"]
    assert after["render_config_sha256"] == before["render_config_sha256"]
    provider.inspect_greeting.assert_called_once_with(raw, REFERENCE)
    provider.normalize_greeting_audio.assert_not_called()
    assert world[1].objects.list(f"assets/{identifier}/") == [f"assets/{identifier}/evidence.json"]
    assert len(paid_calls(legacy, provider.TTS_MODEL)) == 1
    assert not paid_calls(legacy, provider.MODEL)


@pytest.mark.parametrize("stage", ["narration_submitting", "narration_pending", "fal_submitting",
    "fal_pending", "failed", "uncertain", "cancelled", "completed"])
def test_new_default_never_rebinds_existing_oscar_pipeline_or_starts_new_request(world, clean_reference, monkeypatch, stage):
    old = new_service(world, profile=provider.CLEAN_BODY_GENERATOR_PROFILE)
    legacy = (old, *world[1:])
    identifier = start(legacy, name="Oscar", job_id="existing-oscar-failed-episode")
    world[1].mutate("same-take/" + identifier, lambda saved: {**saved, "stage": stage})
    before = copy.deepcopy(state(legacy, identifier))
    active = new_service(legacy)
    for method in ("_master", "_reference", "_tts_reference"):
        setattr(active, method, Mock(side_effect=AssertionError("Existing identity may not read new assets")))
    monkeypatch.setattr(provider, "preflight_speech_guard", Mock(side_effect=AssertionError("No new preflight")))
    world[2].request.reset_mock()
    assert active.submit("Oscar", "existing-oscar-failed-episode") == identifier
    assert state(legacy, identifier) == before
    assert world[1].objects.list("same-take/") == ["same-take/" + identifier]
    world[2].request.assert_not_called()


@pytest.mark.parametrize("status", ["failed", "uncertain", "cancelled", "budget_blocked", "frozen_text",
    "frozen_video", "sent", "claimed", "send_started"])
def test_profile_activation_never_reopens_terminal_frozen_or_inflight_worker_episode(monkeypatch, status):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    store, config, key = submitted_job(METHOD, name="Oscar")
    store.mutate("jobs/" + key, lambda old: {**old, "status": status, "budget_settled": True})
    before = copy.deepcopy(store.read("jobs/" + key)[0])
    engine = Mock(generator_profile=NEW, verification_method=METHOD)
    core.Worker(store, engine, config, lambda job: True, now=lambda: NOW).tick()
    assert store.read("jobs/" + key)[0] == before
    assert store.objects.list("assets/") == []
    assert store.objects.list("public/") == []
    engine.submit.assert_not_called()
    engine.inspect.assert_not_called()
    engine.prepare_media.assert_not_called()


def test_existing_text_fallback_cannot_gain_late_video_or_second_payload_after_profile_activation(monkeypatch):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    store, config, key = submitted_job(METHOD, name="Oscar")
    original = store.read("jobs/" + key)[0]
    service = core.Service(config, store)
    message, metadata = service.select(original, "original ordinary text reminder")
    assert metadata["video_variant"] == "text"
    assert not store.finish(key, "paid-existing", MEDIA)
    assert service.select(original, "different late reminder") == (message, metadata)
    frozen = store.read("jobs/" + key)[0]
    assert frozen["status"] == "frozen_text"
    assert store.prepare(original)["status"] == "frozen_text"
    engine = Mock(generator_profile=NEW, verification_method=METHOD)
    engine.inspect.return_value = {"status": "processing"}
    core.Worker(store, engine, config, lambda job: True, now=lambda: NOW).tick()
    assert store.read("jobs/" + key)[0]["status"] == "frozen_text"
    engine.submit.assert_not_called()
    engine.prepare_media.assert_not_called()
    assert store.objects.list("public/") == []
    assert store.objects.list("assets/") == []


@pytest.mark.parametrize("filename", ["raw-narration.wav", "normalized-narration.wav", "prepared-plan.json"])
def test_retained_normalization_or_plan_tampering_never_publishes_or_regenerates(normalized_world, filename):
    world, _, _ = normalized_world
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "processing"
    completed = world[0].inspect(identifier)
    key = f"assets/{identifier}/{filename}"
    _, version = world[1].objects.read(key)
    world[1].objects.write(key, b"tampered retained input", version)
    before = len(paid_calls(world))
    with pytest.raises(speech.SpeechGuardError):
        world[0].prepare_media(completed, "Sue")
    world[0].compositor.assert_not_called()
    assert "final_speech_proof" not in state(world, identifier)
    assert len(paid_calls(world)) == before == 2


def test_free_verifier_retry_reuses_identical_normalized_plan_without_paid_retry(normalized_world, monkeypatch):
    world, _, plan = normalized_world
    identifier = start(world)
    verify = provider.verify_greeting_words.side_effect
    provider.verify_greeting_words.side_effect = speech.SpeechGuardError("speech_guard_busy")
    assert world[0].inspect(identifier)["status"] == "processing"
    saved = copy.deepcopy(state(world, identifier))
    assets = copy.deepcopy({key: value for key, value in world[1].objects.values.items()
                            if key.startswith(f"assets/{identifier}/")})
    downloads = world[0]._download_fal.call_count
    preparations = world[3].call_count
    provider.verify_greeting_words.side_effect = verify
    now = core.utcnow() + timedelta(seconds=61)
    monkeypatch.setattr(provider, "utcnow", lambda: now)
    assert world[0].inspect(identifier)["status"] == "processing"
    assert state(world, identifier)["audio_plan_sha256"] == saved["audio_plan_sha256"]
    bundle = json.loads(assets[f"assets/{identifier}/prepared-plan.json"][0])
    assert state(world, identifier)["metadata"] == bundle["metadata"]
    assert all(world[1].objects.values[key] == value for key, value in assets.items())
    assert world[0]._download_fal.call_count == downloads
    assert world[3].call_count == preparations
    assert plan[0].call_count == 1
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1


def test_new_normalized_pipeline_completes_with_same_strict_final_audio_lineage(normalized_world):
    world, _, plan = normalized_world
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "processing"
    completed = world[0].inspect(identifier)
    result = world[0].prepare_media(completed, "Sue")
    assert completed["verification_method"] == METHOD
    assert result["speech_proof"]["policy_version"] == speech.GENERAL_PRONUNCIATION_POLICY_VERSION
    assert result["speech_proof"]["config_sha256"] == speech.GENERAL_PRONUNCIATION_CONFIG_SHA256
    assert result["speech_proof"]["verification_method"] == METHOD
    assert result["speech_proof"]["audio_lineage"]["accepted"] is True
    assert plan[1].call_count == 1
    provider.verify_video_greeting_words.assert_not_called()
    assert len(paid_calls(world)) == 2


@pytest.mark.parametrize("code", ["speech_guard_name_mismatch", "speech_guard_low_confidence",
    "speech_guard_pronunciation_unresolved", "speech_guard_output_invalid"])
def test_normalization_never_bypasses_strict_final_name_screen(normalized_world, code):
    world, _, _ = normalized_world
    provider.verify_greeting_words.side_effect = speech.SpeechGuardError(code)
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["underlying_code"] == code
    assert not paid_calls(world, provider.MODEL)
    assert world[0].inspect(identifier)["status"] == "failed"
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1


@pytest.mark.parametrize("code", ["narration_pitch_outlier", "invalid_greeting_timing",
    "clipped_narration_audio", "quiet_narration_audio", "uncertain_narration_pitch"])
def test_normalization_never_bypasses_unchanged_acoustic_and_cadence_checks(normalized_world, code):
    world, _, _ = normalized_world
    provider.inspect_greeting.side_effect = provider.VideoProviderError(code, "Unchanged strict guard rejected")
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["underlying_code"] == code
    world[3].assert_not_called()
    provider.verify_greeting_words.assert_not_called()
    assert not paid_calls(world, provider.MODEL)


def test_true_overlong_speech_is_not_trimmed_into_a_successful_greeting(normalized_world):
    world, _, _ = normalized_world
    raw = wav_audio(leading=0.3, duration=3.2, trailing=0.3)
    world[0]._download_fal.return_value = raw
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "failed"
    saved = state(world, identifier)
    assert saved["underlying_code"] == "greeting_too_long"
    assert saved["failure_stage"] == "audio_normalization"
    assert world[1].objects.read(f"assets/{identifier}/raw-narration.wav")[0] == raw
    provider.inspect_greeting.assert_not_called()
    world[3].assert_not_called()
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("padding_sample", [19, 32, -32])
def test_long_low_noise_tail_passes_only_after_unchanged_real_acoustic_screen(
        normalized_world, monkeypatch, padding_sample):
    world, _, _ = normalized_world
    raw = wav_audio(leading=0, duration=1.36, trailing=1.66, padding_sample=padding_sample)
    reference = wav_audio(leading=0, duration=8.44, trailing=0)
    monkeypatch.setattr(acoustic, "REFERENCE_SHA256", hashlib.sha256(reference).hexdigest())
    guard = Mock(side_effect=lambda audio, _: acoustic.inspect_greeting(audio, reference))
    monkeypatch.setattr(provider, "inspect_greeting", guard)
    world[0]._download_fal.return_value = raw
    identifier = start(world, name="Oscar", job_id="new-synthetic-noise-tail-episode")
    assert world[0].inspect(identifier)["status"] == "processing"
    saved = state(world, identifier)
    provenance = saved["metadata"]["greeting_normalization"]
    assert provenance["version"] == "bounded-noise-edge-padding-v1"
    assert provenance["raw_duration_seconds"] == 3.02
    assert 1.36 <= provenance["normalized_duration_seconds"] <= 1.47
    assert provenance["interior_samples_removed"] == 0
    assert provenance["all_above_noise_floor_original_channel_samples_retained"] is True
    assert provenance["maximum_discarded_absolute_pcm16"] <= 32
    assert provenance["speech_or_acoustic_acceptance_claimed"] is False
    normalized = world[1].objects.read(f"assets/{identifier}/normalized-narration.wav")[0]
    guard.assert_called_once_with(normalized, REFERENCE)
    assert saved["audio_qa"]["quality"]["guard_version"] == "crisp-recording-acoustic-v1"
    assert saved["audio_qa"]["quality"]["prepared_greeting_seconds"] <= 2.5
    assert abs(saved["audio_qa"]["quality"]["pitch_shift_semitones"]) <= 3
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert len(paid_calls(world, provider.MODEL)) == 1
    assert saved["estimated_cents"] == 9


@pytest.mark.parametrize("tail_sample", [33, -33])
def test_even_one_tail_sample_above_pinned_noise_floor_is_retained_and_blocks_overlength(
        normalized_world, tail_sample):
    world, _, _ = normalized_world
    raw = bytearray(wav_audio(leading=0, duration=1.36, trailing=1.66, padding_sample=19))
    struct.pack_into("<h", raw, len(raw) - 2, tail_sample)
    raw = bytes(raw)
    world[0]._download_fal.return_value = raw
    identifier = start(world)
    assert world[0].inspect(identifier)["status"] == "failed"
    saved = state(world, identifier)
    assert saved["failure_stage"] == "audio_normalization"
    assert saved["underlying_code"] == "greeting_too_long"
    assert world[1].objects.read(f"assets/{identifier}/raw-narration.wav")[0] == raw
    provider.inspect_greeting.assert_not_called()
    world[3].assert_not_called()
    assert not paid_calls(world, provider.MODEL)


def test_real_media_path_preserves_normalized_cadence_and_native_final_audio(tmp_path, monkeypatch):
    ffmpeg = ffmpeg_or_skip()
    source = tmp_path / "synthetic-master.mp4"
    media._run([
        ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "lavfi", "-i", "color=c=0x305060:s=1024x576:r=30",
        "-f", "lavfi", "-i", "sine=frequency=240:sample_rate=48000",
        "-frames:v", "253", "-t", "8.44", "-c:v", "libx264", "-threads", "1",
        "-c:a", "aac", "-pix_fmt", "yuv420p", str(source),
    ])
    master = source.read_bytes()
    monkeypatch.setattr(media, "SOURCE_SHA256", hashlib.sha256(master).hexdigest())
    reference = wav_audio(leading=0, duration=8.44, trailing=0)
    monkeypatch.setattr(acoustic, "REFERENCE_SHA256", hashlib.sha256(reference).hexdigest())
    raw = wav_audio(leading=0, duration=1.36, trailing=1.66, padding_sample=19)
    with pytest.raises(provider.VideoProviderError) as rejected_raw:
        acoustic.inspect_greeting(raw, reference)
    assert rejected_raw.value.code == "greeting_too_long"
    normalized = normalization.normalize_greeting_audio(raw)
    qa = acoustic.inspect_greeting(normalized["audio_bytes"], reference)
    assert qa["quality"]["prepared_greeting_seconds"] <= 2.5
    inputs = media.prepare_inputs(master, normalized["audio_bytes"], first_name="Oscar", ffmpeg=ffmpeg)
    metadata = inputs["metadata"]
    metadata["greeting_normalization"] = normalized["provenance"]
    assert metadata["narration_sha256"] == hashlib.sha256(normalized["audio_bytes"]).hexdigest()
    assert 15 <= metadata["greeting_frames"] <= 75
    assert 2 < metadata["context_seconds"] < 5
    plan = lineage.prepare_audio_plan(master, inputs["greeting_audio_bytes"], metadata,
                                     first_name="Oscar", ffmpeg=ffmpeg)
    assert plan["manifest"]["preparation_sha256"] == provider._digest_json(metadata)
    assert plan["manifest"]["prepared_greeting_sha256"] == metadata["greeting_audio_sha256"]
    assert plan["manifest"]["planned_audio_sha256"] == hashlib.sha256(plan["audio_bytes"]).hexdigest()
    assert plan["manifest"]["checked_audio_sha256"] == hashlib.sha256(plan["greeting_audio_bytes"]).hexdigest()
    assert len(plan["manifest"]["joined_pcm_sha256"]) == 64
    assert plan["manifest"]["joined_sample_count"] > metadata["greeting_frames"] * media.SAMPLES_PER_FRAME
    # Synthetic context replaces provider output and the name proof is a fixture.
    # This verifies media lineage, not real pronunciation, lip-sync, or an organic send.
    final = media.compose(master, inputs["video_bytes"], inputs["greeting_audio_bytes"], metadata,
                          first_name="Oscar", planned_audio_bytes=plan["audio_bytes"], ffmpeg=ffmpeg)
    fixture_proof = speech_proof("Oscar", plan["greeting_audio_bytes"])
    fixture_proof.update(policy_version=speech.GENERAL_PRONUNCIATION_POLICY_VERSION,
                         config_sha256=speech.GENERAL_PRONUNCIATION_CONFIG_SHA256)
    proof = lineage.verify_final_audio(final["media_bytes"], plan["audio_bytes"], plan["manifest"],
        fixture_proof, metadata, first_name="Oscar", expected_manifest_sha256=plan["manifest_sha256"], ffmpeg=ffmpeg)
    assert proof["accepted"] is True
    assert proof["final_media_sha256"] == hashlib.sha256(final["media_bytes"]).hexdigest()
    assert proof["full_pcm_sha256"] == plan["manifest"]["full_pcm_sha256"]
    assert proof["greeting_pcm_sha256"] == plan["manifest"]["greeting_pcm_sha256"]
    assert proof["aac_track_sha256"] == provider._digest_json(plan["manifest"]["aac_track"])
    assert lineage.inspect_aac_track(final["media_bytes"], video_tracks=1) == plan["manifest"]["aac_track"]
    assert final["qa"]["original_body_frames"] == 227
    assert final["qa"]["body_frames_exact_before_encoding"] is True
    assert b"Hey Oscar," in final["captions_bytes"]
    original_path, tampered_path = tmp_path / "original.mp4", tmp_path / "tampered-audio.mp4"
    original_path.write_bytes(final["media_bytes"])
    media._run([
        ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-i", str(original_path), "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000",
        "-map", "0:v:0", "-map", "1:a:0", "-c:v", "copy", "-c:a", "aac", "-b:a", "160k",
        "-movie_timescale", "48000", "-t", str(final["duration_seconds"]), str(tampered_path),
    ])
    with pytest.raises(provider.VideoProviderError):
        lineage.verify_final_audio(tampered_path.read_bytes(), plan["audio_bytes"], plan["manifest"],
            fixture_proof, metadata, first_name="Oscar", expected_manifest_sha256=plan["manifest_sha256"], ffmpeg=ffmpeg)


def test_factory_env_selects_new_profile_without_changing_transport_or_enabling_generation(monkeypatch):
    config = SimpleNamespace(enabled=True, generation_enabled=True, origin="https://video.example.invalid",
        bucket="private", not_before="2026-10-14", live=False, allowlist={"9542053205"})
    monkeypatch.setattr(bootstrap, "Config", lambda: config)
    for key, value in {
        "FAL_API_KEY": "offline-kling", "FAL_TTS_API_KEY": "offline-tts",
        "VIDEO_FOLLOWUP_SAME_TAKE_MASTER_KEY": MASTER_KEY,
        "TASKER_TRANSPORT_HEALTH_URL": "https://script.google.com/offline",
        "SMS_CHATBOT_ALLOWED_TOKEN": "offline-token",
        "VIDEO_FOLLOWUP_GENERATION_PROFILE": NEW,
        "VIDEO_FOLLOWUP_AUDIO_VERIFICATION_METHOD": METHOD,
    }.items():
        monkeypatch.setenv(key, value)
    store = object()
    monkeypatch.setattr(bootstrap, "get_service", Mock(return_value=SimpleNamespace(store=store)))
    constructor, worker = Mock(return_value=object()), Mock()
    monkeypatch.setattr(provider, "SameTakeVideoProvider", constructor)
    monkeypatch.setattr(bootstrap, "Worker", worker)
    assert bootstrap.create_worker() is worker.return_value
    constructor.assert_called_once_with("offline-kling", store, MASTER_KEY, tts_key="offline-tts",
        generator_profile=NEW, verification_method=METHOD)
    assert worker.call_args.args[2] is config
    monkeypatch.setattr(bootstrap, "Config", lambda: core.Config({}))
    constructor.reset_mock()
    assert bootstrap.create_worker() is None
    constructor.assert_not_called()
