"""Offline policy/rollout tests, not actual pronunciation or sent-video proof."""

import copy
import hashlib
from unittest.mock import Mock

import pytest

import video_followup as core
import video_followup_pronunciation as pronunciation
import video_followup_same_take as provider
import video_followup_speech_guard as speech
from test_video_followup_same_take import (
    MASTER, MASTER_KEY, clean_reference, paid_calls, planned, prepared,
    speech_proof, start, state, world,
)
from test_video_greeting_normalization_integration import new_service, wav_audio
from test_video_general_pronunciation_safety import final_proof
from test_video_pronunciation_integration import MEDIA, NOW, submitted_job
from test_video_pronunciation_policy import audio, recognized, planned as planned_proof


POLICY = speech.REVIEWED_VARIANT_POLICY_VERSION
METHOD = speech.REVIEWED_VARIANT_VERIFICATION_METHOD
SPEECH_HASH = "060ec33e383b742872a129c3652143f658313851fd61594cec243789b5dcb8fd"
LINEAGE_HASH = "528159aa9b387b42fbc0a4987953e5e46fd36e808ee098a677b5f8cab5ba7592"
OLD_POLICIES = (speech.POLICY_VERSION, speech.PRONUNCIATION_POLICY_VERSION,
                speech.GENERAL_PRONUNCIATION_POLICY_VERSION)
OLD_METHODS = (speech.PLANNED_AUDIO_VERIFICATION_METHOD, speech.PRONUNCIATION_VERIFICATION_METHOD,
               speech.GENERAL_PRONUNCIATION_VERIFICATION_METHOD)


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
        monkeypatch.setattr(target, Mock(side_effect=AssertionError("Offline tests only")))
    monkeypatch.setattr(speech, "_run_child", Mock(side_effect=AssertionError("No model work")))


def issue(audio, name):
    return speech.verify_greeting_words(audio, name, policy_version=POLICY)


def test_new_policy_is_exact_directional_and_all_old_identities_stay_frozen():
    assert POLICY == "spoken-greeting-tiny-en-v5"
    assert METHOD == "planned-aac-lineage-v4"
    assert speech.REVIEWED_VARIANT_CONFIG == {
        "policy": POLICY,
        "base_general_pronunciation_config_sha256": "e61b237ccaa3f4cccb369f0a88b1f9d0d4e01838242d9b74e90c8ed4891f03a2",
        "matching_rule": "whole-greeting-canonical-name-directed-reviewed-variants-v1",
        "observed_name_variants": {"colin": ("collin",), "sheikh": ("shake",)},
    }
    assert speech.REVIEWED_VARIANT_CONFIG_SHA256 == SPEECH_HASH
    assert speech.REVIEWED_VARIANT_VERIFICATION_CONFIG_SHA256 == LINEAGE_HASH
    assert speech.REVIEWED_VARIANT_VERIFICATION_CONFIG == {
        **speech.PLANNED_AUDIO_VERIFICATION_CONFIG,
        "method": METHOD, "speech_config_sha256": SPEECH_HASH,
    }
    assert [speech._policy_identity(policy)["config_sha256"] for policy in OLD_POLICIES] == [
        "b3a1b5798d53032e5c23da361c467141c1cfd502a8def78e8d3ddeaabde2322e",
        "bb77463c65987cdf07caef001a49d29cb70a93ee7372e7e9edcacd07fde5fea3",
        "e61b237ccaa3f4cccb369f0a88b1f9d0d4e01838242d9b74e90c8ed4891f03a2",
    ]
    assert [speech._digest_json(speech.PLANNED_VERIFICATION_PROFILES[m]) for m in OLD_METHODS] == [
        "9f7fe677874406a34d5969c0c9f3a547225165764685805c5df50c97f3e831d6",
        "45aa4b4c344179321d82eaf66ab0c36bcaa7cf8f56b48b07f9b63423bc2a6cf5",
        "3d70d23288207ff942cc5385ee6a926eb529434e950e4f4345e333f944500e25",
    ]
    assert speech.PRONUNCIATION_EQUIVALENCE_GROUPS == (
        ("kathryn", "katherine"), ("nikki", "nicky"), ("cydney", "sydney"), ("brook", "brooke"))
    assert pronunciation.LEXICON_MANIFEST == speech.GENERAL_PRONUNCIATION_LEXICON_MANIFEST
    assert pronunciation.MATCHING_CONFIG == speech.GENERAL_PRONUNCIATION_MATCHING_CONFIG
    assert provider._generator_profile(provider.BOUNDED_PAD_GENERATOR_PROFILE)["render_config_sha256"] == \
        "7857875c05ec9c332209b9254440e068d34af911d0d24d74a77366b07aabce19"


@pytest.mark.parametrize("name,observed", [("Colin", "Collin"), ("COLIN", "Collin"),
    ("Sheikh", "Shake"), ("SHEIKH", "SHAKE")])
def test_reviewed_observation_is_bound_to_original_canonical_name(audio, recognized, name, observed):
    recognized[0]["transcript"] = "Hey " + observed + "!"
    proof = issue(audio, name)
    assert proof["expected_name"] == name
    assert proof["normalized_transcript"] == "hey " + observed.casefold()
    assert proof["config_sha256"] == SPEECH_HASH
    assert proof["source_audio_sha256"] == hashlib.sha256(audio).hexdigest()
    assert len(recognized[1]) == 1
    assert speech.validate_speech_proof(proof, first_name=name)
    assert not speech.validate_speech_proof(proof, first_name=observed)
    final = planned_proof(proof, METHOD)
    assert speech.validate_speech_proof(final, first_name=name, media_sha256=final["media_sha256"])


@pytest.mark.parametrize("policy", OLD_POLICIES)
@pytest.mark.parametrize("name,observed", [("Colin", "Collin"), ("Sheikh", "Shake")])
def test_old_policy_never_inherits_new_aliases(audio, recognized, policy, name, observed):
    recognized[0]["transcript"] = "Hey " + observed
    with pytest.raises(speech.SpeechGuardError):
        speech.verify_greeting_words(audio, name, policy_version=policy)


@pytest.mark.parametrize("name,transcript", [
    ("Colin", "Hey Colleen!"), ("Colin", "Hey Colon"), ("Colin", "Hey Calling"),
    ("Colin", "Hey Colin Collin"), ("Colin", "Hey Collin again"),
    ("Sheikh", "Hey Shane"), ("Sheikh", "Hey Shakeera"), ("Sheikh", "Hey Shakes"),
    ("Sheikh", "Hey Shack"), ("Sheikh", "Hey Shake Shake"), ("Sheikh", "Hey Shake and Colin"),
    ("Sheikh", "Hey Shake it"), ("Sheikh", "Shake"), ("Sheikh", "Hi Shake"),
    ("Sheikh", "Hey Shake! Hey Shake!"), ("Sheikh", "Hey Shake 2"),
    ("Sheikh", "Hey Shake https://example.com"), ("Sheikh", "Hey Shake@example.com"),
    ("Shake", "Hey Sheikh"), ("Collin", "Hey Colin"),
    ("Dan", "Hey then"), ("Cydney", "Hey Cindy"), ("Tomekia", "Hey Tamika"),
])
def test_unapproved_reverse_nearby_and_extra_words_stay_rejected(audio, recognized, name, transcript):
    recognized[0]["transcript"] = transcript
    with pytest.raises(speech.SpeechGuardError) as error:
        issue(audio, name)
    assert error.value.code == "speech_guard_pronunciation_unresolved"
    assert not speech.is_transient_speech_failure(error.value)


@pytest.mark.parametrize("name,observed", [("Colin", "Colin"), ("Sheikh", "Sheikh"),
    ("Colin", "Collin"), ("Sheikh", "Shake"), ("Marc", "Mark"),
    ("Cydney", "Sydney"), ("Brook", "Brooke"), ("Jerrick", "Jerrick")])
def test_new_policy_preserves_existing_whole_phrase_behavior(audio, recognized, name, observed):
    recognized[0]["transcript"] = "Hey " + observed + "!"
    assert issue(audio, name)["accepted"] is True


@pytest.mark.parametrize("segments", [[], [{"avg_logprob": -1.01, "no_speech_prob": 0}],
    [{"avg_logprob": -0.1, "no_speech_prob": .60001}],
    [{"avg_logprob": True, "no_speech_prob": 0}],
    [{"avg_logprob": -0.1, "no_speech_prob": float("nan")}],
])
@pytest.mark.parametrize("name,observed", [("Colin", "Collin"), ("Sheikh", "Shake")])
def test_reviewed_alias_cannot_bypass_confidence(audio, recognized, name, observed, segments):
    recognized[0].update(transcript="Hey " + observed, segments=segments)
    with pytest.raises(speech.SpeechGuardError, match="speech_guard_low_confidence"):
        issue(audio, name)


@pytest.mark.parametrize("name,observed", [("Colin", "Collin"), ("Sheikh", "Shake")])
def test_new_issuance_still_preflights_dictionary_before_recognition(audio, recognized, monkeypatch, name, observed):
    recognized[0]["transcript"] = "Hey " + observed
    monkeypatch.setattr(speech, "preflight_pronunciation_guard",
        Mock(side_effect=speech.SpeechGuardError("speech_guard_pronunciation_data_invalid")))
    with pytest.raises(speech.SpeechGuardError, match="speech_guard_pronunciation_data_invalid"):
        issue(audio, name)
    assert recognized[1] == []


@pytest.mark.parametrize("field,value", [
    ("policy_version", speech.GENERAL_PRONUNCIATION_POLICY_VERSION),
    ("config_sha256", speech.GENERAL_PRONUNCIATION_CONFIG_SHA256),
    ("expected_name", "Collin"), ("normalized_transcript", "hey colon"),
    ("verification_method", speech.GENERAL_PRONUNCIATION_VERIFICATION_METHOD),
    ("verification_config_sha256", speech.GENERAL_PRONUNCIATION_VERIFICATION_CONFIG_SHA256),
])
def test_cross_version_or_renamed_final_proof_is_rejected(audio, recognized, field, value):
    recognized[0]["transcript"] = "Hey Collin"
    proof = planned_proof(issue(audio, "Colin"), METHOD)
    proof[field] = value
    assert not speech.validate_speech_proof(proof, first_name="Colin", media_sha256="b" * 64)


@pytest.mark.parametrize("seconds", [0.499, 2.501, float("nan"), True])
def test_final_greeting_duration_is_not_relaxed(audio, recognized, seconds):
    recognized[0]["transcript"] = "Hey Shake"
    proof = planned_proof(issue(audio, "Sheikh"), METHOD)
    proof["greeting_seconds"] = seconds
    assert not speech.validate_speech_proof(proof, first_name="Sheikh", media_sha256="b" * 64)


def test_future_bounded_pipeline_pins_new_policy_and_reuses_existing_old_pipeline(
        world, planned, clean_reference, monkeypatch):
    old_service = new_service(world)
    old_world = (old_service, *world[1:])
    old_identifier = start(old_world, "Colin", "existing-v4-episode")
    old = copy.deepcopy(state(old_world, old_identifier))
    future = new_service(world, method=METHOD)
    calls_before = world[2].request.call_count
    assert future.submit("Colin", "existing-v4-episode") == old_identifier
    assert state(old_world, old_identifier) == old
    assert world[2].request.call_count == calls_before

    raw = wav_audio()
    future._download_fal = Mock(return_value=raw)
    def prepare(master, content, *, first_name):
        assert master == MASTER
        inputs = prepared(first_name)
        inputs["metadata"]["narration_sha256"] = hashlib.sha256(content).hexdigest()
        return inputs
    world[3].side_effect = prepare
    def observed(content, name, **kwargs):
        assert kwargs == {"policy_version": POLICY}
        result = speech_proof(name, content)
        return {**result, **speech._policy_identity(POLICY), "normalized_transcript": "hey collin"}
    monkeypatch.setattr(provider, "verify_greeting_words", Mock(side_effect=observed))
    current = (future, *world[1:])
    identifier = start(current, "Colin", "new-future-v5-episode")
    assert future.inspect(identifier)["status"] == "processing"
    saved = state(current, identifier)
    assert saved["verification_method"] == METHOD
    assert saved["verification_config"] == speech.REVIEWED_VARIANT_VERIFICATION_CONFIG
    assert saved["verification_config_sha256"] == LINEAGE_HASH
    assert saved["greeting_script"] == "Hey Colin!"
    assert saved["generator_profile"] == provider.BOUNDED_PAD_GENERATOR_PROFILE
    assert saved["estimated_cents"] == 9
    assert saved["reserved_cents"] == 10
    assert state(old_world, old_identifier) == old
    assert len(paid_calls(current, provider.TTS_MODEL)) == 2
    assert len(paid_calls(current, provider.MODEL)) == 1
    assert provider.verify_greeting_words.call_count >= 1
    assert all(call.kwargs == {"policy_version": POLICY} for call in provider.verify_greeting_words.call_args_list)


@pytest.mark.parametrize("status", ["failed", "uncertain", "cancelled", "budget_blocked",
    "frozen_text", "frozen_video", "sent", "claimed", "send_started"])
@pytest.mark.parametrize("saved_method", [speech.GENERAL_PRONUNCIATION_VERIFICATION_METHOD, METHOD])
def test_activation_cannot_reopen_terminal_or_owned_episodes(saved_method, status, monkeypatch):
    monkeypatch.setattr(core, "utcnow", lambda: NOW)
    store, config, key = submitted_job(saved_method)
    store.mutate("jobs/" + key, lambda old: {**old, "status": status, "budget_settled": True})
    before = copy.deepcopy(store.read("jobs/" + key)[0])
    debit = store.read("budget/ledger")[0]["reservations"][key]["cents"]
    engine = Mock(verification_method=METHOD)
    assert store.prepare(before) == before
    core.Worker(store, engine, config, lambda job: True, now=lambda: NOW).tick()
    # Existing bookkeeping may retire the active index/release inflight
    # capacity, but cannot change the job, its debit, or publish new media.
    assert store.read("jobs/" + key)[0] == before
    assert store.read("budget/ledger")[0]["reservations"][key]["cents"] == debit
    assert store.objects.list("assets/") == []
    assert store.objects.list("public/") == []
    engine.submit.assert_not_called()
    engine.inspect.assert_not_called()
    engine.prepare_media.assert_not_called()


@pytest.mark.parametrize("saved_method,proof_method", [
    (speech.GENERAL_PRONUNCIATION_VERIFICATION_METHOD, METHOD),
    (METHOD, speech.GENERAL_PRONUNCIATION_VERIFICATION_METHOD),
])
def test_existing_job_cannot_publish_a_different_policy_proof(saved_method, proof_method):
    store, _, key = submitted_job(saved_method)
    proof = final_proof(proof_method, name="Sue", observed="sue")
    assert speech.validate_speech_proof(proof, first_name="Sue", media_sha256=proof["media_sha256"])
    before = copy.deepcopy(store.objects.values)
    with pytest.raises(speech.SpeechGuardError):
        store.finish(key, "paid-existing", {**MEDIA, "speech_proof": proof})
    assert store.objects.values == before


def test_adding_policy_changes_no_default_generation_model_or_budget(world):
    assert world[0].verification_method == speech.LEGACY_VERIFICATION_METHOD
    assert world[0].generator_profile == provider.LEGACY_GENERATOR_PROFILE
    assert (provider.TTS_REQUEST_CENTS, provider.LIPSYNC_REQUEST_CENTS, provider.RESERVATION_CENTS) == (2, 7, 10)
    config = core.Config({})
    assert (config.enabled, config.generation_enabled, config.live) == (False, False, False)
    assert (config.daily_cents, config.monthly_cents, config.reserve_cents) == (300, 7500, 10)
    assert speech.DECODING["initial_prompt"] is speech.DECODING["prefix"] is speech.DECODING["hotwords"] is None
    world[2].request.assert_not_called()
