"""Offline v4 proof contracts, not validation of generated speech quality."""

import copy
import hashlib
import subprocess
import sys
from unittest.mock import Mock

import pytest

import video_followup as core
import video_followup_pronunciation as pronunciation
import video_followup_same_take as provider
import video_followup_speech_guard as guard
from test_video_pronunciation_policy import audio, recognized, planned as final_proof
from test_video_followup_same_take import paid_calls, planned, start, state, world
from test_video_pronunciation_integration import recognition


POLICY = guard.GENERAL_PRONUNCIATION_POLICY_VERSION
METHOD = guard.GENERAL_PRONUNCIATION_VERIFICATION_METHOD
OLD_METHODS = (guard.PLANNED_AUDIO_VERIFICATION_METHOD, guard.PRONUNCIATION_VERIFICATION_METHOD)


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection"):
        monkeypatch.setattr(target, Mock(side_effect=AssertionError("offline tests only")))


def issue(audio, name):
    return guard.verify_greeting_words(audio, name, policy_version=POLICY)


def test_new_identities_are_separate_pinned_and_match_runtime_literals():
    assert POLICY == "spoken-greeting-tiny-en-v4"
    assert METHOD == "planned-aac-lineage-v3"
    assert guard.GENERAL_PRONUNCIATION_CONFIG_SHA256 == "e61b237ccaa3f4cccb369f0a88b1f9d0d4e01838242d9b74e90c8ed4891f03a2"
    assert guard.GENERAL_PRONUNCIATION_VERIFICATION_CONFIG_SHA256 == "3d70d23288207ff942cc5385ee6a926eb529434e950e4f4345e333f944500e25"
    assert guard.GENERAL_PRONUNCIATION_LEXICON_MANIFEST == pronunciation.LEXICON_MANIFEST
    assert guard.GENERAL_PRONUNCIATION_MATCHING_CONFIG == pronunciation.MATCHING_CONFIG
    assert guard.preflight_pronunciation_guard() == pronunciation.LEXICON_MANIFEST
    assert guard.PLANNED_VERIFICATION_POLICIES == {
        OLD_METHODS[0]: guard.POLICY_VERSION, OLD_METHODS[1]: guard.PRONUNCIATION_POLICY_VERSION,
        METHOD: POLICY}


@pytest.mark.parametrize("name,observed", [
    ("Sherri", "Sherry"), ("Sherri", "Cherie"), ("Shelley", "Shelly"), ("Marc", "Mark"),
    ("CYDNEY", "Sydney"), ("Brook", "Brooke"), ("Kathryn", "Katherine"),
    ("Nikki", "Nicky"), ("Tomekia", "Tomekia"), ("Jos\u00e9", "Jos\u00e9"), ("Hey", "Hey"),
])
def test_whole_audio_observation_uses_exact_reviewed_or_general_match(audio, recognized, name, observed):
    result, calls = recognized
    result["transcript"] = "Hey " + observed + "!"
    proof = issue(audio, name)
    assert proof["expected_name"] == name
    assert proof["normalized_transcript"] == "hey " + observed.casefold()
    assert proof["policy_version"] == POLICY
    assert proof["config_sha256"] == guard.GENERAL_PRONUNCIATION_CONFIG_SHA256
    assert proof["source_audio_sha256"] == hashlib.sha256(audio).hexdigest()
    assert proof["confidence"] == result["segments"]
    assert len(calls) == 1
    assert guard.validate_speech_proof(proof, first_name=name, audio_sha256=hashlib.sha256(audio).hexdigest())


@pytest.mark.parametrize("name,observed", [
    ("Sherri", "Hey Sherry thanks!"), ("Sherri", "Hey Sherry Hey!"),
    ("Sherri", "Hey Sherry Sherry"), ("Sherri", "Hey Sherry and Sue"),
    ("Sherri", "Hey Sherry Sue Anna Mary Jane"), ("Sherri", "Hey Sharon"),
    ("Cydney", "Hey Cindy"), ("Tomekia", "Hey Tamika"),
    ("Katherine", "Hey Katharine"), ("Sherri", "Hey hey Sherry"),
])
def test_unknown_distinct_extra_or_ambiguous_phrases_are_nonretryable(audio, recognized, name, observed):
    recognized[0]["transcript"] = observed
    with pytest.raises(guard.SpeechGuardError) as raised:
        issue(audio, name)
    assert raised.value.code == "speech_guard_pronunciation_unresolved"
    assert not guard.is_transient_speech_failure(raised.value)
    assert raised.value.uncertain is False
    assert guard.safe_speech_failure_diagnostics(raised.value)["policy_version"] == POLICY


def test_full_multitoken_tail_reaches_matcher_without_truncation(audio, recognized, monkeypatch):
    recognized[0]["transcript"] = "Hey An A!"
    compare = Mock(return_value="equivalent")
    monkeypatch.setattr(pronunciation, "compare_pronunciation", compare)
    proof = issue(audio, "Anna")
    assert proof["normalized_transcript"] == "hey an a"
    compare.assert_called_once_with("Anna", "hey an a")
    assert guard._normalized_phrase("Hey An A!") is None


@pytest.mark.parametrize("name,observed", [("CYDNEY", "Sydney"), ("Kathryn", "Katherine"), ("Tomekia", "Tomekia")])
def test_exact_and_reviewed_names_do_not_depend_on_dictionary_coverage(audio, recognized, monkeypatch, name, observed):
    recognized[0]["transcript"] = "Hey " + observed
    monkeypatch.setattr(pronunciation, "compare_pronunciation", Mock(side_effect=AssertionError("must use preserved match")))
    assert issue(audio, name)["accepted"] is True


@pytest.mark.parametrize("confidence", [
    [{"avg_logprob": -1.01, "no_speech_prob": 0}],
    [{"avg_logprob": -.1, "no_speech_prob": .60001}],
    [{"avg_logprob": True, "no_speech_prob": 0}],
    [{"avg_logprob": 10**1000, "no_speech_prob": 0}],
    [{"avg_logprob": -10**1000, "no_speech_prob": 0}],
    [{"avg_logprob": -.1, "no_speech_prob": 10**1000}],
    [{"avg_logprob": -.1, "no_speech_prob": -10**1000}], [],
])
def test_general_comparison_never_relaxes_confidence(audio, recognized, confidence):
    recognized[0].update(transcript="Hey Sherry", segments=confidence)
    with pytest.raises(guard.SpeechGuardError) as raised:
        issue(audio, "Sherri")
    assert raised.value.code == "speech_guard_low_confidence"
    assert not guard.is_transient_speech_failure(raised.value)


@pytest.mark.parametrize("field", ["LEXICON_MANIFEST", "MATCHING_CONFIG"])
def test_runtime_identity_change_blocks_before_recognition(audio, recognized, monkeypatch, field):
    monkeypatch.setattr(pronunciation, field, {})
    with pytest.raises(guard.SpeechGuardError, match="pronunciation_data_invalid"):
        issue(audio, "Sue")
    assert not recognized[1]


def test_missing_lexicon_is_terminal_before_recognition_or_payment(audio, recognized, world, monkeypatch):
    def unavailable():
        raise pronunciation.PronunciationDataError("missing")
    monkeypatch.setattr(pronunciation, "preflight_lexicon", unavailable)
    with pytest.raises(guard.SpeechGuardError) as raised:
        issue(audio, "Sue")
    assert raised.value.code == "speech_guard_pronunciation_data_invalid"
    assert not guard.is_transient_speech_failure(raised.value)
    assert not recognized[1]
    world[0].verification_method = METHOD
    with pytest.raises(guard.SpeechGuardError, match="pronunciation_data_invalid"):
        start(world)
    assert not paid_calls(world)
    assert not any(key.startswith("same-take/") for key in world[1].objects.values)


@pytest.mark.parametrize("asset_state", ["missing", "corrupt"])
@pytest.mark.parametrize("name,observed,readable", [
    ("Tomekia", "Tomekia", True), ("CYDNEY", "Sydney", True),
    ("Kathryn", "Katherine", True), ("Sherri", "Sherry", False),
])
@pytest.mark.parametrize("final", [False, True])
def test_stored_exact_reviewed_proofs_do_not_need_lexical_assets_but_new_issuance_does(
        audio, recognized, monkeypatch, tmp_path, asset_state, name, observed, readable, final):
    recognized[0]["transcript"] = "Hey " + observed
    proof = issue(audio, name)
    if final:
        proof = final_proof(proof, METHOD)
    path = tmp_path / "unavailable-lexicon.gz"
    if asset_state == "corrupt":
        path.write_bytes(b"not the pinned dictionary")
    monkeypatch.setattr(pronunciation, "_PATH", path)
    pronunciation._load_lexicon.cache_clear()
    try:
        bindings = {"media_sha256": "b" * 64} if final else {"audio_sha256": hashlib.sha256(audio).hexdigest()}
        assert guard.validate_speech_proof(proof, first_name=name, **bindings) is readable
        previous_calls = len(recognized[1])
        with pytest.raises(guard.SpeechGuardError, match="pronunciation_data_invalid"):
            issue(audio, name)
        assert len(recognized[1]) == previous_calls
    finally:
        pronunciation._load_lexicon.cache_clear()


def test_existing_policies_do_not_import_new_runtime(audio, recognized, monkeypatch):
    monkeypatch.setitem(sys.modules, "video_followup_pronunciation", None)
    recognized[0]["transcript"] = "Hey Sue"
    for policy in (guard.POLICY_VERSION, guard.PRONUNCIATION_POLICY_VERSION):
        proof = guard.verify_greeting_words(audio, "Sue", policy_version=policy)
        assert guard.validate_speech_proof(proof, first_name="Sue")
    with pytest.raises(guard.SpeechGuardError, match="pronunciation_data_invalid"):
        issue(audio, "Sue")


def test_isolated_recognition_script_loads_without_parent_pronunciation_import():
    command = [sys.executable, "-I", "-B", "-c",
        "import runpy,sys; v=runpy.run_path(sys.argv[1],run_name='offline_import'); "
        "assert 'video_followup_pronunciation' not in sys.modules; "
        "assert v['CONFIG_SHA256']=='b3a1b5798d53032e5c23da361c467141c1cfd502a8def78e8d3ddeaabde2322e'",
        guard.__file__]
    subprocess.run(command, check=True, capture_output=True, timeout=10)


@pytest.mark.parametrize("method", list(guard.PLANNED_VERIFICATION_PROFILES))
def test_new_method_preserves_exact_provider_and_store_binding(method):
    profile = provider._verification_profile(method)
    assert provider._saved_verification_profile(profile) == profile
    assert core._verification_binding(profile) == {key: profile[key] for key in (
        "verification_method", "verification_config_sha256")}
    assert provider._speech_policy_kwargs(profile) == (
        {} if method == OLD_METHODS[0] else {"policy_version": guard.PLANNED_VERIFICATION_POLICIES[method]})


@pytest.mark.parametrize("method", [guard.LEGACY_VERIFICATION_METHOD, *OLD_METHODS])
def test_v4_proof_never_validates_for_saved_legacy_pipeline(audio, recognized, method):
    recognized[0]["transcript"] = "Hey Sue"
    proof = issue(audio, "Sue")
    saved = {"first_name": "Sue", **provider._verification_profile(method)}
    assert not provider._validate_pipeline_speech_proof(saved, proof)


@pytest.mark.parametrize("changes", [
    {"policy_version": guard.POLICY_VERSION}, {"policy_version": guard.PRONUNCIATION_POLICY_VERSION},
    {"config_sha256": guard.PRONUNCIATION_CONFIG_SHA256}, {"expected_name": "Sherry"},
    {"normalized_transcript": "hey sherry thanks"}, {"source_audio_sha256": "0" * 64},
    {"model_revision": "changed"}, {"accepted": "true"},
])
def test_general_proof_binds_identity_observation_name_and_audio(audio, recognized, changes):
    recognized[0]["transcript"] = "Hey Sherry"
    proof = issue(audio, "Sherri")
    assert not guard.validate_speech_proof({**proof, **changes}, first_name="Sherri",
        audio_sha256=hashlib.sha256(audio).hexdigest())


@pytest.mark.parametrize("removed", [
    ("audio_lineage",), ("verification_method",), ("verification_config_sha256",),
    ("audio_lineage", "verification_method", "verification_config_sha256"),
])
def test_v4_final_proof_requires_complete_frozen_lineage(audio, recognized, removed):
    recognized[0]["transcript"] = "Hey Sherry"
    final = final_proof(issue(audio, "Sherri"), METHOD)
    assert guard.validate_speech_proof(final, first_name="Sherri", media_sha256="b" * 64)
    for field in removed:
        final.pop(field)
    assert not guard.validate_speech_proof(final, first_name="Sherri")
    assert not guard.validate_speech_proof(final, first_name="Sherri", media_sha256="b" * 64)


@pytest.mark.parametrize("other", OLD_METHODS)
def test_v4_final_proof_cannot_mix_old_method_tuple(audio, recognized, other):
    recognized[0]["transcript"] = "Hey Sherry"
    assert not guard.validate_speech_proof(final_proof(issue(audio, "Sherri"), other), first_name="Sherri", media_sha256="b" * 64)


@pytest.mark.parametrize("field", ["first_name", "checked_audio_sha256", "speech_proof_sha256", "full_pcm_sha256", "final_media_sha256"])
def test_v4_final_lineage_rejects_changed_bindings(audio, recognized, field):
    recognized[0]["transcript"] = "Hey Sherry"
    final = final_proof(issue(audio, "Sherri"), METHOD)
    final["audio_lineage"][field] = "changed"
    assert not guard.validate_speech_proof(final, first_name="Sherri", media_sha256="b" * 64)


def test_future_v4_pipeline_uses_one_tts_one_lipsync_and_exact_checked_audio(world, recognition):
    world[0].verification_method = METHOD
    recognition["text"] = "Hey Sherry!"
    identifier = start(world, "Sherri")
    assert world[0].inspect(identifier)["status"] == "processing"
    saved = state(world, identifier)
    assert saved["greeting_speech_proof"]["policy_version"] == POLICY
    provider.verify_greeting_words.assert_called_once_with(b"decoded AAC greeting", "Sherri", policy_version=POLICY)
    assert len(paid_calls(world, provider.TTS_MODEL)) == len(paid_calls(world, provider.MODEL)) == 1
    assert saved["estimated_cents"] == 9


def test_unresolved_v4_pipeline_is_terminal_without_lipsync_or_retry(world, recognition):
    world[0].verification_method = METHOD
    recognition["text"] = "Hey Cindy!"
    identifier = start(world, "Cydney")
    for _ in range(2):
        assert world[0].inspect(identifier)["status"] == "failed"
    saved = state(world, identifier)
    assert saved["underlying_code"] == "speech_guard_pronunciation_unresolved"
    assert saved["speech_diagnostics"]["normalized_transcript"] == "hey cindy"
    assert saved["speech_diagnostics"]["policy_version"] == POLICY
    assert saved["preparation_retry_after"] is None
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("saved_method", OLD_METHODS)
def test_saved_old_pipeline_keeps_original_matcher_after_v4_selection(world, recognition, saved_method):
    world[0].verification_method = saved_method
    identifier = start(world, "Sherri")
    world[0].verification_method = METHOD
    recognition["text"] = "Hey Sherry!"
    assert world[0].submit("Sherri", "job") == identifier
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier)["verification_method"] == saved_method
    assert state(world, identifier)["underlying_code"] == "speech_guard_name_mismatch"
    assert len(paid_calls(world, provider.TTS_MODEL)) == 1
    assert not paid_calls(world, provider.MODEL)


@pytest.mark.parametrize("stage", ["failed", "cancelled", "uncertain"])
def test_terminal_pipeline_cannot_be_reinterpreted_under_v4(world, recognition, stage):
    identifier = start(world)
    world[0]._update(identifier, {"stage": stage})
    world[0].verification_method = METHOD
    before = copy.deepcopy(state(world, identifier))
    assert world[0].submit("Sue", "job") == identifier
    assert world[0].inspect(identifier)["status"] == "failed"
    assert state(world, identifier) == before
    assert len(paid_calls(world)) == 1
