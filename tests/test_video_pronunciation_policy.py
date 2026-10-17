"""Offline matching-contract tests, not evidence of generated audio quality."""

import copy
import hashlib
import io
import itertools
import wave

import pytest

import video_followup_speech_guard as guard


OLD_CONFIG = "b3a1b5798d53032e5c23da361c467141c1cfd502a8def78e8d3ddeaabde2322e"
OLD_PLANNED_CONFIG = "9f7fe677874406a34d5969c0c9f3a547225165764685805c5df50c97f3e831d6"
NEW_CONFIG = "bb77463c65987cdf07caef001a49d29cb70a93ee7372e7e9edcacd07fde5fea3"
NEW_PLANNED_CONFIG = "45aa4b4c344179321d82eaf66ab0c36bcaa7cf8f56b48b07f9b63423bc2a6cf5"


@pytest.fixture
def audio():
    output = io.BytesIO()
    with wave.open(output, "wb") as stream:
        stream.setnchannels(1)
        stream.setsampwidth(2)
        stream.setframerate(16000)
        stream.writeframes(b"\0\0" * 16000)
    return output.getvalue()


@pytest.fixture
def recognized(monkeypatch):
    result = {"transcript": "Hey Sydney!",
        "segments": [{"avg_logprob": -0.1, "no_speech_prob": 0.01}],
        "model_revision": guard.MODEL_REVISION, "config_sha256": OLD_CONFIG}
    calls = []
    monkeypatch.setattr(guard, "_speech_paths", lambda: {
        "python": "/offline/python", "model_dir": "/offline/model"})

    def recognize(content, python, model_dir):
        calls.append((content, python, model_dir))
        return copy.deepcopy(result)

    def forbidden(*args, **kwargs):
        pytest.fail("Offline contract tests must not execute a recognition child")

    monkeypatch.setattr(guard, "_recognize", recognize)
    monkeypatch.setattr(guard, "_run_child", forbidden)
    return result, calls


def issue(audio, name):
    return guard.verify_greeting_words(audio, name,
        policy_version=guard.PRONUNCIATION_POLICY_VERSION)


def stored_proof(name, *, policy=guard.POLICY_VERSION, observed=None):
    if policy == guard.LEGACY_PROOF_IDENTITY["policy_version"]:
        identity = guard.LEGACY_PROOF_IDENTITY
    else:
        identity = {"policy_version": policy, "model_id": guard.MODEL_ID,
            "model_revision": guard.MODEL_REVISION,
            "model_manifest_sha256": guard.MODEL_MANIFEST_SHA256,
            "config_sha256": NEW_CONFIG if policy == guard.PRONUNCIATION_POLICY_VERSION else OLD_CONFIG}
    return {"accepted": True, **identity, "expected_name": name,
        "normalized_transcript": "hey " + (observed or name.casefold()),
        "source_audio_sha256": "a" * 64,
        "confidence": [{"avg_logprob": -0.1, "no_speech_prob": 0.01}]}


def planned(proof, method):
    config = guard.PLANNED_VERIFICATION_PROFILES[method]
    return {**proof, "media_sha256": "b" * 64, "greeting_seconds": 1.5,
        "verification_method": method,
        "verification_config_sha256": guard._digest_json(config),
        "audio_lineage": {"accepted": True, "lineage_version": config["lineage_version"],
            "first_name": proof["expected_name"], "final_media_sha256": "b" * 64,
            "checked_audio_sha256": proof["source_audio_sha256"], "greeting_seconds": 1.5,
            "manifest_sha256": "1" * 64, "planned_audio_sha256": "2" * 64,
            "full_pcm_sha256": "3" * 64, "greeting_pcm_sha256": "4" * 64,
            "aac_track_sha256": "5" * 64, "speech_proof_sha256": guard._digest_json(proof)}}


def test_old_wire_config_and_both_planned_versions_are_literal_and_separate():
    assert guard.POLICY_VERSION == "spoken-greeting-tiny-en-v2"
    assert guard.CONFIG_SHA256 == OLD_CONFIG
    assert guard.SPELLING_EQUIVALENCE_GROUPS == (("kathryn", "katherine"), ("nikki", "nicky"))
    assert guard.PLANNED_AUDIO_VERIFICATION_CONFIG_SHA256 == OLD_PLANNED_CONFIG
    assert guard.PRONUNCIATION_POLICY_VERSION == "spoken-greeting-tiny-en-v3"
    assert guard.PRONUNCIATION_CONFIG_SHA256 == NEW_CONFIG
    assert guard.PRONUNCIATION_EQUIVALENCE_GROUPS == (
        ("kathryn", "katherine"), ("nikki", "nicky"), ("cydney", "sydney"), ("brook", "brooke"))
    assert guard.PRONUNCIATION_VERIFICATION_METHOD == "planned-aac-lineage-v2"
    assert guard.PRONUNCIATION_VERIFICATION_CONFIG_SHA256 == NEW_PLANNED_CONFIG
    assert guard.PLANNED_VERIFICATION_PROFILES == {
        "planned-aac-lineage-v1": guard.PLANNED_AUDIO_VERIFICATION_CONFIG,
        "planned-aac-lineage-v2": guard.PRONUNCIATION_VERIFICATION_CONFIG,
        "planned-aac-lineage-v3": guard.GENERAL_PRONUNCIATION_VERIFICATION_CONFIG}
    assert {key: value for key, value in guard.PRONUNCIATION_VERIFICATION_CONFIG.items()
            if key not in {"method", "speech_config_sha256"}} == {
        key: value for key, value in guard.PLANNED_AUDIO_VERIFICATION_CONFIG.items()
        if key not in {"method", "speech_config_sha256"}}
    assert guard.DECODING == {"language": "en", "task": "transcribe", "beam_size": 5,
        "temperature": 0.0, "condition_on_previous_text": False,
        "initial_prompt": None, "prefix": None, "hotwords": None,
        "vad_filter": False, "word_timestamps": False, "max_new_tokens": 64}


@pytest.mark.parametrize("first_name,observed", [
    (expected.upper(), observed) for group in (
        ("cydney", "sydney"), ("brook", "brooke"), ("kathryn", "katherine"), ("nikki", "nicky"))
    for expected, observed in itertools.permutations(group, 2)])
def test_v3_approved_pairs_preserve_observation_and_canonical_name(audio, recognized, first_name, observed):
    result, calls = recognized
    result["transcript"] = "Hey " + observed.title() + "!"
    proof = issue(audio, first_name)
    assert proof["policy_version"] == guard.PRONUNCIATION_POLICY_VERSION
    assert proof["config_sha256"] == NEW_CONFIG
    assert proof["expected_name"] == first_name
    assert proof["normalized_transcript"] == "hey " + observed
    assert guard.validate_speech_proof(proof, first_name=first_name,
        audio_sha256=hashlib.sha256(audio).hexdigest())
    assert not guard.validate_speech_proof(proof, first_name=observed.title())
    assert calls == [(audio, "/offline/python", "/offline/model")]


@pytest.mark.parametrize("name", ["CYDNEY", "Sydney", "Brook", "Brooke", "Cindy", "Jim", "D'Andre"])
def test_exact_names_still_pass_v3_without_any_alias_rewrite(audio, recognized, name):
    result, _ = recognized
    result["transcript"] = "Hey " + name + "!"
    proof = issue(audio, name)
    assert proof["expected_name"] == name
    assert proof["normalized_transcript"] == "hey " + name.casefold()


@pytest.mark.parametrize("name,transcript", [
    ("CYDNEY", "Hey Cindy!"), ("Sydney", "Hey Cindy!"), ("Cindy", "Hey Sydney!"),
    ("Cindy", "Hey Cydney!"), ("CYDNEY", "Hey Sidney!"), ("CYDNEY", "Hey Cyd!"),
    ("Brook", "Hey Brock!"), ("Brooke", "Hey Brooks!"), ("Brooke", "Hey Brooke-Sue!"),
    ("Marc", "Hey Mark!"), ("Kathryn", "Hey Catherine!"), ("Nikki", "Hey Nicole!"),
    ("CYDNEY", "Hey Brooke!"), ("Brook", "Hey Sydney!"),
    ("CYDNEY", "Sydney"), ("CYDNEY", "Hi Sydney"), ("CYDNEY", "Hey Sydney and Cindy"),
    ("CYDNEY", "Hey Sydney, it's Yoni"), ("CYDNEY", "Hey Sydney again"),
    ("CYDNEY", "Hey Sydney! Hey Sydney!"), ("CYDNEY", "Hey Sydney 2"),
    ("CYDNEY", "Hey Sydney@example.com"), ("CYDNEY", "Hey Sydney https://example.com"),
    ("CYDNEY", ""), ("CYDNEY", None), ("CYDNEY", ["Hey Sydney"]),
])
def test_v3_is_not_fuzzy_matching_and_requires_only_the_whole_greeting(audio, recognized, name, transcript):
    result, calls = recognized
    result["transcript"] = transcript
    with pytest.raises(guard.SpeechGuardError, match="speech_guard_name_mismatch") as caught:
        issue(audio, name)
    assert not guard.is_transient_speech_failure(caught.value)
    assert len(calls) == 1


@pytest.mark.parametrize("segments", [
    [], None, "invalid", [{}], [None],
    [{"avg_logprob": -1.00001, "no_speech_prob": 0.01}],
    [{"avg_logprob": -0.1, "no_speech_prob": 0.60001}],
    [{"avg_logprob": float("nan"), "no_speech_prob": 0.01}],
    [{"avg_logprob": -0.1, "no_speech_prob": float("inf")}],
    [{"avg_logprob": True, "no_speech_prob": False}],
    [{"avg_logprob": "-0.1", "no_speech_prob": "0.01"}],
    [{"avg_logprob": -0.1, "no_speech_prob": 0.01}] * 9,
    [{"avg_logprob": -0.1, "no_speech_prob": 0.01}, {"avg_logprob": -2, "no_speech_prob": 0.01}],
])
def test_v3_equivalence_never_relaxes_confidence_or_retries(audio, recognized, segments):
    result, calls = recognized
    result["segments"] = segments
    with pytest.raises(guard.SpeechGuardError, match="speech_guard_low_confidence") as caught:
        issue(audio, "CYDNEY")
    assert not guard.is_transient_speech_failure(caught.value)
    assert len(calls) == 1


def test_v3_retains_exact_inclusive_confidence_boundary(audio, recognized):
    result, _ = recognized
    result["segments"] = [{"avg_logprob": -1.0, "no_speech_prob": 0.6}]
    assert guard.validate_speech_proof(issue(audio, "CYDNEY"), first_name="CYDNEY")


@pytest.mark.parametrize("name,transcript", [("CYDNEY", "Hey Sydney!"), ("Brook", "Hey Brooke!")])
@pytest.mark.parametrize("explicit", [False, True])
def test_default_and_explicit_v2_do_not_reinterpret_old_failures(audio, recognized, name, transcript, explicit):
    result, _ = recognized
    result["transcript"] = transcript
    kwargs = {"policy_version": guard.POLICY_VERSION} if explicit else {}
    with pytest.raises(guard.SpeechGuardError, match="speech_guard_name_mismatch"):
        guard.verify_greeting_words(audio, name, **kwargs)


@pytest.mark.parametrize("policy", [None, "", "v3", "spoken-greeting-tiny-en-v99", {}, [],
    guard.LEGACY_PROOF_IDENTITY["policy_version"]])
def test_unsupported_issuance_policy_rejected_before_recognition(audio, recognized, policy):
    _, calls = recognized
    with pytest.raises(guard.SpeechGuardError, match="speech_guard_output_invalid"):
        guard.verify_greeting_words(audio, "CYDNEY", policy_version=policy)
    assert calls == []


@pytest.mark.parametrize("field,value", [
    ("config_sha256", NEW_CONFIG), ("config_sha256", "0" * 64),
    ("model_revision", "other-model"), ("model_revision", None)])
def test_new_policy_does_not_accept_changed_child_protocol(audio, recognized, field, value):
    result, _ = recognized
    result[field] = value
    with pytest.raises(guard.SpeechGuardError, match="speech_guard_output_invalid"):
        issue(audio, "CYDNEY")


@pytest.mark.parametrize("policy", [guard.LEGACY_PROOF_IDENTITY["policy_version"], guard.POLICY_VERSION])
@pytest.mark.parametrize("name,observed", [("CYDNEY", "sydney"), ("Brook", "brooke")])
def test_old_proofs_cannot_claim_new_pronunciation_equivalences(policy, name, observed):
    proof = stored_proof(name, policy=policy, observed=observed)
    assert not guard.validate_speech_proof(proof, first_name=name)


@pytest.mark.parametrize("policy", [guard.LEGACY_PROOF_IDENTITY["policy_version"],
    guard.POLICY_VERSION, guard.PRONUNCIATION_POLICY_VERSION])
def test_existing_exact_proofs_stay_valid_without_recognition_or_provider(monkeypatch, policy):
    def forbidden(*args, **kwargs):
        pytest.fail("Stored proof validation may not re-recognize, generate, or reopen anything")

    monkeypatch.setattr(guard, "_recognize", forbidden)
    monkeypatch.setattr(guard, "_speech_paths", forbidden)
    proof = stored_proof("CYDNEY", policy=policy)
    before = copy.deepcopy(proof)
    assert guard.validate_speech_proof(proof, first_name="CYDNEY", audio_sha256="a" * 64)
    assert proof == before


@pytest.mark.parametrize("field,value", [
    ("policy_version", guard.POLICY_VERSION),
    ("policy_version", guard.LEGACY_PROOF_IDENTITY["policy_version"]),
    ("config_sha256", OLD_CONFIG), ("config_sha256", "0" * 64),
    ("model_id", "different"), ("model_revision", "0" * 40),
    ("model_manifest_sha256", "0" * 64), ("expected_name", "Sydney"),
    ("expected_name", "Cydney"), ("normalized_transcript", "hey cindy"),
    ("normalized_transcript", "Hey Sydney!"), ("normalized_transcript", "hey sydney again"),
    ("source_audio_sha256", "0" * 64), ("accepted", "true"),
])
def test_v3_proof_binds_complete_identity_canonical_case_audio_and_observed_phrase(field, value):
    proof = stored_proof("CYDNEY", policy=guard.PRONUNCIATION_POLICY_VERSION, observed="sydney")
    proof[field] = value
    assert not guard.validate_speech_proof(proof, first_name="CYDNEY", audio_sha256="a" * 64)


@pytest.mark.parametrize("policy,method,accepted", [
    (guard.POLICY_VERSION, guard.PLANNED_AUDIO_VERIFICATION_METHOD, True),
    (guard.PRONUNCIATION_POLICY_VERSION, guard.PRONUNCIATION_VERIFICATION_METHOD, True),
    (guard.POLICY_VERSION, guard.PRONUNCIATION_VERIFICATION_METHOD, False),
    (guard.PRONUNCIATION_POLICY_VERSION, guard.PLANNED_AUDIO_VERIFICATION_METHOD, False),
    (guard.LEGACY_PROOF_IDENTITY["policy_version"], guard.PLANNED_AUDIO_VERIFICATION_METHOD, False),
    (guard.LEGACY_PROOF_IDENTITY["policy_version"], guard.PRONUNCIATION_VERIFICATION_METHOD, False),
])
def test_planned_method_and_speech_policy_are_an_exact_tuple(policy, method, accepted):
    proof = planned(stored_proof("CYDNEY", policy=policy), method)
    before = copy.deepcopy(proof)
    assert guard.validate_speech_proof(proof, first_name="CYDNEY", media_sha256="b" * 64) is accepted
    assert proof == before


def test_bare_v3_pre_render_greeting_proof_remains_valid_without_lineage():
    proof = stored_proof("CYDNEY", policy=guard.PRONUNCIATION_POLICY_VERSION, observed="sydney")
    assert guard.validate_speech_proof(proof, first_name="CYDNEY", audio_sha256="a" * 64)


@pytest.mark.parametrize("with_media_argument", [False, True])
@pytest.mark.parametrize("removed", [
    ("verification_method",), ("verification_config_sha256",), ("audio_lineage",),
    ("verification_method", "verification_config_sha256"),
    ("verification_method", "audio_lineage"),
    ("verification_config_sha256", "audio_lineage"),
    ("verification_method", "verification_config_sha256", "audio_lineage"),
])
def test_final_v3_proof_rejects_any_removed_lineage_fields(with_media_argument, removed):
    proof = planned(stored_proof("CYDNEY", policy=guard.PRONUNCIATION_POLICY_VERSION, observed="sydney"),
        guard.PRONUNCIATION_VERIFICATION_METHOD)
    for field in removed:
        proof.pop(field)
    kwargs = {"media_sha256": "b" * 64} if with_media_argument else {}
    assert not guard.validate_speech_proof(proof, first_name="CYDNEY", **kwargs)


@pytest.mark.parametrize("final_fields", [
    {"media_sha256": "b" * 64}, {"media_sha256": None},
    {"greeting_seconds": 1.5}, {"greeting_seconds": None},
    {"media_sha256": "b" * 64, "greeting_seconds": 1.5},
])
def test_any_final_v3_field_requires_lineage_even_without_media_argument(final_fields):
    proof = {**stored_proof("CYDNEY", policy=guard.PRONUNCIATION_POLICY_VERSION, observed="sydney"),
        **final_fields}
    assert not guard.validate_speech_proof(proof, first_name="CYDNEY")


@pytest.mark.parametrize("policy", [guard.LEGACY_PROOF_IDENTITY["policy_version"], guard.POLICY_VERSION])
@pytest.mark.parametrize("with_media_argument", [False, True])
def test_old_direct_final_proofs_do_not_gain_new_lineage_requirement(policy, with_media_argument):
    proof = {**stored_proof("CYDNEY", policy=policy), "media_sha256": "b" * 64, "greeting_seconds": 1.5}
    kwargs = {"media_sha256": "b" * 64} if with_media_argument else {}
    assert guard.validate_speech_proof(proof, first_name="CYDNEY", **kwargs)


@pytest.mark.parametrize("field,value", [
    ("verification_method", guard.PLANNED_AUDIO_VERIFICATION_METHOD),
    ("verification_method", None), ("verification_method", []),
    ("verification_config_sha256", OLD_PLANNED_CONFIG),
    ("verification_config_sha256", "0" * 64),
    ("policy_version", guard.POLICY_VERSION), ("config_sha256", OLD_CONFIG),
    ("greeting_seconds", 2.0), ("media_sha256", "c" * 64),
    ("normalized_transcript", "hey cydney"),
    ("confidence", [{"avg_logprob": -0.2, "no_speech_prob": 0.01}]),
    ("audio_lineage", None),
])
def test_new_planned_proof_cannot_mix_versions_or_change_frozen_recognition(field, value):
    proof = planned(stored_proof("CYDNEY", policy=guard.PRONUNCIATION_POLICY_VERSION, observed="sydney"),
        guard.PRONUNCIATION_VERIFICATION_METHOD)
    assert guard.validate_speech_proof(proof, first_name="CYDNEY", media_sha256="b" * 64)
    proof[field] = value
    assert not guard.validate_speech_proof(proof, first_name="CYDNEY", media_sha256="b" * 64)


@pytest.mark.parametrize("field,value", [
    ("first_name", "Sydney"), ("final_media_sha256", "c" * 64),
    ("checked_audio_sha256", "0" * 64), ("speech_proof_sha256", "0" * 64),
    ("lineage_version", "v2"), ("greeting_seconds", 2), ("accepted", False),
    ("manifest_sha256", "invalid"), ("full_pcm_sha256", None),
])
def test_new_planned_lineage_keeps_name_media_and_pcm_binding(field, value):
    proof = planned(stored_proof("CYDNEY", policy=guard.PRONUNCIATION_POLICY_VERSION, observed="sydney"),
        guard.PRONUNCIATION_VERIFICATION_METHOD)
    proof["audio_lineage"][field] = value
    assert not guard.validate_speech_proof(proof, first_name="CYDNEY", media_sha256="b" * 64)


@pytest.mark.parametrize("policy", [guard.POLICY_VERSION, guard.PRONUNCIATION_POLICY_VERSION])
def test_diagnostics_require_complete_policy_identity_and_retain_actual_observation(audio, recognized, policy):
    result, _ = recognized
    result["transcript"] = "Hey Cindy!"
    with pytest.raises(guard.SpeechGuardError) as caught:
        guard.verify_greeting_words(audio, "CYDNEY", policy_version=policy)
    details = guard.safe_speech_failure_diagnostics(caught.value)
    assert details["accepted"] is False
    assert details["policy_version"] == policy
    assert details["observed_transcript"] == "Hey Cindy!"
    assert details["normalized_transcript"] == "hey cindy"
    assert details["source_audio_sha256"] == hashlib.sha256(audio).hexdigest()
    details["config_sha256"] = NEW_CONFIG if policy == guard.POLICY_VERSION else OLD_CONFIG
    assert guard._safe_recognition_diagnostics(details) == {}


def test_v3_diagnostics_drop_arbitrary_text_secrets_and_nonfinite_numbers(audio, recognized):
    result, _ = recognized
    result.update(transcript="https://secret.example/token=abc", segments=[{
        "avg_logprob": float("nan"), "no_speech_prob": float("inf"), "api_key": "secret"}])
    with pytest.raises(guard.SpeechGuardError) as caught:
        issue(audio, "CYDNEY")
    details = guard.safe_speech_failure_diagnostics(caught.value)
    assert details == {"accepted": False, "policy_version": guard.PRONUNCIATION_POLICY_VERSION,
        "model_id": guard.MODEL_ID, "model_revision": guard.MODEL_REVISION,
        "model_manifest_sha256": guard.MODEL_MANIFEST_SHA256, "config_sha256": NEW_CONFIG,
        "source_audio_sha256": hashlib.sha256(audio).hexdigest(), "confidence": []}
