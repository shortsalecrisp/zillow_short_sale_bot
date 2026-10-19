"""Future-only evidence binding; all provider, recognizer and runtime calls mocked."""
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import video_followup_same_take as provider
import video_followup_speech_guard as speech
from test_video_followup_same_take import world, planned, no_network, start, state, paid_calls


def descriptor(world, identifier):
    return json.loads(world[1].objects.read(f"assets/{identifier}/evidence.json")[0])


@pytest.mark.parametrize("method,count,lexical", [
    (speech.PLANNED_AUDIO_VERIFICATION_METHOD, 1, False),
    (speech.PRONUNCIATION_VERIFICATION_METHOD, 2, False),
    (speech.GENERAL_PRONUNCIATION_VERIFICATION_METHOD, 3, True),
    (speech.REVIEWED_VARIANT_VERIFICATION_METHOD, 4, True)])
def test_full_speech_chain_preserves_every_existing_digest(method, count, lexical):
    result = provider._speech_configuration(method)
    assert len(result["configuration_chain"]) == count
    assert bool(result["lexicon_manifest"]) is lexical
    assert result["config_sha256"] == speech.PLANNED_VERIFICATION_PROFILES[method]["speech_config_sha256"]
    for item in result["configuration_chain"]:
        assert provider._digest_json(item["config"]) == item["sha256"]
    recognition = result["configuration_chain"][0]["config"]
    assert recognition["decoding"] == speech.DECODING
    assert recognition["minimum_logprob"] == -1.0
    assert recognition["maximum_no_speech"] == 0.6
    assert recognition["preprocessing"] == {"silence_padding_ms": 0}
    assert recognition["packages"] == speech.PACKAGE_VERSIONS
    recognition["decoding"]["beam_size"] = 999
    assert speech.DECODING["beam_size"] == 5


def test_create_once_descriptor_chains_into_plan_original_and_final_proof(world, planned):
    identifier = start(world)
    saved = state(world, identifier)
    evidence = descriptor(world, identifier)
    binding = provider._evidence_binding(saved)
    assert binding["evidence_sha256"] == provider._digest_json(evidence)
    assert evidence["identity"]["job_key"] == "job"
    assert evidence["identity"]["first_name"] == "Sue"
    assert evidence["runtime"] == provider._runtime_identity.return_value
    assert evidence["source"] == provider._source_identity.return_value
    original_bytes = world[1].objects.read(f"assets/{identifier}/evidence.json")[0]
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    result = world[0].prepare_media(snapshot, "Sue")
    saved = state(world, identifier)
    bundle = world[0]._read_audio_plan(saved)["bundle"]
    for value in (bundle, saved["greeting_speech_proof"], result["speech_proof"]):
        assert {key: value[key] for key in binding} == binding
    assert result["speech_proof"]["audio_lineage"]["speech_proof_sha256"] == provider._digest_json(saved["greeting_speech_proof"])
    assert planned[0].call_args.kwargs["ffmpeg"] == "/test/ffmpeg"
    assert planned[1].call_args.kwargs["ffmpeg"] == "/test/ffmpeg"
    assert world[1].objects.read(f"assets/{identifier}/evidence.json")[0] == original_bytes
    assert len(json.dumps(binding)) < 300
    assert len(original_bytes) < 64000
    assert saved["estimated_cents"] == 9
    assert len(paid_calls(world)) == 2
    assert provider.verify_greeting_words.call_count == 1


@pytest.mark.parametrize("missing", [*provider.EVIDENCE_FIELDS, "all"])
def test_deleting_marker_or_binding_cannot_downgrade_new_pipeline(world, planned, missing):
    identifier = start(world)
    remove = set(provider.EVIDENCE_FIELDS) if missing == "all" else {missing}
    world[1].mutate("same-take/" + identifier, lambda old: {key: value for key, value in old.items() if key not in remove})
    world[2].request.reset_mock()
    with pytest.raises(speech.SpeechGuardError):
        world[0].submit("Sue", "job")
    world[2].request.assert_not_called()


@pytest.mark.parametrize("field", provider.EVIDENCE_FIELDS)
def test_binding_is_immutable_to_pipeline_updates(world, planned, field):
    identifier = start(world)
    before = state(world, identifier)
    with pytest.raises(provider.VideoProviderError):
        world[0]._update(identifier, {field: before[field]})
    assert state(world, identifier) == before


@pytest.mark.parametrize("change", ["missing", "bytes", "config", "identity", "version"])
def test_descriptor_tampering_fails_closed_even_after_rehash(world, planned, change):
    identifier = start(world)
    key = f"assets/{identifier}/evidence.json"
    content, generation = world[1].objects.read(key)
    if change == "missing":
        world[1].objects.delete(key, generation)
    elif change == "bytes":
        world[1].objects.write(key, content + b" ", generation)
    else:
        value = json.loads(content)
        if change == "config":
            value["speech"]["configuration_chain"][0]["config"]["minimum_logprob"] = -99
        elif change == "identity":
            value["identity"]["first_name"] = "Jim"
        else:
            value["version"] = "future-unapproved"
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        world[1].objects.write(key, raw, generation)
        world[1].mutate("same-take/" + identifier, lambda old: {**old, "evidence_sha256": hashlib.sha256(raw).hexdigest()})
    with pytest.raises(speech.SpeechGuardError):
        world[0]._read(identifier)
    assert len(paid_calls(world)) == 1


@pytest.mark.parametrize("part", ["runtime", "source"])
def test_observed_runtime_or_source_drift_blocks_lipsync_without_retry(world, planned, part):
    identifier = start(world)
    probe = provider._runtime_identity if part == "runtime" else provider._source_identity
    probe.return_value = {**copy.deepcopy(probe.return_value), "drift": True}
    if part == "source":
        probe.return_value["files_sha256"] = {"fixture.py": "c" * 64}
    result = world[0].inspect(identifier)
    assert result["status"] == "failed"
    assert state(world, identifier)["underlying_code"] == "speech_guard_dependency_mismatch"
    assert state(world, identifier)["preparation_attempts"] == 1
    assert not paid_calls(world, provider.MODEL)
    provider.verify_greeting_words.assert_not_called()


def test_unrelated_revision_preserves_provenance_without_blocking_identical_source_bytes(world, planned):
    identifier = start(world)
    original = descriptor(world, identifier)
    provider._source_identity.return_value = {**provider._source_identity.return_value,
                                              "reported_revision": "f" * 40}
    assert world[0].inspect(identifier)["status"] == "processing"
    result = world[0].prepare_media(world[0].inspect(identifier), "Sue")
    assert result["speech_proof"]["evidence_sha256"] == provider._digest_json(original)
    assert descriptor(world, identifier)["source"]["reported_revision"] == "a" * 40


@pytest.mark.parametrize("target", ["plan", "original", "final"])
def test_binding_removal_rejects_every_proof_boundary(world, planned, target):
    identifier = start(world)
    world[0].inspect(identifier)
    snapshot = world[0].inspect(identifier)
    saved = state(world, identifier)
    if target == "plan":
        key = f"assets/{identifier}/prepared-plan.json"
        raw, generation = world[1].objects.read(key)
        bundle = json.loads(raw)
        bundle.pop("evidence_sha256")
        raw = json.dumps(bundle, sort_keys=True, separators=(",", ":")).encode()
        world[1].objects.write(key, raw, generation)
        world[1].mutate("same-take/" + identifier, lambda old: {**old, "audio_plan_sha256": hashlib.sha256(raw).hexdigest()})
        with pytest.raises(speech.SpeechGuardError):
            world[0]._read_audio_plan(state(world, identifier))
    elif target == "original":
        proof = dict(saved["greeting_speech_proof"])
        proof.pop("evidence_sha256")
        world[1].mutate("same-take/" + identifier, lambda old: {**old, "greeting_speech_proof": proof})
        with pytest.raises(speech.SpeechGuardError):
            world[0].prepare_media(snapshot, "Sue")
    else:
        proof = world[0].prepare_media(snapshot, "Sue")["speech_proof"]
        proof.pop("evidence_sha256")
        assert not speech.validate_speech_proof(proof, first_name="Sue", media_sha256=hashlib.sha256(b"output").hexdigest())
    assert len(paid_calls(world)) == 2


def test_existing_planned_pipeline_is_not_backfilled_or_reclassified(world, planned):
    identifier = start(world)
    key = f"assets/{identifier}/evidence.json"
    _, generation = world[1].objects.read(key)
    world[1].objects.delete(key, generation)
    world[1].mutate("same-take/" + identifier, lambda old: {key: value for key, value in old.items() if key not in provider.EVIDENCE_FIELDS})
    historical = state(world, identifier)
    provider._runtime_identity.reset_mock()
    assert world[0].submit("Sue", "job") == identifier
    assert state(world, identifier) == historical
    world[0].inspect(identifier)
    result = world[0].prepare_media(world[0].inspect(identifier), "Sue")
    assert not set(provider.EVIDENCE_FIELDS).intersection(result["speech_proof"])
    assert world[1].objects.read(key)[0] is None
    provider._runtime_identity.assert_not_called()


def test_evidence_preflight_failure_precedes_new_paid_request(world, planned, monkeypatch):
    monkeypatch.setattr(provider, "_runtime_identity", Mock(side_effect=speech.SpeechGuardError("speech_guard_dependency_mismatch")))
    with pytest.raises(speech.SpeechGuardError):
        start(world)
    assert not paid_calls(world)
    assert not world[1].objects.list("same-take/")


def test_runtime_probe_is_metadata_only_and_no_name_prompt():
    assert "WhisperModel" not in provider._RUNTIME_PROBE
    assert "transcribe" not in provider._RUNTIME_PROBE
    assert "expected_name" not in provider._RUNTIME_PROBE
    assert "importlib.metadata.distribution" in provider._RUNTIME_PROBE
    assert "h.update(chunk)" in provider._RUNTIME_PROBE


def test_runtime_fingerprint_cache_revalidates_stat_identity(tmp_path, monkeypatch):
    python, ffmpeg, package = (tmp_path / name for name in ("python", "ffmpeg", "package.py"))
    for path in (python, ffmpeg, package):
        path.write_bytes(b"fixture")
    model = tmp_path / "model"
    model.mkdir()
    for name in ("manifest.json", *speech.MODEL_HASHES):
        (model / name).write_bytes(b"fixture")
    monkeypatch.setattr(speech, "verify_model_assets", Mock())
    monkeypatch.setattr(provider, "_ffmpeg_executable", lambda: str(ffmpeg))
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if command[-1] == "-version":
            return SimpleNamespace(stdout=b"ffmpeg version fixture\n", stderr=b"")
        result = {"packages": speech.PACKAGE_VERSIONS,
            "python": {"sha256": provider._file_sha256(python)},
            "package_files_sha256": provider._file_sha256(package), "package_file_count": 1,
            "file_guards": {str(package): provider._stat_identity(package)}}
        return SimpleNamespace(stdout=json.dumps(result).encode(), stderr=b"")
    monkeypatch.setattr(provider.subprocess, "run", run)
    provider._RUNTIME_CACHE.clear()
    paths = {"python": str(python), "model_dir": str(model)}
    first = provider._runtime_identity(paths)
    assert provider._runtime_identity(paths) == first
    assert len(calls) == 2
    first["decoder"]["sha256"] = "caller mutation"
    assert provider._runtime_identity(paths)["decoder"]["sha256"] != "caller mutation"
    package.write_bytes(b"changed installed package")
    changed = provider._runtime_identity(paths)
    assert len(calls) == 4
    assert changed["recognizer"]["package_files_sha256"] == provider._file_sha256(package)
    provider._RUNTIME_CACHE.clear()


def test_linux_decoder_fingerprint_hashes_actual_bounded_shared_libraries(tmp_path, monkeypatch):
    library = tmp_path / "libavcodec.so.59"
    library.write_bytes(b"actual decoder library")
    monkeypatch.setattr(provider.subprocess, "run", Mock(return_value=SimpleNamespace(
        returncode=0, stdout=f"libavcodec.so.59 => {library} (0x1234)\n".encode(), stderr=b"")))
    value, guards = provider._decoder_linkage("/test/ffmpeg", "Linux")
    assert value["mode"] == "linux-observed-shared-libraries-v1"
    assert value["files"] == [{"path": str(library), "realpath": str(library.resolve()),
                               "sha256": hashlib.sha256(library.read_bytes()).hexdigest()}]
    assert guards[str(library)] == provider._stat_identity(library)


def test_linux_decoder_aslr_addresses_do_not_change_immutable_identity(tmp_path, monkeypatch):
    library = tmp_path / "libavcodec.so.59"
    library.write_bytes(b"same actual library bytes")
    probe = Mock(side_effect=[SimpleNamespace(returncode=0, stderr=b"", stdout=(
        f"linux-vdso.so.1 (0x{address})\nlibavcodec.so.59 => {library} (0x{address})\n").encode())
        for address in ("123abc", "456def")])
    monkeypatch.setattr(provider.subprocess, "run", probe)
    assert provider._decoder_linkage("/test/ffmpeg", "Linux")[0] == provider._decoder_linkage("/test/ffmpeg", "Linux")[0]


@pytest.mark.parametrize("output,code", [(b"libavcodec => not found", 0), (b"", 0),
    (b"unexpected failure", 1), (b"x" * 65537, 0),
    (b"\n".join(f"/lib/library{n}.so".encode() for n in range(257)), 0)])
def test_linux_decoder_fingerprint_fails_closed_on_unknown_or_unbounded_linkage(monkeypatch, output, code):
    monkeypatch.setattr(provider.subprocess, "run", Mock(return_value=SimpleNamespace(
        returncode=code, stdout=output, stderr=b"")))
    with pytest.raises(ValueError):
        provider._decoder_linkage("/test/ffmpeg", "Linux")


def test_non_linux_decoder_never_claims_linux_shared_library_parity(monkeypatch):
    run = Mock(side_effect=AssertionError("No alternate decoder probes"))
    monkeypatch.setattr(provider.subprocess, "run", run)
    assert provider._decoder_linkage("/test/ffmpeg", "Darwin") == ({
        "mode": "reported-runtime-versions-only", "linux_parity_claim": False}, {})
    run.assert_not_called()
