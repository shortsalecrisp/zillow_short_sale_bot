"""Offline tests for pinned shared phonetic data; never run speech recognition."""

import gzip
import hashlib
import json
from pathlib import Path
from types import MappingProxyType

import pytest

import video_followup_pronunciation as pronunciation
from video_followup_pronunciation_data import LEXICON_MANIFEST, MATCHING_CONFIG


@pytest.fixture(autouse=True)
def fresh_cache():
    pronunciation._load_lexicon.cache_clear()
    yield
    pronunciation._load_lexicon.cache_clear()


def test_exact_manifest_license_and_shared_data_bytes():
    root = Path(pronunciation.__file__).parent
    folder = root / "data/video_pronunciation"
    assert json.loads((folder / "manifest.json").read_text()) == LEXICON_MANIFEST
    compressed = (folder / "unique-phones-v1.json.gz").read_bytes()
    assert hashlib.sha256(compressed).hexdigest() == LEXICON_MANIFEST["gzip_sha256"]
    raw = gzip.decompress(compressed)
    assert hashlib.sha256(raw).hexdigest() == LEXICON_MANIFEST["json_sha256"]
    assert len(raw) == LEXICON_MANIFEST["json_bytes"]
    assert hashlib.sha256((folder / "LICENSE-CMUdict").read_bytes()).hexdigest() == LEXICON_MANIFEST["license_sha256"]
    assert pronunciation.preflight_lexicon() == LEXICON_MANIFEST
    assert len(pronunciation._load_lexicon()) == 116744
    assert MATCHING_CONFIG["infer_or_drop_phones"] is False


def test_unique_lexicon_immutable_and_ambiguous_entries_absent():
    words = pronunciation._load_lexicon()
    assert words["sydney"] == "S IH1 D N IY0"
    assert words["cindy"] == "S IH1 N D IY0"
    assert "a" not in words
    assert "katherine" not in words
    assert "cydney" not in words
    with pytest.raises(TypeError):
        words["cindy"] = words["sydney"]


@pytest.mark.parametrize("name,phrase", [
    ("Marc", "hey mark"), ("Mark", "hey marc"), ("Shelly", "hey shelley"),
    ("Shelley", "hey shelly"), ("Sydney", "hey sidney"), ("Sidney", "hey sydney"),
    ("Brook", "hey brooke"), ("Nikki", "hey nicky"), ("Cheryl", "hey sheryl"),
])
def test_complete_unique_stressed_homophones(name, phrase):
    assert pronunciation.compare_pronunciation(name, phrase) == "equivalent"


@pytest.mark.parametrize("name,phrase", [
    ("Sydney", "hey cindy"), ("Cindy", "hey sydney"), ("Brook", "hey brooks"),
    ("Brook", "hey brock"), ("Sue", "hey susan"), ("Kathryn", "hey kathy"),
    ("Marc", "hey mark again"), ("Marc", "hey mark mark"), ("Elise", "hey a lease"),
    ("Sonia", "hey sonya"), ("Jerrick", "hey jerrick"), ("Cydney", "hey sydney"),
    ("Katherine", "hey kathryn"), ("__proto__", "hey mark"), ("Marc", "Hey Mark!"),
    ("Marc", "hey mark unknownunlisted"), (None, "hey mark"), ("Marc", None),
])
def test_differences_and_missing_lexical_evidence_are_unresolved(name, phrase):
    assert pronunciation.compare_pronunciation(name, phrase) == "unresolved"


@pytest.mark.parametrize("text,expected", [
    ("Hey, MARK!", "hey mark"), ("  HEY An A.  ", "hey an a"),
    ("Hey O\u2019Neil", "hey o'neil"), ("\u201cHey Mark!\u201d", "hey mark"),
    ("Hey\nMark", "hey mark"), ("Hey Hey", "hey hey"),
    ("Hey Mary-Jane", "hey mary-jane"), ("Hey Brook Brook", "hey brook brook"),
])
def test_normalization_preserves_all_tail_words(text, expected):
    assert pronunciation.normalize_greeting(text) == expected


@pytest.mark.parametrize("text", [
    None, 5, "", "Mark", "Hi Mark", "Hey Mark 2", "Hey Mark_again", "Hey Mark! Hey Mark!",
    "Hey Hey Mark", "Hey Mark Hey", "Hey Mark Sue Again Now Yes", "Hey " + "a" * 33,
    "Hey Mark; Sue", "Hey Mark/Sue", "Hey Mark@example.com", "Hey <Mark>",
    "Hey " + "a" * 513,
])
def test_invalid_or_extra_greeting_format_never_normalized_away(text):
    assert pronunciation.normalize_greeting(text) is None


def test_word_boundary_equality_and_repeated_tokens_compare_every_phone(monkeypatch):
    words = MappingProxyType({"anna": "AE1 N AH0", "an": "AE1 N", "a": "AH0",
        "coco": "K OW1 K OW1", "co": "K OW1", "extra": "EH1 K S T R AH0"})
    monkeypatch.setattr(pronunciation, "_load_lexicon", lambda: words)
    assert pronunciation.compare_pronunciation("Anna", "hey an a") == "equivalent"
    assert pronunciation.compare_pronunciation("Coco", "hey co co") == "equivalent"
    assert pronunciation.compare_pronunciation("Anna", "hey an a extra") == "unresolved"


@pytest.mark.parametrize("other", ["B R UH0 K", "B R UH2 K", "B R AH1 K", "B R UH1", "P R UH1 K", "B R UH1 K S"])
def test_stress_and_all_phonemes_remain_required(monkeypatch, other):
    monkeypatch.setattr(pronunciation, "_load_lexicon", lambda: {"brook": "B R UH1 K", "other": other})
    assert pronunciation.compare_pronunciation("Brook", "hey other") == "unresolved"


def test_phone_length_bound_never_truncates_into_match(monkeypatch):
    monkeypatch.setattr(pronunciation, "_load_lexicon", lambda: {"name": "B " * 65, "other": "B " * 65})
    assert pronunciation.compare_pronunciation("Name", "hey other") == "unresolved"


@pytest.mark.parametrize("kind", ["missing", "empty", "corrupt", "overlong"])
def test_missing_or_modified_data_fails_closed(tmp_path, monkeypatch, kind):
    path = tmp_path / "lexicon.gz"
    if kind != "missing":
        source = pronunciation._PATH.read_bytes()
        path.write_bytes({"empty": b"", "corrupt": b"X" + source[1:], "overlong": source + b"X"}[kind])
    monkeypatch.setattr(pronunciation, "_PATH", path)
    with pytest.raises(pronunciation.PronunciationDataError, match="integrity"):
        pronunciation.preflight_lexicon()


def test_preflight_manifest_copy_does_not_mutate_pinned_identity():
    result = pronunciation.preflight_lexicon()
    result["json_sha256"] = "0" * 64
    assert pronunciation.preflight_lexicon()["json_sha256"] == LEXICON_MANIFEST["json_sha256"]
