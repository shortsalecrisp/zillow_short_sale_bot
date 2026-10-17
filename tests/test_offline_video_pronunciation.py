"""Pure offline lexical analysis tests; no model, provider or network work."""

import ast
from dataclasses import FrozenInstanceError
import hashlib
import math
from pathlib import Path

import pytest

from scripts import offline_video_pronunciation as analyzer
from video_followup_speech_guard import validate_speech_proof


CONFIDENCE = [{"avg_logprob": -0.5, "no_speech_prob": 0.01}]
WORDS = b"""brook B R UH1 K
brooke B R UH1 K
cydney S IH1 D N IY0
sydney S IH1 D N IY0
cindy S IH1 N D IY0
sue S UW1
soon S UW1 N
thanks TH AE1 NG K S
anna AE1 N AH0
an AE1 N
a AH0
o'neil OW0 N IY1 L
mary-jane M EH1 R IY0 JH EY1 N
"""


@pytest.fixture
def lexicon():
    return analyzer.parse_cmudict_bytes(WORDS)


def compare(lexicon, name="Brook", transcript="Hey Brooke!", confidence=None):
    return analyzer.compare_greeting(name, transcript, CONFIDENCE if confidence is None else confidence, lexicon)


def test_parser_preserves_stress_all_variants_and_inline_comments():
    data = b";;; header\n# header\nNAME N EY1 M # name comment\nname(2) N EY2 M\nname(3) N EY1 M\n"
    lexicon = analyzer.parse_cmudict_bytes(data)
    assert lexicon.pronunciations["name"] == (("N", "EY1", "M"), ("N", "EY2", "M"), ("N", "EY1", "M"))
    assert lexicon.entry_count == 3
    assert lexicon.source_sha256 == hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("data", [b"", b"# only comment\n", b"word\n", b"word(0) W ER1 D\n",
    b"word(two) W ER1 D\n", b"word W ER D\n", b"word W1 ER1 D\n", b"word W ER3 D\n",
    b"word W er1 D\n", b"word W ER1 SIL D\n", b"word W ER1 D \xff\n", "word W ER1 D"])
def test_parser_rejects_malformed_data(data):
    with pytest.raises(analyzer.LexiconError):
        analyzer.parse_cmudict_bytes(data)


def test_parser_keeps_dictionary_punctuation_not_aliases():
    value = analyzer.parse_cmudict_bytes(b"a. EY1\n'a EY1\na's EY1 Z\nword-word W ER1 D\n")
    assert set(value.pronunciations) == {"a.", "'a", "a's", "word-word"}


def test_lexicon_snapshot_is_deeply_immutable(lexicon):
    with pytest.raises(TypeError):
        lexicon.pronunciations["brook"] = (("B",),)
    with pytest.raises(TypeError):
        lexicon.pronunciations["brook"][0][0] = "P"
    with pytest.raises(FrozenInstanceError):
        lexicon.source_sha256 = "0" * 64
    source = {"brook": [["B", "R", "UH1", "K"]]}
    copied = analyzer.Lexicon(source, "0" * 64, 1)
    source["brook"][0][0] = "P"
    assert copied.pronunciations["brook"][0][0] == "B"


def test_fixed_loader_pin_and_no_override(tmp_path, monkeypatch):
    path = tmp_path / "dictionary"
    path.write_bytes(WORDS)
    with pytest.raises(analyzer.LexiconError, match="sha256_mismatch"):
        analyzer.load_cmudict(path)
    with pytest.raises(TypeError):
        analyzer.load_cmudict(path, expected_sha256=hashlib.sha256(WORDS).hexdigest())
    monkeypatch.setattr(analyzer, "CMUDICT_SHA256", hashlib.sha256(WORDS).hexdigest())
    assert analyzer.load_cmudict(path).entry_count == 13
    path.write_bytes(WORDS + b"different D IH1 F ER0 AH0 N T\n")
    with pytest.raises(analyzer.LexiconError, match="sha256_mismatch"):
        analyzer.load_cmudict(path)


def test_missing_dictionary_does_not_download(tmp_path):
    with pytest.raises(FileNotFoundError):
        analyzer.load_cmudict(tmp_path / "missing")


@pytest.mark.parametrize(("name", "transcript"), [("Brook", "Hey Brooke!"), ("CYDNEY", "Hey Sydney."),
    ("Sue", "HEY, Sue!"), ("O'Neil", "Hey O'Neil"), ("Mary-Jane", "Hey Mary-Jane!")])
def test_only_identical_unique_complete_phones_are_candidates(lexicon, name, transcript):
    result = compare(lexicon, name, transcript)
    assert result["decision"] == "same_phonemes_candidate"
    assert result["canonical_expected_name"] == name
    assert result["raw_observed_transcript"] == transcript
    assert result["expected_phone_sequences"] == result["observed_phone_sequences"]


@pytest.mark.parametrize("phones", ["B R UH0 K", "B R UH2 K", "B R AH1 K", "B R UH1",
    "P R UH1 K", "B R UH1 K S", "B R UH1 AH0 K"])
def test_stress_vowel_consonant_syllable_and_suffix_distinctions_preserved(phones):
    lexicon = analyzer.parse_cmudict_bytes(b"brook B R UH1 K\nother " + phones.encode() + b"\n")
    result = compare(lexicon, transcript="Hey Other")
    assert result["decision"] == "different_lexical_pronunciations"
    assert result["reason"].endswith("not_audio_verdict")


def test_cindy_is_not_cydney(lexicon):
    assert compare(lexicon, "Cydney", "Hey Cindy")["decision"] == "different_lexical_pronunciations"


def test_word_boundary_homophone_uses_every_phone(lexicon):
    result = compare(lexicon, "Anna", "Hey An A!")
    assert result["decision"] == "same_phonemes_candidate"
    assert result["normalized_observed_tail"] == "an a"
    assert result["observed_phone_sequences"] == [["AE1", "N", "AH0"]]


def test_repeated_word_boundary_homophone_uses_every_phone():
    lexicon = analyzer.parse_cmudict_bytes(b"coco K OW1 K OW1\nco K OW1\n")
    result = compare(lexicon, "Coco", "Hey Co Co!")
    assert result["decision"] == "same_phonemes_candidate"
    assert result["observed_phone_sequences"] == [["K", "OW1", "K", "OW1"]]


def test_wrong_repeated_tail_is_full_lexical_difference_not_truncated(lexicon):
    result = compare(lexicon, transcript="Hey Brooke Brooke!")
    assert result["decision"] == "different_lexical_pronunciations"
    assert result["observed_phone_sequences"] == [["B", "R", "UH1", "K"] * 2]


def test_extra_dictionary_word_is_never_discarded(lexicon):
    result = compare(lexicon, "Brook", "Hey Brooke Thanks!")
    assert result["decision"] == "different_lexical_pronunciations"
    assert result["observed_phone_sequences"] == [["B", "R", "UH1", "K", "TH", "AE1", "NG", "K", "S"]]
    assert result["actual_audio_verified"] is False


@pytest.mark.parametrize("extra", [b"brook(2) B R UH0 K\n", b"brooke(2) B R UH0 K\n",
    b"brook(2) B R UH0 K\nbrooke(2) B R UH0 K\n"])
def test_overlapping_ambiguous_pronunciations_never_choose_matching_variant(extra):
    result = compare(analyzer.parse_cmudict_bytes(WORDS + extra))
    assert result["decision"] == "pronunciation_unresolved"
    assert result["reason"] == "ambiguous_lexical_pronunciations"


def test_duplicate_identical_variants_preserved_but_not_false_ambiguity():
    lexicon = analyzer.parse_cmudict_bytes(WORDS + b"brook(2) B R UH1 K\n")
    assert len(lexicon.pronunciations["brook"]) == 2
    assert compare(lexicon)["decision"] == "same_phonemes_candidate"


@pytest.mark.parametrize(("name", "transcript", "reason"), [
    ("Unknownname", "Hey Brook", "expected_out_of_vocabulary"),
    ("Brook", "Hey Unknownname", "observed_out_of_vocabulary"),
    ("Brook", "Hey Brooke Unknownname", "observed_out_of_vocabulary"),
    ("Elise", "Hey Elise", "expected_out_of_vocabulary"),
])
def test_oov_including_exact_spelling_is_unresolved(lexicon, name, transcript, reason):
    result = compare(lexicon, name, transcript)
    assert result["decision"] == "pronunciation_unresolved"
    assert result["reason"] == reason


@pytest.mark.parametrize("name", [None, 12, True, "", "A", "Mary Ann", "Brook3", "Brook_", "-Brook",
    "Agent", "UNKNOWN", "there", "test", "A-B-C-D", "B" * 33])
def test_canonical_name_shape_remains_current_guard(lexicon, name):
    assert compare(lexicon, name)["reason"] == "expected_name_format_invalid"


@pytest.mark.parametrize("transcript", [None, 12, "", "Brook", "Hello Brook", "Hey", "Heybrook",
    "Hey Brook 1", "Hey Brook / Sue", "Hey Brook; Sue", "Hey Brook, Sue", "Hey <Brook>",
    "Hey Brooke! Thanks", "Hey Br00k", "Hey B_rook", "Hey Brook \U0001f600", "Hey " + "B" * 513,
    "Hey Brook Sue Anna An A", "Hey hey Brook", "Hey Brook Hey"])
def test_unparseable_repeated_greeting_or_overlong_tails_are_unresolved(lexicon, transcript):
    assert compare(lexicon, transcript=transcript)["decision"] == "pronunciation_unresolved"


@pytest.mark.parametrize("transcript", ["Hey, Brooke!", '"Hey Brooke."', "  Hey... Brooke??  ",
    "HEY\tBrooke", "\u201cHey Brooke!\u201d"])
def test_only_bounded_punctuation_and_whitespace_normalized(lexicon, transcript):
    assert compare(lexicon, transcript=transcript)["decision"] == "same_phonemes_candidate"


def test_curly_apostrophe_preserves_canonical_raw_identity(lexicon):
    result = compare(lexicon, "O\u2019Neil", "Hey O\u2019Neil")
    assert result["decision"] == "same_phonemes_candidate"
    assert result["canonical_expected_name"] == "O\u2019Neil"


@pytest.mark.parametrize("confidence", [[], [None], {}, "confident", [CONFIDENCE[0]] * 9,
    [{"avg_logprob": -1.001, "no_speech_prob": 0.1}], [{"avg_logprob": 0.01, "no_speech_prob": 0.1}],
    [{"avg_logprob": -0.1, "no_speech_prob": 0.60001}], [{"avg_logprob": -0.1, "no_speech_prob": -0.01}],
    [{"avg_logprob": True, "no_speech_prob": 0.1}], [{"avg_logprob": -0.1, "no_speech_prob": False}],
    [{"avg_logprob": "-0.1", "no_speech_prob": 0.1}], [{"no_speech_prob": 0.1}],
    [{"avg_logprob": math.nan, "no_speech_prob": 0.1}], [{"avg_logprob": -0.1, "no_speech_prob": math.inf}]])
def test_strict_existing_confidence_bounds(lexicon, confidence):
    result = compare(lexicon, confidence=confidence)
    assert result["decision"] == "pronunciation_unresolved"
    assert result["reason"] == "confidence_unresolved"


def test_confidence_none_is_unresolved(lexicon):
    assert analyzer.compare_greeting("Brook", "Hey Brooke", None, lexicon)["reason"] == "confidence_unresolved"


@pytest.mark.parametrize("field", ["avg_logprob", "no_speech_prob"])
@pytest.mark.parametrize("sign", [-1, 1])
def test_unbounded_integer_confidence_is_unresolved_not_exception(lexicon, field, sign):
    segment = {**CONFIDENCE[0], field: sign * 10 ** 1000}
    result = compare(lexicon, confidence=[segment])
    assert result["decision"] == "pronunciation_unresolved"
    assert result["reason"] == "confidence_unresolved"


@pytest.mark.parametrize("segments", [1, 8])
def test_confidence_inclusive_boundaries(lexicon, segments):
    result = compare(lexicon, confidence=[{"avg_logprob": -1.0, "no_speech_prob": 0.6}] * segments)
    assert result["decision"] == "same_phonemes_candidate"


def test_combination_limit_checked_before_product(monkeypatch):
    phones = ["AA0", "AA1", "AA2", "AE0", "AE1"]
    records = ["anna AE1 N AH0"]
    for word in ("one", "two", "three"):
        records.extend(f"{word}({index + 1}) {phone}" for index, phone in enumerate(phones))
    lexicon = analyzer.parse_cmudict_bytes(("\n".join(records) + "\n").encode())
    monkeypatch.setattr(analyzer.itertools, "product", lambda *args: (_ for _ in ()).throw(AssertionError("not bounded")))
    observed, error = analyzer._sequences(("one", "two", "three"), lexicon)
    assert observed == []
    assert error == "pronunciation_combination_limit"


def test_long_sequences_are_not_truncated_into_matches():
    lexicon = analyzer.parse_cmudict_bytes(b"brook B R UH1 K\none " + b"B " * 40 + b"\ntwo " + b"R " * 40 + b"\n")
    result = compare(lexicon, transcript="Hey One Two")
    assert result["reason"] == "observed_phone_sequence_limit"


def test_mutating_output_does_not_change_later_analysis(lexicon):
    first = compare(lexicon)
    first["expected_phone_sequences"][0][0] = "P"
    first["lexicon"]["source_sha256"] = "0" * 64
    first["limitations"].clear()
    second = compare(lexicon)
    assert second["expected_phone_sequences"][0][0] == "B"
    assert second["lexicon"]["source_sha256"] == lexicon.source_sha256
    assert len(second["limitations"]) == 3


@pytest.mark.parametrize("transcript", ["Hey Brooke", "Hey Cindy", "Hey Unknownname"])
def test_all_outcomes_are_nonproduction_and_rejected_as_proofs(lexicon, transcript):
    result = compare(lexicon, transcript=transcript)
    assert result["production_usable"] is result["actual_audio_verified"] is result["cadence_verified"] is False
    assert "accepted" not in result
    assert "verification_method" not in result
    assert "policy_version" not in result
    assert result["lexicon"]["pinned_cmudict"] is False
    assert result["lexicon"]["revision"] is None
    assert not validate_speech_proof(result, first_name="Brook", audio_sha256="1" * 64, media_sha256="2" * 64)


def test_invalid_lexicon_is_unresolved():
    assert analyzer.compare_greeting("Brook", "Hey Brooke", CONFIDENCE, {})["reason"] == "lexicon_invalid"


def test_supplied_sha_cannot_claim_verified_pinned_provenance(lexicon):
    declared = analyzer.Lexicon(lexicon.pronunciations, analyzer.CMUDICT_SHA256, lexicon.entry_count)
    result = compare(declared)
    assert result["lexicon"]["pinned_cmudict"] is False
    assert result["lexicon"]["revision"] is None
    with pytest.raises(TypeError):
        analyzer.Lexicon(lexicon.pronunciations, analyzer.CMUDICT_SHA256, lexicon.entry_count,
                         _source_pin_verified=True)


def test_parser_bounds(monkeypatch):
    for constant, value, payload in [("MAX_DICTIONARY_BYTES", 3, b"word W ER1 D\n"),
            ("MAX_DICTIONARY_ROWS", 1, b"word W ER1 D\nname N EY1 M\n"),
            ("MAX_LINE_CHARS", 3, b"word W ER1 D\n"),
            ("MAX_VARIANTS_PER_WORD", 1, b"word W ER1 D\nword(2) W ER0 D\n")]:
        with monkeypatch.context() as patch:
            patch.setattr(analyzer, constant, value)
            with pytest.raises(analyzer.LexiconError):
                analyzer.parse_cmudict_bytes(payload)


def test_fixed_provenance_and_no_production_or_network_imports():
    assert analyzer.CMUDICT_SHA256 == "81917843c7f44ce2b094ac63873c2c7a4cf802040792c455ba3ca406891c3d22"
    assert analyzer.CMUDICT_REVISION == "74790861f652b15e4ac49015a90074ad62a27690"
    source = ast.parse(Path(analyzer.__file__).read_text())
    imports = set()
    for node in ast.walk(source):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imports.add(node.module)
    assert imports == {"dataclasses", "hashlib", "itertools", "json", "math", "pathlib", "re", "types", "unicodedata"}
