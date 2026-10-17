"""Offline lexical comparison only, not an audio verifier or production policy.

The dictionary describes pronunciations of written words. Comparing its entries
does not establish what a recording says, the speaker's accent, or the preferred
pronunciation of a particular person's name. No production module imports this
analyzer, and its results must never be used as production speech proofs.
"""

from dataclasses import dataclass, field
import hashlib
import itertools
import json
import math
from pathlib import Path
import re
from types import MappingProxyType
import unicodedata


CMUDICT_REVISION = "74790861f652b15e4ac49015a90074ad62a27690"
CMUDICT_SHA256 = "81917843c7f44ce2b094ac63873c2c7a4cf802040792c455ba3ca406891c3d22"
CMUDICT_LICENSE_SHA256 = "bd4ce8e44170a5f9f481310ca85c51de3c4f851a65e679b40e603b143bd3542a"
CMUDICT_URL = f"https://raw.githubusercontent.com/cmusphinx/cmudict/{CMUDICT_REVISION}/cmudict.dict"
CMUDICT_LICENSE_URL = f"https://raw.githubusercontent.com/cmusphinx/cmudict/{CMUDICT_REVISION}/LICENSE"
ANALYZER_VERSION = "offline-unique-lexical-phones-v1"
MAX_DICTIONARY_BYTES = 16 * 1024 * 1024
MAX_DICTIONARY_ROWS = 200000
MAX_LINE_CHARS = 1024
MAX_WORD_PHONES = 64
MAX_VARIANTS_PER_WORD = 64
MAX_TRANSCRIPT_CHARS = 512
MAX_TAIL_TOKENS = 4
MAX_COMBINATIONS = 64
MAX_SEQUENCE_PHONES = 64

_VOWELS = frozenset("AA AE AH AO AW AY EH ER EY IH IY OW OY UH UW".split())
_CONSONANTS = frozenset("B CH D DH F G HH JH K L M N NG P R S SH T TH V W Y Z ZH".split())
_PHONES = _CONSONANTS | frozenset(phone + stress for phone in _VOWELS for stress in "012")
_WORD = r"[^\W\d_]+(?:[-'][^\W\d_]+){0,2}"
_APOSTROPHES = str.maketrans({"\u2018": "'", "\u2019": "'", "\u02bc": "'"})
_QUOTES = str.maketrans({"\u201c": '"', "\u201d": '"'})
_RESERVED_NAMES = frozenset({"agent", "unknown", "there", "test"})
_CONFIG = MappingProxyType({
    "analyzer_version": ANALYZER_VERSION,
    "cmudict_revision": CMUDICT_REVISION,
    "cmudict_sha256": CMUDICT_SHA256,
    "rule": "both-unique-complete-phone-sequences-equal-preserving-stress",
    "name_shape": "current-single-name-word-with-up-to-two-apostrophes-or-hyphens",
    "observed_shape": "whole-hey-plus-bounded-complete-name-tail",
    "minimum_logprob": -1.0,
    "maximum_no_speech": 0.6,
    "max_confidence_segments": 8,
    "max_tail_tokens": MAX_TAIL_TOKENS,
    "max_combinations": MAX_COMBINATIONS,
    "max_sequence_phones": MAX_SEQUENCE_PHONES,
    "aliases": False,
    "g2p": False,
    "fuzzy_matching": False,
})
ANALYZER_CONFIG_SHA256 = hashlib.sha256(json.dumps(
    dict(_CONFIG), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


class LexiconError(ValueError):
    """A bounded dictionary could not be parsed or did not match its fixed pin."""


@dataclass(frozen=True)
class Lexicon:
    pronunciations: object
    source_sha256: str
    entry_count: int
    _source_pin_verified: bool = field(default=False, init=False, repr=False)

    def __post_init__(self):
        snapshot = {word: tuple(tuple(phones) for phones in variants)
                    for word, variants in self.pronunciations.items()}
        object.__setattr__(self, "pronunciations", MappingProxyType(snapshot))


def parse_cmudict_bytes(data):
    """Parse data without I/O. Unpinned fixtures remain identified as unpinned."""
    if not isinstance(data, bytes) or not 0 < len(data) <= MAX_DICTIONARY_BYTES:
        raise LexiconError("dictionary_size_invalid")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise LexiconError("dictionary_encoding_invalid") from None
    lines = text.splitlines()
    if len(lines) > MAX_DICTIONARY_ROWS:
        raise LexiconError("dictionary_row_limit")
    pronunciations = {}
    count = 0
    for line in lines:
        if len(line) > MAX_LINE_CHARS:
            raise LexiconError("dictionary_line_limit")
        line = line.strip()
        if not line or line.startswith((";;;", "#")):
            continue
        line = re.split(r"\s+#", line, maxsplit=1)[0]
        fields = line.split()
        if len(fields) < 2:
            raise LexiconError("dictionary_entry_invalid")
        spelling, phones = fields[0], tuple(fields[1:])
        variant = re.fullmatch(r"([^\s()]+)(?:\(([1-9][0-9]*)\))?", spelling)
        if not variant or len(variant.group(1)) > 128:
            raise LexiconError("dictionary_spelling_invalid")
        if not 1 <= len(phones) <= MAX_WORD_PHONES or any(phone not in _PHONES for phone in phones):
            raise LexiconError("dictionary_phones_invalid")
        word = unicodedata.normalize("NFKC", variant.group(1)).casefold()
        values = pronunciations.setdefault(word, [])
        if len(values) >= MAX_VARIANTS_PER_WORD:
            raise LexiconError("dictionary_variant_limit")
        values.append(phones)
        count += 1
    if not count:
        raise LexiconError("dictionary_empty")
    source_sha256 = hashlib.sha256(data).hexdigest()
    lexicon = Lexicon(pronunciations, source_sha256, count)
    object.__setattr__(lexicon, "_source_pin_verified", source_sha256 == CMUDICT_SHA256)
    return lexicon


def load_cmudict(path):
    """Read only the exact reviewed CMUdict data, never download or infer words."""
    with Path(path).open("rb") as source:
        data = source.read(MAX_DICTIONARY_BYTES + 1)
    if not data or len(data) > MAX_DICTIONARY_BYTES:
        raise LexiconError("dictionary_size_invalid")
    if hashlib.sha256(data).hexdigest() != CMUDICT_SHA256:
        raise LexiconError("dictionary_sha256_mismatch")
    return parse_cmudict_bytes(data)


def _expected_name(value):
    if not isinstance(value, str) or len(value) > MAX_TRANSCRIPT_CHARS:
        return None
    name = unicodedata.normalize("NFKC", value).translate(_APOSTROPHES).strip()
    if (not 1 < len(name) <= 32 or not re.fullmatch(_WORD, name)
            or name.casefold() in _RESERVED_NAMES):
        return None
    return name.casefold()


def _observed_tail(value):
    if not isinstance(value, str) or not 0 < len(value) <= MAX_TRANSCRIPT_CHARS:
        return None, "transcript_format_invalid"
    phrase = unicodedata.normalize("NFKC", value).translate(_APOSTROPHES).translate(_QUOTES).casefold()
    match = re.fullmatch(r'''[\s"'.,!?]*hey[\s,.:;!?]+(''' + _WORD + r'''(?:\s+''' + _WORD + r''')*)[\s"'.,!?]*''', phrase)
    if not match:
        return None, "transcript_format_invalid"
    tokens = tuple(match.group(1).split())
    if len(tokens) > MAX_TAIL_TOKENS or any(len(token) > 32 for token in tokens):
        return None, "observed_token_limit"
    if "hey" in tokens:
        return None, "repeated_greeting"
    return tokens, None


def _confidence_ok(value):
    if not isinstance(value, list) or not 1 <= len(value) <= 8:
        return False
    for segment in value:
        if not isinstance(segment, dict):
            return False
        logprob, silence = segment.get("avg_logprob"), segment.get("no_speech_prob")
        if (type(logprob) not in {int, float} or type(silence) not in {int, float}
                or not -1 <= logprob <= 0 or not 0 <= silence <= 0.6
                or not math.isfinite(logprob) or not math.isfinite(silence)):
            return False
    return True


def _sequences(tokens, lexicon):
    variants = []
    combinations = 1
    for token in tokens:
        entries = lexicon.pronunciations.get(token)
        if not entries:
            return [], "out_of_vocabulary"
        choices = tuple(dict.fromkeys(entries))
        combinations *= len(choices)
        if combinations > MAX_COMBINATIONS:
            return [], "pronunciation_combination_limit"
        variants.append(choices)
    # Bound the complete search before expansion; never pick the first variant.
    sequences = set()
    for combination in itertools.product(*variants):
        phones = tuple(phone for word in combination for phone in word)
        if len(phones) > MAX_SEQUENCE_PHONES:
            return [], "phone_sequence_limit"
        sequences.add(phones)
    return [list(phones) for phones in sorted(sequences)], None


def compare_greeting(name, transcript, confidence, lexicon):
    """Compare complete lexical pronunciations, without recognizing any audio.

    A candidate is a dictionary-level statement only. A lexical difference is
    not a finding that the actual recording says an incorrect name.
    """
    result = {
        "artifact_kind": "offline_lexical_pronunciation_analysis",
        "analyzer_version": ANALYZER_VERSION,
        "analyzer_config_sha256": ANALYZER_CONFIG_SHA256,
        "production_usable": False,
        "actual_audio_verified": False,
        "cadence_verified": False,
        "decision": "pronunciation_unresolved",
        "reason": "input_invalid",
        "canonical_expected_name": name if isinstance(name, str) and len(name) <= MAX_TRANSCRIPT_CHARS else None,
        "raw_observed_transcript": transcript if isinstance(transcript, str) and len(transcript) <= MAX_TRANSCRIPT_CHARS else None,
        "normalized_expected_name": None,
        "normalized_observed_tail": None,
        "expected_phone_sequences": [],
        "observed_phone_sequences": [],
        "confidence_within_existing_bounds": False,
        "limitations": ["Lexical data is not waveform evidence.",
                        "Dictionary coverage and pronunciations are not universal.",
                        "No individual or accent-specific pronunciation is established."],
    }
    if not isinstance(lexicon, Lexicon):
        result["reason"] = "lexicon_invalid"
        return result
    pinned = lexicon._source_pin_verified and lexicon.source_sha256 == CMUDICT_SHA256
    result["lexicon"] = {
        "source_sha256": lexicon.source_sha256, "entry_count": lexicon.entry_count,
        "pinned_cmudict": pinned, "revision": CMUDICT_REVISION if pinned else None,
        "url": CMUDICT_URL if pinned else None,
        "license_url": CMUDICT_LICENSE_URL if pinned else None,
        "license_sha256": CMUDICT_LICENSE_SHA256 if pinned else None,
    }
    expected = _expected_name(name)
    if expected is None:
        result["reason"] = "expected_name_format_invalid"
        return result
    result["normalized_expected_name"] = expected
    observed, error = _observed_tail(transcript)
    if error:
        result["reason"] = error
        return result
    result["normalized_observed_tail"] = " ".join(observed)
    if not _confidence_ok(confidence):
        result["reason"] = "confidence_unresolved"
        return result
    result["confidence_within_existing_bounds"] = True
    expected_phones, expected_error = _sequences((expected,), lexicon)
    observed_phones, observed_error = _sequences(observed, lexicon)
    result["expected_phone_sequences"] = expected_phones
    result["observed_phone_sequences"] = observed_phones
    if expected_error or observed_error:
        result["reason"] = "expected_" + expected_error if expected_error else "observed_" + observed_error
    elif len(expected_phones) != 1 or len(observed_phones) != 1:
        result["reason"] = "ambiguous_lexical_pronunciations"
    elif expected_phones[0] == observed_phones[0]:
        result["decision"] = "same_phonemes_candidate"
        result["reason"] = "unique_complete_phone_sequences_equal"
    else:
        result["decision"] = "different_lexical_pronunciations"
        result["reason"] = "unique_complete_phone_sequences_differ_not_audio_verdict"
    return result
