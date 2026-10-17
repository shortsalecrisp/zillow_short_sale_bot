"""Pinned lexical equivalence for the parent-side, opt-in v4 speech guard.

This module never recognizes or generates audio. Its verdict must be combined
with the existing unprompted audio recognition, confidence and media proof.
"""

from functools import lru_cache
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
from types import MappingProxyType
import unicodedata

from video_followup_pronunciation_data import LEXICON_MANIFEST, MATCHING_CONFIG

_PATH = Path(__file__).resolve().parent / "data/video_pronunciation/unique-phones-v1.json.gz"
_WORD = r"[^\W\d_]+(?:[-'][^\W\d_]+){0,2}"
_APOSTROPHES = str.maketrans({"\u2018": "'", "\u2019": "'", "\u02bc": "'"})
_QUOTES = str.maketrans({"\u201c": '"', "\u201d": '"'})


class PronunciationDataError(ValueError):
    """Pinned lexical data is unavailable or altered; never a retry permission."""


@lru_cache(maxsize=1)
def _load_lexicon():
    try:
        with _PATH.open("rb") as stream:
            compressed = stream.read(LEXICON_MANIFEST["gzip_bytes"] + 1)
        if (len(compressed) != LEXICON_MANIFEST["gzip_bytes"]
                or hashlib.sha256(compressed).hexdigest() != LEXICON_MANIFEST["gzip_sha256"]):
            raise ValueError()
        with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
            raw = stream.read(LEXICON_MANIFEST["json_bytes"] + 1)
        if (len(raw) != LEXICON_MANIFEST["json_bytes"]
                or hashlib.sha256(raw).hexdigest() != LEXICON_MANIFEST["json_sha256"]):
            raise ValueError()
        words = json.loads(raw)
        if not isinstance(words, dict) or len(words) != LEXICON_MANIFEST["entries"]:
            raise ValueError()
        return MappingProxyType(words)
    except (OSError, ValueError, TypeError, EOFError):
        raise PronunciationDataError("pronunciation_lexicon_integrity") from None


def preflight_lexicon():
    _load_lexicon()
    return dict(LEXICON_MANIFEST)


def normalize_greeting(text):
    if not isinstance(text, str) or not 0 < len(text) <= MATCHING_CONFIG["max_transcript_chars"]:
        return None
    text = unicodedata.normalize("NFKC", text).translate(_APOSTROPHES).translate(_QUOTES).casefold()
    match = re.fullmatch(r'''[\s"'.,!?]*hey[\s,.:;!?]+(''' + _WORD + r'''(?:\s+''' + _WORD + r''')*)[\s"'.,!?]*''', text)
    if not match:
        return None
    tokens = match.group(1).split()
    if (len(tokens) > MATCHING_CONFIG["max_tail_tokens"]
            or any(len(token) > MATCHING_CONFIG["max_token_chars"] for token in tokens)
            or (len(tokens) > 1 and "hey" in tokens)):
        return None
    return "hey " + " ".join(tokens)


def compare_pronunciation(name, normalized_phrase):
    if (not isinstance(name, str) or not 1 < len(name) <= 32
            or not re.fullmatch(_WORD, name)
            or not isinstance(normalized_phrase, str)
            or normalize_greeting(normalized_phrase) != normalized_phrase):
        return "unresolved"
    folded = unicodedata.normalize("NFKC", name).translate(_APOSTROPHES).casefold()
    lexicon = _load_lexicon()
    expected = lexicon.get(folded)
    observed = [lexicon.get(token) for token in normalized_phrase[4:].split()]
    if not expected or not all(observed):
        return "unresolved"
    phones = " ".join(observed)
    if (len(expected.split()) > MATCHING_CONFIG["max_sequence_phones"]
            or len(phones.split()) > MATCHING_CONFIG["max_sequence_phones"]):
        return "unresolved"
    return "equivalent" if expected == phones else "unresolved"
