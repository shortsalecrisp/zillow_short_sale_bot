"""Offline stored-diagnostic triage, never an audio verdict or retry plan."""

import argparse
from collections import Counter
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import video_followup_pronunciation as lexical
import video_followup_speech_guard as speech


def analyze(findings):
    rows = findings["rows"]
    identities = [row["outbox_row"] for row in rows]
    if len(rows) != 46 or len(set(identities)) != 46 or set(identities) != set(findings["selected_exact_text_receipts"]):
        raise ValueError("Require the complete unchanged exact-46 receipt set")
    lexical.preflight_lexicon()
    lexicon = lexical._load_lexicon()
    pitch, pronunciation = [], []
    for row in rows:
        profile = row.get("observed_pipeline_tuple_not_phase_qualification") or {}
        base = {"outbox_row": row["outbox_row"], "first_name": row["first_name"],
                "stored_method": profile.get("verification_method"),
                "stored_generator": profile.get("generator_profile")}
        if row["category"] == "generated_audio_pitch_rejected":
            qa = row.get("stored_pitch_measurements") or {}
            shift = qa.get("pitch_shift_semitones")
            numeric = type(shift) in (int, float) and math.isfinite(shift)
            pitch.append({**base, "shift_semitones": shift if numeric else None,
                          "diagnostic": ("below_reference_outside_guard" if shift < -3 else
                                         "above_reference_outside_guard" if shift > 3 else
                                         "contradictory_stored_diagnostic") if numeric else "missing_numeric_diagnostic"})
        if row["category"] == "name_or_pronunciation_rejected":
            diag = row.get("stored_recognition_diagnostics_not_actual_listening") or {}
            transcript = diag.get("observed_transcript")
            phrase = speech._policy_normalized_phrase(transcript, speech.REVIEWED_VARIANT_POLICY_VERSION)
            if not transcript:
                reason = "missing_stored_transcript"
            elif not phrase:
                reason = "whole_greeting_shape_unresolved"
            elif speech._policy_phrase_matches(row["first_name"], phrase, speech.REVIEWED_VARIANT_POLICY_VERSION):
                reason = "already_covered_by_current_pure_text_policy_not_audio_credit"
            elif row["first_name"].casefold() not in lexicon:
                reason = "canonical_lexicon_missing_or_ambiguous"
            elif any(token not in lexicon for token in phrase[4:].split()):
                reason = "observed_lexicon_missing_or_ambiguous"
            else:
                reason = "complete_stressed_phone_sequences_differ"
            pronunciation.append({**base, "stored_transcript_not_audio_judgment": transcript,
                                  "diagnostic": reason})
    return {"schema": "stored-quality-triage-v1", "input_receipts": 46,
            "scope": "Stored measurements and pure lexical comparison only; no recognition, decoding, listening, generation, retries or historical requalification.",
            "pitch_counts": dict(Counter(row["diagnostic"] for row in pitch)),
            "pronunciation_counts": dict(Counter(row["diagnostic"] for row in pronunciation)),
            "pitch": pitch, "pronunciation": pronunciation,
            "thresholds_policy_generator_and_jobs_unchanged": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("findings", type=Path)
    args = parser.parse_args()
    if args.findings.stat().st_size > 2_000_000:
        raise ValueError("Findings exceed the offline input bound")
    print(json.dumps(analyze(json.loads(args.findings.read_text())), indent=2))
