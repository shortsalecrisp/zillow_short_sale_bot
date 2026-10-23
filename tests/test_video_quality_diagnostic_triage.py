"""No model, decoder, network, storage mutation or provider work is allowed."""

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import pytest

SPEC = importlib.util.spec_from_file_location("quality_triage", Path(__file__).resolve().parents[1] /
                                           "scripts/triage_video_quality_diagnostics.py")
triage = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(triage)


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    for target in ("socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection",
                   "subprocess.Popen", "video_followup_speech_guard._recognize",
                   "video_followup_speech_guard._speech_paths"):
        monkeypatch.setattr(target, Mock(side_effect=AssertionError("diagnostics only")))


def findings(category="other", **extra):
    rows = [{"outbox_row": i, "first_name": "Jim", "category": "other"} for i in range(46)]
    rows[0].update(category=category, **extra)
    return {"rows": rows, "selected_exact_text_receipts": list(range(46))}


@pytest.mark.parametrize("shift,expected", [(3.0, "contradictory_stored_diagnostic"),
    (3.01, "above_reference_outside_guard"), (-3.01, "below_reference_outside_guard"),
    (None, "missing_numeric_diagnostic"), (True, "missing_numeric_diagnostic"),
    (float("nan"), "missing_numeric_diagnostic")])
def test_pitch_is_not_reaccepted_or_threshold_relaxed(shift, expected):
    result = triage.analyze(findings("generated_audio_pitch_rejected",
                                  stored_pitch_measurements={"pitch_shift_semitones": shift}))
    assert result["pitch"][0]["diagnostic"] == expected
    assert "accepted" not in result["pitch"][0]


@pytest.mark.parametrize("name,text,expected", [
    ("Brook", "Hey Brooke", "already_covered_by_current_pure_text_policy_not_audio_credit"),
    ("Colin", "Hey Collin", "already_covered_by_current_pure_text_policy_not_audio_credit"),
    ("Sheikh", "Hey shake", "already_covered_by_current_pure_text_policy_not_audio_credit"),
    ("Cydney", "Hey Cindy", "canonical_lexicon_missing_or_ambiguous"),
    ("Sheikh", "Hey shake thanks", "complete_stressed_phone_sequences_differ"),
    ("Misty", None, "missing_stored_transcript")])
def test_text_diagnostics_never_issue_audio_proofs(name, text, expected):
    result = triage.analyze(findings("name_or_pronunciation_rejected", first_name=name,
        stored_recognition_diagnostics_not_actual_listening={"observed_transcript": text}))
    assert result["pronunciation"][0]["diagnostic"] == expected
    assert "accepted" not in result["pronunciation"][0]


def test_duplicate_or_incomplete_receipt_sets_rejected():
    data = findings()
    data["rows"][-1]["outbox_row"] = 0
    with pytest.raises(ValueError):
        triage.analyze(data)
    data = findings()
    data["rows"].pop()
    with pytest.raises(ValueError):
        triage.analyze(data)
