import copy
import json
from datetime import datetime, timezone

import pytest

from corrected_phone_contact import HOLD_NOTE, MARKER, active_episode, followup_identity_anchor, prepare, state_hash


def parse(value):
    if not value:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def fixture():
    row = [""] * 45
    for i, value in {0: "Judy", 1: "Wang", 2: "214-500-7663", 4: "120 Piping Rock Dr",
                     5: "Fort Worth", 6: "TX", 7: "x", 8: "x", 9: HOLD_NOTE, 27: "340857261",
                     22: "2026-10-28T15:39:27Z", 23: "2026-10-26T15:00:00Z",
                     30: "phone_identity_review_required"}.items():
        row[i] = value
    def entry(phone, prefix, request, date):
        e = [""] * 19
        e[0], e[1], e[2], e[3], e[4] = date, "sent", request, request.removeprefix("render-"), phone
        e[6] = prefix + json.dumps({"crm_row": 6411, "stable_id": row[27]})
        e[13], e[14], e[17] = date, date, 6411
        return e
    current = entry("2145007663", "__initial_outreach__:", "render-initial-6411-98ffb61bfde011ac", "2026-10-28T15:39:33Z")
    current[0], current[13] = "2026-10-28T15:38:13Z", "2026-10-28T15:39:25Z"
    old = entry("2193970173", "__scheduled_followup__:", "old-followup", row[23])
    payload = {"formerPhone": "2193970173", "initialRequestId": current[2], "formerFollowupRequestId": old[2]}
    return row, [current, old], payload


def test_explicit_episode_preserves_old_history_and_binds_new_receipt():
    row, entries, payload = fixture()
    before = copy.deepcopy(row)
    episode = prepare(6411, row, [row], entries, payload, parse)
    assert row == before
    assert episode["initialReceipt"]["phone"] == "2145007663"
    assert episode["priorJ"] == HOLD_NOTE
    row[9] = ""
    row[25] = MARKER + " " + json.dumps(episode)
    assert active_episode(6411, row, entries, parse) == episode
    from video_followup import snapshot
    assert snapshot(row=6411, stable_id=row[27], phone=row[2], first_name=row[0],
                    initial_sent_at=episode["initialReceipt"]["sent_at"],
                    initial_receipt=episode["initialReceipt"])["initial_receipt"] == episode["initialReceipt"]


@pytest.mark.parametrize("column", [10, 12, 13, 15, 16, 17, 19, 28, 29, 31, 32, 33, 35, 36, 37, 38, 39, 40])
def test_protected_states_fail_closed(column):
    row, entries, payload = fixture()
    row[column] = "protected"
    with pytest.raises(ValueError):
        prepare(6411, row, [row], entries, payload, parse)


@pytest.mark.parametrize("status", ["queued", "claimed", "send_started", "uncertain", "failed", "sent"])
def test_any_other_current_phone_request_blocks_repeat(status):
    row, entries, payload = fixture()
    extra = copy.deepcopy(entries[0]); extra[1], extra[2], extra[3] = status, "other", "other"
    with pytest.raises(ValueError):
        prepare(6411, row, [row], entries + [extra], payload, parse)


def test_duplicate_and_changed_identity_fail_closed():
    row, entries, payload = fixture()
    with pytest.raises(ValueError):
        prepare(6411, row, [row, row], entries, payload, parse)
    episode = prepare(6411, row, [row], entries, payload, parse)
    row[9] = ""; row[25] = MARKER + " " + json.dumps(episode)
    before = state_hash(row)
    row[2] = "214-555-1111"
    assert before != state_hash(row)
    with pytest.raises(ValueError):
        active_episode(6411, row, entries, parse)


def test_later_callback_or_followup_is_not_ignored():
    row, entries, payload = fixture()
    episode = prepare(6411, row, [row], entries, payload, parse)
    row[25] = MARKER + " " + json.dumps(episode)
    row[9] = "Please call me tomorrow"
    assert active_episode(6411, row, entries, parse) is None
    row[9] = ""; row[23] = "2026-10-28T22:00:00Z"
    assert active_episode(6411, row, entries, parse) is None


def test_handset_anchor_differs_from_terminal_receipt_without_changing_cadence_proof():
    row, entries, payload = fixture()
    row[22] = "2026-10-28T15:39:27.833Z"
    entries[0][14] = "2026-10-28T15:39:33.402Z"
    episode = prepare(6411, row, [row], entries, payload, parse)
    row[9] = ""; row[25] = MARKER + " " + json.dumps(episode)
    before = copy.deepcopy((row, entries, episode))
    assert active_episode(6411, row, entries, parse) == episode
    anchor = followup_identity_anchor(row, episode, parse)
    from video_followup import snapshot
    context = snapshot(row=6411, stable_id=row[27], phone=row[2], first_name=row[0],
                       initial_sent_at=anchor, initial_receipt=episode["initialReceipt"])
    assert context["initial_sent_at"] == "2026-10-28T15:39:27.833Z"
    assert context["initial_receipt"]["sent_at"] == "2026-10-28T15:39:33.402Z"
    assert (row, entries, episode) == before


@pytest.mark.parametrize("owning", ["", "2026-10-28T15:39:24Z", "2026-10-28T15:39:34Z"])
def test_changed_or_unconfirmed_handset_anchor_fails_closed(owning):
    row, entries, payload = fixture()
    episode = prepare(6411, row, [row], entries, payload, parse)
    row[9] = ""; row[25] = MARKER + " " + json.dumps(episode)
    row[22] = owning
    with pytest.raises(ValueError, match="Owning handset"):
        active_episode(6411, row, entries, parse)
