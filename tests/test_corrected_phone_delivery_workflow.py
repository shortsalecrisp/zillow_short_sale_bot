import copy
import json

import pytest

from followup_delivery_review import merge_ledger
from test_followup_delivery_review import fixture, run


def corrected():
    row, prior = fixture()
    prior[5] = row[11] = "Hey Sallynn, this is Yoni with Crisp Short Sales."
    row[2] = "909-223-6904"
    row[25] = "Verified direct phone correction; old H/W belongs to former phone."
    return row, prior


def complete(result):
    return dict(result, complete=True)


def current_receipt(row, old):
    row = copy.deepcopy(row)
    sent = copy.deepcopy(old)
    sent[2:5] = ["render-initial-6452-fedcba9876543210", "initial-6452-fedcba9876543210", "9092236904"]
    sent[0], sent[13], sent[14] = "2026-10-28T13:58:00Z", "2026-10-28T13:59:00Z", "2026-10-28T14:00:00Z"
    row[22] = "2026-10-28T13:59:30Z"
    return row, sent


def test_corrected_phone_is_explicit_initial_work_not_followup_authority():
    row, prior = corrected()
    before = copy.deepcopy((row, prior))
    result = run([row], [prior])
    item = result["items"][0]
    assert item["status"] == "blocked"
    assert item["reason"] == "corrected_phone_initial_contact_required"
    assert item["remediation"]["former_phone"] == "7202436784"
    assert item["remediation"]["former_initial_receipt"]["request_id"] == prior[2]
    assert item["remediation"]["send_authorized"] is False
    assert result["remediation_queue"] == [item]
    assert (row, prior) == before


@pytest.mark.parametrize("state", ["queued", "claimed", "send_started", "uncertain", "failed", "superseded", "sent"])
def test_every_current_phone_attempt_requires_reconciliation_not_another_initial(state):
    row, prior = corrected()
    pending = list(prior)
    pending[1:5] = [state, "render-initial-6452-fedcba9876543210", "initial-6452-fedcba9876543210", "9092236904"]
    pending[0] = "2026-10-28T13:58:00Z"
    pending[13:15] = (["2026-10-28T13:59:00Z", "2026-10-28T14:00:00Z"]
                      if state == "sent" else ["", ""])
    result = run([row], [prior, pending])
    item = result["items"][0]
    assert item["status"] == "blocked"
    assert item["reason"] == "current_phone_attempt_reconcile_only"
    assert item["remediation"]["action"] == "reconcile_current_phone_attempt"
    assert item["remediation"]["current_phone_attempts"][0]["status"] == state
    assert item["remediation"]["send_authorized"] is False


@pytest.mark.parametrize("change", ["duplicate", "other_stable", "unbound_time", "empty_body", "wrong_row"])
def test_initial_work_classification_requires_one_exact_former_phone_receipt(change):
    row, prior = corrected()
    entries = [prior]
    if change == "duplicate": entries.append(list(prior))
    if change == "other_stable": row[27] = "other"
    if change == "unbound_time": row[22] = "2026-10-26T21:08:00Z"
    if change == "empty_body": prior[5] = ""
    if change == "wrong_row": prior[17] = 999
    item = run([row], entries)["items"][0]
    assert item["status"] == "blocked"
    assert "remediation" not in item


def test_current_sent_readback_closes_only_old_initial_issue_and_restarts_calendar():
    row, prior = corrected()
    first = complete(run([row], [prior]))
    ledger = merge_ledger(None, first, "next-existing-slot")
    old_key = next(iter(ledger["episodes"]))
    row, sent = current_receipt(row, prior)
    fresh = complete(run([row], [prior, sent], elapsed=lambda *_: 0))
    item = fresh["items"][0]
    assert item["status"] == "deferred" and item["reason"] == "six_business_hours_not_due"
    assert item["initial_contact_readback_verified"] is True
    after = merge_ledger(ledger, fresh, "later-existing-slot")
    resolved = after["episodes"][old_key]
    assert resolved["status"] == "resolved_initial_contact"
    assert resolved["retry_after"] is None
    assert resolved["resolution"]["initial_receipt"]["phone"] == "9092236904"
    assert resolved["resolution"]["followup_sent"] is False
    assert resolved["history"][-1]["reason"] == "corrected_phone_initial_contact_required"
    assert any(e["status"] == "deferred" for e in after["episodes"].values())
    assert after["remediation_queue"] == []


@pytest.mark.parametrize("change", ["missing_h", "wrong_l", "other_phone", "other_stable", "spoofed_proof"])
def test_unverified_or_different_identity_cannot_close_the_initial_issue(change):
    row, prior = corrected()
    ledger = merge_ledger(None, complete(run([row], [prior])), "next")
    old_key = next(iter(ledger["episodes"]))
    row, sent = current_receipt(row, prior)
    if change == "missing_h": row[7] = ""
    if change == "wrong_l": row[11] = "Unrelated message"
    if change == "other_phone":
        row[2] = "7192105557"
        sent[4] = "7192105557"
    if change == "other_stable":
        row[27] = "different"
        sent[6] = '__initial_outreach__:' + json.dumps({"crm_row": 6452, "stable_id": "different"})
    survey = complete(run([row], [prior, sent]))
    if change == "spoofed_proof":
        survey["items"][0]["current_initial_receipt"]["phone"] = "0000000000"
    after = merge_ledger(ledger, survey, "later")
    assert after["episodes"][old_key]["status"] == "blocked"


def test_missing_capture_items_do_not_drop_unresolved_corrected_phone_work():
    row, prior = corrected()
    survey = complete(run([row], [prior]))
    ledger = merge_ledger(None, survey, "next")
    again = merge_ledger(ledger, survey, "later")
    assert again["remediation_queue"][0]["retry_after"] == "later"
    assert again["remediation_queue"][0]["history"] == []
    empty = dict(survey, items=[], verified_sent=[], remediation_queue=[])
    missing = merge_ledger(again, empty, "later-still")
    assert missing["remediation_queue"] == again["remediation_queue"]


@pytest.mark.parametrize("confirmed", [True, False])
def test_later_real_reply_can_close_initial_gap_without_enabling_a_followup(confirmed):
    row, prior = corrected()
    ledger = merge_ledger(None, complete(run([row], [prior])), "next")
    old_key = next(iter(ledger["episodes"]))
    row, sent = current_receipt(row, prior)
    reply = list(sent)
    reply[2:4] = ["reply-request", "new-question"]
    reply[5], reply[6], reply[14] = "The buyer pays at closing.", "What is your fee?", "2026-10-28T14:05:00Z"
    row[11] = reply[5]
    row[43] = reply[6]
    row[17] = json.dumps([{"role": "assistant", "text": reply[5],
                           **({"receipt_id": reply[2]} if confirmed else {})}])
    survey = complete(run([row], [prior, sent, reply]))
    assert survey["items"][0]["status"] == "blocked"
    assert survey["items"][0]["reason"] == "reply_or_manual_control"
    after = merge_ledger(ledger, survey, "later")
    assert after["episodes"][old_key]["status"] == ("resolved_initial_contact" if confirmed else "blocked")
