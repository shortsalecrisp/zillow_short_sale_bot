import copy
import json
from datetime import datetime, timezone

import pytest
from followup_delivery_review import review


def parse(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def fixture():
    row = [""] * 45
    row[0], row[2], row[6], row[7], row[22], row[27], row[42] = (
        "Sallynn", "720-243-6784", "CO", "x", "2026-10-26T21:06:28Z", "68062427", "x")
    entry = [""] * 19
    entry[0], entry[1], entry[2], entry[3], entry[4] = (
        "2026-10-26T21:05:27Z", "sent", "scheduler-initial-6452-123b8aa7a9daf4b4",
        "initial-6452-123b8aa7a9daf4b4", "7202436784")
    entry[6] = '__initial_outreach__:' + json.dumps({"crm_row":6452,"stable_id":"68062427"})
    entry[13], entry[14], entry[17] = "2026-10-26T21:06:19Z", row[22], 6452
    return row, entry


def run(rows, entries, **overrides):
    options = dict(parse_time=parse, elapsed=lambda *_:8, now=parse("2026-10-28T15:00:00Z"),
                   due_hours=6, grace_minutes=2, contact_hold=lambda r: "mismatch" in str(r[25]),
                   conversation_hold=lambda r: bool(r[13]), within_hours=lambda *_:True)
    options.update(overrides)
    return review(rows, entries, **options)


def test_aq_verified_and_older_rows_are_still_reviewed_without_mutation():
    row, initial = fixture()
    rows = [[] for _ in range(800)] + [row]
    before = copy.deepcopy((rows, initial))
    result = run(rows, [initial])
    assert result['items'][0]['row'] == 802
    assert result['items'][0]['status'] == 'due_unsent'
    assert len(result['items'][0]['episode_key']) == 64
    assert result['sent'] is False and (rows, initial) == before


@pytest.mark.parametrize('field,value',[(2,'9042197539'),(22,''),(22,'bad'),(27,'other')])
def test_identity_or_initial_gap_never_becomes_send_candidate(field,value):
    row, initial=fixture()
    row[field]=value
    assert run([row],[initial])['items'][0]['status']=='blocked'


@pytest.mark.parametrize('field,value',[(8,'x'),(23,'2026-10-28T15:00:00Z'),(19,'yes'),
    (16,'yes'),(13,'handoff'),(21,'inbound-id'),(43,'STOP'),(44,'2026-10-28T14:00:00Z'),
    (10,'O'),(10,'H'),(25,'property_agent_mismatch')])
def test_reply_optout_manual_and_identity_controls_do_not_authorize_recovery(field,value):
    row, initial=fixture()
    row[field]=value
    assert run([row],[initial])['items'][0]['status']=='blocked'


@pytest.mark.parametrize('state',['queued','claimed','send_started','receipt_pending','uncertain','failed','superseded','sent'])
def test_existing_attempt_is_never_a_new_send_candidate(state):
    row, initial=fixture()
    followup=list(initial)
    followup[1:4]=[state,'scheduler-followup-video-abc','followup-video-abc']
    followup[6]='__scheduled_followup__:'+json.dumps({'stable_id':row[27], 'initial_receipt':{
        'phone':'7202436784','request_id':initial[2],'message_id':initial[3]}})
    followup[14]='2026-10-28T14:00:00Z' if state=='sent' else ''
    followup[13]='2026-10-28T13:59:50Z'
    result=run([row],[initial,followup])
    assert result['items'][0]['status'] in {'blocked','pending'}
    if state=='sent':
        assert result['items'][0]['reason']=='sent_receipt_sheet_readback_needed'
        row[8],row[23],row[11]='x',followup[14],followup[5]
        assert run([row],[initial,followup])['counts']=={'sent':1}


def test_duplicate_initial_ids_or_competing_followups_fail_closed():
    row, initial=fixture()
    assert run([row],[initial,initial])['items'][0]['status']=='blocked'
    uncertain=list(initial)
    uncertain[1:4]=['uncertain','scheduler-followup-video-abc','followup-video-abc']
    uncertain[6]='broken metadata'
    assert run([row],[initial,uncertain])['items'][0]['status']=='blocked'


def test_calendar_and_local_hours_preserved():
    row, initial=fixture()
    assert run([row],[initial],elapsed=lambda *_:5)['items'][0]['reason']=='six_business_hours_not_due'
    assert run([row],[initial],within_hours=lambda *_:False)['items'][0]['reason']=='recipient_local_hours'
    assert run([row],[initial],now=parse('2026-10-31T15:00:00Z'))['items'][0]['status']=='deferred'


def test_ledger_keeps_missing_items_and_requires_complete_positive_evidence():
    from followup_delivery_review import merge_ledger
    row, initial=fixture()
    result=run([row],[initial])
    with pytest.raises(ValueError):
        merge_ledger(None,result,'next-slot')
    result['complete']=True
    ledger=merge_ledger(None,result,'next-slot')
    key=result['items'][0]['episode_key']
    assert ledger['episodes'][key]['retry_after']=='next-slot'
    empty={**result,'items':[],'verified_sent':[]}
    assert merge_ledger(ledger,empty,'later')['episodes'][key]==ledger['episodes'][key]
    resolved={**result['items'][0],'status':'sent','reason':'sent_receipt_verified'}
    closed=merge_ledger(ledger,{**empty,'verified_sent':[resolved]},'later')
    assert closed['episodes'][key]['retry_after'] is None
