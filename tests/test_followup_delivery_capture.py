import ast
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_followup_delivery_review import fixture, parse


def capture(race=False, bad_headers=False, omitted_blanks=False):
    row, initial=fixture()
    headers=['']*45
    for i,n in {0:'agent_name',2:'phone',7:'initial_text_sent',8:'followup_text_sent',13:'ai_state',
                16:'handoff_flag',19:'human_override',21:'last_message_id',27:'created-at',
                43:'last_inbound_text',44:'last_inbound_at'}.items(): headers[i]=n
    if bad_headers: headers[43]='unexpected'
    requests=[]
    class Result:
        def __init__(self,data): self.data=data
        def execute(self): return self.data
    class API:
        caps=0
        def spreadsheets(self): return self
        def values(self): return self
        def get(self,**kw):
            requests.append(kw)
            if 'fields' in kw:
                self.caps+=1
                return Result({'sheets':[{'properties':{'title':n,'gridProperties':{'rowCount':size}}}
                    for n,size in [('Sheet1',1000),('sms_pending_sends',10+int(race and self.caps>1)),
                                   ('unrelated_debug',100+self.caps)]]})
            if '!A1:AS1' in kw['range']: return Result({'values':[headers]})
            return Result({'values':[initial[:5],[]] if omitted_blanks else [initial[:5]]})
        def batchGet(self,**kw):
            requests.append(kw)
            return Result({'valueRanges':[{'values':[[]]*899+[row[:30]]},
                {'values':[[]]*899+[row[42:45]]},{'values':[initial,['']*6+['old metadata']]
                    if omitted_blanks else [initial]}]})
        def batchUpdate(self,**kw): raise AssertionError('Read-only review must not write')
    source=Path(__file__).resolve().parents[1]/'bot_min.py'
    node=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef)
              and n.name=='review_followup_delivery')
    ns=dict(sheets_service=API(),GSHEET_ID='test',GSHEET_TAB='Sheet1',_parse_sheet_datetime=parse,
            business_hours_elapsed=lambda *_a,**_k:8,FOLLOWUP_INCLUDE_WEEKENDS=False,
            datetime=SimpleNamespace(now=lambda **_:parse('2026-10-28T15:00:00Z')),SCHEDULER_TZ=timezone.utc,
            FU_HOURS=6,FOLLOWUP_DUE_GRACE_MINUTES=2,_followup_has_contact_hold=lambda _:False,
            _followup_has_recorded_conversation=lambda _:False)
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),'exec'),ns)
    return ns['review_followup_delivery'],requests


def test_full_capture_includes_old_rows_and_end_index_without_writes():
    run,requests=capture()
    result=run()
    assert result['complete'] is True and result['items'][0]['row']==901
    assert result['items'][0]['status']=='due_unsent'
    ranges=next(r['ranges'] for r in requests if 'ranges' in r)
    assert ranges==['Sheet1!A2:AD1000','Sheet1!AQ2:AS1000','sms_pending_sends!A2:S10']
    assert requests[-2]['range']=='sms_pending_sends!A2:E10'


def test_racing_capacity_never_returns_a_send_candidate():
    run,_=capture(race=True)
    result=run()
    assert result['complete'] is False
    assert result['items'][0]['status']=='blocked'
    assert result['items'][0]['reason']=='capture_raced_reread_required'


def test_omitted_empty_status_cells_and_unrelated_sheet_growth_are_not_races():
    run,_=capture(omitted_blanks=True)
    result=run()
    assert result['complete'] is True
    assert result['capture_bounds']['status_index_stable'] is True


def test_header_drift_fails_closed_before_survey():
    run,_=capture(bad_headers=True)
    with pytest.raises(ValueError,match='header contract'): run()


def test_identity_mismatch_cannot_be_overridden_by_older_resolution_note():
    from typing import Any, List
    source=Path(__file__).resolve().parents[1]/'bot_min.py'
    node=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef)
              and n.name=='_followup_has_contact_hold')
    ns=dict(Any=Any,List=List,COL_PHONE_CONF=24,COL_CONTACT_REASON=25)
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),'exec'),ns)
    row=['']*45
    row[24]='verified_agent_cell_not_listing_agent'
    row[25]='sol_exception_resolved; property_agent_mismatch'
    assert ns['_followup_has_contact_hold'](row) is True


@pytest.mark.parametrize('stamp',['2026-10-31T15:00:00Z','2026-10-28T02:00:00Z'])
def test_bounded_recovery_off_hours_never_reads_or_sends(stamp):
    import logging
    source=Path(__file__).resolve().parents[1]/'bot_min.py'
    node=next(n for n in ast.parse(source.read_text()).body if isinstance(n,ast.FunctionDef)
              and n.name=='_follow_up_pass')
    ns=dict(LOG=logging.getLogger(__name__),datetime=SimpleNamespace(now=lambda **_:parse(stamp)),
            SCHEDULER_TZ=timezone.utc)
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),'exec'),ns)
    assert ns['_follow_up_pass'](only_rows={6452},recipient_local_hours=True)==[
        {'row':6452,'status':'deferred','reason':'weekday_business_window'}]
