import copy
import datetime as dt
import json
import re
import urllib.parse
from unittest import mock

import pytest
import verifier_closeout as gate


def snapshot(late=False):
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    headers = [''] * 43
    for i, value in {0:'agent_name',1:'last_name',2:'phone',3:'email',4:'listing_address',
                     7:'initial_text_sent',11:'last_outbound_text',25:'contact_verification_note',42:'codex-verified'}.items():
        headers[i] = value
    def lead(name, phone, address, stable, sent=True):
        row = [''] * 43
        row[:7] = [name,'Agent',phone,name.lower()+'@example.com',address,'Denver','CO']
        row[7], row[11], row[22] = ('x','initial text',now) if sent else ('','','')
        row[24:28] = ['verified_direct_mobile','Source https://official.example/agent','verified_agent_specific_email',stable]
        return row
    main = [headers, lead('Jane','202-555-0199','123 Main','id-2')]
    if late:
        main.append(lead('Brennan','781-561-5169','3860 S Genoa Court #B','id-3',False))
    out = [['status','request_id','message_id','phone','reply_text','sent_at','crm_row'],
           ['sent','render-initial-2-abcdef','initial-2-abcdef','2025550199','initial text',now,2]]
    queue = [['request_id','payload_json','status']]
    tables = {name:dict(start_row=1,end_row=len(values),grid_rows=len(values),truncated=False,values=values)
              for name,values in [('Sheet1',main),('sms_pending_sends',out),('Initial SMS Queue',queue)]}
    tables['Sheet1']['detail_rows'] = list(range(2,len(main)+1))
    return dict(observed_at=now,capture_mode='cohort_sparse_v1',tables=tables)


def initial(s):
    audit = gate.evaluate(s)
    live = gate.meaningful(s['tables']['Sheet1']['values'])
    return dict(audit=audit,frozen=[dict(row=r['row'],identity=list(gate.identity(live[r['row']]))) for r in audit['ledger']])


def test_brennan_added_after_begin_cannot_be_omitted_from_final_gate():
    result = gate.terminal(initial(snapshot()), snapshot(late=True))
    assert result['late_rows'] == [3]
    assert result['sms_open_rows'] == [3]
    assert not result['complete']
    assert result['audit']['execution_closeout']['expected_count'] == 2


def test_missing_initial_capture_cannot_finish():
    with mock.patch.object(gate.sheets,'ensure_headers_tab'), mock.patch.object(gate,'journal_read',return_value=None):
        with pytest.raises(ValueError, match='missing_initial_run_capture_terminal_forbidden'):
            gate.handle('token','sheet',dict(action='final',automation_id='lead-verifier-5-pm',run_receipt_id='missing-begin-proof'))


def test_queued_late_request_is_open_until_sent_and_h_readback():
    first = initial(snapshot())
    s = snapshot(late=True)
    out = s['tables']['sms_pending_sends']
    out['values'].append(['queued','render-initial-3-abcdef','initial-3-abcdef','7815615169','initial text','',3])
    out['end_row'] = out['grid_rows'] = 3
    result = gate.terminal(first,s)
    assert result['execution_ledger']['items'][0]['outcome'] == 'pending_request'
    assert not result['complete']
    out['values'][-1][0],out['values'][-1][5] = 'sent',s['observed_at']
    assert not gate.terminal(first,s)['complete']  # Sheet still blank.
    row = s['tables']['Sheet1']['values'][-1]
    row[7],row[11],row[22] = 'x','initial text',s['observed_at']
    assert gate.terminal(first,s)['complete']


def test_blank_contact_keeps_terminal_open_even_after_sms_sent():
    s = snapshot(); first = initial(s)
    s['tables']['Sheet1']['values'][1][3] = ''
    result = gate.terminal(first,s)
    assert not result['complete']
    assert result['audit']['execution_closeout']['contact_open_rows'] == [2]


def test_successful_empty_bounded_blocks_are_padding_not_truncation():
    with mock.patch.object(gate,'batch_read',return_value=[[['header'],['lead']],[]]):
        values = gate.grid_read('token','sheet','Sheet1',700,'G')
    assert len(values) == 700
    assert values[1] == ['lead'] and values[-1] == []


def test_partial_native_response_fails_closed():
    with mock.patch.object(gate.sheets,'sheets_request',return_value={'valueRanges':[]}):
        with pytest.raises(ValueError, match='incomplete_native_capture'):
            gate.batch_read('token','sheet',['Sheet1!A1:G500'])


def test_journal_digest_and_chunks_have_saved_readback():
    rows = [gate.HEADERS]
    def append(_,__,___,values): rows.extend(values)
    artifact = dict(observed_at=dt.datetime.now(dt.timezone.utc).isoformat(),values=['proof']*1000)
    with mock.patch.object(gate.sheets,'get_values',side_effect=lambda *_:rows), \
         mock.patch.object(gate.sheets,'append_values',side_effect=append):
        gate.journal_write('token','sheet','run-identifier','lead-verifier-5-pm','initial',artifact)
        assert gate.journal_read('token','sheet','run-identifier','initial') == artifact
        rows[-1][-1] = rows[-1][-1][:-4]
        with pytest.raises(Exception): gate.journal_read('token','sheet','run-identifier','initial')


def native_api(late_during_capture=False, supplied_tables=None):
    s = snapshot(late=True)
    tables = supplied_tables or {name:table['values'] for name,table in s['tables'].items()}
    calls = []
    def api(token, method, path, payload=None):
        assert method == 'GET'  # Capture must never write or send.
        if '/values:batchGet?' not in path:
            return {'sheets':[{'properties':{'title':tab,'gridProperties':{'rowCount':len(values)}}}
                              for tab,values in tables.items()]}
        ranges = urllib.parse.parse_qs(path.split('?',1)[1])['ranges']
        blocks = []
        for area in ranges:
            m = re.fullmatch(r"'([^']+)'!([A-Z]+)(\d+):([A-Z]+)(\d+)",area)
            tab,a,start,b,end = m.groups()
            def col(v):
                result=0
                for c in v: result=result*26+ord(c)-64
                return result-1
            source = copy.deepcopy(tables[tab])
            if tab=='Sheet1' and a=='A' and b=='G' and start=='1':
                calls.append(area)
                if late_during_capture and len(calls)==1:
                    source[-1]=[]
            values=[r[col(a):col(b)+1] for r in source[int(start)-1:int(end)]]
            while values and not values[-1]: values.pop()
            blocks.append({'range':area,'values':values})
        return {'valueRanges':blocks}
    return api


def test_native_capture_includes_current_late_identity_and_full_offsets():
    with mock.patch.object(gate.sheets,'sheets_request',side_effect=native_api()):
        snap,_ = gate.capture('token','sheet')
    assert snap['tables']['Sheet1']['detail_rows'] == [2,3]
    assert gate.evaluate(snap)['sms_review_rows'] == [3]
    assert snap['tables']['sms_pending_sends']['values'][1][0] == 'sent'


def test_addition_during_capture_fails_seal_and_requires_fresh_retry():
    with mock.patch.object(gate.sheets,'sheets_request',side_effect=native_api(True)):
        with pytest.raises(ValueError, match='identities_changed_retry_final_capture'):
            gate.capture('token','sheet')


def test_old_uncertainty_is_visible_but_only_explicit_carried_rows_join_current_cohort():
    s = snapshot()
    main = [s['tables']['Sheet1']['values'][0]]
    out = [s['tables']['sms_pending_sends']['values'][0]]
    for n in range(2,205):
        row = copy.deepcopy(s['tables']['Sheet1']['values'][1])
        row[0],row[2],row[4],row[27] = 'Agent'+str(n),'202555'+str(1000+n),'Street '+str(n),'id'+str(n)
        main.append(row)
        out.append(['uncertain' if n==2 else 'sent',f'render-initial-{n}-abc',f'initial-{n}-abc',
                    row[2],'initial text',s['observed_at'] if n!=2 else '',n])
    tables = {'Sheet1':main,'sms_pending_sends':out,
              'Initial SMS Queue':s['tables']['Initial SMS Queue']['values']}
    with mock.patch.object(gate.sheets,'sheets_request',side_effect=native_api(supplied_tables=tables)):
        fresh,_ = gate.capture('token','sheet')
        carried,_ = gate.capture('token','sheet',carried_rows=[2])
    assert 2 not in fresh['tables']['Sheet1']['detail_rows']
    assert fresh['historical_open_initials'][0]['row'] == 2
    assert 2 in carried['tables']['Sheet1']['detail_rows']
    assert gate.evaluate(carried,carried_rows=carried['carried_rows'])['sms_review_rows'] == [2]
