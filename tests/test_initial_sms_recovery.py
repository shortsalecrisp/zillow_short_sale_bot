import json
from initial_sms_recovery import HEADERS, persist, retryable_error


class Queue:
    def __init__(self):
        self.rows = [HEADERS]

    def get_all_values(self):
        return self.rows

    def append_row(self, row, **kwargs):
        self.rows.append(row)


def test_intent_survives_restart_and_deduplicates_exact_payload():
    queue = Queue()
    payload = dict(row=12, phone='6195343527', stable_id='listing',
                   first='Roshanda', address='21100 State St', mark_codex_verified=False)
    request = persist(queue, payload)
    assert persist(queue, dict(payload)) == request
    assert len(queue.rows) == 2
    assert json.loads(queue.rows[1][3]) == payload
    assert queue.rows[1][4] == 'pending'
    assert persist(queue, {**payload, 'phone': '6195343528'}) != request


def test_retry_only_transient_enqueue_failures():
    for status in [429, 500, 502, 503, 504]:
        assert retryable_error(status, 'timeout')
    for status in [400, 401, 403, 404, 409]:
        assert not retryable_error(status, 'safety hold')


def test_producer_saves_request_instead_of_starting_volatile_timer():
    import ast
    import logging
    from pathlib import Path
    from types import SimpleNamespace
    root = Path(__file__).resolve().parents[1]
    function = next(n for n in ast.parse((root / 'bot_min.py').read_text()).body
                    if isinstance(n, ast.FunctionDef) and n.name == 'schedule_initial_sms')
    queue = Queue()
    client = SimpleNamespace(open_by_key=lambda _: SimpleNamespace(worksheet=lambda _: queue))
    ns = dict(SMS_ENABLE=True, SMS_TEST_MODE=False, SMS_TEST_NUMBER='',
              gc=client, GSHEET_ID='test', LOG=logging.getLogger('test'),
              _retry_gspread_call=lambda _, call: call())
    exec(compile(ast.Module(body=[function], type_ignores=[]), '<schedule>', 'exec'), ns)
    ns['schedule_initial_sms']('6195343527', 'Roshanda', '21100 State St', 12, 'listing')
    assert len(queue.rows) == 2
    assert queue.rows[1][4] == 'pending'
    assert json.loads(queue.rows[1][3])['stable_id'] == 'listing'
