import hashlib
import json
import os
import re
from unittest import mock

import pytest
import verifier_closeout as gate
import verifier_proof_storage as proof


class Store:
    def __init__(self):
        self.data = {}
        self.writes = []
        self.timeout = False

    def read(self, key):
        return self.data.get(key, (None, 0))

    def write(self, key, body, generation):
        assert generation == 0 and key not in self.data
        self.writes.append(key)
        version = len(self.data) + 1
        self.data[key] = body, version
        if self.timeout:
            raise TimeoutError('storage accepted but response lost')
        return version


RUN, AUTO, PHASE = 'a-real-run-identifier', 'lead-verifier-5-pm', 'initial'


def test_large_private_proof_round_trip_splits_objects_below_generic_storage_read_limit():
    store = Store()
    raw = json.dumps({'observed_at': '2026-11-17T23:00:00Z',
                      'random': os.urandom(1500000).hex()}).encode()
    with mock.patch.object(proof, 'objects', return_value=store):
        locator = proof.write(raw, RUN, AUTO, PHASE)
        assert len(locator) < 1000
        assert proof.read(locator, RUN, AUTO, PHASE, hashlib.sha256(raw).hexdigest()) == raw
        assert len(store.data) > 3
        assert all(len(body) < 512 * 1024 for body, _ in store.data.values())
        writes = len(store.writes)
        assert proof.write(raw, RUN, AUTO, PHASE) == locator
        assert len(store.writes) == writes


def test_uncertain_storage_acceptance_is_reconciled_without_overwrite():
    store = Store()
    store.timeout = True
    with mock.patch.object(proof, 'objects', return_value=store):
        locator = proof.write(b'{"literal":"proof"}', RUN, AUTO, PHASE)
        assert proof.read(locator, RUN, AUTO, PHASE, proof.sha(b'{"literal":"proof"}')) == b'{"literal":"proof"}'
    assert len(store.writes) == 2


@pytest.mark.parametrize('damage', ['missing', 'generation', 'bytes', 'manifest', 'identity'])
def test_missing_changed_or_cross_run_storage_never_becomes_positive_proof(damage):
    store = Store()
    raw = b'{"literal":"immutable proof"}'
    with mock.patch.object(proof, 'objects', return_value=store):
        locator = proof.write(raw, RUN, AUTO, PHASE)
        ref = json.loads(locator[len(proof.MARKER):])
        key = next(k for k in store.data if 'chunk-' in k)
        body, gen = store.data[key]
        if damage == 'missing': del store.data[key]
        if damage == 'generation': store.data[key] = body, gen + 1
        if damage == 'bytes': store.data[key] = body[:-1] + b'X', gen
        if damage == 'manifest':
            old, gen = store.data[ref['key']]
            store.data[ref['key']] = old + b' ', gen
        with pytest.raises(ValueError):
            proof.read(locator, RUN + '-other' if damage == 'identity' else RUN,
                       AUTO, PHASE, proof.sha(raw))


def fake_journal(rows):
    def read(token, sid, specs):
        return [[r[:7] for r in rows] if spec.endswith('A:G') else
                [[rows[int(re.search(r'!H(\d+):', spec)[1]) - 1][7]]] for spec in specs]
    return read


def test_recovered_sheet_keeps_one_small_native_receipt_with_exact_full_artifact_readback():
    store, rows = Store(), [gate.HEADERS]
    artifact = {'observed_at': '2026-11-17T23:00:00Z', 'complete': False,
                'literal_receipts': ['uncertain', 'held', 'sent'] * 10000}
    with mock.patch.object(proof, 'objects', return_value=store), \
         mock.patch.object(gate, 'batch_read', side_effect=fake_journal(rows)), \
         mock.patch.object(gate.sheets, 'append_values', side_effect=lambda *args: rows.extend(args[3])):
        digest = gate.journal_write('token', gate.RECOVERED_WORKBOOK, RUN, AUTO, PHASE, artifact)
        assert len(rows) == 2 and rows[1][4] == digest
        assert rows[1][7].startswith(proof.MARKER)
        assert gate.journal_read('token', gate.RECOVERED_WORKBOOK, RUN, PHASE) == artifact


def test_storage_failure_cannot_append_a_native_positive_proof_reference():
    with mock.patch.object(proof, 'objects', side_effect=RuntimeError('storage unavailable')), \
         mock.patch.object(gate.sheets, 'append_values') as append:
        with pytest.raises(RuntimeError, match='storage unavailable'):
            gate.journal_write('token', gate.RECOVERED_WORKBOOK, RUN, AUTO, PHASE,
                               {'observed_at': '2026-11-17T23:00:00Z'})
        append.assert_not_called()


def test_historical_lookup_is_read_only_exact_original_and_does_not_mint_an_episode():
    calls = []
    def read(token, sid, specs):
        calls.append((sid, specs))
        return [[gate.HEADERS]]
    with mock.patch.object(gate, 'batch_read', side_effect=read), \
         mock.patch.object(gate.sheets, 'append_values') as append:
        assert gate.journal_read('token', gate.RECOVERED_WORKBOOK, RUN, PHASE) is None
        assert [sid for sid, _ in calls] == [gate.RECOVERED_WORKBOOK, gate.FROZEN_WORKBOOK]
        append.assert_not_called()


def test_literal_legacy_artifact_is_still_available_with_its_original_digest():
    import base64
    import gzip
    artifact = {'observed_at': '2026-11-17T22:00:00Z', 'frozen': [{'row': 6970}],
                'automation_id': AUTO, 'run_receipt_id': RUN}
    raw = json.dumps(artifact, separators=(',', ':')).encode()
    rows = [gate.HEADERS, [RUN, AUTO, PHASE, artifact['observed_at'], proof.sha(raw),
                          '0', '1', base64.b64encode(gzip.compress(raw)).decode()]]
    def read(token, sid, specs):
        return [[gate.HEADERS]] if sid == gate.RECOVERED_WORKBOOK else fake_journal(rows)(token, sid, specs)
    with mock.patch.object(gate, 'batch_read', side_effect=read), \
         mock.patch.object(proof, 'objects') as storage, \
         mock.patch.object(gate.sheets, 'append_values') as append:
        assert gate.journal_read('token', gate.RECOVERED_WORKBOOK, RUN, PHASE) == artifact
        append.assert_not_called()
        storage.assert_not_called()


def test_invalid_run_or_phase_cannot_choose_an_arbitrary_storage_key():
    with mock.patch.object(proof, 'objects') as objects:
        with pytest.raises(ValueError, match='invalid_proof_object_identity'):
            proof.write(b'{}', '../../other-data', AUTO, 'initial')
        with pytest.raises(ValueError, match='invalid_proof_object_identity'):
            proof.write(b'{}', RUN, AUTO, '../terminal')
        objects.assert_not_called()
