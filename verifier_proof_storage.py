"""Create-only private verifier artifacts; native Sheets keeps a small locator."""
import gzip
import hashlib
import json
import re

PREFIX = 'ops/verifier-proof-v1/'
MARKER = 'gcs-proof-v1:'
MAX_ARTIFACT = 64 * 1024 * 1024
CHUNK = 400 * 1024
HISTORY_MARKER = 'gcs-proof-history-v1:'
HISTORY_KEY = 'ops/workbook-recovery-20261117/proof-history/manifest.json'
HISTORY_GENERATION = 1794957117466815
HISTORY_SHA256 = '6a2cce58b80e8cc0124b73d5a00438a6680b64f47e4c185e65cf81c0bbdb5f80'


def objects():
    from video_followup import Config, GCSObjects
    return GCSObjects(Config().bucket)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def create(store, key, data):
    before, generation = store.read(key)
    if before is None:
        try:
            generation = store.write(key, data, 0)
        except Exception:
            # Reconcile an uncertain storage acceptance, never overwrite it.
            before, generation = store.read(key)
            if before != data:
                raise
    else:
        if before != data:
            raise ValueError('immutable_proof_object_conflict')
    after, current = store.read(key)
    if after != data or current != generation:
        raise ValueError('immutable_proof_object_readback_failed')
    return generation


def write(raw, run_id, automation, phase):
    if not re.fullmatch(r'[A-Za-z0-9_-]{12,100}', run_id) or phase not in ('initial', 'terminal'):
        raise ValueError('invalid_proof_object_identity')
    if len(raw) > MAX_ARTIFACT:
        raise ValueError('verifier_proof_artifact_too_large')
    store = objects()
    digest = sha(raw)
    root = PREFIX + run_id + '/' + phase + '/' + digest + '/'
    packed = gzip.compress(raw, mtime=0)
    chunks = []
    for start in range(0, len(packed), CHUNK):
        data = packed[start:start + CHUNK]
        key = root + 'chunk-%04d' % len(chunks)
        chunks.append(dict(key=key, generation=create(store, key, data), sha256=sha(data), size=len(data)))
    manifest = dict(schema='verifier-proof-object-v1', run_id=run_id, automation=automation,
                    phase=phase, sha256=digest, raw_size=len(raw), chunks=chunks)
    body = encoded(manifest)
    key = root + 'manifest.json'
    reference = dict(key=key, generation=create(store, key, body), sha256=sha(body))
    return MARKER + encoded(reference).decode()


def read(locator, run_id, automation, phase, digest):
    ref = json.loads(locator[len(MARKER):])
    root = PREFIX + run_id + '/' + phase + '/' + digest + '/'
    if ref.get('key') != root + 'manifest.json':
        raise ValueError('verifier_proof_reference_binding_mismatch')
    store = objects()
    body, generation = store.read(ref['key'])
    if body is None or generation != ref.get('generation') or sha(body) != ref.get('sha256'):
        raise ValueError('verifier_proof_manifest_mismatch')
    manifest = json.loads(body)
    expected = ('verifier-proof-object-v1', run_id, automation, phase, digest)
    if tuple(manifest.get(k) for k in ('schema', 'run_id', 'automation', 'phase', 'sha256')) != expected:
        raise ValueError('verifier_proof_manifest_binding_mismatch')
    if not 0 <= manifest.get('raw_size', -1) <= MAX_ARTIFACT:
        raise ValueError('verifier_proof_artifact_too_large')
    chunks = manifest.get('chunks', [])
    if not 1 <= len(chunks) <= MAX_ARTIFACT // CHUNK + 2:
        raise ValueError('verifier_proof_chunk_count_invalid')
    packed = bytearray()
    for number, item in enumerate(chunks):
        if item.get('key') != root + 'chunk-%04d' % number or not 0 < item.get('size', 0) <= CHUNK:
            raise ValueError('verifier_proof_chunk_binding_mismatch')
        data, generation = store.read(item['key'])
        if (data is None or generation != item.get('generation') or len(data) != item['size']
                or sha(data) != item.get('sha256')):
            raise ValueError('verifier_proof_chunk_mismatch')
        packed.extend(data)
    import io
    with gzip.GzipFile(fileobj=io.BytesIO(packed)) as zipped:
        raw = zipped.read(MAX_ARTIFACT + 1)
    if len(raw) != manifest['raw_size'] or sha(raw) != digest:
        raise ValueError('verifier_proof_artifact_digest_mismatch')
    return raw


def history_parts(locators, index_rows):
    """Resolve old inline chunks from the already-existing immutable archive."""
    store = objects()
    body, generation = store.read(HISTORY_KEY)
    if body is None or generation != HISTORY_GENERATION or sha(body) != HISTORY_SHA256:
        raise ValueError('historical_proof_manifest_mismatch')
    manifest = json.loads(body)
    if (manifest.get('complete') is not True or manifest.get('tab') != 'Verifier Closeout Proof'
            or manifest.get('rows') != 4554 or manifest.get('columns') != 8):
        raise ValueError('historical_proof_manifest_binding_mismatch')
    cache, output = {}, []
    if len(locators) != len(index_rows):
        raise ValueError('historical_proof_index_length_mismatch')
    for locator, index in zip(locators, index_rows):
        if not re.fullmatch(re.escape(HISTORY_MARKER) + r'[0-9]{1,4}', locator):
            raise ValueError('invalid_historical_proof_locator')
        row = int(locator[len(HISTORY_MARKER):])
        if not 2 <= row <= 4554:
            raise ValueError('invalid_historical_proof_row')
        matches = [x for x in manifest['chunks'] if x['row'] <= row < x['row'] + x['count']]
        if len(matches) != 1:
            raise ValueError('historical_proof_chunk_coverage_mismatch')
        item = matches[0]
        expected_key = HISTORY_KEY.rsplit('/', 1)[0] + '/rows-%05d.json.gz' % item['row']
        if item['key'] != expected_key or not 1 <= item['count'] <= 10:
            raise ValueError('historical_proof_chunk_reference_mismatch')
        if item['key'] not in cache:
            packed, current = store.read(item['key'])
            if packed is None or current != item['generation'] or len(packed) > MAX_ARTIFACT:
                raise ValueError('historical_proof_chunk_generation_mismatch')
            import io
            with gzip.GzipFile(fileobj=io.BytesIO(packed)) as zipped:
                raw = zipped.read(MAX_ARTIFACT + 1)
            if len(raw) > MAX_ARTIFACT or sha(raw) != item['sha256']:
                raise ValueError('historical_proof_chunk_hash_mismatch')
            values = json.loads(raw)
            if not isinstance(values, list) or len(values) != item['count']:
                raise ValueError('historical_proof_chunk_shape_mismatch')
            cache[item['key']] = values
        original = cache[item['key']][row - item['row']]
        if (len(original) != 8 or [str(v) for v in original[:7]] != [str(v) for v in index]
                or not isinstance(original[7], str) or original[7].startswith(HISTORY_MARKER)):
            raise ValueError('historical_proof_row_binding_mismatch')
        output.append(original[7])
    return output
