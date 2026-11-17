"""Create-only private verifier artifacts; native Sheets keeps a small locator."""
import gzip
import hashlib
import json
import re

PREFIX = 'ops/verifier-proof-v1/'
MARKER = 'gcs-proof-v1:'
MAX_ARTIFACT = 64 * 1024 * 1024
CHUNK = 400 * 1024


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
