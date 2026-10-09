"""Scoped runtime benchmarks and durable validation handoffs on the existing worker."""
import base64
import gzip
import hashlib
import io
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
from urllib.request import Request

VERSION = 'expedite_20261009_v2'
RPCS = {'probe': 'market_data_remediation_performance_probe_v2',
        'validation': 'market_data_remediation_validation_step_v2'}
MAX_RAW = 8 * 1024 * 1024
MAX_COMPRESSED = 1024 * 1024


class ScopedRpc:
    def __init__(self, base, existing):
        self.base, self.existing = base, existing

    def call(self, operation, payload):
        if operation not in RPCS:
            raise self.base.WorkerFault('expedite_rpc_operation')
        name = RPCS[operation]
        data = json.dumps({'p_request': payload}, ensure_ascii=False,
                          separators=(',', ':'), allow_nan=False).encode('utf-8')
        if len(data) > 16 * 1024 * 1024:
            raise self.base.WorkerFault('expedite_payload_size')
        try:
            if self.existing.mode == 'rest':
                req = Request(self.existing.origin + '/rest/v1/rpc/' + name, data=data,
                    method='POST', headers={'Authorization': 'Bearer ' + self.existing.key,
                    'apikey': self.existing.key, 'Content-Type': 'application/json'})
                with self.existing.opener.open(req, timeout=45) as response:
                    raw = response.read(3 * MAX_COMPRESSED + 1)
                if len(raw) > 3 * MAX_COMPRESSED:
                    raise self.base.WorkerFault('expedite_response_size')
                result = json.loads(raw)
            else:
                from psycopg.types.json import Jsonb
                with self.existing.lock, self.existing.psycopg.connect(self.existing.db_url,
                        connect_timeout=15, application_name=VERSION) as connection:
                    connection.execute("set local statement_timeout='30s'")
                    # Name comes only from the fixed mapping above, never from a request.
                    result = connection.execute('select public.' + name + '(%s::jsonb)',
                        (Jsonb(payload),)).fetchone()[0]
            if not isinstance(result, dict):
                raise self.base.WorkerFault('expedite_response_shape')
            return result
        except HTTPError as error:
            raise self.base.WorkerFault('expedite_http_' + str(error.code),
                retryable=error.code in (408, 429, 500, 502, 503, 504))
        except (URLError, TimeoutError, ConnectionError):
            raise self.base.WorkerFault('expedite_transport', retryable=True)
        except (ValueError, TypeError):
            raise self.base.WorkerFault('expedite_response_invalid')


def retained_text(page):
    size, compressed_size = page.get('original_bytes'), page.get('compressed_bytes')
    if type(size) is not int or not 0 < size <= MAX_RAW:
        raise ValueError('raw_size')
    if type(compressed_size) is not int or not 0 < compressed_size <= MAX_COMPRESSED:
        raise ValueError('compressed_size')
    encoded = page.get('compressed_body_base64')
    if not isinstance(encoded, str) or len(encoded) > 3 * MAX_COMPRESSED:
        raise ValueError('encoded_size')
    compressed = base64.b64decode(''.join(encoded.split()), validate=True)
    if len(compressed) != compressed_size or hashlib.sha256(compressed).hexdigest() != page.get('compressed_sha256'):
        raise ValueError('compressed_hash')
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        raw = stream.read(MAX_RAW + 1)
    if len(raw) != size or hashlib.sha256(raw).hexdigest() != page.get('source_sha256'):
        raise ValueError('raw_hash')
    return raw.decode('utf-8', errors='strict')


class ExpediteWorker:
    def __init__(self, base, env, stop, rpc_factory=None):
        self.base, self.env, self.stop = base, env, stop
        self.rpc_factory = rpc_factory or (lambda: ScopedRpc(base, base.RpcClient(env)))
        self.rpc = self.rpc_factory()
        self.worker_id = 'validation-' + str(uuid.uuid4())
        self.benchmark_done = False
        self.pending = None
        self.turn = 'SOURCE'
        self.completed = 0

    def request(self, **fields):
        return dict(run_id=self.base.RUN_ID, version=VERSION, worker_id=self.worker_id, **fields)

    def probe(self, key):
        if self.stop.is_set():
            return {'status': 'PAUSED'}
        result = self.rpc_factory().call('probe', self.request(probe_key=key))
        if result.get('status') == 'MEASURED' and result.get('probe_key') != key:
            raise self.base.WorkerFault('probe_scope_mismatch')
        self.base.emit('expedite_performance_probe', probe_key=key,
                       status=result.get('status'), elapsed_seconds=result.get('elapsed_seconds'),
                       started_at=result.get('started_at'), finished_at=result.get('finished_at'),
                       replayed=result.get('replayed', False))
        return result

    def benchmark(self):
        # Durable per-key server records make restart/retry idempotent.
        for key in ('serial_a', 'serial_b', 'combined', 'small_a', 'small_b'):
            if self.probe(key).get('status') != 'MEASURED':
                return False
        if self.stop.is_set():
            return False
        # Each task creates its own transport and database connection.
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix='feature-probe') as pool:
            results = list(pool.map(self.probe, ('parallel_a', 'parallel_b')))
        self.benchmark_done = all(r.get('status') == 'MEASURED' for r in results)
        return self.benchmark_done

    def tick(self):
        if self.stop.is_set():
            return 'PAUSED'
        if self.pending:
            payload = self.request(**self.pending)
        else:
            payload = self.request(action='next', kind=self.turn)
            self.turn = 'FEATURE' if self.turn == 'SOURCE' else 'SOURCE'
        result = self.rpc.call('validation', payload)
        status = result.get('status')
        if status not in {'PAUSED','WAITING_DEPENDENCY','SOURCE_READY','NEEDS_EVIDENCE',
                'VALIDATED','PROMOTED','BLOCKED','WAITING_VALIDATION','FEATURE_RELEASE_PROMOTED'}:
            raise self.base.WorkerFault('validation_response_status')
        if self.pending and status not in {'PAUSED'}:
            if result.get('job_key') != self.pending['job_key']:
                raise self.base.WorkerFault('validation_commit_scope')
            self.pending = None
        if status == 'SOURCE_READY':
            if not str(result.get('job_key','')).startswith('SOURCE:') or not result.get('lease_token'):
                raise self.base.WorkerFault('validation_source_scope')
            self.pending = dict(job_key=result['job_key'],lease_token=result['lease_token'])
            try:
                self.pending.update(action='commit', raw_text=retained_text(result))
            except (ValueError, TypeError, OSError, EOFError):
                self.pending.update(action='fail')
        if status in {'VALIDATED','PROMOTED','NEEDS_EVIDENCE','BLOCKED'}:
            self.completed += 1
            self.base.emit('remediation_validation_checkpoint', status=status,
                job_key=result.get('job_key'), source_sha256=result.get('source_sha256'),
                promoted_rth_rows=result.get('promoted_rth_rows'),
                unresolved_listing_days=result.get('unresolved_listing_days'),
                sqlstate=result.get('sqlstate'), full_remediation_complete=False)
        if self.completed and self.completed % 25 == 0 and not self.pending:
            final = self.rpc.call('validation', self.request(action='finalize'))
            self.base.emit('remediation_release_handoff', status=final.get('status'),
                full_remediation_complete=False)
        return status

    def run(self, controls):
        while not self.stop.is_set():
            try:
                benchmark, validation = controls()
                if benchmark and not self.benchmark_done:
                    if not self.benchmark():
                        self.stop.wait(5)
                    continue
                if validation:
                    status = self.tick()
                    if status in {'PAUSED','WAITING_DEPENDENCY'}:
                        self.stop.wait(2)
                    else:
                        self.stop.wait(0.1)
                else:
                    self.stop.wait(2)
            except self.base.WorkerFault as error:
                self.base.emit('expedite_waiting', code=error.code, retryable=error.retryable)
                self.stop.wait(5 if error.retryable else 30)
            except Exception as error:
                self.base.emit('expedite_unexpected_failure', exception_class=type(error).__name__)
                self.stop.wait(30)
