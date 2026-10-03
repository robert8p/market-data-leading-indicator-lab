"""Private, bounded W10 host preparation. No discovery/validation/trading entrypoint."""
from __future__ import annotations
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import resource
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
import urllib.error
import urllib.request

LOG = logging.getLogger(__name__)
ROOT = Path('/tmp/astra-eq20-w10')
MANIFEST_SHA = 'f037125e1a9749f265add32258a938e9a568b4deb632f234b405fd9e39788707'
RPC_NAME = 'eq20_w10_host_prepare_v1'
MAX_REPLY = 16 * 1024 * 1024
_started = False
_start_lock = threading.Lock()


def file_hash(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    with temporary.open('wb') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def verified_part(data, expected):
    if len(data) != expected['payload_bytes']:
        raise ValueError('PART_BYTE_COUNT_MISMATCH')
    if hashlib.sha256(data).hexdigest() != expected['payload_sha256']:
        raise ValueError('PART_HASH_MISMATCH')
    if data.count(b'\n') != expected['records'] or not data.endswith(b'\n'):
        raise ValueError('PART_RECORD_COUNT_MISMATCH')
    return True


class RPC:
    def __init__(self):
        self.base = os.environ.get('SUPABASE_URL', '').strip().rstrip('/')
        self.key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
        if not self.base.startswith('https://') or not self.key:
            raise RuntimeError('PRIVATE_SERVICE_ROLE_CONFIGURATION_MISSING')

    def call(self, op, owner, fence=None, args=None):
        body = json.dumps({'p_op': op, 'p_owner': owner, 'p_fence': fence, 'p_args': args or {}}).encode()
        request = urllib.request.Request(self.base + '/rest/v1/rpc/' + RPC_NAME, data=body,
            headers={'Authorization': 'Bearer ' + self.key, 'apikey': self.key,
                     'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = response.read(MAX_REPLY + 1)
        except urllib.error.HTTPError as exc:
            # Do not log request headers, private payloads, keys, or arbitrary error bodies.
            raise RuntimeError('PRIVATE_RPC_HTTP_' + str(exc.code)) from None
        if len(raw) > MAX_REPLY:
            raise RuntimeError('PRIVATE_RPC_RESPONSE_TOO_LARGE')
        return json.loads(raw)


def child_export(owner, fence, attempt, part_nos):
    signal.alarm(150)
    resource.setrlimit(resource.RLIMIT_CPU, (28, 30))
    resource.setrlimit(resource.RLIMIT_FSIZE, (32 * 1024 * 1024, 32 * 1024 * 1024))
    rpc = RPC()
    manifest = rpc.call('manifest', owner, fence)
    if manifest['manifest_sha256'] != MANIFEST_SHA:
        raise ValueError('FROZEN_MANIFEST_PIN_MISMATCH')
    atomic_write(ROOT / 'export_manifest.json', manifest['manifest_text'].encode('utf-8'))
    if file_hash(ROOT / 'export_manifest.json') != MANIFEST_SHA:
        raise ValueError('FROZEN_MANIFEST_BYTES_MISMATCH')
    metadata = {m['part_no']: m for m in manifest['manifest']['part_metadata']}
    receipts = []
    for number in part_nos:
        expected = metadata[number]
        path = ROOT / ('transitions_%02d.jsonl' % number)
        if path.is_file():
            data = path.read_bytes()
            verified_part(data, expected)
        else:
            fetched = rpc.call('part', owner, fence, {'part_no': number, 'attempt_id': attempt})
            data = fetched['jsonl_payload'].encode('utf-8')
            verified_part(data, expected)
            atomic_write(path, data)
            if file_hash(path) != expected['payload_sha256']:
                raise ValueError('HOST_READBACK_HASH_MISMATCH')
        receipt = {'part_no': number, 'payload_sha256': expected['payload_sha256'],
                   'payload_bytes': expected['payload_bytes'], 'records': expected['records'],
                   'attempt_id': attempt, 'host_instance': socket.gethostname(),
                   'local_readback_verified': True, 'scientific_definitions_changed': False,
                   'protected_outcomes_accessed': False, 'discovery_fits_executed': 0}
        rpc.call('commit', owner, fence, receipt)
        receipts.append(receipt)
        del data
    return receipts


def export_child_main(argv):
    owner, fence, attempt, numbers = argv
    result = {'process_finished': True, 'phase': 'HOST_EXPORT_INSTALLATION_ONLY',
              'discovery_fits_executed': 0, 'protected_outcomes_accessed': False}
    try:
        result['parts'] = child_export(owner, int(fence), attempt, json.loads(numbers))
        result['success'] = True
    except BaseException as exc:
        result['success'] = False
        result['error_type'] = type(exc).__name__
        result['error'] = str(exc)[:120]
    atomic_write(ROOT / ('receipt_' + attempt + '.json'), json.dumps(result, sort_keys=True).encode())
    return 0 if result['success'] else 1


def one_cycle(rpc, owner, stop):
    status = rpc.call('status', owner)
    if not status['enabled']:
        return 60
    if status['state'] == 'STOPPED_ERROR':
        return 300
    if status['state'] == 'WAITING_PRIVATE_RUNTIME_AND_CACHE_INSTALLATION' and status.get('host_instance') == socket.gethostname():
        return 300
    claim = rpc.call('claim', owner, args={'host_instance': socket.gethostname(), 'implementation_sha256': file_hash(__file__)})
    if not claim.get('acquired'):
        return 30
    fence = claim['fence']
    # New hosts do not treat receipts from an old ephemeral filesystem as files.
    manifest = rpc.call('manifest', owner, fence)
    if manifest['manifest_sha256'] != MANIFEST_SHA:
        raise ValueError('FROZEN_MANIFEST_PIN_MISMATCH')
    if shutil.disk_usage(ROOT).free < 256 * 1024 * 1024:
        rpc.call('release', owner, fence, {'state': 'STOPPED_ERROR', 'error': 'INSUFFICIENT_VERIFIED_LOCAL_DISK'})
        return 300
    parts = manifest['manifest']['part_metadata']
    while not stop.is_set():
        status = rpc.call('status', owner)
        if not status['enabled']:
            rpc.call('release', owner, fence, {'state': 'PAUSED'})
            return 60
        done = set(status['completed_parts']) if status.get('host_instance') == socket.gethostname() else set()
        remaining = [p['part_no'] for p in parts if p['part_no'] not in done]
        if not remaining:
            rpc.call('release', owner, fence, {'state': 'WAITING_PRIVATE_RUNTIME_AND_CACHE_INSTALLATION'})
            LOG.info('EQ20 host export installed: 30 verified parts; private runtime/cache installation remains; fits=0')
            return 300
        reservation = rpc.call('reserve', owner, fence, {'attempt_key': 'host_export_' + uuid.uuid4().hex})
        attempt = reservation['attempt_id']
        receipt_path = ROOT / ('receipt_' + attempt + '.json')
        command = [sys.executable, str(Path(__file__).resolve()), '--export-child', owner,
                   str(fence), attempt, json.dumps(remaining[:5])]
        env = dict(os.environ)
        env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1])
        env['EQ20_HOST_PREPARE_ENABLED'] = 'false'
        env['OPENBLAS_NUM_THREADS'] = '1'
        env['OMP_NUM_THREADS'] = '1'
        process = None
        try:
            process = subprocess.Popen(command, env=env, start_new_session=True,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            started = time.monotonic()
            while process.poll() is None:
                if stop.wait(5) or time.monotonic() - started > 150:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                    break
                control = rpc.call('heartbeat', owner, fence)
                if not control.get('continue'):
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                    break
            result = json.loads(receipt_path.read_bytes()) if receipt_path.is_file() else {
                'process_finished': True, 'success': False, 'error': 'CHILD_TERMINATED_NO_RECEIPT'}
            result['exit_code'] = process.returncode
            result['process_finished'] = process.poll() is not None
            rpc.call('settle', owner, fence, {'attempt_id': attempt, 'receipt': result})
            if not result.get('success') or process.returncode != 0:
                rpc.call('release', owner, fence, {'state': 'STOPPED_ERROR', 'error': result.get('error', 'CHILD_FAILED')})
                return 300
            LOG.info('EQ20 host preparation committed parts=%s fence=%s fits=0', len(result['parts']), fence)
        finally:
            if process is not None and process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
    rpc.call('release', owner, fence, {'state': 'PAUSED'})
    return 60


def run_loop():
    ROOT.mkdir(parents=True, exist_ok=True)
    lock = (ROOT / 'host.lock').open('a+')
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return
    owner = 'render_eq20_host_' + socket.gethostname() + '_' + uuid.uuid4().hex[:12]
    stop = threading.Event()
    failures = 0
    while not stop.is_set():
        try:
            delay = one_cycle(RPC(), owner, stop)
            failures = 0
        except Exception as exc:
            failures += 1
            LOG.warning('EQ20 host preparation error_type=%s; no launch attempted', type(exc).__name__)
            delay = min(300, 15 * (2 ** min(failures, 4)))
        stop.wait(delay)


def start_background():
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
        threading.Thread(target=run_loop, daemon=True, name='eq20-host-prepare').start()


if __name__ == '__main__':
    if len(sys.argv) == 6 and sys.argv[1] == '--export-child':
        raise SystemExit(export_child_main(sys.argv[2:]))
    raise SystemExit('Only the bounded export child entrypoint is permitted')
