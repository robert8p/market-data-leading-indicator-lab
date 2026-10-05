"""Finite actual-worker PostgREST timeout and host-clock pilot.

The authority is read from the database and every admitted probe is charged54
seconds before any experimental HTTP request. No credential, source outcome,
role setting or research owner is exported or changed. A received SQLSTATE57014
is distinguished from a client timeout. The existing sequential worker timer
invokes this module; it creates no schedule, thread or additional worker service.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import resource
import signal
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid

VERSION = 'EQ20_POSTGREST_TIMEOUT_PILOT_V3'
RPC_NAME = 'eq20_postgrest_timeout_pilot_v3'
PROBE_NAME = 'eq20_prospective_timeout_probe_v3'
ROOT = Path('/tmp/astra-eq20-w10/request_timeout_probe')
MAX_BYTES = 262144
MAX_HTTP = 65536
_lock = threading.Lock()
_next_poll = 0.
_guards = None


class Closed(ValueError):
    pass


def require(test, reason):
    if not test:
        raise Closed(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def stamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(result.tzinfo is not None, 'PROBE_EXPLICIT_UTC_TIME_REQUIRED')
    return result.astimezone(timezone.utc)


def guards():
    global _guards
    if _guards is None:
        path = Path(__file__).with_name('eq20_source_supervisor.py')
        spec = importlib.util.spec_from_file_location('eq20_timeout_source_guards', path)
        _guards = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = _guards
        spec.loader.exec_module(_guards)
    return _guards


def atomic(path, value, *, immutable=True):
    raw = canonical(value)
    require(len(raw) <= MAX_BYTES and not path.is_symlink(), 'PROBE_BOUNDED_SAFE_FILE_REQUIRED')
    if path.exists() and immutable:
        require(path.read_bytes() == raw, 'PROBE_IMMUTABLE_LOCAL_RECEIPT_CHANGED')
        return
    require(guards().scratch_safe(len(raw)), 'PROBE_EXISTING_SHARED_SCRATCH_CAP')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with tmp.open('xb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.replace(tmp, path)
        handle = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(handle)
        finally: os.close(handle)
    finally:
        tmp.unlink(missing_ok=True)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise Closed('PROBE_AUTHENTICATED_ROUTE_REDIRECT_REJECTED')


def http_request(rpc_name, payload, *, timeout):
    """Return exact bounded HTTP bytes; transport failure is never SQLSTATE57014."""
    require(rpc_name in (RPC_NAME, PROBE_NAME), 'PROBE_ONLY_REGISTERED_RPC_NAMES')
    base = os.environ.get('SUPABASE_URL', '').strip().rstrip('/')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
    require(base == 'https://oxzabweahkoimtevbbny.supabase.co' and key,
            'PROBE_EXISTING_WORKER_PRIVATE_CONFIGURATION_REQUIRED')
    raw = canonical(payload)
    require(len(raw) <= MAX_BYTES, 'PROBE_REQUEST_BYTES_BOUND')
    request = urllib.request.Request(base+'/rest/v1/rpc/'+rpc_name, data=raw,
        headers={'Authorization': 'Bearer '+key, 'apikey': key, 'Content-Type': 'application/json'}, method='POST')
    began = datetime.now(timezone.utc); monotonic = time.monotonic()
    try:
        try:
            reply = urllib.request.build_opener(NoRedirect()).open(request, timeout=timeout)
        except urllib.error.HTTPError as error:
            reply = error
        with reply:
            body = reply.read(MAX_HTTP+1)
            status = reply.code
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        raise Closed('PROBE_HTTP_TRANSPORT_UNFINISHED') from None
    ended = datetime.now(timezone.utc); elapsed = time.monotonic()-monotonic
    require(len(body) <= MAX_HTTP and math.isfinite(elapsed) and elapsed >= 0,
            'PROBE_RESPONSE_BYTES_OR_TIME_BOUND')
    # A UTC clock step during a call prevents a valid clock calibration.
    require(abs((ended-began).total_seconds()-elapsed) <= .005, 'PROBE_HOST_CLOCK_STEPPED_DURING_REQUEST')
    text = body.decode('utf-8')
    value = json.loads(text)
    require(isinstance(value, dict), 'PROBE_HTTP_JSON_OBJECT_REQUIRED')
    return {'path': '/rpc/'+rpc_name, 'request_utf8': raw.decode(), 'request_sha256': sha(raw),
        'response_utf8': text, 'response_sha256': sha(body), 'http_status': status,
        'actual_request_completed': True, 'client_started_at': began.isoformat(),
        'client_finished_at': ended.isoformat(), 'elapsed_seconds': elapsed}, value


def verify_observations(job, observations):
    require(isinstance(observations, list) and 3 <= len(observations) <= 6,
            'PROBE_FINITE_HTTP_OBSERVATIONS_REQUIRED')
    pins = job.get('rpc_function_pins')
    require(isinstance(pins, dict) and json.loads(job['rpc_function_pins_text']) == pins
        and sha(job['rpc_function_pins_text'].encode()) == job.get('rpc_pinset_sha256'),
        'PROBE_ACTUAL_JOB_PINSET_READBACK_REQUIRED')
    count = 0; overflow = 0; late = 0; best = None
    for item in observations:
        require(item.get('actual_request_completed') is True
            and item.get('path') in ('/rpc/'+PROBE_NAME, '/rpc/'+RPC_NAME)
            and sha(item['request_utf8'].encode()) == item.get('request_sha256')
            and sha(item['response_utf8'].encode()) == item.get('response_sha256'),
            'PROBE_ACTUAL_HTTP_BYTE_HASH_REQUIRED')
        request = json.loads(item['request_utf8']); body = json.loads(item['response_utf8'])
        elapsed = item.get('elapsed_seconds')
        require(type(elapsed) in (float, int) and math.isfinite(elapsed) and 0 <= elapsed <= 3.2,
                'PROBE_ACTUAL_REQUEST_ELAPSED_BOUND')
        if item.get('mode') == 'late_admission':
            require(item.get('path') == '/rpc/'+RPC_NAME and request.get('p_op') == 'status'
                and request.get('p_owner') == job['owner']
                and set(request.get('p_args', {})) == {'request_start_deadline_at'}
                and stamp(request['p_args']['request_start_deadline_at']) < stamp(item['client_started_at'])-timedelta(seconds=60)
                and item.get('http_status') == 400 and body.get('code') == 'P0001'
                and body.get('message') == 'EQ20_TIMEOUT_PILOT_LATE_REQUEST_REJECTED',
                'PROBE_ACTUAL_LATE_ADMISSION_REJECTION_REQUIRED')
            late += 1
            continue
        require(item.get('path') == '/rpc/'+PROBE_NAME
            and request == {'p_mode': item.get('mode'), 'p_nonce': job['nonce']}, 'PROBE_EXACT_REQUEST_NONCE')
        if item['mode'] == 'metadata':
            count += 1
            require(item['http_status'] == 200 and body.get('status') == 'OBSERVED_SETTINGS_ONLY'
                and body.get('nonce') == job['nonce'] and body.get('request_role') == 'service_role'
                and body.get('request_path') == '/rpc/'+PROBE_NAME
                and body.get('rpc_function_pins') == pins, 'PROBE_ACTUAL_RPC_SETTINGS_AND_PINS')
            lower = (stamp(body['server_finished_at'])-stamp(item['client_finished_at'])).total_seconds()*1000
            upper = (stamp(body['server_started_at'])-stamp(item['client_started_at'])).total_seconds()*1000
            require(lower <= upper and all(math.isfinite(x) for x in (lower, upper)), 'PROBE_VALID_CLOCK_OFFSET_INTERVAL')
            error = max(abs(lower), abs(upper))
            if best is None or error < best['maximum_clock_error_ms']:
                best = {'maximum_clock_error_ms': error, 'lower_offset_ms': lower, 'upper_offset_ms': upper,
                    'observed_at': body['server_finished_at'], 'request_sha256': item['request_sha256'],
                    'response_sha256': item['response_sha256']}
        else:
            require(item['mode'] == 'fixed_overflow' and item['http_status'] == 500
                and body.get('code') == '57014' and body.get('message') == 'canceling statement due to statement timeout'
                and 1.8 <= elapsed <= 2.5,
                'PROBE_SERVER_TWO_SECOND_CANCELLATION_NOT_CLIENT_TIMEOUT')
            overflow += 1
    require(1 <= count <= 4 and overflow == 1 and late == 1 and best is not None
            and best['maximum_clock_error_ms'] <= 100, 'PROBE_CLOCK_BOUND_OR_TIMEOUT_NOT_OBSERVED')
    return best


def run_probe(job, *, transport=http_request, record_observation=lambda value: None):
    require(job.get('version') == VERSION and job.get('module_sha256') == sha(Path(__file__).read_bytes())
        and job.get('reserved_governed_seconds') == 30 and job.get('total_funded_seconds') == 54,
        'PROBE_PINNED_REAL_FINITE_RESERVATION_REQUIRED')
    require(job.get('host_instance') == socket.gethostname()
        and job.get('boot_id') == Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
        'PROBE_CURRENT_ACTUAL_HOST_REQUIRED')
    observations = []; began = time.monotonic(); cpu = time.process_time()
    for index in range(4):
        require(time.monotonic()-began+1.8+3.2+1.8 < 15 and time.process_time()-cpu < 2.5,
                'PROBE_BOUNDED_CLOCK_OBSERVATION_BUDGET')
        item, body = transport(PROBE_NAME, {'p_mode': 'metadata', 'p_nonce': job['nonce']}, timeout=1.5)
        item['mode'] = 'metadata'; observations.append(item); record_observation(item)
        require(item['http_status'] == 200 and body.get('rpc_function_pins') == job['rpc_function_pins'],
                'PROBE_RPC_PINSET_CHANGED_OR_METADATA_FAILED')
        lower = (stamp(body['server_finished_at'])-stamp(item['client_finished_at'])).total_seconds()*1000
        upper = (stamp(body['server_started_at'])-stamp(item['client_started_at'])).total_seconds()*1000
        if lower <= upper and max(abs(lower), abs(upper)) <= 100:
            break
    require(time.monotonic()-began+3.2+1.8 < 15 and time.process_time()-cpu < 2.5,
            'PROBE_FIXED_TIMEOUT_PROBE_BUDGET')
    item, _ = transport(PROBE_NAME, {'p_mode': 'fixed_overflow', 'p_nonce': job['nonce']}, timeout=3.0)
    item['mode'] = 'fixed_overflow'; observations.append(item); record_observation(item)
    require(time.monotonic()-began+1.8 < 15 and time.process_time()-cpu < 2.5, 'PROBE_LATE_ADMISSION_TEST_BUDGET')
    item, _ = transport(RPC_NAME, {'p_op': 'status', 'p_owner': job['owner'],
        'p_args': {'request_start_deadline_at': (datetime.now(timezone.utc)-timedelta(days=1)).isoformat()}}, timeout=1.5)
    item['mode'] = 'late_admission'; observations.append(item); record_observation(item)
    clock = verify_observations(job, observations)
    return {'state': 'VERIFIED', 'observations': observations, 'clock_measurement': clock,
        'protected_outcomes_accessed': False, 'research_objective_achieved': False}


def bounded_control(op, owner, args):
    key = uuid.uuid4().hex
    args = dict(args, request_start_deadline_at=(datetime.now(timezone.utc)+timedelta(seconds=.5)).isoformat())
    request = {'p_op': op, 'p_owner': owner, 'p_args': args}
    atomic(ROOT/('control_'+key+'.json'), request)
    child = guards().ReapedChild([sys.executable, '-I', '-S', str(Path(__file__).resolve()), '--control', key])
    began = time.monotonic(); start_cpu = time.process_time()
    try:
        while not child.reap():
            if time.monotonic()-began >= 2.5 or time.process_time()-start_cpu >= .2:
                child.stop(); break
            time.sleep(.01)
    finally:
        child.stop()
        proof = {'process_identity': child.original_identity, 'process_finished': child.termination_proof is not None,
            'process_termination_proof': child.termination_proof, 'exit_code': child.exit_code,
            'child_cpu_seconds': child.cpu, 'child_wall_seconds': time.monotonic()-began,
            'parent_cpu_seconds': time.process_time()-start_cpu,
            'request_sha256': sha(canonical(request)), 'host_instance': socket.gethostname()}
        atomic(ROOT/('control_wait4_'+key+'.json'), proof)
    reply_path = ROOT/('control_reply_'+key+'.json')
    require(child.termination_proof == 'SPECIFIC_CHILD_WAIT4' and child.cpu is not None
        and child.cpu <= 1 and proof['child_wall_seconds'] <= 2.5 and reply_path.exists(),
        'PROBE_CONTROL_UNKNOWN_OR_OUTSIDE_ENVELOPE')
    observed = json.loads(reply_path.read_bytes())
    require(observed.get('actual_request_completed') is True, 'PROBE_CONTROL_HTTP_ACK_REQUIRED')
    value = json.loads(observed['response_utf8'])
    require(observed['http_status'] == 200 and isinstance(value, dict), 'PROBE_CONTROL_RPC_REJECTED')
    return value, {'http': observed, 'wait4': proof, 'request': request, 'local_control_key': key}


def retire_control(observed):
    """Remove transport scratch only after its complete receipt is retained."""
    key = observed.get('local_control_key') if isinstance(observed, dict) else None
    if not isinstance(key, str) or re.fullmatch(r'[0-9a-f]{32}', key) is None:
        return
    for prefix in ('control_', 'control_wait4_', 'control_reply_'):
        (ROOT/(prefix+key+'.json')).unlink(missing_ok=True)
    descriptor = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)


def submit_terminal(owner, job, receipt, *, control=bounded_control):
    attempt = job['attempt_id']; path = ROOT/('replay_'+attempt+'.json')
    prior = json.loads(path.read_bytes()) if path.exists() else {'next_slot': 0}
    require(type(prior.get('next_slot')) is int and 0 <= prior['next_slot'] <= 2,
            'PROBE_FINITE_PREPAID_TERMINAL_SLOTS_EXHAUSTED')
    slot = prior['next_slot']; invocation = uuid.uuid4().hex
    atomic(path, {'next_slot': slot+1}, immutable=False)
    args = {'attempt_id': attempt, 'receipt': receipt}
    if slot:
        args.update(replay_slot=slot, replay_invocation_id=invocation)
    result, observed = control('terminal' if slot == 0 else 'terminal_replay', owner, args)
    require(result.get('committed') is True and result.get('attempt_id') == attempt,
            'PROBE_EXACT_COMMITTED_TERMINAL_ACK_REQUIRED')
    atomic(ROOT/('ack_'+attempt+'.json'), {'result': result, 'control': observed})
    retire_control(observed)
    # ACK survives cleanup; interrupted cleanup never permits a new probe launch.
    (ROOT/('active_'+attempt+'.json')).unlink(missing_ok=True)
    (ROOT/('intent_'+job['invocation_id']+'.json')).unlink(missing_ok=True)
    descriptor = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)
    return result


def supervise_once(owner, scheduled_at, trigger, *, control=bounded_control):
    require(guards().memory_safe() and guards().scratch_safe(MAX_BYTES), 'PROBE_EXISTING_RESOURCE_CEILINGS')
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    active = sorted(ROOT.glob('active_*.json'))
    require(len(active) <= 1, 'PROBE_SINGLE_DURABLE_OWNER_REQUIRED')
    if active:
        job = json.loads(active[0].read_bytes()); attempt = job['attempt_id']
        if (ROOT/('ack_'+attempt+'.json')).exists():
            ack = json.loads((ROOT/('ack_'+attempt+'.json')).read_bytes())
            active[0].unlink();(ROOT/('intent_'+job['invocation_id']+'.json')).unlink(missing_ok=True)
            return ack['result']
        require(job['host_instance'] == socket.gethostname() and job['boot_id'] == Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                'PROBE_PRIOR_HOST_PHYSICAL_TERMINATION_UNVERIFIED')
        terminal = ROOT/('terminal_'+attempt+'.json')
        require(terminal.exists(), 'PROBE_ACTUAL_PRIOR_PHYSICAL_TERMINATION_REQUIRED')
        return submit_terminal(job['owner'], job, json.loads(terminal.read_bytes()), control=control)
    require(not list(ROOT.glob('intent_*.json')), 'PROBE_LOST_CLAIM_REQUIRES_EXACT_RECONCILIATION')
    invocation = uuid.uuid4().hex
    args = {'version': VERSION, 'module_sha256': sha(Path(__file__).read_bytes()),
        'host_instance': socket.gethostname(), 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
        'invocation_id': invocation, 'scheduled_at': scheduled_at, 'trigger': trigger}
    atomic(ROOT/('intent_'+invocation+'.json'), {'owner': owner, 'args': args})
    cpu_start = time.process_time()
    job, claim = control('claim', owner, args)
    if job.get('state') != 'RUNNING':
        (ROOT/('intent_'+invocation+'.json')).unlink()
        retire_control(claim)
        return job
    require(job.get('invocation_id') == invocation, 'PROBE_EXACT_CLAIM_RESPONSE_REQUIRED')
    attempt = job['attempt_id']; atomic(ROOT/('active_'+attempt+'.json'), job)
    atomic(ROOT/('claim_'+attempt+'.json'), claim);retire_control(claim)
    child = guards().ReapedChild([sys.executable, '-I', '-S', str(Path(__file__).resolve()), '--probe', attempt])
    began = time.monotonic(); launch_failure = None
    try:
        # No fallible operation after Popen may sit outside guaranteed reaping.
        # A process-marker failure must also reach truthful terminal persistence.
        atomic(ROOT/('process_'+attempt+'.json'), {'process_identity': child.original_identity})
        while not child.reap():
            if time.monotonic()-began >= 16 or time.process_time()-cpu_start >= .5:
                child.stop(); break
            time.sleep(.02)
    except Exception as error:
        launch_failure = type(error).__name__
    finally:
        child.stop()
    outcome = ROOT/('outcome_'+attempt+'.json')
    value = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'reason': 'PROBE_CHILD_HAS_NO_COMPLETE_HTTP_RECEIPT'}
    if launch_failure is not None:
        value.update(reason='PROBE_POSTLAUNCH_JOURNAL_OR_MONITOR_FAILURE', failure_type=launch_failure)
    elif outcome.exists():
        try:
            value = json.loads(outcome.read_bytes())
            require(isinstance(value, dict), 'PROBE_CHILD_OUTCOME_OBJECT_REQUIRED')
        except Exception as error:
            value = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                'reason': 'PROBE_CHILD_OUTCOME_READBACK_FAILED', 'failure_type': type(error).__name__}
    value.update(version=VERSION, attempt_id=attempt, module_sha256=job['module_sha256'],
        host_instance=job['host_instance'], boot_id=job['boot_id'], process_identity=child.original_identity,
        process_finished=child.termination_proof is not None, process_termination_proof=child.termination_proof,
        exit_code=child.exit_code, child_cpu_seconds=child.cpu, child_wall_seconds=time.monotonic()-began,
        parent_cpu_seconds=time.process_time()-cpu_start, claim_control_receipt=claim,
        accounting_mode='FULL54_CONSERVATIVE_PREDEBIT_NO_REFUND', protected_outcomes_accessed=False)
    # Preserve actual termination before any compliance test. An overrun must
    # become a truthful blocked settlement, never an apparent still-live child.
    if child.cpu is None or child.cpu > 3 or value['child_wall_seconds'] > 16 or value['parent_cpu_seconds'] > .5:
        value['state'] = 'BLOCKED_BY_IDENTIFIED_DEPENDENCY'; value['reason'] = 'PROBE_MEASURED_ENVELOPE_EXCEEDED'
    atomic(ROOT/('terminal_'+attempt+'.json'), value)
    require(child.termination_proof == 'SPECIFIC_CHILD_WAIT4' and child.cpu is not None,
            'PROBE_EXACT_WAIT4_REQUIRED_FOR_SETTLEMENT')
    return submit_terminal(owner, job, value, control=control)


def timer_tick(owner, *, scheduled_at=None, trigger='PERSISTENT_WORKER_STARTUP', control=None):
    global _next_poll
    require(trigger in ('PERSISTENT_WORKER_STARTUP','PERSISTENT_WORKER_TIMER'), 'PROBE_ACTUAL_TIMER_TRIGGER')
    with _lock:
        now = time.monotonic()
        if now < _next_poll:
            return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'reason': 'PROBE_BOUNDED_TIMER_CADENCE'}
        _next_poll = now+300
    return supervise_once(owner, time.time() if scheduled_at is None else scheduled_at, trigger,
                          control=control or bounded_control)


def child_main(mode, key):
    require(re.fullmatch(r'[0-9a-f-]{32,36}', key), 'PROBE_SAFE_CHILD_KEY')
    resource.setrlimit(resource.RLIMIT_AS, (128*1024*1024,128*1024*1024))
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_BYTES,MAX_BYTES))
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.signal(signal.SIGPROF, signal.SIG_DFL)
    signal.setitimer(signal.ITIMER_REAL, 2.3 if mode=='--control' else 15.5)
    signal.setitimer(signal.ITIMER_PROF, .9 if mode=='--control' else 2.9)
    guards().prohibit_descendants()
    if mode=='--control':
        request = json.loads((ROOT/('control_'+key+'.json')).read_bytes())
        observation, _ = http_request(RPC_NAME, request, timeout=1.8)
        atomic(ROOT/('control_reply_'+key+'.json'), observation)
        return 0
    job = json.loads((ROOT/('active_'+key+'.json')).read_bytes())
    observations = []
    def record(item):
        observations.append(item)
        atomic(ROOT/('observations_'+key+'.json'), observations, immutable=False)
    try:
        result = run_probe(job, record_observation=record)
    except Exception as error:
        result = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
            'reason': str(error) if re.fullmatch(r'[A-Z0-9_]{1,180}', str(error)) else type(error).__name__,
            'observations': observations, 'protected_outcomes_accessed': False}
    atomic(ROOT/('outcome_'+key+'.json'), result)
    return 0


if __name__ == '__main__':
    if len(sys.argv) != 3 or sys.argv[1] not in ('--control','--probe'):
        raise SystemExit(2)
    raise SystemExit(child_main(sys.argv[1],sys.argv[2]))
