"""Finite, outcome-blind official calendar preparation on the mission timer.

The exact shared parent debit is admitted by its private RPC. The standalone
calendar catalogue is not a selected evaluation window, eligible evidence or
proof that future execution has occurred. Existing PID identity, wait4 and
conservative recovery controls are retained from the reviewed bounded harness.
"""
from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
import logging
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
import zlib

VERSION = 'EQ20_CALENDAR_PREPARATION_V1'
RPC_NAME = 'eq20_calendar_preparation_v1'
ROOT = Path('/tmp/astra-eq20-w10/calendar_preparation')
MAX_REPLY = 3*1024*1024
MAX_COMPRESSED = 2*1024*1024
MAX_REQUEST = 2*1024*1024
MAX_JSON = 8*1024*1024
MAX_STREAM = 128*1024*1024
MAX_LINE = 1024*1024
LOG = logging.getLogger(__name__)
_timer_lock = threading.Lock()
_next_poll_at = 0.0
_helpers = {}
_last_control_proof = None

class Closed(ValueError):
    pass


def require(test, reason):
    if not test:
        raise Closed(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def load_local(name, expected=None):
    path = Path(__file__).with_name(name+'.py')
    if expected is not None:
        require(sha(path.read_bytes()) == expected, 'INSTALLED_COMPONENT_PIN_MISMATCH')
    if name not in _helpers:
        spec = importlib.util.spec_from_file_location('prospective_'+name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        _helpers[name] = module
    return _helpers[name]


def _self_module():
    class Current:
        pass
    obj = Current()
    obj.__dict__.update(globals())
    return obj


def guards():
    return load_local('eq20_source_supervisor')


def launch(command):
    child=guards().ReapedChild.__new__(guards().ReapedChild)
    try:
        child.__init__(command)
        return child
    except BaseException as error:
        if hasattr(child,'pid'):
            child.stop()
            if getattr(child,'finished',False):
                child.launch_error=type(error).__name__
                return child
        raise


def validate_request_bound(job):
    proof=job.get('request_timeout_verification',{})
    require(proof.get('verified_actual_http_request') is True
        and proof.get('query_timeout_seconds')==2 and proof.get('request_start_deadline_ms')==500
        and proof.get('post_helper_sql_tail_seconds')==3
        and proof.get('per_request_server_permit_required') is True
        and proof.get('client_clock_error_not_used_for_admission') is True
        and proof.get('clock_observation_is_historical_only') is True
        and proof.get('host_instance')==socket.gethostname()
        and proof.get('host_boot_id')==Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        and isinstance(proof.get('artifact_sha256'),str)
        and re.fullmatch(r'[0-9a-f]{64}',proof['artifact_sha256']),
        'ACTUAL_CALENDAR_REQUEST_BOUND_AND_SERVER_PERMIT_REQUIRED')
    return proof


def atomic(path, raw, immutable=True):
    require(len(raw) <= MAX_JSON and not path.is_symlink(), 'CALENDAR_SCRATCH_BOUND')
    if path.exists() and immutable:
        require(path.read_bytes() == raw, 'CALENDAR_IMMUTABLE_FILE_CONFLICT'); return
    require(guards().scratch_safe(len(raw)), 'EXISTING_SHARED_SCRATCH_CEILING')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with tmp.open('xb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.replace(tmp, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    finally:
        tmp.unlink(missing_ok=True)


def resolve(row, ref, kind, status):
    require(isinstance(ref, dict) and isinstance(row, dict), 'AUTHORITATIVE_REGISTRY_READBACK_REQUIRED')
    require(ref.get('kind') == kind and ref.get('status') == status
            and isinstance(ref.get('artifact_key'), str) and ref['artifact_key']
            and re.fullmatch(r'[0-9a-f]{64}', ref.get('implementation_sha256', '')) is not None
            and all(row.get(key) == ref.get(key) for key in ('kind', 'artifact_key', 'status', 'implementation_sha256')),
            'AUTHORITATIVE_REGISTRY_IDENTITY_OR_STATUS')
    raw = row.get('evidence_text')
    require(isinstance(raw, str) and len(raw.encode()) <= MAX_JSON
            and sha(raw.encode()) == ref['implementation_sha256'], 'AUTHORITATIVE_REGISTRY_CONTENT_HASH')
    obj = json.loads(raw)
    require(isinstance(obj, dict), 'REGISTRY_EVIDENCE_OBJECT_REQUIRED')
    return obj


def run_job(job, _fetch_unused=None):
    validate_request_bound(job)
    capture = load_local('eq20_prospective_capture', job.get('capture_module_sha256'))
    require(job.get('action') == 'CAPTURE_CALENDAR' and job.get('resource_reservation_verified') is True
            and job.get('reserved_governed_seconds') == 30
            and job.get('module_sha256') == sha(Path(__file__).read_bytes()),
            'EXACT_AUTHORIZED_CALENDAR_JOB_REQUIRED')
    authority = capture.resolve(job.get('authority_artifact'), job.get('authority_reference'),
        'EQ20_PROSPECTIVE_CALENDAR_PREPARATION_AUTHORITY', 'AUTHORIZED')
    require(authority.get('scope') == 'OUTCOME_BLIND_OFFICIAL_CALENDAR_METADATA_PREPARATION_ONLY'
            and authority.get('maximum_attempts') == 8 and authority.get('maximum_total_governed_seconds') == 240
            and authority.get('unchanged_parent_lifetime_governed_seconds') == 172800
            and authority.get('protected_outcomes_accessed') is False,
            'ACTUAL_FINITE_CALENDAR_AUTHORITY_REQUIRED')
    budget = capture.GovernedBudget(12, 6.2)
    receipt, payload = capture.ProviderClient(budget).capture(job['request'], job['provider_permit'])
    catalog = capture.calendar_catalog(payload)
    acknowledgement = budget.io('CALENDAR_REGISTRY_PUBLICATION', 1.9,
        lambda: RPC().direct('calendar_put', job['owner'], {
            'attempt_id': job['attempt_id'], 'provider_receipt': receipt, 'catalog': catalog,
            'catalog_canonical_sha256': sha(canonical(catalog))}))
    require(acknowledgement.get('committed') is True
            and acknowledgement.get('provider_raw_sha256') == receipt['raw_sha256']
            and acknowledgement.get('catalog_canonical_sha256') == sha(canonical(catalog)),
            'ACTUAL_CALENDAR_CAPTURE_READBACK_REQUIRED')
    return {'state': 'VERIFIED', 'artifact_key': acknowledgement['artifact_key'],
            'artifact_sha256': acknowledgement['artifact_sha256'],
            'provider_raw_sha256': receipt['raw_sha256'],
            'calendar_sessions': len(catalog['sessions']), 'governed_work': budget.receipt(),
            'verification_scope': 'ACTUAL_PROVIDER_CALENDAR_METADATA_CAPTURE_AND_REGISTERED_EXCHANGE_CROSSCHECK',
            'official_exchange_crosscheck_verified': acknowledgement.get('official_exchange_crosscheck_verified'),
            'evaluation_dates_selected': False, 'protected_outcomes_accessed': False,
            'research_objective_achieved': False}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise Closed('CALENDAR_RPC_REDIRECT_REJECTED')


class RPC:
    def direct(self, op, owner, args, *, transport_receipt=None):
        require(op in ('status','claim','claim_reconcile','calendar_put','terminal','terminal_replay'),'CALENDAR_RPC_OPERATION')
        helper=load_local('eq20_rpc_admission_v3','72fda15472338d0b49afc9ab342bfec4b91de743d76d2fd0c0b0293b1d91fdff')
        return helper.send({'p_op':op,'p_owner':owner,'p_args':dict(args)},
            target_rpc='public.'+RPC_NAME+'(text,text,jsonb)',post=self._post,canonical_bytes=canonical,
            host_instance=socket.gethostname(),boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            read_only=op=='status',transport_receipt=transport_receipt)

    def _post(self,rpc_name,raw):
        base=os.environ.get('SUPABASE_URL','').rstrip('/')
        key=os.environ.get('SUPABASE_SERVICE_ROLE_KEY','').strip()
        require(base=='https://oxzabweahkoimtevbbny.supabase.co' and key,'PRIVATE_CONFIGURATION_REQUIRED')
        request = urllib.request.Request(base+'/rest/v1/rpc/'+rpc_name, data=raw,
            headers={'Authorization': 'Bearer '+key, 'apikey': key, 'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.build_opener(NoRedirect()).open(request, timeout=1.7) as reply:
                result = reply.read(MAX_REPLY+1)
        except urllib.error.HTTPError as error:
            raise Closed('CALENDAR_RPC_HTTP_'+str(error.code)) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            raise Closed('CALENDAR_RPC_TRANSPORT_FAILURE') from None
        require(len(result) <= MAX_REPLY, 'CALENDAR_RPC_REPLY_BOUND')
        value = json.loads(result); require(isinstance(value, dict), 'CALENDAR_RPC_REPLY_OBJECT')
        return value

    def call(self, op, owner, args):
        return bounded_rpc(op, owner, args)


def bounded_rpc(op, owner, args):
    """Seven-second control envelope includes mint, mutation, wait4 and workload tail."""
    global _last_control_proof
    key = args['invocation_id'] if op == 'claim' else uuid.uuid4().hex
    require(re.fullmatch(r'[0-9a-f]{32}', key) is not None, 'CALENDAR_RPC_INVOCATION_ID')
    request = ROOT/('rpc_'+key+'.json'); reply = ROOT/('rpc_reply_'+key+'.json')
    atomic(request, canonical({'op': op, 'owner': owner, 'args': args}))
    began = time.monotonic(); cpu = time.process_time(); success = False; value = {}
    child = launch([sys.executable, '-I', '-S', str(Path(__file__).resolve()), '--rpc-helper', key])
    try:
        while not child.reap():
            if time.monotonic()-began > 2.1 or time.process_time()-cpu > 1:
                child.stop(); break
            time.sleep(.02)
        require(child.termination_proof is not None, 'CALENDAR_HTTP_HELPER_NOT_REAPED')
        value = json.loads(reply.read_bytes()) if reply.exists() else {}
        require(child.exit_code == 0 and value.get('success') is True, value.get('error', 'CALENDAR_HTTP_HELPER_FAILURE'))
        success = True; return value['response']
    finally:
        child.stop()
        elapsed=time.monotonic()-began
        _last_control_proof={'operation':op,'request_sha256':sha(request.read_bytes()),
            'process_finished':child.termination_proof=='SPECIFIC_CHILD_WAIT4',
            'process_identity':child.original_identity,'process_termination_proof':child.termination_proof,
            'child_cpu_seconds':child.cpu,'child_wall_seconds':elapsed,
            'parent_cpu_seconds':time.process_time()-cpu,'workload_tail_seconds':0 if success else 3,
            'governed_prefix_seconds':elapsed+(child.cpu or 0)+time.process_time()-cpu+(0 if success else 3),
            'response_readback_verified':success,'transport_receipt':value.get('transport_receipt'),
            'unknown_mint_may_remain_inert':not success,'permitted_workload_only_tail':True}
        if op == 'claim':
            # Retain the exact control request and actual helper termination,
            # including failed/lost responses. A later timer may reconcile only
            # this invocation, before any research launch marker exists.
            proof = {'version': 'EQ20_CALENDAR_CLAIM_HELPER_WAIT4_V1',
                'request_sha256': sha(request.read_bytes()), 'invocation_id': key,
                'host_instance': socket.gethostname(), 'process_identity': child.original_identity,
                'process_finished': child.termination_proof is not None,
                'process_termination_proof': child.termination_proof, 'exit_code': child.exit_code,
                'child_cpu_seconds': child.cpu, 'child_wall_seconds': time.monotonic()-began,
                'parent_cpu_seconds': time.process_time()-cpu,
                'finished_at': datetime.now(timezone.utc).isoformat(),
                'finished_monotonic': time.monotonic(), 'sql_tail_bound_seconds': 3,
                'permitted_workload_only_tail': True,
                'unknown_mint_may_remain_inert': not success,
                'transport_receipt': value.get('transport_receipt'),
                'response_readback_verified': success, 'research_child_launched': False}
            atomic(ROOT/('claim_helper_'+key+'.json'), canonical(proof))
        if not success:
            time.sleep(3)
        if op != 'claim':
            request.unlink(missing_ok=True); reply.unlink(missing_ok=True)


def cleanup_claim(invocation):
    if not isinstance(invocation, str) or re.fullmatch(r'[0-9a-f]{32}', invocation) is None:
        return
    for prefix in ('claim_', 'claim_helper_', 'claim_launch_', 'claim_recovery_count_', 'rpc_', 'rpc_reply_'):
        (ROOT/(prefix+invocation+'.json')).unlink(missing_ok=True)


def finish_local_settlement(attempt, invocation, owner=None, result=None):
    """A retained server ACK makes interrupted cleanup local and cost free."""
    require(re.fullmatch(r'[0-9a-f-]{36}', attempt or '') is not None, 'CALENDAR_SETTLEMENT_ATTEMPT_ID')
    marker = ROOT/('settled_'+attempt+'.json')
    if result is not None:
        require(result.get('committed') is True and result.get('attempt_id') == attempt,
                'CALENDAR_EXACT_SETTLEMENT_ACK_REQUIRED')
        atomic(marker, canonical({'version': 'EQ20_CALENDAR_SETTLEMENT_ACK_V1', 'attempt_id': attempt,
            'claim_invocation_id': invocation, 'owner': owner, 'committed': True,
            'server_response_sha256': sha(canonical(result)), 'server_state': result.get('state')}))
    else:
        ack = json.loads(marker.read_bytes())
        require(ack.get('version') == 'EQ20_CALENDAR_SETTLEMENT_ACK_V1' and ack.get('committed') is True
                and ack.get('attempt_id') == attempt and ack.get('claim_invocation_id') == invocation
                and re.fullmatch(r'[0-9a-f]{64}', ack.get('server_response_sha256', '')) is not None,
                'CALENDAR_RETAINED_SETTLEMENT_ACK_REQUIRED')
    for prefix in ('job_', 'receipt_', 'process_', 'terminal_', 'replay_count_'):
        (ROOT/(prefix+attempt+'.json')).unlink(missing_ok=True)
    cleanup_claim(invocation)
    # Keep ACK across the deletion barrier, then retire it. A crash can leave
    # either the ACK or an empty transaction footprint, never an ambiguous launch.
    descriptor = os.open(ROOT, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
        marker.unlink()
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def reconcile_claim(rpc, request_path, current_args):
    """Settle a known same-host no-launch claim; never retrieve/restart its job."""
    request_raw = request_path.read_bytes(); request = json.loads(request_raw)
    require(request.get('op') == 'claim' and isinstance(request.get('args'), dict), 'CALENDAR_CLAIM_ENVELOPE_REQUIRED')
    invocation = request['args'].get('invocation_id')
    require(re.fullmatch(r'[0-9a-f]{32}', invocation or '') is not None
            and request_path.name == 'claim_'+invocation+'.json', 'CALENDAR_CLAIM_ENVELOPE_IDENTITY')
    reason = None
    helper_path = ROOT/('claim_helper_'+invocation+'.json')
    if request['args'].get('host_instance') != socket.gethostname():
        reason = 'CLAIM_HOST_CHANGED_PHYSICAL_TERMINATION_UNKNOWN'
    elif (ROOT/('claim_launch_'+invocation+'.json')).exists() or any(ROOT.glob('job_*.json')) or any(ROOT.glob('process_*.json')):
        reason = 'CLAIM_RESEARCH_LAUNCH_BOUNDARY_REQUIRES_EXACT_TERMINATION'
    elif not helper_path.exists():
        reason = 'CLAIM_RPC_HELPER_EXACT_TERMINATION_PENDING'
    if reason:
        return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'dependencies': [reason], 'protected_outcomes_accessed': False}
    proof = json.loads(helper_path.read_bytes()); identity = proof.get('process_identity')
    require(proof.get('version') == 'EQ20_CALENDAR_CLAIM_HELPER_WAIT4_V1'
            and proof.get('request_sha256') == sha(request_raw)
            and proof.get('invocation_id') == invocation
            and proof.get('host_instance') == socket.gethostname()
            and proof.get('process_finished') is True
            and proof.get('process_termination_proof') == 'SPECIFIC_CHILD_WAIT4'
            and proof.get('research_child_launched') is False
            and guards().control_identity_equal(identity, identity)
            and identity.get('boot_id') == Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'CLAIM_EXACT_SAME_HOST_HELPER_TERMINATION_REQUIRED')
    require(all(type(proof.get(k)) in (int, float) and math.isfinite(proof[k]) and proof[k] >= 0
                for k in ('child_cpu_seconds', 'child_wall_seconds', 'parent_cpu_seconds', 'finished_monotonic')),
            'CLAIM_HELPER_MEASUREMENTS_REQUIRED')
    if time.monotonic() < proof['finished_monotonic']+3:
        return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'dependencies': ['CLAIM_RPC_SQL_TAIL_NOT_YET_DRAINED'], 'protected_outcomes_accessed': False}
    count_path = ROOT/('claim_recovery_count_'+invocation+'.json')
    count = json.loads(count_path.read_bytes()).get('attempts', 0) if count_path.exists() else 0
    if type(count) is not int or not 0 <= count < 2:
        return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'dependencies': ['FINITE_CLAIM_RECONCILIATION_LIMIT_OR_ACK_UNKNOWN'], 'protected_outcomes_accessed': False}
    replay_id = str(uuid.uuid4())
    recovery = dict(current_args, claim_invocation_id=invocation, claim_request_utf8=request_raw.decode(),
        claim_request_sha256=sha(request_raw), helper_wait4_receipt=proof,
        no_research_launch_marker_verified=True, replay_slot=count, replay_id=replay_id,
        current_boot_id=identity['boot_id'])
    atomic(count_path, canonical({'attempts': count+1, 'last_replay_id': replay_id}), immutable=False)
    result = rpc.call('claim_reconcile', request['owner'], recovery)
    if result.get('reconciled') is True:
        cleanup_claim(invocation)
    return result


def execute_reserved(rpc, owner, job, *, parent_cpu_start=None):
    cpu = time.process_time() if parent_cpu_start is None else parent_cpu_start
    require(job.get('reserved_governed_seconds') == 30, 'CALENDAR_RESERVATION_REQUIRED')
    attempt = job.get('attempt_id')
    require(isinstance(attempt, str) and re.fullmatch(r'[0-9a-f-]{36}', attempt), 'ATTEMPT_ID_REQUIRED')
    job_path = ROOT/('job_'+attempt+'.json'); receipt_path = ROOT/('receipt_'+attempt+'.json')
    require(not job_path.exists() and not receipt_path.exists(), 'CALENDAR_ATTEMPT_REEXECUTION_FORBIDDEN')
    atomic(job_path, canonical(job))
    began = time.monotonic()
    admission_control=_last_control_proof
    child = launch([sys.executable, '-I', '-S', str(Path(__file__).resolve()), '--job', owner, attempt])
    try:
        if child.original_identity:
            atomic(ROOT/('process_'+attempt+'.json'), canonical(child.original_identity))
        while not child.reap():
            if time.monotonic()-began > 7 or time.process_time()-cpu > .5:
                break
            time.sleep(.02)
    finally:
        child.stop()
    child_wall = time.monotonic()-began
    if child.exit_code != 0: time.sleep(3)
    receipt_raw = receipt_path.read_bytes() if receipt_path.exists() and receipt_path.stat().st_size <= MAX_JSON else None
    try:
        receipt = json.loads(receipt_raw) if receipt_raw is not None else None
        require(isinstance(receipt, dict) and len(canonical(receipt)) <= 52*1024, 'BOUNDED_TERMINAL_RECEIPT_REQUIRED')
    except (ValueError, TypeError):
        receipt = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                   'error': 'BOUNDED_CALENDAR_INVALID_RECEIPT' if receipt_path.exists() else 'BOUNDED_CALENDAR_NO_RECEIPT',
                   'protected_outcomes_accessed': 'UNKNOWN_READ_MAY_HAVE_OCCURRED',
                   'child_receipt_sha256': sha(receipt_raw) if receipt_raw is not None else None}
    parent_cpu = time.process_time()-cpu
    physical = child.termination_proof == 'SPECIFIC_CHILD_WAIT4' and guards().control_identity_equal(child.original_identity, child.original_identity)
    measured = all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in (child.cpu, child_wall, parent_cpu))
    compliant = physical and measured and child.cpu <= 5 and child_wall <= 7 and parent_cpu <= .5
    if not compliant:
        # Physical quiescence and resource compliance are different facts.
        # Preserve exact wait4 even when measured scheduling/CPU exceeds a
        # prescribed guard. Such work cannot be verified or automatically retried.
        receipt = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                   'error': 'CALENDAR_MEASURED_ENVELOPE_EXCEEDED' if physical and measured else 'CALENDAR_CHILD_TERMINATION_OR_MEASUREMENT_UNKNOWN',
                   'protected_outcomes_accessed': receipt.get('protected_outcomes_accessed', 'UNKNOWN_READ_MAY_HAVE_OCCURRED'),
                   'child_receipt_sha256': sha(receipt_raw) if receipt_raw is not None else None,
                   'research_objective_achieved': False}
    receipt.update(process_finished=physical, process_termination_proof=child.termination_proof,
        process_identity=child.original_identity, host_instance=socket.gethostname(), exit_code=child.exit_code,
        child_cpu_seconds=child.cpu, child_wall_seconds=child_wall,
        parent_cpu_seconds=parent_cpu, accounting_mode='FULL_RESERVATION_CONSERVATIVE',
        resource_envelope_verified=compliant,
        admission_control_termination=admission_control,
        claim_control_receipt=admission_control,
        governed_envelope={'control_and_terminal': 14, 'child_wall_bound': 7, 'child_cpu_bound': 5,
                          'sql_tail_bound': 3, 'parent_cpu_bound': .5, 'total_bound': 29.5, 'reserved': 30})
    require(len(canonical(receipt)) <= 60*1024, 'BOUNDED_TERMINAL_RECEIPT_REQUIRED')
    args = {'attempt_id': attempt, 'claim_invocation_id': job.get('claim_invocation_id'), 'receipt': receipt}
    atomic(ROOT/('terminal_'+attempt+'.json'), canonical({'owner': owner, 'args': args}))
    require(physical and measured, 'CALENDAR_CHILD_TERMINATION_OR_MEASUREMENT_UNKNOWN')
    result = rpc.call('terminal', owner, args)
    if result.get('committed') is True:
        finish_local_settlement(attempt, job.get('claim_invocation_id'), owner, result)
    return result


def supervise_once(rpc, owner, *, trigger, scheduled_at):
    parent_cpu_start = time.process_time()
    args = {'version': VERSION, 'module_sha256': sha(Path(__file__).read_bytes()), 'capture_module_sha256': sha(Path(__file__).with_name('eq20_prospective_capture.py').read_bytes()), 'host_instance': socket.gethostname(),
            'host_boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'trigger': trigger, 'scheduled_at': scheduled_at, 'invocation_id': uuid.uuid4().hex}
    settled = sorted(ROOT.glob('settled_*.json')) if ROOT.exists() else []
    if settled:
        require(len(settled) == 1, 'CALENDAR_MULTIPLE_PENDING_SETTLEMENT_CLEANUPS')
        ack = json.loads(settled[0].read_bytes())
        finish_local_settlement(ack.get('attempt_id'), ack.get('claim_invocation_id'))
        return {'state': 'VERIFIED', 'verification_scope': 'LOCAL_CLEANUP_OF_OBSERVED_SERVER_SETTLEMENT_ONLY',
                'committed': True, 'attempt_id': ack['attempt_id'], 'protected_outcomes_accessed': False,
                'research_objective_achieved': False}
    # Recover only an exact locally persisted wait4 terminal receipt. This
    # performs no market read and never restarts the historical child; each
    # additional transport consumes one of two prepaid12-second slots. Failed
    # terminal transactions cannot erase those admission-time debits.
    pending = sorted(ROOT.glob('terminal_*.json')) if ROOT.exists() else []
    if pending:
        require(len(pending) == 1, 'CALENDAR_MULTIPLE_PENDING_TERMINALS')
        request = json.loads(pending[0].read_bytes())
        attempt = request['args']['attempt_id']
        count_path = ROOT/('replay_count_'+attempt+'.json')
        count = json.loads(count_path.read_bytes()).get('attempts', 0) if count_path.exists() else 0
        if count >= 2:
            return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                    'dependencies': ['FINITE_IMMUTABLE_TERMINAL_REPLAY_LIMIT_OR_ACK_UNKNOWN'],
                    'protected_outcomes_accessed': False, 'next_poll_seconds': 300}
        replay = dict(request['args'], replay_id=str(uuid.uuid4()), replay_slot=count, **args)
        atomic(count_path, canonical({'attempts': count+1, 'last_replay_id': replay['replay_id']}), immutable=False)
        result = rpc.call('terminal_replay', request['owner'], replay)
        if result.get('committed') is True:
            finish_local_settlement(attempt, request['args'].get('claim_invocation_id'), request['owner'], result)
        return result
    pending_claims = sorted(path for path in ROOT.glob('claim_*.json')
        if re.fullmatch(r'claim_[0-9a-f]{32}\.json', path.name)) if ROOT.exists() else []
    if pending_claims:
        require(len(pending_claims) == 1, 'CALENDAR_MULTIPLE_PENDING_CLAIMS')
        return reconcile_claim(rpc, pending_claims[0], args)
    # Read-only when current true prerequisites do not permit a calendar job.
    atomic(ROOT/('claim_'+args['invocation_id']+'.json'), canonical({'op': 'claim', 'owner': owner, 'args': args}))
    job = rpc.call('claim', owner, args)
    if job.get('action') not in ('CAPTURE_CALENDAR',):
        cleanup_claim(args['invocation_id'])
        return job
    require(job.get('claim_invocation_id') in (None, args['invocation_id']), 'CALENDAR_CLAIM_RESPONSE_INVOCATION')
    job = dict(job, claim_invocation_id=args['invocation_id'])
    atomic(ROOT/('claim_launch_'+args['invocation_id']+'.json'), canonical({'attempt_id': job.get('attempt_id'),
        'invocation_id': args['invocation_id'], 'host_instance': socket.gethostname(), 'research_launch_intent': True}))
    return execute_reserved(rpc, owner, job, parent_cpu_start=parent_cpu_start)


def timer_tick(owner, *, scheduled_at=None, trigger='PERSISTENT_WORKER_STARTUP', rpc=None):
    global _next_poll_at
    require(trigger in ('PERSISTENT_WORKER_STARTUP', 'PERSISTENT_WORKER_TIMER'), 'HONEST_WORKER_TRIGGER_REQUIRED')
    with _timer_lock:
        now = time.monotonic()
        if now < _next_poll_at:
            return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'reason': 'BOUNDED_METADATA_CADENCE', 'next_poll_seconds': 300}
        _next_poll_at = now+300
    return supervise_once(rpc or RPC(), owner, trigger=trigger, scheduled_at=time.time() if scheduled_at is None else scheduled_at)


def _rpc_helper(key):
    require(re.fullmatch(r'[0-9a-f]{32}', key), 'RPC_HELPER_KEY')
    signal.signal(signal.SIGALRM, signal.SIG_DFL); signal.setitimer(signal.ITIMER_REAL, 2)
    signal.signal(signal.SIGPROF, signal.SIG_DFL); signal.setitimer(signal.ITIMER_PROF, .5)
    guards().prohibit_descendants()
    request = json.loads((ROOT/('rpc_'+key+'.json')).read_bytes())
    transport_receipt={}
    try:
        value = {'success': True, 'response': RPC().direct(request['op'], request['owner'], request['args'],transport_receipt=transport_receipt)}
    except Exception as error:
        value = {'success': False, 'error': str(error) if re.fullmatch(r'[A-Z0-9_]{1,200}', str(error)) else type(error).__name__}
    value['transport_receipt']=transport_receipt
    with (ROOT/('rpc_reply_'+key+'.json')).open('xb') as out:
        out.write(canonical(value)); out.flush(); os.fsync(out.fileno())
    return 0 if value['success'] else 1


def _job(owner, attempt):
    require(re.fullmatch(r'[0-9a-f-]{36}', attempt), 'JOB_ATTEMPT_KEY')
    signal.signal(signal.SIGALRM, signal.SIG_DFL); signal.setitimer(signal.ITIMER_REAL, 6.8)
    signal.signal(signal.SIGPROF, signal.SIG_DFL); signal.setitimer(signal.ITIMER_PROF, 4.8)
    resource.setrlimit(resource.RLIMIT_AS, (192*1024*1024, 192*1024*1024))
    resource.setrlimit(resource.RLIMIT_FSIZE, (256*1024*1024, 256*1024*1024))
    guards().prohibit_descendants()
    job = json.loads((ROOT/('job_'+attempt+'.json')).read_bytes())
    job['_deadline'] = time.monotonic()+6.3
    job['owner'] = owner
    try:
        receipt = run_job(job)
    except Exception as error:
        receipt = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                   'error': str(error) if re.fullmatch(r'[A-Z0-9_]{1,200}', str(error)) else type(error).__name__,
                   'protected_outcomes_accessed': False,
                   'research_objective_achieved': False}
    atomic(ROOT/('receipt_'+attempt+'.json'), canonical(receipt))
    return 0 if receipt['state'] in ('VERIFIED', 'RUNNING') else 1


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--rpc-helper':
        raise SystemExit(_rpc_helper(sys.argv[2]))
    if len(sys.argv) == 4 and sys.argv[1] == '--job':
        raise SystemExit(_job(sys.argv[2], sys.argv[3]))
    raise SystemExit('Only an admitted outcome-blind calendar preparation job is executable')
