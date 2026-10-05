"""Governed pre-release input readback and synthetic final-configuration QA.

This lane breaks the circular requirement to finish QA before the only input
preparation can start. It uses the existing FP01 finite allocation and foreground
ownership; scientific fitting remains behind separate source and release gates.
Private engine bytes are read by hash. QA outputs never become research trials.
"""
from __future__ import annotations
import base64
from datetime import datetime, timezone
import fcntl
import hashlib
import importlib.util
import json
import logging
import math
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import socket
import sqlite3
import sys
import threading
import time
from types import SimpleNamespace
import urllib.error
import urllib.request
import uuid
import zlib
from itertools import zip_longest

VERSION = 'EQ20_FP01_PRERELEASE_QA_V2_20261004'
WAVE = 'FP01'
ENTRYPOINT = Path(__file__).resolve()
RPC_NAME = 'eq20_fp01_prerelease_qa_v1'
ROOT = Path('/tmp/astra-eq20-w10/mission_continuation/prerelease_qa')
MAX_FILE = 64 * 1024 * 1024
MAX_REPLY = 3 * 1024 * 1024
MAX_QA_RAW = 128 * 1024 * 1024
MAX_QA_FILE = 32 * 1024 * 1024
QA_PART = 128 * 1024
MAX_QA_PACKED = 16 * 1024 * 1024
SQL_TAIL_SECONDS = 3
CONTROL_GOVERNED_SECONDS = 7
QA_CHILD_CPU_SECONDS = 5
SHA256 = re.compile(r'[0-9a-f]{64}')
BASE_SHA = '00e6a4c6e6245b4a8e2a398561fa8a51a01dc502273aaebbd446bd25cc7af346'
BOUND_SHA = '5730d2f79ab96557b25679dd984a3c56bb7864c7a1b5b0d34b97c7528c815006'
POLICY_SHA = '323c48c56e816106096da4148c3b223ae7d2ffe5bb7170df20eb8c9704342abf'
SCOPE_SHA = 'bb797e6337663bfc7cc08c54d083bb8e1711a52fc97ee793e5a19095e90c079f'
CONTRACT_SHA = 'a3b1fa43d92ba5315005697da952f9574d44f18a1290c1992edb9027f2ee1b34'
RPC_ADMISSION_SHA = '72fda15472338d0b49afc9ab342bfec4b91de743d76d2fd0c0b0293b1d91fdff'
_stop = threading.Event()
_lock = threading.Lock()
_last_poll = 0.0
_last_rpc_proof = None
_source_guards = None
_rpc_admission = None
_rpc_transport_path = None
_DIRECT_RPC = False
_ACCOUNT_WORK_DEADLINE = float('inf')
LOG = logging.getLogger(__name__)

class GateClosed(ValueError):
    """An explicit dependency; never evidence of research failure or success."""

class RegisteredSliceYield(Exception):
    """Exit only after a committed engine checkpoint; never expand a lease."""

def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()

def digest(value):
    return hashlib.sha256(value).hexdigest()

def object_hash(value):
    return digest(canonical_bytes(value))

def require(condition, reason):
    if not condition:
        raise GateClosed(reason)

def require_hash(value, reason='IMMUTABLE_HASH_REQUIRED'):
    require(isinstance(value, str) and SHA256.fullmatch(value), reason)
    return value

def atomic_file(path, raw, *, immutable=True):
    require(isinstance(raw, bytes) and len(raw) <= MAX_FILE, 'MISSION_SCRATCH_FILE_BOUND')
    require(not path.is_symlink(), 'MISSION_SCRATCH_SYMLINK_REJECTED')
    if path.exists() and immutable:
        require(path.read_bytes() == raw, 'MISSION_IMMUTABLE_FILE_CONFLICT')
        return
    require(source_guards().scratch_safe(len(raw)), 'EXISTING_SHARED_SCRATCH_CEILING')
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('xb') as handle:
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)

def source_guards():
    """Reuse reviewed PID identity, wait4 and kernel isolation controls."""
    global _source_guards
    if _source_guards is None:
        path = Path(__file__).with_name('eq20_source_supervisor.py')
        spec = importlib.util.spec_from_file_location('eq20_mission_source_guards', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _source_guards = module
    return _source_guards

def rpc_admission():
    global _rpc_admission
    if _rpc_admission is None:
        path=Path(__file__).with_name('eq20_rpc_admission_v3.py')
        require(path.is_file() and not path.is_symlink() and digest(path.read_bytes())==RPC_ADMISSION_SHA,
                'QA_SHARED_SERVER_PERMIT_IMPLEMENTATION_PIN_REQUIRED')
        spec=importlib.util.spec_from_file_location('eq20_qa_rpc_admission_v3',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        _rpc_admission=module
    return _rpc_admission

class MissionRPC:
    def __init__(self):
        self.base = os.environ.get('SUPABASE_URL', '').rstrip('/')
        self.key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
        require(self.base == 'https://oxzabweahkoimtevbbny.supabase.co' and self.key,
                'PRIVATE_CONFIGURATION_REQUIRED')

    def call(self, op, owner, args=None):
        if not _DIRECT_RPC:
            return bounded_rpc(op, owner, args or {})
        return self.direct_call(op, owner, args)

    def direct_call(self, op, owner, args=None):
        document=dict(p_op=op,p_owner=owner,p_args=dict(args or {}))
        transport={}
        path=_rpc_transport_path or ROOT/('direct_rpc_transport_'+str(os.getpid())+'.json')
        def save_transport(_stage=None,_wire=None):
            # The returned server permit has only a500ms admission window.
            # The durable original request already exists. Publish a conservative
            # pre-mint intent and the final census; never scan or fsync between
            # receiving the permit and sending its one exact mutation.
            if _stage=='PERMITTED_MUTATION':return
            atomic_file(path,canonical_bytes(transport),immutable=False)
        try:
            return rpc_admission().send(document,target_rpc='public.'+RPC_NAME+'(text,text,jsonb)',
                post=self._post,canonical_bytes=canonical_bytes,host_instance=socket.gethostname(),
                boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                read_only=(op=='status'),transport_receipt=transport,before_send=save_transport)
        finally:
            if transport:save_transport()

    def _post(self, rpc_name, body):
        require(isinstance(body,bytes) and len(body)<=MAX_REPLY,'QA_PERMIT_WIRE_BODY_BOUND')
        request = urllib.request.Request(self.base + '/rest/v1/rpc/' + rpc_name, data=body,
            headers={'Authorization': 'Bearer ' + self.key, 'apikey': self.key,
                     'Content-Type': 'application/json'}, method='POST')
        try:
            opener = urllib.request.build_opener(NoRedirect())
            with opener.open(request, timeout=1.7) as response:
                raw = response.read(MAX_REPLY + 1)
        except urllib.error.HTTPError as exc:
            try:
                message = json.loads(exc.read(4096)).get('message', '')
                code = message if isinstance(message, str) and re.fullmatch(r'[A-Z0-9_]{1,160}', message) else 'DATABASE_ERROR'
            except Exception:
                code = 'DATABASE_ERROR'
            raise GateClosed('MISSION_RPC_HTTP_%s_%s' % (exc.code, code)) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            # Never replay reservations or running compute after unknown acknowledgement.
            raise GateClosed('MISSION_RPC_TRANSPORT_FAILURE') from None
        require(len(raw) <= MAX_REPLY, 'MISSION_RPC_RESPONSE_BOUND')
        result = json.loads(raw)
        require(isinstance(result, dict), 'MISSION_RPC_RESPONSE_SHAPE')
        return result

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *unused, **kwargs):
        raise GateClosed('MISSION_RPC_REDIRECT_REJECTED')

def bounded_rpc(op, owner, args):
    """Seven-second control envelope, including the bounded admission/query tail."""
    global _last_rpc_proof
    control_cpu_started=time.process_time();control_started=time.monotonic()
    require(isinstance(args,dict),'QA_EXACT_LOGICAL_RPC_ARGUMENTS_REQUIRED')
    args={key:value for key,value in args.items()
          if key not in ('request_start_deadline_at','_eq20_request_permit')}
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    key = uuid.uuid4().hex
    request_path = ROOT / ('rpc_' + key + '.json')
    response_path = ROOT / ('rpc_reply_' + key + '.json')
    transport_path = ROOT / ('rpc_transport_' + key + '.json')
    request_raw = canonical_bytes(dict(op=op, owner=owner, args=args))
    invocation=args.get('invocation_id') if op=='tick' else None
    if invocation is not None:
        require(isinstance(invocation,str) and re.fullmatch(r'[0-9a-f]{32}',invocation),'QA_ADMISSION_INVOCATION_REQUIRED')
        atomic_file(ROOT/('admission_request_'+invocation+'.json'),request_raw)
    atomic_file(request_path, request_raw)
    guards = source_guards()
    cpu_start = time.process_time()
    child = guards.ReapedChild([sys.executable, '-I', '-S', str(ENTRYPOINT), '--rpc-helper', key])
    started = time.monotonic()
    success = False
    try:
        while not child.reap():
            if time.monotonic() - started > 2.1 or time.process_time() - cpu_start > 1.0:
                child.stop(); break
            time.sleep(.02)
        require(child.termination_proof is not None and child.cpu is not None,
                'MISSION_RPC_HELPER_TERMINATION_UNVERIFIED')
        if response_path.exists():
            value = json.loads(response_path.read_bytes())
            if value.get('success') is True and child.exit_code == 0:
                success = True
                return value['response']
            raise GateClosed(value.get('error', 'MISSION_RPC_HELPER_FAILED'))
        raise GateClosed('MISSION_RPC_HELPER_NO_RECEIPT')
    finally:
        child.stop()
        if not success:
            # A request may have reached PostgreSQL just before helper death.
            # A workload mutation needs a received server-minted500ms permit.
            # Its query timer is2s. Local UTC drift is not an admission premise.
            # An unreceived mint may remain inert; it cannot authorize workload.
            until = time.monotonic() + SQL_TAIL_SECONDS
            while time.monotonic() < until:
                time.sleep(min(.05, until - time.monotonic()))
        transport_proof=None
        if transport_path.exists():
            require(not transport_path.is_symlink() and transport_path.stat().st_size<=16384,
                    'QA_BOUNDED_ACTUAL_PERMIT_TRANSPORT_PROOF_REQUIRED')
            transport_proof=json.loads(transport_path.read_bytes())
            require(isinstance(transport_proof,dict)
                    and transport_proof.get('logical_request_sha256')==object_hash(dict(p_op=op,p_owner=owner,p_args=args)),
                    'QA_EXACT_LOGICAL_PERMIT_TRANSPORT_BINDING_REQUIRED')
        _last_rpc_proof = dict(operation=op, request_sha256=digest(request_raw),
            attempt_id=args.get('attempt_id'), host_instance=socket.gethostname(),
            process_identity=child.original_identity, process_finished=bool(child.termination_proof),
            process_termination_proof=child.termination_proof, exit_code=child.exit_code,
            measured_child_cpu_seconds=child.cpu, bounded_governed_seconds=CONTROL_GOVERNED_SECONDS,
            measured_parent_cpu_seconds=time.process_time()-control_cpu_started,
            observed_control_elapsed_seconds=time.monotonic()-control_started,
            control_governed_prefix_seconds=(time.process_time()-control_cpu_started)+(child.cpu or 0)+(time.monotonic()-control_started),
            control_bound_exceeded=((time.process_time()-control_cpu_started)+(child.cpu or 0)+(time.monotonic()-control_started)>CONTROL_GOVERNED_SECONDS),
            proof_scope='EXACT_HTTP_HELPER_AND_PERMITTED_WORKLOAD_SQL_TAIL',
            server_permit_transport=transport_proof,server_permit_transport_source_sha256=RPC_ADMISSION_SHA,
            unreceived_permit_cannot_authorize_workload=True,
            all_possible_metadata_mint_sql_termination_claimed=False,
            response_return_authorized=success,
            sql_tail_waited_seconds=0 if success else SQL_TAIL_SECONDS,
            completed_at=datetime.now(timezone.utc).isoformat())
        if invocation is not None:
            atomic_file(ROOT/('admission_proof_'+invocation+'.json'),canonical_bytes(_last_rpc_proof))
        if op=='terminal_recover' and not args.get('replay_id') and isinstance(args.get('attempt_id'),str):
            # Save the original recovery helper's physical proof before a new
            # timer/RPC can replace the ordinary latest-proof pointer.
            atomic_file(ROOT/('recovery_rpc_proof_'+args['attempt_id']+'.json'),canonical_bytes(_last_rpc_proof))
        atomic_file(ROOT / 'last_rpc_proof.json', canonical_bytes(_last_rpc_proof), immutable=False)
        request_path.unlink(missing_ok=True); response_path.unlink(missing_ok=True);transport_path.unlink(missing_ok=True)

def execute_reserved_child(rpc, owner, job):
    """One charged segment, one child, one durable terminal request."""
    require(job.get('reserved_cpu_seconds') == 30, 'CHILD_SEGMENT_RESERVATION_REQUIRED')
    attempt = job['attempt_id']
    require(isinstance(attempt, str) and re.fullmatch(r'[0-9a-f-]{36}', attempt), 'CHILD_ATTEMPT_FORMAT')
    admission_error='EXPLICIT_STOP_PRESERVED' if _stop.is_set() else None
    admission_proof=_last_rpc_proof
    if isinstance(admission_proof,dict) and admission_proof.get('control_bound_exceeded') is True:
        admission_error='QA_OBSERVED_ADMISSION_CONTROL_BOUND_VIOLATION'
    if admission_error is None:
        try:
            validate_request_bound(job)
        except GateClosed as error:
            admission_error=str(error)
    if admission_error is not None:
        ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
        args = dict(attempt_id=attempt, receipt=dict(
            state='EXPLICIT_STOP_PRESERVED', error=admission_error, attempt_id=attempt, process_started=False,
            process_finished=True, process_identity=None,
            process_termination_proof='NO_CHILD_STARTED_AFTER_ADMISSION_STOP',
            host_instance=socket.gethostname(), protected_outcomes_accessed=False,
            admission_control_termination=admission_proof,
            accounting_mode='FULL_RESERVATION_CONSERVATIVE'))
        atomic_file(ROOT / ('terminal_' + attempt + '.json'),
                    canonical_bytes(dict(operation='terminal_fail', owner=owner, args=args)))
        return rpc.call('terminal_fail', owner, args)
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    job_path = ROOT / ('job_' + attempt + '.json')
    receipt_path = ROOT / ('receipt_' + attempt + '.json')
    require(not job_path.exists() and not receipt_path.exists(), 'ATTEMPT_REEXECUTION_FORBIDDEN')
    atomic_file(job_path, canonical_bytes(job))
    guards = source_guards()
    began = time.monotonic(); parent_cpu = time.process_time()
    process = guards.ReapedChild([sys.executable, '-I', '-S', str(ENTRYPOINT),
                                  '--reserved-child', owner, attempt])
    launch_error = None
    try:
        if process.original_identity is not None:
            atomic_file(ROOT / ('process_' + attempt + '.json'), canonical_bytes(process.original_identity))
        while not process.reap():
            if time.monotonic() - began > 7.0 or time.process_time() - parent_cpu > .5:
                break
            time.sleep(.02)
    except Exception as exc:
        launch_error = type(exc).__name__
    finally:
        # Failure to persist an identity or read a receipt must not leave a child
        # running. Only this wait4 owner consumes or signals that exact child.
        process.stop()
    require(process.termination_proof is not None and process.cpu is not None,
            'CHILD_TERMINATION_UNVERIFIED')
    # Client death alone does not end a DB statement. Charge the fixed SQL tail.
    if process.exit_code != 0:
        time.sleep(SQL_TAIL_SECONDS)
    try:
        receipt = (json.loads(receipt_path.read_bytes()) if receipt_path.exists() and not launch_error else
                   dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY', error=launch_error or 'BOUNDED_CHILD_NO_RECEIPT',
                        protected_outcomes_accessed=False))
    except Exception:
        receipt = dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY', error='CHILD_RECEIPT_READBACK_REJECTED',
                       protected_outcomes_accessed=False)
    receipt.update(attempt_id=attempt, process_finished=True, exit_code=process.exit_code,
                   process_termination_proof=process.termination_proof,
                   process_identity=process.original_identity, host_instance=socket.gethostname(),
                   child_cpu_seconds=process.cpu, child_wall_seconds=time.monotonic()-began,
                   parent_cpu_seconds=time.process_time()-parent_cpu,
                   accounting_mode='FULL_RESERVATION_CONSERVATIVE',
                   governed_envelope=dict(control_and_terminal=2*CONTROL_GOVERNED_SECONDS, child_wall_bound=7,
                       child_cpu_bound=QA_CHILD_CPU_SECONDS, sql_tail_bound=SQL_TAIL_SECONDS, parent_cpu_bound=.5,
                       total_bound=29.5, reserved=30))
    operation = ('terminal_commit' if receipt.get('state') == 'VERIFIED' and process.exit_code == 0
                 else 'terminal_progress' if receipt.get('state') == 'RUNNING' and process.exit_code == 0
                 else 'terminal_fail')
    request = dict(operation=operation, owner=owner, args=dict(attempt_id=attempt, receipt=receipt))
    # Preserve the exact terminal receipt before transport. An unknown ACK can
    # never restart compute or silently reset this attempt's resource allowance.
    atomic_file(ROOT / ('terminal_' + attempt + '.json'), canonical_bytes(request))
    return rpc.call(operation, owner, request['args'])

def _recovery_terminal(rpc,job,args):
    request=dict(operation='terminal_recover',owner=job['original_owner'],args=args)
    atomic_file(ROOT/('recovery_terminal_'+job['attempt_id']+'.json'),canonical_bytes(request))
    return rpc.call('terminal_recover',job['original_owner'],args)


def recover_undelivered_admission(rpc,job):
    """A failed exact admission HTTP handoff cannot have launched its child."""
    invocation=job.get('original_invocation_id');attempt=job['attempt_id']
    require(isinstance(invocation,str) and re.fullmatch(r'[0-9a-f]{32}',invocation),
            'QA_ORIGINAL_ADMISSION_INVOCATION_REQUIRED')
    request_path=ROOT/('admission_request_'+invocation+'.json')
    proof_path=ROOT/('admission_proof_'+invocation+'.json')
    for path in (request_path,proof_path):
        require(path.is_file() and not path.is_symlink() and path.stat().st_size<=1024*1024,
                'QA_DURABLE_ORIGINAL_ADMISSION_REQUEST_AND_WAIT4_REQUIRED')
    raw=request_path.read_bytes();request=json.loads(raw);proof=json.loads(proof_path.read_bytes())
    require(raw==canonical_bytes(request) and request==dict(op='tick',owner=job['original_owner'],
            args=job.get('original_tick_arguments')) and request['args'].get('invocation_id')==invocation,
            'QA_EXACT_ORIGINAL_ADMISSION_REQUEST_BINDING_REQUIRED')
    identity=proof.get('process_identity')
    require(proof.get('operation')=='tick' and proof.get('request_sha256')==digest(raw)
            and proof.get('response_return_authorized') is False
            and proof.get('process_finished') is True and proof.get('process_termination_proof')=='SPECIFIC_CHILD_WAIT4'
            and proof.get('host_instance')==socket.gethostname() and isinstance(identity,dict)
            and type(identity.get('pid')) is int and identity['pid']>1
            and identity.get('process_group')==identity['pid'] and type(identity.get('start_ticks')) is int
            and identity.get('boot_id')==Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'QA_ACTUAL_UNDELIVERED_ADMISSION_HELPER_PROOF_REQUIRED')
    require(not(ROOT/('admission_received_'+invocation+'.json')).exists()
            and not(ROOT/('process_'+attempt+'.json')).exists(),
            'QA_ADMISSION_RECEIVED_OR_CHILD_LAUNCH_MARKER_PRESENT')
    time.sleep(SQL_TAIL_SECONDS)
    receipt=dict(state='EXPLICIT_STOP_PRESERVED',error='QA_LOST_ADMISSION_ACK_NO_CHILD',
        process_started=False,process_finished=True,process_identity=None,
        process_termination_proof='NO_CHILD_STARTED_AFTER_ADMISSION_STOP',attempt_id=attempt,
        host_instance=socket.gethostname(),protected_outcomes_accessed=False,
        accounting_mode='FULL_RESERVATION_CONSERVATIVE')
    terminal=dict(operation='terminal_fail',owner=job['original_owner'],args=dict(attempt_id=attempt,receipt=receipt))
    atomic_file(ROOT/('terminal_'+attempt+'.json'),canonical_bytes(terminal))
    recovery=dict(state='EXPLICIT_STOP_PRESERVED',process_started=False,process_finished=True,
        process_identity=None,process_termination_proof='QA_UNDELIVERED_ADMISSION_ACTUAL_HELPER_WAIT4',
        admission_request_canonical_utf8=raw.decode(),admission_request_sha256=digest(raw),
        admission_helper_termination=proof,original_invocation_id=invocation,
        received_or_launch_marker_present=False,sql_tail_waited_seconds=SQL_TAIL_SECONDS,
        attempt_id=attempt,host_instance=socket.gethostname(),protected_outcomes_accessed=False,
        accounting_mode='FULL_RESERVATION_CONSERVATIVE')
    return _recovery_terminal(rpc,job,dict(attempt_id=attempt,recovery_id=job['recovery_id'],
        receipt=receipt,original_terminal_operation='terminal_fail',recovery_receipt=recovery))


def recover_recorded_attempt(rpc, job):
    """Prove local PID quiescence; never infer another host's death from expiry."""
    attempt = job.get('attempt_id')
    require(isinstance(attempt, str) and re.fullmatch(r'[0-9a-f-]{36}', attempt), 'RECOVERY_ATTEMPT_FORMAT')
    require(job.get('reserved_cpu_seconds') == 30 and isinstance(job.get('recovery_id'), str)
            and re.fullmatch(r'[0-9a-f-]{36}', job['recovery_id']), 'SEPARATELY_RESERVED_RECOVERY_REQUIRED')
    validate_request_bound(job)
    require(job.get('host_instance') == socket.gethostname(), 'PRIOR_HOST_TERMINATION_RECEIPT_REQUIRED')
    identity_path = ROOT / ('process_' + attempt + '.json')
    terminal_path = ROOT / ('terminal_' + attempt + '.json')
    if not identity_path.exists() and not terminal_path.exists():
        return recover_undelivered_admission(rpc,job)
    if not identity_path.exists() and terminal_path.is_file():
        require(not terminal_path.is_symlink() and terminal_path.stat().st_size <= 1024 * 1024,
                'ORIGINAL_TERMINAL_FILE_REJECTED')
        saved_raw = terminal_path.read_bytes(); saved = json.loads(saved_raw)
        prior = saved.get('args', {}).get('receipt', {})
        if prior.get('process_started') is False:
            require(saved.get('owner') == job['original_owner'] and saved.get('operation') == 'terminal_fail'
                    and saved.get('args', {}).get('attempt_id') == attempt
                    and prior.get('attempt_id') == attempt and prior.get('host_instance') == socket.gethostname()
                    and prior.get('process_finished') is True and prior.get('process_identity') is None
                    and prior.get('process_termination_proof') == 'NO_CHILD_STARTED_AFTER_ADMISSION_STOP'
                    and prior.get('protected_outcomes_accessed') is False
                    and prior.get('accounting_mode') == 'FULL_RESERVATION_CONSERVATIVE',
                    'SAVED_NO_CHILD_TERMINAL_BINDING_REQUIRED')
            proof = dict(state='EXPLICIT_STOP_PRESERVED', process_started=False, process_finished=True,
                process_identity=None, process_termination_proof='QA_SAVED_NO_CHILD_TERMINAL_RECEIPT',
                terminal_file_sha256=digest(saved_raw), attempt_id=attempt, host_instance=socket.gethostname(),
                protected_outcomes_accessed=False, accounting_mode='FULL_RESERVATION_CONSERVATIVE')
            return _recovery_terminal(rpc,job,dict(attempt_id=attempt,
                recovery_id=job['recovery_id'], receipt=prior, original_terminal_operation='terminal_fail',
                recovery_receipt=proof))
    if identity_path.is_file():
        require(not identity_path.is_symlink() and identity_path.stat().st_size <= 8192,
                'CHILD_IDENTITY_RECORD_REJECTED')
        identity = json.loads(identity_path.read_bytes())
    else:
        # An actual wait4 terminal receipt can survive an interrupted identity
        # file write; it proves the same original PID without inventing one.
        terminal = ROOT / ('terminal_' + attempt + '.json')
        require(terminal.is_file() and not terminal.is_symlink()
                and terminal.stat().st_size <= 1024 * 1024, 'CHILD_IDENTITY_RECORD_REQUIRED')
        saved = json.loads(terminal.read_bytes())
        prior = saved.get('args', {}).get('receipt', {})
        require(saved.get('owner') == job['original_owner'] and prior.get('attempt_id') == attempt
                and prior.get('host_instance') == socket.gethostname()
                and prior.get('process_termination_proof') == 'SPECIFIC_CHILD_WAIT4'
                and prior.get('process_finished') is True, 'ACTUAL_WAIT4_IDENTITY_REQUIRED')
        identity = prior.get('process_identity')
    guards = source_guards(); guards.assert_proc_namespace()
    proof = guards.quiesce_recorded_child(identity, guards.ParentBudget(), reserve_rpc=False)
    require(proof.get('process_finished') is True, 'RECORDED_CHILD_NOT_QUIESCENT')
    time.sleep(SQL_TAIL_SECONDS)
    # Recovery is a new, separately reserved 30-second metadata segment. The
    # original incomplete attempt still receives its full 30-second charge.
    receipt = dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY', process_finished=True,
        process_identity=identity, process_termination_proof=proof,
        host_instance=socket.gethostname(), attempt_id=attempt,
        protected_outcomes_accessed=False, accounting_mode='FULL_RESERVATION_CONSERVATIVE',
        error='RECORDED_CHILD_EXIT_VERIFIED_PRIOR_RESULT_NOT_REEXECUTED')
    args = dict(attempt_id=attempt, recovery_id=job['recovery_id'], receipt=receipt)
    if terminal_path.exists():
        require(terminal_path.is_file() and not terminal_path.is_symlink()
                and terminal_path.stat().st_size <= 1024 * 1024, 'ORIGINAL_TERMINAL_FILE_REJECTED')
        original = json.loads(terminal_path.read_bytes())
        prior = original.get('args', {}).get('receipt', {})
        require(original.get('owner') == job['original_owner']
                and original.get('operation') in ('terminal_commit', 'terminal_progress', 'terminal_fail')
                and original.get('args', {}).get('attempt_id') == attempt
                and prior.get('attempt_id') == attempt
                and prior.get('host_instance') == socket.gethostname()
                and prior.get('process_finished') is True
                and prior.get('process_identity') == identity
                and prior.get('process_termination_proof') == 'SPECIFIC_CHILD_WAIT4'
                and prior.get('protected_outcomes_accessed') is False,
                'IMMUTABLE_ORIGINAL_TERMINAL_BINDING_REJECTED')
        # The extra control call belongs to this new bounded recovery reservation.
        # Replay the original exact receipt; never relabel valid progress as failure.
        args.update(receipt=prior, original_terminal_operation=original['operation'],
                    recovery_receipt=receipt)
    return _recovery_terminal(rpc,job,args)


def replay_recorded_recovery_terminal(rpc,job):
    """One separately charged replay of saved metadata; no research child rerun."""
    attempt=job.get('attempt_id')
    require(job.get('reserved_cpu_seconds')==30 and isinstance(attempt,str)
            and re.fullmatch(r'[0-9a-f-]{36}',attempt)
            and all(isinstance(job.get(k),str) and re.fullmatch(r'[0-9a-f-]{36}',job[k]) for k in ('recovery_id','replay_id')),
            'QA_SEPARATELY_RESERVED_RECOVERY_REPLAY_REQUIRED')
    validate_request_bound(job)
    require(job.get('host_instance')==socket.gethostname(),'QA_SAME_HOST_RECOVERY_REPLAY_REQUIRED')
    request_path=ROOT/('recovery_terminal_'+attempt+'.json')
    proof_path=ROOT/('recovery_rpc_proof_'+attempt+'.json')
    for path in (request_path,proof_path):
        require(path.is_file() and not path.is_symlink() and path.stat().st_size<=1024*1024,
                'QA_SAVED_RECOVERY_REQUEST_AND_PHYSICAL_PROOF_REQUIRED')
    raw=request_path.read_bytes();saved=json.loads(raw);proof=json.loads(proof_path.read_bytes())
    require(raw==canonical_bytes(saved) and saved.get('operation')=='terminal_recover'
            and saved.get('owner')==job['original_owner'] and saved.get('args',{}).get('attempt_id')==attempt
            and saved['args'].get('recovery_id')==job['recovery_id'] and 'replay_id' not in saved['args'],
            'QA_EXACT_SAVED_FIRST_RECOVERY_REQUEST_REQUIRED')
    expected=object_hash(dict(op='terminal_recover',owner=saved['owner'],args=saved['args']))
    identity=proof.get('process_identity')
    require(proof.get('operation')=='terminal_recover' and proof.get('attempt_id')==attempt
            and proof.get('request_sha256')==expected and proof.get('host_instance')==socket.gethostname()
            and proof.get('process_finished') is True and proof.get('process_termination_proof')=='SPECIFIC_CHILD_WAIT4'
            and isinstance(identity,dict) and type(identity.get('pid')) is int and identity['pid']>1
            and identity.get('process_group')==identity['pid'] and type(identity.get('start_ticks')) is int
            and isinstance(identity.get('boot_id'),str) and identity['boot_id'],
            'QA_ACTUAL_SAVED_RECOVERY_HTTP_HELPER_PROOF_REQUIRED')
    require(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==identity['boot_id'],
            'QA_SAME_BOOT_RECOVERY_HTTP_HELPER_PROOF_REQUIRED')
    time.sleep(SQL_TAIL_SECONDS)
    receipt=dict(proof_scope='EXACT_SAVED_RECOVERY_TERMINAL_REQUEST_REPLAY',
        saved_request_canonical_utf8=raw.decode(),saved_request_sha256=digest(raw),
        rpc_request_canonical_utf8=canonical_bytes(dict(op='terminal_recover',owner=saved['owner'],args=saved['args'])).decode(),
        prior_rpc_helper_termination=proof,sql_tail_waited_seconds=SQL_TAIL_SECONDS,
        host_instance=socket.gethostname(),research_child_reexecuted=False,research_child_signalled=False,
        protected_outcomes_accessed=False,research_objective_achieved=False)
    args=dict(saved['args'],replay_id=job['replay_id'],replay_receipt=receipt)
    atomic_file(ROOT/('recovery_replay_'+attempt+'.json'),canonical_bytes(dict(operation='terminal_recover',owner=saved['owner'],args=args)))
    return rpc.call('terminal_recover',saved['owner'],args)

def _rpc_helper(key):
    global _rpc_transport_path
    require(re.fullmatch(r'[0-9a-f]{32}', key), 'RPC_HELPER_ARGUMENT')
    _rpc_transport_path=ROOT/('rpc_transport_'+key+'.json')
    signal.signal(signal.SIGALRM, signal.SIG_DFL); signal.setitimer(signal.ITIMER_REAL, 2.0)
    signal.signal(signal.SIGPROF, signal.SIG_DFL); signal.setitimer(signal.ITIMER_PROF, .5)
    source_guards().prohibit_descendants()
    request = json.loads((ROOT / ('rpc_' + key + '.json')).read_bytes())
    try:
        response = MissionRPC().direct_call(request['op'], request['owner'], request['args'])
        result = dict(success=True, response=response)
    except Exception as exc:
        message = str(exc)
        result = dict(success=False, error=message if re.fullmatch(r'[A-Z0-9_]{1,200}', message) else type(exc).__name__)
    with (ROOT / ('rpc_reply_' + key + '.json')).open('xb') as handle:
        handle.write(canonical_bytes(result)); handle.flush(); os.fsync(handle.fileno())
    return 0 if result['success'] else 1
PHASES = ('COMMON_READBACK','PARTITION_READBACK','DICTIONARY_READBACK',
          'SYNTHETIC_BASELINE','SYNTHETIC_INTERRUPTED','SYNTHETIC_RESUME','COMPARE','VERIFIED')
QA_NAMES = re.compile(r'(wave_scope|checkpoint|stage_[0-5]_(fit|train|test)|fold_[0-5]|qstate_seal)\.json|trial_ledger\.jsonl|qstate_[0-5]_fit(_s[0-7])?\.sqlite')


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def validate_request_bound(job):
    proof=job.get('request_timeout_verification')
    require(isinstance(proof,dict) and proof.get('verified_actual_http_request') is True
            and proof.get('query_timeout_seconds')==2 and proof.get('request_start_deadline_ms')==500
            and proof.get('per_request_server_permit_required') is True
            and proof.get('client_clock_error_not_used_for_admission') is True
            and proof.get('clock_observation_is_historical_only') is True
            and proof.get('post_helper_sql_tail_seconds')==SQL_TAIL_SECONDS
            and proof.get('late_queued_request_termination_inferred_from_helper_exit') is False
            and proof.get('host_instance')==socket.gethostname()
            and proof.get('host_boot_id')==Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            and isinstance(proof.get('artifact_key'),str) and 0<len(proof['artifact_key'])<=256,
            'QA_ACTUAL_REQUEST_TIMEOUT_AND_SAME_HOST_SERVER_PERMIT_REQUIRED')
    require_hash(proof.get('artifact_sha256'),'QA_IMMUTABLE_REQUEST_BOUND_RECEIPT_REQUIRED')
    return proof


def validate_job(job):
    require(job.get('action') == 'PRERELEASE_QA' and job.get('admission_role') == 'PRERELEASE_QA'
            and job.get('qa_admission_verified') is True and job.get('release_verified') is False
            and job.get('resource_reservation_verified') is True, 'QA_SEPARATE_ADMISSION_REQUIRED')
    require(job.get('qa_wrapper_sha256') == digest(ENTRYPOINT.read_bytes()), 'QA_IMPLEMENTATION_PIN_REQUIRED')
    if WAVE in ('FP02','FP03'):
        require(job.get('qa_core_sha256')==digest(Path(__file__).read_bytes()),'QA_CORE_IMPLEMENTATION_PIN_REQUIRED')
    require(job.get('reserved_cpu_seconds') == 30 and job.get('host_instance') == socket.gethostname(),
            'QA_CURRENT_HOST_AND_FINITE_RESERVATION_REQUIRED')
    validate_request_bound(job)
    require_hash(job.get('release_artifact_sha256'))
    state = job.get('qa_state', {})
    require(state.get('phase') in PHASES and type(state.get('revision')) is int and state['revision'] >= 0,
            'QA_REGISTERED_PROGRESS_STATE_REQUIRED')
    registered = job.get('registration', {}).get('contract', {})
    require(registered.get('wave')==WAVE,'QA_SEPARATE_WAVE_BINDING_REQUIRED')
    require(registered.get('maximum_cpu_seconds') == 7200 and registered.get('maximum_scratch_bytes') == 2147483648
            and registered.get('dates') == ['2025-09-01','2026-05-31']
            and registered.get('confirmation_or_holdout_access_allowed') is False,
            'QA_EXISTING_DEVELOPMENT_SCOPE_AND_RESOURCES_REQUIRED')
    for key, value in [('engine_sha256',BASE_SHA),('bound_engine_sha256',BOUND_SHA),
                       ('source_policy_sha256',POLICY_SHA),('template_source_scope_sha256',SCOPE_SHA)]:
        require(registered.get(key) == value, 'QA_ORIGINAL_ENGINE_SCOPE_PIN_REQUIRED')
    for name in ('source_manifest_sha256','population_manifest_sha256','source_adapter_sha256','external_quantiles_sha256'):
        require_hash(registered.get(name))
    return state, registered


def load_adapter(job):
    meta = job['registration']['source_adapter']
    raw = meta.get('content_utf8', '').encode()
    require(meta.get('api_version') == 'EQ20_FP01_ADAPTER_V2' and 0 < len(raw) <= 1048576
            and len(raw) == meta.get('bytes') and digest(raw) == meta.get('sha256')
            and meta['sha256'] == job['registration']['contract']['source_adapter_sha256'],
            'QA_EXACT_V2_ADAPTER_REQUIRED')
    path = ROOT / ('adapter_' + meta['sha256'] + '.py')
    atomic_file(path, raw)
    adapter = load_module('eq20_registered_fp01_qa_adapter', path)
    require(adapter.API_VERSION == 'EQ20_FP01_ADAPTER_V2', 'QA_ADAPTER_API_REQUIRED')
    adapter._MISSION = sys.modules[__name__]
    return adapter


def _rpc(job, op, args):
    require(time.monotonic() < _ACCOUNT_WORK_DEADLINE - 1.85, 'QA_COMMITTED_WORK_YIELD')
    return MissionRPC().direct_call(op, job['_rpc_owner'], dict(args,
        attempt_id=job['attempt_id'], release_artifact_sha256=job['release_artifact_sha256']))


def readback_meta(adapter, path, meta):
    require(path.is_file() and not path.is_symlink() and path.stat().st_size == meta['raw_bytes'],
            'QA_ACTUAL_CURRENT_HOST_FILE_REQUIRED')
    actual = adapter._file_hash(path)
    require(actual == meta['raw_sha256'], 'QA_ACTUAL_CURRENT_HOST_READBACK_MISMATCH')
    return dict(name=meta['name'], raw_bytes=meta['raw_bytes'], raw_sha256=actual)


def advance(job, next_phase, receipt, **changes):
    state = job['qa_state']
    receipt = dict(receipt, host_instance=socket.gethostname(),
                   source_manifest_sha256=job['registration']['contract']['source_manifest_sha256'],
                   protected_outcomes_accessed=False, research_objective_achieved=False)
    progress = {key: state.get(key, 0) for key in ('partition_cursor','dictionary_cursor','validated_files','validated_partitions')}
    progress['readback_chain_sha256'] = state.get('readback_chain_sha256', digest(b''))
    progress.update(changes)
    if receipt.get('kind') in ('COMMON_READBACK','PARTITION_READBACK','DICTIONARY_READBACK'):
        pair=[progress['readback_chain_sha256'], receipt]
        progress['readback_chain_sha256'] = object_hash(pair)
        progress['readback_chain_canonical_utf8'] = canonical_bytes(pair).decode()
    result = _rpc(job, 'qa_progress', dict(expected_revision=state['revision'],
        from_phase=state['phase'], to_phase=next_phase, progress=progress, receipt=receipt))
    require(result.get('committed') is True, 'QA_ACTUAL_PROGRESS_COMMIT_REQUIRED')
    return dict(state='RUNNING', qa_committed=True, qa_receipt_sha256=result.get('receipt_sha256'),
                protected_outcomes_accessed=False, research_objective_achieved=False)


def source_readback_step(job, adapter, prepared):
    state = job['qa_state']; phase = state['phase']
    cache = adapter._job(job, ROOT)
    roles = adapter._source_files(job)
    source = job['registration']['source_readiness']
    inputs = prepared['inputs']; base = prepared['bound'].base
    pages, counts, dictionaries = adapter._partition_plan(inputs.manifest, source)
    total_partitions = sum(page['last_index'] - page['first_index'] + 1 for page in pages)
    plan=state.get('partition_plan');dates=sorted(counts)
    selected_dates=sorted(set([dates[0],dates[len(dates)//2],dates[-1]]))
    require(isinstance(plan,list) and 1<=len(plan)<=6 and all(type(i) is int and 0<=i<total_partitions for i in plan)
            and plan==sorted(set(plan)) and state.get('partition_plan_sha256')==object_hash(plan)
            and state.get('selected_source_dates')==selected_dates
            and state.get('source_partition_count')==total_partitions,'QA_FIXED_OUTCOME_BLIND_PARTITION_PLAN_REQUIRED')
    if phase == 'COMMON_READBACK':
        files = [readback_meta(adapter, cache / 'inputs' / meta['name'], meta) for meta in roles.values()]
        return advance(job, 'PARTITION_READBACK', dict(kind=phase, files=files,
            expected_partitions=len(plan), source_partition_count=total_partitions,
            representative_partition_plan=plan,partition_plan_sha256=object_hash(plan),selected_source_dates=selected_dates,
            expected_dictionary_files=16, expected_common_files=6),
            validated_files=6, expected_partitions=len(plan))
    if phase == 'PARTITION_READBACK':
        cursor = state['partition_cursor']
        if cursor == len(plan):
            return advance(job, 'DICTIONARY_READBACK', dict(kind='PARTITION_SET_COMPLETE', files=[],
                verified_partitions=cursor))
        require(0 <= cursor < len(plan), 'QA_PARTITION_CURSOR_REQUIRED')
        partition_index=plan[cursor]
        page = next((value for value in pages if value['first_index'] <= partition_index <= value['last_index']), None)
        require(page is not None, 'QA_CONTIGUOUS_PARTITION_PLAN_REQUIRED')
        descriptor = adapter._partition(job, cache, page, partition_index)
        require(0 < descriptor['session_count'] <= 64, 'QA_REGISTERED_ATOMIC_PARTITION_MAX64_REQUIRED')
        files = {item['role']: item for item in descriptor['files']}
        directory = adapter._slot(cache, 'qa_partition', str(partition_index), list(files.values()))
        verified = []
        for meta in files.values():
            adapter._materialize(job, cache, dict(meta, _routing=dict(partition_index=partition_index,
                page_artifact_key=page['artifact_key'])), directory / meta['name'])
            verified.append(readback_meta(adapter, directory / meta['name'], meta))
        inputs._w10_source_registry.attach(directory / files['unit_registry']['name'])
        membership = hashlib.sha256(); previous = None; count = 0; boundary = []
        try:
            for technical, labels in zip_longest(base.jsonl(directory / files['features']['name']),
                                                base.jsonl(directory / files['labels']['name'])):
                adapter._deadline(.50)
                require(technical is not None and labels is not None, 'QA_FEATURE_LABEL_ROW_COUNT_MISMATCH')
                key = [technical.get('session_date'), technical.get('security_id')]
                adapter._key(key)
                require(previous is None or key > previous, 'QA_PARTITION_MEMBER_ORDER_REQUIRED')
                require([labels.get('session_date'), labels.get('security_id')] == key, 'QA_LABEL_KEY_ALIGNMENT_REQUIRED')
                membership.update(canonical_bytes(key) + b'\n')
                if count == 0 or count == descriptor['session_count']-1:
                    # Deep boundary validation is separate from the complete
                    # per-file hash readback and construction's all-row QA.
                    inputs.validate_pair(technical, labels, True); boundary.append(key)
                previous = key; count += 1
        finally:
            inputs._w10_source_registry.detach()
        require(count == descriptor['session_count'] and membership.hexdigest() == descriptor['membership_sha256']
                and boundary[0] == descriptor['first_key'] and previous == descriptor['last_key'],
                'QA_COMPLETE_PARTITION_MEMBERSHIP_READBACK_REQUIRED')
        return advance(job, 'PARTITION_READBACK', dict(kind=phase, partition_index=partition_index,plan_position=cursor, files=verified,
            membership_sha256=membership.hexdigest(), source_sessions=count,
            boundary_schema_keys=boundary, all_rows_semantically_reevaluated=False),
            partition_cursor=cursor+1, validated_partitions=state['validated_partitions']+1,
            validated_files=state['validated_files']+3)
    require(phase == 'DICTIONARY_READBACK', 'QA_SOURCE_READBACK_PHASE_REQUIRED')
    cursor = state['dictionary_cursor']
    if cursor == 16:
        return advance(job, 'SYNTHETIC_BASELINE', dict(kind='DICTIONARY_SET_COMPLETE', files=[],
            verified_dictionary_files=16))
    require(0 <= cursor < 16, 'QA_DICTIONARY_CURSOR_REQUIRED')
    prefix = '0123456789abcdef'[cursor]
    meta = dict(dictionaries[prefix], _routing=dict(unit_prefix=prefix))
    directory = adapter._slot(cache, 'qa_dictionary', prefix, [meta])
    path = directory / meta['name']; adapter._materialize(job, cache, meta, path)
    verified = readback_meta(adapter, path, meta)
    with sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True) as connection:
        connection.execute('PRAGMA query_only=ON')
        first = connection.execute('SELECT unit_id,unit_key FROM units ORDER BY unit_id LIMIT 1').fetchone()
        if first is not None:
            require(first[0].startswith(prefix) and digest(canonical_bytes(json.loads(first[1]))) == first[0],
                    'QA_SOURCE_UNIT_DICTIONARY_SCHEMA_REQUIRED')
    return advance(job, 'DICTIONARY_READBACK', dict(kind=phase, prefix=prefix, files=[verified],
        sampled_dictionary_entry_validated=first is not None, full_dictionary_values_inspected=False),
        dictionary_cursor=cursor+1, validated_files=state['validated_files']+1)


def synthetic_inputs(base, policy, scope, contract):
    """Deterministic supported-quantile QA with explicit later unknown labels.

    Twelve first-date units exceed the frozen ten-unit/three-identity source
    minimum. Five later monthly units exercise fold transitions and conservative
    unknown handling. Every observation is synthetic and certifies no rule.
    """
    features = sorted(set(scope['feature_grammar']) | set(scope['technical_features']))
    keys = {}
    class Registry:
        def __init__(self):
            self.scope = scope
            for field, spec in scope['feature_grammar'].items():
                if spec['kind'] == 'NUMERIC':
                    for number in range(26):
                        key = ['REGISTERED_SYNTHETIC_QA_ONLY', field, 'SYNTHETIC_%03d' % number]
                        keys[policy.digest(key)] = policy.canonical(key).decode()
        def observations(self, session):
            for field, spec in scope['feature_grammar'].items():
                if spec['kind'] == 'NUMERIC':
                    key = ['REGISTERED_SYNTHETIC_QA_ONLY', field, session['security_id']]
                    keys[policy.digest(key)] = policy.canonical(key).decode()
                    number = int(session['security_id'].split('_')[-1])
                    yield field, key, 'SYNTHETIC_ISSUER_%03d' % number, float(number+1), session['decisions'][0]['decision_ts']
        def key_for(self, key):
            return keys[key]
    days = ['2025-09-02','2026-01-02','2026-02-02','2026-03-02','2026-04-01','2026-05-01']
    def members(start, end, with_labels, after_key):
        for day_number, day in enumerate(days):
            if not start <= day <= end:
                continue
            for number in range(12 if day_number == 0 else 1):
                key = [day, 'SYNTHETIC_%03d' % (number if day_number == 0 else 20+day_number)]
                if after_key is not None and key <= after_key:
                    continue
                stamp = day + 'T15:00:35+00:00'
                value = float((number if day_number == 0 else 20+day_number)+1)
                row = dict(decision_ts=stamp, reference_state='AVAILABLE', p_reference=100.0,
                    features={name:dict(state='AVAILABLE',
                        value=(1.0 if scope['feature_grammar'].get(name,{}).get('kind')=='POSITIVE_OBSERVED_STATE' else value),
                        available_at=stamp) for name in features})
                session = dict(session_date=day, security_id=key[1], decisions=[row])
                labels = dict(session_date=day, security_id=key[1], decisions=[dict(decision_ts=stamp,
                    state='QUALIFY' if day_number==0 or day_number%2 else 'UNRESOLVED')]) if with_labels else None
                yield session, labels
    return SimpleNamespace(features=features, contract=contract, research_mode=base.FULL_STREAM,
        manifest={'sampling_design':None}, discovery_start='2025-09-01', discovery_end='2026-05-31',
        manifest_sha256=digest(b'EQ20_REGISTERED_SYNTHETIC_FINAL_CONFIGURATION_QA_V1'),
        budget=base.Budget(maximum_cpu_seconds=7200, maximum_rss_bytes=256*1024*1024),
        sessions=members, expected_session_count_range=lambda start,end:sum(12 if n==0 else 1 for n,day in enumerate(days) if start<=day<=end),
        unchanged=lambda:None, _w10_source_registry=Registry(), synthetic_fixture_only=True)


def qa_work(checkpoint):
    active = checkpoint.get('active_stage') or {}
    return [len(checkpoint.get('stage_commits',{})), len(checkpoint.get('completed_folds',[])),
        checkpoint.get('trial_records',0), active.get('processed_sessions',0),
        int(active.get('stream_exhausted') is True),
        active.get('payload',{}).get('external_store',{}).get('finalized_fields',0)]


def semantic_summary(base, output, checkpoint, scope):
    require(checkpoint['trial_records'] == 6000 and checkpoint['completed_folds'] == list(range(6))
            and len(checkpoint['stage_commits']) == 17, 'QA_ACTUAL_FINAL_CONFIGURATION_ACCOUNTING_REQUIRED')
    stage_hashes = {}; stage_semantic_hashes={}; fold_hashes = {}
    for key, meta in sorted(checkpoint['stage_commits'].items()):
        actual = base.file_sha256(output/meta['path'])
        require(actual == meta['sha256'], 'QA_STAGE_COMMIT_HASH_MISMATCH'); stage_hashes[key] = actual
        if key.endswith('_fit'):
            value=json.loads((output/meta['path']).read_bytes())
            numeric_fields=sum(spec['kind']=='NUMERIC' for spec in scope['feature_grammar'].values())
            stage_semantic_hashes[key]=fit_stage_semantic_hash(value,key,numeric_fields)
        else:
            stage_semantic_hashes[key]=actual
    selected_counts={};nonnull_thresholds=0;unresolved_forward_signals=0
    for key, sha in sorted(checkpoint['fold_sha256'].items()):
        require(base.file_sha256(output/('fold_'+key+'.json')) == sha, 'QA_FOLD_COMMIT_HASH_MISMATCH')
        fold_hashes[key] = sha
        fold=json.loads((output/('fold_'+key+'.json')).read_bytes())
        selected_counts[key]=len(fold['training_selected_ids'])
        nonnull_thresholds+=sum(gate['threshold'] is not None for rule in fold['fitted_rules'] for gate in rule['gates'])
        if fold.get('forward_test'):
            unresolved_forward_signals+=sum(metric['unresolved_first_signals'] for metric in fold['forward_test']['rules'].values())
    raw = (output/'trial_ledger.jsonl').read_bytes()
    require(digest(raw) == checkpoint['trial_ledger_sha256'] and len(raw.splitlines()) == 6000,
            'QA_IMMUTABLE6000_RECORD_LEDGER_REQUIRED')
    require(nonnull_thresholds==12000 and any(selected_counts.values()) and unresolved_forward_signals>0,
            'QA_SUPPORTED_QUANTILES_SELECTION_AND_UNKNOWN_PATH_REQUIRED')
    return dict(stage_hashes=stage_hashes,stage_semantic_hashes=stage_semantic_hashes,
        fit_serialization_normalization='OMIT_ONLY_RESULT_SNAPSHOT_EXTERNAL_STORE_FINALIZED_FIELDS_V1',
        nonnull_fitted_thresholds=nonnull_thresholds,training_selected_counts=selected_counts,
        unresolved_forward_first_signals=unresolved_forward_signals,
        fold_hashes=fold_hashes, ledger_sha256=digest(raw),
        templates=1000, contexts=6, synthetic_fit_records=6000, source_features=106, technical_features=11,
        template_family_sha256=scope['template_family_sha256'], actual_research_fit_records=0)


def fit_stage_semantic_hash(value,stage_key,numeric_fields):
    """Ignore the proved operational cache counter, preserving all science.

    The original stage records snapshot() before evidence() completes the
    resumable field cache. Interruptions can change this one snapshot counter.
    Exact raw stage bytes remain separately pinned; no other field is omitted.
    """
    value=json.loads(canonical_bytes(value))
    require(value.get('stage_key')==stage_key and value.get('kind')=='quantiles'
            and isinstance(value.get('result',{}).get('evidence'),dict),'QA_EXACT_FIT_STAGE_SCHEMA_REQUIRED')
    store=value.get('result',{}).get('snapshot',{}).get('external_store',{})
    require(type(store.get('finalized_fields')) is int and 0<=store['finalized_fields']<=numeric_fields,
            'QA_BOUNDED_FIT_CACHE_COUNTER_REQUIRED')
    del store['finalized_fields']
    return object_hash(value)


def comparable_summary(value):
    """Raw artifact pins stay in both receipts; semantic hashes compare fits."""
    require(isinstance(value,dict),'QA_SUMMARY_REQUIRED')
    raw=value.get('stage_hashes');semantic=value.get('stage_semantic_hashes')
    require(isinstance(raw,dict) and isinstance(semantic,dict) and set(raw)==set(semantic)
            and len(raw)==17 and value.get('fit_serialization_normalization')==
            'OMIT_ONLY_RESULT_SNAPSHOT_EXTERNAL_STORE_FINALIZED_FIELDS_V1','QA_EXACT_SEMANTIC_COMPARISON_SCHEMA_REQUIRED')
    for key in raw:
        require_hash(raw[key]);require_hash(semantic[key])
        if not key.endswith('_fit'):
            require(raw[key]==semantic[key],'QA_NONFIT_ARTIFACTS_MUST_COMPARE_EXACT_BYTES')
    return {key:val for key,val in value.items() if key!='stage_hashes'}


def registered_templates(job, bound, scope_path, features):
    if WAVE=='FP01':
        return bound.registered_templates(scope_path,features)
    require(WAVE in ('FP02','FP03'),'QA_REGISTERED_WAVE_REQUIRED')
    prefix=WAVE.lower();registered=job['registration']['contract']
    path=Path(__file__).with_name('eq20_'+prefix+'_continuation.py')
    require(digest(path.read_bytes())==registered.get(prefix+'_template_compiler_sha256'),
            'QA_EXACT_SUCCESSOR_TEMPLATE_COMPILER_REQUIRED')
    compiler=load_module('eq20_registered_'+prefix+'_qa_template_compiler',path)
    scope,templates,quantiles=getattr(compiler,'registered_'+prefix+'_templates')(scope_path,features,registered,bound)
    scope=dict(scope,template_family_sha256=registered[prefix+'_template_contract']['template_family_sha256'])
    return scope,templates,quantiles


def _transfer_root(job):
    directory=ROOT/job['release_artifact_sha256']/'qa_transfer'
    require(not directory.is_symlink(),'QA_TRANSFER_DIRECTORY_REQUIRED')
    directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    return directory


def _qa_output_name(mode):
    # The independently reviewed external store admits the fixed fp01_ prefix.
    # Distinct successor controller roots and embedded wave tags keep histories apart.
    return ('fp01_qa_' if WAVE=='FP01' else 'fp01_'+WAVE.lower()+'_qa_')+mode


def _file_sha(path):
    digestor=hashlib.sha256()
    with path.open('rb') as handle:
        while True:
            block=handle.read(1024*1024)
            if not block:return digestor.hexdigest()
            digestor.update(block)


def _read_checkpoint_body(path):
    value=json.loads(path.read_bytes());claimed=value.pop('checkpoint_sha256',None)
    require(claimed==object_hash(value),'QA_ORIGINAL_CHECKPOINT_CHECKSUM_REQUIRED')
    return value


def encode_snapshot(output, external, checkpoint, transfer):
    def sealing_deadline(margin=.05):
        require(time.monotonic()<_ACCOUNT_WORK_DEADLINE-2.1-margin,'QA_COMMITTED_WORK_YIELD')
    sealed=external.snapshot_files(output,deadline=sealing_deadline)
    files=[];total=0;encoded_total=0
    for path in sorted(output.iterdir()):
        if not QA_NAMES.fullmatch(path.name):continue
        sealing_deadline()
        require(path.is_file() and not path.is_symlink() and 0<path.stat().st_size<=MAX_QA_FILE,'QA_SNAPSHOT_PHYSICAL_FILE_BOUND')
        raw_sha=_file_sha(path);total+=path.stat().st_size
        require(total<=MAX_QA_RAW,'QA_SNAPSHOT_AGGREGATE_BOUND')
        encoded=transfer/('encoded_'+raw_sha+'.zlib')
        if not encoded.exists():
            require(source_guards().scratch_safe(path.stat().st_size+1024*1024),'QA_EXISTING_SCRATCH_CEILING')
            temporary=encoded.with_suffix('.tmp');compressor=zlib.compressobj(3)
            with path.open('rb') as source,temporary.open('wb') as target:
                while True:
                    sealing_deadline();block=source.read(1024*1024)
                    if not block:break
                    target.write(compressor.compress(block))
                target.write(compressor.flush());target.flush();os.fsync(target.fileno())
            os.replace(temporary,encoded)
        require(not encoded.is_symlink(),'QA_ENCODED_FILE_SYMLINK')
        blob_sha=_file_sha(encoded);parts=[]
        with encoded.open('rb') as handle:
            for number in range(math.ceil(encoded.stat().st_size/QA_PART)):
                block=handle.read(QA_PART);parts.append(dict(part_no=number,bytes=len(block),sha256=digest(block)))
        encoded_total+=encoded.stat().st_size
        require(encoded_total<=MAX_QA_PACKED,'QA_ENCODED_AGGREGATE_BOUND')
        files.append(dict(name=path.name,raw_bytes=path.stat().st_size,raw_sha256=raw_sha,
            blob_sha256=blob_sha,encoded_bytes=encoded.stat().st_size,parts=parts))
    manifest=dict(codec='FILES_ZLIB_PARTS_V1',files=files,sealed_files=sealed,checkpoint=checkpoint,
        raw_bytes=total,encoded_bytes=encoded_total,files_manifest_sha256=object_hash(files),
        checkpoint_sha256=object_hash(checkpoint),canonical_checkpoint_utf8=canonical_bytes(checkpoint).decode(),
        work_vector=qa_work(checkpoint),synthetic_only=True)
    manifest['raw_sha256']=object_hash([{k:v for k,v in item.items() if k in ('name','raw_bytes','raw_sha256')} for item in files])
    manifest['payload_sha256']=object_hash(manifest)
    decode_snapshot(manifest)
    return manifest


def decode_snapshot(snapshot):
    require(snapshot.get('codec')=='FILES_ZLIB_PARTS_V1' and snapshot.get('synthetic_only') is True,
            'QA_SYNTHETIC_FILE_MANIFEST_REQUIRED')
    require(snapshot.get('payload_sha256')==object_hash({k:v for k,v in snapshot.items() if k!='payload_sha256'}),
            'QA_SNAPSHOT_MANIFEST_HASH_REQUIRED')
    files=snapshot.get('files');require(isinstance(files,list) and 2<=len(files)<=128,'QA_SNAPSHOT_FILE_COUNT')
    names=set();total=0;encoded_total=0
    for item in files:
        name=item.get('name');require(isinstance(name,str) and QA_NAMES.fullmatch(name) and name not in names,'QA_SNAPSHOT_FILENAME_REQUIRED')
        names.add(name)
        require(type(item.get('raw_bytes')) is int and 0<item['raw_bytes']<=MAX_QA_FILE
                and type(item.get('encoded_bytes')) is int and 0<item['encoded_bytes']<=MAX_QA_PACKED,'QA_SNAPSHOT_FILE_BOUND')
        require_hash(item.get('raw_sha256'));require_hash(item.get('blob_sha256'))
        parts=item.get('parts');require(isinstance(parts,list) and len(parts)==math.ceil(item['encoded_bytes']/QA_PART),'QA_SNAPSHOT_PART_COUNT')
        for number,part in enumerate(parts):
            require(part.get('part_no')==number and type(part.get('bytes')) is int
                    and part['bytes']==min(QA_PART,item['encoded_bytes']-number*QA_PART),'QA_SNAPSHOT_PART_RANGE')
            require_hash(part.get('sha256'))
        total+=item['raw_bytes'];encoded_total+=item['encoded_bytes']
    require(total==snapshot.get('raw_bytes') and total<=MAX_QA_RAW and encoded_total==snapshot.get('encoded_bytes')
            and encoded_total<=MAX_QA_PACKED and object_hash(files)==snapshot.get('files_manifest_sha256')
            and object_hash(snapshot.get('checkpoint'))==snapshot.get('checkpoint_sha256')
            and qa_work(snapshot['checkpoint'])==snapshot.get('work_vector'),'QA_SNAPSHOT_LOGICAL_READBACK_REQUIRED')
    require(snapshot.get('raw_sha256')==object_hash([{k:v for k,v in item.items()
        if k in ('name','raw_bytes','raw_sha256')} for item in files]),'QA_RAW_FILE_SET_HASH_REQUIRED')
    require(snapshot.get('canonical_checkpoint_utf8')==canonical_bytes(snapshot['checkpoint']).decode(),
            'QA_CANONICAL_CHECKPOINT_BINDING_REQUIRED')
    return snapshot


def _pending(job):
    path=_transfer_root(job)/'pending.json'
    if not path.is_file():
        require(not job['qa_state'].get('pending_snapshot'),'QA_PENDING_SEALED_LOCAL_BYTES_REQUIRED')
        return None
    require(not path.is_symlink() and path.stat().st_size<1048576,'QA_PENDING_MANIFEST_BOUND')
    pending=json.loads(path.read_bytes())
    require(pending['prepared_sha256']==job['release_artifact_sha256']
            and pending['host_instance']==socket.gethostname(),'QA_PENDING_SCOPE_REQUIRED')
    if pending['expected_revision']!=job['qa_state']['revision']:
        previous=job['qa_state'].get('previous_qa_snapshot') or {}
        require(previous.get('payload_sha256')==pending.get('snapshot',{}).get('payload_sha256'),
                'QA_PENDING_COMMIT_REVISION_CONFLICT')
        path.unlink();return None
    require(pending['from_phase']==job['qa_state']['phase'],'QA_PENDING_PHASE_REQUIRED')
    remote=job['qa_state'].get('pending_snapshot')
    if remote:
        require(remote.get('snapshot',{}).get('payload_sha256')==pending.get('snapshot',{}).get('payload_sha256'),
                'QA_REMOTE_PENDING_MANIFEST_CONFLICT')
        if pending.get('intent_sha256'):
            require(pending['intent_sha256']==remote.get('intent_sha256'),'QA_REMOTE_INTENT_HASH_CONFLICT')
        else:
            pending['intent_sha256']=require_hash(remote.get('intent_sha256'))
            atomic_file(path,canonical_bytes(pending),immutable=False)
    return pending


def publish_snapshot(job,pending):
    snapshot=decode_snapshot(pending['snapshot']);transfer=_transfer_root(job)
    batch=[]
    def flush():
        nonlocal batch
        if not batch:return
        args=dict(parts=batch)
        if pending.get('intent_sha256'):
            args['intent_sha256']=pending['intent_sha256']
        else:
            intent={k:pending[k] for k in ('expected_revision','from_phase','to_phase','mode','snapshot','receipt')}
            intent['canonical_unsigned_snapshot_utf8']=canonical_bytes({k:v for k,v in snapshot.items() if k!='payload_sha256'}).decode()
            args['snapshot_intent']=intent
        response=_rpc(job,'qa_snapshot_parts_put',args)
        returned=response.get('parts')
        require(response.get('committed') is True and isinstance(returned,list) and returned==batch,
                'QA_ACTUAL_STORED_PART_READBACK_REQUIRED')
        intent_sha=require_hash(response.get('intent_sha256'),'QA_IMMUTABLE_SNAPSHOT_INTENT_REQUIRED')
        require(not pending.get('intent_sha256') or pending['intent_sha256']==intent_sha,'QA_SNAPSHOT_INTENT_CHANGED')
        pending['intent_sha256']=intent_sha
        atomic_file(transfer/'pending.json',canonical_bytes(pending),immutable=False)
        for part in returned:
            data=base64.b64decode(part['payload_base64'],validate=True)
            require(len(data)==part['bytes'] and digest(data)==part['payload_sha256'],'QA_PART_BYTE_READBACK_REQUIRED')
            atomic_file(transfer/('uploaded_'+part['blob_sha256']+'_'+str(part['part_no'])+'.json'),
                        canonical_bytes(dict(bytes=part['bytes'],sha256=part['payload_sha256'])))
        batch=[]
    for meta in snapshot['files']:
        path=transfer/('encoded_'+meta['raw_sha256']+'.zlib')
        require(path.is_file() and not path.is_symlink() and path.stat().st_size==meta['encoded_bytes'],
                'QA_SEALED_ENCODED_FILE_REQUIRED')
        with path.open('rb') as handle:
            for part in meta['parts']:
                data=handle.read(part['bytes']);require(digest(data)==part['sha256'],'QA_SEALED_PART_CHANGED')
                ack=transfer/('uploaded_'+meta['blob_sha256']+'_'+str(part['part_no'])+'.json')
                if ack.exists():
                    require(not ack.is_symlink() and json.loads(ack.read_bytes())==dict(bytes=part['bytes'],sha256=part['sha256']),
                            'QA_UPLOADED_PART_ACK_CONFLICT');continue
                batch.append(dict(blob_sha256=meta['blob_sha256'],part_no=part['part_no'],total_parts=len(meta['parts']),
                    bytes=part['bytes'],payload_sha256=part['sha256'],payload_base64=base64.b64encode(data).decode()))
                if len(batch)==8:flush()
    flush()
    require_hash(pending.get('intent_sha256'),'QA_STORED_SNAPSHOT_INTENT_REQUIRED')
    result=_rpc(job,'qa_snapshot_put',{k:v for k,v in pending.items() if k not in ('prepared_sha256','host_instance')})
    require(result.get('committed') is True and result.get('snapshot',{}).get('payload_sha256')==snapshot['payload_sha256'],
            'QA_REAL_DURABLE_SNAPSHOT_COMMIT_REQUIRED')
    decode_snapshot(result['snapshot'])
    (_transfer_root(job)/'pending.json').unlink()
    return dict(state='RUNNING',qa_committed=True,synthetic_fixture_only=True,snapshot_key=result.get('snapshot_key'),
        snapshot_sha256=snapshot['payload_sha256'],protected_outcomes_accessed=False,research_objective_achieved=False)


def _preparing(job):
    path=_transfer_root(job)/'preparing.json'
    if not path.exists():return None
    require(path.is_file() and not path.is_symlink() and path.stat().st_size<=MAX_QA_FILE,
            'QA_PREPARING_RECORD_REQUIRED')
    value=json.loads(path.read_bytes())
    require(value.get('prepared_sha256')==job['release_artifact_sha256']
            and value.get('host_instance')==socket.gethostname(),'QA_PREPARING_SCOPE_REQUIRED')
    if value.get('expected_revision')!=job['qa_state']['revision']:
        # Its manifest can only advance after the corresponding server commit.
        require(value['expected_revision']+1==job['qa_state']['revision']
                and job['qa_state'].get('previous_qa_snapshot'),'QA_PREPARING_REVISION_CONFLICT')
        path.unlink();return None
    require(value.get('from_phase')==job['qa_state']['phase']
            and value.get('mode') in ('baseline','resumed'),'QA_PREPARING_PHASE_REQUIRED')
    return value


def finish_preparing(job, preparing, external=None):
    """Finish sealing saved committed work before any new engine invocation."""
    registered=job['registration']['contract'];path=Path(__file__).with_name('eq20_fp01_external_quantiles.py')
    require(digest(path.read_bytes())==registered['external_quantiles_sha256'],'QA_EXTERNAL_ACCUMULATOR_PIN_REQUIRED')
    if external is None:
        external=load_module('eq20_registered_external_quantiles',path)
    require(Path(external.__file__).resolve()==path.resolve(),'QA_EXTERNAL_SEAL_IMPLEMENTATION_REQUIRED')
    output=ROOT/job['release_artifact_sha256']/_qa_output_name(preparing['mode'])
    require(output.is_dir() and not output.is_symlink(),'QA_COMMITTED_PREPARING_OUTPUT_REQUIRED')
    checkpoint=_read_checkpoint_body(output/'checkpoint.json')
    require(object_hash(checkpoint)==preparing['checkpoint_sha256'],'QA_COMMITTED_PREPARING_CHECKPOINT_CHANGED')
    transfer=_transfer_root(job);snapshot=encode_snapshot(output,external,checkpoint,transfer)
    pending={k:v for k,v in preparing.items() if k!='checkpoint_sha256'}
    pending['snapshot']=snapshot
    atomic_file(transfer/'pending.json',canonical_bytes(pending))
    (transfer/'preparing.json').unlink()
    return publish_snapshot(job,pending)


def restore_snapshot(job, output, metadata, external):
    result=_rpc(job,'qa_snapshot_get',dict(snapshot_key=metadata['snapshot_key']))
    snapshot=decode_snapshot(result['snapshot'])
    require(snapshot['payload_sha256']==metadata['payload_sha256'],'QA_REGISTERED_SNAPSHOT_BINDING_REQUIRED')
    transfer=_transfer_root(job);parts_root=transfer/'restored_parts';parts_root.mkdir(exist_ok=True)
    batch=[]
    def fetch():
        nonlocal batch
        if not batch:return
        response=_rpc(job,'qa_snapshot_parts_get',dict(snapshot_key=metadata['snapshot_key'],parts=batch))
        returned=response.get('parts');require(isinstance(returned,list) and len(returned)==len(batch),'QA_RESTORE_PART_RESPONSE_REQUIRED')
        for wanted,part in zip(batch,returned):
            data=base64.b64decode(part['payload_base64'],validate=True)
            require(part['blob_sha256']==wanted['blob_sha256'] and part['part_no']==wanted['part_no']
                    and len(data)==wanted['bytes'] and digest(data)==wanted['payload_sha256'],'QA_ACTUAL_RESTORED_PART_HASH_REQUIRED')
            atomic_file(parts_root/(part['blob_sha256']+'_'+str(part['part_no'])+'.part'),data)
        batch=[]
    for meta in snapshot['files']:
        for part in meta['parts']:
            path=parts_root/(meta['blob_sha256']+'_'+str(part['part_no'])+'.part')
            if path.exists():
                require(not path.is_symlink() and path.stat().st_size==part['bytes'] and _file_sha(path)==part['sha256'],'QA_RESTORE_CACHE_CHANGED')
            else:
                batch.append(dict(blob_sha256=meta['blob_sha256'],part_no=part['part_no'],bytes=part['bytes'],payload_sha256=part['sha256']))
                if len(batch)==8:fetch()
    fetch()
    require(source_guards().scratch_safe(snapshot['raw_bytes']),'QA_SNAPSHOT_EXISTING_SCRATCH_CEILING')
    marker=transfer/('restoring_'+output.name+'.json')
    current=dict(snapshot_sha256=snapshot['payload_sha256'])
    if not marker.exists() or json.loads(marker.read_bytes())!=current:
        if output.exists():
            require(output.is_dir() and not output.is_symlink() and output.parent==ROOT/job['release_artifact_sha256']
                    and not any(p.is_symlink() for p in output.rglob('*')),'QA_OWNED_RESTORE_DIRECTORY_REQUIRED')
            shutil.rmtree(output)
        output.mkdir(parents=True,exist_ok=False);atomic_file(marker,canonical_bytes(current),immutable=False)
    for meta in snapshot['files']:
        require(time.monotonic()<_ACCOUNT_WORK_DEADLINE-2.1,'QA_COMMITTED_WORK_YIELD')
        target=output/meta['name']
        if target.exists():
            require(target.stat().st_size==meta['raw_bytes'] and _file_sha(target)==meta['raw_sha256'],'QA_RESTORED_FILE_CHANGED');continue
        temp=target.with_suffix(target.suffix+'.tmp');raw_hash=hashlib.sha256();encoded_hash=hashlib.sha256();size=0
        decoder=zlib.decompressobj()
        with temp.open('wb') as handle:
            for part in meta['parts']:
                data=(parts_root/(meta['blob_sha256']+'_'+str(part['part_no'])+'.part')).read_bytes();encoded_hash.update(data)
                pending=data
                while pending:
                    raw=decoder.decompress(pending,min(1024*1024,meta['raw_bytes']-size+1));pending=decoder.unconsumed_tail
                    size+=len(raw);require(size<=meta['raw_bytes'],'QA_RESTORED_DECOMPRESSION_BOUND');raw_hash.update(raw);handle.write(raw)
                    require(not pending or raw,'QA_INVALID_ZLIB_PROGRESS')
            require(decoder.eof and not decoder.unused_data and size==meta['raw_bytes']
                    and raw_hash.hexdigest()==meta['raw_sha256'] and encoded_hash.hexdigest()==meta['blob_sha256'],
                    'QA_COMPLETE_RESTORED_FILE_READBACK_REQUIRED')
            handle.flush();os.fsync(handle.fileno())
        os.replace(temp,target)
    require(object_hash(_read_checkpoint_body(output/'checkpoint.json'))==snapshot['checkpoint_sha256'],'QA_RESTORED_CHECKPOINT_BYTES_REQUIRED')
    external.verify_snapshot_checkpoint(output,snapshot['checkpoint'],snapshot['sealed_files'])
    external.release_snapshot_seal(output)
    return snapshot['checkpoint'],snapshot['payload_sha256']


def synthetic_step(job, prepared, adapter):
    state=job['qa_state'];phase=state['phase'];mode='baseline' if phase=='SYNTHETIC_BASELINE' else 'resumed'
    registered=job['registration']['contract'];bound=prepared['bound'];base=bound.base
    policy=prepared['inputs'].frozen_source_policy_module
    scope_path=prepared['scope_path'];scope=json.loads(scope_path.read_bytes())
    require(base.file_sha256(Path(base.__file__))==BASE_SHA and base.file_sha256(Path(bound.__file__))==BOUND_SHA
            and digest(Path(policy.__file__).read_bytes())==POLICY_SHA and digest(scope_path.read_bytes())==SCOPE_SHA,
            'QA_ORIGINAL_PRIVATE_IMPLEMENTATION_PINS_REQUIRED')
    path=Path(__file__).with_name('eq20_fp01_external_quantiles.py')
    require(digest(path.read_bytes())==registered['external_quantiles_sha256'],'QA_EXTERNAL_ACCUMULATOR_PIN_REQUIRED')
    external=load_module('eq20_registered_external_quantiles',path)
    directory=ROOT/job['release_artifact_sha256'];directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    output=directory/_qa_output_name(mode)
    previous=state.get('previous_qa_snapshot');restored_sha=None
    if previous and previous.get('mode')==mode:
        checkpoint,restored_sha=restore_snapshot(job,output,previous,external)
    else:
        if output.exists():
            require(not output.is_symlink() and not any(p.is_symlink() for p in output.rglob('*')),'QA_OWNED_PARTIAL_DIRECTORY_REQUIRED')
            shutil.rmtree(output)
        output.mkdir(exist_ok=False)
        checkpoint=None
    fixture=synthetic_inputs(base,policy,scope,prepared['inputs'].contract)
    policy.install(base,scope,fixture._w10_source_registry)
    started=time.monotonic();forced=False
    def hook(event, cp):
        nonlocal forced
        if phase=='SYNTHETIC_INTERRUPTED' and event=='after_session_checkpoint':
            forced=True;raise RegisteredSliceYield('QA_FORCED_COMMITTED_SESSION_INTERRUPTION')
        if event in ('after_session_checkpoint','after_fold_checkpoint','after_quantile_field') and (time.monotonic()-started>1.25 or time.monotonic()>_ACCOUNT_WORK_DEADLINE-2.5):
            raise RegisteredSliceYield('QA_ROUTINE_BOUNDED_SEGMENT')
    external.install_external_quantiles(base,scope,fixture._w10_source_registry,output,
        source_policy=policy,scratch_guard=source_guards().scratch_safe,yield_guard=lambda:hook('after_quantile_field',{}))
    selected_scope,templates,quantiles=registered_templates(job,bound,scope_path,fixture.features)
    wave=dict(version=VERSION,wave=WAVE,scope_sha256=SCOPE_SHA,synthetic_fixture_only=True,
              template_family_sha256=selected_scope['template_family_sha256'],templates=[t.rule_id for t in templates])
    if checkpoint is None:
        base.immutable_json(output/'wave_scope.json',wave)
        checkpoint=dict(state='QA_SYNTHETIC_RUNNING',wave_scope_sha256=object_hash(wave),checkpoint_schema=3,
            completed_folds=[],fold_sha256={},stage_commits={},active_stage=None,trial_records=0,
            protected_outcomes_accessed=False,cpu_charged_seconds=0.,resume_count=0)
    else:
        require(checkpoint['wave_scope_sha256']==object_hash(wave),'QA_SYNTHETIC_SCOPE_RESTORE_REQUIRED')
        checkpoint['resume_count']+=1
        fixture.budget.charged_cpu_seconds=checkpoint['cpu_charged_seconds'];fixture.budget.start_cpu=time.process_time()
    prior_work=qa_work(checkpoint);base.write_checkpoint(output/'checkpoint.json',checkpoint)
    try:
        checkpoint=bound._run_w10_locked(fixture,output,checkpoint,templates,quantiles,8192,1,1,hook)
        checkpoint['state']='QA_SYNTHETIC_COMPLETE'
    except RegisteredSliceYield:
        checkpoint=base.read_checkpoint(output/'checkpoint.json');external.refresh_checkpoint_external_progress(checkpoint,output)
        checkpoint['state']='QA_SYNTHETIC_YIELDED'
    except base.BudgetExceeded as error:
        require(str(error)=='Uncommitted CPU lease expired; resume from last complete session checkpoint','QA_REAL_RESOURCE_STOP')
        checkpoint=base.read_checkpoint(output/'checkpoint.json');external.refresh_checkpoint_external_progress(checkpoint,output)
        require(qa_work(checkpoint)>prior_work,'QA_CPU_YIELD_WITHOUT_COMMITTED_WORK')
        checkpoint['state']='QA_SYNTHETIC_YIELDED'
    checkpoint['cpu_charged_seconds']=max(checkpoint['cpu_charged_seconds'],fixture.budget.consumed())
    base.write_checkpoint(output/'checkpoint.json',checkpoint)
    next_phase=phase;summary=None
    if phase=='SYNTHETIC_INTERRUPTED':
        require(forced and checkpoint['active_stage']['processed_sessions']==1 and checkpoint['active_stage']['stage_key']=='0_fit','QA_REAL_COMMITTED_INTERRUPTION_REQUIRED')
        next_phase='SYNTHETIC_RESUME'
    elif checkpoint.get('trial_records')==6000 and checkpoint.get('completed_folds')==list(range(6)):
        summary=semantic_summary(base,output,checkpoint,selected_scope)
        next_phase='SYNTHETIC_INTERRUPTED' if mode=='baseline' else 'COMPARE'
    receipt=dict(kind='SYNTHETIC_CHECKPOINT',mode=mode,forced_interruption=forced,
        actual_restored_snapshot_sha256=restored_sha,synthetic_fixture_only=True,
        actual_research_fit_records=0,summary=summary,host_instance=socket.gethostname(),
        protected_outcomes_accessed=False,baseline_mode='ORDINARY_BOUNDED_CHECKPOINTED_RUN')
    transfer=_transfer_root(job)
    preparing=dict(prepared_sha256=job['release_artifact_sha256'],host_instance=socket.gethostname(),
        expected_revision=state['revision'],from_phase=phase,to_phase=next_phase,mode=mode,
        checkpoint_sha256=object_hash(checkpoint),receipt=receipt)
    atomic_file(transfer/'preparing.json',canonical_bytes(preparing))
    return finish_preparing(job,preparing,external)


def final_compare(job):
    state=job['qa_state'];left=state.get('baseline_summary');right=state.get('resumed_summary')
    require(isinstance(left,dict) and comparable_summary(left)==comparable_summary(right) and left.get('synthetic_fit_records')==6000
            and left.get('templates')==1000 and left.get('contexts')==6
            and len(left.get('stage_hashes',{}))==17 and len(left.get('fold_hashes',{}))==6,
            'QA_BASELINE_INTERRUPTION_RESUME_FINAL_CONFIG_MISMATCH')
    require(state.get('interruption_verified') is True and state['dictionary_cursor']==16
            and state['partition_cursor']==state['expected_partitions']
            and state['validated_partitions']==state['expected_partitions']
            and state['validated_files']==6+3*state['expected_partitions']+16,
            'QA_ACTUAL_CURRENT_HOST_PREPARATION_AND_RESUME_REQUIRED')
    require(isinstance(state.get('partition_plan'),list) and 1<=len(state['partition_plan'])<=6
            and state['partition_plan']==sorted(set(state['partition_plan']))
            and state['expected_partitions']==len(state['partition_plan'])
            and state.get('partition_plan_sha256')==object_hash(state['partition_plan'])
            and type(state.get('source_partition_count')) is int and state['source_partition_count']>=len(state['partition_plan'])
            and all(type(index) is int and 0<=index<state['source_partition_count'] for index in state['partition_plan'])
            and isinstance(state.get('selected_source_dates'),list) and len(state['selected_source_dates'])==3,
            'QA_IMMUTABLE_REPRESENTATIVE_READBACK_PLAN_REQUIRED')
    registered=job['registration']['contract']
    evidence=dict(version=2,wave=WAVE,state='VERIFIED',verification_scope='CURRENT_HOST_IMMUTABLE_INPUT_READBACK_AND_SYNTHETIC_FULL_CONFIG_QA',
        host_instance=socket.gethostname(),prepared_artifact_sha256=job['release_artifact_sha256'],
        request_timeout_verification=validate_request_bound(job),
        qa_wrapper_sha256=digest(ENTRYPOINT.read_bytes()),qa_core_sha256=digest(Path(__file__).read_bytes()),
        source_manifest_sha256=registered['source_manifest_sha256'],
        population_manifest_sha256=registered['population_manifest_sha256'],source_adapter_sha256=registered['source_adapter_sha256'],
        external_quantiles_sha256=registered['external_quantiles_sha256'],source_policy_sha256=POLICY_SHA,
        engine_sha256=BASE_SHA,bound_engine_sha256=BOUND_SHA,scope_sha256=SCOPE_SHA,original_contract_raw_sha256=CONTRACT_SHA,
        clock=dict(primary_delay_seconds=35,maximum_reference_age_seconds=95,sensitivities_seconds=[5,65,125],same_primary_grid=True),
        validated_files=state['validated_files'],validated_partitions=state['validated_partitions'],
        current_host_readback_chain_sha256=state['readback_chain_sha256'],
        baseline_summary=left,resumed_summary=right,interruption_verified=True,
        raw_stage_artifact_hashes_preserved=True,scientific_comparison_exact=True,
        restored_snapshot_sha256=state.get('restored_snapshot_sha256'),
        baseline_interruption_resume_qa_verified=True,final_configuration_qa_verified=True,current_host_readback_verified=True,
        representative_partition_readback=True,partition_plan=state['partition_plan'],
        partition_plan_sha256=state['partition_plan_sha256'],source_partition_count=state['source_partition_count'],
        selected_source_dates=state['selected_source_dates'],
        exhaustive_partition_readback_deferred_to_stream=True,input_file_bytes_all_read_back=False,
        all_common_and_dictionary_file_bytes_read_back=True,all_source_rows_semantically_reevaluated=False,
        actual_research_fit_records=0,synthetic_fit_records_total=12000,
        synthetic_fixture_scope='SUPPORTED_QUANTILES_AND_SELECTED_RULES_WITH_LATER_UNKNOWN_LABELS_FINAL_CONFIGURATION',
        baseline_mode='ORDINARY_BOUNDED_CHECKPOINTED_RUN',full_monolithic_uninterrupted_process_claim=False,
        actual_source_labels_used_for_selection=False,protected_outcomes_accessed=False,
        publication_replay_granted_by_qa=False,execution_qualification_granted_by_qa=False,
        source_semantic_or_population_certificate_granted_by_qa=False,research_objective_achieved=False)
    if WAVE in ('FP02','FP03'):
        prefix=WAVE.lower()
        evidence[prefix+'_template_design_sha256']=registered[prefix+'_template_contract_sha256']
        evidence[prefix+'_template_compiler_sha256']=registered[prefix+'_template_compiler_sha256']
    require_hash(evidence['restored_snapshot_sha256'],'QA_ACTUAL_RESTORED_SNAPSHOT_PIN_REQUIRED')
    result=_rpc(job,'qa_final',dict(expected_revision=state['revision'],evidence=evidence))
    require(result.get('committed') is True and result.get('state')=='VERIFIED','QA_ACTUAL_FINAL_RECEIPT_REQUIRED')
    return dict(state='VERIFIED',qa_artifact_key=result['artifact_key'],qa_artifact_sha256=result['artifact_sha256'],
        protected_outcomes_accessed=False,research_objective_achieved=False)


def execute_segment(job,owner,attempt):
    job=dict(job,_rpc_owner=owner,attempt_id=attempt)
    state,_=validate_job(job)
    pending=_pending(job)
    if pending is not None:
        return publish_snapshot(job,pending)
    preparing=_preparing(job)
    if preparing is not None:
        return finish_preparing(job,preparing)
    if state['phase']=='COMPARE':
        return final_compare(job)
    adapter=load_adapter(job)
    prepared=adapter.load_certified_inputs(job,ROOT)
    if state['phase'] in PHASES[:3]:
        return source_readback_step(job,adapter,prepared)
    require(state['phase'] in PHASES[3:6],'QA_EXECUTABLE_PHASE_REQUIRED')
    return synthetic_step(job,prepared,adapter)


def timer_tick(owner,*,scheduled_at,trigger,rpc=None):
    global _last_poll
    if _stop.is_set() or not _lock.acquire(blocking=False):
        return dict(state='QA_ADMISSION_STOPPED_OR_LOCAL_OWNER_ACTIVE')
    handle=None
    try:
        now=time.monotonic()
        if now-_last_poll<60:
            return dict(state='QA_BOUNDED_POLL_INTERVAL')
        _last_poll=now;ROOT.mkdir(parents=True,exist_ok=True,mode=0o700)
        path=ROOT/'owner.lock';require(not path.is_symlink(),'QA_OWNER_LOCK_SYMLINK')
        handle=path.open('a+b');fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        rpc=rpc or MissionRPC()
        job=rpc.call('tick',owner,dict(version=VERSION,wave=WAVE,wrapper_sha256=digest(ENTRYPOINT.read_bytes()),
            qa_core_sha256=digest(Path(__file__).read_bytes()),
            host_instance=socket.gethostname(),host_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            scheduled_at=scheduled_at,trigger=trigger,
            invocation_id=uuid.uuid4().hex,prior_rpc_termination=_last_rpc_proof))
        if job.get('action')=='PRERELEASE_QA':
            invocation=job.get('invocation_id')
            require(isinstance(invocation,str) and re.fullmatch(r'[0-9a-f]{32}',invocation),'QA_ADMITTED_INVOCATION_REQUIRED')
            atomic_file(ROOT/('admission_received_'+invocation+'.json'),canonical_bytes(dict(
                invocation_id=invocation,attempt_id=job['attempt_id'],host_instance=socket.gethostname())))
            return execute_reserved_child(rpc,owner,job)
        if job.get('action')=='RECOVER_RECORDED_CHILD':
            return recover_recorded_attempt(rpc,job)
        if job.get('action')=='REPLAY_RECORDED_RECOVERY_TERMINAL':
            return replay_recorded_recovery_terminal(rpc,job)
        return job
    except Exception as error:
        LOG.warning('EQ20 pre-release QA dependency: %s',type(error).__name__)
        return dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY',reason=str(error)[:180])
    finally:
        if handle is not None:handle.close()
        _lock.release()


def request_stop():
    _stop.set()


def join_shutdown(timeout=0):
    return not _lock.locked()


def _reserved_child(owner,attempt):
    global _DIRECT_RPC,_ACCOUNT_WORK_DEADLINE
    require(re.fullmatch(r'[0-9a-f-]{36}',attempt),'QA_CHILD_ARGUMENT_REQUIRED')
    signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.setitimer(signal.ITIMER_REAL,6.5)
    resource.setrlimit(resource.RLIMIT_CPU,(QA_CHILD_CPU_SECONDS,QA_CHILD_CPU_SECONDS));resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
    resource.setrlimit(resource.RLIMIT_FSIZE,(256*1024*1024,256*1024*1024))
    os.nice(10);source_guards().prohibit_descendants()
    _DIRECT_RPC=True;_ACCOUNT_WORK_DEADLINE=time.monotonic()+5.0
    job=json.loads((ROOT/('job_'+attempt+'.json')).read_bytes())
    try:
        receipt=execute_segment(job,owner,attempt)
    except Exception as error:
        message=str(error);code=message if re.fullmatch(r'[A-Z0-9_]{1,180}',message) else type(error).__name__
        receipt=dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY',error=code,protected_outcomes_accessed=False)
        if code=='QA_COMMITTED_WORK_YIELD':
            receipt.update(state='RUNNING',qa_bounded_transfer_or_snapshot_preparation=True)
        if code=='FP01_COMMITTED_PREPARATION_YIELD':
            adapter=sys.modules.get('eq20_registered_fp01_qa_adapter')
            require(adapter is not None,'QA_REAL_PREPARATION_ADAPTER_REQUIRED')
            progress=adapter.preparation_progress(dict(job,_rpc_owner=owner,attempt_id=attempt),ROOT)
            receipt.update(state='RUNNING',preparation_progress=progress)
    atomic_file(ROOT/('receipt_'+attempt+'.json'),canonical_bytes(receipt))
    return 0 if receipt['state'] in ('RUNNING','VERIFIED') else 1


def main():
    if len(sys.argv)==3 and sys.argv[1]=='--rpc-helper':
        return _rpc_helper(sys.argv[2])
    if len(sys.argv)==4 and sys.argv[1]=='--reserved-child':
        return _reserved_child(sys.argv[2],sys.argv[3])
    raise SystemExit('QA_CLI_REJECTED')


if __name__=='__main__':
    raise SystemExit(main())
