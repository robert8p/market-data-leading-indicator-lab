"""Bounded full-population source preparation in the existing EQ20 worker.

This operational module contains no private research values or reconstructed
feature formulas. It captures bounded raw-session pointers/hashes and invokes
the byte-pinned private compiler. Provisional compact outputs retain the full
candidate denominator and unknowns; they cannot issue their own certificates.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import fcntl
import base64
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
from decimal import Decimal

VERSION = 'EQ20_FULL_POPULATION_SOURCE_V1_20261004'
RPC_NAME = 'eq20_full_population_source_v1'
ROOT = Path('/tmp/astra-eq20-w10/full_population_source')
MAX_REPLY = 2 * 1024 * 1024
MAX_REQUEST = 1792 * 1024
PARTITION_SCRATCH_ALLOWANCE = 96 * 1024 * 1024
PROCESS_GUARDS_SHA256 = '6a67b00323cc74178aab3deb045ac767a05c4b4d919bb383650379988eca472b'
ASSEMBLY_SHA256 = '0a2c91c78cd9050617bef7e66be7346a7af8727014e4bfe56b84754c069c5ab0'
RPC_ADMISSION_SHA256 = '72fda15472338d0b49afc9ab342bfec4b91de743d76d2fd0c0b0293b1d91fdff'
MIN_POLL_SECONDS = 60
LOG = logging.getLogger(__name__)
_stop = threading.Event()
_last_poll = 0.0
_last_proof = None
_guards = None
_lock = threading.Lock()
_child_metrics = None
_run_cpu_start = None
SHA = re.compile(r'[0-9a-f]{64}')
ACTION_GROUPS = {
    'reverse_splits', 'forward_splits', 'unit_splits', 'cash_dividends',
    'stock_dividends', 'spin_offs', 'cash_mergers', 'stock_mergers',
    'stock_and_cash_mergers', 'redemptions', 'name_changes',
    'worthless_removals', 'rights_distributions',
}


class SourceGateClosed(ValueError):
    pass


def require(value, reason):
    if not value:
        raise SourceGateClosed(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def source_guards():
    global _guards
    if _guards is None:
        path = Path(__file__).with_name('eq20_source_supervisor.py')
        require(path.is_file() and not path.is_symlink() and sha(path.read_bytes())==PROCESS_GUARDS_SHA256,
                'SOURCE_REVIEWED_PROCESS_GUARDS_PIN_REQUIRED')
        spec = importlib.util.spec_from_file_location('eq20_fp_source_process_guards', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _guards = module
    return _guards


def parse_alpaca_corporate_action_page(payload, *, page_sha256, captured_at):
    """Parse the actual v1 nested envelope, retaining fields and uncertainty.

    The older unrelated backfill read groups at the top level and silently
    omitted nested actions.  This separately versioned parser makes malformed
    or unknown envelopes explicit.  It performs no provider request and grants
    no identity, timing, publication or historical-completeness admission.
    """
    require(isinstance(payload, dict) and isinstance(page_sha256, str)
            and SHA.fullmatch(page_sha256), 'ACTION_PAGE_PROVENANCE_REQUIRED')
    require(isinstance(captured_at, str), 'ACTION_CAPTURE_TIME_REQUIRED')
    try:
        capture_time = datetime.fromisoformat(captured_at.replace('Z', '+00:00'))
    except ValueError:
        raise SourceGateClosed('ACTION_CAPTURE_TIME_REQUIRED') from None
    require(capture_time.tzinfo is not None, 'ACTION_CAPTURE_TIME_REQUIRED')
    groups = payload.get('corporate_actions')
    require(isinstance(groups, dict), 'NESTED_CORPORATE_ACTIONS_ENVELOPE_REQUIRED')
    require(not set(groups) - ACTION_GROUPS, 'UNREGISTERED_CORPORATE_ACTION_GROUP')
    token = payload.get('next_page_token')
    require(token is None or isinstance(token, str) and 0 < len(token) <= 4096,
            'ACTION_PAGINATION_TOKEN_REJECTED')
    actions = []
    for group in sorted(groups):
        rows = groups[group]
        require(isinstance(rows, list) and len(rows) <= 1000, 'ACTION_GROUP_BOUND')
        for index, row in enumerate(rows):
            require(isinstance(row, dict), 'ACTION_RECORD_SHAPE')
            # Do not infer symbol identity from only the surviving merger side.
            symbols = {key: row[key] for key in (
                'symbol', 'old_symbol', 'new_symbol', 'acquiree_symbol',
                'acquirer_symbol', 'initiating_symbol', 'target_symbol') if key in row}
            require(all(isinstance(x, str) and 0 < len(x) <= 64 for x in symbols.values()),
                    'ACTION_SYMBOL_SHAPE')
            raw = canonical(row)
            require(len(raw) <= MAX_REQUEST, 'ACTION_RECORD_BOUND')
            actions.append(dict(action_group=group, page_index=index,
                provider_record=json.loads(raw), provider_record_sha256=sha(raw),
                observed_symbol_fields=symbols, source_page_sha256=page_sha256,
                captured_at=captured_at, identity_disposition='UNADJUDICATED',
                publication_replay_certified=False, historical_completeness_certified=False))
    require(len(actions) <= 1000, 'ACTION_PAGE_BOUND')
    return dict(version=VERSION, actions=actions, next_page_token=token,
        transport_page_complete=token is None, capture_time=captured_at,
        page_sha256=page_sha256, historical_publication_time_inferred=False,
        historical_completeness_certified=False, source_admitted=False)


def _atomic(path, raw, *, immutable=True):
    require(len(raw) <= MAX_REPLY and not path.is_symlink(), 'SOURCE_SCRATCH_BOUND')
    if immutable and path.exists():
        require(path.read_bytes() == raw, 'SOURCE_LOCAL_IMMUTABILITY_CONFLICT')
        return
    require(source_guards().scratch_safe(len(raw)), 'EXISTING_SHARED_SCRATCH_CEILING')
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temp.open('xb') as handle:
            handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise SourceGateClosed('SOURCE_RPC_REDIRECT_REJECTED')


def _direct_unmetered(op, owner, args):
    base = os.environ.get('SUPABASE_URL', '').rstrip('/')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
    require(base == 'https://oxzabweahkoimtevbbny.supabase.co' and key, 'PRIVATE_CONFIGURATION_REQUIRED')
    value = dict(args)
    value['original_request_sha256']=sha(canonical(dict(op=op,owner=owner,args=args)))
    value['request_helper_identity']=source_guards().process_identity(os.getpid())
    value['request_host_instance']=socket.gethostname()
    document=dict(p_op=op,p_owner=owner,p_args=value)
    require(len(canonical(document))<=MAX_REQUEST,'SOURCE_COMPACT_TRANSFER_BOUND')
    path=Path(__file__).with_name('eq20_rpc_admission_v3.py')
    require(path.is_file() and not path.is_symlink() and sha(path.read_bytes())==RPC_ADMISSION_SHA256,
            'SOURCE_SHARED_RPC_ADMISSION_PIN_REQUIRED')
    spec=importlib.util.spec_from_file_location('eq20_source_rpc_admission_v3',path)
    admission=importlib.util.module_from_spec(spec);spec.loader.exec_module(admission)
    def post(rpc_name,data):
        require(rpc_name in (RPC_NAME,'eq20_request_permit_v3') and len(data)<=2*1024*1024,'SOURCE_FIXED_RPC_ENVELOPE_BOUND')
        return _post_fixed(base,key,rpc_name,data)
    # Both HTTP calls stay inside this already watched helper/direct operation.
    # Nothing that scans scratch or publishes files runs between mint and send.
    return admission.send(document,target_rpc='public.'+RPC_NAME+'(text,text,jsonb)',
        post=post,canonical_bytes=canonical,host_instance=socket.gethostname(),
        boot_id=value['request_helper_identity']['boot_id'],read_only=op=='status')


def _post_fixed(base,key,rpc_name,data):
    request = urllib.request.Request(base + '/rest/v1/rpc/' + rpc_name, data=data,
        headers={'Authorization': 'Bearer ' + key, 'apikey': key, 'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.build_opener(_NoRedirect()).open(request, timeout=1.5) as response:
            raw = response.read(MAX_REPLY + 1)
    except urllib.error.HTTPError as error:
        try:
            message = json.loads(error.read(4096)).get('message', '')
            code = message if isinstance(message, str) and re.fullmatch(r'[A-Z0-9_]{1,160}', message) else 'DATABASE_ERROR'
        except Exception:
            code = 'DATABASE_ERROR'
        raise SourceGateClosed('SOURCE_RPC_HTTP_%s_%s' % (error.code, code)) from None
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        raise SourceGateClosed('SOURCE_RPC_TRANSPORT_FAILURE') from None
    require(len(raw) <= MAX_REPLY, 'SOURCE_RPC_REPLY_BOUND')
    result = json.loads(raw)
    require(isinstance(result, dict), 'SOURCE_RPC_REPLY_SHAPE')
    return result


def _direct(op,owner,args):
    """Existing source accounting unit: child CPU plus observed RPC elapsed."""
    if _child_metrics is None:
        return _direct_unmetered(op,owner,args)
    require(16-time.process_time()-_child_metrics['rpc_elapsed_seconds']>=2.5,
            'SOURCE_GOVERNED_CHILD_YIELD')
    _child_metrics['rpc_pending']=op
    _atomic(ROOT/('metrics_'+_child_metrics['attempt_id']+'.json'),canonical(_child_metrics),immutable=False)
    started=time.monotonic();prior=signal.getitimer(signal.ITIMER_REAL)[0]
    signal.setitimer(signal.ITIMER_REAL,min(2.1,prior) if prior>0 else 2.1)
    try:
        result=_direct_unmetered(op,owner,args)
        _child_metrics['rpc_pending']=None
        return result
    except Exception:
        _child_metrics['transport_failure']=True
        raise
    finally:
        elapsed=time.monotonic()-started
        _child_metrics['rpc_elapsed_seconds']+=elapsed
        _child_metrics['rpc_calls']+=1
        signal.setitimer(signal.ITIMER_REAL,max(.001,prior-elapsed) if prior>0 else .001)
        remaining=15.5-time.process_time()-_child_metrics['rpc_elapsed_seconds']
        signal.setitimer(signal.ITIMER_PROF,max(.001,remaining))
        _atomic(ROOT/('metrics_'+_child_metrics['attempt_id']+'.json'),canonical(_child_metrics),immutable=False)


def bounded_rpc(op, owner, args):
    """One exact helper, actual wait4, and an included three-second SQL tail."""
    global _last_proof
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    token = uuid.uuid4().hex
    request = ROOT / ('rpc_' + token + '.json')
    response = ROOT / ('reply_' + token + '.json')
    raw = canonical(dict(op=op, owner=owner, args=args))
    call_started=time.monotonic()
    _atomic(request, raw)
    child = source_guards().ReapedChild([sys.executable, '-I', '-S',
        str(Path(__file__).resolve()), '--rpc-helper', token])
    started = time.monotonic(); cpu_started = time.process_time(); success = False
    try:
        while not child.reap():
            if time.monotonic() - started >= 1.8 or time.process_time() - cpu_started >= .75:
                child.stop(); break
            time.sleep(.02)
        require(child.termination_proof is not None and child.cpu is not None,
                'SOURCE_HELPER_QUIESCENCE_REQUIRED')
        if response.exists():
            require(not response.is_symlink() and response.stat().st_size <= MAX_REPLY,
                    'SOURCE_REPLY_FILE_BOUND')
            value = json.loads(response.read_bytes())
            if value.get('success') is True and child.exit_code == 0:
                success = True
                return value['response']
            raise SourceGateClosed(value.get('error', 'SOURCE_HELPER_FAILED'))
        raise SourceGateClosed('SOURCE_HELPER_NO_RECEIPT')
    finally:
        child.stop()
        if not success:
            until = time.monotonic() + 3
            while time.monotonic() < until:
                time.sleep(min(.05, until - time.monotonic()))
        _last_proof = dict(operation=op, request_sha256=sha(raw),
            attempt_id=args.get('attempt_id'), host_instance=socket.gethostname(),
            process_identity=child.original_identity, process_finished=bool(child.termination_proof),
            process_termination_proof=child.termination_proof, exit_code=child.exit_code,
            measured_child_cpu_seconds=child.cpu, bounded_governed_seconds=7,
            rpc_elapsed_seconds=time.monotonic()-call_started,
            proof_scope='EXACT_HTTP_HELPER_AND_BOUNDED_SQL_TAIL',
            sql_tail_waited_seconds=0 if success else 3,rpc_completed_successfully=success,
            completed_at=datetime.now(timezone.utc).isoformat())
        _atomic(ROOT / 'last_rpc_proof.json', canonical(_last_proof), immutable=False)
        request.unlink(missing_ok=True); response.unlink(missing_ok=True)


def run_once(owner, *, scheduled_at, trigger, rpc=bounded_rpc):
    global _run_cpu_start
    _run_cpu_start=time.process_time()
    require(isinstance(owner, str) and 0 < len(owner) <= 160, 'SOURCE_OWNER_REQUIRED')
    require(type(scheduled_at) in (int, float) and math.isfinite(scheduled_at), 'SOURCE_TIMER_TIME_REQUIRED')
    require(trigger in ('PERSISTENT_WORKER_STARTUP', 'PERSISTENT_WORKER_TIMER', 'MANUAL_OPERATOR_CHECK'),
            'SOURCE_TRIGGER_REQUIRED')
    if _stop.is_set():
        return dict(state='EXPLICIT_STOP_PRESERVED')
    global _last_proof
    if _last_proof is None:
        proof_path=ROOT/'last_rpc_proof.json'
        if proof_path.is_file() and not proof_path.is_symlink() and proof_path.stat().st_size<=16384:
            saved=json.loads(proof_path.read_bytes())
            if saved.get('host_instance')==socket.gethostname():_last_proof=saved
    args = dict(version=VERSION, wrapper_sha256=sha(Path(__file__).read_bytes()),
        host_instance=socket.gethostname(), invocation_id=uuid.uuid4().hex,
        scheduled_at=scheduled_at, trigger=trigger, prior_rpc_termination=_last_proof)
    invocation=args['invocation_id']
    intent=dict(owner=owner,args=args,request_sha256=sha(canonical(dict(op='tick',owner=owner,args=args))),
        parent_identity=source_guards().process_identity(os.getpid()),host_instance=socket.gethostname())
    _atomic(ROOT/('intent_'+invocation+'.json'),canonical(intent))
    try:
        response = rpc('tick', owner, args)
    finally:
        if isinstance(_last_proof,dict) and _last_proof.get('operation')=='tick' and _last_proof.get('request_sha256')==intent['request_sha256']:
            _atomic(ROOT/('intent_rpc_proof_'+invocation+'.json'),canonical(_last_proof))
    if response.get('action') == 'RECOVER_SOURCE_CHILD':
        return recover_source_child(response, owner, rpc)
    if response.get('action') != 'RUN_SOURCE_BATCH':
        return response
    require(response.get('reserved_governed_seconds') == 30, 'SOURCE_FINITE_RESERVATION_REQUIRED')
    response=dict(response,origin_invocation_id=invocation)
    _atomic(ROOT/('intent_ack_'+invocation+'.json'),canonical(response))
    return execute_source_child(response, owner, rpc)


def known_accounting_floor(child_cpu,parent_cpu,control_proof,metrics,attempt):
    """Missing census data cannot erase finite observed resource violations."""
    def finite(value):return type(value) in (int,float) and math.isfinite(value) and value>=0
    child_cpu=child_cpu if finite(child_cpu) else 0.0
    parent_cpu=parent_cpu if finite(parent_cpu) else 0.0
    control=control_proof if isinstance(control_proof,dict) else {}
    own=metrics if isinstance(metrics,dict) and metrics.get('attempt_id')==attempt else {}
    helper=control.get('measured_child_cpu_seconds');helper=helper if finite(helper) else 0.0
    control_rpc=control.get('rpc_elapsed_seconds');control_rpc=control_rpc if finite(control_rpc) else 0.0
    child_rpc=own.get('rpc_elapsed_seconds');child_rpc=child_rpc if finite(child_rpc) else 0.0
    minimum=math.ceil((child_cpu+parent_cpu+helper+control_rpc+child_rpc+7)*1000)/1000
    violation=child_cpu+child_rpc>16 or parent_cpu+helper+control_rpc>7 or minimum>30
    return dict(observed_child_cpu_seconds=child_cpu,observed_parent_cpu_seconds=parent_cpu,
        observed_control_helper_cpu_seconds=helper,observed_control_rpc_seconds=control_rpc,
        observed_child_rpc_seconds=child_rpc,retained_terminal_allowance_seconds=7,
        minimum_charge_seconds=max(30,minimum),known_resource_overrun=violation,
        complete_measurement_claimed=False)


def execute_source_child(job, owner, rpc):
    attempt = job['attempt_id']
    require(re.fullmatch(r'[0-9a-f-]{36}', attempt), 'SOURCE_ATTEMPT_FORMAT')
    job_path = ROOT / ('job_' + attempt + '.json')
    result_path = ROOT / ('result_' + attempt + '.json')
    require(not job_path.exists() and not result_path.exists(), 'SOURCE_CHILD_REPLAY_FORBIDDEN')
    _atomic(job_path, canonical(job))
    control_proof=_last_proof
    guards = source_guards(); began = time.monotonic(); cpu_started = time.process_time()
    _atomic(ROOT/('launch_intent_'+attempt+'.json'),canonical(dict(attempt_id=attempt,
        origin_invocation_id=job['origin_invocation_id'],host_instance=socket.gethostname(),
        launch_admitted_at=datetime.now(timezone.utc).isoformat())))
    process = guards.ReapedChild([sys.executable, '-I', '-S', str(Path(__file__).resolve()),
        '--source-child', owner, attempt])
    try:
        require(process.original_identity is not None, 'SOURCE_EXACT_CHILD_IDENTITY_REQUIRED')
        _atomic(ROOT / ('process_' + attempt + '.json'), canonical(process.original_identity))
        while not process.reap():
            if time.monotonic() - began > 17 or time.process_time() - cpu_started > .5:
                break
            time.sleep(.02)
    finally:
        process.stop()
    require(process.termination_proof is not None and process.cpu is not None,
            'SOURCE_CHILD_QUIESCENCE_REQUIRED')
    cleanup_owned_partition(attempt)
    if process.exit_code != 0:
        time.sleep(3)
    if result_path.is_file() and not result_path.is_symlink() and result_path.stat().st_size <= MAX_REPLY:
        receipt = json.loads(result_path.read_bytes())
    else:
        receipt = dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY', error='SOURCE_CHILD_NO_RECEIPT')
    metrics=receipt.get('resource_metrics',{})
    metrics_path=ROOT/('metrics_'+attempt+'.json')
    journal=json.loads(metrics_path.read_bytes()) if metrics_path.is_file() and not metrics_path.is_symlink() and metrics_path.stat().st_size<=8192 else None
    verified=bool(isinstance(control_proof,dict) and control_proof.get('process_finished') is True
        and control_proof.get('operation')=='tick' and control_proof.get('exit_code')==0
        and metrics==journal and metrics.get('attempt_id')==attempt and metrics.get('rpc_pending') is None
        and metrics.get('transport_failure') is False
        and type(metrics.get('rpc_elapsed_seconds')) in (int,float) and math.isfinite(metrics['rpc_elapsed_seconds']))
    parent_cpu=time.process_time()-(_run_cpu_start if _run_cpu_start is not None else cpu_started)
    known=known_accounting_floor(process.cpu,parent_cpu,control_proof,journal,attempt)
    parent_helper_cpu=control_proof.get('measured_child_cpu_seconds',0) if verified else 0
    rpc_elapsed=(metrics['rpc_elapsed_seconds']+control_proof['rpc_elapsed_seconds']) if verified else 0
    measured=math.ceil((process.cpu+parent_cpu+parent_helper_cpu+rpc_elapsed+7)*1000)/1000 if verified else known['minimum_charge_seconds']
    overrun=known['known_resource_overrun'] or bool(verified and (process.cpu+metrics['rpc_elapsed_seconds']>16
        or parent_cpu+parent_helper_cpu+control_proof['rpc_elapsed_seconds']>7 or measured>30))
    receipt.update(attempt_id=attempt, process_finished=True, process_identity=process.original_identity,
        process_termination_proof=process.termination_proof, host_instance=socket.gethostname(),
        exit_code=process.exit_code, child_cpu_seconds=process.cpu,
        child_wall_seconds=time.monotonic()-began, parent_cpu_seconds=time.process_time()-cpu_started,
        protected_outcomes_accessed=False,
        accounting_mode='VERIFIED_PREFIX_PLUS_TERMINAL_ALLOWANCE' if verified else 'FULL_RESERVATION_CONSERVATIVE',
        measurement_verified=verified,resource_overrun=overrun,measured_governed_seconds=measured,
        known_component_accounting=known,
        parent_helper_cpu_seconds=parent_helper_cpu,parent_total_cpu_seconds=parent_cpu,
        combined_rpc_elapsed_seconds=rpc_elapsed,terminal_allowance_seconds=7,
        governed_envelope=dict(parent_control_bound=7,child_cpu_plus_rpc_bound=16,terminal_bound=7,reserved=30))
    request = dict(owner=owner, args=dict(attempt_id=attempt, receipt=receipt))
    _atomic(ROOT / ('terminal_' + attempt + '.json'), canonical(request))
    return rpc('terminal', owner, request['args'])


def recover_source_child(job, owner, rpc):
    require(job.get('host_instance') == socket.gethostname(), 'PRIOR_HOST_TERMINATION_RECEIPT_REQUIRED')
    attempt = job['attempt_id']
    identity_path = ROOT / ('process_' + attempt + '.json')
    guards=source_guards(); guards.assert_proc_namespace()
    unlaunched=not identity_path.exists()
    if unlaunched:
        invocation=job.get('original_invocation_id')
        require(isinstance(invocation,str) and re.fullmatch(r'[0-9a-f]{32}',invocation),'SOURCE_ORIGINAL_INVOCATION_REQUIRED')
        intent_path=ROOT/('intent_'+invocation+'.json');helper_path=ROOT/('intent_rpc_proof_'+invocation+'.json')
        require(not (ROOT/('launch_intent_'+attempt+'.json')).exists()
                and intent_path.is_file() and helper_path.is_file() and not intent_path.is_symlink() and not helper_path.is_symlink()
                and intent_path.stat().st_size<=MAX_REQUEST and helper_path.stat().st_size<=16384,'SOURCE_EXACT_NO_LAUNCH_INTENT_PROOF_REQUIRED')
        intent_raw=intent_path.read_bytes();intent=json.loads(intent_raw);helper=json.loads(helper_path.read_bytes())
        expected=sha(canonical(dict(op='tick',owner=intent.get('owner'),args=intent.get('args'))))
        require(intent.get('owner')==job['original_owner'] and intent.get('host_instance')==socket.gethostname()
                and intent.get('request_sha256')==expected==job.get('original_request_sha256')==helper.get('request_sha256')
                and intent.get('args',{}).get('invocation_id')==invocation
                and intent.get('parent_identity',{}).get('boot_id')==guards.process_identity(os.getpid())['boot_id']
                and helper.get('operation')=='tick' and helper.get('process_finished') is True
                and helper.get('process_termination_proof')=='SPECIFIC_CHILD_WAIT4'
                and (helper.get('rpc_completed_successfully') is True or helper.get('sql_tail_waited_seconds')==3),
                'SOURCE_UNLAUNCHED_EXACT_REQUEST_AND_HELPER_PROOF_REQUIRED')
        identity=dict(kind='UNLAUNCHED_RESERVATION',invocation_id=invocation,request_sha256=expected,
                      boot_id=intent['parent_identity']['boot_id'])
        inspection=dict(process_identity=identity,proof_scope='EXACT_UNLAUNCHED_RESERVATION',
            same_host_identity_inspected=True,quiescence_claimed=False,launch_intent_absent=True,
            original_request_sha256=expected,intent_canonical_utf8=intent_raw.decode(),intent_sha256=sha(intent_raw),
            actual_reservation_helper_termination=helper)
    else:
        require(identity_path.is_file() and not identity_path.is_symlink() and identity_path.stat().st_size<=8192,
                'SOURCE_PRIOR_CHILD_IDENTITY_REQUIRED')
        identity=json.loads(identity_path.read_bytes())
        require(type(identity.get('pid')) is int and identity['pid']>1
                and type(identity.get('start_ticks')) is int and isinstance(identity.get('boot_id'),str)
                and identity.get('process_group')==identity['pid'],'SOURCE_PRIOR_PROCESS_IDENTITY_REQUIRED')
        # No signal or SQL-tail wait occurs before a fresh maintenance reserve.
        try:observed=guards.process_identity(identity['pid'])
        except FileNotFoundError:observed=None
        inspection=dict(process_identity=identity,observed_identity=observed,
            same_host_identity_inspected=True,quiescence_claimed=False,
            proof_scope='READONLY_ORIGINAL_PROCESS_IDENTITY_ONLY')
    reserved=rpc('recovery_reserve',owner,dict(attempt_id=attempt,proof=inspection,
        wrapper_sha256=sha(Path(__file__).read_bytes()),version=VERSION,host_instance=socket.gethostname()))
    if reserved.get('reserved_governed_seconds') != 30:return reserved
    proof=(dict(process_finished=True,proof='EXACT_UNLAUNCHED_RESERVATION_REQUEST_AND_HELPER_QUIESCENCE') if unlaunched
           else guards.quiesce_recorded_child(identity,guards.ParentBudget(),reserve_rpc=False))
    require(proof.get('process_finished') is True,'SOURCE_ORIGINAL_CHILD_STILL_ACTIVE')
    time.sleep(3)
    proof.update(process_identity=identity,sql_tail_waited_seconds=3)
    prior_terminal=None
    terminal=ROOT / ('terminal_' + attempt + '.json')
    if terminal.is_file():
        require(not terminal.is_symlink() and terminal.stat().st_size<=MAX_REPLY,'SOURCE_TERMINAL_FILE_BOUND')
        original_raw=terminal.read_bytes();saved=json.loads(original_raw);prior=saved.get('args',{}).get('receipt',{})
        require(saved.get('owner')==job['original_owner'] and saved['args'].get('attempt_id')==attempt
                and prior.get('process_identity')==identity and prior.get('process_finished') is True
                and prior.get('process_termination_proof')=='SPECIFIC_CHILD_WAIT4'
                and prior.get('protected_outcomes_accessed') is False,'SOURCE_ORIGINAL_TERMINAL_BINDING_REQUIRED')
        prior_terminal=prior
        proof.update(original_terminal_request_utf8=original_raw.decode(),original_terminal_request_sha256=sha(original_raw))
    saved_proof=rpc('recovery_proof',job['original_owner'],dict(attempt_id=attempt,recovery_id=reserved['recovery_id'],
        proof=proof,prior_rpc_termination=_last_proof))
    require(saved_proof.get('committed') is True,'SOURCE_DURABLE_ORIGINAL_QUIESCENCE_REQUIRED')
    cleanup_owned_partition(attempt)
    receipt=dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY',error='SOURCE_INTERRUPTED_CHILD_RECONCILED',
        attempt_id=attempt,process_finished=True,process_identity=identity,
        process_termination_proof=proof,host_instance=socket.gethostname(),protected_outcomes_accessed=False)
    if prior_terminal is not None:receipt=prior_terminal
    return rpc('recovery_terminal',job['original_owner'],dict(attempt_id=attempt,
        recovery_id=reserved['recovery_id'],receipt=receipt,prior_rpc_termination=_last_proof))


def _code_ready(job):
    require(isinstance(job.get('bundle_sha256'),str) and SHA.fullmatch(job['bundle_sha256'])
            and isinstance(job.get('files'),list) and 9<=len(job['files'])<=16,'SOURCE_PRIVATE_MANIFEST_REQUIRED')
    directory=ROOT/'private_code'/job['bundle_sha256']
    for item in job['files']:
        require(isinstance(item,dict) and isinstance(item.get('name'),str)
                and re.fullmatch(r'[a-z0-9_]+\.(py|json)',item['name'])
                and type(item.get('bytes')) is int and 0<item['bytes']<=1024*1024
                and isinstance(item.get('sha256'),str) and SHA.fullmatch(item['sha256']),
                'SOURCE_PRIVATE_MANIFEST_FILE_REQUIRED')
        path=directory/item['name']
        if not path.is_file() or path.is_symlink() or path.stat().st_size!=item['bytes'] or sha(path.read_bytes())!=item['sha256']:
            return False
    return True


def cleanup_owned_partition(attempt):
    """Only after actual child quiescence; never touches another work tree."""
    require(re.fullmatch(r'[0-9a-f-]{36}',attempt),'SOURCE_CLEANUP_ATTEMPT_REQUIRED')
    directory=ROOT/('partition_'+attempt)
    if not directory.exists():
        return
    require(directory.is_dir() and not directory.is_symlink(),'SOURCE_OWNED_PARTITION_PATH_REQUIRED')
    entries=list(directory.iterdir())
    require(len(entries)<=3 and all(p.name in {'unit_registry.sqlite','unit_registry.sqlite-journal',
                'unit_registry.sqlite-wal'} and p.is_file() and not p.is_symlink()
                and p.stat().st_size<=64*1024*1024 for p in entries),'SOURCE_OWNED_CLEANUP_BOUND')
    for path in entries:
        path.unlink()
    directory.rmdir()


def _install_code(job,owner):
    directory=ROOT/'private_code'/job['bundle_sha256'];directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    reply=_direct('bundle',owner,dict(attempt_id=job['attempt_id'],bundle_sha256=job['bundle_sha256']))
    require(isinstance(reply.get('files'),list) and len(reply['files'])==len(job['files']), 'SOURCE_PRIVATE_BUNDLE_REQUIRED')
    expected={item['name']:item for item in job['files']}
    for item in reply['files']:
        name=item.get('name');require(name in expected and re.fullmatch(r'[a-z0-9_]+\.(py|json)',name),'SOURCE_PRIVATE_FILENAME')
        raw=item['payload_text'].encode();meta=expected[name]
        require(len(raw)==meta['bytes'] and sha(raw)==meta['sha256'],'SOURCE_PRIVATE_FILE_PIN')
        _atomic(directory/name,raw)
    require(_code_ready(job),'SOURCE_PRIVATE_CODE_READBACK_REQUIRED')
    return _direct('code_ack',owner,dict(attempt_id=job['attempt_id'],bundle_sha256=job['bundle_sha256'],
        files=job['files'],host_instance=socket.gethostname()))


def captured_specialist_inputs(source,job,owner,direct):
    """Resume actual bounded registry pages; admission remains independent.

    Only page pointers and hashes persist in SQL. The local immutable cache is
    replaceable from the original registry and cannot confer PIT or family
    admission. Every page is hash-read before its current-host acknowledgement.
    """
    if source.get('admission') is None:return source,None
    ordinal=source['local_ordinal'];common=dict(attempt_id=job['attempt_id'],local_ordinal=ordinal)
    manifest=direct('specialist_manifest',owner,common)
    raw=manifest.get('binding_text');binding_hash=manifest.get('binding_sha256')
    require(isinstance(raw,str) and sha(raw.encode())==binding_hash and SHA.fullmatch(binding_hash),
            'SOURCE_ACTUAL_SPECIALIST_BINDING_REQUIRED')
    binding=json.loads(raw);kinds=manifest.get('kinds')
    require(binding.get('kinds')==kinds and isinstance(kinds,list) and len(kinds)==len(set(kinds))
        and set(kinds)<= {'FUNDAMENTAL_FACT','SEC_FILING','SHORT_INTEREST','SHORT_VOLUME'}
        and binding.get('maximum_issuer_payload_bytes')==8*1024*1024,
        'SOURCE_BOUNDED_SPECIALIST_FAMILIES_REQUIRED')
    directory=ROOT/'specialist_cache'/binding_hash
    require(not directory.is_symlink() and not directory.parent.is_symlink(),'SOURCE_SPECIALIST_CACHE_SYMLINK')
    directory.mkdir(parents=True,exist_ok=True,mode=0o700)
    inputs=dict(source['admission']['specialist_inputs']);families={name:[] for name in kinds};total=0
    for kind in kinds:
        previous=None
        for number in range(2048):
            path=directory/(kind+'_%04d.json'%number)
            require(not path.is_symlink(),'SOURCE_SPECIALIST_PAGE_SYMLINK')
            if path.is_file():
                require(path.stat().st_size<=MAX_REPLY,'SOURCE_SPECIALIST_LOCAL_PAGE_BOUND');reply=json.loads(path.read_bytes())
            else:
                reply=direct('specialist_page',owner,dict(common,binding_sha256=binding_hash,kind=kind,page_no=number))
                _atomic(path,canonical(reply))
            payload=reply.get('payload_text')
            require(isinstance(payload,str) and len(payload.encode())<=2*1024*1024
                and sha(payload.encode())==reply.get('payload_sha256')
                and reply.get('binding_sha256')==binding_hash and reply.get('kind')==kind and reply.get('page_no')==number
                and reply.get('previous_cursor')==previous,'SOURCE_EXACT_SPECIALIST_PAGE_READBACK_REQUIRED')
            total+=len(payload.encode());require(total<=8*1024*1024,'SOURCE_SPECIALIST_BOUNDED_ISSUER_MATERIALIZATION_REQUIRED')
            rows=json.loads(payload);require(isinstance(rows,list) and len(rows)==reply.get('records') and len(rows)<=128,
                                             'SOURCE_SPECIALIST_PAGE_COUNT_REQUIRED')
            for row in rows:
                text=row.get('payload_text');require(isinstance(text,str) and sha(text.encode())==row.get('payload_sha256'),
                    'SOURCE_SPECIALIST_EXACT_RECORD_HASH_REQUIRED')
                families[kind].append(json.loads(text,parse_float=Decimal))
            ack=direct('specialist_ack',owner,dict(common,binding_sha256=binding_hash,kind=kind,page_no=number,
                payload_sha256=reply['payload_sha256'],actual_local_readback=True))
            require(ack.get('committed') is True and ack.get('payload_sha256')==reply['payload_sha256'],
                    'SOURCE_SPECIALIST_ACTUAL_PAGE_ACK_REQUIRED')
            if reply.get('terminal_empty_page') is True:
                require(not rows,'SOURCE_SPECIALIST_EMPTY_TERMINAL_REQUIRED');break
            require(rows and reply.get('last_cursor') is not None,'SOURCE_SPECIALIST_ADVANCING_CURSOR_REQUIRED')
            previous=reply['last_cursor']
        else:raise SourceGateClosed('SOURCE_SPECIALIST_FINITE_PAGE_LIMIT')
    verified=direct('specialist_verify',owner,dict(common,binding_sha256=binding_hash))
    require(verified.get('verified') is True and isinstance(verified.get('capture_receipt_sha256'),str)
            and SHA.fullmatch(verified['capture_receipt_sha256']),'SOURCE_SPECIALIST_COMPLETE_REGISTRY_READBACK_REQUIRED')
    inputs.update(facts=families.get('FUNDAMENTAL_FACT',[]),events=families.get('SEC_FILING',[]),
        positioning={kind:families.get(kind,[]) for kind in ('SHORT_INTEREST','SHORT_VOLUME')},
        capture_complete=True,actual_registry_capture_receipt_sha256=verified['capture_receipt_sha256'])
    admitted=dict(source['admission'],specialist_inputs=inputs)
    return dict(source,admission=admitted),dict(local_ordinal=ordinal,binding_sha256=binding_hash,
        capture_receipt_sha256=verified['capture_receipt_sha256'])


def cleanup_committed_specialist_cache(binding_hash):
    require(isinstance(binding_hash,str) and SHA.fullmatch(binding_hash),'SOURCE_SPECIALIST_CLEANUP_KEY')
    directory=ROOT/'specialist_cache'/binding_hash
    if not directory.exists():return
    require(directory.is_dir() and not directory.is_symlink() and not directory.parent.is_symlink(),'SOURCE_SPECIALIST_CLEANUP_DIRECTORY')
    files=list(directory.iterdir())
    require(len(files)<=8192 and sum(p.stat().st_size for p in files)<=32*1024*1024,'SOURCE_SPECIALIST_CLEANUP_BOUND')
    for path in files:
        require(path.is_file() and not path.is_symlink() and path.stat().st_size<=MAX_REPLY
            and re.fullmatch(r'(FUNDAMENTAL_FACT|SEC_FILING|SHORT_INTEREST|SHORT_VOLUME)_[0-9]{4}\.json',path.name),
            'SOURCE_SPECIALIST_EXACT_OWNED_CACHE_REQUIRED')
        reply=json.loads(path.read_bytes());payload=reply.get('payload_text')
        require(reply.get('binding_sha256')==binding_hash and isinstance(payload,str)
            and sha(payload.encode())==reply.get('payload_sha256'),'SOURCE_SPECIALIST_CLEANUP_HASH_REQUIRED')
        path.unlink()
    directory.rmdir()


def compile_source_batch(job, owner, *, direct=_direct):
    if not _code_ready(job):
        _install_code(job,owner)
        return dict(state='RUNNING',code_readback_committed=True,protected_outcomes_accessed=False)
    directory=ROOT/'private_code'/job['bundle_sha256']
    spec=importlib.util.spec_from_file_location('eq20_registered_full_population_compiler',directory/'eq20_full_population_compiler_v1.py')
    compiler_module=importlib.util.module_from_spec(spec);spec.loader.exec_module(compiler_module)
    compiler=compiler_module.Compiler(directory)
    if job.get('phase')=='ASSEMBLY':
        path=Path(__file__).with_name('eq20_full_population_assembly.py')
        require(path.is_file() and not path.is_symlink() and sha(path.read_bytes())==ASSEMBLY_SHA256,'SOURCE_ASSEMBLY_IMPLEMENTATION_PIN_REQUIRED')
        spec=importlib.util.spec_from_file_location('eq20_registered_full_population_assembly',path)
        assembly=importlib.util.module_from_spec(spec);spec.loader.exec_module(assembly)
        return assembly.step(job,owner,direct,ROOT,directory,source_guards().scratch_safe)
    raw=direct('source_batch',owner,dict(attempt_id=job['attempt_id']))
    if raw.get('complete') is True:
        return dict(state='VERIFIED',complete=True,protected_outcomes_accessed=False)
    require(isinstance(raw.get('sources'),list) and 0<len(raw['sources'])<=8,'BOUNDED_FULL_SOURCE_BATCH_REQUIRED')
    require(isinstance(raw.get('calendar'),dict),'SOURCE_EXACT_CALENDAR_REQUIRED')
    require(source_guards().scratch_safe(PARTITION_SCRATCH_ALLOWANCE),'SOURCE_PARTITION_PHYSICAL_SCRATCH_ADMISSION')
    if job.get('phase')=='CLOCK_QA':
        require(len(raw['sources'])==1,'SOURCE_FIXED_FIRST_CLOCK_QA_SESSION_REQUIRED')
        receipt=compiler.clock_sensitivity(dict(raw['sources'][0],calendar=raw['calendar']),source_corpus_sha256=job['assembly_job']['source_corpus_sha256'])
        result=direct('clock_commit',owner,dict(attempt_id=job['attempt_id'],receipt=receipt))
        require(result.get('committed') is True,'SOURCE_ACTUAL_CLOCK_QA_COMMIT_REQUIRED')
        return dict(result,state='RUNNING',complete=True,protected_outcomes_accessed=False)
    compiled=[];specialist_receipts=[]
    for value in raw['sources']:
        value,specialist_receipt=captured_specialist_inputs(value,job,owner,direct)
        if specialist_receipt:specialist_receipts.append(specialist_receipt)
        compiled.append(compiler.compile(dict(value,calendar=raw['calendar'])))
    included=[value for value in compiled if value['technical'] is not None]
    output=ROOT/('partition_'+job['attempt_id']);output.mkdir(exist_ok=False,mode=0o700)
    files=compiler_module.partition_bytes(compiled,output)
    artifacts=[]
    for role,value in files.items():
        require(len(value)<=32*1024*1024,'SOURCE_COMPILED_PARTITION_BOUND')
        packed=zlib.compress(value,3)
        require(len(packed)<=512*1024,'SOURCE_COMPACT_TRANSFER_BOUND')
        artifacts.append(dict(role=role,raw_sha256=sha(value),raw_bytes=len(value),
            encoded_sha256=sha(packed),encoded_bytes=len(packed),codec='zlib',payload_base64=base64.b64encode(packed).decode()))
    units={unit['unit_sha256']:compiler_module.canonical(unit['unit_key']).decode()
           for value in included for unit in value['unit_registry_member']['units']}
    receipt=direct('compiled_commit',owner,dict(attempt_id=job['attempt_id'],sources=raw['source_members'],
        member_states=[value['state'] for value in compiled],files=artifacts,
        units=[dict(unit_id=key,unit_key_text=value) for key,value in sorted(units.items())],
        membership_sha256=sha(b''.join(canonical(value['key'])+b'\n' for value in included)),
        specialist_capture_receipts=specialist_receipts,
        decisions=sum(len(value['technical']['decisions']) for value in included),
        research_objective_achieved=False,protected_outcomes_accessed=False))
    require(receipt.get('committed') is True,'SOURCE_ACTUAL_PARTITION_COMMIT_REQUIRED')
    # Verified outputs are durable private blobs. Remove only this completed
    # transient SQLite to preserve the original shared scratch ceiling.
    (output/'unit_registry.sqlite').unlink();output.rmdir()
    for binding_hash in sorted({r['binding_sha256'] for r in specialist_receipts}):
        cleanup_committed_specialist_cache(binding_hash)
    return dict(state='RUNNING',committed=True,receipt_key=receipt['receipt_key'],
                source_members_committed=len(compiled),protected_outcomes_accessed=False)


def _source_child(owner,attempt):
    global _child_metrics
    require(re.fullmatch(r'[0-9a-f-]{36}',attempt),'SOURCE_CHILD_ARGUMENT')
    signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.setitimer(signal.ITIMER_REAL,16.5)
    signal.signal(signal.SIGPROF,signal.SIG_DFL);signal.setitimer(signal.ITIMER_PROF,15.5)
    resource.setrlimit(resource.RLIMIT_CPU,(16,16))
    sys.dont_write_bytecode=True
    resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
    resource.setrlimit(resource.RLIMIT_FSIZE,(64*1024*1024,64*1024*1024))
    os.nice(10);source_guards().prohibit_descendants()
    job=json.loads((ROOT/('job_'+attempt+'.json')).read_bytes())
    _child_metrics=dict(attempt_id=attempt,rpc_elapsed_seconds=0.0,rpc_calls=0,
        rpc_calls_unit='LOGICAL_SOURCE_OPERATIONS_INCLUDING_PERMIT_AND_MUTATION',
        rpc_pending=None,transport_failure=False)
    _atomic(ROOT/('metrics_'+attempt+'.json'),canonical(_child_metrics),immutable=False)
    committed=0
    try:
        result=dict(state='RUNNING',bounded_yield=True,protected_outcomes_accessed=False)
        for _ in range(64):
            if 16-time.process_time()-_child_metrics['rpc_elapsed_seconds']<5:
                break
            result=compile_source_batch(job,owner)
            committed+=result.get('source_members_committed',0)
            if result.get('complete') is True:
                break
    except Exception as error:
        code=str(error)
        result=dict(state='RUNNING' if code=='SOURCE_GOVERNED_CHILD_YIELD' else 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
            error=code if re.fullmatch(r'[A-Z0-9_]{1,180}',code) else type(error).__name__,protected_outcomes_accessed=False)
    result.update(resource_metrics=_child_metrics,source_members_committed=committed)
    _atomic(ROOT/('result_'+attempt+'.json'),canonical(result))
    return 0 if result['state'] in ('RUNNING','VERIFIED') else 1

def timer_tick(owner, *, scheduled_at, trigger):
    """Called sequentially by the existing timer; never creates a worker."""
    global _last_poll
    if _stop.is_set() or not _lock.acquire(blocking=False):
        return dict(state='SOURCE_ADMISSION_STOPPED_OR_LOCAL_OWNER_ACTIVE')
    handle = None
    try:
        now = time.monotonic()
        if now - _last_poll < MIN_POLL_SECONDS:
            return dict(state='SOURCE_BOUNDED_POLL_INTERVAL')
        _last_poll = now
        ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = ROOT / 'owner.lock'
        require(not path.is_symlink(), 'SOURCE_LOCAL_LOCK_SYMLINK')
        handle = path.open('a+b')
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return dict(state='SOURCE_LOCAL_OWNER_ACTIVE')
        return run_once(owner, scheduled_at=scheduled_at, trigger=trigger)
    except Exception as error:
        LOG.warning('EQ20 full-population source dependency: %s', type(error).__name__)
        return dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY', reason=str(error)[:180])
    finally:
        if handle is not None:
            handle.close()
        _lock.release()


def request_stop():
    _stop.set()


def join_shutdown(timeout=0):
    # Work executes synchronously in the already joined mission timer.
    return not _lock.locked()


def _helper(token):
    require(re.fullmatch(r'[0-9a-f]{32}', token), 'SOURCE_HELPER_TOKEN')
    resource.setrlimit(resource.RLIMIT_CPU, (1, 1))
    resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_FSIZE, (MAX_REPLY, MAX_REPLY))
    path = ROOT / ('rpc_' + token + '.json')
    require(not path.is_symlink() and path.stat().st_size <= MAX_REQUEST, 'SOURCE_HELPER_INPUT_BOUND')
    request = json.loads(path.read_bytes())
    try:
        result = dict(success=True, response=_direct(request['op'], request['owner'], request['args']))
    except Exception as error:
        result = dict(success=False, error=str(error)[:180])
    raw = canonical(result)
    require(len(raw) <= MAX_REPLY, 'SOURCE_HELPER_OUTPUT_BOUND')
    target = ROOT / ('reply_' + token + '.json')
    with target.open('xb') as handle:
        handle.write(raw); handle.flush(); os.fsync(handle.fileno())


if __name__ == '__main__':
    if len(sys.argv)==3 and sys.argv[1]=='--rpc-helper':
        _helper(sys.argv[2])
    elif len(sys.argv)==4 and sys.argv[1]=='--source-child':
        raise SystemExit(_source_child(sys.argv[2],sys.argv[3]))
    else:
        raise SystemExit('SOURCE_CLI_REJECTED')
