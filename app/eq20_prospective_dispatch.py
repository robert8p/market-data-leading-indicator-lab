"""Bounded consumer of separately certified prospective evidence capsules.

The source/feature producer and frozen execution-fill producer are independent
prerequisites, not implemented by this consumer. No market network endpoint is
called here. The existing mission controller invokes timer_tick; no thread or
heavy competing schedule is created. All actual work needs a private immutable
job, acknowledged pre-session activation, current ownership and a finite
allocation already debited to its authorized parent resource ledger.
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

VERSION = 'EQ20_PROSPECTIVE_BATCH_CONSUMER_V2'
RPC_NAME = 'eq20_prospective_consumer_v2'
KERNEL_SHA256 = '99077b64970ff02e6a6fcc9dc9b9da4f9411eef08bc396c51cf220f35f481987'
INFERENCE_SHA256 = 'd92f62331f535e7391cf23627ecd23cbca7209c7fbc903c095256ee8849025a3'
METHOD_CONTRACT_SHA256 = '9143ac194c39051d5224890e19e9b3b33dc5264e3085d2d4bff4126a7aea6d21'
ROOT = Path('/tmp/astra-eq20-w10/prospective_consumer')
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

FINAL_RECEIPTS = {
 'independent_mathematical_review_sha256': ('INDEPENDENT_STATISTICAL_REVIEW', 'VERIFIED_CONDITIONAL_MATHEMATICS_NOT_MARKET_ASSUMPTIONS'),
 'conditional_dependence_assumption_review_sha256': ('SUCCESSOR_FINAL_DEPENDENCE_REVIEW', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'exposure_eligibility_ledger_sha256': ('SUCCESSOR_FINAL_EXPOSURE_RECONCILIATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'point_in_time_full_population_reconciliation_sha256': ('SUCCESSOR_FINAL_POPULATION_RECONCILIATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'complete_natural_stream_first_signal_reconciliation_sha256': ('SUCCESSOR_FINAL_FIRST_SIGNAL_RECONCILIATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'causal_input_publication_readiness_sha256': ('SUCCESSOR_FINAL_CAUSAL_INPUT_RECONCILIATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'frozen_execution_policy_sha256': ('SUCCESSOR_FINAL_EXECUTION_POLICY_RECONCILIATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'execution_evidence_reconciliation_sha256': ('SUCCESSOR_FINAL_EXECUTION_EVIDENCE_RECONCILIATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'finite_resource_reservation_sha256': ('SUCCESSOR_FINAL_RESOURCE_RECONCILIATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'immutable_alpha_allocation_sha256': ('SUCCESSOR_FINAL_ALPHA_RECONCILIATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
 'original_historical_acceptance_adjudication_sha256': ('SUCCESSOR_FINAL_HISTORICAL_ADJUDICATION', 'VERIFIED_FOR_REGISTERED_FIXED_HORIZON'),
}


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


def atomic(path, raw, immutable=True):
    require(len(raw) <= MAX_JSON and not path.is_symlink(), 'CONSUMER_SCRATCH_BOUND')
    if path.exists() and immutable:
        require(path.read_bytes() == raw, 'CONSUMER_IMMUTABLE_FILE_CONFLICT'); return
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


def verify_job(job):
    """Validate authenticated metadata before requesting any protected capsule."""
    require(isinstance(job, dict) and job.get('action') in
            ('PROCESS_SECURITY_BATCH', 'AGGREGATE_DATE', 'EVALUATE_CLAIM'), 'REGISTERED_CONSUMER_ACTION')
    require(job.get('reserved_governed_seconds') == 30 and job.get('resource_predebit_verified') is True,
            'ACTUAL_FINITE_PREDEBIT_RESERVATION_REQUIRED')
    require(job.get('activation_commit_acknowledged') is True, 'COMMITTED_PREOPEN_ACTIVATION_REQUIRED')
    kernel = load_local('eq20_prospective_kernel', KERNEL_SHA256)
    load_local('eq20_cluster_inference', INFERENCE_SHA256)
    work = resolve(job.get('work_artifact'), job.get('work_reference'),
                   'SUCCESSOR_PROSPECTIVE_WORK_ITEM', 'READY_CERTIFIED_INPUTS')
    require(work.get('version') == 'EQ20_PROSPECTIVE_WORK_ITEM_V2'
            and work.get('action') == job['action']
            and work.get('activation_key') == job.get('activation_key'), 'WORK_ITEM_SCOPE')
    contract = resolve(job.get('contract_artifact'), work.get('contract_reference'),
                       'SUCCESSOR_PROSPECTIVE_SESSION_CONTRACT', 'REGISTERED_OUTCOME_BLIND')
    kernel.family(contract)
    act = resolve(job.get('activation_artifact'), job.get('activation_reference'),
                  'SUCCESSOR_EVIDENCE_ACTIVATION', 'AWAITING_ELIGIBLE_EVIDENCE')
    plan = act.get('frozen_plan', {})
    require(act.get('activation_key') == job['activation_key']
            and plan.get('method_contract_sha256') == METHOD_CONTRACT_SHA256
            and all(plan.get(key) == contract[key] for key in ('candidate_family_sha256',
                'execution_policy_sha256', 'official_calendar_sha256', 'population_manifest_sha256',
                'official_session_dates_sha256'))
            and plan.get('official_session_dates') == contract['official_session_dates'], 'ACTIVATION_FROZEN_SCOPE')
    calendar = resolve(act.get('resolved_bindings', {}).get('calendar'), plan.get('bindings', {}).get('calendar'),
                       'SUCCESSOR_OFFICIAL_CALENDAR_CERTIFICATE', 'VERIFIED_OFFICIAL_CALENDAR')
    require(calendar.get('sessions') == contract.get('official_sessions')
            and calendar.get('official_calendar_sha256') == contract.get('official_calendar_sha256'),
            'ACTUAL_OFFICIAL_CALENDAR_ROWS_MUST_MATCH_CONTRACT')
    first_open = kernel.stamp(act.get('first_official_session_open_at'))
    require(kernel.stamp(job['contract_artifact'].get('created_at')) < first_open
            and kernel.stamp(act.get('registered_at')) < first_open
            and kernel.stamp(job.get('commit_acknowledgement_at')) < first_open,
            'PREREGISTRATION_PRECEDES_RESERVED_EVIDENCE')
    collector_row = act.get('resolved_bindings', {}).get('collector')
    collector_ref = plan.get('bindings', {}).get('collector')
    collector = resolve(collector_row, collector_ref, 'SUCCESSOR_COLLECTOR_RELEASE', 'VERIFIED_EXECUTABLE_RELEASE')
    require(collector.get('prospective_session_contract_sha256') == kernel.digest(contract)
            and collector.get('consumer_module_sha256') == sha(Path(__file__).read_bytes())
            and collector.get('consumer_kernel_sha256') == KERNEL_SHA256,
            'COLLECTOR_RELEASE_CONTRACT_AND_CONSUMER_PINS')
    require(plan.get('bindings', {}).get('population', {}).get('implementation_sha256')
            == contract['population_policy_artifact_sha256'], 'ORIGINAL_POPULATION_POLICY_RECEIPT_PIN')
    # These are real source/execution producer release receipts, not metadata
    # booleans claiming that this label-consuming kernel itself produced fills.
    producer_docs = {}
    for name, kind in (('source_producer', 'SUCCESSOR_PROSPECTIVE_SOURCE_PRODUCER_RELEASE'),
                       ('execution_producer', 'SUCCESSOR_PROSPECTIVE_EXECUTION_PRODUCER_RELEASE')):
        artifact = job.get(name+'_artifact')
        producer = resolve(artifact, collector.get(name+'_reference'), kind, 'VERIFIED_EXECUTABLE_RELEASE')
        require(producer.get('session_contract_sha256') == kernel.digest(contract)
                and producer.get('scope_sha256') == plan.get('scope_sha256')
                and producer.get('real_implementation_verified') is True
                and producer.get('additional_paid_cost') == 0, 'REAL_ENTITLED_PRODUCER_IMPLEMENTATION_REQUIRED')
        require(kernel.stamp(artifact.get('created_at')) < first_open, 'PRODUCER_RELEASE_AFTER_RESERVED_START')
        producer_docs[name] = producer
    batch_pin = collector.get('batch_module_sha256')
    require(isinstance(batch_pin, str) and re.fullmatch('[0-9a-f]{64}', batch_pin) is not None, 'BATCH_IMPLEMENTATION_RELEASE_PIN')
    batch = load_local('eq20_prospective_batch', batch_pin)
    for name, module_name in (('source_producer','eq20_prospective_features'), ('execution_producer','eq20_execution_replay')):
        pin = producer_docs[name].get('module_sha256')
        require(isinstance(pin, str) and re.fullmatch('[0-9a-f]{64}', pin) is not None, 'REAL_PRODUCER_PUBLIC_MODULE_PIN')
        load_local(module_name, pin)
        job['_source_module_sha256' if name == 'source_producer' else '_execution_module_sha256'] = pin
    bindings = job.get('source_producer_bindings')
    require(isinstance(bindings, dict) and bindings.get('reference') == collector['source_producer_reference']
            and bindings.get('artifact') == job['source_producer_artifact']
            and isinstance(bindings.get('source_files'), dict), 'REAL_PRIVATE_SOURCE_CODE_BINDINGS_REQUIRED')
    files = producer_docs['source_producer'].get('code_files')
    require(isinstance(files, list) and 0 < len(files) <= 8
            and set(bindings['source_files']) == {q.get('name') for q in files}, 'EXACT_PRIVATE_SOURCE_FILES_REQUIRED')
    for descriptor in files:
        text = bindings['source_files'].get(descriptor['name'])
        require(isinstance(text, str) and len(text.encode()) <= 262144 and sha(text.encode()) == descriptor.get('sha256'),
                'PRIVATE_SOURCE_CODE_CONTENT_PIN')
    certificate = resolve(job.get('input_certificate_artifact'), work.get('input_certificate_reference'),
                          'SUCCESSOR_INPUT_CAPSULE_CERTIFICATE', 'VERIFIED_CAUSAL_COMPLETE_SCOPE')
    require(certificate.get('activation_key') == job['activation_key']
            and certificate.get('action') == job['action']
            and certificate.get('work_artifact_key') == job['work_reference']['artifact_key']
            and all(certificate.get(key) == value for key, value in kernel.scope(contract).items()),
            'INPUT_CERTIFICATE_FROZEN_SCOPE')
    if job['action'] in ('PROCESS_SECURITY_BATCH', 'AGGREGATE_DATE'):
        require(certificate.get('session_date') == work.get('session_date')
                and work.get('session_date') in contract['official_session_dates'], 'REGISTERED_WORK_SESSION_IDENTITY')
    source_module = load_local('eq20_prospective_features', job['_source_module_sha256'])
    source_module.resolve_release(job['source_producer_bindings'], contract)
    # Code is authenticated and initialized from its preregistered release before
    # requesting protected raw capsules; source values are not opened here.
    source_module.load_producer(contract, job['source_producer_bindings'])
    batch.verify_plan(sys.modules.get(__name__) or _self_module(), job, contract, work, collector, certificate)
    require(certificate.get('outcome_based_input_filtering') is False
            and certificate.get('complete_scheduled_input_slots_retained') is True
            and certificate.get('source_and_execution_producer_review_complete') is True,
            'INPUT_CAUSAL_COMPLETENESS_REVIEW_REQUIRED')
    return contract, work, act


def verified_final_gates(job, contract, act):
    """Resolve eleven final receipts; activation does not stand in for any QC."""
    kernel = load_local('eq20_prospective_kernel', KERNEL_SHA256)
    work = json.loads(job['work_artifact']['evidence_text'])
    refs, rows = work.get('final_receipt_references'), job.get('final_receipt_artifacts')
    require(isinstance(refs, dict) and isinstance(rows, dict)
            and set(refs) == set(rows) == set(FINAL_RECEIPTS), 'EXACT_ELEVEN_FINAL_REVIEW_RECEIPTS')
    facts = {}
    for name, (kind, status) in FINAL_RECEIPTS.items():
        fact = resolve(rows[name], refs[name], kind, status)
        if name == 'independent_mathematical_review_sha256':
            require(fact.get('reviewed_source_sha256') == INFERENCE_SHA256
                    and fact.get('contract_sha256_excluding_selfhash') == METHOD_CONTRACT_SHA256,
                    'FINAL_MATHEMATICAL_REVIEW_PIN')
        else:
            require(fact.get('activation_key') == job['activation_key']
                    and all(fact.get(key) == value for key, value in kernel.scope(contract).items())
                    and fact.get('official_session_dates') == contract['official_session_dates']
                    and fact.get('full_day_receipt_manifest_sha256') == work.get('full_day_receipt_manifest_sha256')
                    and fact.get('all_twelve_claim_slots_reviewed') is True,
                    'FINAL_REVIEW_COMPLETE_COHORT_AND_FAMILY_SCOPE')
        facts[name] = fact
    dependence = facts['conditional_dependence_assumption_review_sha256']
    exposure = facts['exposure_eligibility_ledger_sha256']
    resource_fact = facts['finite_resource_reservation_sha256']
    alpha = facts['immutable_alpha_allocation_sha256']
    require(dependence.get('conditional_date_independence_supported') is True
            and dependence.get('nonrejection_only_justification') is False
            and isinstance(dependence.get('independent_reviewer_id'), str) and dependence['independent_reviewer_id'],
            'ACTUAL_FINAL_CONDITIONAL_DEPENDENCE_REVIEW_REQUIRED')
    require(exposure.get('evidence_class') == 'PROSPECTIVE_VALIDATION'
            and exposure.get('access_history_complete') is True
            and exposure.get('unknown_lineage_access') is False
            and exposure.get('selection_after_protected_access') is False, 'ACTUAL_FINAL_EXPOSURE_ELIGIBILITY_REQUIRED')
    require(resource_fact.get('no_live_trades_or_new_paid_cost') is True
            and resource_fact.get('allocation_key') == job.get('allocation_key')
            and resource_fact.get('actual_parent_debits_reconciled') is True,
            'FINAL_RESOURCE_AND_COST_PROVENANCE')
    require(alpha.get('epoch') == act['epoch'] and alpha.get('alpha_fraction') == act['alpha_fraction']
            and alpha.get('activation_artifact_sha256') == job['activation_reference']['implementation_sha256']
            and alpha.get('unused_alpha_recycled') is False, 'FINAL_IMMUTABLE_ALPHA_RECONCILIATION')
    population = facts['point_in_time_full_population_reconciliation_sha256']
    require(re.fullmatch(r'[0-9a-f]{64}', population.get('dated_population_certificate_vector_sha256', '')),
            'FINAL_DATED_POPULATION_CERTIFICATE_VECTOR_REQUIRED')
    gates = {name: ref['implementation_sha256'] for name, ref in refs.items()}
    gates.update(all_receipts_resolved_and_verified=True, evidence_class='PROSPECTIVE_VALIDATION',
        candidate_family_sha256=contract['candidate_family_sha256'],
        point_in_time_population_sha256=contract['population_manifest_sha256'],
        official_calendar_sha256=contract['official_calendar_sha256'],
        dated_population_certificate_vector_sha256=population['dated_population_certificate_vector_sha256'],
        conditional_date_independence_supported=True, outcome_based_stopping_or_extension=False,
        no_live_trades_or_new_paid_cost=True, earliest_reserved_evidence_at=act['first_official_session_open_at'])
    return gates


def raw_chunks(compressed, meta):
    """Stream bounded decompression with final length/hash/trailing-data checks."""
    require(len(compressed) == meta['compressed_bytes'] and sha(compressed) == meta['compressed_sha256'],
            'COMPRESSED_CAPSULE_HASH_OR_LENGTH')
    decoder = zlib.decompressobj(); total = 0; checksum = hashlib.sha256()
    for start in range(0, len(compressed), 32768):
        pending = compressed[start:start+32768]
        while pending:
            out = decoder.decompress(pending, 65536)
            pending = decoder.unconsumed_tail
            total += len(out)
            require(total <= meta['raw_bytes'] <= MAX_STREAM, 'DECOMPRESSION_BOUND')
            checksum.update(out)
            if out: yield out
    end = decoder.flush(65536); total += len(end); checksum.update(end)
    require(total <= meta['raw_bytes'], 'DECOMPRESSION_BOUND')
    if end: yield end
    require(decoder.eof and not decoder.unused_data and total == meta['raw_bytes']
            and checksum.hexdigest() == meta['raw_sha256'], 'RAW_CAPSULE_HASH_LENGTH_OR_TRAILING_DATA')


def json_lines(chunks):
    pending = bytearray()
    for chunk in chunks:
        pending.extend(chunk)
        while b'\n' in pending:
            line, _, rest = pending.partition(b'\n'); pending = bytearray(rest)
            require(0 < len(line) <= MAX_LINE, 'JSONL_LINE_BOUND')
            yield json.loads(line)
        require(len(pending) <= MAX_LINE, 'JSONL_LINE_BOUND')
    if pending:
        yield json.loads(pending)


def run_job(job, fetch_compressed):
    """Concrete deterministic dispatch, called inside a reserved isolated child."""
    contract, work, act = verify_job(job)
    kernel = load_local('eq20_prospective_kernel', KERNEL_SHA256)
    final_gates = verified_final_gates(job, contract, act) if job['action'] == 'EVALUATE_CLAIM' else None
    if job['action'] == 'PROCESS_SECURITY_BATCH':
        return load_local('eq20_prospective_batch').run_batch(_self_module(), job, contract, work)
    if job['action'] == 'AGGREGATE_DATE':
        return load_local('eq20_prospective_batch').run_aggregate(_self_module(), job, contract, work)
    batch = load_local('eq20_prospective_batch')
    try:
        days = list(batch.committed_records(_self_module(), job, work))
        batch.deadline(job, .5)
    except batch.PreparationYield:
        return {'version': VERSION, 'state': 'RUNNING', 'action': job['action'], 'activation_key': job['activation_key'],
            'work_artifact_key': job['work_reference']['artifact_key'], 'work_artifact_sha256': job['work_reference']['implementation_sha256'],
            'protected_outcomes_accessed': True, 'research_objective_achieved': False}
    require(job.get('committed_day_receipts') == {d['session_date']: d['receipt_sha256'] for d in days},
            'INPUT_DAY_RECEIPTS_NOT_COMMITTED_IN_REGISTRY')
    require(kernel.digest([d['receipt_sha256'] for d in days]) == work.get('full_day_receipt_manifest_sha256'),
            'FINAL_DAY_RECEIPT_MANIFEST_PIN')
    registration = {**kernel.scope(contract), 'method': load_local('eq20_cluster_inference', INFERENCE_SHA256).METHOD,
        'registered_at': act['registered_at'], 'candidate_frozen_at': act['resolved_bindings']['candidate']['created_at'],
        'epoch': act['epoch'], 'alpha_fraction': act['alpha_fraction'], 'fixed_horizon_sessions': 252,
        'point_in_time_population_sha256': contract['population_manifest_sha256'],
        'first_session_date': contract['official_session_dates'][0], 'last_session_date': contract['official_session_dates'][-1],
        'official_session_dates': contract['official_session_dates']}
    result = kernel.evaluate_fixed_horizon_claim(contract, work['claim_slot'], days, registration, final_gates,
        load_local('eq20_cluster_inference', INFERENCE_SHA256))
    return {'version': VERSION, 'state': 'VERIFIED', 'verification_scope': 'REGISTERED_FIXED_HORIZON_CONSUMPTION_ONLY',
        'action': job['action'], 'activation_key': job['activation_key'], 'work_artifact_key': job['work_reference']['artifact_key'],
        'work_artifact_sha256': job['work_reference']['implementation_sha256'],
        'input_raw_sha256': work['committed_receipts_manifest']['manifest_sha256'], 'output': result,
        'protected_outcomes_accessed': True, 'research_objective_achieved': False}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise Closed('CONSUMER_RPC_REDIRECT_REJECTED')


class RPC:
    def direct(self, op, owner, args):
        require(op in ('status', 'claim', 'claim_reconcile', 'input', 'input_part', 'output_page', 'commit_outputs', 'terminal', 'terminal_replay'), 'CONSUMER_RPC_OPERATION')
        base = os.environ.get('SUPABASE_URL', '').rstrip('/')
        key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
        require(base == 'https://oxzabweahkoimtevbbny.supabase.co' and key, 'PRIVATE_CONFIGURATION_REQUIRED')
        args = dict(args, request_start_deadline_at=(datetime.now(timezone.utc)+timedelta(seconds=.5)).isoformat())
        raw = canonical({'p_op': op, 'p_owner': owner, 'p_args': args})
        require(len(raw) <= MAX_REQUEST, 'CONSUMER_RPC_REQUEST_BOUND')
        request = urllib.request.Request(base+'/rest/v1/rpc/'+RPC_NAME, data=raw,
            headers={'Authorization': 'Bearer '+key, 'apikey': key, 'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.build_opener(NoRedirect()).open(request, timeout=1.7) as reply:
                result = reply.read(MAX_REPLY+1)
        except urllib.error.HTTPError as error:
            raise Closed('CONSUMER_RPC_HTTP_'+str(error.code)) from None
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            raise Closed('CONSUMER_RPC_TRANSPORT_FAILURE') from None
        require(len(result) <= MAX_REPLY, 'CONSUMER_RPC_REPLY_BOUND')
        value = json.loads(result); require(isinstance(value, dict), 'CONSUMER_RPC_REPLY_OBJECT')
        return value

    def call(self, op, owner, args):
        return bounded_rpc(op, owner, args)


def bounded_rpc(op, owner, args):
    """Six-second control envelope includes exact wait4 and unknown SQL tail."""
    key = args['invocation_id'] if op == 'claim' else uuid.uuid4().hex
    require(re.fullmatch(r'[0-9a-f]{32}', key) is not None, 'CONSUMER_RPC_INVOCATION_ID')
    request = ROOT/('rpc_'+key+'.json'); reply = ROOT/('rpc_reply_'+key+'.json')
    atomic(request, canonical({'op': op, 'owner': owner, 'args': args}))
    child = guards().ReapedChild([sys.executable, '-I', '-S', str(Path(__file__).resolve()), '--rpc-helper', key])
    began = time.monotonic(); cpu = time.process_time(); success = False
    try:
        while not child.reap():
            if time.monotonic()-began > 2.1 or time.process_time()-cpu > 1:
                child.stop(); break
            time.sleep(.02)
        require(child.termination_proof is not None, 'CONSUMER_HTTP_HELPER_NOT_REAPED')
        value = json.loads(reply.read_bytes()) if reply.exists() else {}
        require(child.exit_code == 0 and value.get('success') is True, value.get('error', 'CONSUMER_HTTP_HELPER_FAILURE'))
        success = True; return value['response']
    finally:
        child.stop()
        if op == 'claim':
            # Retain the exact control request and actual helper termination,
            # including failed/lost responses. A later timer may reconcile only
            # this invocation, before any research launch marker exists.
            proof = {'version': 'EQ20_CONSUMER_CLAIM_HELPER_WAIT4_V1',
                'request_sha256': sha(request.read_bytes()), 'invocation_id': key,
                'host_instance': socket.gethostname(), 'process_identity': child.original_identity,
                'process_finished': child.termination_proof is not None,
                'process_termination_proof': child.termination_proof, 'exit_code': child.exit_code,
                'child_cpu_seconds': child.cpu, 'child_wall_seconds': time.monotonic()-began,
                'parent_cpu_seconds': time.process_time()-cpu,
                'finished_at': datetime.now(timezone.utc).isoformat(),
                'finished_monotonic': time.monotonic(), 'sql_tail_bound_seconds': 2,
                'response_readback_verified': success, 'research_child_launched': False}
            atomic(ROOT/('claim_helper_'+key+'.json'), canonical(proof))
        if not success:
            time.sleep(2)
        if op != 'claim':
            request.unlink(missing_ok=True); reply.unlink(missing_ok=True)


def cleanup_claim(invocation):
    if not isinstance(invocation, str) or re.fullmatch(r'[0-9a-f]{32}', invocation) is None:
        return
    for prefix in ('claim_', 'claim_helper_', 'claim_launch_', 'claim_recovery_count_', 'rpc_', 'rpc_reply_'):
        (ROOT/(prefix+invocation+'.json')).unlink(missing_ok=True)


def finish_local_settlement(attempt, invocation, owner=None, result=None):
    """A retained server ACK makes interrupted cleanup local and cost free."""
    require(re.fullmatch(r'[0-9a-f-]{36}', attempt or '') is not None, 'CONSUMER_SETTLEMENT_ATTEMPT_ID')
    marker = ROOT/('settled_'+attempt+'.json')
    if result is not None:
        require(result.get('committed') is True and result.get('attempt_id') == attempt,
                'CONSUMER_EXACT_SETTLEMENT_ACK_REQUIRED')
        atomic(marker, canonical({'version': 'EQ20_CONSUMER_SETTLEMENT_ACK_V1', 'attempt_id': attempt,
            'claim_invocation_id': invocation, 'owner': owner, 'committed': True,
            'server_response_sha256': sha(canonical(result)), 'server_state': result.get('state')}))
    else:
        ack = json.loads(marker.read_bytes())
        require(ack.get('version') == 'EQ20_CONSUMER_SETTLEMENT_ACK_V1' and ack.get('committed') is True
                and ack.get('attempt_id') == attempt and ack.get('claim_invocation_id') == invocation
                and re.fullmatch(r'[0-9a-f]{64}', ack.get('server_response_sha256', '')) is not None,
                'CONSUMER_RETAINED_SETTLEMENT_ACK_REQUIRED')
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
    require(request.get('op') == 'claim' and isinstance(request.get('args'), dict), 'CONSUMER_CLAIM_ENVELOPE_REQUIRED')
    invocation = request['args'].get('invocation_id')
    require(re.fullmatch(r'[0-9a-f]{32}', invocation or '') is not None
            and request_path.name == 'claim_'+invocation+'.json', 'CONSUMER_CLAIM_ENVELOPE_IDENTITY')
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
    require(proof.get('version') == 'EQ20_CONSUMER_CLAIM_HELPER_WAIT4_V1'
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
    require(job.get('reserved_governed_seconds') == 30, 'CONSUMER_RESERVATION_REQUIRED')
    attempt = job.get('attempt_id')
    require(isinstance(attempt, str) and re.fullmatch(r'[0-9a-f-]{36}', attempt), 'ATTEMPT_ID_REQUIRED')
    job_path = ROOT/('job_'+attempt+'.json'); receipt_path = ROOT/('receipt_'+attempt+'.json')
    require(not job_path.exists() and not receipt_path.exists(), 'CONSUMER_ATTEMPT_REEXECUTION_FORBIDDEN')
    atomic(job_path, canonical(job))
    child = guards().ReapedChild([sys.executable, '-I', '-S', str(Path(__file__).resolve()), '--job', owner, attempt])
    began = time.monotonic()
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
    if child.exit_code != 0: time.sleep(2)
    receipt_raw = receipt_path.read_bytes() if receipt_path.exists() and receipt_path.stat().st_size <= MAX_JSON else None
    try:
        receipt = json.loads(receipt_raw) if receipt_raw is not None else None
        require(isinstance(receipt, dict) and len(canonical(receipt)) <= 52*1024, 'BOUNDED_TERMINAL_RECEIPT_REQUIRED')
    except (ValueError, TypeError):
        receipt = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                   'error': 'BOUNDED_CONSUMER_INVALID_RECEIPT' if receipt_path.exists() else 'BOUNDED_CONSUMER_NO_RECEIPT',
                   'protected_outcomes_accessed': 'UNKNOWN_READ_MAY_HAVE_OCCURRED',
                   'child_receipt_sha256': sha(receipt_raw) if receipt_raw is not None else None}
    parent_cpu = time.process_time()-cpu
    physical = child.termination_proof == 'SPECIFIC_CHILD_WAIT4' and guards().control_identity_equal(child.original_identity, child.original_identity)
    measured = all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in (child.cpu, child_wall, parent_cpu))
    compliant = physical and measured and child.cpu <= 6 and child_wall <= 7 and parent_cpu <= .5
    if not compliant:
        # Physical quiescence and resource compliance are different facts.
        # Preserve exact wait4 even when measured scheduling/CPU exceeds a
        # prescribed guard. Such work cannot be verified or automatically retried.
        receipt = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                   'error': 'CONSUMER_MEASURED_ENVELOPE_EXCEEDED' if physical and measured else 'CONSUMER_CHILD_TERMINATION_OR_MEASUREMENT_UNKNOWN',
                   'protected_outcomes_accessed': receipt.get('protected_outcomes_accessed', 'UNKNOWN_READ_MAY_HAVE_OCCURRED'),
                   'child_receipt_sha256': sha(receipt_raw) if receipt_raw is not None else None,
                   'research_objective_achieved': False}
    receipt.update(process_finished=physical, process_termination_proof=child.termination_proof,
        process_identity=child.original_identity, host_instance=socket.gethostname(), exit_code=child.exit_code,
        child_cpu_seconds=child.cpu, child_wall_seconds=child_wall,
        parent_cpu_seconds=parent_cpu, accounting_mode='FULL_RESERVATION_CONSERVATIVE',
        resource_envelope_verified=compliant,
        governed_envelope={'control_and_terminal': 12, 'child_wall_bound': 7, 'child_cpu_bound': 6,
                          'sql_tail_bound': 2, 'parent_cpu_bound': .5, 'total_bound': 27.5, 'reserved': 30})
    require(len(canonical(receipt)) <= 60*1024, 'BOUNDED_TERMINAL_RECEIPT_REQUIRED')
    args = {'attempt_id': attempt, 'claim_invocation_id': job.get('claim_invocation_id'), 'receipt': receipt}
    atomic(ROOT/('terminal_'+attempt+'.json'), canonical({'owner': owner, 'args': args}))
    require(physical and measured, 'CONSUMER_CHILD_TERMINATION_OR_MEASUREMENT_UNKNOWN')
    result = rpc.call('terminal', owner, args)
    if result.get('committed') is True:
        finish_local_settlement(attempt, job.get('claim_invocation_id'), owner, result)
    return result


def supervise_once(rpc, owner, *, trigger, scheduled_at):
    parent_cpu_start = time.process_time()
    args = {'version': VERSION, 'module_sha256': sha(Path(__file__).read_bytes()), 'kernel_sha256': KERNEL_SHA256,
            'inference_sha256': INFERENCE_SHA256, 'batch_module_sha256': sha(Path(__file__).with_name('eq20_prospective_batch.py').read_bytes()), 'host_instance': socket.gethostname(),
            'trigger': trigger, 'scheduled_at': scheduled_at, 'invocation_id': uuid.uuid4().hex}
    settled = sorted(ROOT.glob('settled_*.json')) if ROOT.exists() else []
    if settled:
        require(len(settled) == 1, 'CONSUMER_MULTIPLE_PENDING_SETTLEMENT_CLEANUPS')
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
        require(len(pending) == 1, 'CONSUMER_MULTIPLE_PENDING_TERMINALS')
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
        require(len(pending_claims) == 1, 'CONSUMER_MULTIPLE_PENDING_CLAIMS')
        return reconcile_claim(rpc, pending_claims[0], args)
    # Read-only when current true prerequisites do not permit a consumer job.
    atomic(ROOT/('claim_'+args['invocation_id']+'.json'), canonical({'op': 'claim', 'owner': owner, 'args': args}))
    job = rpc.call('claim', owner, args)
    if job.get('action') not in ('PROCESS_SECURITY_BATCH', 'AGGREGATE_DATE', 'EVALUATE_CLAIM'):
        cleanup_claim(args['invocation_id'])
        return job
    require(job.get('claim_invocation_id') in (None, args['invocation_id']), 'CONSUMER_CLAIM_RESPONSE_INVOCATION')
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
    try:
        value = {'success': True, 'response': RPC().direct(request['op'], request['owner'], request['args'])}
    except Exception as error:
        value = {'success': False, 'error': str(error) if re.fullmatch(r'[A-Z0-9_]{1,200}', str(error)) else type(error).__name__}
    with (ROOT/('rpc_reply_'+key+'.json')).open('xb') as out:
        out.write(canonical(value)); out.flush(); os.fsync(out.fileno())
    return 0 if value['success'] else 1


def _job(owner, attempt):
    require(re.fullmatch(r'[0-9a-f-]{36}', attempt), 'JOB_ATTEMPT_KEY')
    signal.signal(signal.SIGALRM, signal.SIG_DFL); signal.setitimer(signal.ITIMER_REAL, 6.8)
    signal.signal(signal.SIGPROF, signal.SIG_DFL); signal.setitimer(signal.ITIMER_PROF, 5.8)
    resource.setrlimit(resource.RLIMIT_AS, (192*1024*1024, 192*1024*1024))
    resource.setrlimit(resource.RLIMIT_FSIZE, (256*1024*1024, 256*1024*1024))
    guards().prohibit_descendants()
    job = json.loads((ROOT/('job_'+attempt+'.json')).read_bytes())
    job['_deadline'] = time.monotonic()+6.3
    job['owner'] = owner
    access_attempted = False
    def fetch(meta):
        nonlocal access_attempted
        access_attempted = True
        value = RPC().direct('input', owner, {'attempt_id': attempt, 'compressed_sha256': meta['compressed_sha256']})
        require(isinstance(value.get('payload_base64'), str), 'CAPSULE_READBACK_REQUIRED')
        return base64.b64decode(value['payload_base64'], validate=True)
    try:
        receipt = run_job(job, fetch)
    except Exception as error:
        receipt = {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                   'error': str(error) if re.fullmatch(r'[A-Z0-9_]{1,200}', str(error)) else type(error).__name__,
                   'protected_outcomes_accessed': 'UNKNOWN_READ_MAY_HAVE_OCCURRED' if access_attempted or job.get('_protected_access_attempted') else False,
                   'research_objective_achieved': False}
    atomic(ROOT/('receipt_'+attempt+'.json'), canonical(receipt))
    return 0 if receipt['state'] in ('VERIFIED', 'RUNNING') else 1


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--rpc-helper':
        raise SystemExit(_rpc_helper(sys.argv[2]))
    if len(sys.argv) == 4 and sys.argv[1] == '--job':
        raise SystemExit(_job(sys.argv[2], sys.argv[3]))
    raise SystemExit('Only an admitted bounded prospective consumer job is executable')
