"""Prospective consumer on the registered, shared V3 accounting lifecycle.

V2's scientific dispatcher remains byte-for-byte preserved.  This composite
entry point separately authenticates that dispatcher, the new transport batch,
and the actual provider lifecycle whose physical wait4/finite recovery logic it
reuses in an isolated namespace.  It opens no network session without an actual
acknowledged activation or a retained, unresolved local cycle.
"""
from __future__ import annotations

import importlib.util
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import signal
import socket
import sys
import time

VERSION = 'EQ20_PROSPECTIVE_BATCH_CONSUMER_V3'
ROOT = Path('/tmp/astra-eq20-w10/prospective_consumer_v3')
ENTRYPOINT = Path(__file__).resolve()
FROZEN_SCIENTIFIC_CORE_SHA256 = '36c39c54dc740ffe334834fa8843c1de7b6496af9be18b1340e078a748537315'
LIFECYCLE_CORE_SHA256 = 'd764ef18185c95d95846381326198604ff0a574b28c5ab5e9aa6d792160e2317'
CAPTURE_CORE_SHA256 = '1a1899f26b21c455646378cfe89bbe84df838c71245cc4c8825fb85b2bf8538b'
ACCOUNTING_SHA256 = '92d80a6fa5b23ad49775c1e3b6a93b824059493709cdaffd556b6a67cdce8b6f'
PERMIT_SHA256 = '72fda15472338d0b49afc9ab342bfec4b91de743d76d2fd0c0b0293b1d91fdff'
_modules = {}
_runtime = None
_science = None
_transport = None


class Closed(ValueError):
    pass


def require(value, reason):
    if not value:
        raise Closed(reason)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def load_local(name, expected=None):
    actual_name = 'eq20_prospective_batch_v3' if name == 'eq20_prospective_batch' else name
    path = ENTRYPOINT.with_name(actual_name+'.py')
    require(path.is_file() and (expected is None or sha(path.read_bytes()) == expected),
            'CONSUMER_V3_INSTALLED_COMPONENT_PIN_MISMATCH')
    if actual_name not in _modules:
        spec = importlib.util.spec_from_file_location('consumer_v3_'+actual_name, path)
        obj = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = obj
        spec.loader.exec_module(obj)
        _modules[actual_name] = obj
    return _modules[actual_name]


def module_pins():
    return {
        'module_sha256': sha(ENTRYPOINT.read_bytes()),
        'consumer_scientific_core_sha256': FROZEN_SCIENTIFIC_CORE_SHA256,
        'consumer_lifecycle_core_sha256': LIFECYCLE_CORE_SHA256,
        'batch_module_sha256': sha(ENTRYPOINT.with_name('eq20_prospective_batch_v3.py').read_bytes()),
        'accounting_module_sha256': ACCOUNTING_SHA256,
        'rpc_admission_module_sha256': PERMIT_SHA256,
        'capture_core_sha256': CAPTURE_CORE_SHA256,
        'pipeline_module_sha256': sha(ENTRYPOINT.with_name('eq20_prospective_capture_pipeline.py').read_bytes()),
    }


class ScientificRPC:
    def direct(self, op, owner, args):
        require(_transport is not None, 'CONSUMER_PROTECTED_TRANSPORT_REQUIRES_RESERVED_REAPED_CHILD')
        return _transport(op, owner, args)


def scientific_core():
    global _science
    if _science is None:
        path = ENTRYPOINT.with_name('eq20_prospective_dispatch.py')
        require(sha(path.read_bytes()) == FROZEN_SCIENTIFIC_CORE_SHA256,
                'FROZEN_V2_SCIENTIFIC_DISPATCHER_CHANGED')
        spec = importlib.util.spec_from_file_location('consumer_v3_frozen_scientific_dispatcher', path)
        obj = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = obj
        spec.loader.exec_module(obj)
        # The original verifier checks the executing consumer's release hash.
        # Both that composite identity and the untouched underlying scientific
        # code hash are explicit, separately required registry fields.
        obj.__file__ = str(ENTRYPOINT)
        obj.VERSION = VERSION
        obj.ROOT = ROOT
        obj.load_local = load_local
        obj.RPC = ScientificRPC
        _science = obj
    return _science


def lifecycle():
    global _runtime
    if _runtime is None:
        path = ENTRYPOINT.with_name('eq20_prospective_capture_runtime.py')
        require(sha(path.read_bytes()) == LIFECYCLE_CORE_SHA256,
                'CONSUMER_REGISTERED_PHYSICAL_LIFECYCLE_CHANGED')
        load_local('eq20_prospective_accounting_v3', ACCOUNTING_SHA256)
        load_local('eq20_rpc_admission_v3', PERMIT_SHA256)
        spec = importlib.util.spec_from_file_location('consumer_v3_isolated_physical_lifecycle', path)
        obj = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = obj
        spec.loader.exec_module(obj)
        obj.VERSION = VERSION
        obj.RPC_NAME = 'eq20_prospective_consumer_v3'
        obj.ROOT = ROOT
        obj.ENTRYPOINT = ENTRYPOINT
        # Its shared metadata-pool stop path deliberately remains unchanged:
        # capture and consumer spend the same finite 320-probe entitlement.
        obj._ALLOWED = {'probe','status','funded_probe','claim','input','input_part',
            'output_page','commit_outputs','terminal','terminal_replay',
            'claim_reconcile','close_conservative'}
        obj.module_pins = module_pins
        obj.validate_job = validate_job
        obj.run_slice = run_slice
        obj.result_committed = result_committed
        original_complete = obj.complete_terminal
        def complete_terminal(*args, **kwargs):
            result = original_complete(*args, **kwargs)
            # A physical terminal ACK is not a research output commit.
            result['committed'] = (result.get('committed') is True
                and result.get('research_output_commit_verified') is True)
            return result
        obj.complete_terminal = complete_terminal
        _runtime = obj
    return _runtime


def validate_job(claim):
    runtime = lifecycle()
    require(isinstance(claim, dict) and claim.get('state') == 'RUNNING'
        and claim.get('normal_governed_seconds') == 30 and claim.get('total_held_seconds') == 54,
        'ACTUAL_FINITE_CONSUMER_V3_RESERVATION_REQUIRED')
    job = runtime.hydrate_job(claim)
    require(job.get('operation') == 'CONSUMER_BATCH', 'EXACT_CONSUMER_OPERATION_REQUIRED')
    for key, value in module_pins().items():
        require(job.get(key) == value, 'CONSUMER_REGISTERED_COMPOSITE_PIN_MISMATCH')
    require(job.get('host_instance') == claim.get('host_instance') == socket.gethostname()
        and job.get('host_boot_id') == runtime.boot_id()
        and all(job.get(key) == claim.get(key) for key in ('attempt_id','activation_key','work_key'))
        and job.get('owner') == claim.get('owner')
        and job.get('resource_reservation_verified') is True
        and job.get('actual_allocation_scope') == 'INITIAL_FULL_HORIZON_PREALLOCATION',
        'CONSUMER_EXACT_OWNED_FUNDED_NATIVE_JOB_REQUIRED')
    wall = job.get('maximum_wall_seconds')
    require(type(wall) in (int,float) and 0 < wall <= 7
        and job.get('maximum_governed_seconds') == 30,
        'PRESERVED_CONSUMER_SIX_CPU_SEVEN_WALL_BOUND')
    proof = job.get('request_timeout_verification', {})
    require(proof.get('verified_actual_http_request') is True
        and proof.get('query_timeout_seconds') == 2 and proof.get('request_start_deadline_ms') == 500
        and proof.get('post_helper_sql_tail_seconds') == 3
        and proof.get('per_request_server_permit_required') is True
        and proof.get('client_clock_error_not_used_for_admission') is True
        and proof.get('clock_observation_is_historical_only') is True
        and proof.get('host_instance') == socket.gethostname()
        and proof.get('host_boot_id') == runtime.boot_id(),
        'ACTUAL_CURRENT_HOST_QUERY_TIMEOUT_AND_PER_REQUEST_PERMIT_REQUIRED')
    runtime.exact_hash(proof.get('artifact_sha256'))
    # The server's actual V3 held reservation is the accounting authority.
    # These legacy-shape fields are accepted only as exact copies of that
    # authenticated binding; they do not manufacture a permanent V2 debit.
    require(job.get('reserved_governed_seconds') == 30
        and job.get('resource_predebit_verified') is True,
        'CONSUMER_NATIVE_COMPATIBILITY_BINDING_REQUIRED')
    scientific = scientific_core()
    contract, work, activation = scientific.verify_job(job)
    collector = json.loads(activation['resolved_bindings']['collector']['evidence_text'])
    require(collector.get('consumer_scientific_core_sha256') == FROZEN_SCIENTIFIC_CORE_SHA256
        and collector.get('consumer_lifecycle_core_sha256') == LIFECYCLE_CORE_SHA256
        and collector.get('accounting_module_sha256') == ACCOUNTING_SHA256
        and collector.get('rpc_admission_module_sha256') == PERMIT_SHA256,
        'ACTUAL_COMPOSITE_CONSUMER_RELEASE_REQUIRED')
    return job, contract, work, activation, scientific


def run_slice(claim, cycle, *, rpc=None):
    global _transport
    runtime = lifecycle()
    core = runtime.load('eq20_prospective_capture',CAPTURE_CORE_SHA256)
    budget = core.GovernedBudget(min(6., cycle['child_governed_seconds']),
                                min(6.7, cycle['child_wall_seconds']))
    job, _, _, _, scientific = validate_job(claim)
    job['_deadline'] = time.monotonic()+min(6.5, cycle['child_wall_seconds']-.25)
    journal = runtime.journal_for(cycle)
    raw_rpc = rpc or runtime.RPC()
    def transport(op, owner, args):
        require(owner == cycle['owner'], 'CONSUMER_TRANSPORT_OWNER_CHANGED')
        budget.before(1.9)
        document = {'p_op':op,'p_owner':owner,'p_args':dict(args)}
        call = journal.begin(op, runtime.object_hash(document), transport='DIRECT_IN_REAPED_RESEARCH_CHILD')
        began = time.monotonic(); census = {}
        value = budget.io('DATABASE_'+op, 1.9,
            lambda: raw_rpc.direct(document, transport_receipt=census))
        journal.finish(call, response_sha256=runtime.object_hash(value),
            rpc_elapsed_seconds=time.monotonic()-began, helper_cpu_seconds=0,
            process_identity=None, termination_proof=None, sql_tail_seconds=0,
            transport_receipt=census)
        return value
    require(_transport is None, 'CONSUMER_ALREADY_ACTIVE_CHILD_TRANSPORT')
    _transport = transport
    try:
        result = scientific.run_job(job, None)
    finally:
        _transport = None
    result['governed_work'] = budget.receipt()
    result['calls_sha256'] = journal.export()['calls_sha256']
    result['research_objective_achieved'] = False
    return result


def result_committed(result):
    if result.get('action') == 'PROCESS_SECURITY_BATCH':
        return result.get('server_progress_readback_verified') is True
    return result.get('state') == 'VERIFIED' and isinstance(result.get('output'),dict)


def timer_tick(owner, *, scheduled_at=None, trigger='PERSISTENT_WORKER_STARTUP', rpc=None, activation_key=None):
    mission = sys.modules.get('app.eq20_mission_continuation')
    observed = activation_key or (getattr(mission, '_prospective_activation_observed', None) if mission else None)
    if not observed and not ((ROOT/'active_cycle.json').exists() or (ROOT/'carry.json').exists()):
        return {'state':'AWAITING_ELIGIBLE_EVIDENCE',
            'reason':'ACTUAL_ACKNOWLEDGED_CONSUMER_ACTIVATION_NOT_YET_OBSERVED',
            'committed':False, 'research_objective_achieved':False}
    return lifecycle().timer_tick(owner, scheduled_at=scheduled_at, trigger=trigger,
                                 rpc=rpc, activation_key=observed)


def _consumer_child(attempt):
    runtime = lifecycle()
    require(re.fullmatch(r'[0-9a-f-]{36}',attempt), 'CONSUMER_CHILD_ATTEMPT_ID')
    cycle = runtime.read(ROOT/('cycle_'+attempt+'.json'), runtime.MAX_REPLY)
    require(0 < cycle['child_governed_seconds'] <= 30 and 0 < cycle['child_wall_seconds'] <= 7,
            'CONSUMER_CHILD_REGISTERED_PHYSICAL_LIMITS')
    # Reject an unadmitted envelope before installing the physical research
    # limits or loading any scientific/runtime guard.  On a high-baseline-RSS
    # host, lowering RLIMIT_AS can otherwise make the rejection receipt itself
    # impossible to allocate.  This branch performs no RPC and cannot admit
    # work; the full authenticated reservation is still checked again by
    # validate_job() inside run_slice for every admitted claim.
    if not isinstance(cycle.get('claim'), dict) or cycle['claim'].get('state') != 'RUNNING':
        result = {'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                  'reason':'ACTUAL_FINITE_CONSUMER_V3_RESERVATION_REQUIRED',
                  'research_objective_achieved':False}
        runtime.atomic(ROOT/('result_'+attempt+'.json'), result)
        return 1
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.setitimer(signal.ITIMER_REAL, min(6.8, cycle['child_wall_seconds']))
    signal.signal(signal.SIGPROF, signal.SIG_DFL)
    signal.setitimer(signal.ITIMER_PROF, min(5.8, cycle['child_governed_seconds']))
    resource.setrlimit(resource.RLIMIT_AS, (192*1024*1024,192*1024*1024))
    resource.setrlimit(resource.RLIMIT_FSIZE, (256*1024*1024,256*1024*1024))
    os.nice(10); runtime.guards().prohibit_descendants()
    try:
        result = run_slice(cycle['claim'], cycle)
    except Exception as error:
        result = {'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':str(error)[:180],
                  'research_objective_achieved':False}
    runtime.atomic(ROOT/('result_'+attempt+'.json'), result)
    return 0 if result.get('state') in ('RUNNING','VERIFIED') else 1


def request_stop():
    if _runtime is not None:
        _runtime.request_stop()


def join_shutdown(timeout=0):
    return _runtime is None or _runtime.join_shutdown(timeout)


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--rpc-helper':
        raise SystemExit(lifecycle()._rpc_helper(sys.argv[2]))
    if len(sys.argv) == 3 and sys.argv[1] == '--capture-child':
        raise SystemExit(_consumer_child(sys.argv[2]))
    raise SystemExit('Only the bounded registered consumer entry points are permitted')
