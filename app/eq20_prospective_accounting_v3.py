"""Measured, closed-cycle accounting for the separate prospective V3 design.

One current reservation stays fully held until a later, already funded cycle
can present its exact terminal acknowledgement and all process/RPC measurements.
This module validates proofs and maintains a local append-only call journal; it
does not grant capacity, refund V2 debits, meter another process by assumption,
or infer termination from lease expiry.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_CEILING
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid

VERSION = 'EQ20_PROSPECTIVE_CARRY_ACCOUNTING_V3'
MAXIMUM_RESERVATION = Decimal('54')
NORMAL_LIMIT = Decimal('30')
RECOVERY_LIMIT = 2
RECOVERY_SLOT = Decimal('12')
MAXIMUM_WALL_SECONDS = Decimal('150')
MAXIMUM_RECORDS = 4096
MAXIMUM_PROOF_BYTES = 1024*1024
ROLLOVER_ALLOWANCE = Decimal('12')
SHA = re.compile(r'[0-9a-f]{64}')


class Closed(ValueError):
    pass


def require(test, reason):
    if not test:
        raise Closed(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def number(value, name, maximum=MAXIMUM_RESERVATION):
    require(type(value) in (int, float, str, Decimal) and not isinstance(value, bool), name)
    try:
        result = Decimal(str(value))
    except Exception:
        raise Closed(name) from None
    require(result.is_finite() and 0 <= result <= maximum, name)
    return result


def identity(value, *, host, boot):
    require(isinstance(value, dict) and type(value.get('pid')) is int and value['pid'] > 1
        and type(value.get('start_ticks')) is int and value['start_ticks'] >= 0
        and value.get('process_group') == value['pid'] and value.get('boot_id') == boot
        and re.fullmatch(r'[0-9a-f-]{36}', boot or '') is not None
        and isinstance(host, str) and host, 'EXACT_PROCESS_IDENTITY_REQUIRED')


def validate_transport_receipt(call, elapsed, *, boot):
    """Check both HTTP calls; their elapsed time is already in the outer call."""
    if str(call.get('operation', '')).startswith('PROVIDER_GET_'):
        require(call.get('transport_receipt') is None, 'PROVIDER_TRANSPORT_CANNOT_IMPERSONATE_DB_PERMIT')
        return
    receipt = call.get('transport_receipt')
    require(isinstance(receipt, dict) and receipt.get('version') == 'EQ20_RPC_SERVER_ADMISSION_PERMIT_V3'
        and receipt.get('logical_request_sha256') == call.get('request_sha256')
        and receipt.get('all_started_calls_accounted') is True
        and receipt.get('client_clock_used_for_admission') is False,
        'COMPLETE_SERVER_PERMIT_TRANSPORT_CENSUS_REQUIRED')
    inner = receipt.get('calls'); target = receipt.get('target_rpc')
    require(isinstance(target, str) and re.fullmatch(r'public\.(eq20_[a-z0-9_]+)\(text,text,jsonb\)', target)
        and isinstance(inner, list), 'EXACT_SERVER_PERMIT_RPC_SCOPE_REQUIRED')
    stages = ['READ_ONLY_METADATA'] if call.get('operation') in ('status', 'probe') else ['PERMIT_MINT', 'PERMITTED_MUTATION']
    require([item.get('stage') for item in inner if isinstance(item, dict)] == stages
        and len(inner) == len(stages), 'EXACT_SERVER_PERMIT_SUBCALL_SEQUENCE_REQUIRED')
    total = Decimal(0)
    for item in inner:
        require(item.get('state') == 'RESPONSE_READ_BACK' and SHA.fullmatch(item.get('request_sha256', ''))
            and SHA.fullmatch(item.get('response_sha256', ''))
            and item.get('rpc_name') == ('eq20_request_permit_v3' if item['stage'] == 'PERMIT_MINT'
                                        else target.split('.')[1].split('(')[0]),
            'UNKNOWN_PERMIT_OR_MUTATION_RETAINS_FULL54')
        total += number(item.get('elapsed_seconds'), 'ACTUAL_SERVER_PERMIT_SUBCALL_ELAPSED_REQUIRED', Decimal('9'))
    require(total <= elapsed and inner[-1]['response_sha256'] == call.get('response_sha256'),
            'SERVER_PERMIT_SUBCALLS_NOT_COVERED_BY_ACTUAL_OUTER_CALL')
    if len(stages) == 2:
        permit = receipt.get('permit')
        require(isinstance(permit, dict) and permit.get('host_instance') == call.get('host_instance')
            and permit.get('boot_id') == boot and permit.get('target_rpc') == target
            and permit.get('request_sha256') == call.get('request_sha256')
            and re.fullmatch(r'[0-9a-f-]{36}', permit.get('permit_id', ''))
            and SHA.fullmatch(permit.get('request_jsonb_sha256', '')),
            'ACTUAL_SERVER_PERMIT_AND_PROCESS_SCOPE_REQUIRED')


def validate_closed_cycle(proof, *, attempt_id, activation_key, owner, host, module_sha256,
                          terminal_sha256, physical_sha256):
    """Compute a conservative charge only from a complete, bound prior cycle."""
    require(isinstance(proof, dict) and len(canonical(proof)) <= MAXIMUM_PROOF_BYTES,
            'BOUNDED_CLOSED_CYCLE_PROOF_REQUIRED')
    expected = {'version': VERSION, 'attempt_id': attempt_id, 'activation_key': activation_key,
        'owner': owner, 'host_instance': host, 'module_sha256': module_sha256,
        'terminal_receipt_sha256': terminal_sha256, 'physical_quiescence_sha256': physical_sha256,
        'all_started_calls_accounted': True, 'all_sql_tails_closed': True,
        'terminal_acknowledgement_observed': True, 'prior_v2_debits_changed': False,
        'measurement_boundary': 'AFTER_EXACT_TERMINAL_ACK_AND_HELPER_WAIT4'}
    require(all(proof.get(key) == value for key, value in expected.items()), 'CLOSED_CYCLE_SCOPE_OR_ACK_MISMATCH')
    require(all(SHA.fullmatch(proof.get(key, '')) for key in ('module_sha256',
        'terminal_receipt_sha256', 'physical_quiescence_sha256', 'terminal_ack_sha256')),
        'CLOSED_CYCLE_HASH_BINDINGS_REQUIRED')
    require(proof.get('unknown_call_count') == 0 and type(proof.get('unknown_call_count')) is int,
            'UNKNOWN_CYCLE_RETAINS_FULL54')
    calls = proof.get('calls')
    require(isinstance(calls, list) and 1 <= len(calls) <= MAXIMUM_RECORDS
            and proof.get('calls_sha256') == digest(calls), 'COMPLETE_CALL_MANIFEST_REQUIRED')
    boot = proof.get('boot_id')
    sequence = 0; normal_rpc = Decimal(0); recovery_rpc = Decimal(0); recovery_slots = set(); recovery_by_slot = {}
    ids = set(); terminal_calls = []
    for call in calls:
        require(isinstance(call, dict) and call.get('sequence') == sequence
            and type(call.get('sequence')) is int and call.get('completed') is True
            and call.get('response_readback_verified') is True and call.get('sql_tail_closed') is True
            and call.get('attempt_id') == attempt_id and call.get('owner') == owner
            and call.get('host_instance') == host and SHA.fullmatch(call.get('request_sha256', ''))
            and SHA.fullmatch(call.get('response_sha256', '')), 'ACTUAL_COMPLETED_CALL_RECEIPT_REQUIRED')
        call_id = call.get('call_id')
        require(isinstance(call_id, str) and re.fullmatch(r'[0-9a-f]{32}', call_id)
                and call_id not in ids, 'CALL_ID_UNIQUE_REQUIRED')
        ids.add(call_id)
        elapsed = number(call.get('rpc_elapsed_seconds'), 'CALL_ELAPSED_REQUIRED', Decimal('9'))
        validate_transport_receipt(call, elapsed, boot=boot)
        helper_cpu = number(call.get('helper_cpu_seconds'), 'CALL_HELPER_CPU_REQUIRED', Decimal('6'))
        tail = number(call.get('sql_tail_seconds'), 'CLOSED_SQL_TAIL_MEASUREMENT_REQUIRED', Decimal('9'))
        require(call.get('sql_tail_closed') is True, 'UNKNOWN_SQL_TAIL_RETAINS_FULL54')
        if call.get('transport') == 'REAPED_HTTP_HELPER':
            require(call.get('termination_proof') == 'SPECIFIC_CHILD_WAIT4', 'HTTP_HELPER_WAIT4_REQUIRED')
            identity(call.get('process_identity'), host=host, boot=boot)
        else:
            require(call.get('transport') == 'DIRECT_IN_REAPED_RESEARCH_CHILD' and helper_cpu == 0,
                    'CALL_PROCESS_MEASUREMENT_SCOPE_REQUIRED')
        cost = elapsed+helper_cpu+tail
        slot = call.get('recovery_slot')
        if slot is None:
            normal_rpc += cost
        else:
            require(type(slot) is int and 0 <= slot < RECOVERY_LIMIT, 'FINITE_RECOVERY_SLOT_REQUIRED')
            recovery_slots.add(slot); recovery_rpc += cost
            recovery_by_slot[slot] = recovery_by_slot.get(slot, Decimal(0))+cost
        if call.get('operation') in ('terminal', 'terminal_replay', 'capture_terminal'):
            terminal_calls.append(call)
        sequence += 1
    require(terminal_calls and terminal_calls[-1]['response_sha256'] == proof['terminal_ack_sha256'],
            'ACTUAL_TERMINAL_ACK_IN_CALL_CENSUS_REQUIRED')
    require(calls[-1] == terminal_calls[-1], 'PRIOR_CYCLE_MUST_END_WITH_ACKNOWLEDGED_TERMINAL')
    child = proof.get('research_child')
    require(isinstance(child, dict) and child.get('process_finished') is True
            and child.get('termination_proof') in ('SPECIFIC_CHILD_WAIT4', 'EXACT_OWNED_NO_RESEARCH_LAUNCH'),
            'ACTUAL_RESEARCH_QUIESCENCE_REQUIRED')
    child_cpu = number(child.get('cpu_seconds'), 'CHILD_CPU_MEASUREMENT_REQUIRED', Decimal('30'))
    wall = number(child.get('wall_seconds'), 'CHILD_WALL_MEASUREMENT_REQUIRED', MAXIMUM_WALL_SECONDS)
    if child['termination_proof'] == 'SPECIFIC_CHILD_WAIT4':
        identity(child.get('process_identity'), host=host, boot=boot)
    else:
        require(child_cpu == 0 and wall == 0 and child.get('launch_intent_absent') is True,
                'OWNED_NO_LAUNCH_PROOF_REQUIRED')
    parent_cpu = number(proof.get('parent_control_cpu_seconds'), 'PARENT_CPU_MEASUREMENT_REQUIRED', Decimal('6'))
    recovery_parent = number(proof.get('recovery_parent_cpu_seconds'), 'RECOVERY_PARENT_CPU_REQUIRED', Decimal('6'))
    # When only an aggregate parent measurement exists, charging it against
    # every used slot is conservative. Unused capacity in a second12-second
    # slot cannot cover an overrun in the first slot.
    require(all(value+recovery_parent <= RECOVERY_SLOT for value in recovery_by_slot.values()),
            'INDIVIDUAL_RECOVERY_SLOT_ENVELOPE_EXCEEDED_RETAINS_FULL54')
    cut = proof.get('meter_cut')
    require(isinstance(cut, dict) and re.fullmatch(r'[0-9a-f]{32}', cut.get('meter_id', ''))
        and cut.get('attempt_id') == attempt_id and cut.get('host_instance') == host
        and cut.get('boot_id') == boot and cut.get('terminal_ack_sha256') == proof['terminal_ack_sha256']
        and cut.get('all_old_calls_closed') is True and cut.get('all_children_reaped') is True
        and proof.get('meter_cut_sha256') == digest(cut), 'EXACT_ACK_AND_WAIT4_METER_CUT_REQUIRED')
    number(cut.get('parent_cpu_counter_at_cut'), 'ACTUAL_PARENT_CPU_COUNTER_AT_CUT', Decimal('1000000000000'))
    number(cut.get('monotonic_at_cut'), 'ACTUAL_MONOTONIC_METER_CUT', Decimal('1000000000000'))
    normal = child_cpu+parent_cpu+normal_rpc
    recovery = recovery_parent+recovery_rpc
    require(normal <= NORMAL_LIMIT and recovery <= RECOVERY_SLOT*len(recovery_slots),
            'CLOSED_CYCLE_ENVELOPE_EXCEEDED_RETAINS_FULL54')
    total = (normal+recovery).quantize(Decimal('0.000001'), rounding=ROUND_CEILING)
    require(total <= MAXIMUM_RESERVATION, 'CLOSED_CYCLE_EXCEEDS_RESERVATION')
    require(proof.get('governed_seconds') == format(total, 'f'), 'EXACT_MEASURED_GOVERNED_SUM_REQUIRED')
    return {'version': VERSION, 'mode': 'MEASURED_CLOSED_PRIOR_CYCLE', 'governed_seconds': format(total, 'f'),
        'normal_governed_seconds': format(normal, 'f'), 'recovery_governed_seconds': format(recovery, 'f'),
        'proof_sha256': digest(proof), 'calls_sha256': proof['calls_sha256'], 'call_count': len(calls),
        'reservation_seconds': 54, 'research_objective_achieved': False}


def build_closed_cycle(*, journal, scope, research_child, terminal_ack_sha256,
                       terminal_sha256, physical_sha256, parent_cpu_seconds,
                       recovery_parent_cpu_seconds, meter_cut, terminal_response=None):
    """Build an exact prefix proof after a real ACK/wait4 cut.

    The caller starts and persists the successor meter before assembling this
    proof or doing further control work. That later work is never omitted: it
    belongs to the successor reservation only when the atomic rollover commits.
    If a call is pending, the journal deliberately refuses to build a proof.
    """
    exported = journal.export() if isinstance(journal, CallJournal) else dict(journal)
    result = dict(exported, version=VERSION, **scope,
        research_child=dict(research_child), terminal_ack_sha256=terminal_ack_sha256,
        terminal_receipt_sha256=terminal_sha256, physical_quiescence_sha256=physical_sha256,
        parent_control_cpu_seconds=str(parent_cpu_seconds),
        recovery_parent_cpu_seconds=str(recovery_parent_cpu_seconds),
        terminal_acknowledgement_observed=True, prior_v2_debits_changed=False,
        measurement_boundary='AFTER_EXACT_TERMINAL_ACK_AND_HELPER_WAIT4',
        meter_cut=dict(meter_cut), meter_cut_sha256=digest(meter_cut))
    if terminal_response is not None:
        require(isinstance(terminal_response, dict) and digest(terminal_response) == terminal_ack_sha256,
                'ACTUAL_OUTER_TERMINAL_RESPONSE_HASH_REQUIRED')
        result['terminal_response'] = dict(terminal_response)
        result['terminal_response_canonical_utf8'] = canonical(terminal_response).decode()
    total = number(research_child.get('cpu_seconds'), 'CHILD_CPU_MEASUREMENT_REQUIRED', Decimal('30'))
    total += number(parent_cpu_seconds, 'PARENT_CPU_MEASUREMENT_REQUIRED', Decimal('6'))
    total += number(recovery_parent_cpu_seconds, 'RECOVERY_PARENT_CPU_REQUIRED', Decimal('6'))
    for call in result.get('calls', []):
        total += number(call.get('rpc_elapsed_seconds'), 'CALL_ELAPSED_REQUIRED', Decimal('9'))
        total += number(call.get('helper_cpu_seconds'), 'CALL_HELPER_CPU_REQUIRED', Decimal('6'))
        total += number(call.get('sql_tail_seconds'), 'CLOSED_SQL_TAIL_MEASUREMENT_REQUIRED', Decimal('9'))
    result['governed_seconds'] = format(total.quantize(Decimal('0.000001'), rounding=ROUND_CEILING), 'f')
    validate_closed_cycle(result, attempt_id=scope['attempt_id'], activation_key=scope['activation_key'],
        owner=scope['owner'], host=scope['host_instance'], module_sha256=scope['module_sha256'],
        terminal_sha256=terminal_sha256, physical_sha256=physical_sha256)
    return result


def prepare_rollover(proof, *, next_meter_id, next_attempt_id, next_owner, next_host,
                     next_boot_id, meter_started_before_closure):
    """Validate the funded handoff; this function neither grants nor credits CPU."""
    checked = validate_closed_cycle(proof, attempt_id=proof['attempt_id'], activation_key=proof['activation_key'],
        owner=proof['owner'], host=proof['host_instance'], module_sha256=proof['module_sha256'],
        terminal_sha256=proof['terminal_receipt_sha256'], physical_sha256=proof['physical_quiescence_sha256'])
    prefix = Decimal(checked['governed_seconds'])
    require(prefix+ROLLOVER_ALLOWANCE <= MAXIMUM_RESERVATION,
            'NO_FUNDED_ROLLOVER_HEADROOM_RETAINS_FULL54')
    require(re.fullmatch(r'[0-9a-f]{32}', next_meter_id or '') and next_meter_id != proof['meter_cut']['meter_id']
        and next_owner == proof['owner'] and next_host == proof['host_instance']
        and next_boot_id == proof['boot_id'] and next_attempt_id != proof['attempt_id']
        and re.fullmatch(r'[0-9a-f-]{36}', next_attempt_id or '')
        and re.fullmatch(r'[0-9a-f-]{36}', next_boot_id or '')
        and meter_started_before_closure is True, 'DURABLE_SUCCESSOR_METER_REQUIRED_BEFORE_CLOSURE_WORK')
    meter = {'version': 'EQ20_PROSPECTIVE_SUCCESSOR_METER_V3', 'meter_id': next_meter_id,
        'attempt_id': next_attempt_id, 'owner': next_owner, 'host_instance': next_host, 'boot_id': next_boot_id,
        'predecessor_attempt_id': proof['attempt_id'], 'predecessor_meter_cut_sha256': proof['meter_cut_sha256'],
        'started_before_proof_assembly_and_rollover': True,
        'all_work_since_prior_cut_charged_to_successor_only_if_atomic_admission_commits': True,
        'bounded_rollover_allowance_seconds': '12', 'old_proven_prefix_seconds': checked['governed_seconds'],
        'denied_or_unknown_admission_keeps_old_full54': True}
    return {'mode': 'MEASURED_CLOSED_PRIOR_CYCLE', 'closed_cycle': proof,
        'closed_cycle_canonical_utf8': canonical(proof).decode(),
        'next_meter': meter, 'next_meter_canonical_utf8': canonical(meter).decode(),
        'next_meter_sha256': digest(meter)}


def conservative_charge(reason, *, attempt_id, activation_key):
    require(isinstance(reason, str) and re.fullmatch(r'[A-Z][A-Z0-9_]{0,159}', reason), 'FIXED_REASON_REQUIRED')
    return {'version': VERSION, 'attempt_id': attempt_id, 'activation_key': activation_key,
        'mode': 'FULL54_UNKNOWN_OR_FINAL_CLOSURE', 'governed_seconds': '54.000000',
        'reason': reason, 'credit_seconds': '0.000000', 'research_objective_achieved': False}


def checked_atomic(path, value, *, scratch_safe):
    raw = canonical(value)
    require(len(raw) <= MAXIMUM_PROOF_BYTES and not path.is_symlink() and scratch_safe(len(raw)),
            'EXISTING_SHARED_SCRATCH_CEILING')
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temporary.open('xb') as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class CallJournal:
    """Persist intent before transport; an absent finish can never earn credit."""
    def __init__(self, path, scope, *, scratch_safe):
        self.path = Path(path); self.scope = dict(scope); self.scratch_safe = scratch_safe
        self.data = json.loads(self.path.read_bytes()) if self.path.exists() else {
            'version': VERSION, 'scope': self.scope, 'calls': [], 'pending': None}
        require(self.data.get('version') == VERSION and self.data.get('scope') == self.scope,
                'IMMUTABLE_ACCOUNTING_JOURNAL_SCOPE')

    def begin(self, operation, request_sha256, *, transport, recovery_slot=None):
        require(self.data.get('pending') is None and len(self.data['calls']) < MAXIMUM_RECORDS,
                'UNFINISHED_CALL_OR_FINITE_CALL_LIMIT')
        require(SHA.fullmatch(request_sha256 or '') is not None, 'ACTUAL_REQUEST_SHA_REQUIRED')
        call = dict(self.scope, sequence=len(self.data['calls']), call_id=uuid.uuid4().hex,
            operation=operation, request_sha256=request_sha256, transport=transport,
            recovery_slot=recovery_slot, started_monotonic=time.monotonic(), completed=False)
        self.data['pending'] = call; checked_atomic(self.path, self.data, scratch_safe=self.scratch_safe)
        return call['call_id']

    def finish(self, call_id, *, response_sha256, rpc_elapsed_seconds, helper_cpu_seconds=0,
               process_identity=None, termination_proof=None, sql_tail_seconds=0, transport_receipt=None):
        pending = self.data.get('pending')
        require(isinstance(pending, dict) and pending.get('call_id') == call_id
                and SHA.fullmatch(response_sha256 or '') is not None, 'EXACT_PENDING_CALL_FINISH_REQUIRED')
        # The caller may invoke finish only after bounded, complete readback and
        # physical helper termination. Exceptions deliberately leave the intent.
        call = {k: v for k, v in pending.items() if k != 'started_monotonic'}
        call.update(completed=True, response_readback_verified=True, sql_tail_closed=True,
            response_sha256=response_sha256, rpc_elapsed_seconds=str(rpc_elapsed_seconds),
            helper_cpu_seconds=str(helper_cpu_seconds), sql_tail_seconds=str(sql_tail_seconds),
            process_identity=process_identity, termination_proof=termination_proof)
        if transport_receipt is not None:
            call['transport_receipt'] = dict(transport_receipt)
        validate_transport_receipt(call, number(rpc_elapsed_seconds, 'CALL_ELAPSED_REQUIRED', Decimal('9')),
                                   boot=self.scope.get('boot_id'))
        updated = dict(self.data, calls=self.data['calls']+[call], pending=None)
        checked_atomic(self.path, updated, scratch_safe=self.scratch_safe)
        self.data = updated
        return call

    def export(self):
        require(self.data.get('pending') is None, 'UNKNOWN_CALL_RETAINS_FULL54')
        return {'calls': list(self.data['calls']), 'calls_sha256': digest(self.data['calls']),
                'unknown_call_count': 0, 'all_started_calls_accounted': True, 'all_sql_tails_closed': True}
