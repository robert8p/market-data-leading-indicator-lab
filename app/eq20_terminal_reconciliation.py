"""Finite, separately accounted replay of an exact expired terminal request.

This module does not launch a research child, select a new source item, reset an
old allowance, or decide that a failure is retryable. The private metadata RPC
owns atomic reservation, original-receipt reconciliation and continuation gates.
The caller must hold the existing source supervisor flock on the same host.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import socket
import time
import uuid


RPC_NAME = 'eq20_w10_terminal_reconcile_v1'
RPC_ACTIONS = frozenset(('RECONCILE_RESERVE', 'RECONCILE_COMMIT'))
VERSION = 'SOURCE_TERMINAL_RECONCILIATION_V1'
MAX_RECONCILIATIONS = 2
RESERVED_SECONDS = 30
RETRY_WALL_SECONDS = 45


def _require(supervisor, condition, code):
    if not condition:
        raise supervisor.GuardError(code)


def _object(supervisor, path, maximum):
    value = json.loads(supervisor.read_bounded(path, maximum))
    _require(supervisor, isinstance(value, dict), 'SOURCE_RECONCILIATION_DOCUMENT_REJECTED')
    return value


def _ack(supervisor, value, job):
    _require(supervisor, isinstance(value, dict) and
             value.get('settled') is True and value.get('released') is True and
             value.get('receipt_committed') is True and
             value.get('attempt_id') == job['attempt_id'] and
             value.get('reservation_id') == job['attempt_id'],
             'SOURCE_RECONCILIATION_ACK_REJECTED')
    return value


def _quiescence(supervisor, job, directory, maintenance, budget):
    # Both the original transport and any ambiguous maintenance helper must be
    # quiescent. Their persisted identities and server tails remain authoritative.
    supervisor.reconcile_control_calls(directory, budget)
    supervisor.reconcile_control_calls(maintenance, budget)
    record_path = directory / 'child_process.json'
    if record_path.exists():
        record = _object(supervisor, record_path, 4096)
        proof = supervisor.quiesce_recorded_child(record, budget, reserve_rpc=False)
    else:
        _require(supervisor, not (directory / 'launch_intent.json').exists(),
                 'SOURCE_RECONCILIATION_CHILD_PROOF_MISSING')
        proof = {'process_finished': True, 'proof': 'PERSISTED_NO_CHILD_LAUNCH_INTENT'}
    _require(supervisor, proof.get('process_finished') is True,
             'SOURCE_RECONCILIATION_CHILD_NOT_QUIESCENT')
    wait_seconds = supervisor.wait_for_database_quiescence(directory)
    return {'scope': 'SAME_HOST_EXACT_ORIGINAL_SOURCE_ATTEMPT',
            'attempt_id': job['attempt_id'], 'owner': job['owner'],
            'host_instance': job['host_instance'], 'fence': job['fence'],
            'process_finished': True, 'controls_quiescent': True,
            'termination_proof': proof,
            'bounded_database_quiescence_seconds': wait_seconds,
            'new_research_child_launched': False}


def _salvage(supervisor, job, maintenance, request_sha):
    paths = list(maintenance.glob('control_*.request.json'))
    _require(supervisor, len(paths) <= 32, 'SOURCE_RECONCILIATION_RECEIPT_BACKLOG')
    for path in paths:
        envelope = supervisor.validate_control_envelope(
            _object(supervisor, path, supervisor.MAX_CONTROL_BYTES))
        rpc = envelope['rpc']
        payload = rpc.get('p_payload', {})
        action = rpc.get('p_action')
        if action not in RPC_ACTIONS or payload.get('attempt_id') != job['attempt_id']:
            continue
        _require(supervisor, payload.get('original_request_sha256') == request_sha,
                 'SOURCE_RECONCILIATION_SALVAGE_BINDING_REJECTED')
        if action == 'RECONCILE_RESERVE':
            response_path = supervisor.control_paths(path)['response']
            if not response_path.exists():
                continue
            # Only a positive original acknowledgement is eligible for the
            # existing helper/dispatch/completion proof validator below.
            body = _object(supervisor, response_path, supervisor.MAX_CONTROL_BYTES)
            if not isinstance(body.get('response'), dict) or body['response'].get('original_ack') is None:
                continue
        response = supervisor.salvage_terminal_ack(
            {'action': action, 'payload': payload}, maintenance)
        if response is not None:
            accounting_valid = (response.get('maintenance_charged_cpu_seconds') == RESERVED_SECONDS
                                if action == 'RECONCILE_COMMIT' else
                                response.get('acquired') is False and response.get('reservation_created') is False)
            _require(supervisor, accounting_valid and response.get('original_terminal_allowance_reset') is False,
                     'SOURCE_RECONCILIATION_ACCOUNTING_ACK_REJECTED')
            return _ack(supervisor, response.get('original_ack'), job)
    return None


def reconcile_terminal(supervisor, job, directory, wrapper_sha, exhausted_budget=None):
    """Reserve at most two new metadata attempts; preserve the old six seconds.

    Returns the normal supervisor delay after a verified terminal acknowledgement.
    A missing/ambiguous reply retains the new intent and conservative reservation.
    Only a later bounded invocation can use the second server-enforced attempt.
    """
    budget = supervisor.ParentBudget()
    directory = supervisor.checked_path(directory)
    _require(supervisor, directory.parent == supervisor.SOURCE_ROOT / 'attempts' and
             directory.name == job.get('attempt_id') and
             job.get('host_instance') == socket.gethostname(),
             'SOURCE_RECONCILIATION_SAME_HOST_REQUIRED')
    owner = _object(supervisor, supervisor.SOURCE_ROOT / 'supervisor_identity.json', 4096)
    _require(supervisor, owner.get('owner') == job.get('owner') and
             owner.get('host_instance') == job.get('host_instance'),
             'SOURCE_RECONCILIATION_OWNER_REJECTED')
    original_state_path = directory / 'terminal_state.json'
    original_state_bytes = supervisor.read_bounded(original_state_path, 4096)
    original_state = json.loads(original_state_bytes)
    original_bytes = supervisor.read_bounded(directory / 'terminal_request.json',
                                              supervisor.MAX_RECEIPT_BYTES)
    original = json.loads(original_bytes)
    _require(supervisor, isinstance(original, dict) and
             original.get('action') in ('FINISH', 'FAIL', 'RECOVER') and
             isinstance(original.get('payload'), dict),
             'SOURCE_RECONCILIATION_ORIGINAL_REQUEST_REJECTED')
    original_payload = original['payload']
    _require(supervisor, all(original_payload.get(k) == job.get(k)
                            for k in ('owner', 'host_instance', 'fence', 'attempt_id')),
             'SOURCE_RECONCILIATION_ORIGINAL_IDENTITY_REJECTED')
    receipt = original_payload.get('receipt')
    _require(supervisor, isinstance(receipt, dict) and type(receipt.get('success')) is bool and
             receipt.get('process_finished') is True and
             receipt.get('protected_outcomes_accessed') is False and
             receipt['success'] is (original['action'] == 'FINISH'),
             'SOURCE_RECONCILIATION_ORIGINAL_RECEIPT_REJECTED')
    request_sha = supervisor.sha256(original_bytes)
    _require(supervisor, original_bytes == supervisor.canonical(original) and
             original_state.get('request_sha256') == request_sha,
             'SOURCE_RECONCILIATION_ORIGINAL_HASH_REJECTED')
    spent = original_state.get('spent_seconds')
    _require(supervisor, type(spent) in (float, int) and math.isfinite(spent) and
             0 <= spent <= supervisor.TERMINAL_SECONDS,
             'SOURCE_RECONCILIATION_ORIGINAL_ACCOUNTING_REJECTED')
    margin = supervisor.CONTROL_ALLOWANCE_SECONDS + 0.125
    exhausted = supervisor.TERMINAL_SECONDS - spent < margin
    if exhausted_budget is not None:
        exhausted = exhausted or (exhausted_budget.phase == 'TERMINAL' and
            exhausted_budget.phase_limit() - exhausted_budget.used() < margin)
    _require(supervisor, exhausted,
             'SOURCE_RECONCILIATION_EXHAUSTED_MARGIN_REQUIRED')

    maintenance = supervisor.make_directory(directory / 'terminal_reconciliation')
    state_path = maintenance / 'state.json'
    module_sha = supervisor.sha256(Path(__file__).read_bytes())
    state = _object(supervisor, state_path, 16384) if state_path.exists() else {
        'version': VERSION, 'original_request_sha256': request_sha, 'attempts': []}
    _require(supervisor, state.get('version') == VERSION and
             state.get('original_request_sha256') == request_sha and
             isinstance(state.get('attempts'), list) and
             len(state['attempts']) <= MAX_RECONCILIATIONS,
             'SOURCE_RECONCILIATION_STATE_REJECTED')
    attempts = state['attempts']
    envelope = _object(supervisor, directory / 'job.json', supervisor.MAX_CONTROL_BYTES)
    budget.server_anchor = envelope.get('server_anchor')
    proof = _quiescence(supervisor, job, directory, maintenance, budget)
    proof['original_terminal_state_sha256'] = supervisor.sha256(original_state_bytes)
    salvaged = _salvage(supervisor, job, maintenance, request_sha)
    if salvaged is not None:
        _require(supervisor, supervisor.read_bounded(original_state_path, 4096) == original_state_bytes,
                 'SOURCE_RECONCILIATION_ORIGINAL_ALLOWANCE_CHANGED')
        supervisor.atomic_write(directory / 'commit_ack.json', supervisor.canonical(salvaged), immutable=True)
        supervisor.own_cleanup(directory, budget)
        return 1 if salvaged.get('stage') == 'SOURCE_CORRECTION' else 30

    # A terminal failure may occur well before its original 150-second deadline
    # and 180-second lease end. Waiting must not consume either finite intent.
    server_now = supervisor.parse_timestamp(supervisor.request_start_deadline(
        budget.server_anchor, 0.5)) - 0.5
    if server_now < supervisor.parse_timestamp(job['deadline_at']) + 30:
        return 30
    if attempts:
        previous = attempts[-1]
        _require(supervisor, isinstance(previous, dict) and
                 type(previous.get('started_monotonic')) in (float, int) and
                 math.isfinite(previous['started_monotonic']) and
                 previous.get('boot_id') == Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                 'SOURCE_RECONCILIATION_CLOCK_PROOF_REJECTED')
        if time.monotonic() < previous['started_monotonic'] + RETRY_WALL_SECONDS:
            return 30
    payload = {'version': VERSION, 'owner': job['owner'],
               'host_instance': job['host_instance'], 'fence': job['fence'],
               'attempt_id': job['attempt_id'], 'wrapper_sha256': wrapper_sha,
               'module_sha256': module_sha,
               'original_request_sha256': request_sha,
               'original_request_text': original_bytes.decode('utf-8'),
               'quiescence': proof}
    if len(attempts) == MAX_RECONCILIATIONS:
        # No third reservation or research retry. One bounded final control
        # phase drains existing expired metadata reservations. CONTROL6 plus
        # TERMINAL6 plus DRAIN_CONTROL6 remains inside their prepaid30 ceiling.
        _require(supervisor, state.get('drain') is None,
                 'SOURCE_RECONCILIATION_ATTEMPT_LIMIT_REACHED')
        keys = [x.get('attempt_key') for x in attempts]
        _require(supervisor, all(isinstance(x, str) and 1 <= len(x) <= 200 for x in keys),
                 'SOURCE_RECONCILIATION_DRAIN_KEY_REJECTED')
        drain = {'state': 'DRAIN_DISPATCH_INTENT', 'started_monotonic': time.monotonic(),
                 'existing_reconciliation_keys': keys, 'new_reservation_authorized': False,
                 'maximum_drain_control_seconds': supervisor.PARENT_SECONDS,
                 'existing_metadata_maximum_control_terminal_drain_seconds': 18}
        state['drain'] = drain
        supervisor.atomic_write(state_path, supervisor.canonical(state))
        payload.update(reconciliation_key=keys[-1], prior_reconciliation_keys=keys, drain_only=True)
        try:
            reply = supervisor.control_rpc('RECONCILE_RESERVE', payload, budget, maintenance)
            _require(supervisor, reply.get('acquired') is False and
                     reply.get('reservation_created') is False and reply.get('drain_only') is True,
                     'SOURCE_RECONCILIATION_DRAIN_RESPONSE_REJECTED')
            _require(supervisor, supervisor.read_bounded(original_state_path, 4096) == original_state_bytes,
                     'SOURCE_RECONCILIATION_ORIGINAL_ALLOWANCE_CHANGED')
            drain.update(state='DRAIN_RESULT_RECORDED', result=reply,
                         helper_quiescence_verified=True, completed_monotonic=time.monotonic())
            supervisor.atomic_write(state_path, supervisor.canonical(state))
            if reply.get('original_ack') is None:
                return 30  # The exact original source dependency remains open.
            ack = _ack(supervisor, reply['original_ack'], job)
            supervisor.atomic_write(directory / 'commit_ack.json', supervisor.canonical(ack), immutable=True)
        except BaseException:
            _require(supervisor, supervisor.read_bounded(original_state_path, 4096) == original_state_bytes,
                     'SOURCE_RECONCILIATION_ORIGINAL_ALLOWANCE_CHANGED')
            raise
        supervisor.own_cleanup(directory, budget)
        return 1 if ack.get('stage') == 'SOURCE_CORRECTION' else 30

    # This budget is for a new metadata operation and is charged separately. It
    # never reads or changes the old terminal allowance as a new budget balance.
    ordinal = len(attempts) + 1
    attempt_key = 'terminal_reconcile_' + job['attempt_id'] + '_' + uuid.uuid4().hex
    local = {'ordinal': ordinal, 'attempt_key': attempt_key,
             'started_monotonic': time.monotonic(),
             'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
             'state': 'RESERVATION_DISPATCH_INTENT', 'module_sha256': module_sha}
    attempts.append(local)
    supervisor.atomic_write(state_path, supervisor.canonical(state))
    payload.update(reconciliation_key=attempt_key, drain_only=False)
    try:
        reservation = supervisor.control_rpc('RECONCILE_RESERVE', payload, budget, maintenance)
        if reservation.get('original_ack') is not None:
            ack = _ack(supervisor, reservation['original_ack'], job)
        elif reservation.get('acquired') is False:
            _require(supervisor, reservation.get('reservation_created') is False,
                     'SOURCE_RECONCILIATION_CLOSED_RESPONSE_REJECTED')
            _require(supervisor, supervisor.read_bounded(original_state_path, 4096) == original_state_bytes,
                     'SOURCE_RECONCILIATION_ORIGINAL_ALLOWANCE_CHANGED')
            local.update(state='RESERVATION_CLOSED', response=reservation,
                         helper_quiescence_verified=True, completed_monotonic=time.monotonic())
            supervisor.atomic_write(state_path, supervisor.canonical(state))
            return 30
        else:
            _require(supervisor, reservation.get('acquired') is True and
                     reservation.get('reserved_cpu_seconds') == RESERVED_SECONDS and
                     reservation.get('original_attempt_id') == job['attempt_id'] and
                     reservation.get('original_request_sha256') == request_sha and
                     isinstance(reservation.get('maintenance_id'), str),
                     'SOURCE_RECONCILIATION_RESERVATION_REJECTED')
            local.update(state='RESERVED', maintenance_id=reservation['maintenance_id'])
            supervisor.atomic_write(state_path, supervisor.canonical(state))
            budget.terminal_begin = budget.used()
            budget.phase = 'TERMINAL'
            payload['maintenance_id'] = reservation['maintenance_id']
            # control_rpc updates its authoritative server anchor from replies.
            # The server also checks the separately bounded maintenance deadline.
            reply = supervisor.control_rpc('RECONCILE_COMMIT', payload, budget,
                                            maintenance, terminal=True)
            ack = _ack(supervisor, reply.get('original_ack'), job)
            _require(supervisor, reply.get('maintenance_charged_cpu_seconds') == RESERVED_SECONDS and
                     reply.get('original_terminal_allowance_reset') is False,
                     'SOURCE_RECONCILIATION_ACCOUNTING_ACK_REJECTED')
        _require(supervisor, supervisor.read_bounded(original_state_path, 4096) == original_state_bytes,
                 'SOURCE_RECONCILIATION_ORIGINAL_ALLOWANCE_CHANGED')
        local.update(state='VERIFIED_COMMIT_ACK', helper_quiescence_verified=True,
                     original_ack=ack, completed_monotonic=time.monotonic())
        supervisor.atomic_write(state_path, supervisor.canonical(state))
        supervisor.atomic_write(directory / 'commit_ack.json', supervisor.canonical(ack), immutable=True)
    except BaseException:
        _require(supervisor, supervisor.read_bounded(original_state_path, 4096) == original_state_bytes,
                 'SOURCE_RECONCILIATION_ORIGINAL_ALLOWANCE_CHANGED')
        raise
    supervisor.own_cleanup(directory, budget)
    return 1 if ack.get('stage') == 'SOURCE_CORRECTION' else 30
