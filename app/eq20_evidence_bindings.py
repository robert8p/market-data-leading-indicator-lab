"""Authoritative, outcome-blind binding gate for prospective EQ20 evidence.

Only the authenticated private RPC supplies registry records.  Every record is
read back by exact kind, key, status and SHA256 of PostgreSQL's evidence text.
Client booleans and hash-shaped strings are never substituted for those records.
This module grants no database privilege and does not fetch market outcomes.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from fractions import Fraction
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid

VERSION = 'EQ20_EVIDENCE_BINDING_RUNTIME_V1_20261004'
RPC_NAME = 'eq20_evidence_activation_v1'
METHOD_VERSION = 'EQ20_SUCCESSOR_CERTIFICATION_METHOD_V2_20261004'
METHOD_CONTRACT_SHA256 = '9143ac194c39051d5224890e19e9b3b33dc5264e3085d2d4bff4126a7aea6d21'
METHOD_SOURCE_SHA256 = 'd92f62331f535e7391cf23627ecd23cbca7209c7fbc903c095256ee8849025a3'
MAX_REPLY = 1024 * 1024
HASH = re.compile(r'[0-9a-f]{64}')
ROLE_SPECS = {
    'method': ('SUCCESSOR_CERTIFICATION_METHOD', 'IMPLEMENTED_AWAITING_ACTIVATION_BINDINGS'),
    'math_review': ('INDEPENDENT_STATISTICAL_REVIEW', 'VERIFIED_CONDITIONAL_MATHEMATICS_NOT_MARKET_ASSUMPTIONS'),
    'candidate': ('SUCCESSOR_CANDIDATE_FREEZE', 'REGISTERED_FROZEN_DEVELOPMENT_ONLY'),
    'population': ('SUCCESSOR_FULL_POPULATION_CERTIFICATE', 'VERIFIED_POINT_IN_TIME_FULL_POPULATION'),
    'calendar': ('SUCCESSOR_OFFICIAL_CALENDAR_CERTIFICATE', 'VERIFIED_OFFICIAL_CALENDAR'),
    'execution': ('SUCCESSOR_EXECUTION_POLICY', 'REGISTERED_FROZEN_EXECUTION_POLICY'),
    'causal_source': ('SUCCESSOR_CAUSAL_SOURCE_CERTIFICATE', 'VERIFIED_CAUSAL_FULL_POPULATION_SOURCE'),
    'exposure': ('SUCCESSOR_EXPOSURE_CERTIFICATE', 'VERIFIED_PROSPECTIVE_RESERVATION'),
    'dependence_argument': ('SUCCESSOR_DEPENDENCE_ARGUMENT', 'REGISTERED_OUTCOME_BLIND_ASSUMPTION_JUSTIFICATION'),
    'dependence': ('SUCCESSOR_DEPENDENCE_REVIEW', 'VERIFIED_CONDITIONAL_DATE_INDEPENDENCE'),
    'resource': ('SUCCESSOR_RESOURCE_RESERVATION', 'VERIFIED_FINITE_RESOURCE_RESERVATION'),
    'alpha_family': ('SUCCESSOR_ALPHA_FAMILY_RESERVATION', 'VERIFIED_NONRECYCLING_ALPHA_FAMILY_RESERVATION'),
    'historical_adjudication': ('SUCCESSOR_HISTORICAL_ADJUDICATION', 'VERIFIED_HISTORICAL_REQUIREMENTS_PRESERVED'),
    'collector': ('SUCCESSOR_COLLECTOR_RELEASE', 'VERIFIED_EXECUTABLE_RELEASE'),
}
SCOPE_KEYS = ('candidate_family_sha256', 'population_manifest_sha256',
              'official_calendar_sha256', 'official_session_dates_sha256',
              'execution_policy_sha256', 'method_contract_sha256')
_timer_lock = threading.Lock()
_next_timer_at = 0.0


class BindingClosed(ValueError):
    pass


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
        ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def require(condition, reason):
    if not condition:
        raise BindingClosed(reason)


def utc(value):
    require(isinstance(value, str), 'REGISTRY_TIMESTAMP_MISSING')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(parsed.tzinfo is not None, 'REGISTRY_TIMESTAMP_WITHOUT_TIMEZONE')
    return parsed.astimezone(timezone.utc)


def read_receipt(row, ref, *, expected_kind=None, expected_status=None):
    """Verify actual serialized DB readback; no client assertion can pass this."""
    require(isinstance(row, dict) and isinstance(ref, dict), 'ARTIFACT_READBACK_MISSING')
    for field in ('kind', 'artifact_key', 'status', 'implementation_sha256'):
        require(isinstance(ref.get(field), str) and row.get(field) == ref[field],
                'ARTIFACT_' + field.upper() + '_MISMATCH')
    require(HASH.fullmatch(ref['implementation_sha256']) is not None, 'ARTIFACT_HASH_FORMAT')
    require(expected_kind is None or row['kind'] == expected_kind, 'ARTIFACT_KIND_NOT_AUTHORIZED')
    require(expected_status is None or row['status'] == expected_status, 'ARTIFACT_STATUS_NOT_AUTHORIZED')
    raw = row.get('evidence_text')
    require(isinstance(raw, str) and len(raw.encode()) <= MAX_REPLY, 'ARTIFACT_READBACK_TEXT_BOUND')
    require(hashlib.sha256(raw.encode()).hexdigest() == ref['implementation_sha256'],
            'ARTIFACT_READBACK_HASH_MISMATCH')
    evidence = json.loads(raw)
    require(isinstance(evidence, dict), 'ARTIFACT_EVIDENCE_SHAPE')
    if 'evidence' in row:
        require(evidence == row['evidence'], 'ARTIFACT_JSON_READBACK_MISMATCH')
    utc(row.get('created_at'))
    return evidence


def resolve_receipts(rows, refs):
    """Resolve the exact required registry, rejecting omissions and extra roles."""
    require(isinstance(refs, dict) and set(refs) == set(ROLE_SPECS), 'BINDING_ROLE_SET_MISMATCH')
    require(isinstance(rows, dict) and set(rows) == set(ROLE_SPECS), 'BINDING_READBACK_SET_MISMATCH')
    resolved = {}
    for role, (kind, status) in ROLE_SPECS.items():
        resolved[role] = read_receipt(rows[role], refs[role], expected_kind=kind, expected_status=status)
    return resolved


def verify_readiness(readback):
    """Independent readback check before an activation request can be sent.

    The SQL transaction repeats all authorization-critical checks under its own
    lock. This secondary check cannot turn a server denial into an authorization.
    """
    reasons = []
    try:
        require(isinstance(readback, dict), 'REGISTRY_RESPONSE_SHAPE')
        server_reasons = readback.get('dependencies', [])
        require(isinstance(server_reasons, list) and all(isinstance(x, str) for x in server_reasons),
                'REGISTRY_DEPENDENCIES_SHAPE')
        reasons.extend(server_reasons)
        require(readback.get('desired') == 'RUN', 'EXPLICIT_STOP_OR_CANCEL')
        now = utc(readback.get('server_time'))
        plan = read_receipt(readback.get('plan'), readback.get('plan_reference'),
            expected_kind='SUCCESSOR_EVIDENCE_ACTIVATION_PLAN', expected_status='REGISTERED_OUTCOME_BLIND')
        resolved = resolve_receipts(readback.get('artifacts'), plan.get('bindings'))
        require(plan.get('version') == 'EQ20_SUCCESSOR_ACTIVATION_PLAN_V1', 'PLAN_VERSION_MISMATCH')
        require(plan.get('method_contract_sha256') == METHOD_CONTRACT_SHA256, 'METHOD_CONTRACT_PIN')
        require(all(isinstance(plan.get(k), str) and HASH.fullmatch(plan[k]) for k in SCOPE_KEYS),
                'PLAN_SCOPE_HASH_MISSING')
        require(plan.get('scope_sha256') == canonical_hash({k: plan[k] for k in SCOPE_KEYS}),
                'PLAN_SCOPE_DIGEST_MISMATCH')
        method = resolved['method']
        require(method.get('version') == METHOD_VERSION
                and method.get('contract_sha256') == METHOD_CONTRACT_SHA256
                and method.get('implementation', {}).get('source_sha256') == METHOD_SOURCE_SHA256,
                'REGISTERED_METHOD_V2_REQUIRED')
        require(resolved['math_review'].get('reviewed_source_sha256') == METHOD_SOURCE_SHA256
                and resolved['math_review'].get('contract_sha256_excluding_selfhash') == METHOD_CONTRACT_SHA256,
                'INDEPENDENT_REVIEW_PIN_MISMATCH')
        installed = Path(__file__).with_name('eq20_cluster_inference.py')
        require(hashlib.sha256(installed.read_bytes()).hexdigest() == METHOD_SOURCE_SHA256,
                'DEPLOYED_EVALUATOR_PIN_MISMATCH')
        # The global alpha family persists across calendars and candidate epochs.
        # Requiring its immutable receipt to contain an epoch scope would make
        # every subsequent epoch either impossible or an impermissible reset.
        for role in set(ROLE_SPECS) - {'method', 'math_review', 'alpha_family'}:
            item = resolved[role]
            require(item.get('scope_sha256') == plan['scope_sha256'], role.upper() + '_SCOPE_MISMATCH')
            require(item.get('protected_outcomes_accessed') is False, role.upper() + '_OUTCOME_BLIND_PROVENANCE_MISSING')
        dates = plan.get('official_session_dates')
        require(isinstance(dates, list) and len(dates) == 252
                and all(isinstance(d, str) and date.fromisoformat(d).isoformat() == d for d in dates)
                and sorted(set(dates)) == dates, 'EXACT_252_OFFICIAL_SESSIONS_REQUIRED')
        require(canonical_hash(dates) == plan['official_session_dates_sha256'], 'OFFICIAL_SESSION_VECTOR_HASH_MISMATCH')
        require(not any('2026-06-01' <= d <= '2026-07-31' for d in dates), 'JUNE_JULY_RELATED_LINEAGE_EXPOSED')
        calendar = resolved['calendar']
        sessions = calendar.get('sessions')
        require(isinstance(sessions, list) and [s.get('session_date') for s in sessions] == dates,
                'OFFICIAL_CALENDAR_VECTOR_MISMATCH')
        require(calendar.get('official_calendar_sha256') == plan['official_calendar_sha256'],
                'OFFICIAL_CALENDAR_PIN_MISMATCH')
        latest_freeze = max(utc(readback['plan']['created_at']),
            *(utc(row['created_at']) for row in readback['artifacts'].values()))
        require(latest_freeze <= now, 'FUTURE_REGISTRY_TIMESTAMP')
        opens = [utc(s.get('open_at')) for s in sessions]
        closes = [utc(s.get('close_at')) for s in sessions]
        require(all(o < c for o, c in zip(opens, closes)) and opens == sorted(set(opens))
                and all(closes[i] < opens[i+1] for i in range(len(opens)-1)), 'OFFICIAL_CALENDAR_TIME_ORDER')
        require(max(now, latest_freeze) < opens[0], 'ALL_BINDINGS_AND_ACTIVATION_MUST_PRECEDE_FIRST_SESSION')
        require(calendar.get('first_complete_session_after_registration') == dates[0], 'FIRST_ELIGIBLE_SESSION_NOT_VERIFIED')
        previous_open = utc(calendar.get('previous_official_session_open_at'))
        previous_close = utc(calendar.get('previous_official_session_close_at'))
        require(previous_open < previous_close < opens[0] and previous_open <= now,
                'OFFICIAL_SESSION_SKIPPED_BEFORE_REGISTERED_START')
        candidate = resolved['candidate']
        require(candidate.get('candidate_family_sha256') == plan['candidate_family_sha256']
                and type(candidate.get('candidate_count')) is int and 1 <= candidate['candidate_count'] <= 5,
                'FROZEN_CANDIDATE_FAMILY_MISMATCH')
        require(candidate.get('selection_evidence_class') == 'DEVELOPMENT'
                and candidate.get('selection_cutoff') <= '2026-05-31'
                and candidate.get('post_freeze_changes') is False, 'DEVELOPMENT_ONLY_SELECTION_REQUIRED')
        require(resolved['population'].get('population_manifest_sha256') == plan['population_manifest_sha256']
                and resolved['population'].get('full_natural_population') is True,
                'FULL_POPULATION_RECONCILIATION_REQUIRED')
        require(resolved['execution'].get('execution_policy_sha256') == plan['execution_policy_sha256'],
                'FROZEN_EXECUTION_POLICY_PIN_MISMATCH')
        require(resolved['causal_source'].get('population_manifest_sha256') == plan['population_manifest_sha256']
                and resolved['causal_source'].get('causal_publication_readiness_verified') is True,
                'CAUSAL_FULL_POPULATION_SOURCE_REQUIRED')
        exposure = resolved['exposure']
        require(exposure.get('exposure_class') == 'PROSPECTIVE_RESERVED'
                and exposure.get('access_history_complete') is True
                and exposure.get('unknown_lineage_access') is False
                and exposure.get('prior_evaluation_access') is False,
                'EXPOSURE_HISTORY_NOT_CLEARED')
        require(exposure.get('exposure_ledger_head_sha256') == readback.get('exposure_ledger_head_sha256')
                and exposure.get('access_ledger_head_sha256') == readback.get('access_ledger_head_sha256'),
                'EXPOSURE_ACCESS_LEDGER_CHANGED_SINCE_ATTESTATION')
        dependence = resolved['dependence']
        require(dependence.get('conditional_date_independence_supported') is True
                and dependence.get('argument_artifact_sha256') == plan['bindings']['dependence_argument']['implementation_sha256']
                and dependence.get('independent_reviewer_id')
                and dependence.get('nonrejection_only_justification') is False,
                'ACTUAL_CONDITIONAL_DEPENDENCE_JUSTIFICATION_REQUIRED')
        require(resolved['alpha_family'].get('global_alpha_fraction') == '1/20'
                and resolved['alpha_family'].get('programme_id') == 'EQ20_SUCCESSOR_GLOBAL_ALPHA_V1'
                and resolved['alpha_family'].get('claim_slots') == 12
                and resolved['alpha_family'].get('unused_alpha_recycled') is False
                and resolved['alpha_family'].get('original_and_related_alpha_reconciled') is True,
                'GLOBAL_ALPHA_PROVENANCE_REQUIRED')
        next_alpha = immutable_epoch_alpha(readback.get('next_epoch'))
        require(-math.expm1(math.log(float(next_alpha)) / 252) < 0.05,
                'FIXED_HORIZON_ALPHA_INFORMATION_INFEASIBLE')
        require(resolved['resource'].get('no_new_paid_cost') is True
                and readback.get('resource_allocation_available') is True
                and readback.get('resource_allocation_readback_sha256') == resolved['resource'].get('allocation_sha256'),
                'ACTUAL_FINITE_RESOURCE_RESERVATION_REQUIRED')
        require(resolved['collector'].get('release_verified') is True
                and resolved['collector'].get('protected_access_gate_required') is True
                and resolved['collector'].get('resumable') is True
                and resolved['collector'].get('bounded_retries') is True,
                'VERIFIED_PROSPECTIVE_COLLECTOR_REQUIRED')
        require(resolved['historical_adjudication'].get('original_requirements_preserved') is True
                and resolved['historical_adjudication'].get('july_retroactively_certified') is False,
                'HISTORICAL_ACCEPTANCE_ADJUDICATION_REQUIRED')
        require(plan.get('fixed_horizon_sessions') == 252 and plan.get('claim_slots') == 12
                and plan.get('outcome_based_stopping_or_extension') is False,
                'FIXED_HORIZON_AND_MULTIPLICITY_REQUIRED')
    except (BindingClosed, KeyError, TypeError, ValueError, AttributeError, OSError) as exc:
        reason = str(exc) if isinstance(exc, BindingClosed) else 'INVALID_REGISTRY_DOCUMENT'
        reasons.append(reason)
    return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY' if reasons else 'VERIFIED',
            'dependencies': sorted(set(reasons)), 'protected_outcomes_accessed': False,
            'research_objective_achieved': False, 'grants_evidence_access': False}


class EvidenceRPC:
    def __init__(self):
        self.base = os.environ.get('SUPABASE_URL', '').rstrip('/')
        self.key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '').strip()
        require(self.base == 'https://oxzabweahkoimtevbbny.supabase.co' and self.key,
                'PRIVATE_CONFIGURATION_REQUIRED')

    def call(self, operation, owner, args):
        body = json.dumps({'p_op': operation, 'p_owner': owner, 'p_args': args},
                          separators=(',', ':'), allow_nan=False).encode()
        request = urllib.request.Request(self.base + '/rest/v1/rpc/' + RPC_NAME, data=body,
            headers={'Authorization': 'Bearer ' + self.key, 'apikey': self.key,
                     'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.urlopen(request, timeout=4) as response:
                raw = response.read(MAX_REPLY + 1)
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ConnectionError):
            raise BindingClosed('EVIDENCE_REGISTRY_RPC_FAILURE_NO_REPLAY') from None
        require(len(raw) <= MAX_REPLY, 'EVIDENCE_REGISTRY_RESPONSE_BOUND')
        result = json.loads(raw)
        require(isinstance(result, dict), 'EVIDENCE_REGISTRY_RESPONSE_SHAPE')
        return result


def supervise_once(rpc, owner, *, scheduled_at=None, trigger='PERSISTENT_WORKER_STARTUP'):
    """Read metadata, independently check it, then request one atomic transition."""
    require(trigger in ('PERSISTENT_WORKER_STARTUP', 'PERSISTENT_WORKER_TIMER'), 'OBSERVED_TRIGGER_REQUIRED')
    args = dict(version=VERSION, module_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                host_instance=socket.gethostname(), invocation_id=uuid.uuid4().hex,
                trigger=trigger, scheduled_at=scheduled_at)
    readback = rpc.call('readiness', owner, args)
    if readback.get('existing_activation') is not None:
        if readback.get('dependencies'):
            return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'committed': False,
                    'dependencies': readback['dependencies'], 'protected_outcomes_accessed': False,
                    'research_objective_achieved': False, 'grants_evidence_access': False}
        activation = read_receipt(readback['existing_activation'], readback.get('activation_reference'),
            expected_kind='SUCCESSOR_EVIDENCE_ACTIVATION', expected_status='AWAITING_ELIGIBLE_EVIDENCE')
        require(activation.get('plan_sha256') == (readback.get('plan_reference') or {}).get('implementation_sha256')
                and activation.get('alpha_fraction') == str(immutable_epoch_alpha(activation.get('epoch'))),
                'EXISTING_ACTIVATION_PIN_MISMATCH')
        require(readback.get('desired') == 'RUN', 'EXPLICIT_STOP_OR_CANCEL')
        if readback.get('commit_acknowledgement_verified') is not True:
            args['activation_key'] = activation['activation_key']
            return rpc.call('ack_activation', owner, args)
        return {'state': 'AWAITING_ELIGIBLE_EVIDENCE', 'committed': False,
                'activation_key': activation['activation_key'],
                'dependencies': ['PROSPECTIVE_COLLECTOR_EXECUTION_AND_COMPLETE_FIXED_HORIZON_PENDING'],
                'commit_acknowledgement_verified': True, 'protected_outcomes_accessed': False,
                'research_objective_achieved': False, 'grants_evidence_access': False}
    checked = verify_readiness(readback)
    if checked['state'] != 'VERIFIED':
        # Passive blocked checks remain one read-only RPC. The existing mission
        # supervisor owns governed progress/timer receipts; do not append empty
        # work every poll or charge a prospective allocation that does not exist.
        return dict(checked, committed=False, trigger=trigger,
                    server_time=readback.get('server_time'), next_poll_seconds=300)
    args['plan_sha256'] = (readback.get('plan_reference') or {}).get('implementation_sha256')
    args['python_dependencies'] = checked['dependencies']
    # A Python denial may only make the result stricter. Server-side SQL resolves
    # and repeats the gate transactionally; it never trusts a caller "pass".
    result = rpc.call('activate', owner, args)
    require(result.get('protected_outcomes_accessed') is False, 'REGISTRY_SCOPE_VIOLATION')
    require(isinstance(result.get('activation_key'), str), 'COMMITTED_ACTIVATION_ACKNOWLEDGEMENT_MISSING')
    # This second transaction observes a registration that has already committed.
    # Its server time, not the first call's start time, proves pre-session commit.
    args['activation_key'] = result['activation_key']
    acknowledged = rpc.call('ack_activation', owner, args)
    require(acknowledged.get('protected_outcomes_accessed') is False, 'REGISTRY_SCOPE_VIOLATION')
    return acknowledged


def timer_tick(owner, *, scheduled_at=None, trigger='PERSISTENT_WORKER_STARTUP', rpc=None):
    """Passive metadata task for the existing mission timer; no extra thread."""
    global _next_timer_at
    with _timer_lock:
        now = time.monotonic()
        if now < _next_timer_at:
            return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'throttled': True,
                    'protected_outcomes_accessed': False, 'committed': False}
        # Bound repeated failures too; no uncontrolled immediate retries.
        _next_timer_at = now + 300
    return supervise_once(rpc or EvidenceRPC(), owner, scheduled_at=scheduled_at, trigger=trigger)


def immutable_epoch_alpha(epoch):
    require(type(epoch) is int and 1 <= epoch <= 1000000, 'IMMUTABLE_EPOCH_REQUIRED')
    return Fraction(1, 20 * epoch * (epoch + 1) * 12)
