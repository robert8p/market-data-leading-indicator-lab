"""Registered disjoint FP02 development continuation after an immutable FP01 zero.

The fixed atomic gates, source semantics, folds, selection and economic objective
are inherited by hash. A new outcome-blind ranking selects unused gate pairs.
FP02 owns separate private controls, attempts, receipts, output and finite
allocation. Its transport and process containment are the reviewed continuation
architecture; the original W10 and FP01 histories remain immutable.
"""
from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
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
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
import zlib

VERSION = 'EQ20_FP02_CONTINUATION_V2_20261004'
RPC_NAME = 'eq20_fp02_continuation_v1'
ENTRYPOINT = Path(__file__).resolve()
SQL_TAIL_SECONDS = 3
CONTROL_GOVERNED_SECONDS = 7
QA_CHILD_CPU_SECONDS = 5
SCOPE_SHA256 = 'bb797e6337663bfc7cc08c54d083bb8e1711a52fc97ee793e5a19095e90c079f'
ORIGINAL_CONTRACT_SHA256 = 'a5ee53e53fd423a4d3fe8585b35b6132f4f1d43cb7dd29d07292942e817e8894'
ENGINE_SHA256 = '00e6a4c6e6245b4a8e2a398561fa8a51a01dc502273aaebbd446bd25cc7af346'
BOUND_ENGINE_SHA256 = '5730d2f79ab96557b25679dd984a3c56bb7864c7a1b5b0d34b97c7528c815006'
FROZEN_POLICY_SHA256 = '323c48c56e816106096da4148c3b223ae7d2ffe5bb7170df20eb8c9704342abf'
SHA256 = re.compile(r'[0-9a-f]{64}')
MAX_FILE = 64 * 1024 * 1024
MAX_REPLY = 3 * 1024 * 1024
MAX_TERMINAL_DOCUMENT_BYTES = 256 * 1024
MAX_RECOVERY_TERMINAL_BYTES = 320 * 1024
MAX_RECOVERY_REPLAY_BYTES = 1792 * 1024
# Include continuation scratch in the *existing* source-wide 2 GiB census.
ROOT = Path('/tmp/astra-eq20-w10/mission_continuation/fp02')
LOG = logging.getLogger(__name__)
_started = False
_start_lock = threading.Lock()
_stop = threading.Event()
_thread = None
_DIRECT_RPC = False
_source_guards = None
_last_rpc_proof = None


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
    if path.name.startswith('terminal_'):
        require(len(raw) <= MAX_TERMINAL_DOCUMENT_BYTES, 'MISSION_FINITE_TERMINAL_DOCUMENT_BOUND')
    elif path.name.startswith('recovery_terminal_'):
        require(len(raw) <= MAX_RECOVERY_TERMINAL_BYTES, 'MISSION_FINITE_RECOVERY_DOCUMENT_BOUND')
    elif path.name.startswith('recovery_replay_'):
        require(len(raw) <= MAX_RECOVERY_REPLAY_BYTES, 'MISSION_FINITE_REPLAY_DOCUMENT_BOUND')
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
    finally:
        temporary.unlink(missing_ok=True)


def _manifest(files):
    require(isinstance(files, list) and 2 <= len(files) <= 512, 'SNAPSHOT_MANIFEST_BOUND')
    result = {}
    for item in files:
        name = item.get('name')
        require(isinstance(name, str) and re.fullmatch(
            r'(wave_scope|checkpoint|stage_[0-9]+_(fit|train|test)|fold_[0-9]+|qstate_seal)\.json|trial_ledger\.jsonl|qstate_[0-5]_fit(_s[0-7])?_part_[0-9]{4}\.bin', name),
            'SNAPSHOT_FILENAME_REJECTED')
        require(name not in result, 'DUPLICATE_SNAPSHOT_FILENAME')
        require_hash(item.get('raw_sha256'))
        require_hash(item.get('blob_sha256'))
        require(type(item.get('raw_bytes')) is int and 0 < item['raw_bytes'] <= MAX_FILE,
                'SNAPSHOT_FILE_BOUND')
        result[name] = item
    return result


def _read_json(meta, load):
    raw = load(meta)
    require(isinstance(raw, bytes) and len(raw) == meta['raw_bytes']
            and digest(raw) == meta['raw_sha256'], 'IMMUTABLE_FILE_READBACK_MISMATCH')
    value = json.loads(raw)
    require(isinstance(value, dict), 'DOCUMENT_SHAPE_REJECTED')
    return value


def _training_selection(fold, sampled):
    eligible = []
    for rule in fold['fitted_rules']:
        metric = fold['train']['rules'][rule['rule_id']]
        contamination = (metric.get('exploratory_inverse_probability_estimates', {}).get(
            'conservative_contamination_ratio') if sampled else metric.get('conservative_contamination'))
        successes = metric.get('verified_successful_first_signals')
        require(type(successes) is int and successes >= 0, 'TRAINING_SUPPORT_REJECTED')
        require(contamination is None or (type(contamination) in (int, float)
                and math.isfinite(contamination) and 0 <= contamination <= 1), 'TRAINING_RATE_REJECTED')
        if successes >= 10 and contamination is not None and contamination < .05:
            eligible.append((-successes, contamination, len(rule['gates']), rule['rule_id']))
    return [item[3] for item in sorted(eligible)[:5]]


PAIR_DESIGN_VERSION = 'EQ20_FP02_DISJOINT_ATOMIC_PAIR_DESIGN_V1_20261004'
PAIR_RANK_SEED = 20261004
EXPECTED_PAIR_DESIGN_SHA256 = '859450ebf86ea9cb2c78735b80476c3a3b53a9a3222ac1991f307e02bd8db51b'
FAMILY_QUOTAS = {'EARNINGS': 100, 'FUNDAMENTAL': 500, 'POSITIONING': 150, 'SEC_EVENT': 250}


def semantic_pair(gates):
    """Order-independent conjunction identity; renaming IDs cannot evade exclusion."""
    require(isinstance(gates, list) and len(gates) == 2, 'FP02_EXACT_TWO_ATOMIC_GATES_REQUIRED')
    return sorted(gates, key=canonical_bytes)


def generate_template_design(scope):
    """Pure metadata derivation. No fitted thresholds, labels, or results are inputs.

    Only atomic gates actually present in the frozen W10 template receipt are
    recombined. Thus even an otherwise permitted but previously unused quantile
    or operator is not introduced by this successor design.
    """
    require(scope.get('template_count') == 1000 and len(scope.get('templates', [])) == 1000
            and scope.get('family_quotas') == FAMILY_QUOTAS
            and scope.get('conditions_per_template') == 2
            and scope.get('maximum_total_conditions') == 2,
            'FP02_ORIGINAL_PAIR_GRAMMAR_REQUIRED')
    technical = set(scope['technical_features'])
    source = scope['feature_grammar']
    require(len(technical) == 11 and len(source) == 106 and not technical.intersection(source),
            'FP02_ORIGINAL_FEATURE_FAMILIES_REQUIRED')
    source_gates = {}; technical_gates = {}; used = set(); original_ids = set()
    original_quotas = {name: 0 for name in FAMILY_QUOTAS}
    for item in scope['templates']:
        require(set(item) == {'economic_conditions', 'gates', 'group', 'rule_id', 'temporal_conditions'}
                and item['economic_conditions'] == 2 and item['temporal_conditions'] == 0
                and isinstance(item['rule_id'], str) and item['rule_id'].startswith('W10_')
                and item['rule_id'] not in original_ids, 'FP02_ORIGINAL_TEMPLATE_SCHEMA_REQUIRED')
        original_ids.add(item['rule_id'])
        gates = semantic_pair(item['gates'])
        canonical = canonical_bytes(gates)
        require(canonical not in used, 'FP02_ORIGINAL_SEMANTIC_DUPLICATE')
        used.add(canonical)
        src = [gate for gate in gates if gate.get('feature') in source]
        tech = [gate for gate in gates if gate.get('feature') in technical]
        require(len(src) == len(tech) == 1 and source[src[0]['feature']]['group'] == item['group'],
                'FP02_ORIGINAL_SOURCE_TECHNICAL_PAIR_REQUIRED')
        original_quotas[item['group']] += 1
        for gate in gates:
            require(set(gate) in ({'feature', 'operator', 'quantile'},
                                 {'feature', 'operator', 'fixed_threshold'})
                    and gate['operator'] in ('>=', '<='), 'FP02_FROZEN_ATOMIC_GATE_SCHEMA')
            if 'quantile' in gate:
                require(type(gate['quantile']) in (float, int) and math.isfinite(gate['quantile'])
                        and gate['quantile'] in scope['quantiles'], 'FP02_FROZEN_QUANTILE_REQUIRED')
            else:
                require(gate['feature'] in source and source[gate['feature']]['kind'] == 'POSITIVE_OBSERVED_STATE'
                        and gate['fixed_threshold'] == 1 and gate['operator'] == '>=',
                        'FP02_POSITIVE_OBSERVED_STATE_ONLY')
        source_gates[canonical_bytes(src[0])] = src[0]
        technical_gates[canonical_bytes(tech[0])] = tech[0]
    require(original_quotas == FAMILY_QUOTAS, 'FP02_ORIGINAL_QUOTA_ACCOUNTING')
    candidates = {name: [] for name in FAMILY_QUOTAS}
    for source_key, src in sorted(source_gates.items()):
        group = source[src['feature']]['group']
        for technical_key, tech in sorted(technical_gates.items()):
            gates = semantic_pair([src, tech]); raw = canonical_bytes(gates)
            if raw in used:
                continue
            semantic_sha = digest(raw)
            rank_sha = object_hash(dict(version=PAIR_DESIGN_VERSION, seed=PAIR_RANK_SEED,
                                        group=group, gates=gates))
            candidates[group].append((rank_sha, semantic_sha, gates))
    selected = []; ranks = []
    for group, quota in sorted(FAMILY_QUOTAS.items()):
        require(len(candidates[group]) >= quota, 'FP02_DISJOINT_FAMILY_EXHAUSTED')
        for rank_sha, semantic_sha, gates in sorted(candidates[group])[:quota]:
            rule_id = 'FP02_' + semantic_sha[:24]
            selected.append(dict(economic_conditions=2, gates=gates, group=group,
                                 rule_id=rule_id, temporal_conditions=0))
            ranks.append(dict(rule_id=rule_id, rank_sha256=rank_sha, semantic_pair_sha256=semantic_sha))
    require(len(selected) == len({x['rule_id'] for x in selected}) == 1000,
            'FP02_NEW_TRIAL_NAMESPACE_COLLISION')
    selected.sort(key=lambda x: x['rule_id']); ranks.sort(key=lambda x: x['rule_id'])
    atomic_library = dict(source=[source_gates[key] for key in sorted(source_gates)],
                          technical=[technical_gates[key] for key in sorted(technical_gates)])
    return dict(version=PAIR_DESIGN_VERSION, wave='FP02', original_w10_scope_sha256=SCOPE_SHA256,
        original_w10_template_family_sha256=scope['template_family_sha256'],
        original_atomic_gate_library_sha256=object_hash(atomic_library),
        original_source_atomic_gates=len(source_gates), original_technical_atomic_gates=len(technical_gates),
        source_feature_count=106, technical_feature_count=11, seed=PAIR_RANK_SEED,
        rank_rule='SHA256_CANONICAL_VERSION_SEED_FAMILY_COMMUTATIVE_ATOMIC_PAIR_THEN_SEMANTIC_SHA256',
        family_quotas=FAMILY_QUOTAS, unused_pair_counts={key: len(value) for key, value in candidates.items()},
        unused_pairs=sum(map(len, candidates.values())), template_count=1000,
        templates=selected, ranking_receipts=ranks, template_family_sha256=object_hash(selected),
        selection_rank_receipt_sha256=object_hash(ranks),
        no_new_atomic_gate_or_threshold=True, semantic_overlap_with_w10_or_fp01=0,
        activation_trigger='IMMUTABLE_VERIFIED_FP01_FINAL_ACCOUNTING_CANDIDATE_COUNT_ZERO',
        research_mode='FULL_STREAM', development_dates=['2025-09-01', '2026-05-31'],
        contexts=6, maximum_fit_records=6000, maximum_governed_seconds=7200,
        shared_campaign_governed_ceiling_seconds=172800, independent_resource_authority_required=True,
        candidate_selection='UNCHANGED_TRAIN_ONLY_SELECTION_WITH_REGISTERED_DISJOINT_FP02_TEMPLATES',
        confirmation_evidence_access_allowed=False, protected_outcomes_accessed=False,
        historical_development_exposure_preserved=True, retrospective_prelabel_claim=False,
        prior_campaign_lineage='ALL_W01_THROUGH_W09_PLUS_IMMUTABLE_W10_AND_FP01_TRIAL_LINEAGE_REQUIRED',
        w10_fp01_fp02_sublineage_maximum_fits=18000,
        sublineage_count_is_not_total_historical_campaign_count=True,
        multiplicity_control='FULL_HISTORICAL_CANDIDATE_SELECTION_LINEAGE_AND_SEPARATELY_PREREGISTERED_CONFIRMATION_ALPHA',
        zero_candidate_state='FINITE_REGISTERED_SEARCH_EXHAUSTED_NO_CANDIDATE',
        research_objective_achieved=False)


def registered_fp02_templates(scope_path, features, registered, bound):
    require(digest(Path(scope_path).read_bytes()) == SCOPE_SHA256, 'FROZEN_W10_SCOPE_BYTES_REQUIRED')
    scope = json.loads(Path(scope_path).read_bytes())
    design = registered.get('fp02_template_contract')
    # The full 57,300-pair derivation is independently reproduced at design
    # registration. Each bounded fit segment validates that exact immutable
    # result without spending another generation on unchanged metadata.
    require(isinstance(design, dict) and object_hash(design) == EXPECTED_PAIR_DESIGN_SHA256
            and EXPECTED_PAIR_DESIGN_SHA256 == registered.get('fp02_template_contract_sha256'),
            'EXACT_PRE_REGISTERED_DISJOINT_FP02_TEMPLATE_DESIGN_REQUIRED')
    require(registered.get('fp02_template_compiler_sha256') == digest(Path(__file__).read_bytes()),
            'REGISTERED_FP02_TEMPLATE_COMPILER_PIN_REQUIRED')
    allowed = set(features); result = []; quantiles = set()
    for item in design['templates']:
        gates = []
        for gate in item['gates']:
            require(gate['feature'] in allowed, 'FP02_UNBOUND_FEATURE_REJECTED')
            options = dict(feature=gate['feature'], operator={'>=': 'ge', '<=': 'le'}[gate['operator']])
            if 'quantile' in gate:
                options['quantile'] = float(gate['quantile']); quantiles.add(options['quantile'])
            else:
                options['fixed_threshold'] = float(gate['fixed_threshold'])
            gates.append(bound.ScopeGateTemplate(**options))
        result.append(bound.base.RuleTemplate(item['rule_id'], tuple(gates)))
    return scope, result, sorted(quantiles)


def reconcile_population_days(days, expected_dates, source_manifest_sha256):
    """Reconcile the natural denominator; missing inputs remain in it."""
    require_hash(source_manifest_sha256)
    require(expected_dates and len(set(expected_dates)) == len(expected_dates), 'CALENDAR_RECONCILIATION_REQUIRED')
    require(len(days) == len(expected_dates), 'FULL_POPULATION_DATES_PENDING')
    by_date = {item['session_date']: item for item in days}
    require(len(by_date) == len(days) and set(by_date) == set(expected_dates), 'POPULATION_DATE_MISMATCH')
    population = decisions = missing = unresolved = unknown_types = 0
    hashes = []
    for day in sorted(days, key=lambda item: item['session_date']):
        require(day.get('source_manifest_sha256') == source_manifest_sha256,
                'POPULATION_SOURCE_PIN_CHANGED')
        require(day.get('reference_complete') is True, 'REFERENCE_DENOMINATOR_INCOMPLETE')
        for key in ('security_sessions', 'candidate_decisions', 'missing_raw_sessions',
                    'unresolved_identity_sessions', 'unknown_security_type_sessions'):
            require(type(day.get(key)) is int and day[key] >= 0, 'POPULATION_COUNT_REJECTED')
        require(day['security_sessions'] > 0 and day['candidate_decisions'] > 0,
                'EMPTY_POPULATION_DAY')
        require(day['missing_raw_sessions'] <= day['security_sessions']
                and day['unresolved_identity_sessions'] <= day['security_sessions']
                and day['unknown_security_type_sessions'] <= day['security_sessions'],
                'POPULATION_SUBCOUNT_REJECTED')
        require_hash(day.get('population_day_sha256'))
        hashes.append([day['session_date'], day['population_day_sha256']])
        population += day['security_sessions']; decisions += day['candidate_decisions']
        missing += day['missing_raw_sessions']; unresolved += day['unresolved_identity_sessions']
        unknown_types += day['unknown_security_type_sessions']
    reasons = []
    if missing:
        reasons.append('FULL_WINDOW_RAW_SOURCE_CERTIFICATION_PENDING')
    if unresolved or unknown_types:
        reasons.append('POINT_IN_TIME_IDENTITY_OR_SECURITY_TYPE_CERTIFICATION_PENDING')
    if not all(day.get('source_semantics_certified') is True for day in days):
        reasons.append('SOURCE_SEMANTICS_CERTIFICATION_PENDING')
    if not all(day.get('publication_replay_certified') is True for day in days):
        reasons.append('EXECUTABLE_PUBLICATION_REPLAY_PENDING')
    return dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY' if reasons else 'VERIFIED',
                version=VERSION, stage='FP02_FULL_POPULATION_READINESS',
                dates=len(days), security_sessions=population, candidate_decisions=decisions,
                missing_raw_sessions=missing, unresolved_identity_sessions=unresolved,
                unknown_security_type_sessions=unknown_types, denominator_reduced=False,
                population_manifest_sha256=object_hash(hashes), source_manifest_sha256=source_manifest_sha256,
                reason_codes=reasons, source_values_or_outcomes_read=False)


def run_registered_full_population_stage(bound, inputs, output, scope_path, registered,
                                         *, resume=False, checkpoint_sessions=1,
                                         cpu_lease_seconds=12, hook=None):
    """Reuse verified private stage primitives under a new immutable FP02 scope.

    This is an actual six-context runner, not a callback placeholder. Its caller
    must supply the *separately reviewed and resource-reserved* certified adapter.
    The original W10 outputs, hashes and configured runner are never edited.
    """
    require(registered.get('wave') == 'FP02' and registered.get('research_mode') == 'FULL_STREAM',
            'REGISTERED_FP02_SCOPE_REQUIRED')
    require(registered.get('confirmation_or_holdout_access_allowed') is False,
            'PROTECTED_PERIOD_ENGINE_REJECTED')
    require(digest(Path(bound.__file__).read_bytes()) == BOUND_ENGINE_SHA256
            and digest(Path(bound.base.__file__).read_bytes()) == ENGINE_SHA256,
            'EXACT_PRIVATE_ENGINE_REQUIRED')
    require(digest(Path(scope_path).read_bytes()) == SCOPE_SHA256, 'FROZEN_TEMPLATE_SCOPE_CHANGED')
    require(inputs.research_mode == bound.base.FULL_STREAM and inputs.manifest.get('sampling_design') is None,
            'FULL_NATURAL_STREAM_REQUIRED')
    require(inputs.discovery_start == '2025-09-01' and inputs.discovery_end == '2026-05-31',
            'PROTECTED_OR_REDEFINED_DISCOVERY_WINDOW_REJECTED')
    require(inputs.manifest_sha256 == registered['source_manifest_sha256']
            and inputs.manifest.get('population_manifest_sha256') == registered['population_manifest_sha256'],
            'FULL_POPULATION_INPUT_BINDING_REJECTED')
    require(inputs.budget.maximum_cpu_seconds == registered['maximum_cpu_seconds'],
            'SUCCESSOR_BUDGET_BINDING_REJECTED')
    require(getattr(inputs, 'source_adapter_sha256', None) == registered['source_adapter_sha256'],
            'SUCCESSOR_ADAPTER_BINDING_REJECTED')
    require(getattr(inputs, 'resource_reservation_verified', False) is True
            and getattr(inputs, 'population_certification_verified', False) is True,
            'SUCCESSOR_EXECUTION_GATES_CLOSED')
    require(1 <= checkpoint_sessions <= 32 and 1 <= cpu_lease_seconds <= 12,
            'BOUNDED_SUCCESSOR_SLICE_REQUIRED')
    output = Path(output)
    require(output.name.startswith('fp01_') and 'execution' not in output.parts,
            'SEPARATE_SUCCESSOR_OUTPUT_REQUIRED')
    if not resume:
        output.mkdir(parents=True, exist_ok=False)
    require(output.is_dir() and not output.is_symlink(), 'SUCCESSOR_OUTPUT_REJECTED')
    inputs.unchanged()
    policy = getattr(inputs, 'frozen_source_policy_module', None)
    registry = getattr(inputs, '_w10_source_registry', None)
    require(policy is not None and registry is not None
            and digest(Path(policy.__file__).read_bytes()) == FROZEN_POLICY_SHA256
            and registered.get('source_policy_sha256') == FROZEN_POLICY_SHA256,
            'FROZEN_SOURCE_UNIT_SEMANTICS_REQUIRED')
    source_scope = json.loads(Path(scope_path).read_bytes())
    require(getattr(registry, 'scope', None) == source_scope,
            'CERTIFIED_FULL_POPULATION_SOURCE_REGISTRY_REQUIRED')
    # The unchanged base engine alone would use a different specialist quantile
    # unit. Install the exact reviewed source-unit/sharding/no-union policy too.
    installed = getattr(bound.base, '_eq20_fp01_source_registry', None)
    if installed is None:
        require(not hasattr(bound.base, '_w10_original_evaluate_masks'),
                'FRESH_PRIVATE_ENGINE_PROCESS_REQUIRED')
        policy.install(bound.base, source_scope, registry)
        # The exact W10 policy retains every distinct source unit in RAM. FP02
        # replaces that storage with a separately registered equivalent ledger.
        external_path = Path(__file__).with_name('eq20_fp01_external_quantiles.py')
        require(external_path.is_file() and digest(external_path.read_bytes()) ==
                require_hash(registered.get('external_quantiles_sha256')),
                'REGISTERED_EXTERNAL_SOURCE_UNIT_ACCUMULATOR_REQUIRED')
        external_spec = importlib.util.spec_from_file_location('eq20_registered_external_quantiles', external_path)
        external = importlib.util.module_from_spec(external_spec)
        sys.modules[external_spec.name] = external
        external_spec.loader.exec_module(external)
        require(getattr(external, 'VERSION', None) == 'EQ20_FP01_EXTERNAL_SOURCE_QUANTILES_V1',
                'EXTERNAL_SOURCE_UNIT_API_REQUIRED')
        external.release_snapshot_seal(output, verified_readbacks=getattr(inputs, 'fp01_verified_readbacks', None))
        def finalization_guard():
            inputs.budget.check()
            (hook or (lambda *unused: None))('after_quantile_field', {})
        external.install_external_quantiles(bound.base, source_scope, registry, output,
            source_policy=policy, scratch_guard=source_guards().scratch_safe,
            yield_guard=finalization_guard)
        bound.base._eq20_fp01_source_registry = registry
    else:
        require(installed is registry, 'SUCCESSOR_REGISTRY_REBINDING_FORBIDDEN')
    _, templates, quantiles = registered_fp02_templates(scope_path, inputs.features, registered, bound)
    wave_scope = dict(registered, source_input_manifest_sha256=inputs.manifest_sha256,
                      registered_templates=[item.rule_id for item in templates],
                      evidence='FULL_POPULATION_DEVELOPMENT_ONLY', checkpoint_schema=3)
    scope_hash = object_hash(wave_scope)
    with bound.base.exclusive_run_lock(output):
        if resume:
            require(json.loads((output / 'wave_scope.json').read_bytes()) == wave_scope,
                    'SUCCESSOR_SCOPE_RESUME_MISMATCH')
            cp = bound.base.read_checkpoint(output / 'checkpoint.json')
            require(cp['wave_scope_sha256'] == scope_hash, 'SUCCESSOR_CHECKPOINT_BINDING_REJECTED')
            for item in cp['stage_commits'].values():
                require(bound.base.file_sha256(output / item['path']) == item['sha256'],
                        'SUCCESSOR_COMMITTED_STAGE_CHANGED')
            for key, value in cp['fold_sha256'].items():
                require(bound.base.file_sha256(output / f'fold_{key}.json') == value,
                        'SUCCESSOR_COMMITTED_FOLD_CHANGED')
            if cp['state'] == 'FP02_FULL_POPULATION_DEVELOPMENT_COMPLETE':
                require(bound.base.file_sha256(output / 'trial_ledger.jsonl') == cp['trial_ledger_sha256'],
                        'SUCCESSOR_COMPLETED_LEDGER_CHANGED')
                return cp
            inputs.budget.charged_cpu_seconds = cp['cpu_charged_seconds']
            inputs.budget.start_cpu = time.process_time()
            cp['resume_count'] += 1
        else:
            bound.base.immutable_json(output / 'wave_scope.json', wave_scope)
            cp = dict(state='FP02_DEVELOPMENT_IN_PROGRESS', wave_scope_sha256=scope_hash,
                      checkpoint_schema=3, completed_folds=[], fold_sha256={}, stage_commits={},
                      active_stage=None, trial_records=0, protected_outcomes_accessed=False,
                      cpu_charged_seconds=0.0, resume_count=0)
        bound.base.write_checkpoint(output / 'checkpoint.json', cp)
        try:
            result = bound._run_w10_locked(inputs, output, cp, templates, quantiles, 8192,
                                          checkpoint_sessions, cpu_lease_seconds, hook or (lambda *unused: None))
        except BaseException as exc:
            durable = bound.base.read_checkpoint(output / 'checkpoint.json')
            external = sys.modules.get('eq20_registered_external_quantiles')
            if external is not None:
                external.refresh_checkpoint_external_progress(durable, output)
            durable['state'] = ('FP02_DEVELOPMENT_YIELDED' if isinstance(exc, RegisteredSliceYield)
                                else 'FP02_OPERATIONALLY_BLOCKED')
            durable['cpu_charged_seconds'] = max(durable['cpu_charged_seconds'], inputs.budget.consumed())
            durable['error_type'] = type(exc).__name__
            bound.base.write_checkpoint(output / 'checkpoint.json', durable)
            raise
        # This is the new FP02 directory and scope; frozen W10 history is untouched.
        require(result['trial_records'] == 6000 and result['completed_folds'] == list(range(6)),
                'SUCCESSOR_FINAL_ACCOUNTING_REJECTED')
        result['state'] = 'FP02_FULL_POPULATION_DEVELOPMENT_COMPLETE'
        result['research_objective_achieved'] = False
        bound.base.write_checkpoint(output / 'checkpoint.json', result)
        return result


def execute_fp01_segment(job, owner, attempt):
    """Execute the registered engine once a real, independently reviewed adapter exists.

    The adapter API is limited to installing/reading certified inputs and
    restoring/committing immutable checkpoints. Selection and fitting always
    run through the exact existing engine above. No outcome preview is used to
    decide whether the release is eligible; the database resolves all proofs.
    """
    job = dict(job, _rpc_owner=owner, attempt_id=attempt)
    binding = job.get('registration', {})
    registered = binding.get('contract', {})
    adapter_spec = binding.get('source_adapter', {})
    require(job.get('release_verified') is True and job.get('resource_reservation_verified') is True,
            'DATABASE_VERIFIED_FP02_RELEASE_REQUIRED')
    require(registered.get('implementation_sha256') == digest(Path(__file__).read_bytes()),
            'FP02_REGISTERED_IMPLEMENTATION_MISMATCH')
    require(job.get('reserved_cpu_seconds') == 30 and registered.get('maximum_cpu_seconds') == 7200,
            'FINITE_FP02_SEGMENT_OR_STAGE_LIMIT_REQUIRED')
    require(adapter_spec.get('api_version') == 'EQ20_FP01_ADAPTER_V2', 'FP02_ADAPTER_API_REQUIRED')
    source = adapter_spec.get('content_utf8', '').encode()
    require(0 < len(source) <= 1048576 and len(source) == adapter_spec.get('bytes')
            and digest(source) == adapter_spec.get('sha256') == registered.get('source_adapter_sha256'),
            'FP02_REGISTERED_ADAPTER_BYTES_MISMATCH')
    path = ROOT / ('adapter_' + adapter_spec['sha256'] + '.py')
    atomic_file(path, source)
    spec = importlib.util.spec_from_file_location('eq20_registered_fp01_adapter', path)
    adapter = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = adapter
    spec.loader.exec_module(adapter)
    adapter._MISSION = sys.modules[__name__]
    require(getattr(adapter, 'API_VERSION', None) == 'EQ20_FP01_ADAPTER_V2', 'FP02_ADAPTER_API_MISMATCH')
    for method in ('load_certified_inputs', 'restore_checkpoint', 'commit_checkpoint'):
        require(callable(getattr(adapter, method, None)), 'FP02_DURABLE_ADAPTER_API_INCOMPLETE')
    output = ROOT / ('fp01_' + job['release_artifact_sha256'])
    resume = adapter.restore_checkpoint(output, job)
    verified_readbacks = None
    require(type(resume) is bool and resume == bool(job.get('previous_snapshot')),
            'FP02_DURABLE_RESTORE_BINDING_REQUIRED')
    if resume:
        evidence = job['previous_snapshot']['evidence']
        require(evidence.get('release_artifact_sha256') == job['release_artifact_sha256']
                and evidence.get('protected_outcomes_accessed') is False,
                'FP02_PREVIOUS_SNAPSHOT_RELEASE_MISMATCH')
        _manifest(evidence.get('files'))
        require(callable(getattr(adapter, 'verify_restored_snapshot', None)),
                'FP02_ACTUAL_RESTORED_SNAPSHOT_VERIFIER_REQUIRED')
        verified_readbacks = adapter.verify_restored_snapshot(output, evidence)
    prepared = adapter.load_certified_inputs(job, ROOT)
    require(isinstance(prepared, dict), 'FP02_CERTIFIED_INPUT_ADAPTER_SHAPE')
    bound, inputs, scope_path = prepared['bound'], prepared['inputs'], prepared['scope_path']
    inputs.fp01_verified_readbacks = verified_readbacks
    prior_checkpoint = (bound.base.read_checkpoint(output / 'checkpoint.json') if resume else {})
    began = time.monotonic()
    def checkpoint_hook(event, state):
        if event in ('after_session_checkpoint', 'after_fold_checkpoint', 'after_ledger_rebuild', 'after_quantile_field'):
            if time.monotonic() - began >= 1.5 or time.monotonic() >= _ACCOUNT_WORK_DEADLINE - 2.3:
                raise RegisteredSliceYield()
    try:
        checkpoint = run_registered_full_population_stage(bound, inputs, output, scope_path, registered,
            resume=resume, checkpoint_sessions=1, cpu_lease_seconds=1, hook=checkpoint_hook)
        finished = True
    except RegisteredSliceYield:
        checkpoint = bound.base.read_checkpoint(output / 'checkpoint.json')
        checkpoint['state'] = 'FP02_DEVELOPMENT_YIELDED'
        bound.base.write_checkpoint(output / 'checkpoint.json', checkpoint)
        finished = False
    except bound.base.BudgetExceeded as exc:
        # Preserve actual resource stops. Only the original engine's exact
        # bounded chunk expiry with *advancing committed work* is resumable.
        if str(exc) != 'Uncommitted CPU lease expired; resume from last complete session checkpoint':
            raise
        checkpoint = bound.base.read_checkpoint(output / 'checkpoint.json')
        require(committed_work(checkpoint) != committed_work(prior_checkpoint),
                'FP02_CPU_YIELD_WITHOUT_COMMITTED_PROGRESS')
        checkpoint['state'] = 'FP02_DEVELOPMENT_YIELDED'
        bound.base.write_checkpoint(output / 'checkpoint.json', checkpoint)
        finished = False
    accounting = None
    if finished:
        prior_state = job.get('previous_snapshot', {}).get('evidence', {}).get('checkpoint', {}).get('state') if job.get('previous_snapshot') else None
        if prior_state == 'FP02_FULL_POPULATION_DEVELOPMENT_COMPLETE':
            accounting = verify_fp01_terminal_output(output, checkpoint, inputs, registered)
            job['final_accounting'] = accounting
        else:
            # Commit the completed scientific checkpoint first. A separately
            # bounded following segment reconstructs its entire immutable ledger.
            finished = False
    # The reviewed adapter must persist and read back the actual immutable
    # snapshot. SQL independently resolves this artifact before any transition.
    if accounting is None:
        saved = adapter.commit_checkpoint(output, job)
    else:
        # Final accounting certifies the already committed complete snapshot.
        # It adds an immutable receipt, not another CPU-only snapshot version.
        prior = job['previous_snapshot']
        saved = dict(status='READBACK_VERIFIED', artifact_key=prior['artifact_key'],
            artifact_sha256=prior['implementation_sha256'],
            checkpoint_sha256=prior['evidence']['checkpoint_sha256'])
    checkpoint_sha = object_hash({key: value for key, value in checkpoint.items() if key != 'checkpoint_sha256'})
    require(isinstance(saved, dict) and saved.get('status') == 'READBACK_VERIFIED'
            and isinstance(saved.get('artifact_key'), str), 'FP02_DURABLE_COMMIT_REQUIRED')
    require_hash(saved.get('artifact_sha256'), 'FP02_DURABLE_COMMIT_HASH_REQUIRED')
    require(saved.get('checkpoint_sha256') == checkpoint_sha,
            'FP02_DURABLE_COMMIT_CHECKPOINT_MISMATCH')
    return dict(state='VERIFIED' if finished else 'RUNNING', action='RUN_FP02',
        snapshot_artifact_key=saved['artifact_key'], snapshot_artifact_sha256=saved['artifact_sha256'],
        checkpoint_sha256=checkpoint_sha,
        release_artifact_sha256=job['release_artifact_sha256'],
        fits=checkpoint['trial_records'], templates=1000, contexts=len(checkpoint['completed_folds']),
        final_accounting_complete=finished, final_accounting=accounting, protected_outcomes_accessed=False,
        research_objective_achieved=False)



def verify_fp01_terminal_output(output, checkpoint, inputs, registered):
    """Rebuild all full-population trial records and freeze train-only candidates."""
    output = Path(output)
    require(checkpoint.get('checkpoint_sha256') == object_hash({
        key: value for key, value in checkpoint.items() if key != 'checkpoint_sha256'}),
        'FP02_FINAL_CHECKPOINT_SELF_HASH_REQUIRED')
    require(checkpoint.get('state') == 'FP02_FULL_POPULATION_DEVELOPMENT_COMPLETE'
            and checkpoint.get('completed_folds') == list(range(6))
            and checkpoint.get('trial_records') == 6000
            and checkpoint.get('active_stage') is None
            and checkpoint.get('protected_outcomes_accessed') is False,
            'FP02_ACTUAL_COMPLETE_CHECKPOINT_REQUIRED')
    scope_file = output / 'wave_scope.json'
    require(scope_file.is_file() and not scope_file.is_symlink()
            and scope_file.stat().st_size <= MAX_FILE, 'FP02_FINAL_SCOPE_FILE_BOUND')
    scope = json.loads(scope_file.read_bytes())
    scope_sha = object_hash(scope)
    require(scope_sha == checkpoint['wave_scope_sha256'] and scope.get('wave') == 'FP02'
            and scope.get('research_mode') == 'FULL_STREAM'
            and scope.get('source_input_manifest_sha256') == inputs.manifest_sha256
            and scope.get('template_source_scope_sha256') == SCOPE_SHA256,
            'FP02_FINAL_SCOPE_BINDING_REQUIRED')
    ids = scope.get('registered_templates', [])
    require(isinstance(ids, list) and len(ids) == len(set(ids)) == 1000,
            'FP02_FINAL_TEMPLATE_ACCOUNTING_REQUIRED')
    ids = set(ids)
    design = registered.get('fp02_template_contract')
    require(isinstance(design, dict) and object_hash(design) == EXPECTED_PAIR_DESIGN_SHA256
            and ids == {row['rule_id'] for row in design['templates']},
            'FP02_FINAL_DISJOINT_TEMPLATE_FAMILY_BINDING_REQUIRED')
    windows = list(inputs.contract['inner_folds']) + [dict(train_start='2025-09-01',
        train_end='2026-05-31', test_start=None, test_end=None)]
    require(len(windows) == 6, 'FP02_ORIGINAL_FOLD_WINDOWS_REQUIRED')
    expected = {f'{index}_{kind}' for index in range(6)
                for kind in (('fit', 'train', 'test') if index < 5 else ('fit', 'train'))}
    require(set(checkpoint.get('stage_commits', {})) == expected, 'FP02_ALL_STAGE_RECEIPTS_REQUIRED')
    for key, entry in checkpoint['stage_commits'].items():
        path = output / ('stage_' + key + '.json')
        require(entry.get('path') == path.name and path.is_file() and not path.is_symlink()
                and path.stat().st_size <= MAX_FILE and digest(path.read_bytes()) == entry.get('sha256'),
                'FP02_STAGE_RECEIPT_CHANGED')
    ledger = hashlib.sha256(); fit_index = hashlib.sha256(); folds = []; frozen = []; union_ids = []
    for index, window in enumerate(windows):
        path = output / ('fold_%d.json' % index)
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= MAX_FILE,
                'FP02_FOLD_FILE_BOUND')
        raw = path.read_bytes(); sha = digest(raw)
        require(sha == checkpoint['fold_sha256'].get(str(index)), 'FP02_FINAL_FOLD_PIN_REQUIRED')
        fold = json.loads(raw)
        require(fold.get('fold_index') == index and fold.get('dates') == window
                and fold.get('wave_scope_sha256') == scope_sha, 'FP02_FINAL_FOLD_IDENTITY_REQUIRED')
        rules = fold.get('fitted_rules', [])
        require(len(rules) == 1000 and {rule.get('rule_id') for rule in rules} == ids
                and len({rule.get('rule_id') for rule in rules}) == len(rules)
                and set(fold.get('train', {}).get('rules', {})) == ids, 'FP02_ALL_FITS_REQUIRED')
        test = fold.get('forward_test')
        require((index == 5 and test is None) or (index < 5 and isinstance(test, dict)
                and set(test.get('rules', {})) == ids), 'FP02_ALL_FORWARD_FOLDS_REQUIRED')
        selected = _training_selection(fold, False)
        require(selected == fold.get('training_selected_ids'), 'FP02_TRAIN_ONLY_SELECTION_CHANGED')
        for rule in rules:
            rule_id = rule['rule_id']; fit_sha = object_hash(rule)
            trial = dict(wave_scope_sha256=scope_sha, fold_index=index, rule_id=rule_id,
                fit_sha256=fit_sha, train=fold['train']['rules'][rule_id],
                forward_test=test['rules'][rule_id] if test else None,
                selected_from_training=rule_id in selected, status='DEVELOPMENT_TRIAL_COMPLETE')
            ledger.update(canonical_bytes(trial) + b'\n')
            fit_index.update(canonical_bytes([index, rule_id, fit_sha]) + b'\n')
        folds.append(dict(context_id=index, fits=1000, fold_sha256=sha, training_selected_ids=selected))
        if index == 5:
            frozen = [rule for rule in rules if rule['rule_id'] in selected]
            union_ids = selected
    actual = output / 'trial_ledger.jsonl'
    require(actual.is_file() and not actual.is_symlink() and actual.stat().st_size <= MAX_FILE
            and ledger.hexdigest() == checkpoint.get('trial_ledger_sha256') == digest(actual.read_bytes()),
            'FP02_FULL_LEDGER_RECONSTRUCTION_FAILED')
    family = dict(rules=frozen, union_ids=union_ids)
    return dict(state='VERIFIED', fits=6000, templates=1000, contexts=6, forward_folds=5,
        trial_ledger_sha256=ledger.hexdigest(), fit_index_sha256=fit_index.hexdigest(),
        folds=folds, checkpoint_sha256=object_hash({k: v for k, v in checkpoint.items() if k != 'checkpoint_sha256'}),
        frozen_rules=frozen, union_ids=union_ids, candidate_count=len(frozen),
        candidate_family_sha256=object_hash(family), candidate_family_canonical_utf8=canonical_bytes(family).decode(),
        selection_evidence_class='DEVELOPMENT', selection_cutoff='2026-05-31',
        selection_rule='UNCHANGED_TRAIN_ONLY_SELECTION_WITH_REGISTERED_DISJOINT_FP02_TEMPLATES',
        original_contract_sha256=ORIGINAL_CONTRACT_SHA256, frozen_template_scope_sha256=SCOPE_SHA256,
        source_manifest_sha256=inputs.manifest_sha256,
        population_manifest_sha256=registered['population_manifest_sha256'],
        prior_w10_receipt_sha256=registered['immutable_parent_w10_receipt_sha256'],
        prior_fp01_receipt_sha256=registered['immutable_parent_fp01_receipt_sha256'],
        fp02_template_design_sha256=EXPECTED_PAIR_DESIGN_SHA256,
        candidate_freeze_state=('FROZEN_OUTPUT_AWAITING_PROSPECTIVE_BINDING' if frozen else
            'FINITE_AUTHORIZED_BATCH_COMPLETE_NO_CANDIDATE'),
        remaining_parameterized_atomic_pairs_after_batch=56300,
        grammar_exhausted=False,
        no_candidate_next_stage='FP03_FIXED_REGISTERED_DISJOINT_BATCH',
        protected_outcomes_accessed=False, post_freeze_changes=False, research_objective_achieved=False)


def committed_work(checkpoint):
    """CPU/resume/error metadata cannot masquerade as scientific progress."""
    checkpoint = checkpoint or {}
    active = checkpoint.get('active_stage') or {}
    return (tuple(checkpoint.get('completed_folds', ())), checkpoint.get('trial_records', 0),
            tuple(sorted(checkpoint.get('stage_commits', {}))), tuple(active.get('cursor') or ()),
            active.get('processed_sessions', 0), active.get('stream_exhausted', False),
            active.get('external_finalized_fields', 0))


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

    def direct_call(self, op, owner, args=None, *, transport_receipt=None):
        path=Path(__file__).with_name('eq20_rpc_admission_v3.py')
        require(digest(path.read_bytes())=='72fda15472338d0b49afc9ab342bfec4b91de743d76d2fd0c0b0293b1d91fdff',
                'PINNED_SERVER_ADMISSION_TRANSPORT_REQUIRED')
        spec=importlib.util.spec_from_file_location('eq20_registered_rpc_permit',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        return module.send(dict(p_op=op,p_owner=owner,p_args=dict(args or {})),
            target_rpc='public.'+RPC_NAME+'(text,text,jsonb)',post=self._post,
            canonical_bytes=canonical_bytes,host_instance=socket.gethostname(),
            boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            read_only=op=='status',transport_receipt=transport_receipt)

    def _post(self,rpc_name,body):
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
    # Legacy census keeps its fixed12: admission7 + bounded metadata query5.
    # Completed census receipts are reused; this creates no new census stage.
    control_limit=5 if op=='census' else CONTROL_GOVERNED_SECONDS
    helper_wall=1.2 if op=='census' else 2.1
    helper_parent_cpu=.25 if op=='census' else 1.0
    ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    key = uuid.uuid4().hex
    request_path = ROOT / ('rpc_' + key + '.json')
    response_path = ROOT / ('rpc_reply_' + key + '.json')
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
    value = {}
    try:
        while not child.reap():
            if time.monotonic() - started > helper_wall or time.process_time() - cpu_start > helper_parent_cpu:
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
            # The server-issued single-use permit admits for <=.5s; the
            # actual query bound is <=2s. No local clock extrapolation is used.
            until = time.monotonic() + SQL_TAIL_SECONDS
            while time.monotonic() < until:
                time.sleep(min(.05, until - time.monotonic()))
        _last_rpc_proof = dict(operation=op, request_sha256=digest(request_raw),
            attempt_id=args.get('attempt_id'), host_instance=socket.gethostname(),
            process_identity=child.original_identity, process_finished=bool(child.termination_proof),
            process_termination_proof=child.termination_proof, exit_code=child.exit_code,
            measured_child_cpu_seconds=child.cpu, bounded_governed_seconds=control_limit,
            measured_parent_cpu_seconds=time.process_time()-control_cpu_started,
            observed_control_elapsed_seconds=time.monotonic()-control_started,
            control_governed_prefix_seconds=(time.process_time()-control_cpu_started)+(child.cpu or 0)+(time.monotonic()-control_started),
            control_bound_exceeded=((time.process_time()-control_cpu_started)+(child.cpu or 0)+(time.monotonic()-control_started)>control_limit),
            transport_receipt=value.get('transport_receipt'),
            permitted_workload_only_tail=True, unknown_mint_may_remain_inert=not success,
            all_sql_requests_proven_terminal=success,
            proof_scope='EXACT_HTTP_HELPER_AND_BOUNDED_SQL_TAIL',
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
        request_path.unlink(missing_ok=True); response_path.unlink(missing_ok=True)


def read_terminal_file(rpc, owner, snapshot_id, meta):
    require(type(meta.get('chunks')) is int and 1 <= meta['chunks'] <= 256,
            'TERMINAL_BLOB_CHUNKS_REJECTED')
    cache = ROOT / 'account_cache' / snapshot_id
    require(re.fullmatch(r'[0-9a-f-]{36}', snapshot_id), 'SNAPSHOT_ID_FORMAT')
    cache.mkdir(parents=True, exist_ok=True, mode=0o700)
    cached = cache / meta['raw_sha256']
    if cached.is_file() and not cached.is_symlink():
        raw = cached.read_bytes()
        require(len(raw) == meta['raw_bytes'] and digest(raw) == meta['raw_sha256'], 'ACCOUNT_CACHE_PIN_MISMATCH')
        return raw
    encoded = bytearray()
    for number in range(meta['chunks']):
        piece_path = cache / (meta['blob_sha256'] + '_' + str(number) + '.part')
        if piece_path.is_file() and not piece_path.is_symlink():
            part = piece_path.read_bytes()
        else:
            if time.monotonic() >= _ACCOUNT_WORK_DEADLINE - 2.3:
                raise GateClosed('ACCOUNTING_COMMITTED_PREPARATION_YIELD')
            reply = rpc.call('terminal_blob', owner, dict(snapshot_id=snapshot_id,
                             blob_sha256=meta['blob_sha256'], part_no=number))
            part = base64.b64decode(reply['payload_base64'], validate=True)
            require(len(part) <= 262144 and digest(part) == reply['payload_sha256'],
                    'TERMINAL_BLOB_PART_MISMATCH')
            atomic_file(piece_path, part)
        encoded.extend(part)
        require(len(encoded) <= MAX_FILE, 'TERMINAL_BLOB_LIMIT')
    require(len(encoded) == meta['encoded_bytes'] and digest(encoded) == meta['blob_sha256'],
            'TERMINAL_BLOB_HASH_MISMATCH')
    decoder = zlib.decompressobj()
    raw = decoder.decompress(encoded, meta['raw_bytes'] + 1)
    require(decoder.eof and not decoder.unused_data and len(raw) == meta['raw_bytes']
            and digest(raw) == meta['raw_sha256'], 'TERMINAL_FILE_DECODE_MISMATCH')
    atomic_file(cached, raw)
    return raw


def supervise_once(rpc, owner, *, trigger='PERSISTENT_WORKER_TIMER', scheduled_at=None):
    """One bounded metadata transition. A heartbeat is never marked as science."""
    args = dict(wrapper_sha256=digest(Path(__file__).read_bytes()), version=VERSION,
                host_instance=socket.gethostname(), host_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(), trigger=trigger,
                invocation_id=uuid.uuid4().hex, scheduled_at=scheduled_at,
                prior_rpc_termination=_last_rpc_proof)
    result = rpc.call('tick', owner, args)
    if result.get('action') == 'RUN_FP02':
        invocation=result.get('invocation_id')
        require(isinstance(invocation,str) and re.fullmatch(r'[0-9a-f]{32}',invocation), 'MISSION_ADMITTED_INVOCATION_REQUIRED')
        atomic_file(ROOT/('admission_received_'+invocation+'.json'),canonical_bytes(dict(
            invocation_id=invocation,attempt_id=result['attempt_id'],host_instance=socket.gethostname())))
        return execute_reserved_child(rpc, owner, result)
    if result.get('action') == 'RECOVER_RECORDED_CHILD':
        return recover_recorded_attempt(rpc, result)
    if result.get('action') == 'REPLAY_RECORDED_RECOVERY_TERMINAL':
        return replay_recorded_recovery_terminal(rpc, result)
    return result


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
            'QA_ACTUAL_REQUEST_TIMEOUT_AND_SERVER_PERMIT_REQUIRED')
    require_hash(proof.get('artifact_sha256'),'QA_IMMUTABLE_REQUEST_BOUND_RECEIPT_REQUIRED')
    return proof


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


def evaluate_registered_evidence(contract, ledger, candidate, *, as_of=None):
    """The registry supplies verified bindings; this never grants outcome access."""
    from app.eq20_evidence_eligibility import evaluate_eligibility
    return evaluate_eligibility(contract, ledger, candidate, as_of=as_of)


_last_tick_monotonic = 0.0
_tick_lock = threading.Lock()


def timer_tick(owner, *, scheduled_at, trigger):
    """Sequential parent-timer dispatch; there is no additional scheduler/thread."""
    global _last_tick_monotonic
    if trigger not in ('PERSISTENT_WORKER_STARTUP', 'PERSISTENT_WORKER_TIMER'):
        raise GateClosed('OBSERVED_PERSISTENT_TRIGGER_REQUIRED')
    with _tick_lock:
        now = time.monotonic()
        if _last_tick_monotonic and now - _last_tick_monotonic < 300:
            return dict(throttled=True, state='IMPLEMENTED')
        _last_tick_monotonic = now
        return supervise_once(MissionRPC(), owner, trigger=trigger, scheduled_at=scheduled_at)


_ACCOUNT_WORK_DEADLINE = float('inf')


def _reserved_child(owner, attempt):
    global _DIRECT_RPC, _ACCOUNT_WORK_DEADLINE
    require(re.fullmatch(r'[0-9a-f-]{36}', attempt), 'ACCOUNTING_CHILD_ARGUMENT_REJECTED')
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.setitimer(signal.ITIMER_REAL, 6.5)
    signal.signal(signal.SIGPROF, signal.SIG_DFL)
    signal.setitimer(signal.ITIMER_PROF, 4.5)
    resource.setrlimit(resource.RLIMIT_CPU, (5, 5))
    resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
    resource.setrlimit(resource.RLIMIT_FSIZE, (256 * 1024 * 1024, 256 * 1024 * 1024))
    os.nice(10)
    source_guards().prohibit_descendants()
    _DIRECT_RPC = True
    _ACCOUNT_WORK_DEADLINE = time.monotonic() + 5.0
    job = json.loads((ROOT / ('job_' + attempt + '.json')).read_bytes())
    rpc = MissionRPC()
    try:
        if job['action'] == 'RUN_FP02':
            # The unchanged engine owns SIGPROF for its one-second scientific
            # chunk leases. Kernel CPU and wall limits still bound this process.
            signal.setitimer(signal.ITIMER_PROF, 0)
            resource.setrlimit(resource.RLIMIT_FSIZE, (256 * 1024 * 1024, 256 * 1024 * 1024))
            receipt = execute_fp01_segment(job, owner, attempt)
        else:
            raise GateClosed('REGISTERED_FP02_CHILD_ACTION_REQUIRED')
    except Exception as exc:
        message = str(exc)
        code = message if re.fullmatch(r'[A-Z0-9_]{1,200}', message) else type(exc).__name__
        cache = ROOT / 'account_cache' / job.get('snapshot', {}).get('snapshot_id', 'absent')
        progress = sorted((item.name, item.stat().st_size) for item in cache.iterdir()
                          if item.is_file() and not item.name.endswith('.tmp')) if cache.exists() else []
        receipt = dict(state='RUNNING' if code in ('ACCOUNTING_COMMITTED_PREPARATION_YIELD','FP01_COMMITTED_PREPARATION_YIELD') else 'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                       error=code, cache_acknowledgements=progress, cache_acknowledgements_sha256=object_hash(progress),
                       protected_outcomes_accessed=False)
        if code == 'FP01_COMMITTED_PREPARATION_YIELD':
            adapter = sys.modules.get('eq20_registered_fp01_adapter')
            require(adapter is not None and callable(getattr(adapter, 'preparation_progress', None)),
                    'FP02_VERIFIED_PREPARATION_LEDGER_REQUIRED')
            receipt.update(preparation_progress=adapter.preparation_progress(job, ROOT),
                           release_artifact_sha256=job['release_artifact_sha256'],
                           research_objective_achieved=False)
    atomic_file(ROOT / ('receipt_' + attempt + '.json'), canonical_bytes(receipt))
    return 0 if receipt['state'] in ('VERIFIED','RUNNING') else 1


def _rpc_helper(key):
    require(re.fullmatch(r'[0-9a-f]{32}', key), 'RPC_HELPER_ARGUMENT')
    signal.signal(signal.SIGALRM, signal.SIG_DFL); signal.setitimer(signal.ITIMER_REAL, 2.0)
    signal.signal(signal.SIGPROF, signal.SIG_DFL); signal.setitimer(signal.ITIMER_PROF, .5)
    source_guards().prohibit_descendants()
    request = json.loads((ROOT / ('rpc_' + key + '.json')).read_bytes())
    transport_receipt={}
    try:
        response = MissionRPC().direct_call(request['op'], request['owner'], request['args'],transport_receipt=transport_receipt)
        result = dict(success=True, response=response,transport_receipt=transport_receipt)
    except Exception as exc:
        message = str(exc)
        result = dict(success=False, error=message if re.fullmatch(r'[A-Z0-9_]{1,200}', message) else type(exc).__name__,transport_receipt=transport_receipt)
    with (ROOT / ('rpc_reply_' + key + '.json')).open('xb') as handle:
        handle.write(canonical_bytes(result)); handle.flush(); os.fsync(handle.fileno())
    return 0 if result['success'] else 1


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--rpc-helper':
        raise SystemExit(_rpc_helper(sys.argv[2]))
    if len(sys.argv) != 4 or sys.argv[1] != '--reserved-child':
        raise SystemExit('Only the bounded final-accounting child is permitted')
    raise SystemExit(_reserved_child(sys.argv[2], sys.argv[3]))
