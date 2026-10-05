"""Registered prospective feature production and exact-trade reference labels.

No market IO, fitting or live order submission occurs here. Private scientific
arithmetic is executed only from the authenticated producer-release readback.
An upstream admitted batch verifies its complete input capsule and dated natural
population; this module does not turn self-reported source coverage into a
source certificate. Feature construction and first-signal selection precede
any call to the separate reference/execution outcome builders.
"""
from __future__ import annotations

from bisect import bisect_right
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys
import types

VERSION = 'EQ20_PROSPECTIVE_FEATURE_PRODUCER_V1'
ARITHMETIC_VERSION = 'EQ20_PROSPECTIVE_PRIVATE_ARITHMETIC_V1'
RAW_SCHEMA = 'EQ20_PROSPECTIVE_RAW_SECURITY_SESSION_V1'
ENTRYPOINT = 'eq20_prospective_arithmetic_v1.py'
ALLOWED_FILES = {ENTRYPOINT, 'w10_exact_source_projection_v2.py', 'causal_engine.py', 'compact_discovery_inputs.py'}
KERNEL_SHA256 = '99077b64970ff02e6a6fcc9dc9b9da4f9411eef08bc396c51cf220f35f481987'
EXECUTION_SHA256 = '637336c4b11ae7f7151de9caac52c82ad359ffe5786b763f9224c92352e623e3'
MAX_CODE_BYTES = 1024*1024
MAX_SOURCE_ROWS = 200000
_CACHE = {}
_HELPERS = {}


class Closed(ValueError):
    pass


def require(value, reason):
    if not value:
        raise Closed(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def file_hash(raw):
    return hashlib.sha256(raw).hexdigest()


def time_at(value):
    require(isinstance(value, str), 'EXPLICIT_SOURCE_CLOCK_REQUIRED')
    value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(value.tzinfo is not None, 'EXPLICIT_SOURCE_TIMEZONE_REQUIRED')
    return value.astimezone(timezone.utc)


def helper(name, expected):
    path = Path(__file__).with_name(name+'.py')
    require(file_hash(path.read_bytes()) == expected, 'PRODUCER_COMPONENT_PIN_MISMATCH')
    if name not in _HELPERS:
        spec = importlib.util.spec_from_file_location('prospective_source_'+name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        _HELPERS[name] = module
    return _HELPERS[name]


def resolve_release(binding, contract):
    """Consume the actual private registry reply, not a capsule Boolean."""
    require(isinstance(binding, dict), 'ACTUAL_SOURCE_PRODUCER_BINDING_REQUIRED')
    ref, row = binding.get('reference'), binding.get('artifact')
    require(isinstance(ref, dict) and isinstance(row, dict)
            and ref.get('kind') == 'SUCCESSOR_PROSPECTIVE_SOURCE_PRODUCER_RELEASE'
            and ref.get('status') == 'VERIFIED_EXECUTABLE_RELEASE'
            and isinstance(ref.get('artifact_key'), str) and ref['artifact_key']
            and re.fullmatch('[0-9a-f]{64}', ref.get('implementation_sha256', '')) is not None
            and all(row.get(k) == ref.get(k) for k in ('kind', 'artifact_key', 'status', 'implementation_sha256')),
            'SOURCE_PRODUCER_REGISTRY_IDENTITY')
    raw = row.get('evidence_text')
    require(isinstance(raw, str) and len(raw.encode()) <= MAX_CODE_BYTES
            and file_hash(raw.encode()) == ref['implementation_sha256'], 'SOURCE_PRODUCER_READBACK_HASH')
    release = json.loads(raw)
    require(isinstance(release, dict) and release.get('version') == VERSION
            and release.get('entrypoint') == ENTRYPOINT
            and release.get('module_sha256') == file_hash(Path(__file__).read_bytes())
            and release.get('session_contract_sha256') == digest(contract), 'SOURCE_PRODUCER_SCOPE_AND_INSTALLED_PIN')
    kernel = helper('eq20_prospective_kernel', KERNEL_SHA256)
    kernel.family(contract)
    require(all(release.get(k) == v for k, v in kernel.scope(contract).items()), 'SOURCE_PRODUCER_FULL_CONTRACT_SCOPE')
    require(time_at(row.get('created_at')) < time_at(contract['official_sessions'][0]['open_at']),
            'SOURCE_PRODUCER_MUST_PRECEDE_RESERVED_SESSION')
    require(release.get('additional_paid_cost') == 0 and release.get('private_arithmetic_version') == ARITHMETIC_VERSION
            and release.get('feature_selection') == 'ONLY_FROZEN_CANDIDATE_GATE_FEATURES'
            and release.get('source_truth_verified_by_this_module') is False
            and release.get('reference_outcome_mode') == contract.get('reference_outcome_mode')
            and release.get('reference_outcome_mode') in ('ORIGINAL_CERTIFIED_MINUTE_BAR_COVERAGE_V1','EXACT_SUBSEQUENT_TRADE_V1'), 'PRODUCER_CAPABILITY_BOUNDARY')
    return release


def load_producer(contract, binding):
    release = resolve_release(binding, contract)
    manifest, files = release.get('code_files'), binding.get('source_files')
    require(isinstance(manifest, list) and len(manifest) == 4
            and all(isinstance(x, dict) and set(x) == {'name', 'sha256', 'artifact_reference'} for x in manifest), 'EXACT_PRIVATE_CODE_MANIFEST')
    names = [x['name'] for x in manifest]
    require(names == sorted(set(names)) and set(names) <= ALLOWED_FILES
            and set(names) == ALLOWED_FILES
            and isinstance(files, dict) and set(files) == set(names), 'PRIVATE_CODE_NAMES_REQUIRED')
    total = 0
    for item in manifest:
        text = files[item['name']]
        require(isinstance(text, str), 'PRIVATE_CODE_TEXT_REQUIRED')
        total += len(text.encode())
        require(total <= MAX_CODE_BYTES and file_hash(text.encode()) == item['sha256'], 'PRIVATE_SOURCE_BYTES_PIN')
    definitions = release.get('original_feature_definitions')
    require(isinstance(definitions, dict)
            and digest(definitions) == release.get('original_feature_definitions_sha256'), 'REGISTERED_ORIGINAL_FEATURE_DEFINITIONS')
    scope_sha = binding['reference']['implementation_sha256']
    if scope_sha not in _CACHE:
        # A separately registered private module, never a mutation of a loaded
        # W10 module or its frozen history. Content is authenticated above.
        module = types.ModuleType('eq20_prospective_arithmetic_'+scope_sha)
        module.__dict__['__file__'] = '<registered:'+ENTRYPOINT+'>'
        sys.modules[module.__name__] = module
        exec(compile(files[ENTRYPOINT], module.__file__, 'exec'), module.__dict__)
        require(getattr(module, 'VERSION', None) == ARITHMETIC_VERSION
                and callable(getattr(module, 'Producer', None)), 'REGISTERED_PRIVATE_ARITHMETIC_API')
        instance = module.Producer(contract, definitions, files)
        require(getattr(instance, 'session_contract_sha256', None) == digest(contract), 'PRIVATE_ARITHMETIC_SCOPE_PIN')
        _CACHE[scope_sha] = instance
    return _CACHE[scope_sha], release


def produce_features(contract, raw_session, producer_bindings):
    """Build only causal source features. No label/future-trade array is read."""
    require(isinstance(raw_session, dict) and raw_session.get('schema') == RAW_SCHEMA, 'EXACT_RAW_SOURCE_SESSION_SCHEMA')
    producer, release = load_producer(contract, producer_bindings)
    require(raw_session.get('session_contract_sha256') == digest(contract)
            and raw_session.get('session_date') in contract['official_session_dates']
            and isinstance(raw_session.get('security_id'), str) and raw_session['security_id'], 'RAW_SOURCE_SCOPE_AND_MEMBER')
    source = raw_session.get('source_inputs')
    require(isinstance(source, dict), 'RAW_SOURCE_INPUTS_REQUIRED')
    for name in ('facts', 'events', 'bars', 'short_interest_states', 'short_volume_states'):
        require(isinstance(source.get(name), list) and len(source[name]) <= MAX_SOURCE_ROWS
                and all(isinstance(r, dict) for r in source[name]), 'BOUNDED_RAW_SOURCE_FAMILY_ROWS')
    # The producer sees no quote/trade outcomes, precomputed labels or fitted
    # threshold data. Entire source streams remain bound by the upstream capsule.
    feature_input = {k: raw_session.get(k) for k in ('session_date', 'security_id', 'issuer_id', 'instrument_key',
        'issuer_cik', 'population_day_sha256', 'regular_open', 'regular_close', 'identity_disposition',
        'source_family_dispositions', 'source_capture', 'source_inputs')}
    result = producer.produce(feature_input)
    require(isinstance(result, dict) and isinstance(result.get('features'), dict)
            and isinstance(result.get('provenance'), dict), 'ACTUAL_FEATURE_PRODUCER_OUTPUT_REQUIRED')
    features = result['features']
    kernel = helper('eq20_prospective_kernel', KERNEL_SHA256)
    selected = kernel.select_first_signals(contract, features)
    require(features.get('session_date') == raw_session['session_date']
            and features.get('security_id') == raw_session['security_id'], 'PRODUCED_MEMBER_IDENTITY')
    require(result['provenance'].get('session_contract_sha256') == digest(contract)
            and result['provenance'].get('feature_names') == contract['feature_names']
            and result['provenance'].get('original_source_files_sha256') == digest([{k: item[k] for k in ('name', 'sha256')} for item in release['code_files']]),
            'ACTUAL_PRODUCER_PROVENANCE_CHAIN')
    return {'version': VERSION, 'features': features, 'feature_only_selection': selected,
            'provenance': result['provenance'],
            'producer_release_sha256': producer_bindings['reference']['implementation_sha256'],
            'verification_scope': 'DETERMINISTIC_SOURCE_PROJECTION_NOT_SOURCE_TRUTH_CERTIFICATE',
            'outcome_label_arrays_opened_by_feature_builder': False, 'thresholds_refitted': False}


def build_reference_labels(contract, features, market_payload, producer_bindings=None):
    """The preregistered A evidence mode is fixed before protected outcomes."""
    mode = contract.get('reference_outcome_mode')
    require(mode in ('ORIGINAL_CERTIFIED_MINUTE_BAR_COVERAGE_V1','EXACT_SUBSEQUENT_TRADE_V1'), 'FROZEN_REFERENCE_OUTCOME_MODE_REQUIRED')
    if mode == 'ORIGINAL_CERTIFIED_MINUTE_BAR_COVERAGE_V1':
        require(isinstance(market_payload, dict) and market_payload.get('schema') == RAW_SCHEMA
                and market_payload.get('session_contract_sha256') == digest(contract), 'RAW_REFERENCE_SOURCE_CONTRACT_SCOPE')
        producer, release = load_producer(contract, producer_bindings)
        kernel = helper('eq20_prospective_kernel', KERNEL_SHA256)
        kernel.select_first_signals(contract, features)
        result = producer.reference_labels(market_payload, features)
        require(isinstance(result, dict) and result.get('session_contract_sha256') == digest(contract)
                and result.get('reference_outcome_mode') == mode, 'ORIGINAL_REFERENCE_BUILDER_SCOPE')
        return result
    kernel = helper('eq20_prospective_kernel', KERNEL_SHA256)
    kernel.select_first_signals(contract, features)
    execution = helper('eq20_execution_replay', EXECUTION_SHA256)
    execution.need(execution.validate_policy(contract.get('execution_policy')) == contract.get('execution_policy_sha256'),
                   'REGISTERED_REFERENCE_SOURCE_EXECUTION_POLICY')
    market = execution.Market(market_payload, {'session_date': features['session_date'],
        'security_id': features['security_id']}, contract['execution_policy_sha256'])
    require(market.open == time_at(features['regular_open']) and market.close == time_at(features['regular_close']),
            'REFERENCE_SOURCE_OFFICIAL_SESSION_CLOCK')
    coverage = market.coverage
    causal_identity = isinstance(coverage, dict) and all(coverage.get(k) is True for k in (
        'timestamps_synchronized', 'trade_conditions_verified', 'identity_verified', 'as_traded_prices_verified'))
    complete = causal_identity and coverage.get('trades_complete') is True and coverage.get('gaps') == [] \
        and time_at(coverage.get('start_at')) <= market.open and time_at(coverage.get('end_at')) >= market.close \
        and not any(t['condition_state'] == 'UNRESOLVED' or (t['condition_state'] == 'REGULAR_ELIGIBLE' and t['size'] == 0) for t in market.trades)
    eligible = [t for t in market.trades if causal_identity and t['condition_state'] == 'REGULAR_ELIGIBLE' and t['size'] > 0]
    times = [t['ts'] for t in eligible]
    best = [None]*len(eligible)
    winner = None
    for i in range(len(eligible)-1, -1, -1):
        row = eligible[i]
        if winner is None or row['price'] >= winner['price']:
            winner = row
        best[i] = winner
    labels = []
    for row in features['decisions']:
        decision = time_at(row['decision_ts'])
        label = {'decision_ts': row['decision_ts'], 'state': 'UNRESOLVED', 'coverage_complete': complete,
            'ordering_ambiguity': False, 'target_observation_valid': False,
            'source_reason': 'EXACT_SUBSEQUENT_REGULAR_ELIGIBLE_TRADES_ONLY'}
        if row['reference_state'] == 'AVAILABLE' and causal_identity:
            at = bisect_right(times, decision)
            target = best[at] if at < len(best) else None
            threshold = Decimal(str(row['p_reference']))*Decimal('1.20')
            if target is not None and target['price'] >= threshold:
                label.update(state='QUALIFY', target_observation_valid=True,
                    target_interval_start=target['ts'].isoformat(), target_interval_end=target['ts'].isoformat(),
                    target_price=float(target['price']), target_source_id=target['source_id'],
                    reason_code='VALID_STRICT_POST_DECISION_TWENTY_PERCENT_TRADE')
            elif complete:
                label.update(state='NONQUALIFY', reason_code='COMPLETE_NO_VALID_POST_DECISION_TARGET')
        labels.append(label)
    return {'version': VERSION, 'reference_labels': labels, 'source_payload_sha256': digest(market_payload),
            'reference_anchor': 'AVAILABLE_COMPLETED_BAR_CLOSE', 'target_ordering': 'EXACT_TRADE_TS_STRICTLY_AFTER_DECISION',
            'protected_outcomes_accessed': True, 'research_objective_achieved': False}
