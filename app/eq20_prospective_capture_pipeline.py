"""Deterministic bridge from committed provider pages to prospective capsules.

The private SQL peer owns the queue, exact source ledger, first-alert CAS and
access gates. This module transforms only the immutable action returned by that
peer. It creates no population, provider-entitlement or execution-quality truth.
Those policies must be independently registered against the original mission
before the first eligible observation. All unknown members and source defects
remain visible. The existing bounded collector invokes ``perform_action``.
"""
from __future__ import annotations

import base64
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys
import zlib

VERSION = 'EQ20_PROSPECTIVE_CAPTURE_PIPELINE_V1'
ORIGINAL_REQUIREMENT_SHA256 = 'a3b1fa43d92ba5315005697da952f9574d44f18a1290c1992edb9027f2ee1b34'
KERNEL_SHA256 = '99077b64970ff02e6a6fcc9dc9b9da4f9411eef08bc396c51cf220f35f481987'
SOURCE_SHA256 = '675e12c6f5ff492c2d99489dc44a117423d9df85862f2d9261d5a44593013ab4'
INCREMENTAL_SHA256 = 'd4f2e5c2477ed1e96fdd5d630ec1bcb9688053f0a9cf47e2e49925b90abcc71a'
DEPENDENCE_SHA256 = 'cd6af8de61d6ea0cb613abd6e3d0c4f16a05e3031137303edfad226e0406d300'
MAX_PART_BYTES = 131072
MAX_PAGE_BYTES = 2*1024*1024
MAX_PREFIX_BYTES = 8*1024*1024
MAX_SOURCE_ROWS = 20000
MAX_DECISION_MEMBERS = 1024
MAX_COMMIT_BYTES = 1536*1024
MAX_QUOTE_ROWS = 250000
MAX_TRADE_ROWS = 500000
FAMILIES = ('facts', 'events', 'bars', 'short_interest_states', 'short_volume_states')
DISPOSITIONS = ('MARKET', 'SEC', 'FUNDAMENTAL', 'REPORTED_EPS', 'SHORT_INTEREST', 'SHORT_VOLUME')
UNKNOWNS = {'SOURCE_NOT_COVERED', 'STALE', 'IDENTITY_UNRESOLVED',
            'TIMING_UNCERTIFIED', 'NOT_APPLICABLE'}
_MODULES = {}


class Closed(ValueError):
    pass


def require(value, reason):
    if not value:
        raise Closed(reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def digest(value):
    return sha(canonical(value))


def exact_hash(value):
    require(isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value), 'EXACT_SHA256_REQUIRED')
    return value


def utc(value):
    require(isinstance(value, str), 'EXPLICIT_TIMESTAMP_REQUIRED')
    item = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(item.tzinfo is not None, 'EXPLICIT_TIMEZONE_REQUIRED')
    return item.astimezone(timezone.utc)


def load(name, pin):
    path = Path(__file__).with_name(name+'.py')
    require(path.is_file() and sha(path.read_bytes()) == pin, 'CAPTURE_INSTALLED_COMPONENT_PIN')
    if name not in _MODULES:
        spec = importlib.util.spec_from_file_location('capture_pipeline_'+name, path)
        obj = importlib.util.module_from_spec(spec); sys.modules[spec.name] = obj
        spec.loader.exec_module(obj); _MODULES[name] = obj
    return _MODULES[name]


def resolve(binding, kind, status, *, maximum=MAX_PREFIX_BYTES):
    require(isinstance(binding, dict), 'AUTHENTICATED_REGISTRY_BINDING_REQUIRED')
    ref, row = binding.get('reference'), binding.get('artifact')
    require(isinstance(ref, dict) and isinstance(row, dict)
            and ref.get('kind') == kind and ref.get('status') == status
            and 0 < len(ref.get('artifact_key', '')) <= 256
            and all(ref.get(k) == row.get(k) for k in ('kind', 'status', 'artifact_key', 'implementation_sha256')),
            'EXACT_REGISTRY_REFERENCE_REQUIRED')
    raw = row.get('evidence_text')
    require(isinstance(raw, str) and len(raw.encode()) <= maximum
            and sha(raw.encode()) == exact_hash(ref.get('implementation_sha256')), 'REGISTRY_READBACK_HASH')
    value = json.loads(raw)
    require(isinstance(value, dict), 'REGISTRY_OBJECT_REQUIRED')
    return value


def validate_plan(job):
    """Validate code/scope and actual registry readbacks before any raw fetch."""
    require(isinstance(job, dict) and job.get('action') == 'CAPTURE_DATED_WINDOW'
            and job.get('operation') == 'PROSPECTIVE_CAPTURE_BATCH', 'FINITE_CAPTURE_WINDOW_JOB_REQUIRED')
    require(job.get('pipeline_module_sha256') == sha(Path(__file__).read_bytes()), 'CAPTURE_PIPELINE_CURRENT_PIN_REQUIRED')
    plan = resolve(job.get('capture_plan_binding'), 'SUCCESSOR_PROSPECTIVE_CAPTURE_PLAN', 'REGISTERED_OUTCOME_BLIND')
    contract = resolve(job.get('contract_binding'), 'SUCCESSOR_PROSPECTIVE_SESSION_CONTRACT', 'REGISTERED_OUTCOME_BLIND')
    kernel = load('eq20_prospective_kernel', KERNEL_SHA256); kernel.family(contract)
    scope = kernel.scope(contract)
    require(plan.get('version') == VERSION and all(plan.get(k) == v for k, v in scope.items())
            and plan.get('session_contract_canonical_text') == canonical(contract).decode()
            and plan.get('original_requirement_sha256') == ORIGINAL_REQUIREMENT_SHA256
            and plan.get('pipeline_module_sha256') == job['pipeline_module_sha256']
            and plan.get('runtime_module_sha256') == job.get('module_sha256')
            and plan.get('incremental_module_sha256') == INCREMENTAL_SHA256
            and plan.get('source_module_sha256') == SOURCE_SHA256
            and plan.get('kernel_sha256') == KERNEL_SHA256
            and plan.get('outcome_based_population_or_capture_filtering') is False
            and plan.get('reference_outcome_mode') == 'ORIGINAL_CERTIFIED_MINUTE_BAR_COVERAGE_V1'
            and plan.get('execution_capture_mode') == 'COMMITTED_FIRST_ALERT_SECURITY_ONLY'
            and plan.get('missing_source_policy') == 'RETAIN_UNKNOWN_AND_FULL_DECISION_GRID',
            'FROZEN_ORIGINAL_CAPTURE_SEMANTICS_AND_CODE_REQUIRED')
    day = job.get('session_date')
    official = next((x for x in contract['official_sessions'] if x['session_date'] == day), None)
    require(official is not None and job.get('regular_open') == official['open_at']
            and job.get('regular_close') == official['close_at'], 'ACTUAL_ACTIVATED_OFFICIAL_SESSION_REQUIRED')
    start, end = utc(job['window_start_at']), utc(job['window_end_at'])
    require(start < end and 0 < (end-start).total_seconds() <= 150
            and type(job.get('maximum_wall_seconds')) is int and 0 < job['maximum_wall_seconds'] <= 150
            and job.get('maximum_governed_seconds') == 30
            and utc(job['not_before']) <= end and utc(job['deadline_at']) <= end,
            'FINITE_IMMUTABLE_CAPTURE_WINDOW_REQUIRED')
    require(job.get('capture_plan_reference') == job['capture_plan_binding']['reference'], 'EXACT_CAPTURE_PLAN_REFERENCE')
    require(type(plan.get('maximum_provider_pages_per_window')) is int
            and 1 <= plan['maximum_provider_pages_per_window'] <= 128
            and type(plan.get('maximum_decision_members_per_action')) is int
            and 1 <= plan['maximum_decision_members_per_action'] <= MAX_DECISION_MEMBERS
            and type(plan.get('maximum_source_rows_per_member')) is int
            and 1 <= plan['maximum_source_rows_per_member'] <= MAX_SOURCE_ROWS
            and type(plan.get('maximum_prefix_bytes')) is int
            and 1 <= plan['maximum_prefix_bytes'] <= MAX_PREFIX_BYTES,
            'FINITE_REGISTERED_WORK_AND_STORAGE_BOUNDS_REQUIRED')
    first_open = min(utc(x['open_at']) for x in contract['official_sessions'])
    for name in ('capture_plan_binding', 'contract_binding', 'population_policy_binding', 'source_policy_binding'):
        require(utc(job[name]['artifact']['created_at']) < first_open, 'PREREGISTERED_CAPTURE_POLICY_REQUIRED')
    population_policy = resolve(job['population_policy_binding'], 'SUCCESSOR_CAPTURE_POPULATION_POLICY', 'VERIFIED_ORIGINAL_POPULATION_MECHANICAL_POLICY')
    source_policy = resolve(job['source_policy_binding'], 'SUCCESSOR_CAPTURE_SOURCE_POLICY', 'VERIFIED_CAUSAL_SOURCE_MECHANICAL_POLICY')
    for value in (population_policy, source_policy):
        require(value.get('session_contract_sha256') == scope['session_contract_sha256']
                and value.get('original_requirement_sha256') == ORIGINAL_REQUIREMENT_SHA256
                and value.get('independent_semantic_review_reference') is not None,
                'ACTUAL_ORIGINAL_SCOPE_AND_REVIEWED_MECHANICAL_POLICIES_REQUIRED')
    dependence = load('eq20_dependence_gate', DEPENDENCE_SHA256)
    adjudication = resolve(job.get('dependence_adjudication_binding'),
        'SUCCESSOR_DEPENDENCE_ADJUDICATION', 'VERIFIED_CONDITIONAL_ASSUMPTION_REVIEW')
    inputs = job.get('dependence_inputs')
    require(isinstance(inputs,dict), 'ACTUAL_DEPENDENCE_ARGUMENT_AND_SUPPORT_READBACKS_REQUIRED')
    result = dependence.adjudicate(inputs['argument_row'], inputs['argument_ref'], inputs['review_row'],
        inputs['review_ref'], inputs['supporting_rows'], inputs['policy_row'], inputs['policy_ref'],
        scope=job['dependence_scope'], source_lineage_sha256=plan['source_lineage_sha256'],
        first_reserved_outcome_at=contract['official_sessions'][0]['open_at'])
    require(result.get('state') == 'VERIFIED' and adjudication.get('gate_result') == result,
            'SUBSTANTIVE_DEPENDENCE_REVIEW_NOT_SATISFIED')
    return contract, plan, population_policy, source_policy


def readback(value, expected=None):
    raw = value.get('receipt_evidence_text') if isinstance(value, dict) else None
    require(isinstance(raw, str) and len(raw.encode()) <= MAX_PREFIX_BYTES
            and sha(raw.encode()) == value.get('receipt_sha256'), 'ACTUAL_COMMITTED_READBACK_REQUIRED')
    result = json.loads(raw)
    require(isinstance(result, dict) and (expected is None or all(result.get(k) == v for k, v in expected.items())),
            'COMMITTED_READBACK_SCOPE_MISMATCH')
    return result


def validate_page(receipt, payload, request):
    """The original received bytes are the evidence, not a reserialized body."""
    require(isinstance(receipt, dict) and receipt.get('version') == 'EQ20_PROVIDER_FIRST_RECEIPT_V1'
            and receipt.get('request') == request and receipt.get('request_sha256') == digest(request)
            and receipt.get('provider') == request.get('provider')
            and isinstance(receipt.get('quota_permit_id'), str)
            and re.fullmatch(r'[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}', receipt['quota_permit_id'])
            and receipt.get('publication_or_eligibility_certificate_granted') is False
            and receipt.get('credential_material_persisted') is False, 'ACTUAL_PROVIDER_RECEIPT_SCOPE')
    raw = base64.b64decode(receipt.get('raw_payload_base64', ''), validate=True)
    require(0 < len(raw) <= MAX_PAGE_BYTES and len(raw) == receipt.get('raw_bytes')
            and sha(raw) == receipt.get('raw_sha256') and json.loads(raw) == payload, 'EXACT_PROVIDER_BODY_HASH')
    require(utc(receipt['requested_at']) <= utc(receipt['first_received_at']), 'ACTUAL_FIRST_RECEIPT_CLOCK_REQUIRED')
    return raw


def classification(row, policy, identity):
    """Apply a reviewed data policy; never infer ordinary-common from CS alone.

    The policy's primary predicate requires an independently reconciled member
    assertion in addition to the exact provider type/venue fields. Unknowns are
    retained in the candidate ledger and prevent full-natural certification.
    """
    require(isinstance(row, dict) and isinstance(row.get('raw'), dict), 'ACTUAL_DATED_REFERENCE_ROW_REQUIRED')
    raw = row['raw']; symbol = row.get('provider_symbol')
    require(isinstance(symbol, str) and re.fullmatch(r'[A-Za-z0-9._-]{1,32}', symbol), 'DATED_PROVIDER_SYMBOL_REQUIRED')
    require(policy.get('version') == 'EQ20_CAPTURE_POPULATION_MECHANICAL_POLICY_V1'
            and policy.get('reference_mode') == 'DATED_ALL_TYPES_ACTIVE_AND_INACTIVE'
            and policy.get('primary_requires_independent_class_and_pit_identity') is True,
            'REVIEWED_FULL_FRAME_POPULATION_POLICY_REQUIRED')
    fields = policy.get('fields')
    require(fields == {'type':'type', 'venue':'primary_exchange', 'locale':'locale', 'market':'market'},
            'EXACT_REVIEWED_PROVIDER_FIELD_PROJECTION_REQUIRED')
    for field in ('primary_type_values', 'known_nonprimary_type_values', 'primary_venue_values'):
        values = policy.get(field)
        require(isinstance(values, list) and len(values) <= 128 and len(set(values)) == len(values)
                and all(isinstance(x, str) and x for x in values), 'FINITE_REGISTERED_POPULATION_VALUES')
    require(not set(policy['primary_type_values']) & set(policy['known_nonprimary_type_values']), 'CONFLICTING_POPULATION_CLASS_POLICY')
    known_excluded = raw.get('type') in policy['known_nonprimary_type_values']
    exact_source = raw.get('locale') == 'us' and raw.get('market') == 'stocks'
    reference_sha = row.get('revision_id'); exact_hash(reference_sha)
    if isinstance(identity, dict) and set(identity) >= {'reference','artifact'}:
        binding = identity
        identity = resolve(binding, 'SUCCESSOR_CAPTURE_MEMBER_CLASS_AND_IDENTITY', 'VERIFIED_POINT_IN_TIME_MEMBER')
        identity = dict(identity, certificate_sha256=binding['reference']['implementation_sha256'])
    else:
        # A copied set of booleans cannot admit a primary natural member.
        identity = None
    source_ref = identity.get('identity_source_reference') if isinstance(identity, dict) else None
    reference_bound = isinstance(source_ref, dict) and all(isinstance(source_ref.get(k),str) and source_ref[k]
        for k in ('kind','status','artifact_key','implementation_sha256')) \
        and re.fullmatch(r'[0-9a-f]{64}', source_ref.get('implementation_sha256','')) is not None
    positive = isinstance(identity, dict) and reference_bound and identity.get('reference_row_sha256') == reference_sha \
        and identity.get('provider_symbol') == symbol and identity.get('session_date') == row.get('session_date') \
        and identity.get('population_policy_sha256') == policy.get('policy_content_sha256') \
        and isinstance(identity.get('security_id'), str) and 0 < len(identity['security_id']) <= 256 \
        and identity.get('point_in_time_identity_verified') is True \
        and identity.get('primary_ordinary_common_verified') is True \
        and identity.get('identity_source_reference') is not None
    qualified = exact_source and raw.get('type') in policy['primary_type_values'] \
        and raw.get('primary_exchange') in policy['primary_venue_values'] and positive
    require(not (known_excluded and positive), 'CONFLICTING_KNOWN_CLASS_AND_IDENTITY_EVIDENCE')
    disposition = 'KNOWN_NONPRIMARY' if known_excluded and exact_source else 'VERIFIED_PRIMARY' if qualified else 'UNRESOLVED_MEMBERSHIP'
    instrument = identity.get('instrument_key') if qualified else None
    admitted = type(instrument) is int and instrument > 0 and identity.get('instrument_identity_namespace_verified') is True
    security_id = identity.get('security_id') if qualified else None
    if not isinstance(security_id, str) or not security_id:
        security_id = 'UNRESOLVED_CANDIDATE:'+digest([row.get('session_date'), symbol, reference_sha])
    # The provisional roster ID is deliberately not a numeric instrument key.
    output = {'provider_symbol':symbol, 'security_id':security_id, 'disposition':disposition,
        'reference_row_sha256':reference_sha, 'reference_page_sha256':row.get('provider_page_sha256'),
        'identity_disposition':'IDENTITY_ADMITTED' if admitted else 'IDENTITY_UNRESOLVED',
        'instrument_key':instrument if admitted else None,
        'issuer_cik':identity.get('issuer_cik') if qualified else None,
        'issuer_id':identity.get('issuer_id') if qualified else None,
        'identity_certificate_sha256':identity.get('certificate_sha256') if qualified else None,
        'unknown_retained':disposition == 'UNRESOLVED_MEMBERSHIP', 'outcomes_consulted':False}
    output['disposition_sha256'] = digest(output)
    return output


def assemble_prefix(contract, member, source_rows, manifest, *, decision_ts, capture=None):
    """Build the full captured causal prefix, retaining each first revision."""
    kernel = load('eq20_prospective_kernel', KERNEL_SHA256)
    scope = kernel.scope(contract); at = utc(decision_ts)
    require(isinstance(member, dict) and isinstance(manifest, dict)
            and all(manifest.get(k) == v for k, v in scope.items()), 'SOURCE_PREFIX_FULL_SCOPE')
    day = member.get('session_date'); security = member.get('security_id')
    official = next((x for x in contract['official_sessions'] if x['session_date'] == day), None)
    require(official is not None and manifest.get('session_date') == day and manifest.get('security_id') == security,
            'SOURCE_PREFIX_ACTUAL_DATED_MEMBER')
    require(isinstance(source_rows, list) and len(source_rows) <= MAX_SOURCE_ROWS
            and manifest.get('source_rows_sha256') == digest([[r.get('family'), r.get('row_sha256')] for r in source_rows])
            and manifest.get('complete_eligible_source_prefix') is True
            and manifest.get('first_receipt_and_revision_identity_verified') is True
            and utc(manifest.get('as_of_at')) == at, 'ACTUAL_COMPLETE_CAUSAL_SOURCE_LEDGER_READBACK')
    inputs = dict((name, []) for name in FAMILIES); seen = set()
    for entry in source_rows:
        require(isinstance(entry, dict) and entry.get('family') in inputs and isinstance(entry.get('row'), dict),
                'SOURCE_REVISION_ROW_SHAPE')
        value = entry['row']; family = entry['family']; key = (family, value.get('revision_id'))
        text = entry.get('row_evidence_text')
        require(isinstance(key[1], str) and key[1] and key not in seen
                and isinstance(text, str) and sha(text.encode()) == entry.get('row_sha256')
                and json.loads(text) == value, 'IMMUTABLE_SOURCE_REVISION_IDENTITY')
        seen.add(key)
        require(utc(value.get('first_received_at')) <= at and utc(value.get('available_at')) >= utc(value['first_received_at'])
                and type(value.get('source_sequence')) is int and value['source_sequence'] <= manifest['source_sequence'],
                'TRUE_FIRST_RECEIPT_AND_CAUSAL_CUTOFF_REQUIRED')
        # Rows first received by the cut are retained even when a later public
        # release floor means their values are not yet available to arithmetic.
        if family == 'bars' and value.get('record_kind') == 'RAW_MINUTE_BAR':
            require(capture is not None, 'PINNED_ACTUAL_PROVIDER_NORMALIZER_REQUIRED')
            parsed = capture.market_bar_rows({'bars':{member['provider_symbol']:[value['raw']]}},
                {'first_received_at':value['first_received_at'], 'raw_sha256':value['provider_page_sha256']}, [member['provider_symbol']])[0]
            if parsed['record_kind'] == 'UNPARSEABLE_BAR_RECEIPT':
                # The actual immutable page/gap receipt remains in the source
                # ledger. It cannot be assigned an invented decision timestamp.
                require(manifest.get('source_family_dispositions',{}).get('MARKET') != 'ADMITTED'
                        or member.get('identity_disposition') != 'IDENTITY_ADMITTED',
                        'UNPLACEABLE_ADMITTED_MARKET_OBSERVATION_REQUIRES_CAUSAL_SOURCE_ADJUDICATION')
                continue
            parsed.update(source_sequence=value['source_sequence'], revision_id=value['revision_id'],
                captured_raw_row_sha256=entry['row_sha256'])
            inputs[family].append(parsed)
        else:
            inputs[family].append(dict(deepcopy(value), captured_raw_row_sha256=entry['row_sha256']))
    dispositions = manifest.get('source_family_dispositions')
    require(isinstance(dispositions, dict) and set(dispositions) == set(DISPOSITIONS)
            and all(x == 'ADMITTED' or x in UNKNOWNS for x in dispositions.values()), 'ACTUAL_SOURCE_ADMISSION_DISPOSITIONS')
    calendar = manifest.get('calendar_context')
    if calendar is None:
        # The exact activated vector is shared by the batch. Its contents do
        # not need to cross the transport once per member and decision.
        calendar = {x['session_date']:{'certified':True, 'is_trading_day':True,
            'regular_open':x['open_at'], 'regular_close':x['close_at']}
            for x in contract['official_sessions']}
    require(isinstance(calendar, dict) and digest(calendar) == manifest.get('calendar_context_sha256')
            and calendar.get(day) == {'certified':True, 'is_trading_day':True,
                'regular_open':official['open_at'], 'regular_close':official['close_at']}, 'ACTUAL_OFFICIAL_SOURCE_CALENDAR_CONTEXT')
    raw = {'schema':'EQ20_PROSPECTIVE_RAW_SECURITY_SESSION_V1', 'session_contract_sha256':scope['session_contract_sha256'],
        'session_date':day, 'security_id':security, 'issuer_id':member.get('issuer_id'),
        'instrument_key':member.get('instrument_key'), 'issuer_cik':member.get('issuer_cik'),
        'population_day_sha256':manifest['population_day_sha256'],
        'regular_open':official['open_at'], 'regular_close':official['close_at'],
        'identity_disposition':member['identity_disposition'], 'source_family_dispositions':dispositions,
        'source_capture':{'excluded_source_keys':[],
            'feature_source_policy':'CAUSAL_FIRST_RECEIPT_OR_FROZEN_PUBLICATION_FLOOR_V1',
            'source_manifest_sha256':manifest['upstream_source_manifest_sha256'],
            'calendar_context':calendar, 'calendar_context_sha256':manifest['calendar_context_sha256'],
            'coverage':manifest.get('coverage', [])}, 'source_inputs':inputs}
    require(len(canonical(raw)) <= MAX_PREFIX_BYTES, 'SOURCE_PREFIX_REGISTERED_BYTE_BOUND')
    return raw


def _decimal(value, *, positive=False):
    try:
        result = Decimal(str(value))
    except (ValueError, InvalidOperation):
        return None
    return result if result.is_finite() and (not positive or result > 0) else None


def execution_market(contract, member, captured, policy, *, first_alert):
    """Normalize actual selected-security quotes/trades with explicit unknowns.

    Persistence ends at the next observed quote. A last snapshot reaches the
    close only with an actual complete-stream close receipt. Missing conditions or halt
    coverage makes the whole execution source unassessable while alerts remain.
    """
    require(isinstance(first_alert, dict) and first_alert.get('committed') is True
            and first_alert.get('security_id') == member.get('security_id')
            and first_alert.get('session_date') == member.get('session_date'), 'ACTUAL_COMMITTED_FIRST_ALERT_REQUIRED')
    exact_hash(first_alert.get('receipt_sha256'))
    day = member['session_date']; security = member['security_id']
    official = next((x for x in contract['official_sessions'] if x['session_date'] == day), None)
    require(official is not None, 'OFFICIAL_EXECUTION_SESSION_REQUIRED')
    opened, closed = utc(official['open_at']), utc(official['close_at'])
    require(policy.get('version') == 'EQ20_CAPTURE_SOURCE_MECHANICAL_POLICY_V1'
            and policy.get('quote_trade_normalization') == 'OBSERVED_FIRST_RECEIPT_AND_NEXT_QUOTE_INTERVAL_V1',
            'REGISTERED_QUOTE_TRADE_MECHANICAL_POLICY_REQUIRED')
    quote_codes = policy.get('normal_quote_condition_sets'); trade_codes = policy.get('eligible_trade_condition_sets')
    require(isinstance(quote_codes, list) and isinstance(trade_codes, list)
            and len(quote_codes) <= 128 and len(trade_codes) <= 128
            and all(isinstance(x, list) and all(isinstance(y, str) for y in x) for x in quote_codes+trade_codes),
            'REVIEWED_EXACT_CONDITION_SETS_REQUIRED')
    require(isinstance(captured, dict) and isinstance(captured.get('quotes'), list)
            and isinstance(captured.get('trades'), list) and len(captured['quotes']) <= MAX_QUOTE_ROWS
            and len(captured['trades']) <= MAX_TRADE_ROWS, 'FINITE_EXECUTION_RECORD_CENSUS')
    quotes = []; trades = []; issues = []; seenq = {}; seent = {}
    for row in captured['quotes']:
        require(isinstance(row, dict) and isinstance(row.get('raw'), dict), 'RAW_QUOTE_RECEIPT_REQUIRED')
        value = row['raw']
        try:
            at = utc(value['t']); received = utc(row['first_received_at'])
        except (Closed, ValueError, TypeError, KeyError):
            issues.append({'kind':'UNPARSEABLE_QUOTE_RECEIPT', 'revision_id':row.get('revision_id')})
            continue
        require(row.get('provider_symbol') == member['provider_symbol'], 'QUOTE_RECEIPT_SECURITY_IDENTITY')
        if received < at or not opened <= at <= closed:
            issues.append({'kind':'QUOTE_TIME_UNCERTIFIED_OR_OUTSIDE_SESSION', 'revision_id':row.get('revision_id')})
            continue
        rid = row.get('revision_id'); exact_hash(rid)
        if rid in seenq:
            require(seenq[rid]['raw'] == value, 'IMMUTABLE_QUOTE_REVISION_CONFLICT')
            continue
        seenq[rid] = row
    ordered = sorted(seenq.values(), key=lambda x:(utc(x['raw']['t']), x['revision_id']))
    dated_coverage = captured.get('coverage', {})
    coverage_authenticated = False
    if isinstance(captured.get('coverage_readback'),dict):
        native = readback(captured['coverage_readback'])
        coverage_authenticated = native.get('execution_coverage') == dated_coverage \
            and native.get('member',{}).get('session_date') == day \
            and native.get('member',{}).get('security_id') == security \
            and native.get('source_manifest_readback',{}).get('receipt_sha256') == captured.get('capture_manifest_sha256')
    verified_close = isinstance(dated_coverage, dict) and dated_coverage.get('quote_stream_closed_at') == official['close_at'] \
        and isinstance(dated_coverage.get('stream_close_receipt_sha256'), str) \
        and re.fullmatch(r'[0-9a-f]{64}', dated_coverage['stream_close_receipt_sha256']) is not None \
        and dated_coverage.get('quotes_complete') is True \
        and policy.get('last_quote_carry_rule') == 'ONLY_ACTUAL_COMPLETE_STREAM_CLOSED_AT_OFFICIAL_CLOSE' \
        and coverage_authenticated
    for index, row in enumerate(ordered):
        value = row['raw']; at = utc(value['t'])
        end = utc(ordered[index+1]['raw']['t']) if index+1 < len(ordered) else closed if verified_close else at
        if end <= at:
            issues.append({'kind':'DUPLICATE_OR_UNCLOSED_QUOTE_INTERVAL', 'revision_id':row['revision_id']}); continue
        state = 'NORMAL' if value.get('c') in quote_codes else 'UNRESOLVED'
        bid, ask = _decimal(value.get('bp'), positive=True), _decimal(value.get('ap'), positive=True)
        sizes = [value.get('bs'), value.get('as')]
        valid = bid is not None and ask is not None and bid <= ask and all(type(x) is int and x >= 0 for x in sizes)
        if not valid: state = 'UNRESOLVED'
        tick = policy.get('tick_size_by_price_rule')
        require(isinstance(tick, dict) and tick.get('version') == 'REGISTERED_PIECEWISE_TICK_V1'
                and _decimal(tick.get('price_boundary'), positive=True) is not None
                and _decimal(tick.get('below_tick'), positive=True) is not None
                and _decimal(tick.get('at_or_above_tick'), positive=True) is not None, 'FROZEN_REVIEWED_TICK_RULE_REQUIRED')
        price_tick = tick['below_tick'] if bid is not None and bid < Decimal(str(tick['price_boundary'])) else tick['at_or_above_tick']
        quotes.append({'start_at':at.isoformat(), 'end_at':end.isoformat(), 'available_at':row['first_received_at'],
            'source_id':row['revision_id'], 'market_state':state,
            'bid':str(bid) if bid is not None else None, 'ask':str(ask) if ask is not None else None,
            'bid_size':sizes[0] if valid else 0, 'ask_size':sizes[1] if valid else 0, 'tick_size':price_tick})
    for row in captured['trades']:
        require(isinstance(row, dict) and isinstance(row.get('raw'), dict), 'RAW_TRADE_RECEIPT_REQUIRED')
        value = row['raw']
        try:
            at = utc(value['t']); received = utc(row['first_received_at'])
        except (Closed, ValueError, TypeError, KeyError):
            issues.append({'kind':'UNPARSEABLE_TRADE_RECEIPT', 'revision_id':row.get('revision_id')})
            continue
        require(row.get('provider_symbol') == member['provider_symbol'], 'TRADE_RECEIPT_SECURITY_IDENTITY')
        if received < at or not opened <= at <= closed:
            issues.append({'kind':'TRADE_TIME_UNCERTIFIED_OR_OUTSIDE_SESSION', 'revision_id':row.get('revision_id')})
            continue
        rid = row.get('revision_id'); exact_hash(rid)
        if rid in seent:
            require(seent[rid]['raw'] == value, 'IMMUTABLE_TRADE_REVISION_CONFLICT'); continue
        seent[rid] = row
    event_ids = defaultdict(list)
    for row in seent.values():
        raw = row['raw']
        event = (raw.get('x'), raw.get('i'))
        if event[0] is None or event[1] is None:
            issues.append({'kind':'TRADE_EXCHANGE_EVENT_IDENTITY_MISSING', 'revision_id':row['revision_id']})
        event_ids[event].append(row)
    conflicting = {row['revision_id'] for values in event_ids.values() if len(values)>1 for row in values}
    if conflicting:
        issues.append({'kind':'CONFLICTING_TRADE_EVENT_REVISIONS', 'revision_ids':sorted(conflicting)})
    for index, row in enumerate(sorted(seent.values(), key=lambda x:(utc(x['raw']['t']), x['revision_id']))):
        if row['revision_id'] in conflicting:
            continue
        value = row['raw']; price = _decimal(value.get('p'), positive=True); size = value.get('s')
        if price is None or type(size) is not int or size < 0:
            issues.append({'kind':'INVALID_TRADE_ROW', 'revision_id':row['revision_id']}); continue
        conditions = value.get('c')
        state = 'REGULAR_ELIGIBLE' if conditions in trade_codes else 'EXCLUDED_CONDITION' if conditions in policy.get('excluded_trade_condition_sets', []) else 'UNRESOLVED'
        trades.append({'trade_ts':utc(value['t']).isoformat(), 'available_at':row['first_received_at'],
            'sequence':index, 'source_id':row['revision_id'], 'price':str(price), 'size':size, 'condition_state':state})
    coverage = deepcopy(captured.get('coverage', {}))
    require(isinstance(coverage, dict), 'AUTHENTICATED_EXECUTION_COVERAGE_OBJECT')
    reviewed = policy.get('verified_quote_trade_properties')
    require(isinstance(reviewed, dict), 'REVIEWED_PROVIDER_EXECUTION_PROPERTIES_REQUIRED')
    for name in ('quotes_complete', 'trades_complete', 'timestamps_synchronized', 'quote_conditions_verified',
                 'trade_conditions_verified', 'identity_verified', 'as_traded_prices_verified', 'halts_complete'):
        # Both actual dated coverage and the preregistered mechanical proof must
        # support a property. A copied policy flag alone never certifies a date.
        coverage[name] = coverage_authenticated and coverage.get(name) is True and reviewed.get(name) is True
    coverage['gaps'] = list(coverage.get('gaps', []))+issues
    try:
        actual_range = utc(coverage.get('start_at')) <= utc(first_alert['first_decision_ts']) \
            and utc(coverage.get('end_at')) >= closed
    except (Closed, ValueError, TypeError):
        actual_range = False
    if not coverage_authenticated or not actual_range:
        coverage['quotes_complete'] = coverage['trades_complete'] = False
        coverage['gaps'].append({'kind':'AUTHENTICATED_ACTUAL_EXECUTION_COVERAGE_RANGE_MISSING'})
    if not quotes or not trades:
        coverage['gaps'].append({'kind':'NO_COMPLETE_OBSERVED_EXECUTION_STREAM'})
    return {'schema':'EQ20_CERTIFIED_QUOTE_TRADE_SESSION_V1', 'session_date':day, 'security_id':security,
        'regular_open':official['open_at'], 'regular_close':official['close_at'],
        'execution_policy_sha256':contract['execution_policy_sha256'], 'coverage':coverage,
        'quotes':quotes, 'trades':trades, 'first_alert_receipt_sha256':first_alert['receipt_sha256'],
        'capture_manifest_sha256':exact_hash(captured.get('capture_manifest_sha256')),
        'normalization_proves_actual_fill':False}


def _rpc(rpc, op, job, values):
    return rpc(op, dict(values, attempt_id=job['attempt_id'], work_key=job['work_key']))


def _cache_value(cache, job, item):
    if cache is None:
        return None
    value = cache.load(job['activation_key'], job['session_date'], item['security_id'])
    if not isinstance(value, dict):
        return None
    state = item.get('first_signal_state')
    if not isinstance(state, dict) or value.get('decision_index') != item['decision_index']-1 \
            or value.get('source_sequence') != state.get('last_source_sequence') \
            or value.get('raw_prefix_sha256') != state.get('last_raw_prefix_sha256') \
            or not isinstance(value.get('source_rows'), list):
        return None
    rows = value['source_rows']
    if len(rows) > MAX_SOURCE_ROWS or digest([[r.get('family'), r.get('row_sha256')] for r in rows]) != value.get('source_rows_sha256'):
        return None
    return value


def prefix_descriptor(raw, rows, manifest):
    """Compact assembly binding; native revisions remain the actual evidence.

    This records inputs to the pinned normalizer. It is not a source-truth or
    model certificate. The final consumer recomputes source values and checks
    the actual online first-alert selection against the frozen code.
    """
    used = {(family, row['revision_id']) for family in FAMILIES for row in raw['source_inputs'][family]}
    normalized = []; excluded = []
    for entry in rows:
        row = entry['row']
        cell = [entry['family'], row['source_sequence'], row['revision_id'], entry['row_sha256']]
        (normalized if (entry['family'], row['revision_id']) in used else excluded).append(cell)
    return {'version':'EQ20_CAPTURE_PREFIX_ASSEMBLY_BINDING_V1',
        'session_contract_sha256':raw['session_contract_sha256'], 'session_date':raw['session_date'],
        'security_id':raw['security_id'], 'population_day_sha256':raw['population_day_sha256'],
        'instrument_key':raw['instrument_key'], 'issuer_cik':raw['issuer_cik'], 'issuer_id':raw['issuer_id'],
        'identity_disposition':raw['identity_disposition'], 'regular_open':raw['regular_open'], 'regular_close':raw['regular_close'],
        'source_family_dispositions':raw['source_family_dispositions'],
        'source_manifest_sha256':manifest['upstream_source_manifest_sha256'],
        'source_rows_sha256':manifest['source_rows_sha256'], 'source_sequence':manifest['source_sequence'],
        'normalized_revision_count':len(normalized), 'normalized_revision_vector_sha256':digest(normalized),
        'unparseable_revision_count':len(excluded), 'unparseable_revision_vector_sha256':digest(excluded),
        'calendar_context_sha256':raw['source_capture']['calendar_context_sha256'],
        'raw_prefix_sha256':digest(raw), 'raw_prefix_bytes':len(canonical(raw)),
        'normalizer_module_sha256':SOURCE_SHA256, 'incremental_module_sha256':INCREMENTAL_SHA256,
        'source_truth_certified_by_assembly':False}


def _decision_batch(job, work, *, rpc, capture, incremental, source_cache):
    members = work['members']
    require(isinstance(members, list) and 1 <= len(members) <= MAX_DECISION_MEMBERS, 'BOUNDED_DECISION_ACTION')
    by_index = {item['action_member_index']:item for item in members}
    begin = work['next_member_ordinal']
    require(sorted(by_index) == list(range(begin, begin+len(members))), 'CONTIGUOUS_PENDING_ACTION_MEMBERS')
    cached = {}; requests = []
    for item in members:
        hit = _cache_value(source_cache, job, item)
        # Keep only cache identities here. Large row values are reloaded one
        # member at a time after the bounded transport page arrives.
        if hit is not None:
            cached[item['security_id']] = {k:hit[k] for k in ('source_sequence','raw_prefix_sha256','source_rows_sha256','decision_index')}
        requests.append({'security_id':item['security_id'], 'action_member_index':item['action_member_index'],
            'cached_after_sequence':hit['source_sequence'] if hit else 0,
            'prior_state_sha256':item.get('first_signal_state_sha256'),
            'cached_raw_prefix_sha256':hit['raw_prefix_sha256'] if hit else None})
    pending = {}; complete = []; cursor = {'member_index':begin, 'row_offset':0}
    # One or a few bounded pages obtain a contiguous group. A large individual
    # prefix may cross page boundaries; no member is silently skipped.
    for _ in range(MAX_SOURCE_ROWS+1):
        page = _rpc(rpc, 'source_prefix_batch', job, {'action_key':work['action_key'], 'mode':'READ',
            'cursor':cursor, 'cache_bindings':requests})
        require(isinstance(page.get('fragments'), list) and len(canonical(page)) <= MAX_PREFIX_BYTES,
                'BOUNDED_AUTHENTICATED_PREFIX_BATCH_READBACK')
        for fragment in page['fragments']:
            index = fragment['action_member_index']; item = by_index.get(index)
            require(item is not None and fragment.get('security_id') == item['security_id'], 'PREFIX_BATCH_MEMBER_IDENTITY')
            slot = pending.setdefault(index, {'header':fragment['prefix'], 'rows':[]})
            require(slot['header'] == fragment['prefix'], 'PREFIX_MANIFEST_CHANGED_DURING_READBACK')
            require(fragment.get('row_offset') == len(slot['rows']), 'PREFIX_BATCH_EXACT_ROW_CURSOR')
            slot['rows'].extend(fragment['source_rows'])
            require(len(slot['rows']) <= MAX_SOURCE_ROWS, 'PREFIX_ROW_LIMIT')
            if fragment.get('complete') is True:
                require(index not in complete, 'PREFIX_MEMBER_COMPLETED_TWICE')
                complete.append(index)
        next_cursor = page.get('next_cursor')
        if complete or next_cursor is None:
            break
        require(isinstance(next_cursor, dict) and next_cursor != cursor, 'PREFIX_BATCH_CURSOR_MUST_ADVANCE')
        cursor = next_cursor
    require(complete and complete == list(range(begin, begin+len(complete))), 'PREFIX_BATCH_CONTIGUOUS_COMPLETED_MEMBERS')
    prepared = []; acknowledgements = []; retained_bytes = 0
    for index in complete:
        item = by_index[index]; slot = pending[index]; prefix = slot['header']
        manifest = readback(prefix['source_manifest_readback'])
        rows = slot['rows']
        if prefix.get('delta_after_sequence', 0):
            hit = _cache_value(source_cache, job, item)
            require(hit is not None and hit['source_sequence'] == prefix['delta_after_sequence'], 'ACTUAL_VERIFIED_CACHE_REQUIRED_FOR_DELTA')
            rows = hit['source_rows']+rows
        rows.sort(key=lambda r:(r['family'], r['row']['source_sequence']))
        raw = assemble_prefix(job['_contract'], prefix['member'], rows, manifest, decision_ts=item['decision_ts'], capture=capture)
        descriptor = prefix_descriptor(raw, rows, manifest)
        member_bytes = len(canonical(rows))+len(canonical(raw))
        if prepared and retained_bytes+member_bytes > 16*1024*1024:
            break
        ack = {'security_id':item['security_id'], 'decision_index':item['decision_index'],
            'action_member_index':index, 'source_manifest_sha256':prefix['source_manifest_readback']['receipt_sha256'],
            'raw_prefix_sha256':descriptor['raw_prefix_sha256'], 'assembly_descriptor':descriptor}
        if acknowledgements and len(canonical({'assemblies':acknowledgements+[ack]})) > MAX_COMMIT_BYTES:
            break
        acknowledgements.append(ack); prepared.append((item, raw, rows, manifest)); retained_bytes += member_bytes
    committed = _rpc(rpc, 'source_prefix_batch', job, {'action_key':work['action_key'], 'mode':'COMMIT', 'assemblies':acknowledgements})
    readbacks = committed.get('prefixes')
    require(isinstance(readbacks, list) and len(readbacks) == len(prepared), 'EXACT_BATCH_PREFIX_ACKNOWLEDGEMENTS')
    output = []; caches = []
    for (item, raw, rows, manifest), binding in zip(prepared, readbacks):
        require(binding.get('security_id') == item['security_id'], 'PREFIX_ACK_MEMBER_IDENTITY')
        decision = incremental.produce_decision(raw, item['decision_ts'], binding['prefix_readback'])
        state = incremental.advance_first_signals(item.get('first_signal_state'), decision)
        value = {'security_id':item['security_id'], 'expected_state_sha256':item.get('first_signal_state_sha256'),
            'decision':decision, 'first_signal_state':state,
            'decision_canonical_text':canonical(decision).decode(), 'state_canonical_text':canonical(state).decode(),
            'decision_unsigned_canonical_text':canonical({k:v for k,v in decision.items() if k != 'receipt_sha256'}).decode(),
            'state_unsigned_canonical_text':canonical({k:v for k,v in state.items() if k != 'state_sha256'}).decode(),
            'prefix_receipt_sha256':binding['prefix_readback']['receipt_sha256']}
        if output and len(canonical({'rows':output+[value]})) > MAX_COMMIT_BYTES:
            break
        require(len(canonical(value)) <= MAX_COMMIT_BYTES, 'ONE_DECISION_RECEIPT_TRANSPORT_BOUND')
        output.append(value); caches.append((item['security_id'], {'source_rows':rows,
            'source_sequence':manifest['source_sequence'], 'source_rows_sha256':manifest['source_rows_sha256'],
            'raw_prefix_sha256':digest(raw), 'decision_index':item['decision_index']}))
    response = _rpc(rpc, 'decision_commit', job, {'action_key':work['action_key'], 'expected_member_cursor':begin, 'rows':output})
    require(response.get('committed_progress') is True and response.get('next_member_ordinal') == begin+len(output),
            'ACTUAL_PARTIAL_ACTION_CAS_ACK_REQUIRED')
    observation = None
    if response.get('new_first_alerts', 0):
        fact = readback(response.get('decision_batch_readback'))
        require(fact == {k:v for k,v in response.items() if k != 'decision_batch_readback'}
                and fact.get('action_key') == work['action_key']
                and fact.get('first_member_ordinal') == begin
                and fact.get('next_member_ordinal') == begin+len(output), 'ACTUAL_FIRST_ALERT_BATCH_ACK_REQUIRED')
        observation = {'version':'EQ20_CAPTURE_DECISION_COMMIT_OBSERVATION_V1',
            'activation_key':job['activation_key'], 'session_date':job['session_date'],
            'observer_attempt_id':job['attempt_id'], 'observer_host_instance':job['host_instance'],
            'observer_boot_id':job['host_boot_id'], 'pipeline_module_sha256':job['pipeline_module_sha256'],
            'action_key':work['action_key'], 'first_member_ordinal':begin, 'next_member_ordinal':begin+len(output),
            'decision_batch_readback':response['decision_batch_readback']}
        observation['observation_sha256'] = digest(observation)
    if source_cache is not None:
        for security, value in caches:
            source_cache.store(job['activation_key'], job['session_date'], security, value)
    return dict(response, decision_commit_observation=observation) if observation is not None else response


def verify_native_capture_record(contract, raw, member_binding, source_policy, *, capture):
    """Recompute captured values from native ledger bytes before any labels.

    The caller authenticates ``member_binding`` through its registered segment
    manifest. The SQL capsule commit binds its digest to the actual final native
    source readback; a payload's self-hash alone grants no authorization.
    """
    proof = raw.get('capture_provenance') if isinstance(raw, dict) else None
    require(isinstance(proof,dict) and proof.get('version') == 'EQ20_CAPTURE_FINAL_SOURCE_BINDING_V1'
            and isinstance(member_binding,dict) and member_binding.get('security_id') == raw.get('security_id')
            and member_binding.get('raw_source_sha256') == digest(raw), 'NATIVE_SEGMENT_MEMBER_SOURCE_BINDING_REQUIRED')
    receipt = dict(proof, raw_source_sha256=digest(raw), raw_source_bytes=len(canonical(raw)),
        execution_market_sha256=digest(raw.get('execution_market')))
    require(member_binding.get('source_receipt_sha256') == digest(receipt), 'NATIVE_CAPTURE_SOURCE_RECEIPT_HASH')
    final = readback(proof.get('final_binding_readback'))
    manifest = readback(final.get('source_manifest_readback'))
    member = final.get('member')
    require(isinstance(member,dict) and member.get('session_date') == raw.get('session_date')
            and member.get('security_id') == raw.get('security_id')
            and proof.get('source_manifest_sha256') == final['source_manifest_readback']['receipt_sha256']
            and proof.get('first_alert_receipts') == final.get('first_alert_receipts')
            and proof.get('online_selection_sha256') == final.get('online_selection_sha256')
            and proof.get('online_first_signal_state_sha256') == final.get('online_first_signal_state_sha256'),
            'ACTUAL_FINAL_NATIVE_SOURCE_AND_ONLINE_BINDINGS')
    rows = raw.get('capture_source_ledger'); execution_rows = raw.get('capture_execution_ledger')
    require(isinstance(rows,list) and isinstance(execution_rows,list)
            and len(rows) <= MAX_SOURCE_ROWS and len(execution_rows) <= MAX_QUOTE_ROWS+MAX_TRADE_ROWS,
            'BOUNDED_ACTUAL_SOURCE_AND_EXECUTION_LEDGER_REQUIRED')
    reconstructed = assemble_prefix(contract, member, rows, manifest,
        decision_ts=manifest['as_of_at'], capture=capture)
    require(all(raw.get(k) == value for k,value in reconstructed.items()),
            'NORMALIZED_VALUES_MUST_EQUAL_NATIVE_SOURCE_RECONSTRUCTION')
    captured = {'quotes':[], 'trades':[], 'coverage':final['execution_coverage'],
        'capture_manifest_sha256':final['source_manifest_readback']['receipt_sha256'],
        'coverage_readback':proof['final_binding_readback']}
    for item in execution_rows:
        text = item.get('row_evidence_text'); family = item.get('family')
        require(family in ('quotes','trades') and isinstance(text,str)
                and sha(text.encode()) == item.get('row_sha256') and json.loads(text) == item.get('row'),
                'ACTUAL_EXECUTION_NATIVE_LEDGER_ROW')
        captured[family].append(item['row'])
    require(digest([[r['family'],r['row_sha256']] for r in execution_rows]) == final.get('execution_rows_sha256'),
            'ACTUAL_COMPLETE_EXECUTION_NATIVE_LEDGER_VECTOR')
    first = final.get('first_alert')
    if first is None:
        require(not execution_rows and raw.get('execution_market') is None, 'NO_TARGETED_DATA_WITHOUT_ACTUAL_FIRST_ALERT')
    else:
        market = execution_market(contract,member,captured,source_policy,first_alert=first)
        require(market == raw.get('execution_market'), 'EXECUTION_NORMALIZATION_MUST_EQUAL_NATIVE_SOURCE_RECONSTRUCTION')
    return {'state':'VERIFIED', 'verification_scope':'NATIVE_SOURCE_RECONSTRUCTION_AND_SEGMENT_BINDING_ONLY',
        'raw_source_sha256':digest(raw), 'source_receipt_sha256':member_binding['source_receipt_sha256'],
        'source_truth_certified_by_assembly':False, 'protected_labels_evaluated':False}


def perform_action(job, work, *, rpc, provider, capture, incremental, source_cache=None, page_cache=None):
    """Execute one server-selected action in the caller's admitted child slice.

    ``rpc`` is the collector's measured direct transport bound to its owner.
    No call here creates a reservation, thread, schedule or provider capacity.
    The caller checks its CPU/wall/I/O tail before each invocation and RPC.
    """
    require(isinstance(work, dict) and work.get('work_key') == job.get('work_key')
            and work.get('activation_key') == job.get('activation_key')
            and work.get('session_date') == job.get('session_date'), 'SERVER_ACTION_EXACT_WINDOW_SCOPE')
    action = work.get('action'); key = work.get('action_key')
    require(isinstance(key, str) and 0 < len(key) <= 256 and action in {
        'PROVIDER_PAGE', 'RECONCILE_POPULATION', 'PRODUCE_DECISIONS', 'SEAL_CAPSULES', 'WAIT'}, 'FIXED_SERVER_ACTION_REQUIRED')
    if action == 'WAIT':
        return {'state':'AWAITING_ELIGIBLE_EVIDENCE', 'next_due_at':work.get('next_due_at'), 'committed_progress':False}
    if action == 'PROVIDER_PAGE':
        request = work['request']
        require(page_cache is not None, 'DURABLE_PENDING_PROVIDER_PAGE_CACHE_REQUIRED')
        if request['path'].endswith(('/quotes', '/trades')):
            capture.require_targeted_execution(request, work)
        request_sha = digest(request)
        cached = page_cache.load(request_sha)
        if cached is None:
            receipt, payload = provider.capture(request, work['quota_permit'])
            validate_page(receipt, payload, request)
            require(page_cache.store(request_sha, receipt, payload) is True,
                    'DURABLE_PENDING_PROVIDER_PAGE_WRITE_ACK_REQUIRED')
        else:
            receipt, payload = cached
        raw = validate_page(receipt, payload, request)
        header = {k:v for k,v in receipt.items() if k != 'raw_payload_base64'}
        start = _rpc(rpc, 'page_begin', job, {'action_key':key, 'receipt':header, 'receipt_canonical_text':canonical(header).decode()})
        readback(start, {'raw_sha256':receipt['raw_sha256']})
        for number, offset in enumerate(range(0, len(raw), MAX_PART_BYTES)):
            part = raw[offset:offset+MAX_PART_BYTES]
            _rpc(rpc, 'page_part', job, {'action_key':key, 'raw_sha256':receipt['raw_sha256'], 'part_no':number,
                'part_sha256':sha(part), 'payload_base64':base64.b64encode(part).decode()})
        # The SQL peer derives membership/raw revisions from the exact assembled
        # provider JSON. It never trusts a parallel caller-supplied row array.
        result = _rpc(rpc, 'page_commit', job, {'action_key':key, 'raw_sha256':receipt['raw_sha256']})
        require(result.get('committed_progress') is True, 'ACTUAL_PROVIDER_PAGE_COMMIT_ACK_REQUIRED')
        page_cache.remove(request_sha)
        return result
    if action == 'RECONCILE_POPULATION':
        result = []
        for item in work['reference_rows']:
            result.append(classification(item, work['population_policy'], item.get('identity_readback')))
        require(len(result) <= 256, 'BOUNDED_POPULATION_CLASSIFICATION_PAGE')
        return _rpc(rpc, 'population_commit', job, {'action_key':key, 'expected_ordinal':work['next_ordinal'],
            'dispositions':result, 'dispositions_canonical_text':canonical(result).decode()})
    if action == 'PRODUCE_DECISIONS':
        return _decision_batch(job, work, rpc=rpc, capture=capture, incremental=incremental, source_cache=source_cache)
    # Full raw payloads are constructed only after the official close; the SQL
    # source/coverage manifests bind actual rows and the recorded first alerts.
    require(utc(work['server_time']) >= utc(job['regular_close']), 'COMPLETED_OFFICIAL_SESSION_REQUIRED')
    records = []; source_receipts = []
    for item in work['members']:
        fetched = _rpc(rpc, 'capsule_begin', job, {'action_key':key, 'security_id':item['security_id'], 'mode':'SOURCE_READBACK'})
        manifest = readback(fetched['source_manifest_readback'])
        rows = []; execution_rows = []
        for family, count, destination in [('SOURCE',fetched['source_page_count'],rows),
                                           ('EXECUTION',fetched['execution_page_count'],execution_rows)]:
            require(type(count) is int and 0 <= count <= 8192, 'FINITE_FINAL_SOURCE_PAGE_CENSUS')
            for index in range(count):
                page = _rpc(rpc, 'capsule_begin', job, {'action_key':key, 'security_id':item['security_id'],
                    'mode':'ROW_PAGE','family':family,'page_index':index,
                    'source_manifest_sha256':fetched['source_manifest_readback']['receipt_sha256']})
                require(page.get('source_manifest_sha256') == fetched['source_manifest_readback']['receipt_sha256']
                        and isinstance(page.get('rows'),list), 'FINAL_SOURCE_PAGE_EXACT_MANIFEST')
                destination.extend(page['rows'])
        raw = assemble_prefix(job['_contract'], fetched['member'], rows, manifest,
            decision_ts=manifest['as_of_at'], capture=capture)
        execution_capture = {'quotes':[], 'trades':[], 'coverage':fetched['execution_coverage'],
            'capture_manifest_sha256':fetched['source_manifest_readback']['receipt_sha256'],
            'coverage_readback':fetched['final_binding_readback']}
        for entry in execution_rows:
            text = entry.get('row_evidence_text'); family = entry.get('family')
            require(family in ('quotes','trades') and isinstance(text,str) and sha(text.encode()) == entry.get('row_sha256')
                    and json.loads(text) == entry.get('row'), 'FINAL_EXECUTION_NATIVE_ROW_READBACK')
            execution_capture[family].append(entry['row'])
        require(digest([[r['family'],r['row_sha256']] for r in execution_rows]) == fetched['execution_rows_sha256'],
                'FINAL_EXECUTION_COMPLETE_NATIVE_ROW_VECTOR')
        if fetched.get('first_alert') is not None:
            raw['execution_market'] = execution_market(job['_contract'], fetched['member'], execution_capture,
                work['source_policy'], first_alert=fetched['first_alert'])
        else:
            require(not execution_rows, 'NONSIGNAL_MEMBER_MUST_NOT_HAVE_TARGETED_EXECUTION_ROWS')
            raw['execution_market'] = None
        proof = {'version':'EQ20_CAPTURE_FINAL_SOURCE_BINDING_V1','activation_key':job['activation_key'],
            'session_contract_sha256':manifest['session_contract_sha256'],'session_date':job['session_date'],
            'security_id':item['security_id'],'population_day_sha256':manifest['population_day_sha256'],
            'source_manifest_sha256':fetched['source_manifest_readback']['receipt_sha256'],
            'online_first_signal_state_sha256':fetched['online_first_signal_state_sha256'],
            'online_selection_sha256':fetched['online_selection_sha256'],
            'first_alert_receipts':fetched['first_alert_receipts'],
            'final_binding_readback':fetched['final_binding_readback'],
            'pipeline_module_sha256':job['pipeline_module_sha256'],'full_scheduled_decision_grid_committed':True,
            'source_truth_certified_by_assembly':False}
        raw['capture_provenance'] = proof
        raw['capture_source_ledger'] = rows
        raw['capture_execution_ledger'] = execution_rows
        records.append(raw)
        source_receipts.append(dict(proof, raw_source_sha256=digest(raw),raw_source_bytes=len(canonical(raw)),
            execution_market_sha256=digest(raw['execution_market'])))
    metadata, packed = capture.capsule_segment(records, first_ordinal=work['next_ordinal'],
        maximum_raw_bytes=min(64*1024*1024, job['_plan']['maximum_capsule_raw_bytes']))
    metadata['member_source_bindings'] = [{'ordinal':work['next_ordinal']+index,
        'security_id':value['security_id'], 'source_receipt_sha256':digest(value),
        'raw_source_sha256':value['raw_source_sha256']} for index,value in enumerate(source_receipts)]
    require(len(packed) <= min(MAX_PREFIX_BYTES, job['_plan']['maximum_capsule_compressed_bytes']),
            'REGISTERED_CAPSULE_COMPRESSED_BOUND')
    _rpc(rpc, 'capsule_begin', job, {'action_key':key, 'mode':'DECLARE_SEGMENT', 'metadata':metadata})
    for number, offset in enumerate(range(0, len(packed), MAX_PART_BYTES)):
        part = packed[offset:offset+MAX_PART_BYTES]
        _rpc(rpc, 'capsule_part', job, {'action_key':key, 'compressed_sha256':metadata['compressed_sha256'],
            'part_no':number, 'part_sha256':sha(part), 'payload_base64':base64.b64encode(part).decode()})
    return _rpc(rpc, 'capsule_commit', job, {'action_key':key, 'metadata':metadata,
        'member_source_receipts':source_receipts})
