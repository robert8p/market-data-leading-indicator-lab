"""Fixed-rule prospective evidence processing; no fitting and no market IO.

This is a separately versioned consumer of certified session inputs. It is not
the producer of those inputs and is not an execution-fill simulator. Source
capture, point-in-time population truth and the execution-label implementation
remain independently resolved prerequisites. W10's date guard is untouched.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
from typing import Mapping

VERSION = 'EQ20_PROSPECTIVE_FIXED_RULE_KERNEL_V1'
VALUE_STATES = {'AVAILABLE', 'OBSERVED_ZERO', 'NOT_APPLICABLE', 'SOURCE_NOT_COVERED',
                'IDENTITY_UNRESOLVED', 'STALE', 'TIMING_UNCERTIFIED'}
REFERENCE_STATES = VALUE_STATES | {'NO_DATA', 'INVALID'}
KNOWN = {'AVAILABLE', 'OBSERVED_ZERO'}
OUTCOMES = {'QUALIFY', 'NONQUALIFY', 'UNRESOLVED'}
FILL_STATES = {'FILLED', 'PARTIAL_FILL', 'NO_FILL', 'EXPIRED', 'HALT_BLOCKED', 'UNASSESSABLE'}
MAX_DECISIONS = 390
MAX_FEATURES = 128


class Closed(ValueError):
    pass


def require(condition, reason):
    if not condition:
        raise Closed(reason)


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def stamp(value):
    require(isinstance(value, str), 'TIME_REQUIRED')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(result.tzinfo is not None, 'EXPLICIT_TIMEZONE_REQUIRED')
    return result.astimezone(timezone.utc)


def finite(value, *, positive=False):
    require(type(value) in (int, float) and math.isfinite(value)
            and (not positive or value > 0), 'FINITE_VALUE_REQUIRED')
    return value


def hash_value(value):
    require(isinstance(value, str) and len(value) == 64
            and all(c in '0123456789abcdef' for c in value), 'EXACT_SHA256_REQUIRED')
    return value


def scope(contract):
    return {'session_contract_sha256': digest(contract),
            **{name: contract[name] for name in ('candidate_family_sha256', 'execution_policy_sha256',
                'official_calendar_sha256', 'population_manifest_sha256', 'official_session_dates_sha256',
                'population_policy_artifact_sha256')}}


def family(contract):
    """Compile only supplied immutable thresholds, including sparse abstention."""
    require(isinstance(contract, dict), 'PROSPECTIVE_CONTRACT_OBJECT')
    require(contract.get('schema') == 'EQ20_PROSPECTIVE_SESSION_CONTRACT_V1', 'PROSPECTIVE_CONTRACT_SCHEMA')
    require(contract.get('research_mode') == 'FULL_STREAM' and contract.get('sampling_design') is None,
            'FULL_NATURAL_POPULATION_REQUIRED')
    require(contract.get('thresholds_refitted') is False
            and contract.get('outcome_based_stopping_or_extension') is False, 'FROZEN_RULES_AND_HORIZON_REQUIRED')
    raw = contract.get('frozen_rules')
    require(isinstance(raw, list) and 1 <= len(raw) <= 5, 'FROZEN_CANDIDATE_COUNT')
    features = contract.get('feature_names')
    require(isinstance(features, list) and 1 <= len(features) <= MAX_FEATURES
            and all(isinstance(f, str) and f for f in features)
            and len(set(features)) == len(features), 'EXACT_FEATURE_MANIFEST_REQUIRED')
    ids = []
    for rule in raw:
        require(isinstance(rule, dict) and set(rule) == {'rule_id', 'gates'}, 'FROZEN_RULE_SCHEMA')
        require(isinstance(rule['rule_id'], str) and rule['rule_id']
                and rule['rule_id'] != '__UNION__', 'RULE_ID_REQUIRED')
        ids.append(rule['rule_id'])
        require(isinstance(rule['gates'], list) and 1 <= len(rule['gates']) <= 10, 'RULE_COMPLEXITY_BOUND')
        if len(rule['gates']) > 6:
            hash_value(contract.get('extended_complexity_review_sha256'))
        for gate in rule['gates']:
            require(isinstance(gate, dict) and set(gate) == {'feature', 'threshold', 'operator', 'negate'}, 'GATE_SCHEMA')
            require(gate['feature'] in features and gate['operator'] in ('ge', 'le')
                    and type(gate['negate']) is bool, 'FROZEN_GATE_SEMANTICS')
            if gate['threshold'] is not None:
                finite(gate['threshold'])
    require(len(set(ids)) == len(ids), 'DUPLICATE_RULE_ID')
    union_ids = contract.get('union_ids')
    require(isinstance(union_ids, list) and union_ids and len(set(union_ids)) == len(union_ids)
            and set(union_ids) <= set(ids), 'FROZEN_UNION_MEMBERSHIP')
    frozen = {'rules': raw, 'union_ids': union_ids}
    require(digest(frozen) == contract.get('candidate_family_sha256'), 'CANDIDATE_FAMILY_CONTENT_HASH')
    dates = contract.get('official_session_dates')
    require(isinstance(dates, list) and len(dates) == 252 and dates == sorted(set(dates))
            and all(isinstance(d, str) and date.fromisoformat(d).isoformat() == d for d in dates),
            'EXACT_252_OFFICIAL_DATES')
    require(digest(dates) == contract.get('official_session_dates_sha256'), 'OFFICIAL_VECTOR_CONTENT_HASH')
    calendar = contract.get('official_sessions')
    require(isinstance(calendar, list) and len(calendar) == 252
            and all(isinstance(row, dict) for row in calendar)
            and [row.get('session_date') for row in calendar] == dates,
            'FROZEN_OFFICIAL_SESSION_CLOCK_VECTOR')
    require(not any('2026-06-01' <= d <= '2026-07-31' for d in dates), 'EXPOSED_JUNE_JULY_REJECTED')
    require(contract.get('decision_period_seconds') == 60
            and type(contract.get('decision_offset_seconds')) is int
            and 0 <= contract['decision_offset_seconds'] < 60, 'FROZEN_MINUTE_CLOCK_REQUIRED')
    require(type(contract.get('maximum_reference_staleness_seconds')) is int
            and contract['maximum_reference_staleness_seconds'] >= 0, 'REFERENCE_STALENESS_POLICY_REQUIRED')
    for field in ('execution_policy_sha256', 'official_calendar_sha256', 'population_manifest_sha256',
                  'population_policy_artifact_sha256'):
        hash_value(contract.get(field))
    return raw, union_ids, features


def select_first_signals(contract, session):
    """Select first TRUE from features only, before inspecting any label object."""
    rules, union_ids, features = family(contract)
    require(isinstance(session, dict) and session.get('session_date') in contract['official_session_dates'],
            'REGISTERED_SESSION_REQUIRED')
    require(isinstance(session.get('security_id'), str) and session['security_id'], 'PIT_SECURITY_ID_REQUIRED')
    require('sampling' not in session, 'SAMPLE_IN_PROSPECTIVE_STREAM')
    opened, closed = stamp(session['regular_open']), stamp(session['regular_close'])
    official = contract['official_sessions'][contract['official_session_dates'].index(session['session_date'])]
    require(opened == stamp(official.get('open_at')) and closed == stamp(official.get('close_at')),
            'EXACT_OFFICIAL_OPEN_AND_CLOSE')
    from zoneinfo import ZoneInfo
    ny_open = opened.astimezone(ZoneInfo('America/New_York'))
    require(ny_open.date().isoformat() == session['session_date']
            and (ny_open.hour, ny_open.minute, ny_open.second) == (9, 30, 0)
            and timedelta(minutes=70) <= closed-opened <= timedelta(minutes=390), 'OFFICIAL_SESSION_CLOCK')
    expected = []
    at = opened + timedelta(minutes=10, seconds=contract['decision_offset_seconds'])
    while at <= closed - timedelta(minutes=60):
        expected.append(at); at += timedelta(minutes=1)
    decisions = session.get('decisions')
    require(isinstance(decisions, list) and len(decisions) == len(expected)
            and 1 <= len(decisions) <= MAX_DECISIONS, 'COMPLETE_DECISION_GRID_REQUIRED')
    first = {rule['rule_id']: None for rule in rules}
    unknowns = {rule['rule_id']: 0 for rule in rules}
    union_first = None; union_attribution = None
    for index, (row, when) in enumerate(zip(decisions, expected)):
        require(isinstance(row, dict) and stamp(row.get('decision_ts')) == when, 'EXACT_DECISION_TIMESTAMP')
        require(row.get('reference_state') in REFERENCE_STATES, 'REFERENCE_STATE_REQUIRED')
        reference_ok = row['reference_state'] == 'AVAILABLE'
        if reference_ok:
            finite(row.get('p_reference'), positive=True)
            ended, available = stamp(row.get('reference_bar_end')), stamp(row.get('reference_available_at'))
            require(ended <= available <= when
                    and (when-ended).total_seconds() <= contract['maximum_reference_staleness_seconds'],
                    'REFERENCE_COMPLETION_AVAILABILITY_OR_STALENESS')
        else:
            require(row.get('p_reference') is None, 'UNKNOWN_REFERENCE_NOT_NUMERIC')
        cells = row.get('features')
        require(isinstance(cells, dict) and set(cells) == set(features), 'NO_DROPPED_OR_ADDED_FEATURE_CELLS')
        for cell in cells.values():
            require(isinstance(cell, dict) and set(cell) == {'state', 'value', 'available_at'}
                    and cell['state'] in VALUE_STATES, 'FEATURE_CELL_SCHEMA')
            if cell['state'] in KNOWN:
                finite(cell['value'])
                require(stamp(cell['available_at']) <= when, 'POST_DECISION_FEATURE_REJECTED')
                require(cell['state'] != 'OBSERVED_ZERO' or cell['value'] == 0, 'OBSERVED_ZERO_VALUE_MISMATCH')
            else:
                require(cell['value'] is None, 'UNKNOWN_FEATURE_NOT_NUMERIC')
        true_ids = []
        for rule in rules:
            states = []
            for gate in rule['gates']:
                cell = cells[gate['feature']]
                if gate['threshold'] is None or cell['state'] not in KNOWN:
                    states.append(None)
                else:
                    result = cell['value'] >= gate['threshold'] if gate['operator'] == 'ge' else cell['value'] <= gate['threshold']
                    states.append(not result if gate['negate'] else result)
            # FALSE AND UNKNOWN is FALSE; an absent reference abstains.
            truth = False if False in states else None if None in states or not reference_ok else True
            rule_id = rule['rule_id']
            if truth is None:
                unknowns[rule_id] += 1
            elif truth:
                true_ids.append(rule_id)
                if first[rule_id] is None:
                    first[rule_id] = index
        members = sorted(set(true_ids).intersection(union_ids))
        if union_first is None and members:
            union_first = index; union_attribution = members[0]
    return {'first_indices': first, 'union_first_index': union_first, 'union_attribution': union_attribution,
            'unknown_gate_decisions': unknowns, 'scheduled_decisions': len(decisions),
            'selection_evidence': 'FROZEN_FEATURE_GATES_ONLY', 'thresholds_refitted': False,
            'selection_sha256': digest([session['session_date'], session['security_id'], first,
                                       union_first, union_attribution, contract['candidate_family_sha256']])}


def _outcome(label, row, closed, *, executable=False, contract=None):
    if label is None:
        return {'state': 'UNRESOLVED', 'reason': 'SOURCE_OUTCOME_MISSING', 'fill_state': 'UNASSESSABLE' if executable else None}
    require(isinstance(label, dict) and label.get('state') in OUTCOMES, 'OUTCOME_STATE_REQUIRED')
    decision = stamp(row['decision_ts'])
    require(stamp(label.get('decision_ts')) == decision, 'LABEL_DECISION_ALIGNMENT')
    state = label['state']
    if row['reference_state'] != 'AVAILABLE':
        require(state == 'UNRESOLVED', 'RESOLVED_OUTCOME_WITHOUT_REFERENCE')
    anchor = row.get('p_reference')
    after = decision
    fill_state = None
    if executable:
        require(label.get('execution_policy_sha256') == contract['execution_policy_sha256'], 'EXECUTION_POLICY_PIN')
        fill_state = label.get('fill_state')
        require(fill_state in FILL_STATES, 'ALL_ALERT_EXECUTION_STATUS_REQUIRED')
        if fill_state in ('NO_FILL', 'EXPIRED', 'HALT_BLOCKED', 'UNASSESSABLE'):
            require(state != 'QUALIFY', 'NO_FILL_CANNOT_BE_EXECUTABLE_SUCCESS')
        if fill_state == 'UNASSESSABLE':
            require(state == 'UNRESOLVED', 'UNASSESSABLE_NOT_RELABELLED')
        if fill_state in ('FILLED', 'PARTIAL_FILL'):
            anchor = finite(label.get('p_entry'), positive=True)
            received, ordered, after = [stamp(label.get(k)) for k in ('alert_received_ts', 'entry_order_ts', 'entry_fill_ts')]
            delay = contract.get('primary_response_delay_seconds')
            notify = contract.get('notification_delay_seconds')
            require(type(delay) is int and delay == 120 and type(notify) is int and notify >= 0,
                    'FROZEN_RESPONSE_AND_NOTIFICATION_POLICY_REQUIRED')
            require(decision + timedelta(seconds=notify) <= received
                    and received + timedelta(seconds=delay) <= ordered <= after <= closed,
                    'EXECUTION_TIMING_CAUSALITY')
            finite(label.get('filled_quantity'), positive=True)
            if fill_state == 'PARTIAL_FILL' and state == 'QUALIFY':
                require(contract.get('partial_fill_success_policy') == 'QUALIFY_FILLED_PORTION_WITH_FULL_ALERT_DENOMINATOR',
                        'PARTIAL_FILL_POLICY_NOT_FROZEN')
    if state == 'NONQUALIFY':
        require(label.get('coverage_complete') is True and label.get('ordering_ambiguity') is False,
                'NONQUALIFY_REQUIRES_COMPLETE_UNAMBIGUOUS_EVIDENCE')
    elif state == 'QUALIFY':
        require(label.get('target_observation_valid') is True, 'VALID_TARGET_OBSERVATION_REQUIRED')
        beginning, end = stamp(label.get('target_interval_start')), stamp(label.get('target_interval_end'))
        require(after < beginning <= end <= closed, 'STRICT_POST_ANCHOR_TARGET_ORDER_REQUIRED')
        observed = finite(label.get('target_price'), positive=True)
        require(Decimal(str(observed)) >= Decimal('1.20') * Decimal(str(anchor)), 'ORIGINAL_TWENTY_PERCENT_TARGET')
    return {'state': state, 'fill_state': fill_state, 'source_reason': label.get('reason_code')}


def process_security_session(contract, payload):
    """Process exactly one natural security-session into its first-alert receipt."""
    require(isinstance(payload, dict) and payload.get('schema') == 'EQ20_PROSPECTIVE_SECURITY_SESSION_V1', 'INPUT_SCHEMA')
    session = payload.get('features')
    selected = select_first_signals(contract, session)
    labels = payload.get('reference_labels')
    execution = payload.get('execution_labels')
    require(isinstance(labels, list) and isinstance(execution, list)
            and len(labels) == len(execution) == selected['scheduled_decisions'], 'ALL_DECISION_LABEL_SLOTS_REQUIRED')
    key = (session['session_date'], session['security_id'])
    require(payload.get('session_date') == key[0] and payload.get('security_id') == key[1], 'SOURCE_SECURITY_SESSION_ALIGNMENT')
    require(payload.get('candidate_family_sha256') == contract['candidate_family_sha256'], 'SOURCE_FAMILY_BINDING')
    hash_value(payload.get('population_day_sha256'))
    closed = stamp(session['regular_close'])
    # Labels are inspected after deterministic feature-only selection completes.
    reference = [_outcome(label, row, closed) for label, row in zip(labels, session['decisions'])]
    states = [r['state'] for r in reference]
    opportunity = 'QUALIFY' if 'QUALIFY' in states else 'UNRESOLVED' if 'UNRESOLVED' in states else 'NONQUALIFY'
    ids = [r['rule_id'] for r in contract['frozen_rules']]
    slots = []
    for position in range(6):
        rule_id = ids[position] if position < len(ids) else '__UNION__' if position == 5 else None
        index = selected['union_first_index'] if position == 5 else selected['first_indices'].get(rule_id)
        for anchor in ('REFERENCE', 'EXECUTABLE_ENTRY'):
            slot_id = position*2 + (anchor == 'EXECUTABLE_ENTRY')
            item = {'claim_slot': int(slot_id), 'rule_id': rule_id, 'anchor': anchor,
                    'active': rule_id is not None, 'n_signals': 0, 'n_success': 0,
                    'n_nonqualify': 0, 'n_unresolved': 0, 'first_decision_ts': None}
            if rule_id is not None and index is not None:
                outcome = reference[index] if anchor == 'REFERENCE' else _outcome(execution[index],
                    session['decisions'][index], closed, executable=True, contract=contract)
                item.update(n_signals=1, first_decision_ts=session['decisions'][index]['decision_ts'],
                            outcome_state=outcome['state'], fill_state=outcome.get('fill_state'))
                item[{'QUALIFY': 'n_success', 'NONQUALIFY': 'n_nonqualify', 'UNRESOLVED': 'n_unresolved'}[outcome['state']]] = 1
                item['union_attribution'] = selected['union_attribution'] if position == 5 else None
            slots.append(item)
    result = {'version': VERSION, 'state': 'VERIFIED', 'verification_scope': 'ONE_CERTIFIED_INPUT_SECURITY_SESSION_REPLAY',
              'session_date': key[0], 'security_id': key[1], 'issuer_id': payload.get('issuer_id'),
              **scope(contract),
              'population_day_sha256': payload['population_day_sha256'],
              'scheduled_decisions': selected['scheduled_decisions'], 'first_signal_selection': selected,
              'opportunity_state': opportunity, 'claim_slots': slots,
              'input_sha256': digest(payload), 'contract_sha256': digest(contract),
              'sampling_applied': False, 'thresholds_refitted': False,
              'protected_outcomes_accessed': True, 'research_objective_achieved': False}
    result['receipt_sha256'] = digest(result)
    return result


def resolve_dated_population(contract, population, binding):
    """Read the server-resolved dated certificate; self hashes do not authorize it.

    ``binding`` comes from the authenticated private dispatch RPC, separately
    from the market capsule. It is never taken from a caller-supplied outcome
    object. A post-date roster implements the preregistered population policy;
    the policy does not pretend future IPO/delisting membership was preknown.
    """
    require(isinstance(binding, dict) and isinstance(binding.get('reference'), dict)
            and isinstance(binding.get('artifact'), dict), 'DATED_POPULATION_REGISTRY_BINDING_REQUIRED')
    ref, row = binding['reference'], binding['artifact']
    require(ref.get('kind') == 'SUCCESSOR_DATED_POPULATION_CERTIFICATE'
            and ref.get('status') == 'VERIFIED_POINT_IN_TIME_FULL_NATURAL_POPULATION'
            and isinstance(ref.get('artifact_key'), str) and ref['artifact_key']
            and all(row.get(key) == ref.get(key) for key in ('kind', 'artifact_key', 'status', 'implementation_sha256')),
            'DATED_POPULATION_AUTHORITATIVE_REFERENCE')
    hash_value(ref.get('implementation_sha256'))
    raw = row.get('evidence_text')
    require(isinstance(raw, str) and len(raw.encode()) <= 262144
            and hashlib.sha256(raw.encode()).hexdigest() == ref['implementation_sha256'],
            'DATED_POPULATION_AUTHORITATIVE_READBACK_HASH')
    evidence = json.loads(raw)
    require(isinstance(evidence, dict) and evidence.get('version') == 'EQ20_DATED_POPULATION_CERTIFICATE_V1'
            and all(evidence.get(key) == value for key, value in scope(contract).items())
            and evidence.get('session_date') == population.get('session_date')
            and evidence.get('population_day_sha256') == population.get('population_day_sha256')
            and evidence.get('members_sha256') == population.get('members_sha256')
            and evidence.get('security_session_count') == len(population.get('security_ids', []))
            and evidence.get('expected_decision_count') == population.get('expected_decision_count'),
            'DATED_POPULATION_CERTIFICATE_SCOPE_OR_DENOMINATOR')
    require(all(evidence.get(field) is True for field in ('point_in_time_membership_verified',
            'source_completeness_reviewed', 'no_outcome_based_population_filtering', 'full_natural_population')),
            'DATED_POPULATION_SCIENTIFIC_REVIEW_INCOMPLETE')
    stamp(row.get('created_at'))
    return ref['implementation_sha256']


def aggregate_day(contract, population, receipts, population_binding):
    """Reconcile the complete dated denominator; do not remove missing members."""
    family(contract)
    require(isinstance(population, dict) and population.get('session_date') in contract['official_session_dates'], 'POPULATION_DATE')
    members = population.get('security_ids')
    require(isinstance(members, list) and members and members == sorted(set(members))
            and all(isinstance(x, str) and x for x in members), 'EXACT_DATED_POPULATION_REQUIRED')
    require(digest(members) == population.get('members_sha256'), 'POPULATION_MEMBERSHIP_HASH')
    population_sha = digest({k: v for k, v in population.items() if k != 'population_day_sha256'})
    require(population_sha == population.get('population_day_sha256'), 'POPULATION_DAY_CONTENT_HASH')
    certificate_sha = resolve_dated_population(contract, population, population_binding)
    if isinstance(receipts, list):
        require(len(receipts) == len(members), 'UNPROCESSED_POPULATION_MEMBERS_RETAINED')
    require(hasattr(receipts, '__iter__'), 'STREAMED_SECURITY_RECEIPTS_REQUIRED')
    by_id = {}
    totals = [{'claim_slot': i, 'session_date': population['session_date'], 'n_signals': 0,
               'n_success': 0, 'n_nonqualify': 0, 'n_unresolved': 0} for i in range(12)]
    decisions = 0; opportunities = unresolved_opportunities = unknown_identities = 0
    for receipt in receipts:
        require(isinstance(receipt, dict), 'SECURITY_RECEIPT_OBJECT')
        require(receipt.get('version') == VERSION and receipt.get('state') == 'VERIFIED'
                and all(receipt.get(key) == value for key, value in scope(contract).items()),
                'SECURITY_RECEIPT_REGISTERED_CONTRACT_SCOPE')
        require(receipt.get('receipt_sha256') == digest({k: v for k, v in receipt.items() if k != 'receipt_sha256'}),
                'COMMITTED_SECURITY_RECEIPT_CONTENT_HASH')
        security_id = receipt.get('security_id')
        require(security_id in members and security_id not in by_id
                and receipt.get('session_date') == population['session_date']
                and receipt.get('population_day_sha256') == population_sha
                and receipt.get('candidate_family_sha256') == contract['candidate_family_sha256'],
                'DATED_SECURITY_RECEIPT_IDENTITY_OR_SCOPE')
        by_id[security_id] = receipt['receipt_sha256']
        decisions += receipt['scheduled_decisions']
        opportunities += receipt['opportunity_state'] == 'QUALIFY'
        unresolved_opportunities += receipt['opportunity_state'] == 'UNRESOLVED'
        unknown_identities += not bool(receipt.get('issuer_id'))
        require([x.get('claim_slot') for x in receipt['claim_slots']] == list(range(12)), 'EXACT_TWELVE_CLAIM_SLOTS')
        for source, target in zip(receipt['claim_slots'], totals):
            for field in ('n_signals', 'n_success', 'n_nonqualify', 'n_unresolved'):
                require(type(source.get(field)) is int and source[field] in (0, 1), 'FIRST_SIGNAL_BINARY_ACCOUNTING')
                target[field] += source[field]
            require(source['n_success']+source['n_nonqualify']+source['n_unresolved'] == source['n_signals'],
                    'CONSERVATIVE_OUTCOME_ACCOUNTING')
        require(all(receipt['claim_slots'][i]['n_signals'] == receipt['claim_slots'][i+1]['n_signals']
                    for i in range(0, 12, 2)), 'EXECUTION_DENOMINATOR_CANNOT_DROP_ALERTS')
    require(set(by_id) == set(members), 'FULL_NATURAL_POPULATION_RECONCILIATION')
    require(decisions == population.get('expected_decision_count'), 'FULL_DECISION_GRID_RECONCILIATION')
    result = {'version': VERSION, 'session_date': population['session_date'], 'state': 'VERIFIED',
        **scope(contract),
        'population_day_sha256': population_sha, 'dated_population_certificate_sha256': certificate_sha, 'population_members': len(members), 'scheduled_decisions': decisions,
        'verified_opportunity_sessions': opportunities, 'unresolved_opportunity_sessions': unresolved_opportunities,
        'unresolved_issuer_sessions': unknown_identities, 'claim_rows': totals,
        'security_receipts_manifest_sha256': digest(sorted(by_id.items())),
        'zero_alert_dates_retained': True, 'inverse_probability_weighting_applied': False,
        'protected_outcomes_accessed': True, 'research_objective_achieved': False}
    result['receipt_sha256'] = digest(result)
    return result


def evaluate_fixed_horizon_claim(contract, claim_slot, day_receipts, registration, verified_gates, evaluator):
    """Consume exact252date aggregates only after the authenticated gate resolves."""
    family(contract)
    require(type(claim_slot) is int and 0 <= claim_slot < 12, 'FROZEN_CLAIM_SLOT')
    require(isinstance(day_receipts, list)
            and [d.get('session_date') for d in day_receipts] == contract['official_session_dates'],
            'FIXED_HORIZON_COMPLETE_DATE_VECTOR_REQUIRED')
    require(isinstance(verified_gates, dict), 'AUTHORITATIVE_FINAL_GATE_READBACK_REQUIRED')
    rows = []; population_vector = []
    for day in day_receipts:
        require(isinstance(day, dict) and day.get('version') == VERSION and day.get('state') == 'VERIFIED'
                and all(day.get(key) == value for key, value in scope(contract).items()),
                'DAY_RECEIPT_REGISTERED_CONTRACT_SCOPE')
        require(day.get('receipt_sha256') == digest({k: v for k, v in day.items() if k != 'receipt_sha256'}),
                'COMMITTED_DAY_RECEIPT_CONTENT_HASH')
        require(day.get('unresolved_issuer_sessions') == 0, 'LONGITUDINAL_IDENTITY_CERTIFICATION_PENDING')
        population_vector.append({'session_date': day['session_date'],
            'population_day_sha256': hash_value(day.get('population_day_sha256')),
            'certificate_sha256': hash_value(day.get('dated_population_certificate_sha256'))})
        values = day.get('claim_rows')
        require(isinstance(values, list) and [v.get('claim_slot') for v in values] == list(range(12)), 'DAY_CLAIM_MANIFEST')
        rows.append({k: values[claim_slot][k] for k in ('session_date', 'n_signals', 'n_success', 'n_nonqualify', 'n_unresolved')})
    require(isinstance(registration, dict)
            and registration.get('official_session_dates') == contract['official_session_dates']
            and all(registration.get(key) == value for key, value in scope(contract).items())
            and registration.get('point_in_time_population_sha256') == contract['population_manifest_sha256'],
            'INFERENCE_SCOPE_MISMATCH')
    require(verified_gates.get('dated_population_certificate_vector_sha256') == digest(population_vector),
            'AUTHORITATIVE_COMPLETE_DATED_POPULATION_VECTOR_REQUIRED')
    # Only the authenticated transport resolves this argument; no local boolean
    # construction from an activation status is permitted in the worker.
    result = evaluator.evaluate_claim(rows, registration, verified_gates)
    result['claim_slot'] = claim_slot
    result['full_day_receipt_manifest_sha256'] = digest([d['receipt_sha256'] for d in day_receipts])
    result['research_objective_achieved'] = False
    return result
