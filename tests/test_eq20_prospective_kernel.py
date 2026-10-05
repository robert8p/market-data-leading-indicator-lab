"""Synthetic, outcome-blind semantics checks; no market or reserved data."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import unittest
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('prospective_kernel', ROOT/'app/eq20_prospective_kernel.py')
k = importlib.util.module_from_spec(spec); spec.loader.exec_module(k)


def fixture():
    dates = []
    value = date(2027, 1, 4)
    while len(dates) < 252:
        if value.weekday() < 5:
            dates.append(value.isoformat())
        value += timedelta(days=1)
    # Deliberately synthetic official-clock certificate substitute for unit tests.
    sessions = []
    for d in dates:
        op = datetime.combine(date.fromisoformat(d), datetime.min.time(), tzinfo=ZoneInfo('America/New_York')).replace(hour=9, minute=30)
        sessions.append({'session_date': d, 'open_at': op.isoformat(), 'close_at': (op+timedelta(minutes=73)).isoformat()})
    rules = [{'rule_id': 'A', 'gates': [{'feature': 'x', 'operator': 'ge', 'threshold': 1, 'negate': False}]},
             {'rule_id': 'B', 'gates': [{'feature': 'y', 'operator': 'ge', 'threshold': 1, 'negate': False}]}]
    c = {'schema': 'EQ20_PROSPECTIVE_SESSION_CONTRACT_V1', 'research_mode': 'FULL_STREAM',
         'sampling_design': None, 'thresholds_refitted': False, 'outcome_based_stopping_or_extension': False,
         'frozen_rules': rules, 'feature_names': ['x', 'y'], 'union_ids': ['A', 'B'],
         'candidate_family_sha256': k.digest({'rules': rules, 'union_ids': ['A', 'B']}),
         'official_session_dates': dates, 'official_session_dates_sha256': k.digest(dates),
         'official_sessions': sessions, 'decision_period_seconds': 60, 'decision_offset_seconds': 35,
         'maximum_reference_staleness_seconds': 60, 'execution_policy_sha256': 'a'*64,
         'official_calendar_sha256': 'c'*64, 'population_manifest_sha256': 'd'*64,
         'population_policy_artifact_sha256': 'e'*64,
         'primary_response_delay_seconds': 120, 'notification_delay_seconds': 2}
    first = k.stamp(sessions[0]['open_at']) + timedelta(minutes=10, seconds=35)
    rows = []
    for i, (x, y) in enumerate([(0, 1), (1, 1), (0, 1)]):
        when = first+timedelta(minutes=i)
        rows.append({'decision_ts': when.isoformat(), 'reference_state': 'AVAILABLE', 'p_reference': 10,
            'reference_bar_end': (when-timedelta(seconds=35)).isoformat(),
            'reference_available_at': (when-timedelta(seconds=5)).isoformat(),
            'features': {name: {'state': 'AVAILABLE', 'value': val, 'available_at': when.isoformat()}
                         for name, val in [('x', x), ('y', y)]}})
    feature = {'session_date': dates[0], 'security_id': 'S1', 'regular_open': sessions[0]['open_at'],
               'regular_close': sessions[0]['close_at'], 'decisions': rows}
    reference = [nonqualify(row) for row in rows]
    reference[0] = qualify(rows[0])
    payload = {'schema': 'EQ20_PROSPECTIVE_SECURITY_SESSION_V1', 'features': feature,
          'session_date': dates[0], 'security_id': 'S1', 'issuer_id': 'I1',
          'candidate_family_sha256': c['candidate_family_sha256'], 'population_day_sha256': 'b'*64,
          'reference_labels': reference,
          'execution_labels': [execution_no_fill(c, row) for row in rows]}
    return c, payload


def nonqualify(row):
    return {'decision_ts': row['decision_ts'], 'state': 'NONQUALIFY', 'coverage_complete': True, 'ordering_ambiguity': False}


def qualify(row):
    when = k.stamp(row['decision_ts'])
    return {'decision_ts': row['decision_ts'], 'state': 'QUALIFY', 'target_observation_valid': True,
        'target_interval_start': (when+timedelta(seconds=1)).isoformat(),
        'target_interval_end': (when+timedelta(seconds=2)).isoformat(), 'target_price': 12}


def execution_no_fill(c, row):
    return dict(nonqualify(row), fill_state='NO_FILL', execution_policy_sha256=c['execution_policy_sha256'])


def population(c, payloads):
    members = sorted(p['security_id'] for p in payloads)
    obj = {'session_date': c['official_session_dates'][0], 'security_ids': members,
           'members_sha256': k.digest(members), 'expected_decision_count': sum(len(p['features']['decisions']) for p in payloads)}
    obj['population_day_sha256'] = k.digest(obj)
    for p in payloads:
        p['population_day_sha256'] = obj['population_day_sha256']
    return obj


def population_binding(c, pop):
    evidence = {'version': 'EQ20_DATED_POPULATION_CERTIFICATE_V1', **k.scope(c),
        'session_date': pop['session_date'], 'population_day_sha256': pop['population_day_sha256'],
        'members_sha256': pop['members_sha256'], 'security_session_count': len(pop['security_ids']),
        'expected_decision_count': pop['expected_decision_count'], 'point_in_time_membership_verified': True,
        'source_completeness_reviewed': True, 'no_outcome_based_population_filtering': True, 'full_natural_population': True}
    raw = k.canonical_bytes(evidence).decode()
    ref = {'kind': 'SUCCESSOR_DATED_POPULATION_CERTIFICATE', 'artifact_key': 'SYNTHETIC_POP_'+pop['session_date'],
           'status': 'VERIFIED_POINT_IN_TIME_FULL_NATURAL_POPULATION', 'implementation_sha256': k.digest(evidence)}
    return {'reference': ref, 'artifact': dict(ref, evidence_text=raw, created_at='2027-01-05T00:00:00Z')}


class ProspectiveKernelTest(unittest.TestCase):
    def test_first_signal_and_union_are_independent_feature_only_selections(self):
        c, p = fixture()
        selected = k.select_first_signals(c, p['features'])
        self.assertEqual(selected['first_indices'], {'A': 1, 'B': 0})
        self.assertEqual(selected['union_first_index'], 0)
        self.assertEqual(selected['union_attribution'], 'B')
        receipt = k.process_security_session(c, p)
        self.assertEqual(receipt['claim_slots'][0]['n_nonqualify'], 1)
        self.assertEqual(receipt['claim_slots'][2]['n_success'], 1)
        self.assertEqual(receipt['claim_slots'][10]['n_success'], 1)
        self.assertEqual(receipt['claim_slots'][11]['n_nonqualify'], 1)
        self.assertEqual(len(receipt['claim_slots']), 12)
        # A favourable later label cannot move an already frozen first alert.
        p['reference_labels'][2] = qualify(p['features']['decisions'][2])
        self.assertEqual(k.process_security_session(c, p)['first_signal_selection'], selected)

    def test_union_tie_attribution_is_deterministic(self):
        c, p = fixture(); p['features']['decisions'][0]['features']['x']['value'] = 1
        selected = k.select_first_signals(c, p['features'])
        self.assertEqual(selected['union_attribution'], 'A')
        c['union_ids'] = ['B', 'A']; c['candidate_family_sha256'] = k.digest({'rules': c['frozen_rules'], 'union_ids': c['union_ids']})
        self.assertEqual(k.select_first_signals(c, p['features'])['union_attribution'], 'A')

    def test_unknown_negation_and_false_and_unknown(self):
        c, p = fixture()
        c['frozen_rules'][0]['gates'] = [
            {'feature': 'x', 'operator': 'ge', 'threshold': 1, 'negate': True},
            {'feature': 'y', 'operator': 'ge', 'threshold': 1, 'negate': False}]
        c['candidate_family_sha256'] = k.digest({'rules': c['frozen_rules'], 'union_ids': c['union_ids']})
        for row in p['features']['decisions']:
            row['features']['x'] = {'state': 'SOURCE_NOT_COVERED', 'value': None, 'available_at': None}
        p['features']['decisions'][0]['features']['y']['value'] = 0
        result = k.select_first_signals(c, p['features'])
        self.assertIsNone(result['first_indices']['A'])
        self.assertEqual(result['unknown_gate_decisions']['A'], 2)

    def test_frozen_unknown_threshold_abstains(self):
        c, p = fixture(); c['frozen_rules'][0]['gates'][0]['threshold'] = None
        c['candidate_family_sha256'] = k.digest({'rules': c['frozen_rules'], 'union_ids': c['union_ids']})
        self.assertIsNone(k.select_first_signals(c, p['features'])['first_indices']['A'])

    def test_unknown_reference_abstains_and_keeps_decision(self):
        c, p = fixture(); row = p['features']['decisions'][0]
        row['reference_state'] = 'NO_DATA'; row['p_reference'] = None
        p['reference_labels'][0] = None
        result = k.process_security_session(c, p)
        self.assertEqual(result['scheduled_decisions'], 3)
        self.assertEqual(result['first_signal_selection']['first_indices']['B'], 1)
        self.assertEqual(result['opportunity_state'], 'UNRESOLVED')

    def test_unknown_and_no_fill_alerts_stay_in_both_denominators(self):
        c, p = fixture(); p['reference_labels'][1] = None; p['execution_labels'][1] = None
        receipt = k.process_security_session(c, p)
        for i in (0, 1):
            self.assertEqual(receipt['claim_slots'][i]['n_signals'], 1)
            self.assertEqual(receipt['claim_slots'][i]['n_unresolved'], 1)
        self.assertEqual(receipt['claim_slots'][3]['n_signals'], 1)
        self.assertEqual(receipt['claim_slots'][3]['n_nonqualify'], 1)

    def test_missing_or_shifted_decision_rejected(self):
        for mutation in ('drop', 'clock'):
            c, p = fixture()
            if mutation == 'drop':
                p['features']['decisions'].pop()
            else:
                row = p['features']['decisions'][1]
                row['decision_ts'] = (k.stamp(row['decision_ts'])+timedelta(seconds=1)).isoformat()
            with self.assertRaises(k.Closed):
                k.select_first_signals(c, p['features'])

    def test_late_or_numeric_unknown_feature_rejected(self):
        for mutation in ('late', 'unknown', 'zero'):
            c, p = fixture(); cell = p['features']['decisions'][0]['features']['x']
            if mutation == 'late': cell['available_at'] = (k.stamp(cell['available_at'])+timedelta(microseconds=1)).isoformat()
            elif mutation == 'unknown': cell['state'] = 'STALE'
            else: cell['state'] = 'OBSERVED_ZERO'; cell['value'] = 3
            with self.assertRaises(k.Closed): k.select_first_signals(c, p['features'])

    def test_reference_must_be_completed_available_and_fresh(self):
        for mutation in ('incomplete', 'late', 'stale', 'zero'):
            c, p = fixture(); row = p['features']['decisions'][0]; when = k.stamp(row['decision_ts'])
            if mutation == 'incomplete': row['reference_bar_end'] = (when+timedelta(seconds=1)).isoformat()
            elif mutation == 'late': row['reference_available_at'] = (when+timedelta(seconds=1)).isoformat()
            elif mutation == 'stale': row['reference_bar_end'] = (when-timedelta(seconds=61)).isoformat()
            else: row['p_reference'] = 0
            with self.assertRaises(k.Closed): k.select_first_signals(c, p['features'])

    def test_partial_pre_decision_bar_cannot_prove_target(self):
        c, p = fixture(); p['reference_labels'][0]['target_interval_start'] = p['features']['decisions'][0]['decision_ts']
        with self.assertRaisesRegex(k.Closed, 'STRICT_POST_ANCHOR'):
            k.process_security_session(c, p)

    def test_incomplete_nonqualify_and_label_misalignment_rejected(self):
        for mutation in ('coverage', 'ambiguity', 'alignment', 'target'):
            c, p = fixture()
            if mutation == 'coverage': p['reference_labels'][1]['coverage_complete'] = False
            elif mutation == 'ambiguity': p['reference_labels'][1]['ordering_ambiguity'] = True
            elif mutation == 'alignment': p['reference_labels'][1]['decision_ts'] = p['reference_labels'][0]['decision_ts']
            else: p['reference_labels'][0]['target_price'] = 11.999999
            with self.assertRaises(k.Closed): k.process_security_session(c, p)

    def test_executable_entry_uses_same_alert_and_120_second_manual_latency(self):
        c, p = fixture(); row = p['features']['decisions'][0]; when = k.stamp(row['decision_ts'])
        fill = when+timedelta(seconds=123)
        good = dict(qualify(row), fill_state='FILLED', execution_policy_sha256=c['execution_policy_sha256'],
            p_entry=11, filled_quantity=2, alert_received_ts=(when+timedelta(seconds=2)).isoformat(),
            entry_order_ts=(when+timedelta(seconds=122)).isoformat(), entry_fill_ts=fill.isoformat(),
            target_interval_start=(fill+timedelta(seconds=1)).isoformat(),
            target_interval_end=(fill+timedelta(seconds=2)).isoformat(), target_price=13.2)
        p['execution_labels'][0] = good
        self.assertEqual(k.process_security_session(c, p)['claim_slots'][3]['n_success'], 1)
        good['entry_order_ts'] = (when+timedelta(seconds=121)).isoformat()
        with self.assertRaisesRegex(k.Closed, 'EXECUTION_TIMING'):
            k.process_security_session(c, p)

    def test_nonfill_and_unassessable_cannot_be_relabelled_success(self):
        for fill_state in ('NO_FILL', 'EXPIRED', 'HALT_BLOCKED', 'UNASSESSABLE'):
            c, p = fixture(); p['execution_labels'][0].update(state='QUALIFY', fill_state=fill_state)
            with self.assertRaises(k.Closed): k.process_security_session(c, p)

    def test_complete_population_and_zero_alert_day_preserved(self):
        c, p = fixture(); p2 = deepcopy(p); p2['security_id'] = p2['features']['security_id'] = 'S2'; p2['issuer_id'] = 'I2'
        for payload in (p, p2):
            for row in payload['features']['decisions']:
                for cell in row['features'].values(): cell['value'] = 0
        pop = population(c, [p, p2]); receipts = [k.process_security_session(c, x) for x in (p, p2)]
        day = k.aggregate_day(c, pop, receipts, population_binding(c, pop))
        self.assertEqual(day['population_members'], 2)
        self.assertEqual(day['scheduled_decisions'], 6)
        self.assertTrue(day['zero_alert_dates_retained'])
        self.assertTrue(all(x['n_signals'] == 0 for x in day['claim_rows']))
        with self.assertRaisesRegex(k.Closed, 'UNPROCESSED_POPULATION'):
            k.aggregate_day(c, pop, receipts[:1], population_binding(c, pop))
        with self.assertRaisesRegex(k.Closed, 'IDENTITY_OR_SCOPE'):
            k.aggregate_day(c, pop, [receipts[0], receipts[0]], population_binding(c, pop))

    def test_receipt_corruption_and_execution_denominator_drop_rejected(self):
        c, p = fixture(); pop = population(c, [p]); receipt = k.process_security_session(c, p)
        receipt['claim_slots'][1]['n_signals'] = 0
        with self.assertRaisesRegex(k.Closed, 'CONTENT_HASH'):
            k.aggregate_day(c, pop, [receipt], population_binding(c, pop))
        receipt['claim_slots'][1]['n_nonqualify'] = 0
        receipt['receipt_sha256'] = k.digest({x: v for x, v in receipt.items() if x != 'receipt_sha256'})
        with self.assertRaisesRegex(k.Closed, 'DENOMINATOR_CANNOT_DROP'):
            k.aggregate_day(c, pop, [receipt], population_binding(c, pop))

    def test_contract_cannot_silently_add_candidate_or_extend_dates(self):
        for mutation in ('refit', 'sample', 'candidate', 'date', 'clock'):
            c, p = fixture()
            if mutation == 'refit': c['thresholds_refitted'] = True
            elif mutation == 'sample': c['sampling_design'] = {'denominator': 128}
            elif mutation == 'candidate': c['frozen_rules'][0]['gates'][0]['threshold'] = 2
            elif mutation == 'date': c['official_session_dates'].pop()
            else: p['features']['regular_close'] = (k.stamp(p['features']['regular_close'])+timedelta(minutes=1)).isoformat()
            with self.assertRaises(k.Closed): k.select_first_signals(c, p['features'])

    def test_self_hashed_population_is_not_an_authoritative_dated_certificate(self):
        c, p = fixture(); pop = population(c, [p]); receipt = k.process_security_session(c, p)
        with self.assertRaisesRegex(k.Closed, 'REGISTRY_BINDING_REQUIRED'):
            k.aggregate_day(c, pop, [receipt], {})
        binding = population_binding(c, pop)
        changed = deepcopy(pop); changed['expected_decision_count'] += 3
        changed['population_day_sha256'] = k.digest({x: v for x, v in changed.items() if x != 'population_day_sha256'})
        with self.assertRaisesRegex(k.Closed, 'SCOPE_OR_DENOMINATOR'):
            k.aggregate_day(c, changed, [receipt], binding)

    def test_cross_contract_receipts_are_not_authorized_by_self_hash(self):
        c, p = fixture(); pop = population(c, [p]); receipt = k.process_security_session(c, p)
        changed = deepcopy(c); changed['maximum_reference_staleness_seconds'] = 61
        with self.assertRaisesRegex(k.Closed, 'REGISTERED_CONTRACT_SCOPE'):
            k.aggregate_day(changed, pop, [receipt], population_binding(changed, pop))

    def test_fixed_horizon_dispatch_does_not_promote_activation_to_scientific_proof(self):
        c, p = fixture(); pop = population(c, [p]); day = k.aggregate_day(c, pop, [k.process_security_session(c, p)], population_binding(c, pop))
        days = []
        for d in c['official_session_dates']:
            copied = deepcopy(day); copied['session_date'] = d
            for row in copied['claim_rows']: row['session_date'] = d
            copied['receipt_sha256'] = k.digest({x: v for x, v in copied.items() if x != 'receipt_sha256'})
            days.append(copied)
        spec2 = importlib.util.spec_from_file_location('eq20_inference_for_prospective_test', ROOT/'app/eq20_cluster_inference.py')
        ev = importlib.util.module_from_spec(spec2); spec2.loader.exec_module(ev)
        registration = {**k.scope(c), 'official_session_dates': c['official_session_dates'],
                        'point_in_time_population_sha256': c['population_manifest_sha256']}
        gates = {'activation_state': 'VERIFIED', 'dated_population_certificate_vector_sha256':
            k.digest([{'session_date': d['session_date'], 'population_day_sha256': d['population_day_sha256'],
                       'certificate_sha256': d['dated_population_certificate_sha256']} for d in days])}
        result = k.evaluate_fixed_horizon_claim(c, 0, days, registration, gates, ev)
        self.assertEqual(result['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')
        self.assertIn('all_receipts_resolved_and_verified', result['dependencies'])
        self.assertFalse(result['outcomes_used_for_inference'])
        with self.assertRaisesRegex(k.Closed, 'FIXED_HORIZON_COMPLETE'):
            k.evaluate_fixed_horizon_claim(c, 0, days[:-1], registration, {}, ev)


if __name__ == '__main__':
    unittest.main()
