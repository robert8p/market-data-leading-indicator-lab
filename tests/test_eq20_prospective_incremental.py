"""Incremental cursor and first-alert semantics on public synthetic fixtures."""
from copy import deepcopy
from datetime import timedelta
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    obj = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(obj)
    return obj


p = load('incremental_test_subject', ROOT/'app/eq20_prospective_incremental.py')
f = load('incremental_kernel_fixture', ROOT/'tests/test_eq20_prospective_kernel.py')


def prepared(c):
    # Pure state helper only. This fixture never represents a producer release,
    # source readback, market input or resource admission.
    obj = p.Prepared.__new__(p.Prepared)
    obj.kernel = f.k
    obj.contract = deepcopy(c)
    obj.rules, obj.union_ids, obj.feature_names = f.k.family(c)
    obj.scope = f.k.scope(c)
    obj.source_release_sha256 = '1'*64
    obj.release_sha256 = '2'*64
    obj.private_module_sha256 = '3'*64
    obj.grids = {}
    for session in c['official_sessions']:
        at = f.k.stamp(session['open_at'])+timedelta(minutes=10, seconds=c['decision_offset_seconds'])
        grid = []
        while at <= f.k.stamp(session['close_at'])-timedelta(minutes=60):
            grid.append(at); at += timedelta(minutes=1)
        obj.grids[session['session_date']] = tuple(grid)
    return obj


def receipt(obj, payload, index):
    row = deepcopy(payload['features']['decisions'][index])
    value = {'version': p.VERSION, **obj.scope, 'session_date': payload['session_date'],
        'security_id': payload['security_id'], 'population_day_sha256': payload['population_day_sha256'],
        'decision_index': index, 'decision_ts': row['decision_ts'], 'decision_row': row,
        'decision_row_sha256': p.digest(row), 'source_sequence': index,
        'raw_prefix_sha256': p.digest(['SYNTHETIC_PREFIX', index]), 'prefix_receipt_sha256': '4'*64,
        'producer_release_sha256': obj.source_release_sha256, 'incremental_release_sha256': obj.release_sha256,
        'private_module_sha256': obj.private_module_sha256, 'feature_only': True, 'outcome_label_arrays_opened': False}
    return dict(value, receipt_sha256=p.digest(value))


def run(obj, payload):
    state = None
    results = []
    for index in range(len(payload['features']['decisions'])):
        state = obj.advance_first_signals(state, receipt(obj, payload, index))
        results.append(state)
    return state, results


class IncrementalTest(unittest.TestCase):
    def assert_full_parity(self, contract, payload):
        obj = prepared(contract)
        state, rows = run(obj, payload)
        original = f.k.select_first_signals(contract, payload['features'])
        for key in ('first_indices', 'union_first_index', 'union_attribution', 'unknown_gate_decisions', 'selection_sha256'):
            self.assertEqual(state[key], original[key])
        self.assertTrue(state['complete'])
        self.assertEqual(state['next_decision_index'], len(payload['features']['decisions']))
        return obj, state, rows

    def test_exact_first_signal_and_union_emitted_once_and_prefix_chain_advances(self):
        c, payload = f.fixture()
        obj, state, rows = self.assert_full_parity(c, payload)
        self.assertEqual(set(rows[0]['new_first_signals']), {'B', '__UNION__'})
        self.assertEqual(set(rows[1]['new_first_signals']), {'A'})
        self.assertEqual(rows[2]['new_first_signals'], {})
        self.assertEqual(state['first_signals']['__UNION__']['decision_index'], 0)
        self.assertEqual(state['first_signals']['__UNION__']['attribution_rule_id'], 'B')
        self.assertEqual(rows[1]['previous_state_sha256'], rows[0]['state_sha256'])
        self.assertEqual(len({r['feature_prefix_chain_sha256'] for r in rows}), 3)

    def test_union_tie_attribution_is_original_lexicographic_choice(self):
        c, payload = f.fixture()
        payload['features']['decisions'][0]['features']['x']['value'] = 1
        c['union_ids'] = ['B', 'A']
        c['candidate_family_sha256'] = p.digest({'rules': c['frozen_rules'], 'union_ids': c['union_ids']})
        _, state, rows = self.assert_full_parity(c, payload)
        self.assertEqual(state['union_attribution'], 'A')
        self.assertEqual(rows[0]['new_first_signals']['__UNION__']['attribution_rule_id'], 'A')

    def test_false_and_unknown_negation_and_unknown_reference_match_original(self):
        for mode in ('AND_UNKNOWN', 'NULL_THRESHOLD', 'UNKNOWN_REFERENCE'):
            c, payload = f.fixture()
            if mode == 'AND_UNKNOWN':
                c['frozen_rules'][0]['gates'] = [
                    {'feature': 'x', 'operator': 'ge', 'threshold': 1, 'negate': True},
                    {'feature': 'y', 'operator': 'ge', 'threshold': 1, 'negate': False}]
                for row in payload['features']['decisions']:
                    row['features']['x'] = {'state': 'SOURCE_NOT_COVERED', 'value': None, 'available_at': None}
                payload['features']['decisions'][0]['features']['y']['value'] = 0
            elif mode == 'NULL_THRESHOLD':
                c['frozen_rules'][0]['gates'][0]['threshold'] = None
            else:
                payload['features']['decisions'][0].update(reference_state='NO_DATA', p_reference=None)
            c['candidate_family_sha256'] = p.digest({'rules': c['frozen_rules'], 'union_ids': c['union_ids']})
            self.assert_full_parity(c, payload)

    def test_exact_cursor_no_skip_no_reexecution_and_previous_integrity(self):
        c, payload = f.fixture(); obj = prepared(c)
        with self.assertRaisesRegex(p.Closed, 'START_GRID'):
            obj.advance_first_signals(None, receipt(obj, payload, 1))
        first = obj.advance_first_signals(None, receipt(obj, payload, 0))
        for index in (0, 2):
            with self.assertRaisesRegex(p.Closed, 'PRIOR_STATE_SCOPE_AND_CURSOR'):
                obj.advance_first_signals(first, receipt(obj, payload, index))
        first['first_indices']['A'] = 0
        with self.assertRaisesRegex(p.Closed, 'PRIOR_STATE_SCOPE_AND_CURSOR'):
            obj.advance_first_signals(first, receipt(obj, payload, 1))

    def test_cross_contract_date_member_and_population_state_cannot_mix(self):
        c, payload = f.fixture(); obj = prepared(c)
        first = obj.advance_first_signals(None, receipt(obj, payload, 0))
        for key, value in (('security_id', 'OTHER'), ('population_day_sha256', 'f'*64),
                           ('session_date', c['official_session_dates'][1])):
            next_receipt = receipt(obj, payload, 1)
            next_receipt[key] = value
            next_receipt['receipt_sha256'] = p.digest({k: v for k, v in next_receipt.items() if k != 'receipt_sha256'})
            with self.assertRaises(ValueError): obj.advance_first_signals(first, next_receipt)
        next_receipt = receipt(obj, payload, 1)
        next_receipt['execution_policy_sha256'] = 'f'*64
        next_receipt['receipt_sha256'] = p.digest({k: v for k, v in next_receipt.items() if k != 'receipt_sha256'})
        with self.assertRaisesRegex(p.Closed, 'SCOPE_AND_CONTENT_CHAIN'):
            obj.advance_first_signals(first, next_receipt)

    def test_reference_staleness_postdecision_features_and_unknown_numeric_rejected(self):
        for mode in ('STALE_REFERENCE', 'FUTURE_FEATURE', 'UNKNOWN_NUMERIC'):
            c, payload = f.fixture(); obj = prepared(c)
            row = payload['features']['decisions'][0]
            if mode == 'STALE_REFERENCE':
                row['reference_bar_end'] = (f.k.stamp(row['decision_ts'])-timedelta(seconds=61)).isoformat()
            elif mode == 'FUTURE_FEATURE':
                row['features']['x']['available_at'] = (f.k.stamp(row['decision_ts'])+timedelta(seconds=1)).isoformat()
            else:
                row['features']['x'].update(state='SOURCE_NOT_COVERED', value=0, available_at=None)
            with self.assertRaises(ValueError): obj.advance_first_signals(None, receipt(obj, payload, 0))

    def test_registry_status_hash_and_installed_component_are_enforced(self):
        ref = {'kind': 'EXPECTED', 'status': 'VERIFIED', 'artifact_key': 'SYNTHETIC', 'implementation_sha256': p.digest({})}
        binding = {'reference': ref, 'artifact': dict(ref, evidence_text='{}')}
        self.assertEqual(p.resolve(binding, 'EXPECTED', 'VERIFIED'), {})
        binding['artifact']['evidence_text'] += ' '
        with self.assertRaisesRegex(p.Closed, 'READBACK_HASH'): p.resolve(binding, 'EXPECTED', 'VERIFIED')
        with self.assertRaisesRegex(p.Closed, 'INSTALLED_COMPONENT_PIN'):
            p.helper('eq20_prospective_kernel', '0'*64)

    def test_outcomes_are_never_consulted_by_first_signal_state(self):
        c, payload = f.fixture()
        class Protected:
            def __iter__(self): raise AssertionError('protected outcome read')
        payload['reference_labels'] = Protected()
        payload['execution_labels'] = Protected()
        self.assert_full_parity(c, payload)


if __name__ == '__main__':
    unittest.main()
