"""Synthetic registry documents only; no market outcomes or remote mutations."""
import copy
from datetime import date, timedelta
from fractions import Fraction
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch


spec = importlib.util.spec_from_file_location('eq20_bindings_test',
    Path(__file__).parents[1] / 'app' / 'eq20_evidence_bindings.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def row(kind, key, status, evidence):
    text = json.dumps(evidence, sort_keys=True, ensure_ascii=False)
    return dict(kind=kind, artifact_key=key, status=status,
                evidence_text=text, implementation_sha256=hashlib.sha256(text.encode()).hexdigest(),
                created_at='2028-01-02T00:00:00Z')


def reference(r):
    return {k: r[k] for k in ('kind', 'artifact_key', 'status', 'implementation_sha256')}


def fixture(start=date(2028, 1, 3)):
    dates = []
    day = start
    while len(dates) < 252:
        if day.weekday() < 5:
            dates.append(day.isoformat())
        day += timedelta(days=1)
    plan = dict(version='EQ20_SUCCESSOR_ACTIVATION_PLAN_V1',
        candidate_family_sha256='a'*64, population_manifest_sha256='b'*64,
        official_calendar_sha256='c'*64, official_session_dates=dates,
        official_session_dates_sha256=m.canonical_hash(dates), execution_policy_sha256='d'*64,
        method_contract_sha256=m.METHOD_CONTRACT_SHA256, fixed_horizon_sessions=252,
        claim_slots=12, outcome_based_stopping_or_extension=False)
    plan['scope_sha256'] = m.canonical_hash({k: plan[k] for k in m.SCOPE_KEYS})
    facts = {role: dict(scope_sha256=plan['scope_sha256'], protected_outcomes_accessed=False)
             for role in m.ROLE_SPECS}
    facts['method'].update(version=m.METHOD_VERSION, contract_sha256=m.METHOD_CONTRACT_SHA256,
        implementation=dict(source_sha256=m.METHOD_SOURCE_SHA256))
    facts['math_review'].update(reviewed_source_sha256=m.METHOD_SOURCE_SHA256,
        contract_sha256_excluding_selfhash=m.METHOD_CONTRACT_SHA256)
    facts['candidate'].update(candidate_family_sha256='a'*64, candidate_count=3,
        selection_evidence_class='DEVELOPMENT', selection_cutoff='2026-05-31', post_freeze_changes=False)
    facts['population'].update(population_manifest_sha256='b'*64, full_natural_population=True)
    facts['calendar'].update(official_calendar_sha256='c'*64,
        first_complete_session_after_registration=dates[0],
        previous_official_session_open_at='2027-12-31T14:30:00Z',
        previous_official_session_close_at='2027-12-31T21:00:00Z',
        sessions=[dict(session_date=d, open_at=d+'T14:30:00Z', close_at=d+'T21:00:00Z') for d in dates])
    facts['execution'].update(execution_policy_sha256='d'*64)
    facts['causal_source'].update(population_manifest_sha256='b'*64, causal_publication_readiness_verified=True)
    facts['exposure'].update(exposure_class='PROSPECTIVE_RESERVED', access_history_complete=True,
        unknown_lineage_access=False, prior_evaluation_access=False,
        exposure_ledger_head_sha256='e'*64, access_ledger_head_sha256='f'*64)
    facts['dependence'].update(conditional_date_independence_supported=True,
        independent_reviewer_id='SYNTHETIC_FIXTURE_REVIEWER', nonrejection_only_justification=False)
    facts['alpha_family'].update(programme_id='EQ20_SUCCESSOR_GLOBAL_ALPHA_V1', global_alpha_fraction='1/20', claim_slots=12,
        unused_alpha_recycled=False, original_and_related_alpha_reconciled=True)
    facts['resource'].update(no_new_paid_cost=True, allocation_sha256='1'*64)
    facts['collector'].update(release_verified=True, protected_access_gate_required=True,
        resumable=True, bounded_retries=True)
    facts['historical_adjudication'].update(original_requirements_preserved=True,
        july_retroactively_certified=False)
    rows = {role: row(kind, 'SYNTHETIC_'+role, status, facts[role])
            for role, (kind, status) in m.ROLE_SPECS.items()}
    facts['dependence']['argument_artifact_sha256'] = rows['dependence_argument']['implementation_sha256']
    rows['dependence'] = row(*m.ROLE_SPECS['dependence'][:1], 'SYNTHETIC_dependence',
                             m.ROLE_SPECS['dependence'][1], facts['dependence'])
    plan['bindings'] = {k: reference(v) for k, v in rows.items()}
    pr = row('SUCCESSOR_EVIDENCE_ACTIVATION_PLAN', 'SYNTHETIC_PLAN', 'REGISTERED_OUTCOME_BLIND', plan)
    return dict(server_time='2028-01-03T00:00:00Z', desired='RUN', dependencies=[], next_epoch=1,
        plan=pr, plan_reference=reference(pr), artifacts=rows,
        exposure_ledger_head_sha256='e'*64, access_ledger_head_sha256='f'*64,
        resource_allocation_available=True, resource_allocation_readback_sha256='1'*64)


def edit_receipt(doc, role, changes):
    r = doc['artifacts'][role]
    payload = json.loads(r['evidence_text']); payload.update(changes)
    doc['artifacts'][role] = row(r['kind'], r['artifact_key'], r['status'], payload)
    p = json.loads(doc['plan']['evidence_text'])
    p['bindings'][role] = reference(doc['artifacts'][role])
    pr = row(doc['plan']['kind'], doc['plan']['artifact_key'], doc['plan']['status'], p)
    doc['plan'] = pr; doc['plan_reference'] = reference(pr)


class EvidenceBindingsTests(unittest.TestCase):
    def test_complete_synthetic_readback_passes_gate_only(self):
        result = m.verify_readiness(fixture())
        self.assertEqual(result['state'], 'VERIFIED', result)
        self.assertFalse(result['research_objective_achieved'])
        self.assertFalse(result['grants_evidence_access'])

    def test_bare_hashes_and_booleans_do_not_resolve_receipts(self):
        doc = fixture(); doc['artifacts'] = {k: {'verified': True, 'sha256': 'a'*64} for k in m.ROLE_SPECS}
        self.assertEqual(m.verify_readiness(doc)['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_tampered_actual_text_rejected(self):
        doc = fixture(); doc['artifacts']['dependence']['evidence_text'] += ' '
        self.assertIn('ARTIFACT_READBACK_HASH_MISMATCH', m.verify_readiness(doc)['dependencies'])

    def test_renamed_or_wrong_status_receipt_rejected(self):
        for field, value in [('artifact_key', 'OTHER'), ('status', 'VERIFIED')]:
            with self.subTest(field=field):
                doc = fixture(); doc['artifacts']['exposure'][field] = value
                self.assertEqual(m.verify_readiness(doc)['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_unknown_exposure_rejected_even_with_valid_hash(self):
        doc = fixture(); edit_receipt(doc, 'exposure', {'unknown_lineage_access': True})
        self.assertIn('EXPOSURE_HISTORY_NOT_CLEARED', m.verify_readiness(doc)['dependencies'])

    def test_changed_exposure_or_access_ledger_rejected(self):
        for key in ['exposure_ledger_head_sha256', 'access_ledger_head_sha256']:
            doc = fixture(); doc[key] = '0'*64
            self.assertIn('EXPOSURE_ACCESS_LEDGER_CHANGED_SINCE_ATTESTATION', m.verify_readiness(doc)['dependencies'])

    def test_declared_independence_without_passed_argument_rejected(self):
        doc = fixture(); edit_receipt(doc, 'dependence', {'argument_artifact_sha256': '0'*64})
        self.assertIn('ACTUAL_CONDITIONAL_DEPENDENCE_JUSTIFICATION_REQUIRED', m.verify_readiness(doc)['dependencies'])

    def test_nonrejection_alone_is_not_dependence_justification(self):
        doc = fixture(); edit_receipt(doc, 'dependence', {'nonrejection_only_justification': True})
        self.assertEqual(m.verify_readiness(doc)['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_resource_boolean_without_actual_allocation_does_not_pass(self):
        doc = fixture(); doc['resource_allocation_readback_sha256'] = None
        self.assertIn('ACTUAL_FINITE_RESOURCE_RESERVATION_REQUIRED', m.verify_readiness(doc)['dependencies'])

    def test_multiplicity_recycling_blocked(self):
        doc = fixture(); edit_receipt(doc, 'alpha_family', {'unused_alpha_recycled': True})
        self.assertIn('GLOBAL_ALPHA_PROVENANCE_REQUIRED', m.verify_readiness(doc)['dependencies'])

    def test_first_session_already_open_blocks_retroactive_registration(self):
        doc = fixture(); doc['server_time'] = '2028-01-03T14:30:00Z'
        self.assertIn('ALL_BINDINGS_AND_ACTIVATION_MUST_PRECEDE_FIRST_SESSION', m.verify_readiness(doc)['dependencies'])

    def test_calendar_middle_substitution_rejected(self):
        doc = fixture(); payload = json.loads(doc['artifacts']['calendar']['evidence_text'])
        payload['sessions'][9]['session_date'] = '2028-01-01'
        edit_receipt(doc, 'calendar', payload)
        self.assertIn('OFFICIAL_CALENDAR_VECTOR_MISMATCH', m.verify_readiness(doc)['dependencies'])

    def test_june_july_is_never_prospective_confirmation(self):
        doc = fixture(date(2026, 6, 1))
        self.assertIn('JUNE_JULY_RELATED_LINEAGE_EXPOSED', m.verify_readiness(doc)['dependencies'])

    def test_stop_or_server_denial_cannot_be_removed_by_python(self):
        doc = fixture(); doc['desired'] = 'STOP'
        self.assertIn('EXPLICIT_STOP_OR_CANCEL', m.verify_readiness(doc)['dependencies'])
        doc = fixture(); doc['dependencies'] = ['RESOURCE_BUSY']
        self.assertIn('RESOURCE_BUSY', m.verify_readiness(doc)['dependencies'])

    def test_development_candidate_cannot_include_june_selection(self):
        doc = fixture(); edit_receipt(doc, 'candidate', {'selection_cutoff': '2026-06-01'})
        self.assertIn('DEVELOPMENT_ONLY_SELECTION_REQUIRED', m.verify_readiness(doc)['dependencies'])

    def test_runtime_pin_is_actual_file_hash(self):
        with patch.object(m.Path, 'read_bytes', return_value=b'changed evaluator'):
            self.assertIn('DEPLOYED_EVALUATOR_PIN_MISMATCH', m.verify_readiness(fixture())['dependencies'])

    def test_startup_trigger_is_not_timer_proof(self):
        calls = []
        class RPC:
            def call(self, op, owner, args):
                calls.append((op, args.copy()))
                return fixture() if op == 'readiness' else {'activation_key': 'SYNTHETIC_ACTIVATION',
                    'protected_outcomes_accessed': False, 'committed': True}
        m.supervise_once(RPC(), 'test_owner')
        self.assertEqual(calls[0][1]['trigger'], 'PERSISTENT_WORKER_STARTUP')
        self.assertEqual(calls[1][0], 'activate')
        self.assertEqual(calls[2][0], 'ack_activation')

    def test_blocked_gate_is_read_only_without_activation(self):
        calls=[]
        class RPC:
            def call(self, op, owner, args):
                calls.append(op)
                if op == 'readiness':
                    return dict(desired='RUN', server_time='2027-12-02T00:00:00Z', dependencies=['CANDIDATE_MISSING'])
                return {'protected_outcomes_accessed': False}
        m.supervise_once(RPC(), 'test_owner', trigger='PERSISTENT_WORKER_TIMER')
        self.assertEqual(calls, ['readiness'])

    def test_global_alpha_receipt_is_not_tied_to_each_epoch_calendar(self):
        doc = fixture(); edit_receipt(doc, 'alpha_family', {'scope_sha256': 'GLOBAL_PROGRAMME_SCOPE'})
        self.assertEqual(m.verify_readiness(doc)['state'], 'VERIFIED')

    def test_official_session_cannot_be_skipped_before_start(self):
        doc = fixture()
        edit_receipt(doc, 'calendar', {'previous_official_session_open_at': '2028-01-03T01:00:00Z',
                                     'previous_official_session_close_at': '2028-01-03T02:00:00Z'})
        self.assertIn('OFFICIAL_SESSION_SKIPPED_BEFORE_REGISTERED_START', m.verify_readiness(doc)['dependencies'])

    def test_malformed_nested_record_fails_closed(self):
        for role, payload in [('method', {'implementation': 'not an object'}),
                              ('calendar', {'sessions': [None]*252})]:
            with self.subTest(role=role):
                doc = fixture(); edit_receipt(doc, role, payload)
                self.assertEqual(m.verify_readiness(doc)['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_epoch_allocation_is_immutable_summable_no_recycling(self):
        self.assertEqual(m.immutable_epoch_alpha(1), Fraction(1,480))
        self.assertEqual(sum(12*m.immutable_epoch_alpha(k) for k in range(1,101)), Fraction(5,101))
        for value in [0, -1, 1.0, True, 1000001]:
            with self.assertRaises(m.BindingClosed): m.immutable_epoch_alpha(value)

    def test_automated_epoch_cannot_repeat_fixed_window_infeasibility(self):
        doc = fixture(); doc['next_epoch'] = 40
        self.assertEqual(m.verify_readiness(doc)['state'], 'VERIFIED')
        doc['next_epoch'] = 41
        self.assertIn('FIXED_HORIZON_ALPHA_INFORMATION_INFEASIBLE', m.verify_readiness(doc)['dependencies'])

    def test_retry_throttle_does_not_create_second_call(self):
        with patch.object(m, '_next_timer_at', 200), patch.object(m.time, 'monotonic', return_value=100):
            self.assertTrue(m.timer_tick('owner')['throttled'])


if __name__ == '__main__':
    unittest.main()
