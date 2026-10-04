"""Synthetic scientific-boundary and immutable-recovery tests; no market reads."""
import copy
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
import tempfile
from types import SimpleNamespace
import zlib

PATH = Path(__file__).parents[1] / 'app' / 'eq20_mission_continuation.py'
spec = importlib.util.spec_from_file_location('mission_test', PATH)
mission = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mission)


def fixture():
    ids = ['W10_%024d' % i for i in range(1000)]
    windows = [dict(train_start='2025-09-01', train_end='2025-12-31',
                    test_start='2026-01-01', test_end='2026-01-31') for _ in range(5)]
    windows.append(dict(train_start='2025-09-01', train_end='2026-05-31', test_start=None, test_end=None))
    scope = dict(wave='W10', registered_scope_sha256=mission.SCOPE_SHA256,
                 research_mode='PROBABILITY_SAMPLE_DISCOVERY_ONLY',
                 sampling_design={'nominal_inclusion_probability': '1/128'},
                 statistical_qualification=False, execution_evaluation=False,
                 template_count=1000, registered_templates=[{'rule_id': rule} for rule in ids])
    scope_sha = mission.object_hash(scope)
    files = []
    raw = {}
    def add(name, value, encoded=False):
        data = value if encoded else mission.canonical_bytes(value) + b'\n'
        raw[name] = data
        files.append(dict(name=name, raw_sha256=mission.digest(data), raw_bytes=len(data),
                          blob_sha256='b' * 64, encoded_bytes=len(data), chunks=1))
    add('wave_scope.json', scope)
    cp = dict(state='W10_SAMPLE_DEVELOPMENT_COMPLETE_NO_RULE_QUALIFIED',
              wave_scope_sha256=scope_sha, completed_folds=list(range(6)),
              trial_records=6000, protected_outcomes_accessed=False,
              active_stage=None, stage_commits={}, fold_sha256={})
    ledger = bytearray()
    for index in range(6):
        rules = [dict(rule_id=rule, gates=[dict(feature='x', threshold=1)]) for rule in ids]
        metric = dict(verified_successful_first_signals=0,
                      exploratory_inverse_probability_estimates={'conservative_contamination_ratio': None})
        train = dict(rules={rule: metric for rule in ids})
        test = copy.deepcopy(train) if index < 5 else None
        fold = dict(fold_index=index, dates=windows[index], fitted_rules=rules,
                    train=train, forward_test=test, training_selected_ids=[], wave_scope_sha256=scope_sha)
        add(f'fold_{index}.json', fold)
        cp['fold_sha256'][str(index)] = files[-1]['raw_sha256']
        for kind in (('fit', 'train', 'test') if index < 5 else ('fit', 'train')):
            name = f'stage_{index}_{kind}.json'
            add(name, {'synthetic_stage': name})
            cp['stage_commits'][f'{index}_{kind}'] = dict(path=name, sha256=files[-1]['raw_sha256'])
        for rule in rules:
            trial = dict(wave_scope_sha256=scope_sha, fold_index=index, rule_id=rule['rule_id'],
                         fit_sha256=mission.object_hash(rule), train=metric,
                         forward_test=metric if test else None,
                         selected_from_training=False, status='DEVELOPMENT_TRIAL_COMPLETE')
            ledger.extend(mission.canonical_bytes(trial) + b'\n')
    add('trial_ledger.jsonl', bytes(ledger), encoded=True)
    cp['trial_ledger_sha256'] = mission.digest(bytes(ledger))
    cp['checkpoint_sha256'] = mission.object_hash(cp)
    add('checkpoint.json', cp)
    snapshot = dict(snapshot_id='synthetic-final', lane='discovery', runner_stage='COMPLETE',
                    pending_attempt=None, unsettled_attempts=0, files=files,
                    report=dict(run_complete=True, success=True, protected_outcomes_accessed=False,
                                actual_trial_records=6000))
    return snapshot, raw, ids, windows


def rewrite(snapshot, raw, name, mutate):
    doc = json.loads(raw[name]); mutate(doc)
    if name == 'checkpoint.json':
        doc.pop('checkpoint_sha256'); doc['checkpoint_sha256'] = mission.object_hash(doc)
    raw[name] = mission.canonical_bytes(doc) + b'\n'
    for item in snapshot['files']:
        if item['name'] == name:
            item.update(raw_bytes=len(raw[name]), raw_sha256=mission.digest(raw[name]))


class TerminalAccountingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = fixture()

    def run_case(self, mutate=None):
        snapshot, raw, ids, windows = copy.deepcopy(self.base)
        if mutate:
            mutate(snapshot, raw)
        return mission.verify_w10_terminal_snapshot(snapshot, lambda meta: raw[meta['name']], ids, windows)

    def test_complete_development_receipt_reconstructs_all_6000_trials(self):
        receipt = self.run_case()
        self.assertEqual((receipt['fits'], receipt['templates'], receipt['contexts']), (6000, 1000, 6))
        self.assertTrue(receipt['no_marker_result_preserved'])
        self.assertFalse(receipt['research_objective_achieved'])
        self.assertEqual(receipt['next_stage'], 'FP01_FULL_POPULATION_READINESS_AND_DEVELOPMENT')

    def test_real_terminal_receipt_is_required(self):
        with self.assertRaisesRegex(mission.GateClosed, 'REAL_COMPLETED'):
            self.run_case(lambda s, r: s['report'].update(run_complete=False))

    def test_active_owner_is_never_displaced(self):
        with self.assertRaisesRegex(mission.GateClosed, 'W10_STILL_ACTIVE'):
            self.run_case(lambda s, r: s.update(pending_attempt='healthy'))

    def test_completed_checkpoint_missing_fold_cannot_pass(self):
        with self.assertRaisesRegex(mission.GateClosed, 'INCOMPLETE_OR_DUPLICATE_CONTEXTS'):
            self.run_case(lambda s, r: rewrite(s, r, 'checkpoint.json',
                                             lambda cp: cp.update(completed_folds=[0, 1, 2, 3, 4, 4])))

    def test_wrong_ledger_is_not_reconciled_by_counts(self):
        with self.assertRaisesRegex(mission.GateClosed, 'LEDGER_PROJECTION_MISMATCH'):
            self.run_case(lambda s, r: rewrite(s, r, 'checkpoint.json',
                                             lambda cp: cp.update(trial_ledger_sha256='f' * 64)))

    def test_mutated_verified_file_fails_before_inspection(self):
        with self.assertRaisesRegex(mission.GateClosed, 'IMMUTABLE_FILE_READBACK_MISMATCH'):
            self.run_case(lambda s, r: r.update({'fold_0.json': r['fold_0.json'] + b' '}))

    def test_duplicate_template_cannot_hide_behind_6000_reported_fits(self):
        def bad(s, r):
            rewrite(s, r, 'fold_0.json', lambda f: f['fitted_rules'].__setitem__(1, f['fitted_rules'][0]))
            sha = next(f['raw_sha256'] for f in s['files'] if f['name'] == 'fold_0.json')
            rewrite(s, r, 'checkpoint.json', lambda cp: cp['fold_sha256'].update({'0': sha}))
        with self.assertRaisesRegex(mission.GateClosed, 'MISSING_OR_DUPLICATE_FIT'):
            self.run_case(bad)

    def test_post_test_selected_marker_cannot_replace_training_rule(self):
        def bad(s, r):
            rewrite(s, r, 'fold_0.json', lambda f: f.update(training_selected_ids=['W10_%024d' % 9]))
            sha = next(f['raw_sha256'] for f in s['files'] if f['name'] == 'fold_0.json')
            rewrite(s, r, 'checkpoint.json', lambda cp: cp['fold_sha256'].update({'0': sha}))
        with self.assertRaisesRegex(mission.GateClosed, 'TRAINING_SELECTION_REPLAY_MISMATCH'):
            self.run_case(bad)


class FullPopulationFinalAccountingTests(unittest.TestCase):
    def build(self, output, candidate=True):
        snapshot, raw, ids, windows = fixture()
        scope = dict(wave='FP01', research_mode='FULL_STREAM',
                     source_input_manifest_sha256='c' * 64,
                     template_source_scope_sha256=mission.SCOPE_SHA256,
                     registered_templates=ids)
        scope_sha = mission.object_hash(scope)
        raw['wave_scope.json'] = mission.canonical_bytes(scope) + b'\n'
        cp = json.loads(raw['checkpoint.json'])
        cp.update(state='FP01_FULL_POPULATION_DEVELOPMENT_COMPLETE', wave_scope_sha256=scope_sha)
        ledger = bytearray()
        for index in range(6):
            fold = json.loads(raw[f'fold_{index}.json'])
            fold['wave_scope_sha256'] = scope_sha
            for rule in fold['fitted_rules']:
                rule['gates'][0].update(operator='>=', negate=False)
                selected = candidate and index == 5 and rule['rule_id'] == ids[0]
                fold['train']['rules'][rule['rule_id']] = dict(
                    verified_successful_first_signals=12 if selected else 0,
                    conservative_contamination=0 if selected else None)
                if index < 5:
                    fold['forward_test']['rules'][rule['rule_id']] = dict(
                        verified_successful_first_signals=0, conservative_contamination=None)
            fold['training_selected_ids'] = mission._training_selection(fold, False)
            raw[f'fold_{index}.json'] = mission.canonical_bytes(fold) + b'\n'
            cp['fold_sha256'][str(index)] = mission.digest(raw[f'fold_{index}.json'])
            for rule in fold['fitted_rules']:
                trial = dict(wave_scope_sha256=scope_sha, fold_index=index, rule_id=rule['rule_id'],
                    fit_sha256=mission.object_hash(rule), train=fold['train']['rules'][rule['rule_id']],
                    forward_test=fold['forward_test']['rules'][rule['rule_id']] if index < 5 else None,
                    selected_from_training=rule['rule_id'] in fold['training_selected_ids'],
                    status='DEVELOPMENT_TRIAL_COMPLETE')
                ledger.extend(mission.canonical_bytes(trial) + b'\n')
        raw['trial_ledger.jsonl'] = bytes(ledger)
        cp['trial_ledger_sha256'] = mission.digest(bytes(ledger))
        cp.pop('checkpoint_sha256', None)
        cp['checkpoint_sha256'] = mission.object_hash(cp)
        raw['checkpoint.json'] = mission.canonical_bytes(cp) + b'\n'
        for name, data in raw.items():
            (output / name).write_bytes(data)
        inputs = SimpleNamespace(manifest_sha256='c' * 64, contract={'inner_folds': windows[:5]})
        registered = dict(population_manifest_sha256='d' * 64, immutable_parent_w10_receipt_sha256='e' * 64)
        return cp, inputs, registered, ids

    def test_actual_full_population_ledger_freezes_only_training_selected_rules(self):
        with tempfile.TemporaryDirectory() as tmp:
            cp, inputs, registered, ids = self.build(Path(tmp))
            result = mission.verify_fp01_terminal_output(Path(tmp), cp, inputs, registered)
        self.assertEqual((result['fits'], result['templates'], result['contexts']), (6000, 1000, 6))
        self.assertEqual(result['candidate_count'], 1)
        self.assertEqual(result['union_ids'], ids[:1])
        self.assertEqual(result['candidate_family_sha256'], mission.object_hash(
            dict(rules=result['frozen_rules'], union_ids=ids[:1])))
        self.assertFalse(result['research_objective_achieved'])
        self.assertEqual(result['selection_evidence_class'], 'DEVELOPMENT')

    def test_zero_candidate_preserves_full_accounting_without_certification(self):
        with tempfile.TemporaryDirectory() as tmp:
            cp, inputs, registered, ids = self.build(Path(tmp), candidate=False)
            result = mission.verify_fp01_terminal_output(Path(tmp), cp, inputs, registered)
        self.assertEqual(result['candidate_count'], 0)
        self.assertEqual(result['frozen_rules'], [])
        self.assertTrue(result['candidate_freeze_state'].startswith('NO_ELIGIBLE_CANDIDATE'))
        self.assertFalse(result['research_objective_achieved'])

    def test_rehashed_post_test_candidate_selection_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)
            cp, inputs, registered, ids = self.build(output, candidate=False)
            fold = json.loads((output / 'fold_5.json').read_bytes())
            fold['training_selected_ids'] = ids[:1]
            raw = mission.canonical_bytes(fold) + b'\n'
            (output / 'fold_5.json').write_bytes(raw)
            cp['fold_sha256']['5'] = mission.digest(raw)
            cp.pop('checkpoint_sha256')
            cp['checkpoint_sha256'] = mission.object_hash(cp)
            with self.assertRaisesRegex(mission.GateClosed, 'FP01_TRAIN_ONLY_SELECTION_CHANGED'):
                mission.verify_fp01_terminal_output(output, cp, inputs, registered)


class PopulationTests(unittest.TestCase):
    def day(self):
        return dict(session_date='2025-09-02', source_manifest_sha256='a' * 64,
                    population_day_sha256='b' * 64, security_sessions=10,
                    candidate_decisions=3200, missing_raw_sessions=8,
                    unresolved_identity_sessions=1, unknown_security_type_sessions=1,
                    reference_complete=True, source_semantics_certified=False,
                    publication_replay_certified=False)

    def test_missing_inputs_remain_in_natural_denominator(self):
        result = mission.reconcile_population_days([self.day()], ['2025-09-02'], 'a' * 64)
        self.assertEqual(result['security_sessions'], 10)
        self.assertEqual(result['candidate_decisions'], 3200)
        self.assertFalse(result['denominator_reduced'])
        self.assertEqual(result['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_missing_date_prevents_full_population_claim(self):
        with self.assertRaisesRegex(mission.GateClosed, 'FULL_POPULATION_DATES_PENDING'):
            mission.reconcile_population_days([], ['2025-09-02'], 'a' * 64)

    def test_source_revision_invalidates_census_binding(self):
        with self.assertRaisesRegex(mission.GateClosed, 'POPULATION_SOURCE_PIN_CHANGED'):
            mission.reconcile_population_days([self.day()], ['2025-09-02'], 'c' * 64)

    def test_successor_cannot_invent_budget(self):
        with self.assertRaisesRegex(mission.GateClosed, 'RESOURCE_AUTHORITY_SHA256_REQUIRED'):
            mission.successor_contract(dict(state='VERIFIED', fits=6000),
                                       dict(population_manifest_sha256='a' * 64), {})

    def test_timer_tick_has_run_identity_without_claiming_market_progress(self):
        rpc = Mock()
        rpc.call.return_value = dict(stage='WAIT_W10_COMPLETE', committed=False)
        mission.supervise_once(rpc, 'owner', scheduled_at=42)
        payload = rpc.call.call_args.args[2]
        self.assertEqual(payload['trigger'], 'PERSISTENT_WORKER_TIMER')
        self.assertEqual(payload['scheduled_at'], 42)
        self.assertEqual(len(payload['invocation_id']), 32)


class ExecutableTransitionTests(unittest.TestCase):
    def test_ready_fp01_is_dispatched_to_the_actual_reserved_child(self):
        rpc = Mock()
        job = dict(action='RUN_FP01', reserved_cpu_seconds=30,
                   attempt_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')
        rpc.call.return_value = job
        with patch.object(mission, 'execute_reserved_child', return_value={'committed': True}) as child:
            result = mission.supervise_once(rpc, 'owner', scheduled_at=42)
        child.assert_called_once_with(rpc, 'owner', job)
        self.assertTrue(result['committed'])

    def test_incomplete_readiness_does_not_load_an_adapter_or_launch_compute(self):
        rpc = Mock()
        rpc.call.return_value = dict(stage='BLOCKED_BY_IDENTIFIED_DEPENDENCY',
            reason_codes=['POINT_IN_TIME_CLASS_ADMISSION_REQUIRED'])
        with patch.object(mission, 'execute_reserved_child') as child:
            result = mission.supervise_once(rpc, 'owner', scheduled_at=42)
        child.assert_not_called()
        self.assertEqual(result['reason_codes'], ['POINT_IN_TIME_CLASS_ADMISSION_REQUIRED'])

    def test_unverified_fp01_release_fails_before_import_or_outcome_access(self):
        with self.assertRaisesRegex(mission.GateClosed, 'DATABASE_VERIFIED_FP01_RELEASE_REQUIRED'):
            mission.execute_fp01_segment(dict(registration={}), 'owner', 'attempt')

    def test_unknown_ack_does_not_reexecute_a_child(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(mission, 'ROOT', Path(tmp)):
            attempt = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
            (Path(tmp) / ('job_' + attempt + '.json')).write_text('{}')
            with patch.object(mission, 'source_guards') as guards:
                with self.assertRaisesRegex(mission.GateClosed, 'ATTEMPT_REEXECUTION_FORBIDDEN'):
                    mission.execute_reserved_child(Mock(), 'owner', dict(action='VERIFY_W10_TERMINAL',
                        reserved_cpu_seconds=30, attempt_id=attempt))
            guards.assert_not_called()

    def test_verified_blob_parts_resume_without_refetch_or_false_completion(self):
        raw = b'{"synthetic":true,"value":123}'
        encoded = zlib.compress(raw)
        midpoint = len(encoded) // 2
        parts = [encoded[:midpoint], encoded[midpoint:]]
        def reply(part):
            return dict(payload_base64=base64.b64encode(part).decode(), payload_sha256=mission.digest(part))
        meta = dict(chunks=2, blob_sha256=mission.digest(encoded), raw_sha256=mission.digest(raw),
                    raw_bytes=len(raw), encoded_bytes=len(encoded))
        snapshot = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
        with tempfile.TemporaryDirectory() as tmp, patch.object(mission, 'ROOT', Path(tmp)), \
                patch.object(mission, 'source_guards', return_value=Mock(scratch_safe=Mock(return_value=True))):
            first = Mock()
            first.call.side_effect = [reply(parts[0]), mission.GateClosed('ACCOUNTING_COMMITTED_PREPARATION_YIELD')]
            with self.assertRaisesRegex(mission.GateClosed, 'PREPARATION_YIELD'):
                mission.read_terminal_file(first, 'owner', snapshot, meta)
            self.assertFalse((Path(tmp) / 'account_cache' / snapshot / meta['raw_sha256']).exists())
            resumed = Mock()
            resumed.call.return_value = reply(parts[1])
            self.assertEqual(mission.read_terminal_file(resumed, 'owner', snapshot, meta), raw)
            self.assertEqual(resumed.call.call_count, 1)
            self.assertEqual(resumed.call.call_args.args[2]['part_no'], 1)

    def test_committed_work_ignores_accounting_and_error_only_changes(self):
        prior = dict(completed_folds=[0], trial_records=1000,
                     stage_commits={'0_fit': {}, '0_train': {}, '0_test': {}},
                     active_stage={'stage_key': '1_fit', 'cursor': ['2025-09-02', 'A'],
                                   'processed_sessions': 1}, cpu_charged_seconds=4, resume_count=1)
        changed = copy.deepcopy(prior)
        changed.update(cpu_charged_seconds=27, resume_count=7, error='SAME_INPUT_STALL')
        self.assertEqual(mission.committed_work(prior), mission.committed_work(changed))
        changed['active_stage']['processed_sessions'] = 2
        changed['active_stage']['cursor'] = ['2025-09-02', 'B']
        self.assertNotEqual(mission.committed_work(prior), mission.committed_work(changed))

    def test_recovery_replays_exact_committed_progress_without_new_compute(self):
        attempt = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
        recovery = 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
        identity = dict(pid=1234, process_group=1234, start_ticks=99, boot_id='synthetic')
        prior = dict(state='RUNNING', attempt_id=attempt, host_instance=mission.socket.gethostname(),
                     process_finished=True, process_identity=identity,
                     process_termination_proof='SPECIFIC_CHILD_WAIT4', protected_outcomes_accessed=False,
                     child_cpu_seconds=1.2, actual_scientific_cursor=['2025-09-02', 'B'])
        original = dict(owner='original', operation='terminal_progress',
                        args=dict(attempt_id=attempt, receipt=prior))
        guards = Mock()
        guards.assert_proc_namespace = Mock()
        guards.quiesce_recorded_child.return_value = dict(process_finished=True, proof='RECORDED_PID_ABSENT')
        with tempfile.TemporaryDirectory() as tmp, patch.object(mission, 'ROOT', Path(tmp)), \
                patch.object(mission, 'source_guards', return_value=guards), patch.object(mission.time, 'sleep'):
            # Deliberately no process file: actual wait4 receipt supplies the
            # original identity after interrupted identity-file persistence.
            (Path(tmp) / ('terminal_' + attempt + '.json')).write_bytes(mission.canonical_bytes(original))
            rpc = Mock()
            mission.recover_recorded_attempt(rpc, dict(attempt_id=attempt, recovery_id=recovery,
                reserved_cpu_seconds=30, host_instance=mission.socket.gethostname(), original_owner='original'))
        op, owner, args = rpc.call.call_args.args
        self.assertEqual((op, owner), ('terminal_recover', 'original'))
        self.assertEqual(args['receipt'], prior)
        self.assertEqual(args['original_terminal_operation'], 'terminal_progress')
        self.assertEqual(args['recovery_receipt']['process_termination_proof']['proof'], 'RECORDED_PID_ABSENT')
        guards.ReapedChild.assert_not_called()

    def test_recovery_requires_separate_finite_resource_reservation(self):
        with self.assertRaisesRegex(mission.GateClosed, 'SEPARATELY_RESERVED_RECOVERY_REQUIRED'):
            mission.recover_recorded_attempt(Mock(), dict(
                attempt_id='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'))

    def test_shutdown_does_not_restart_or_signal_inflight_children(self):
        stop = Mock()
        with patch.object(mission, '_stop', stop), patch.object(mission, '_thread', None):
            mission.request_stop()
            self.assertTrue(mission.join_shutdown(.01))
        stop.set.assert_called_once()


if __name__ == '__main__':
    unittest.main()
