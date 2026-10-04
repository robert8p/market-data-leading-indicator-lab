"""Actual-disk regression tests; no helper, network, worker or database runs."""
import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch


SOURCE = (Path(__file__).resolve().parents[1] / 'app' / 'eq20_source_supervisor.py')
spec = importlib.util.spec_from_file_location('terminal_ack_candidate', SOURCE)
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class TerminalAcknowledgementRecovery(unittest.TestCase):
    def setUp(self):
        # These checks isolate acknowledgement salvage. The separately reserved
        # reconciliation path has its own end-to-end bounded recovery tests.
        self.maintenance_patch = patch.object(supervisor, 'reconcile_exhausted_terminal',
            side_effect=supervisor.GuardError('SOURCE_TERMINAL_RETRY_MARGIN_EXHAUSTED'))
        self.maintenance_patch.start()
        self.addCleanup(self.maintenance_patch.stop)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name) / 'root'
        self.root.mkdir()
        self.source_root = self.root / 'source_execution'
        self.source_root.mkdir()
        self.root_patch = patch.multiple(supervisor, ROOT=self.root, SOURCE_ROOT=self.source_root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.addCleanup(self.temp.cleanup)
        self.rpc_patch = patch.object(supervisor, 'control_rpc', side_effect=AssertionError('RPC must not run'))
        self.rpc = self.rpc_patch.start()
        self.addCleanup(self.rpc_patch.stop)
        self.job = {'attempt_id': '22222222-2222-4222-8222-222222222222',
                    'owner': 'render_eq20_source_fixture', 'host_instance': 'fixture-host', 'fence': 12}
        self.directory = supervisor.make_directory(self.source_root / 'attempts' / self.job['attempt_id'])
        self.boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.now = time.monotonic()
        self.anchor = {'server_epoch': 1700000000.0, 'received_monotonic': self.now - 20,
                       'boot_id': self.boot}
        self.identity = {'pid': 2147483647, 'start_ticks': 1, 'process_group': 2147483647,
                         'boot_id': self.boot, 'state': 'R'}
        self.request = {'action': 'FINISH', 'payload': dict(self.job, wrapper_sha256='a' * 64,
            receipt={'success': True, 'attempt_id': self.job['attempt_id'],
                     'process_finished': True, 'protected_outcomes_accessed': False})}
        self.ack = {'settled': True, 'released': True, 'receipt_committed': True,
                    'attempt_id': self.job['attempt_id'], 'reservation_id': self.job['attempt_id'],
                    'charged_cpu_seconds': 17.234, 'stage': 'SOURCE_CORRECTION'}
        self.request_path = self.directory / ('control_' + 'c' * 32 + '.request.json')
        self.paths = supervisor.control_paths(self.request_path)
        self.write_fixture()

    def write(self, path, value):
        supervisor.atomic_write(path, supervisor.canonical(value))

    def write_fixture(self, spent=3.8, in_flight=False):
        self.write(self.directory / 'job.json', {'job': self.job, 'server_anchor': self.anchor})
        self.write(self.directory / 'terminal_request.json', self.request)
        self.write(self.directory / 'terminal_state.json', {'version': 1, 'spent_seconds': spent,
            'request_sha256': supervisor.sha256(supervisor.canonical(self.request)),
            'request_in_flight': in_flight})
        self.envelope = {'version': 'SOURCE_CONTROL_ENVELOPE_V1',
            'rpc': {'p_action': self.request['action'], 'p_payload': self.request['payload']},
            'server_anchor': self.anchor, 'boot_id': self.boot, 'created_monotonic': self.now - 10,
            'bootstrap_deadline_monotonic': self.now - 7.5}
        self.write(self.request_path, self.envelope)
        self.request_sha = supervisor.sha256(supervisor.canonical(self.envelope))
        self.write(self.paths['process'], {'request_sha256': self.request_sha, 'process_identity': self.identity})
        self.marker = {'version': 1, 'request_sha256': self.request_sha,
            'process_identity': self.identity, 'dispatch_monotonic': self.now - 9.75,
            'http_timeout_seconds': supervisor.CONTROL_HTTP_SECONDS,
            'request_start_deadline_at': supervisor.request_start_deadline(
                self.anchor, 0.5, at_monotonic=self.now - 9.75)}
        self.write(self.paths['dispatch'], self.marker)
        self.body = {'success': True, 'response': self.ack,
            'dispatch_sha256': supervisor.sha256(supervisor.canonical(self.marker)),
            'rpc_finished_monotonic': self.now - 9.25}
        self.write(self.paths['response'], self.body)
        self.completion = {'version': 1, 'request_sha256': self.request_sha,
            'process_identity': self.identity, 'process_finished': True,
            'termination_proof': 'SPECIFIC_CHILD_WAIT4', 'dispatch_verified': True,
            'dispatch_sha256': supervisor.sha256(supervisor.canonical(self.marker)),
            'tail_wait_until_monotonic': self.now - 7.25, 'completed_monotonic': self.now - 7,
            'rpc_elapsed_seconds': 2.5, 'quiescence_scope': 'PROTECTED_CONTROL'}
        self.write(self.paths['completion'], self.completion)

    def budget(self):
        budget = supervisor.ParentBudget()
        budget.phase = 'TERMINAL'
        budget.terminal_begin = 0.0
        budget.server_anchor = self.anchor
        return budget

    def finish(self):
        # The wrapper may have been updated; the old immutable request controls
        # acknowledgement identity and is never rewritten with the new hash.
        return supervisor.finish_attempt(self.job, self.directory, 'b' * 64, None, self.budget())

    def assert_closed(self, code):
        with self.assertRaisesRegex(supervisor.GuardError, code):
            self.finish()
        self.assertFalse((self.directory / 'commit_ack.json').exists())
        self.rpc.assert_not_called()

    def test_exhausted_rpc_margin_salvages_exact_response_and_preserves_spent(self):
        before = (self.directory / 'terminal_state.json').read_bytes()
        with patch.object(supervisor, 'own_cleanup', return_value=False):
            self.assertEqual(self.finish(), 1)
        self.assertEqual(json.loads((self.directory / 'commit_ack.json').read_bytes()), self.ack)
        self.assertEqual((self.directory / 'terminal_state.json').read_bytes(), before)
        self.rpc.assert_not_called()

    def test_real_cleanup_after_salvage_and_next_recovery_unblocked(self):
        self.assertEqual(supervisor.recover_local(self.job['owner'], self.job['host_instance'],
                         'b' * 64, self.budget()), 1)
        self.assertFalse(self.directory.exists())
        self.assertIsNone(supervisor.recover_local(self.job['owner'], self.job['host_instance'],
                          'b' * 64, supervisor.ParentBudget()))
        self.rpc.assert_not_called()

    def test_interrupted_conservative_admission_is_not_reset(self):
        self.write_fixture(spent=5.95, in_flight=True)
        before = (self.directory / 'terminal_state.json').read_bytes()
        with patch.object(supervisor, 'own_cleanup', return_value=False):
            self.assertEqual(self.finish(), 1)
        self.assertEqual((self.directory / 'terminal_state.json').read_bytes(), before)

    def test_fail_and_recover_use_exact_failed_receipt(self):
        for action in ('FAIL', 'RECOVER'):
            with self.subTest(action=action):
                self.request['action'] = action
                self.request['payload']['receipt']['success'] = False
                self.write_fixture()
                with patch.object(supervisor, 'own_cleanup', return_value=False):
                    self.assertEqual(self.finish(), 30)
                (self.directory / 'commit_ack.json').unlink()
        self.rpc.assert_not_called()

    def test_missing_response_does_not_replay_exhausted_request(self):
        self.paths['response'].unlink()
        before = (self.directory / 'terminal_state.json').read_bytes()
        self.assert_closed('SOURCE_TERMINAL_RETRY_MARGIN_EXHAUSTED')
        self.assertEqual((self.directory / 'terminal_state.json').read_bytes(), before)

    def test_failed_helper_response_does_not_manufacture_ack(self):
        self.write(self.paths['response'], {'success': False, 'error_code': 'SOURCE_HELPER_TIMEOUT'})
        self.assert_closed('SOURCE_TERMINAL_RETRY_MARGIN_EXHAUSTED')

    def test_other_immutable_receipt_is_never_accepted(self):
        self.request['payload']['receipt']['success'] = False
        self.write(self.directory / 'terminal_request.json', self.request)
        self.write(self.directory / 'terminal_state.json', {'version': 1, 'spent_seconds': 3.8,
            'request_sha256': supervisor.sha256(supervisor.canonical(self.request))})
        self.assert_closed('SOURCE_TERMINAL_RETRY_MARGIN_EXHAUSTED')

    def test_response_must_match_dispatch_hash(self):
        self.body['dispatch_sha256'] = '0' * 64
        self.write(self.paths['response'], self.body)
        self.assert_closed('SOURCE_CONTROL_DISPATCH_RESPONSE_REJECTED')

    def test_response_time_must_be_within_original_http_bound(self):
        self.body['rpc_finished_monotonic'] = self.marker['dispatch_monotonic'] + 2.6
        self.write(self.paths['response'], self.body)
        self.assert_closed('SOURCE_CONTROL_DISPATCH_RESPONSE_REJECTED')

    def test_read_only_completion_cannot_settle_protected_control(self):
        self.completion['quiescence_scope'] = 'READ_ONLY_OPERATIONAL'
        self.write(self.paths['completion'], self.completion)
        self.assert_closed('SOURCE_CONTROL_COMPLETION_REJECTED')

    def test_completion_must_pin_the_exact_dispatch(self):
        self.completion['dispatch_sha256'] = '0' * 64
        self.write(self.paths['completion'], self.completion)
        self.assert_closed('SOURCE_CONTROL_COMPLETION_REJECTED')

    def test_unverified_dispatch_cannot_salvage(self):
        self.completion['dispatch_verified'] = False
        self.write(self.paths['completion'], self.completion)
        self.assert_closed('SOURCE_CONTROL_COMPLETION_REJECTED')

    def test_completion_requires_explicit_quiescence_proof(self):
        self.completion['termination_proof'] = None
        self.write(self.paths['completion'], self.completion)
        self.assert_closed('SOURCE_CONTROL_COMPLETION_REJECTED')

    def test_exact_original_helper_still_alive_is_rejected_without_signal(self):
        with patch.object(supervisor, 'process_identity', return_value=self.identity), \
                patch.object(supervisor.os, 'killpg', side_effect=AssertionError('Must never signal')):
            self.assert_closed('SOURCE_CONTROL_ORPHAN_TERMINATION_UNVERIFIED')

    def test_recycled_pid_does_not_block_valid_ack(self):
        replacement = dict(self.identity, start_ticks=2)
        with patch.object(supervisor, 'process_identity', return_value=replacement), \
                patch.object(supervisor, 'own_cleanup', return_value=False):
            self.assertEqual(self.finish(), 1)

    def test_original_helper_cannot_be_mistaken_for_recycled_pid_after_group_change(self):
        original = dict(self.identity, process_group=self.identity['pid'] - 1)
        with patch.object(supervisor, 'process_identity', return_value=original), \
                patch.object(supervisor.os, 'killpg', side_effect=AssertionError('Must never signal')):
            self.assert_closed('SOURCE_CONTROL_ORPHAN_TERMINATION_UNVERIFIED')

    def test_other_reservation_is_rejected(self):
        self.body['response']['reservation_id'] = '33333333-3333-4333-8333-333333333333'
        self.write(self.paths['response'], self.body)
        self.assert_closed('SOURCE_TERMINAL_COMMIT_UNCONFIRMED')

    def test_unconfirmed_settlement_is_rejected(self):
        self.body['response']['receipt_committed'] = False
        self.write(self.paths['response'], self.body)
        self.assert_closed('SOURCE_TERMINAL_COMMIT_UNCONFIRMED')

    def test_missing_completion_is_reconciled_then_salvaged_without_rpc(self):
        self.paths['completion'].unlink()
        with patch.object(supervisor, 'own_cleanup', return_value=False):
            self.assertEqual(self.finish(), 1)
        completed = json.loads(self.paths['completion'].read_bytes())
        self.assertEqual(completed['termination_proof']['proof'], 'RECORDED_PID_ABSENT')
        self.rpc.assert_not_called()

    def test_original_spend_overrun_remains_closed(self):
        self.write_fixture(spent=6.01)
        self.assert_closed('SOURCE_TERMINAL_ACCOUNTING_REQUIRES_REVIEW')


if __name__ == '__main__':
    unittest.main(verbosity=2)
