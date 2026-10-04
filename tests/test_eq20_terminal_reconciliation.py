"""Disk-level terminal-reconciliation fault tests; no research or network runs."""
import importlib.util
import json
import socket
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


supervisor = load('reconciliation_test_supervisor', ROOT / 'app/eq20_source_supervisor.py')
module = load('reconciliation_test_module', ROOT / 'app/eq20_terminal_reconciliation.py')


class TerminalReconciliation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'root'
        self.root.mkdir()
        self.source_root = self.root / 'source_execution'
        self.source_root.mkdir()
        patches = patch.multiple(supervisor, ROOT=self.root, SOURCE_ROOT=self.source_root)
        patches.start()
        self.addCleanup(patches.stop)
        self.host = socket.gethostname()
        self.job = {'attempt_id': '22222222-2222-4222-8222-222222222222',
                    'owner': 'render_eq20_source_fixture', 'host_instance': self.host, 'fence': 12,
                    'deadline_at': self.timestamp(time.time() - 120)}
        self.directory = supervisor.make_directory(self.source_root / 'attempts' / self.job['attempt_id'])
        self.maintenance = self.directory / 'terminal_reconciliation'
        self.anchor = {'server_epoch': time.time(), 'received_monotonic': time.monotonic(),
                       'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
        self.request = {'action': 'FINISH', 'payload': dict(self.job, wrapper_sha256='a' * 64,
            receipt={'success': True, 'process_finished': True, 'protected_outcomes_accessed': False,
                     'attempt_id': self.job['attempt_id']})}
        self.ack = {'settled': True, 'released': True, 'receipt_committed': True,
                    'attempt_id': self.job['attempt_id'], 'reservation_id': self.job['attempt_id'],
                    'charged_cpu_seconds': 30, 'stage': 'SOURCE_CORRECTION'}
        self.reservation_id = '33333333-3333-4333-8333-333333333333'
        self.write(self.source_root / 'supervisor_identity.json',
                   {'owner': self.job['owner'], 'host_instance': self.host})
        self.write(self.directory / 'job.json', {'job': self.job, 'server_anchor': self.anchor})
        self.write_original()
        self.calls = []
        transport = patch.object(supervisor, 'control_rpc', side_effect=self.rpc)
        self.control_rpc = transport.start()
        self.addCleanup(transport.stop)
        cleanup = patch.object(supervisor, 'own_cleanup', return_value=False)
        self.cleanup = cleanup.start()
        self.addCleanup(cleanup.stop)

    @staticmethod
    def timestamp(epoch):
        return datetime.fromtimestamp(epoch, timezone.utc).isoformat()

    def write(self, path, data):
        supervisor.atomic_write(path, supervisor.canonical(data))

    def write_original(self, spent=3.8):
        self.write(self.directory / 'terminal_request.json', self.request)
        self.write(self.directory / 'terminal_state.json', {'version': 1, 'spent_seconds': spent,
            'request_sha256': supervisor.sha256(supervisor.canonical(self.request)), 'request_in_flight': False})
        self.original_state = (self.directory / 'terminal_state.json').read_bytes()
        self.original_request = (self.directory / 'terminal_request.json').read_bytes()

    def rpc(self, action, payload, budget, directory, terminal=False):
        self.assertEqual((self.directory / 'terminal_state.json').read_bytes(), self.original_state)
        self.assertEqual((self.directory / 'terminal_request.json').read_bytes(), self.original_request)
        self.assertEqual(payload['original_request_text'].encode(), self.original_request)
        self.assertTrue(payload['quiescence']['controls_quiescent'])
        self.assertFalse(payload['quiescence']['new_research_child_launched'])
        self.calls.append((action, dict(payload), budget.phase, terminal))
        if action == 'RECONCILE_RESERVE':
            self.assertEqual(budget.phase, 'CONTROL')
            self.assertFalse(terminal)
            return {'acquired': True, 'maintenance_id': self.reservation_id,
                    'reserved_cpu_seconds': 30, 'original_attempt_id': self.job['attempt_id'],
                    'original_request_sha256': supervisor.sha256(self.original_request)}
        self.assertEqual(action, 'RECONCILE_COMMIT')
        self.assertEqual(budget.phase, 'TERMINAL')
        self.assertTrue(terminal)
        return dict(self.ack, original_ack=dict(self.ack), maintenance_charged_cpu_seconds=30,
                    original_terminal_allowance_reset=False)

    def run_reconciliation(self):
        return module.reconcile_terminal(supervisor, self.job, self.directory, 'b' * 64)

    def local_state(self):
        return json.loads((self.maintenance / 'state.json').read_bytes())

    def expire_local_wait(self):
        state = self.local_state()
        state['attempts'][-1]['started_monotonic'] -= 46
        self.write(self.maintenance / 'state.json', state)

    def exhaust_reservation_dispatches(self):
        self.control_rpc.side_effect = supervisor.GuardError('SOURCE_HELPER_TIMEOUT')
        for ordinal in (1, 2):
            with self.assertRaisesRegex(supervisor.GuardError, 'SOURCE_HELPER_TIMEOUT'):
                self.run_reconciliation()
            self.assertEqual(len(self.local_state()['attempts']), ordinal)
            self.expire_local_wait()

    def drain_reply(self, ack=None):
        return {'acquired': False, 'reservation_created': False, 'drain_only': True,
                'original_terminal_allowance_reset': False, 'original_ack': ack,
                'reason': 'SOURCE_RECONCILIATION_ATTEMPT_LIMIT_REACHED',
                'drained_maintenance_ids': [self.reservation_id]}

    def test_separate_two_phase_budget_never_resets_original_allowance(self):
        self.assertEqual(self.run_reconciliation(), 1)
        self.assertEqual([x[0] for x in self.calls], ['RECONCILE_RESERVE', 'RECONCILE_COMMIT'])
        self.assertEqual((self.directory / 'terminal_state.json').read_bytes(), self.original_state)
        self.assertEqual(json.loads((self.directory / 'commit_ack.json').read_bytes()), self.ack)
        self.assertEqual(self.local_state()['attempts'][0]['state'], 'VERIFIED_COMMIT_ACK')

    def test_original_wrapper_is_not_replaced_in_terminal_request(self):
        self.run_reconciliation()
        payload = self.calls[1][1]
        self.assertEqual(payload['wrapper_sha256'], 'b' * 64)
        self.assertEqual(json.loads(payload['original_request_text'])['payload']['wrapper_sha256'], 'a' * 64)

    def test_unexpired_original_deadline_does_not_consume_maintenance_intent(self):
        self.job['deadline_at'] = self.timestamp(time.time() + 100)
        self.assertEqual(self.run_reconciliation(), 30)
        self.control_rpc.assert_not_called()
        self.assertFalse((self.maintenance / 'state.json').exists())

    def test_actual_old_lease_margin_is_respected_after_deadline(self):
        self.job['deadline_at'] = self.timestamp(time.time() - 15)
        self.assertEqual(self.run_reconciliation(), 30)
        self.control_rpc.assert_not_called()

    def test_not_an_expired_margin_is_rejected(self):
        self.write_original(spent=1)
        with self.assertRaisesRegex(supervisor.GuardError, 'EXHAUSTED_MARGIN_REQUIRED'):
            self.run_reconciliation()
        self.control_rpc.assert_not_called()

    def test_original_overrun_is_not_given_a_new_allowance(self):
        self.write_original(spent=6.01)
        with self.assertRaisesRegex(supervisor.GuardError, 'ORIGINAL_ACCOUNTING_REJECTED'):
            self.run_reconciliation()
        self.control_rpc.assert_not_called()

    def test_actual_recovery_cpu_can_exhaust_margin_without_resetting_persisted_spend(self):
        self.write_original(spent=2.3)
        exhausted = SimpleNamespace(phase='TERMINAL', phase_limit=lambda: 3.7, used=lambda: 0.2)
        self.assertEqual(module.reconcile_terminal(supervisor, self.job, self.directory,
                         'b' * 64, exhausted_budget=exhausted), 1)
        self.assertEqual((self.directory / 'terminal_state.json').read_bytes(), self.original_state)

    def test_other_host_and_owner_are_rejected(self):
        self.job['host_instance'] = 'not-current-host'
        with self.assertRaisesRegex(supervisor.GuardError, 'SAME_HOST_REQUIRED'):
            self.run_reconciliation()
        self.job['host_instance'] = self.host
        self.write(self.source_root / 'supervisor_identity.json', {'owner': 'different', 'host_instance': self.host})
        with self.assertRaisesRegex(supervisor.GuardError, 'OWNER_REJECTED'):
            self.run_reconciliation()
        self.control_rpc.assert_not_called()

    def test_changed_request_cannot_be_reconciled_under_old_hash(self):
        self.request['payload']['receipt']['unexpected_change'] = True
        self.write(self.directory / 'terminal_request.json', self.request)
        with self.assertRaisesRegex(supervisor.GuardError, 'ORIGINAL_HASH_REJECTED'):
            self.run_reconciliation()
        self.control_rpc.assert_not_called()

    def test_missing_original_child_identity_after_launch_stays_closed(self):
        self.write(self.directory / 'launch_intent.json', {'launched': True})
        with self.assertRaisesRegex(supervisor.GuardError, 'CHILD_PROOF_MISSING'):
            self.run_reconciliation()
        self.control_rpc.assert_not_called()

    def test_original_child_and_control_quiescence_precedes_reservation(self):
        self.write(self.directory / 'child_process.json', {'pid': 123, 'start_ticks': 12})
        order = []
        with (patch.object(supervisor, 'reconcile_control_calls', side_effect=lambda *_: order.append('control')),
             patch.object(supervisor, 'quiesce_recorded_child', side_effect=lambda *_a, **_k:
                 (order.append('child') or {'process_finished': True, 'proof': 'RECORDED_PID_ABSENT'})),
             patch.object(supervisor, 'wait_for_database_quiescence', side_effect=lambda *_:
                 (order.append('database') or 0))):
            self.run_reconciliation()
        self.assertEqual(order[:4], ['control', 'control', 'child', 'database'])

    def test_reservation_ambiguity_retains_intent_and_enforces_wait(self):
        self.control_rpc.side_effect = supervisor.GuardError('SOURCE_HELPER_TIMEOUT')
        with self.assertRaisesRegex(supervisor.GuardError, 'SOURCE_HELPER_TIMEOUT'):
            self.run_reconciliation()
        self.assertEqual(len(self.local_state()['attempts']), 1)
        self.assertEqual(self.run_reconciliation(), 30)
        self.assertEqual(self.control_rpc.call_count, 1)
        self.assertEqual((self.directory / 'terminal_state.json').read_bytes(), self.original_state)

    def test_two_failed_dispatch_intents_only_allow_one_existing_key_drain(self):
        self.exhaust_reservation_dispatches()
        keys = [x['attempt_key'] for x in self.local_state()['attempts']]
        self.control_rpc.side_effect = lambda *_a, **_k: self.drain_reply()
        self.assertEqual(self.run_reconciliation(), 30)
        action, payload, budget, directory = self.control_rpc.call_args.args
        self.assertEqual(action, 'RECONCILE_RESERVE')
        self.assertIs(payload['drain_only'], True)
        self.assertEqual(payload['reconciliation_key'], keys[-1])
        self.assertEqual(payload['prior_reconciliation_keys'], keys)
        self.assertEqual(budget.phase, 'CONTROL')
        self.assertEqual(len(self.local_state()['attempts']), 2)
        self.assertEqual(self.local_state()['drain']['state'], 'DRAIN_RESULT_RECORDED')
        self.assertFalse(self.local_state()['drain']['new_reservation_authorized'])
        with self.assertRaisesRegex(supervisor.GuardError, 'ATTEMPT_LIMIT_REACHED'):
            self.run_reconciliation()
        self.assertEqual(self.control_rpc.call_count, 3)
        self.assertFalse((self.directory / 'commit_ack.json').exists())
        self.cleanup.assert_not_called()

    def test_ambiguous_drain_is_never_dispatched_twice(self):
        self.exhaust_reservation_dispatches()
        with self.assertRaisesRegex(supervisor.GuardError, 'SOURCE_HELPER_TIMEOUT'):
            self.run_reconciliation()
        self.assertEqual(self.local_state()['drain']['state'], 'DRAIN_DISPATCH_INTENT')
        with self.assertRaisesRegex(supervisor.GuardError, 'ATTEMPT_LIMIT_REACHED'):
            self.run_reconciliation()
        self.assertEqual(self.control_rpc.call_count, 3)
        self.assertEqual(len(self.local_state()['attempts']), 2)
        self.assertEqual((self.directory / 'terminal_state.json').read_bytes(), self.original_state)
        self.assertFalse((self.directory / 'commit_ack.json').exists())

    def test_drain_cannot_accept_a_new_reservation(self):
        self.exhaust_reservation_dispatches()
        self.control_rpc.side_effect = lambda *_a, **_k: dict(self.drain_reply(), acquired=True)
        with self.assertRaisesRegex(supervisor.GuardError, 'DRAIN_RESPONSE_REJECTED'):
            self.run_reconciliation()
        self.assertEqual(len(self.local_state()['attempts']), 2)
        self.cleanup.assert_not_called()

    def test_drain_exact_existing_original_ack_allows_cleanup_without_replay(self):
        self.exhaust_reservation_dispatches()
        self.control_rpc.side_effect = lambda *_a, **_k: self.drain_reply(self.ack)
        self.assertEqual(self.run_reconciliation(), 1)
        self.assertEqual(self.control_rpc.call_count, 3)
        self.assertEqual(json.loads((self.directory / 'commit_ack.json').read_bytes()), self.ack)
        self.cleanup.assert_called_once()

    def test_closed_resource_response_is_durable_without_commit(self):
        closed = {'acquired': False, 'reservation_created': False,
                  'reason': 'AMENDED_LIFETIME_CEILING_EXHAUSTED',
                  'drained_maintenance_ids': [self.reservation_id]}
        self.control_rpc.side_effect = lambda *_a, **_k: closed
        self.assertEqual(self.run_reconciliation(), 30)
        self.assertEqual(self.control_rpc.call_count, 1)
        state = self.local_state()['attempts'][0]
        self.assertEqual(state['state'], 'RESERVATION_CLOSED')
        self.assertEqual(state['response'], closed)
        self.assertFalse((self.directory / 'commit_ack.json').exists())
        self.cleanup.assert_not_called()

    def test_missing_boolean_success_and_action_mismatch_fail_closed(self):
        for value in (None, False, 0, 'true'):
            self.request['payload']['receipt']['success'] = value
            self.write_original()
            with self.assertRaisesRegex(supervisor.GuardError, 'ORIGINAL_RECEIPT_REJECTED'):
                self.run_reconciliation()
        self.control_rpc.assert_not_called()

    def test_wrong_reservation_or_unconfirmed_commit_never_cleans_original(self):
        self.control_rpc.side_effect = lambda *_a, **_k: {'acquired': True, 'reserved_cpu_seconds': 31}
        with self.assertRaisesRegex(supervisor.GuardError, 'RESERVATION_REJECTED'):
            self.run_reconciliation()
        self.cleanup.assert_not_called()
        self.assertFalse((self.directory / 'commit_ack.json').exists())

    def test_server_ack_of_existing_exact_receipt_requires_no_commit_replay(self):
        self.control_rpc.side_effect = lambda *_a, **_k: {'original_ack': self.ack}
        self.assertEqual(self.run_reconciliation(), 1)
        self.assertEqual(self.control_rpc.call_count, 1)

    def test_cached_verified_metadata_ack_is_salvaged_even_after_two_intents(self):
        self.maintenance.mkdir()
        request_sha = supervisor.sha256(self.original_request)
        now = time.monotonic()
        boot = self.anchor['boot_id']
        self.write(self.maintenance / 'state.json', {'version': module.VERSION,
            'original_request_sha256': request_sha, 'attempts': [
                {'started_monotonic': now-100, 'boot_id': boot},
                {'started_monotonic': now-50, 'boot_id': boot}]})
        self.write(self.maintenance / ('control_'+'c'*32+'.request.json'),
            {'rpc': {'p_action': 'RECONCILE_COMMIT', 'p_payload': {
                'attempt_id': self.job['attempt_id'], 'original_request_sha256': request_sha}}})
        response = dict(self.ack, original_ack=self.ack, maintenance_charged_cpu_seconds=30,
                        original_terminal_allowance_reset=False)
        with (patch.object(supervisor, 'reconcile_control_calls'),
              patch.object(supervisor, 'validate_control_envelope', side_effect=lambda x: x),
              patch.object(supervisor, 'salvage_terminal_ack', return_value=response)):
            self.assertEqual(self.run_reconciliation(), 1)
        self.control_rpc.assert_not_called()
        self.assertEqual(json.loads((self.directory / 'commit_ack.json').read_bytes()), self.ack)

    def test_verified_drain_ack_is_salvaged_after_final_dispatch(self):
        self.exhaust_reservation_dispatches()
        self.control_rpc.side_effect = supervisor.GuardError('SOURCE_HELPER_TIMEOUT')
        with self.assertRaisesRegex(supervisor.GuardError, 'SOURCE_HELPER_TIMEOUT'):
            self.run_reconciliation()
        request_sha = supervisor.sha256(self.original_request)
        request_path = self.maintenance / ('control_'+'d'*32+'.request.json')
        self.write(request_path, {'rpc': {'p_action': 'RECONCILE_RESERVE', 'p_payload': {
            'attempt_id': self.job['attempt_id'], 'original_request_sha256': request_sha,
            'drain_only': True}}})
        response = dict(self.ack, **self.drain_reply(self.ack))
        self.write(supervisor.control_paths(request_path)['response'],
                   {'success': True, 'response': response})
        with (patch.object(supervisor, 'reconcile_control_calls'),
              patch.object(supervisor, 'validate_control_envelope', side_effect=lambda x: x),
              patch.object(supervisor, 'salvage_terminal_ack', return_value=response)):
            self.assertEqual(self.run_reconciliation(), 1)
        self.assertEqual(self.control_rpc.call_count, 3)
        self.assertEqual(json.loads((self.directory / 'commit_ack.json').read_bytes()), self.ack)

    def test_protected_or_unfinished_original_receipt_is_rejected(self):
        self.request['payload']['receipt']['protected_outcomes_accessed'] = True
        self.write_original()
        with self.assertRaisesRegex(supervisor.GuardError, 'ORIGINAL_RECEIPT_REJECTED'):
            self.run_reconciliation()
        self.control_rpc.assert_not_called()


if __name__ == '__main__':
    unittest.main()
