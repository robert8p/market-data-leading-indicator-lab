"""Offline control-clock regression tests; no helpers, network or private data.

The real control_rpc envelope, response, dispatch and accounting checks run.
Only process creation and filesystem publication latency are simulated, so a
slow helper exit cannot be confused with the earlier HTTP receipt boundary.
"""
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


SOURCE = Path(__file__).resolve().parents[1] / 'app' / 'eq20_source_supervisor.py'
SERVER_NOW = '2026-10-04T05:02:05+00:00'


def load_supervisor():
    spec = importlib.util.spec_from_file_location('control_clock_subject', SOURCE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_control(response, *, action='POLL', helper_delay=2.0,
                cleanup_delay=1.0, receipt_override=None, wrong_boot=False):
    module = load_supervisor()
    clock = SimpleNamespace(now=100.0)

    def sleep(seconds):
        clock.now += seconds
        if clock.now > 115:
            raise AssertionError('Synthetic control path exceeded its bound')

    module.time = SimpleNamespace(monotonic=lambda: clock.now,
                                  process_time=lambda: 0.0, sleep=sleep)
    boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    prior_anchor = {'server_epoch': module.parse_timestamp(SERVER_NOW) - 5,
                    'received_monotonic': 95.0, 'boot_id': boot}
    budget = module.ParentBudget()
    budget.server_anchor = prior_anchor
    observed = {'module': module, 'budget': budget, 'prior_anchor': prior_anchor,
                'clock': clock, 'boot': boot}

    def write(path, raw, immutable=False):
        path = module.checked_path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if immutable and path.exists():
            raise AssertionError('Attempted immutable overwrite')
        path.write_bytes(raw)

    module.atomic_write = write

    class FakeHelper:
        def __init__(self, command):
            self.request_path = Path(command[-3])
            self.response_path = Path(command[-2])
            self.original_identity = {'pid': 99999999, 'start_ticks': 1,
                'process_group': 99999999, 'boot_id': boot}
            self.exit_code = 0
            self.usage = SimpleNamespace(ru_maxrss=1024)
            self.cpu = 0.03
            self.termination_proof = 'SPECIFIC_CHILD_WAIT4'
            self.ready = False
            self.stopped = False

        def reap(self):
            if not self.ready:
                raw = self.request_path.read_bytes()
                envelope = json.loads(raw)
                identity = dict(self.original_identity)
                if wrong_boot:
                    identity['boot_id'] = 'wrong-boot'
                sent = clock.now + 0.1
                received = sent + 0.05
                marker = {'version': 1, 'request_sha256': module.sha256(raw),
                    'process_identity': identity, 'dispatch_monotonic': sent,
                    'http_timeout_seconds': module.CONTROL_HTTP_SECONDS,
                    'request_start_deadline_at': module.request_start_deadline(
                        envelope['server_anchor'], 0.5, at_monotonic=sent)}
                paths = module.control_paths(self.request_path)
                write(paths['dispatch'], module.canonical(marker), immutable=True)
                body = {'success': True, 'response': response,
                    'dispatch_sha256': module.sha256(module.canonical(marker)),
                    'rpc_finished_monotonic': received if receipt_override is None
                        else receipt_override}
                # Permit malformed numeric fixtures to exercise the reader's
                # rejection. The production writer itself forbids NaN/Inf.
                write(self.response_path, json.dumps(body).encode(), immutable=True)
                observed.update(received=received, sent=sent)
                clock.now = received + helper_delay
                self.ready = True
            return True

        def stop(self):
            if not self.stopped:
                clock.now += cleanup_delay
                self.stopped = True

    module.ReapedChild = FakeHelper
    with tempfile.TemporaryDirectory(prefix='source-control-clock-') as temporary:
        module.ROOT = Path(temporary)
        module.SOURCE_ROOT = module.ROOT / 'source_execution'
        directory = module.make_directory(module.SOURCE_ROOT / 'control')
        try:
            observed['response'] = module.control_rpc(action, {}, budget, directory)
        except module.GuardError as exc:
            observed['error'] = str(exc)
    return observed


class ControlClockTests(unittest.TestCase):
    def assert_valid_receipt_anchor(self, result):
        self.assertNotIn('error', result)
        anchor = result['budget'].server_anchor
        self.assertEqual(anchor['received_monotonic'], result['received'])
        self.assertEqual(anchor['boot_id'], result['boot'])
        self.assertEqual(anchor['server_epoch'], result['module'].parse_timestamp(SERVER_NOW))
        self.assertGreater(result['clock'].now - result['received'], 0.5)
        return anchor

    def test_poll_uses_receipt_before_slow_helper_and_parent_cleanup(self):
        response = {'server_now': SERVER_NOW, 'enabled': True, 'checkpoint': 3024}
        result = run_control(response)
        self.assert_valid_receipt_anchor(result)
        self.assertEqual(result['response'], response)
        self.assertAlmostEqual(result['budget'].rpc_elapsed, 0.05)
        self.assertEqual(result['budget'].helper_cpu, 0.03)

    def test_start_uses_nested_job_clock_without_changing_response(self):
        response = {'acquired': True, 'job': {'server_now': SERVER_NOW,
                                             'attempt_id': 'synthetic-attempt'}}
        result = run_control(response, action='START')
        self.assert_valid_receipt_anchor(result)
        self.assertEqual(result['response'], response)

    def test_status_recovery_uses_actual_receipt_boundary(self):
        result = run_control({'server_now': SERVER_NOW, 'owned_pending': None}, action='STATUS')
        self.assert_valid_receipt_anchor(result)
        self.assertEqual(result['budget'].prior_terminal_spent, 0.0)

    def test_later_dispatch_retains_exact_half_second_window(self):
        result = run_control({'server_now': SERVER_NOW})
        anchor = self.assert_valid_receipt_anchor(result)
        module, now = result['module'], result['clock'].now
        deadline = module.parse_timestamp(module.request_start_deadline(anchor, 0.5))
        server_lower_bound = module.parse_timestamp(SERVER_NOW) + now - result['received']
        self.assertAlmostEqual(deadline - server_lower_bound, 0.5, places=6)
        # The old receipt-at-cleanup bug would expire this same request by 2.5s.
        late_anchor_deadline = module.parse_timestamp(SERVER_NOW) + 0.5
        self.assertLess(late_anchor_deadline, server_lower_bound)

    def test_nonclock_actions_do_not_refresh_or_reset_accounting(self):
        for action in ('HEARTBEAT', 'CHECK'):
            with self.subTest(action=action):
                result = run_control({'continue': True, 'server_now': SERVER_NOW}, action=action)
                self.assertNotIn('error', result)
                self.assertIs(result['budget'].server_anchor, result['prior_anchor'])
                self.assertAlmostEqual(result['budget'].rpc_elapsed, 0.05)

    def test_unacquired_start_cannot_supply_a_new_anchor(self):
        result = run_control({'acquired': False, 'job': {'server_now': SERVER_NOW}}, action='START')
        self.assertNotIn('error', result)
        self.assertIs(result['budget'].server_anchor, result['prior_anchor'])

    def test_response_time_must_match_original_dispatch_bounds(self):
        for received in (False, float('nan'), float('inf'), 100.05, 103.0):
            with self.subTest(received=received):
                result = run_control({'server_now': SERVER_NOW}, receipt_override=received)
                self.assertEqual(result.get('error'), 'SOURCE_CONTROL_DISPATCH_RESPONSE_REJECTED')
                self.assertIs(result['budget'].server_anchor, result['prior_anchor'])

    def test_response_time_in_future_is_rejected(self):
        result = run_control({'server_now': SERVER_NOW}, helper_delay=0.0,
                             receipt_override=100.2)
        self.assertEqual(result.get('error'), 'SOURCE_CONTROL_DISPATCH_RESPONSE_REJECTED')
        self.assertIs(result['budget'].server_anchor, result['prior_anchor'])

    def test_wrong_boot_dispatch_cannot_refresh_anchor(self):
        result = run_control({'server_now': SERVER_NOW}, wrong_boot=True)
        self.assertEqual(result.get('error'), 'SOURCE_CONTROL_DISPATCH_REJECTED')
        self.assertIs(result['budget'].server_anchor, result['prior_anchor'])

    def test_anchor_requires_explicit_finite_same_boot_receipt(self):
        module = load_supervisor()
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        with patch.object(module.time, 'monotonic', return_value=100):
            for received, boot_id in ((True, boot), (-1, boot), (101, boot),
                    (float('nan'), boot), (float('inf'), boot), (99, 'wrong-boot')):
                with self.subTest(received=received, boot=boot_id):
                    with self.assertRaisesRegex(module.GuardError, 'SOURCE_SERVER_CLOCK_ANCHOR_REJECTED'):
                        module.make_server_anchor(SERVER_NOW,
                            received_monotonic=received, boot_id=boot_id)
            for server in ('invalid', '2026-10-04T05:02:05', None, 123):
                with self.subTest(server=server):
                    with self.assertRaisesRegex(module.GuardError, 'SOURCE_SERVER_CLOCK_ANCHOR_REJECTED'):
                        module.make_server_anchor(server, received_monotonic=99, boot_id=boot)
            with self.assertRaises(TypeError):
                module.make_server_anchor(SERVER_NOW)

    def test_helper_diagnostics_only_expose_allowlisted_action(self):
        module = load_supervisor()
        helper = SimpleNamespace(exit_code=1, usage=object(), cpu=0.03)
        with patch.object(module, 'read_bounded', side_effect=FileNotFoundError):
            with self.assertLogs(module.LOG, level='WARNING') as logs:
                module.log_helper_failure(helper, Path('/unused'),
                    'SOURCE_HELPER_EXIT_FAILURE', action='START')
            self.assertIn('action=START', logs.output[0])
            with self.assertLogs(module.LOG, level='WARNING') as logs:
                module.log_helper_failure(helper, Path('/unused'),
                    'SOURCE_HELPER_EXIT_FAILURE', action='private payload must not be logged')
            self.assertIn('action=UNKNOWN', logs.output[0])
            self.assertNotIn('private payload', logs.output[0])


if __name__ == '__main__':
    unittest.main()
