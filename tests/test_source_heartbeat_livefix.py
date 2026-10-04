"""Deterministic orchestration tests; no network, subprocesses, or real waits.

The actual supervisor loop, job/pin validation, budget admission, child receipt
validation and terminal accounting run unchanged. Only the operating-system and
remote boundaries are replaced. A fake child completes after multiple heartbeat
intervals, so the tests exercise the renewal scheduling inside supervise_once.
"""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest


def supervisor_path():
    override = os.environ.get('SOURCE_SUPERVISOR_PATH')
    if override:
        return Path(override).resolve()
    here = Path(__file__).resolve()
    for parent in (here.parent, here.parent.parent):
        for candidate in (parent / 'eq20_source_supervisor.py',
                          parent / 'app' / 'eq20_source_supervisor.py'):
            if candidate.is_file():
                return candidate
    raise RuntimeError('Set SOURCE_SUPERVISOR_PATH to the public supervisor')


def load_supervisor(path=None):
    spec = importlib.util.spec_from_file_location('heartbeat_supervisor_test',
                                                 path or supervisor_path())
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


OBSERVED_PARENT_USED = 2.306495567 + 0.49814614397473633


def run_scenario(path=None, *, mode='observed', finish_at=25.0, stop_at=None,
                 guard=None, deadline_seconds=150):
    module = load_supervisor(path)
    clock = SimpleNamespace(now=0.0, raced=False)
    calls, dispatched, children, resource_checks = [], [], [], []
    outcome = {}

    def sleep(seconds):
        clock.now = round(clock.now + seconds, 8)
        if clock.now > 155:
            raise AssertionError('The bounded supervisor failed to stop')

    module.time = SimpleNamespace(monotonic=lambda: clock.now,
                                  process_time=lambda: 0.0, sleep=sleep)
    real_budget = module.ParentBudget

    class FakeBudget(real_budget):
        def __init__(self):
            super().__init__()
            self.rpc_elapsed = 0.49814614397473633
            self.helper_cpu = 1.18382

        def cpu(self):
            if guard == 'parent' and clock.now >= 12:
                used = 5.8
            elif mode == 'observed' and clock.now >= 20:
                used = OBSERVED_PARENT_USED
            elif mode == 'race' and clock.raced:
                used = OBSERVED_PARENT_USED
            else:
                used = 1.8
            return used - self.rpc_elapsed

    module.ParentBudget = FakeBudget
    module.assert_proc_namespace = lambda: None
    module.recover_local = lambda *_args: None
    module.log_source_status = lambda *_args: None
    module.cleanup_control = lambda *_args: None
    module.memory_safe = lambda: True
    module.scratch_safe = lambda *_args: True
    module.wait_for_database_quiescence = lambda *_args: None
    module.process_identity = lambda pid: {'pid': pid, 'start_ticks': 1,
        'state': 'S', 'process_group': pid, 'boot_id': 'synthetic-test'}
    module.descendants = lambda _pid: ['synthetic-child'] if (
        guard == 'descendant' and clock.now >= 12) else []
    module.process_cpu = lambda _pid: 17.5 if (
        guard == 'child' and clock.now >= 12) else 7.307015

    def check_resources(child, reap_deadline=None):
        resource_checks.append(clock.now)
        if guard == 'rss' and clock.now >= 12:
            raise module.GuardError('SOURCE_RESIDENT_OR_SCRATCH_GUARD')

    module.check_child_resources = check_resources

    def write(path, raw, immutable=False):
        # Boundary substitute avoids disk fsync costs; real path/byte checks run.
        path = module.checked_path(path)
        assert isinstance(raw, bytes) and len(raw) <= module.MAX_FILE_BYTES
        path.parent.mkdir(parents=True, exist_ok=True)
        if immutable:
            assert not path.exists()
        path.write_bytes(raw)

    module.atomic_write = write

    poll = {'enabled': True, 'stage': 'SOURCE_CORRECTION',
            'protocol_version': module.PROTOCOL, 'entrypoint': module.ENTRYPOINT,
            'bundle_sha256': module.PRIVATE_BUNDLE_SHA256,
            'files': [{'name': name, 'bytes': pin[0], 'sha256': pin[1]}
                      for name, pin in module.PRIVATE_FILES.items()],
            'server_now': '2026-10-04T04:38:25+00:00'}

    def rpc(action, payload, budget, directory, **_kwargs):
        calls.append((action, clock.now))
        if action == 'HEARTBEAT' and mode == 'race':
            clock.raced = True
        # Uses the real admission check, before any modeled network dispatch.
        budget.admit()
        dispatched.append((action, clock.now))
        if action == 'POLL':
            return poll
        if action == 'START':
            epoch = module.parse_timestamp(poll['server_now'])
            deadline = module.datetime.fromtimestamp(epoch + deadline_seconds,
                                                       module.timezone.utc).isoformat()
            job = {'action': module.ACTION, 'protocol_version': module.PROTOCOL,
                   'owner': payload['owner'], 'host_instance': payload['host_instance'],
                   'attempt_key': payload['attempt_key'], 'fence': 155,
                   'attempt_id': 'synthetic_heartbeat_attempt',
                   'reservation_id': 'synthetic_heartbeat_attempt',
                   'deadline_at': deadline, 'server_now': poll['server_now'],
                   'reserved_cpu_seconds': module.RESERVED_SECONDS,
                   'child_budget_seconds': module.CHILD_SECONDS,
                   'bundle_sha256': module.PRIVATE_BUNDLE_SHA256,
                   'scope_sha256': module.SCOPE_SHA256, 'config_sha256': 'a' * 64,
                   'checkpoint': 3024}
            outcome['job'] = job
            return {'acquired': True, 'job': job}
        if action == 'HEARTBEAT':
            if mode == 'other_yield':
                raise module.BudgetYield('SOURCE_CONTROL_RESERVATION_DEADLINE')
            if mode == 'inflight_guard':
                raise module.GuardError('SOURCE_CONTROL_COMBINED_GUARD')
            if mode == 'malformed':
                return None
            return {'continue': mode != 'remote_stop'}
        raise AssertionError('Unexpected control action: ' + action)

    module.control_rpc = rpc

    class FakeChild:
        def __init__(self, command):
            self.directory = Path(command[-1]).parent
            self.job = json.loads(Path(command[-1]).read_text())['job']
            self.pid = 99999999  # Never signaled or inspected by an OS call.
            self.finished = False
            self.usage = None
            self.exit_code = None
            self.termination_proof = 'SPECIFIC_CHILD_WAIT4'
            self.stopped_at = None
            self.journal = {'rpc_calls': 1, 'rpc_elapsed_seconds': 0.1,
                            'pending': None, 'child_operation_metrics': {
                                'NEXT': {'calls': 1, 'cpu_seconds': 0.01,
                                         'rpc_elapsed_seconds': 0.1}}}
            if guard == 'rpc':
                self.journal['pending'] = {'started_monotonic': 0.0,
                                          'timeout_seconds': 12.0}
            write(self.directory / 'budget.json', module.canonical(self.journal))
            children.append(self)

        @property
        def cpu(self):
            return None if self.usage is None else self.usage.ru_utime + self.usage.ru_stime

        def reap(self):
            if not self.finished and clock.now >= finish_at:
                self.finished = True
                self.exit_code = 0
                self.usage = SimpleNamespace(ru_utime=7.307015, ru_stime=0.0,
                                            ru_maxrss=169787392 // 1024)
                result = {'version': 'W10_SOURCE_WORKER_RESULT_V2',
                          'action': module.ACTION, 'attempt_id': self.job['attempt_id'],
                          'status': 'YIELDED', 'checkpoint': 3025,
                          'committed_operations': 1, 'committed_members': 0,
                          'committed_issuers': 1, 'hydrated_pages': 0,
                          'protected_outcomes_accessed': False,
                          'thresholds_fitted': False, 'source_review_granted': False}
                receipt = {'attempt_id': self.job['attempt_id'], 'success': True,
                           'public_adapter_receipt': True,
                           'descendant_creation_blocked': True,
                           'bundle_sha256': module.PRIVATE_BUNDLE_SHA256,
                           'child_rpc_pending': None, 'child_rpc_calls': 1,
                           'child_rpc_elapsed_seconds': 0.1,
                           'child_operation_metrics': self.journal['child_operation_metrics'],
                           'result': result, 'transport_failure': False}
                write(self.directory / 'child_receipt.json', module.canonical(receipt))
            return self.finished

        def stop(self):
            if not self.reap():
                self.stopped_at = clock.now
                self.finished = True
                self.exit_code = -9
                self.usage = SimpleNamespace(ru_utime=7.307015, ru_stime=0.0,
                                            ru_maxrss=169787392 // 1024)

    module.ReapedChild = FakeChild

    def finish(job, directory, wrapper, receipt, budget):
        assert budget.phase == 'TERMINAL'
        outcome['receipt'] = receipt
        return 30

    module.finish_attempt = finish
    stop = SimpleNamespace(is_set=lambda: stop_at is not None and clock.now >= stop_at)
    with tempfile.TemporaryDirectory(prefix='source-heartbeat-test-') as temporary:
        module.ROOT = Path(temporary)
        module.SOURCE_ROOT = module.ROOT / 'source_execution'
        outcome['return_value'] = module.supervise_once('synthetic_test_owner', stop)
    outcome.update(calls=calls, dispatched=dispatched, stopped_at=children[0].stopped_at,
                   elapsed=clock.now, resource_checks=resource_checks)
    return outcome


class HeartbeatOrchestrationTests(unittest.TestCase):
    def test_observed_usage_skips_second_renewal_and_retains_result(self):
        result = run_scenario()
        self.assertTrue(result['receipt']['success'])
        self.assertEqual(result['receipt']['result']['checkpoint'], 3025)
        self.assertEqual(result['elapsed'], 25)
        self.assertIsNone(result['stopped_at'])
        self.assertEqual([t for action, t in result['dispatched'] if action == 'HEARTBEAT'], [10])
        self.assertAlmostEqual(result['receipt']['parent_cpu_seconds'] +
                               result['receipt']['parent_rpc_elapsed_seconds'], OBSERVED_PARENT_USED)

    def test_repeated_unaffordable_renewals_preserve_original_deadline(self):
        result = run_scenario(finish_at=60)
        self.assertTrue(result['receipt']['success'])
        self.assertEqual(result['elapsed'], 60)
        self.assertEqual([t for action, t in result['dispatched'] if action == 'HEARTBEAT'], [10])
        self.assertGreater(len(result['resource_checks']), 100)

    def test_affordable_renewals_still_dispatch(self):
        result = run_scenario(mode='affordable')
        self.assertTrue(result['receipt']['success'])
        self.assertEqual([t for action, t in result['dispatched'] if action == 'HEARTBEAT'], [10, 20])

    def test_affordable_remote_stop_terminates(self):
        result = run_scenario(mode='remote_stop')
        self.assertFalse(result['receipt']['success'])
        self.assertEqual(result['receipt']['error'], 'SOURCE_CONTROL_GATE_CLOSED')
        self.assertEqual(result['stopped_at'], 10)

    def test_exact_predispatch_admission_race_is_deferred(self):
        result = run_scenario(mode='race')
        self.assertTrue(result['receipt']['success'])
        self.assertEqual([t for action, t in result['calls'] if action == 'HEARTBEAT'], [10])
        self.assertEqual([t for action, t in result['dispatched'] if action == 'HEARTBEAT'], [])

    def test_unrelated_budget_yield_propagates(self):
        result = run_scenario(mode='other_yield')
        self.assertFalse(result['receipt']['success'])
        self.assertEqual(result['receipt']['error'], 'SOURCE_CONTROL_RESERVATION_DEADLINE')
        self.assertEqual(result['stopped_at'], 10)

    def test_inflight_guard_propagates(self):
        result = run_scenario(mode='inflight_guard')
        self.assertFalse(result['receipt']['success'])
        self.assertEqual(result['receipt']['error'], 'SOURCE_CONTROL_COMBINED_GUARD')
        self.assertEqual(result['stopped_at'], 10)

    def test_malformed_heartbeat_reply_is_not_accepted(self):
        result = run_scenario(mode='malformed')
        self.assertFalse(result['receipt']['success'])
        self.assertEqual(result['stopped_at'], 10)

    def test_local_stop_still_terminates(self):
        result = run_scenario(stop_at=12)
        self.assertFalse(result['receipt']['success'])
        self.assertEqual(result['stopped_at'], 12)

    def test_parent_child_rss_descendant_and_rpc_guards_still_terminate(self):
        for guard in ('parent', 'child', 'rss', 'descendant', 'rpc'):
            with self.subTest(guard=guard):
                result = run_scenario(guard=guard)
                self.assertFalse(result['receipt']['success'])
                self.assertLessEqual(result['stopped_at'], 12)
                self.assertIsNone(result['receipt']['result'])

    def test_reservation_deadline_still_terminates_without_renewal(self):
        result = run_scenario(finish_at=150)
        self.assertFalse(result['receipt']['success'])
        # validate_job deducts START allowance, watch preserves terminal time.
        self.assertEqual(result['stopped_at'], 150 - 3.5 - 6)

    def test_affordability_margin_and_terminal_phase(self):
        module = load_supervisor()
        budget = module.ParentBudget()
        budget.used = lambda: 2.375
        self.assertTrue(module.heartbeat_affordable(budget))
        budget.used = lambda: 2.3750001
        self.assertFalse(module.heartbeat_affordable(budget))
        budget.used = lambda: 0.0
        budget.phase = 'TERMINAL'
        self.assertFalse(module.heartbeat_affordable(budget))


if __name__ == '__main__':
    unittest.main()
