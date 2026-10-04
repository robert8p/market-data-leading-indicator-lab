"""Offline phase admission and truthful child-stop receipt regressions."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / 'app' / 'eq20_source_supervisor.py'


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def subject():
    return load(SOURCE, 'member_phase_subject')


JOB = {'host_instance': 'synthetic-host', 'config_sha256': 'a' * 64}
ARGS = {'issuer_cik': '0000019617', 'blob_sha256': 'b' * 64,
        'pages': [{'chunk_no': n, 'payload_sha256': 'c' * 64} for n in range(4)]}


def ack(operation='CACHE_REPLY_ACK', recorded=4):
    return {'version': 'W10_CACHE_REPLY_ACK_V3' if operation == 'CACHE_REPLY_ACK' else 'W10_CACHE_ACK_V3',
            'issuer_cik': ARGS['issuer_cik'], 'blob_sha256': ARGS['blob_sha256'],
            'host_instance': JOB['host_instance'], 'config_sha256': JOB['config_sha256'],
            'acknowledged_pages': 4, 'recorded_pages': recorded}


class MemberPhaseTests(unittest.TestCase):
    def test_fresh_ack_requests_phase_yield_for_both_cache_kinds(self):
        module = subject()
        for operation in ('CACHE_REPLY_ACK', 'CACHE_ACK_BATCH'):
            with self.subTest(operation=operation):
                self.assertTrue(module.source_reply_needs_phase_yield(
                    operation, ARGS, ack(operation), JOB, 8.0))

    def test_sufficient_headroom_is_preserved(self):
        module = subject()
        self.assertFalse(module.source_reply_needs_phase_yield(
            'CACHE_REPLY_ACK', ARGS, ack(), JOB, 14.0))
        self.assertFalse(module.source_reply_needs_phase_yield(
            'NEXT', {}, {'task': 'MEMBER'}, JOB, 15.0))

    def test_member_transition_cannot_start_after_expensive_previous_task(self):
        module = subject()
        self.assertTrue(module.source_reply_needs_phase_yield(
            'NEXT', {}, {'task': 'MEMBER'}, JOB, 8.0))
        self.assertTrue(module.source_reply_needs_phase_yield(
            'ADMISSIONS', {}, {'next_task': {'task': 'MEMBER'}}, JOB, 8.0))
        self.assertFalse(module.source_reply_needs_phase_yield(
            'NEXT', {}, {'task': 'CAPTURE_BATCH'}, JOB, 8.0))

    def test_replayed_ack_does_not_manufacture_new_progress(self):
        module = subject()
        self.assertFalse(module.source_reply_needs_phase_yield(
            'CACHE_REPLY_ACK', ARGS, ack(recorded=0), JOB, 8.0))

    def test_invalid_ack_does_not_arm_the_phase_flag(self):
        module = subject()
        variants = [('recorded_pages', True), ('recorded_pages', -1),
                    ('recorded_pages', 5), ('acknowledged_pages', 3),
                    ('version', 'incorrect'), ('host_instance', 'foreign'),
                    ('config_sha256', 'd' * 64), ('issuer_cik', '0000000001'),
                    ('blob_sha256', 'e' * 64)]
        for key, value in variants:
            with self.subTest(key=key, value=value):
                response = ack(); response[key] = value
                self.assertFalse(module.source_reply_needs_phase_yield(
                    'CACHE_REPLY_ACK', ARGS, response, JOB, 8.0))

    def test_phase_request_yields_without_changing_remaining_budget(self):
        module = subject()
        budget = object.__new__(module.ChildBudget)
        budget.phase_yield_requested = True
        budget.remaining = lambda: 8.0
        self.assertTrue(budget.should_yield())
        with self.assertRaisesRegex(module.BudgetYield, 'SOURCE_PREPARATION_PHASE_YIELD'):
            budget.check()
        self.assertEqual(budget.remaining(), 8.0)
        budget.phase_yield_requested = False
        self.assertFalse(budget.should_yield())

    def test_validated_handoff_consumes_only_cooperative_request(self):
        module = subject()
        budget = object.__new__(module.ChildBudget)
        budget.phase_yield_requested = True
        budget.remaining = lambda: 8.0
        budget.last_guard = module.time.monotonic()
        budget.finish_work_phase()
        self.assertFalse(budget.phase_yield_requested)
        self.assertEqual(budget.remaining(), 8.0)
        budget.phase_yield_requested = True
        budget.remaining = lambda: 0.0
        with self.assertRaisesRegex(module.BudgetYield, 'CHILD_COMBINED_ALLOWANCE_EXHAUSTED'):
            budget.finish_work_phase()

    def test_existing_watch_causes_survive_missing_child_receipt(self):
        fixture = load(HERE / 'test_source_heartbeat_livefix.py', 'heartbeat_fixture')
        expected = {'parent': 'SOURCE_PARENT_CONTROL_GUARD',
                    'child': 'SOURCE_CHILD_COMBINED_GUARD',
                    'rss': 'SOURCE_RESIDENT_OR_SCRATCH_GUARD',
                    'descendant': 'SOURCE_DESCENDANT_REJECTED',
                    'rpc': 'SOURCE_CHILD_COMBINED_GUARD'}
        for guard, error in expected.items():
            with self.subTest(guard=guard):
                result = fixture.run_scenario(SOURCE, guard=guard)
                self.assertEqual(result['receipt']['error'], error)
                self.assertEqual(result['receipt']['exit_code'], -9)
                self.assertFalse(result['receipt']['measurement_verified'])
                self.assertEqual(result['receipt']['measured_cpu_seconds'], 30)

    def test_absent_receipt_has_its_own_truthful_label(self):
        fixture = load(HERE / 'test_source_heartbeat_livefix.py', 'missing_receipt_fixture')
        real_exists = Path.exists
        def exists(path):
            return False if path.name == 'child_receipt.json' else real_exists(path)
        with patch.object(Path, 'exists', exists):
            result = fixture.run_scenario(SOURCE)
        self.assertEqual(result['receipt']['error'], 'SOURCE_CHILD_RECEIPT_MISSING')
        self.assertIsNone(result['receipt']['result'])
        self.assertFalse(result['receipt']['success'])


if __name__ == '__main__':
    unittest.main()
