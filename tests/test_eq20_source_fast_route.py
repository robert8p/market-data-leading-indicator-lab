"""Guard tests for the fixed fast RPC route; no external requests or mutation."""
import unittest
from unittest import mock

from app import eq20_source_supervisor as s


class AdmissionReached(Exception):
    pass


class FastRouteTests(unittest.TestCase):
    def test_blocked_status_is_visible_deduplicated_and_payload_redacted(self):
        poll = {'stage': 'BLOCKED', 'enabled': False, 'checkpoint': 2836,
                'last_error': 'ISSUER_HISTORY_EXCEEDS_32MIB_GUARD'}
        with mock.patch.object(s, '_last_status_signature', None), \
                mock.patch.object(s, '_last_status_logged_at', None), \
                mock.patch.object(s, 'LOG') as log:
            self.assertTrue(s.log_source_status(poll, now=1000.0))
            log.warning.assert_called_once_with(
                'EQ20 source status stage=%s enabled=%s durable_checkpoint=%s last_error_code=%s',
                'BLOCKED', False, 2836, 'ISSUER_HISTORY_EXCEEDS_32MIB_GUARD')
            self.assertFalse(s.log_source_status(poll, now=1029.0))
            self.assertFalse(s.log_source_status(dict(poll, checkpoint=2837), now=1299.0))
            self.assertTrue(s.log_source_status(dict(poll, checkpoint=2837), now=1300.0))
            self.assertEqual(log.warning.call_args.args[3], 2837)
            self.assertTrue(s.log_source_status(dict(poll, last_error='secret payload\nSELECT data'), now=1301.0))
            self.assertEqual(log.warning.call_args.args[-1], 'REDACTED_INVALID_ERROR_CODE')
            self.assertNotIn('secret', str(log.mock_calls))
            self.assertTrue(s.log_source_status(dict(poll, stage='SOURCE_CORRECTION',
                                                    enabled=True, last_error=None), now=1302.0))
            self.assertEqual(log.info.call_args.args[1:], ('SOURCE_CORRECTION', True, 2836, 'NONE'))

    def test_only_registered_fast_operations_receive_short_admission(self):
        expected = {'NEXT', 'MEMBER_INPUT', 'CAPTURE_READ',
                    'CACHE_READ_BATCH', 'CACHE_ACK_BATCH', 'READ_MEMBER',
                    'UPLOAD_STATUS', 'SEAL_METADATA', 'CACHE_REPLY_ACK'}
        self.assertEqual(s.FAST_DATA_OPERATIONS, expected)
        for operation in s.DATA_OPERATIONS:
            route = (s.FAST_RPC, 3.0) if operation in expected else (s.DATA_RPC, 9.0)
            self.assertEqual(s.source_data_route(operation), route)
        for operation in ('SQL', 'FINALIZE_ALL', None, {}, True):
            with self.assertRaises(s.GuardError):
                s.source_data_route(operation)

    def test_endpoint_and_operation_cannot_disagree(self):
        for operation in s.DATA_OPERATIONS:
            route, _timeout = s.source_data_route(operation)
            args = {'p_operation': operation, 'p_payload': {}}
            self.assertEqual(s.child_operation_key(route, args), operation)
            wrong = s.DATA_RPC if route == s.FAST_RPC else s.FAST_RPC
            with self.assertRaisesRegex(s.GuardError, 'SOURCE_SERVER_TIMEOUT_CONTRACT_REQUIRED'):
                s.child_operation_key(wrong, args)
        self.assertEqual(s.child_operation_key(s.CONTROL_RPC,
            {'p_action': 'CHECK', 'p_payload': {}}), 'CONTROL_CHECK')
        with self.assertRaises(s.GuardError):
            s.child_operation_key(s.FAST_RPC, {'p_action': 'CHECK', 'p_payload': {}})

    def test_timeout_contract_is_checked_before_admission_or_dispatch(self):
        budget = s.ChildBudget.__new__(s.ChildBudget)
        budget.rpc_calls = 0
        admitted = []
        def admission(timeout):
            admitted.append(timeout)
            raise AdmissionReached()
        budget.before_rpc = admission
        for operation in s.DATA_OPERATIONS:
            name, timeout = s.source_data_route(operation)
            args = {'p_operation': operation, 'p_payload': {}}
            with self.assertRaises(AdmissionReached):
                budget._call_locked(name, args, timeout)
            self.assertEqual(admitted[-1], timeout)
            before = len(admitted)
            with self.assertRaises(s.GuardError):
                budget._call_locked(name, args, 9.0 if timeout == 3.0 else 3.0)
            self.assertEqual(len(admitted), before)

    def test_short_operation_fits_without_weakening_slow_operation_guard(self):
        budget = s.ChildBudget.__new__(s.ChildBudget)
        budget.check = lambda: None
        budget.remaining = lambda: 8.0
        budget.before_rpc(3.0)
        with self.assertRaises(s.BudgetYield):
            budget.before_rpc(9.0)
        budget.remaining = lambda: 3.749
        with self.assertRaises(s.BudgetYield):
            budget.before_rpc(3.0)
        budget.remaining = lambda: 3.75
        budget.before_rpc(3.0)

    def test_hydration_does_not_require_or_inflate_scientific_progress(self):
        job = {'attempt_id': 'test-attempt', 'checkpoint': 2836}
        result = {'version': 'W10_SOURCE_WORKER_RESULT_V2', 'action': s.ACTION,
            'attempt_id': job['attempt_id'], 'status': 'YIELDED', 'checkpoint': 2836,
            'protected_outcomes_accessed': False, 'thresholds_fitted': False,
            'source_review_granted': False, 'committed_operations': 0,
            'committed_members': 0, 'committed_issuers': 0, 'hydrated_pages': 16}
        self.assertIs(s.validate_result(result, job), result)
        for invalid in (-1, True, 65537, 1.5, '16'):
            with self.assertRaises(s.GuardError):
                s.validate_result(dict(result, hydrated_pages=invalid), job)
        with self.assertRaises(s.GuardError):
            s.validate_result(dict(result, committed_operations=1), job)

    def test_hydration_operation_metrics_are_reconciled_normally(self):
        metrics = {'CACHE_READ_BATCH': {'calls': 2, 'cpu_seconds': .01,
                                      'rpc_elapsed_seconds': .3},
                   'CACHE_ACK_BATCH': {'calls': 1, 'cpu_seconds': .02,
                                     'rpc_elapsed_seconds': .2},
                   'CACHE_REPLY_ACK': {'calls': 1, 'cpu_seconds': .01,
                                       'rpc_elapsed_seconds': .1},
                   'SEAL_METADATA': {'calls': 1, 'cpu_seconds': .01,
                                     'rpc_elapsed_seconds': .1},
                   'UPLOAD_STATUS': {'calls': 1, 'cpu_seconds': .01,
                                     'rpc_elapsed_seconds': .1}}
        self.assertEqual(s.validate_operation_metrics(metrics, 6, .8), metrics)
        with self.assertRaises(s.GuardError):
            s.validate_operation_metrics(metrics, 6, .7)


if __name__ == '__main__':
    unittest.main(verbosity=2)
