"""Admission diagnostics only; no service calls, cache mutation or source input."""
import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('source_admission_diagnostics',
    Path(__file__).resolve().parents[1]/'app/eq20_source_supervisor.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


class AdmissionDiagnostics(unittest.TestCase):
    def setUp(self):
        m._last_admission_signature = m._last_admission_logged_at = None

    def admission(self, memory_reason='SAFE', scratch_reason='SAFE', start=None):
        memory = {'reason': memory_reason, 'cgroup_limit_bytes': 536870912,
            'cgroup_used_bytes': 64*1024*1024, 'cgroup_guard_bytes': 456340275,
            'child_rss_bytes': None, 'child_state': None}
        scratch = {'reason': scratch_reason, 'bytes': 1900000000, 'free_bytes': 1000000000}
        calls = []
        def rpc(action, *args):
            calls.append(action)
            return {'enabled': True, 'stage': 'SOURCE_CORRECTION'} if action == 'POLL' else start
        with patch.object(m, 'assert_proc_namespace'), patch.object(m, 'recover_local', return_value=None), \
                patch.object(m, 'make_directory', return_value=Path('/synthetic-control')), \
                patch.object(m, 'control_rpc', side_effect=rpc), patch.object(m, 'log_source_status'), \
                patch.object(m, 'validate_pins'), patch.object(m, 'cleanup_control', return_value=True), \
                patch.object(m, 'atomic_write'), patch.object(m, 'memory_status', return_value=memory) as mem, \
                patch.object(m, 'scratch_status', return_value=scratch) as disk, \
                self.assertLogs(m.LOG, level='WARNING') as logs:
            result = m.supervise_once('current-owner', SimpleNamespace(is_set=lambda: False))
        self.assertEqual(mem.call_count, 1)
        if memory_reason == 'SAFE':
            disk.assert_called_once_with(m.MAX_FILE_BYTES+m.MAX_CODE_BYTES)
        else:
            disk.assert_not_called()
        return result, calls, logs.output

    def test_failed_scratch_admission_reports_actual_reason_and_never_starts(self):
        result, calls, logs = self.admission(scratch_reason='SCRATCH_TOTAL_LIMIT')
        self.assertEqual((result, calls), (30, ['POLL']))
        self.assertIn('scratch_reason=SCRATCH_TOTAL_LIMIT', logs[0])
        self.assertIn('scratch_bytes=1900000000', logs[0])

    def test_failed_memory_admission_reports_memory_current_without_reset(self):
        result, calls, logs = self.admission(memory_reason='CGROUP_MEMORY_LIMIT')
        self.assertEqual((result, calls), (30, ['POLL']))
        self.assertIn('memory_reason=CGROUP_MEMORY_LIMIT', logs[0])
        self.assertIn('cgroup_used_bytes=67108864', logs[0])
        self.assertIn('scratch_reason=NOT_CHECKED_MEMORY_ADMISSION_CLOSED', logs[0])

    def test_start_rejection_preserves_existing_delay_and_redacts_payload(self):
        for reason, expected in (('OTHER_LIVE_OWNER', 'OTHER_LIVE_OWNER'),
                                 ('private value must not be logged', 'UNRECOGNIZED_REASON_REDACTED'),
                                 ({'private': 'payload'}, 'UNRECOGNIZED_REASON_REDACTED')):
            self.setUp()
            result, calls, logs = self.admission(start={'acquired': False, 'reason': reason, 'retry_seconds': 15})
            self.assertEqual((result, calls), (15, ['POLL', 'START']))
            self.assertIn('reason='+expected, logs[0]); self.assertNotIn('private', logs[0])

    def test_recovery_budget_and_cleanup_waits_are_distinguished(self):
        for used, cleanup, expected in ((3, True, 'CONTROL_PARENT_BUDGET_MARGIN'),
                                       (0, False, 'CONTROL_WITHOUT_START_CLEANUP_PENDING')):
            self.setUp()
            with tempfile.TemporaryDirectory() as temp, patch.object(m, 'ROOT', Path(temp)), \
                    patch.object(m, 'SOURCE_ROOT', Path(temp)/'source'), \
                    patch.object(m, 'reconcile_control_calls'), patch.object(m, 'cleanup_control', return_value=cleanup), \
                    self.assertLogs(m.LOG, level='WARNING') as logs:
                (m.SOURCE_ROOT/'control'/'fixture').mkdir(parents=True)
                delay = m.recover_local('owner', 'host', 'a'*64, SimpleNamespace(used=lambda: used))
            self.assertEqual(delay, 15); self.assertIn(expected, logs.output[0])

    def test_first_reason_is_visible_and_identical_waits_are_rate_limited(self):
        self.assertTrue(m.admission_log_due(('RESOURCE', 'SCRATCH_TOTAL_LIMIT'), now=100))
        self.assertFalse(m.admission_log_due(('RESOURCE', 'SCRATCH_TOTAL_LIMIT'), now=101))
        self.assertTrue(m.admission_log_due(('RESOURCE', 'CGROUP_MEMORY_LIMIT'), now=102))
        self.assertTrue(m.admission_log_due(('RESOURCE', 'CGROUP_MEMORY_LIMIT'), now=402))


if __name__ == '__main__': unittest.main()
