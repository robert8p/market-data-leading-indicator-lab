"""Focused offline regression checks; no service calls or private bundle reads."""
import importlib.util
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('subject', (Path(__file__).resolve().parents[1] / 'app' / 'eq20_source_supervisor.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def readings(used=64 * 1024 * 1024, limit=512 * 1024 * 1024,
             status='Name:\tprivate-do-not-log\nState:\tR (running)\nVmRSS:\t1024 kB\n'):
    data = {'/sys/fs/cgroup/memory.max': str(limit),
            '/sys/fs/cgroup/memory.current': str(used), '/proc/123/status': status}
    def read(path, *args, **kwargs):
        value = data[str(path)]
        if isinstance(value, BaseException):
            raise value
        return value
    return read


class Child:
    pid = 123
    def __init__(self, values=()):
        self.values = iter(values)
        self.calls = 0
    def reap(self):
        self.calls += 1
        return next(self.values, False)


class Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []
    def monotonic(self):
        return self.now
    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class ResourceGuardTests(unittest.TestCase):
    def test_exact_caps_unchanged(self):
        self.assertEqual(m.MAX_CHILD_RSS_BYTES, 256 * 1024 * 1024)
        self.assertEqual(m.MAX_FILE_BYTES, 256 * 1024 * 1024)
        self.assertEqual(m.MAX_SCRATCH_BYTES, 2 * 1024 * 1024 * 1024)
        self.assertEqual((m.RESERVED_SECONDS, m.CHILD_SECONDS, m.PARENT_SECONDS,
                          m.TERMINAL_SECONDS, m.MAX_WALL_SECONDS), (30.0, 18.0, 6.0, 6.0, 150.0))
        self.assertEqual(m.CONTROL_HTTP_SECONDS, 2.5)

    def test_cgroup_boundary_and_one_byte_over(self):
        cap = 512 * 1024 * 1024 * 85 // 100
        with patch.object(m.Path, 'read_text', readings(used=cap)):
            self.assertTrue(m.memory_safe(123))
        with patch.object(m.Path, 'read_text', readings(used=cap + 1)):
            report = m.memory_status(123)
        self.assertEqual(report['reason'], 'CGROUP_MEMORY_LIMIT')
        self.assertEqual(report['cgroup_guard_bytes'], cap)
        self.assertEqual(report['cgroup_used_bytes'], cap + 1)

    def test_absolute_cgroup_cap_remains_450_mib(self):
        with patch.object(m.Path, 'read_text', readings(limit=1024 * 1024 * 1024,
                          used=450 * 1024 * 1024 + 1)):
            self.assertEqual(m.memory_status()['reason'], 'CGROUP_MEMORY_LIMIT')

    def test_child_rss_boundary_and_over(self):
        with patch.object(m.Path, 'read_text', readings(status='VmRSS:\t262144 kB\n')):
            self.assertTrue(m.memory_safe(123))
        with patch.object(m.Path, 'read_text', readings(status='VmRSS:\t262145 kB\n')):
            self.assertEqual(m.memory_status(123)['reason'], 'CHILD_RSS_LIMIT')

    def test_missing_and_error_classifications(self):
        for status, reason in ((FileNotFoundError('private-path'), 'PROC_STATUS_MISSING'),
                               (PermissionError('private-path'), 'PROC_STATUS_OS_ERROR'),
                               ('State:\tR (running)\n', 'PROC_RSS_MISSING'),
                               ('VmRSS:\tbad kB\n', 'PROC_RSS_INVALID'),
                               ('VmRSS:\n', 'PROC_RSS_INVALID')):
            with self.subTest(reason=reason), patch.object(m.Path, 'read_text', readings(status=status)):
                self.assertEqual(m.memory_status(123)['reason'], reason)

    def test_cgroup_errors_have_no_exit_grace(self):
        for limit in ('max', 'invalid', 0, -1):
            child = Child([False, True])
            with self.subTest(limit=limit), patch.object(m.Path, 'read_text', readings(limit=limit)), \
                    patch.object(m, 'reclaim_clean_scratch_cache', return_value=False), \
                    patch.object(m.time, 'sleep') as sleep, self.assertLogs(m.LOG, level='WARNING'):
                with self.assertRaisesRegex(m.GuardError, 'GUARD'):
                    m.check_process_memory(child, 'CHILD', 'GUARD')
                self.assertEqual(child.calls, 1)
                sleep.assert_not_called()

    def test_known_overage_not_waived_when_exit_is_next(self):
        for reader in (readings(used=500 * 1024 * 1024),
                       readings(status='VmRSS:\t300000 kB\n'),
                       readings(status=PermissionError('secret')), readings(status='VmRSS:\tbad kB\n')):
            child = Child([False, True])
            with patch.object(m.Path, 'read_text', reader), \
                    patch.object(m, 'reclaim_clean_scratch_cache', return_value=False), \
                    patch.object(m.time, 'sleep') as sleep, \
                    self.assertLogs(m.LOG, level='WARNING'):
                with self.assertRaisesRegex(m.GuardError, 'GUARD'):
                    m.check_process_memory(child, 'CONTROL', 'GUARD')
                self.assertEqual(child.calls, 1)
                sleep.assert_not_called()

    def test_cgroup_overage_continues_only_after_reclaim_and_strict_recheck(self):
        over = {'reason': 'CGROUP_MEMORY_LIMIT', 'cgroup_limit_bytes': 536870912,
                'cgroup_used_bytes': 456982528, 'cgroup_guard_bytes': 456340275,
                'child_rss_bytes': None, 'child_state': None}
        safe = dict(over, reason='SAFE', cgroup_used_bytes=440000000,
                    child_rss_bytes=223731712, child_state='R')
        child = Child([False])
        with patch.object(m, 'memory_status', side_effect=[over, safe]) as status, \
                patch.object(m, 'reclaim_clean_scratch_cache', return_value=True), \
                self.assertLogs(m.LOG, level='INFO') as logs:
            self.assertFalse(m.check_process_memory(child, 'CHILD', 'GUARD'))
        self.assertEqual(status.call_count, 2)
        self.assertIn('unchanged cgroup guard', ''.join(logs.output))

    def test_reclaim_cannot_waive_persistent_cgroup_overage(self):
        over = {'reason': 'CGROUP_MEMORY_LIMIT', 'cgroup_limit_bytes': 536870912,
                'cgroup_used_bytes': 456982528, 'cgroup_guard_bytes': 456340275,
                'child_rss_bytes': None, 'child_state': None}
        child = Child([False])
        with patch.object(m, 'memory_status', return_value=over), \
                patch.object(m, 'reclaim_clean_scratch_cache', return_value=False), \
                self.assertLogs(m.LOG, level='WARNING'):
            with self.assertRaisesRegex(m.GuardError, 'GUARD'):
                m.check_process_memory(child, 'CHILD', 'GUARD')

    def test_missing_rss_grace_returns_only_after_specific_reap(self):
        clock, child = Clock(), Child([False, False, True])
        with patch.object(m.Path, 'read_text', readings(status='State:\tR (running)\n')), \
                patch.object(m.time, 'monotonic', clock.monotonic), \
                patch.object(m.time, 'sleep', clock.sleep), \
                patch.object(m, 'direct_http', side_effect=AssertionError('No RPC during reap-only grace')), \
                patch.object(m, 'control_rpc', side_effect=AssertionError('No RPC during reap-only grace')), \
                self.assertLogs(m.LOG, level='INFO') as logs:
            self.assertTrue(m.check_process_memory(child, 'CHILD', 'GUARD'))
        self.assertEqual(child.calls, 3)
        self.assertEqual(clock.sleeps, [0.005])
        self.assertIn('memory_reason=PROC_RSS_MISSING', logs.output[0])
        self.assertIn('exit_grace=REAPED', logs.output[0])

    def test_missing_status_grace_requires_reap(self):
        clock, child = Clock(), Child()
        with patch.object(m.Path, 'read_text', readings(status=FileNotFoundError())), \
                patch.object(m.time, 'monotonic', clock.monotonic), \
                patch.object(m.time, 'sleep', clock.sleep), self.assertLogs(m.LOG, level='WARNING') as logs:
            with self.assertRaisesRegex(m.GuardError, 'GUARD'):
                m.check_process_memory(child, 'CONTROL', 'GUARD')
        self.assertAlmostEqual(clock.now - 100.0, 0.05)
        self.assertIn('exit_grace=EXPIRED', logs.output[0])

    def test_existing_helper_or_child_deadline_bounds_grace(self):
        for deadline in (100.0, 100.012, 100.05, 101.0):
            clock, child = Clock(), Child()
            with self.subTest(deadline=deadline), patch.object(m.Path, 'read_text', readings(status='State:\tR\n')), \
                    patch.object(m.time, 'monotonic', clock.monotonic), \
                    patch.object(m.time, 'sleep', clock.sleep), self.assertLogs(m.LOG, level='WARNING'):
                with self.assertRaises(m.GuardError):
                    m.check_process_memory(child, 'CHILD', 'GUARD', deadline)
                self.assertLessEqual(clock.now, min(100.05, deadline) + 1e-10)

    def test_reaped_child_skips_missing_proc_without_claiming_resource_pass(self):
        child = Child([True])
        with patch.object(m, 'memory_status') as status:
            self.assertTrue(m.check_process_memory(child, 'CHILD', 'GUARD'))
            status.assert_not_called()

    def test_scratch_error_not_waived_by_exit(self):
        child = Child([False, True])
        with patch.object(m.Path, 'read_text', readings()), patch.object(m, 'scratch_status',
                return_value={'reason': 'SCRATCH_TOTAL_LIMIT', 'bytes': m.MAX_SCRATCH_BYTES + 1, 'free_bytes': None}), \
                self.assertLogs(m.LOG, level='WARNING') as logs:
            with self.assertRaisesRegex(m.GuardError, 'SOURCE_RESIDENT_OR_SCRATCH_GUARD'):
                m.check_child_resources(child)
        self.assertEqual(child.calls, 1)
        self.assertIn('scratch_reason=SCRATCH_TOTAL_LIMIT', logs.output[0])

    def test_scratch_real_files_and_symlinks(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            source = root / 'source'
            source.mkdir()
            with patch.object(m, 'ROOT', root), patch.object(m, 'SOURCE_ROOT', source):
                (source / 'a').write_bytes(b'abc')
                self.assertEqual(m.scratch_status()['bytes'], 3)
                self.assertTrue(m.scratch_safe())
                (source / 'link').symlink_to(source / 'a')
                self.assertEqual(m.scratch_status()['reason'], 'SCRATCH_NON_REGULAR_FILE')

    def test_scratch_free_disk_guard(self):
        with tempfile.TemporaryDirectory() as root, patch.object(m, 'ROOT', Path(root)), \
                patch.object(m, 'SOURCE_ROOT', Path(root) / 'source'), \
                patch.object(m.shutil, 'disk_usage', return_value=SimpleNamespace(free=64 * 1024 * 1024 - 1)):
            self.assertEqual(m.scratch_status()['reason'], 'SCRATCH_FREE_SPACE_LIMIT')

    def test_diagnostics_never_log_proc_contents_or_error_text(self):
        child = Child([False, True])
        with patch.object(m.Path, 'read_text', readings(status=PermissionError('credential-secret'))), \
                self.assertLogs(m.LOG, level='WARNING') as logs:
            with self.assertRaises(m.GuardError):
                m.check_process_memory(child, 'CHILD', 'GUARD')
        self.assertNotIn('credential-secret', ''.join(logs.output))
        self.assertNotIn('private-do-not-log', ''.join(logs.output))


if __name__ == '__main__':
    unittest.main(verbosity=2)
