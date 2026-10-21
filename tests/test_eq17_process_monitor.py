"""Test subprocess failure detection and bounded retained diagnostics offline."""
import importlib.util
import io
import json
import pathlib
import unittest

spec = importlib.util.spec_from_file_location(
    'eq17_process_monitor', pathlib.Path(__file__).resolve().parents[1]
    / 'app' / 'eq17_process_monitor.py',
)
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)
RELEASE = 'a' * 64


class FakeProcess:
    def __init__(self, stderr, returncode):
        self.stderr = io.StringIO(stderr)
        self.returncode = returncode
        self.waits = 0

    def wait(self):
        self.waits += 1
        return self.returncode


class FakeConnection:
    def __init__(self, output):
        self.output = output

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, values):
        self.output.append((sql, values))


class MonitorTests(unittest.TestCase):
    def test_child_crash_retained_as_safe_classification(self):
        child = FakeProcess('Traceback redacted\nRuntimeError: secret-password=hidden\n', 1)
        status, reason = monitor.inspect_child(child)
        self.assertEqual((status, reason), (1, 'RUNTIMEERROR'))
        self.assertEqual(child.waits, 1)
        self.assertTrue(child.stderr.closed)
        event = monitor.make_event('runtime.py', status, reason, RELEASE, 2, 0.345)
        payload = json.loads(event[4])
        self.assertEqual(payload['component'], 'runtime.py')
        self.assertEqual(payload['reason_code'], 'RUNTIMEERROR')
        self.assertNotIn('secret', event[4].lower())

    def test_manifest_mismatch_is_classified(self):
        status, reason = monitor.inspect_child(FakeProcess(
            'RuntimeError: Group manifest hash mismatch\n', 1))
        self.assertEqual((status, reason), (1, 'GROUP_MANIFEST_HASH_MISMATCH'))

    def test_unknown_exception_never_reaches_storage(self):
        ev = monitor.make_event('server.py', -9,
                                'credential password: secret', RELEASE, 1, 1.5)
        self.assertEqual(json.loads(ev[4])['reason_code'], 'UNCLASSIFIED')
        self.assertNotIn('secret', ev[4])

    def test_crash_persistence_uses_existing_database_table(self):
        rows = []
        j = monitor.ExitJournal(lambda: FakeConnection(rows))
        event = monitor.make_event('server.py', 2, 'SYNTAXERROR', RELEASE, 1, 3)
        self.assertEqual(j.record(event), 1)
        self.assertEqual(len(j.pending), 0)
        self.assertEqual(len(rows), 1)
        self.assertIn('ON CONFLICT (id) DO NOTHING', rows[0][0])
        self.assertEqual(rows[0][1][0], event[0])

    def test_transient_database_outage_keeps_same_event_id(self):
        rows = []
        failing = {'fail': True}

        def connect():
            if failing['fail']:
                raise ConnectionError('do not log this secret')
            return FakeConnection(rows)

        j = monitor.ExitJournal(connect)
        event = monitor.make_event('runtime.py', 1, 'OPERATIONALERROR', RELEASE, 3, 7)
        self.assertEqual(j.record(event), 0)
        self.assertEqual(len(j.pending), 1)
        failing['fail'] = False
        self.assertEqual(j.flush(), 1)
        self.assertEqual(rows[0][1][0], event[0])
        self.assertEqual(j.flush(), 0)
        self.assertEqual(len(rows), 1)

    def test_bounded_queue_discards_oldest_only(self):
        j = monitor.ExitJournal(lambda: (_ for _ in ()).throw(ConnectionError()),
                                max_pending=2)
        for i in range(3):
            event = monitor.make_event('runtime.py', i + 1,
                                       'UNCLASSIFIED', RELEASE, i + 1, i)
            j.record(event)
        self.assertEqual(len(j.pending), 2)
        self.assertEqual(j.evicted, 1)
        self.assertEqual([json.loads(e[4])['exit_status'] for e in j.pending], [2, 3])


if __name__ == '__main__':
    unittest.main()
