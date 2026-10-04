"""Exercise the actual helper with a slow scratch census, without networking."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


PATH = Path(__file__).resolve().parents[1] / 'app' / 'eq20_source_supervisor.py'


class DispatchPreflightTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('dispatch_preflight_subject', PATH)
        self.subject = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.subject)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.subject.ROOT = Path(self.temp.name)
        self.subject.SOURCE_ROOT = self.subject.ROOT / 'source_execution'
        self.directory = self.subject.make_directory(self.subject.SOURCE_ROOT / 'control' / 'fixture')
        self.target = self.directory / ('control_' + 'a' * 32 + '.dispatch.json')

    def test_slow_census_precedes_half_second_acquisition_window(self):
        m = self.subject
        clock = SimpleNamespace(now=100.0)
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        identity = {'pid': os.getpid(), 'start_ticks': 123, 'process_group': os.getpid(), 'boot_id': boot}
        anchor = {'server_epoch': 1700000000.0, 'received_monotonic': 100.0, 'boot_id': boot}
        request = self.directory / ('control_' + 'a' * 32 + '.request.json')
        paths = m.control_paths(request)
        envelope = {'version': 'SOURCE_CONTROL_ENVELOPE_V1',
            'rpc': {'p_action': 'CHECK', 'p_payload': {'owner': 'render_eq20_source_fixture'}},
            'server_anchor': anchor, 'boot_id': boot, 'created_monotonic': 100.0,
            'bootstrap_deadline_monotonic': 102.5}
        raw = m.canonical(envelope)
        m.atomic_write(request, raw, immutable=True)
        m.atomic_write(paths['process'], m.canonical({'request_sha256': m.sha256(raw),
                       'process_identity': identity}), immutable=True)
        observations = []

        def census(extra=0):
            reservations = list(self.directory.glob('*.dispatch.json.new_*'))
            if reservations:
                self.assertEqual(reservations[0].stat().st_size, 4096)
            clock.now += 0.65  # A real cache-size-dependent delay that exceeded the old wire window.
            return True

        def http(rpc, args, timeout, maximum):
            server_now = anchor['server_epoch'] + clock.now - anchor['received_monotonic']
            deadline = m.parse_timestamp(args['p_payload']['request_start_deadline_at'])
            self.assertGreater(deadline, server_now)
            self.assertAlmostEqual(deadline - server_now, 0.5, places=5)
            marker = json.loads(paths['dispatch'].read_bytes())
            self.assertEqual(marker['dispatch_monotonic'], clock.now)
            self.assertEqual(paths['dispatch'].stat().st_mode & 0o777, 0o400)
            observations.append(marker)
            return {'continue': True}

        with patch.object(m, 'time', SimpleNamespace(monotonic=lambda: clock.now,
                process_time=lambda: 0.01, sleep=lambda seconds: None)), \
                patch.object(m, 'scratch_safe', side_effect=census), \
                patch.object(m, 'assert_proc_namespace'), \
                patch.object(m, 'lowered_limit'), patch.object(m, 'prohibit_descendants'), \
                patch.object(m, 'process_identity', return_value=identity), \
                patch.object(m.signal, 'setitimer'), patch.object(m.signal, 'signal'), \
                patch.object(m, 'direct_http', side_effect=http):
            result = m.helper_run(str(request), str(paths['response']), '2.5')
            self.assertEqual(result, 0, paths['response'].read_text() if paths['response'].exists() else 'no response')
        self.assertEqual(len(observations), 1)
        self.assertTrue(json.loads(paths['response'].read_bytes())['success'])
        self.assertEqual((m.PARENT_SECONDS, m.TERMINAL_SECONDS, m.RESERVED_SECONDS), (6, 6, 30))

    def test_failed_scratch_preflight_leaves_no_marker_or_reservation(self):
        with patch.object(self.subject, 'scratch_safe', return_value=False):
            with self.assertRaisesRegex(self.subject.GuardError, 'SOURCE_SHARED_SCRATCH_BOUND'):
                with self.subject.prepared_control_dispatch(self.target):
                    self.fail('Unsafe publication admitted')
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_publication_cannot_exceed_reserved_size_or_overwrite(self):
        m = self.subject
        with m.prepared_control_dispatch(self.target) as publish:
            with self.assertRaisesRegex(m.GuardError, 'SOURCE_CONTROL_DISPATCH_REJECTED'):
                publish(b'a' * 4097)
            publish(b'{"durable":true}')
            with self.assertRaisesRegex(m.GuardError, 'IMMUTABLE_SOURCE_ALREADY_EXISTS'):
                publish(b'{}')
        before = self.target.read_bytes()
        with self.assertRaisesRegex(m.GuardError, 'IMMUTABLE_SOURCE_ALREADY_EXISTS'):
            with m.prepared_control_dispatch(self.target):
                self.fail('Overwrite admitted')
        self.assertEqual(self.target.read_bytes(), before)
        self.assertFalse(list(self.directory.glob('*.new_*')))

    def test_interruption_before_publication_removes_only_own_reservation(self):
        unrelated = self.directory / 'evidence.json'
        unrelated.write_text('retained')
        with self.assertRaisesRegex(RuntimeError, 'interruption'):
            with self.subject.prepared_control_dispatch(self.target):
                raise RuntimeError('interruption')
        self.assertEqual(unrelated.read_text(), 'retained')
        self.assertFalse(self.target.exists())
        self.assertFalse(list(self.directory.glob('*.new_*')))


if __name__ == '__main__':
    unittest.main()
