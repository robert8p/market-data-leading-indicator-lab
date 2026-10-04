"""Graceful admission tests with real bounded fixture processes and receipts."""
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

APP = Path(__file__).resolve().parents[1] / 'app'


def load(name):
    spec = importlib.util.spec_from_file_location(name + '_host_shutdown_test', APP / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HostGracefulShutdown(unittest.TestCase):
    def exercise_child(self, cache):
        module = load('eq20_cache_install' if cache else 'eq20_host_prepare')
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            admission, hard_stop = threading.Event(), threading.Event()
            child_started, allow_finish = root / 'started', root / 'finish'
            attempt = '44444444-4444-4444-8444-444444444444'
            receipt_path = root / (('cache_receipt_' if cache else 'receipt_') + attempt + '.json')
            calls, children, errors = [], [], []

            class RPC:
                def call(self, operation, owner, fence=None, args=None):
                    calls.append((operation, args))
                    if operation == 'status':
                        return {'enabled': True, 'state': 'INSTALLING_EXPORT', 'host_instance': socket.gethostname(),
                                'completed_parts': [], 'completed_archives': [], 'verified': False}
                    if operation == 'claim':
                        return {'acquired': True, 'fence': 7}
                    if operation == 'manifest':
                        return {'manifest_sha256': module.MANIFEST_SHA,
                                'manifest': {'part_metadata': [{'part_no': 0}, {'part_no': 1}]}}
                    if operation == 'reserve':
                        return {'attempt_id': attempt}
                    if operation == 'heartbeat':
                        return {'continue': True}
                    return {'committed': True}

            original_popen = subprocess.Popen
            fixture_code = """import json, pathlib, sys, time
root=pathlib.Path(sys.argv[1]); receipt=pathlib.Path(sys.argv[2])
(root/'started').touch()
end=time.monotonic()+5
while not (root/'finish').exists():
    if time.monotonic()>end: raise SystemExit(3)
    time.sleep(.01)
receipt.write_text(json.dumps({'success':True,'process_finished':True,'parts':[{'part_no':0}],
    'protected_outcomes_accessed':False,'discovery_fits_executed':0}))
"""
            def launch(*args, **kwargs):
                child = original_popen([sys.executable, '-I', '-S', '-c', fixture_code, str(root), str(receipt_path)],
                    start_new_session=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                children.append(child)
                return child

            rpc = RPC()
            def run():
                try:
                    if cache:
                        module.continue_cache_install(rpc, 'render_eq20_host_test', hard_stop, root,
                                                      lambda *_: True, 'a'*64, admission_stop=admission)
                    else:
                        module.one_cycle(rpc, 'render_eq20_host_test', hard_stop, admission_stop=admission)
                except BaseException as error:
                    errors.append(error)

            patches = [patch.object(module.subprocess, 'Popen', side_effect=launch),
                       patch.object(module.os, 'killpg', side_effect=AssertionError('Graceful shutdown killed child'))]
            if cache:
                patches += [patch.object(module, 'PrivateRPC', return_value=rpc),
                            patch.object(module, 'shutil_disk_free', return_value=1024**3)]
            else:
                patches += [patch.object(module, 'ROOT', root), patch.object(module, 'host_memory_safe', return_value=True)]
            for item in patches:
                item.start()
            thread = threading.Thread(target=run)
            try:
                thread.start()
                deadline = time.monotonic()+3
                while not child_started.exists() and not errors and time.monotonic()<deadline:
                    time.sleep(.01)
                self.assertTrue(child_started.exists(), errors)
                admission.set()
                self.assertFalse(hard_stop.is_set())
                thread.join(.05)
                self.assertTrue(thread.is_alive(), 'Active child must be allowed to finish')
                self.assertIsNone(children[0].poll())
                allow_finish.touch()
                thread.join(3)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(len(children), 1)
                self.assertEqual(children[0].returncode, 0)
                self.assertEqual(sum(op == 'reserve' for op, _ in calls), 1)
                settlements = [data for op, data in calls if op == 'settle']
                self.assertEqual(len(settlements), 1)
                self.assertTrue(settlements[0]['receipt']['success'])
                self.assertTrue(settlements[0]['receipt']['process_finished'])
                self.assertNotIn('termination_reason', settlements[0]['receipt'])
                self.assertFalse(any(op == 'release' for op, _ in calls), 'Natural lease expiry must preserve control state')
                if cache:
                    self.assertEqual(sum(op == 'commit' for op, _ in calls), 1)
            finally:
                allow_finish.touch()
                thread.join(3)
                for child in children:
                    if child.poll() is None:
                        child.kill()
                        child.wait(timeout=2)
                for item in reversed(patches):
                    item.stop()

    def test_export_current_child_settles_before_graceful_exit_without_next_reservation(self):
        self.exercise_child(cache=False)

    def test_cache_current_child_commits_and_settles_before_graceful_exit_without_next_reservation(self):
        self.exercise_child(cache=True)

    def test_graceful_stop_before_claim_has_no_rpc_or_process(self):
        stopped = threading.Event(); stopped.set()
        hard_stop = threading.Event()
        for name in ('eq20_host_prepare', 'eq20_cache_install'):
            module = load(name)
            with patch.object(module.subprocess, 'Popen') as launch:
                class NoRPC:
                    def call(self, *args, **kwargs):
                        raise AssertionError('No claim or status is admitted after stop')
                if name == 'eq20_host_prepare':
                    self.assertEqual(module.one_cycle(NoRPC(), 'owner', hard_stop, admission_stop=stopped), 60)
                else:
                    self.assertEqual(module.continue_cache_install(NoRPC(), 'owner', hard_stop, Path('/unused'),
                        lambda *_: True, 'a'*64, admission_stop=stopped), 60)
                launch.assert_not_called()

    def test_stored_thread_join_finishes_current_cycle_without_cancelling_child_event(self):
        module = load('eq20_host_prepare')
        with tempfile.TemporaryDirectory() as temporary:
            module.ROOT = Path(temporary)
            entered, finish = threading.Event(), threading.Event()
            calls = []
            def cycle(rpc, owner, hard_stop, admission_stop=None):
                calls.append(hard_stop)
                entered.set()
                if not finish.wait(2):
                    raise AssertionError('Fixture cycle release missing')
                self.assertFalse(hard_stop.is_set())
                self.assertTrue(admission_stop.is_set())
                return 30
            with patch.object(module, 'one_cycle', side_effect=cycle), patch.object(module, 'RPC', return_value=object()):
                module.start_background()
                self.assertTrue(entered.wait(2))
                module.request_stop()
                self.assertFalse(module.join_shutdown(.01))
                finish.set()
                self.assertTrue(module.join_shutdown(2))
                self.assertEqual(len(calls), 1)
                self.assertTrue(module.join_shutdown(0))


if __name__ == '__main__':
    unittest.main()
