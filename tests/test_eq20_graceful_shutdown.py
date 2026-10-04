"""Exercise real controller loops and shutdown hooks without market IO."""
import ast
import importlib.util
import logging
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch


APP = Path(__file__).resolve().parents[1] / 'app'


def load(name):
    spec = importlib.util.spec_from_file_location(name + '_shutdown_test', APP / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class GracefulShutdownTests(unittest.TestCase):
    def exercise_loop(self, name):
        module = load(name)
        with tempfile.TemporaryDirectory() as temporary:
            module.ROOT = Path(temporary)
            if name == 'eq20_source_supervisor':
                module.SOURCE_ROOT = module.ROOT / 'source_execution'
            entered, finish = threading.Event(), threading.Event()
            calls = []

            def bounded_step(*args):
                calls.append(args)
                entered.set()
                if not finish.wait(2):
                    raise AssertionError('Test did not release its bounded step')
                if name == 'eq20_source_supervisor':
                    # Host shutdown does not cancel the current source child.
                    self.assertFalse(args[1].is_set())
                return 30

            with patch.object(module, 'supervise_once', side_effect=bounded_step), \
                    patch.object(module, 'RPC', create=True, return_value=object()):
                module.start_background()
                self.assertTrue(entered.wait(2))
                module.request_stop()
                self.assertFalse(module.join_shutdown(.01))
                finish.set()
                self.assertTrue(module.join_shutdown(2))
                self.assertEqual(len(calls), 1)
                module.request_stop()
                self.assertTrue(module.join_shutdown(0))

    def test_source_finishes_current_step_and_admits_no_next_attempt(self):
        self.exercise_loop('eq20_source_supervisor')

    def test_runner_finishes_current_step_and_admits_no_next_attempt(self):
        self.exercise_loop('eq20_runner_orchestrator')

    def test_worker_stops_only_loaded_lanes_and_shares_one_join_deadline(self):
        tree = ast.parse((APP / 'worker.py').read_text())
        selected = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name in ('_handle_signal', '_wait_eq20_atomic_shutdown')]
        clock = SimpleNamespace(now=10.0)
        events, waits = [], []
        def join(timeout):
            waits.append(timeout)
            clock.now += 100
            return True
        def lane(name):
            return SimpleNamespace(request_stop=lambda: events.append(name), join_shutdown=join)
        modules = {'app.eq20_source_supervisor': lane('source'),
                   'app.eq20_runner_orchestrator': lane('runner'),
                   'app.eq20_mission_continuation': lane('mission'),
                   'unrelated_research': lane('unrelated')}
        namespace = dict(sys=SimpleNamespace(modules=modules),
                         time=SimpleNamespace(monotonic=lambda: clock.now),
                         logger=logging.getLogger('shutdown_test'),
                         shutdown_event=threading.Event())
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(APP / 'worker.py'), 'exec'), namespace)
        namespace['_handle_signal'](15, None)
        self.assertTrue(namespace['shutdown_event'].is_set())
        self.assertEqual(events, ['source', 'runner', 'mission'])
        namespace['_wait_eq20_atomic_shutdown']()
        self.assertEqual(waits, [210.0, 110.0, 10.0])


if __name__ == '__main__':
    unittest.main()
