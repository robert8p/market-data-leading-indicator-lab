"""Narrow timeout proof tests: synthetic journals, real files and one OS wait4.

There are no network requests or research rows. The OS test really installs the
existing no-descendants filter and reaps its own child. Its journal timestamps
and identity start ticks are explicit fixtures because this test host exposes a
different /proc PID namespace; it is not a production retry or timer claim.
"""
import copy
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

MODULE = Path(__file__).resolve().parents[1] / 'app' / 'eq20_source_supervisor.py'
spec = importlib.util.spec_from_file_location('transient_subject', MODULE)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

JOB = {'attempt_id': 'b658de66-739d-42cf-a7e7-b9686c51ed39', 'owner': 'render_eq20_source_test',
       'host_instance': 'test-host', 'fence': 123, 'config_sha256': 'a' * 64}
IDENTITY = {'pid': 4321, 'start_ticks': 789, 'process_group': 4321,
            'boot_id': '01234567-89ab-cdef-0123-456789abcdef', 'state': 'S'}


def journal(identity=None):
    return {'version': 1, 'process_identity': identity or copy.deepcopy(IDENTITY),
            'descendant_creation_blocked': True, 'child_cpu_seconds': .1,
            'rpc_calls': 2, 'rpc_elapsed_seconds': .5,
            'child_operation_metrics': {'NEXT': {'calls': 2, 'cpu_seconds': .01, 'rpc_elapsed_seconds': .5}},
            'pending': {'operation_key': 'NEXT', 'rpc_name': m.FAST_RPC,
                        'timeout_seconds': 3.0, 'started_monotonic': 96.8}}


def evidence(doc=None, **override):
    values = dict(observed_monotonic=100.0, child_cpu=.2, parent_cpu=.4,
                  parent_rpc=.5, peak_rss=64 * 1024 * 1024)
    values.update(override)
    return m.transient_timeout_journal_evidence(m.canonical(doc or journal()), JOB, IDENTITY, **values)


def budget():
    return SimpleNamespace(cpu=lambda: .4, rpc_elapsed=.5, helper_cpu=.2,
                           helper_measurement_verified=True)


class TransientTimeoutTests(unittest.TestCase):
    def setUp(self):
        m._shutdown_requested.clear()

    def tearDown(self):
        m._shutdown_requested.clear()

    def test_exact_readback_hash_and_separate_incomplete_measurement(self):
        raw = m.canonical(journal())
        result = evidence()
        self.assertEqual(result['journal_utf8'].encode(), raw)
        self.assertEqual(result['actual_journal_readback_sha256'], m.sha256(raw))
        self.assertTrue(result['eligible_for_server_review'])
        self.assertFalse(result['complete_measurement_claimed'])
        self.assertFalse(result['continuation_authorized_by_this_supplement'])
        self.assertAlmostEqual(result['known_child_cpu_plus_rpc_seconds'], 3.9)

    def test_stable_identity_accepts_os_state_change_only(self):
        doc = journal()
        doc['process_identity']['state'] = 'Z'
        self.assertTrue(evidence(doc)['eligible_for_server_review'])
        for key, value in [('pid', 4322), ('start_ticks', 790), ('process_group', 4322),
                           ('boot_id', '11234567-89ab-cdef-0123-456789abcdef')]:
            doc = journal(); doc['process_identity'][key] = value
            with self.subTest(key=key), self.assertRaises(m.GuardError):
                evidence(doc)

    def test_isolation_and_exact_pending_rpc_required(self):
        mutations = [lambda j: j.update(descendant_creation_blocked=False),
                     lambda j: j.update(pending=None),
                     lambda j: j['pending'].update(rpc_name=m.DATA_RPC),
                     lambda j: j['pending'].update(operation_key='NOT_ALLOWED'),
                     lambda j: j['pending'].update(timeout_seconds=9.0),
                     lambda j: j['pending'].update(started_monotonic=99.0)]
        for change in mutations:
            doc = journal(); change(doc)
            with self.subTest(change=change), self.assertRaises(m.GuardError):
                evidence(doc)

    def test_all_existing_routes_keep_exact_timeout(self):
        for op in m.DATA_OPERATIONS | {'CONTROL_CHECK', 'CONTROL_BUNDLE_FILE'}:
            route = (m.CONTROL_RPC, 3.0) if op.startswith('CONTROL_') else m.source_data_route(op)
            doc = journal()
            doc['pending'].update(operation_key=op, rpc_name=route[0], timeout_seconds=route[1],
                                  started_monotonic=100.0-route[1]-.2)
            with self.subTest(operation=op):
                self.assertTrue(evidence(doc)['eligible_for_server_review'])

    def test_missing_or_mismatched_completed_census_is_not_retry_proof(self):
        for change in (lambda j: j.update(child_operation_metrics=None),
                       lambda j: j.update(rpc_calls=1),
                       lambda j: j.update(rpc_elapsed_seconds=.4)):
            doc = journal(); change(doc)
            with self.assertRaises(m.GuardError):
                evidence(doc)

    def test_nonfinite_and_known_resource_violations_are_closed(self):
        for fields in ({'child_cpu': float('nan')}, {'parent_cpu': float('inf')},
                       {'parent_rpc': -1}, {'child_cpu': 18.1}, {'parent_cpu': 5.6},
                       {'peak_rss': m.MAX_CHILD_RSS_BYTES+1}, {'peak_rss': None}):
            with self.subTest(fields=fields), self.assertRaises(m.GuardError):
                evidence(**fields)
        doc = journal(); doc['child_cpu_seconds'] = 1.0
        with self.assertRaises(m.GuardError):
            evidence(doc)

    def test_full_control_read_bound_is_enforced(self):
        with self.assertRaises(m.GuardError):
            m.transient_timeout_journal_evidence(b' ' * 8193, JOB, IDENTITY,
                observed_monotonic=100.0, child_cpu=.2, parent_cpu=.4, parent_rpc=.5, peak_rss=1)

    def test_non_timeout_and_planned_stop_cannot_read_or_authorize(self):
        with patch.object(m, 'read_bounded', side_effect=AssertionError('No journal IO')):
            for reason in (None, 'SOURCE_MEMORY_GUARD', 'SOURCE_CHILD_COMBINED_GUARD', 'STOP'):
                self.assertIsNone(m.transient_timeout_supplement(JOB, None, budget(), reason))
            m._shutdown_requested.set()
            self.assertFalse(m.transient_timeout_supplement(JOB, None, budget(),
                'SOURCE_CHILD_RPC_DEADLINE')['eligible_for_server_review'])

    def test_missing_proof_preserves_an_ordinary_full_charge_failure(self):
        child = SimpleNamespace(finished=True, usage=SimpleNamespace(ru_maxrss=65536), cpu=.2,
                                exit_code=-9, termination_proof='SPECIFIC_CHILD_WAIT4', original_identity=None)
        result = m.terminal_receipt(JOB, child, None, budget(), 'SOURCE_CHILD_RPC_DEADLINE')
        self.assertFalse(result['success'])
        self.assertFalse(result['measurement_verified'])
        self.assertFalse(result['descendant_creation_blocked'])
        self.assertEqual(result['measured_cpu_seconds'], 30)
        self.assertEqual(result['error'], 'SOURCE_CHILD_RPC_DEADLINE')
        self.assertFalse(result['transient_timeout_evidence']['eligible_for_server_review'])

    def test_real_isolated_child_wait4_and_exact_files_retain_failed_accounting(self):
        code = ("import importlib.util,signal; s=importlib.util.spec_from_file_location('m'," + repr(str(MODULE)) + "); "
                "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
                "assert m.prohibit_descendants(); print('ISOLATED',flush=True); signal.pause()")
        proc = subprocess.Popen([sys.executable, '-c', code], start_new_session=True,
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        reaped = False
        try:
            self.assertEqual(proc.stdout.readline(), b'ISOLATED\n')
            os.kill(proc.pid, signal.SIGKILL)
            pid, status, usage = os.wait4(proc.pid, 0)
            reaped = True
            proc.returncode = os.waitstatus_to_exitcode(status)
            self.assertEqual(pid, proc.pid); self.assertEqual(proc.returncode, -9)
            identity = dict(IDENTITY, pid=pid, process_group=pid)
            child = SimpleNamespace(finished=True, usage=usage, cpu=usage.ru_utime+usage.ru_stime,
                                    exit_code=-9, termination_proof='SPECIFIC_CHILD_WAIT4', original_identity=identity)
            doc = journal(identity); doc['child_cpu_seconds'] = 0.0
            with tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp) / 'attempts' / JOB['attempt_id']; directory.mkdir(parents=True)
                (directory / 'job.json').write_bytes(m.canonical({'job': JOB}))
                (directory / 'child_process.json').write_bytes(m.canonical(identity))
                (directory / 'budget.json').write_bytes(m.canonical(doc))
                with patch.object(m, 'SOURCE_ROOT', Path(tmp)), patch.object(m, 'checked_path', side_effect=lambda p: Path(p)), \
                     patch.object(m.time, 'monotonic', return_value=100.0):
                    result = m.terminal_receipt(JOB, child, None, budget(), 'SOURCE_CHILD_RPC_DEADLINE')
                    self.assertTrue(result['transient_timeout_evidence']['eligible_for_server_review'])
                    self.assertFalse(result['success']); self.assertFalse(result['measurement_verified'])
                    self.assertFalse(result['descendant_creation_blocked'])
                    self.assertEqual(result['measured_cpu_seconds'], 30)
                    self.assertEqual(result['termination_proof'], 'SPECIFIC_CHILD_WAIT4')
                    changed = dict(identity, start_ticks=identity['start_ticks']+1)
                    (directory / 'child_process.json').write_bytes(m.canonical(changed))
                    self.assertFalse(m.transient_timeout_supplement(JOB, child, budget(),
                        'SOURCE_CHILD_RPC_DEADLINE')['eligible_for_server_review'])
        finally:
            if not reaped:
                proc.kill(); proc.wait(timeout=2)
            proc.stdout.close(); proc.stderr.close()


if __name__ == '__main__':
    unittest.main()
