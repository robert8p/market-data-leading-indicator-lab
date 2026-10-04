"""Use the exact repository runner to test its guard-preserving image patch."""
from __future__ import annotations
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('runner_diagnostic_builder',ROOT/'scripts/eq20_runner_diagnostic_patch.py')
builder=importlib.util.module_from_spec(spec);spec.loader.exec_module(builder)


def load_source(name,source,path):
    module=types.ModuleType(name);module.__file__=str(path)
    exec(compile(source,str(path),'exec'),module.__dict__)
    return module


class RunnerDiagnosticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original=(ROOT/'app/eq20_runner_orchestrator.py').read_bytes()
        cls.image=builder.patched_bytes(cls.original)
        cls.runtime=load_source('runner_diagnostic_image',cls.image,ROOT/'app/eq20_runner_orchestrator.py')
        cls.reference=load_source('runner_diagnostic_reference',cls.original,ROOT/'app/eq20_runner_orchestrator.py')

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.old_root=self.runtime.ROOT;self.runtime.ROOT=self.root
        self.runtime._shutdown_requested.clear()

    def tearDown(self):
        self.runtime.ROOT=self.old_root;self.tmp.cleanup()

    def test_exact_predecessor_and_reversible_patch(self):
        self.assertEqual(hashlib.sha256(self.original).hexdigest(),builder.BASE_SHA256)
        restored=self.image.decode()
        for before,after in reversed(builder.REPLACEMENTS):
            restored=restored.replace(after,before,1)
        self.assertEqual(restored.encode(),self.original)

    def test_unknown_source_or_double_patch_rejected(self):
        for value in (self.original+b'\n',self.image):
            with self.assertRaises(ValueError):builder.patched_bytes(value)

    def test_existing_runtime_limits_unchanged(self):
        m=self.runtime;r=self.reference
        for field in ('MAX_FILE','MAX_PREPARED_FILE','MAX_SCRATCH_BYTES',
                      'MAX_CORRECTED_PART_BYTES','REQUIRED_TRANSITION_BYTES',
                      'EXTENSION_BUNDLE_SHA256','CORRECTED_EXTENSION_BUNDLE_SHA256',
                      'EXTENSION_FILES','CORRECTED_EXTENSION_FILES'):
            self.assertEqual(getattr(m,field),getattr(r,field))

    def _memory_case(self,used=100,limit=536870912,status='State:\tS\nVmRSS:\t65536 kB\n',failure=None):
        def read(path,*args,**kwargs):
            if str(path).endswith('memory.max'):return str(limit)
            if str(path).endswith('memory.current'):return str(used)
            if failure:raise failure
            return status
        with patch.object(Path,'read_text',read):
            expected=self.reference.memory_safe(321)
            result=self.runtime.runner_memory_observation(321)
            self.assertEqual(result['safe'],expected)
            return result

    def test_memory_predicate_parity_at_boundaries(self):
        for used in (0,100,456340275,456340276,471859200,536870912):
            for rss in (0,1024,262143,262144,262145,524288):
                with self.subTest(used=used,rss=rss):
                    self._memory_case(used=used,status='State:\tS\nVmRSS:\t%d kB\n'%rss)

    def test_missing_and_terminal_proc_state_parity(self):
        for state in ('S','R','Z','X',''):
            self._memory_case(status='State:\t%s\n'%state)
        self._memory_case(failure=FileNotFoundError())
        self._memory_case(failure=PermissionError())
        self._memory_case(status='State:\tS\nVmRSS:\tbad kB\n')

    def test_cgroup_failure_has_exact_reason(self):
        self.assertEqual(self._memory_case(used=500000000)['reason'],'CGROUP_MEMORY_GUARD')

    def test_rss_failure_has_exact_reason(self):
        self.assertEqual(self._memory_case(status='State:\tS\nVmRSS:\t300000 kB\n')['reason'],'CHILD_RSS_GUARD')

    def test_missing_rss_does_not_invent_memory_overrun(self):
        self.assertEqual(self._memory_case(status='State:\tS\n')['reason'],'CHILD_RSS_UNAVAILABLE')

    def test_process_cpu_still_controls_admission(self):
        result=self.runtime.runner_guard_observation(10,3.01,.001,321)
        self.assertEqual(result['guard_reason'],'RUNNER_PARENT_PROCESS_CPU_GUARD')
        self.assertEqual(result['parent_cpu_guard_scope'],'PROCESS_WIDE_UNCHANGED')
        self.assertEqual(result['parent_thread_cpu_seconds'],.001)

    def test_exact_guard_order_and_boundaries(self):
        m=self.runtime
        for wall in (0,149,150,150.001):
            for cpu in (0,2.99,3,3.001):
                for memory in (True,False):
                    with patch.object(m,'runner_memory_observation',return_value=dict(safe=memory,reason='CHILD_RSS_GUARD')) as read:
                        result=m.runner_guard_observation(wall,cpu,.01,321)
                    expected=wall>150 or cpu>3 or not memory
                    self.assertEqual(result['guard_reason'] is not None,expected)
                    self.assertEqual(read.call_count,int(wall<=150 and cpu<=3))
                    if wall>150:self.assertEqual(result['guard_reason'],'RUNNER_WALL_GUARD')
                    elif cpu>3:self.assertEqual(result['guard_reason'],'RUNNER_PARENT_PROCESS_CPU_GUARD')

    def test_phase_marker_is_bounded_metadata_not_completion(self):
        attempt='12345678-1234-1234-1234-123456789abc'
        self.runtime.record_runner_phase(attempt,'CORRECTED_PART_FETCH_VERIFIED',94)
        value=self.runtime.read_runner_phase(attempt)
        self.assertEqual(value['part_no'],94)
        self.assertIs(value['stage_completion_claimed'],False)
        self.assertLess((self.root/('runner_phase_'+attempt+'.json')).stat().st_size,1024)

    def test_phase_marker_rejects_wrong_attempt_extra_fields_and_large_file(self):
        attempt='12345678-1234-1234-1234-123456789abc'
        m=self.runtime;path=self.root/('runner_phase_'+attempt+'.json')
        m.record_runner_phase(attempt,'MARKET_INPUTS_START');original=json.loads(path.read_text())
        for change in (dict(attempt_id='f'*36),dict(secret='must-not-be-returned'),dict(phase='ARBITRARY'),dict(observed_unix_seconds=float('nan'))):
            path.write_text(json.dumps(dict(original,**change)))
            self.assertIsNone(m.read_runner_phase(attempt))
        path.write_bytes(b' '*1025);self.assertIsNone(m.read_runner_phase(attempt))

    def test_phase_marker_rejects_path_and_invalid_part(self):
        for attempt,part in (('../../escape',1),('a'*36,-1),('a'*36,7590),('a'*36,True)):
            with self.assertRaises(ValueError):self.runtime.record_runner_phase(attempt,'CORRECTED_PART_FETCH_START',part)

    def _supervise(self,reason=None,*,exit_code=None,success=False,action='PREPARE_BINDING',heartbeat=True):
        m=self.runtime;attempt='12345678-1234-1234-1234-123456789abc'
        seal=self.root/'sealed_inputs/cache/seal.json';seal.parent.mkdir(parents=True);seal.write_text('{}')
        if success:(self.root/('runner_receipt_'+attempt+'.json')).write_text(json.dumps(dict(success=True)))
        class Process:
            pid=12345
            returncode=exit_code
            def poll(self):return self.returncode
            def wait(self,timeout=None):self.returncode=-9;return self.returncode
        child=Process();calls=[]
        class Rpc:
            def call(self,op,owner,fence=None,args=None):
                calls.append((op,args))
                if op=='status':return dict(desired='RUN',stage='NEEDS_PREPARE')
                if op=='claim':return dict(acquired=True,fence=1)
                if op=='reserve':return dict(attempt_id=attempt,action=action,config_sha256='c'*64,input_binding_sha256='d'*64,
                    config=dict(extension_bundle_sha256='e'*64,unit_manifest_sha256='f'*64))
                if op=='heartbeat':return dict(continue_=heartbeat,**{'continue':heartbeat})
                return {}
        from contextlib import ExitStack
        with ExitStack() as stack:
            stack.enter_context(patch.object(m,'reconcile_handoff',return_value={}))
            stack.enter_context(patch.object(m,'cleanup_corrected_spools'))
            stack.enter_context(patch.object(m,'memory_safe',return_value=True))
            stack.enter_context(patch.object(m,'scratch_safe',return_value=True))
            stack.enter_context(patch.object(m.subprocess,'Popen',return_value=child))
            killed=stack.enter_context(patch.object(m.os,'killpg'))
            stack.enter_context(patch.object(m.time,'sleep'))
            stack.enter_context(patch.object(m,'runner_guard_observation',return_value=dict(guard_reason=reason)))
            delay=m.supervise_once(Rpc(),'test_owner')
        receipt=next(args['receipt'] for op,args in calls if op=='finish')
        return receipt,delay,killed.call_count

    def test_actual_supervisor_reports_each_kill_reason(self):
        for reason in ('RUNNER_WALL_GUARD','RUNNER_PARENT_PROCESS_CPU_GUARD','RUNNER_CHILD_RSS_GUARD','RUNNER_CGROUP_MEMORY_GUARD'):
            with self.subTest(reason=reason):
                # Each invocation has a distinct scratch directory but the same synthetic job id.
                if (self.root/'sealed_inputs').exists():
                    import shutil;shutil.rmtree(self.root/'sealed_inputs')
                receipt,delay,kills=self._supervise(reason)
                self.assertFalse(receipt['success']);self.assertEqual(receipt['error'],'CHILD_EXIT_NO_RECEIPT')
                self.assertEqual(receipt['supervisor_diagnostics']['termination_reason'],reason)
                self.assertEqual((delay,kills),(60,1))

    def test_closed_heartbeat_is_not_mislabelled_memory_failure(self):
        receipt,_,_=self._supervise(heartbeat=False)
        self.assertEqual(receipt['supervisor_diagnostics']['termination_reason'],'RUNNER_HEARTBEAT_CONTINUATION_CLOSED')

    def test_unsupervised_sigkill_is_explicitly_unknown(self):
        receipt,_,kills=self._supervise(exit_code=-9)
        self.assertEqual(kills,0)
        self.assertEqual(receipt['supervisor_diagnostics']['termination_reason'],'NO_SUPERVISOR_KILL_OBSERVED')
        self.assertFalse(receipt['supervisor_diagnostics']['historical_kill_cause_inferred'])

    def test_successful_completion_preserved(self):
        receipt,delay,kills=self._supervise(exit_code=0,success=True)
        self.assertTrue(receipt['success']);self.assertEqual((delay,kills),(1,0))

    def test_required_intentional_interruption_preserved(self):
        receipt,delay,kills=self._supervise(exit_code=86,action='QA_INTERRUPT')
        self.assertTrue(receipt['success']);self.assertTrue(receipt['intentional_interrupt'])
        self.assertEqual((delay,kills),(1,0))
