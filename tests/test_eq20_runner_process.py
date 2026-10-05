from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch, MagicMock

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
MODULE = ROOT/'app/eq20_runner_process.py'
BUILDER = ROOT/'scripts/eq20_isolation_image.py'


def load(path, name):
    module = types.ModuleType(name); module.__file__ = str(path)
    exec(compile(path.read_bytes(),str(path),'exec'),module.__dict__)
    return module


class IsolationTests(unittest.TestCase):
    def setUp(self):
        self.m = load(MODULE,'isolated_test')
        self.tmp = tempfile.TemporaryDirectory(); self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def fixture(self, loop="def loop(): pass\n"):
        raw = ("import threading\n_stop=threading.Event()\ndef request_stop(): _stop.set()\n"+loop).encode()
        path = self.root/'fixture.py'; path.write_bytes(raw)
        return path, hashlib.sha256(raw).hexdigest()

    def test_target_service_and_existing_opt_in_required(self):
        for yes in ('true','yes','1','on',' TRUE '):
            self.assertTrue(self.m.selected_service(dict(RENDER_SERVICE_ID=self.m.TARGET_SERVICE,EQ20_RUNNER_ENABLED=yes)))
        self.assertFalse(self.m.selected_service(dict(RENDER_SERVICE_ID='other',EQ20_RUNNER_ENABLED='true')))
        self.assertFalse(self.m.selected_service(dict(RENDER_SERVICE_ID=self.m.TARGET_SERVICE,EQ20_RUNNER_ENABLED='false')))

    def test_environment_isolated_without_mutating_parent(self):
        original=dict(PYTHONPATH='bad',PYTHONHOME='bad',EQ20_RUNNER_ENABLED='true',OTHER_ENABLED='true',SUPABASE_SERVICE_ROLE_KEY='synthetic-not-a-key')
        env=self.m.child_environment(original)
        self.assertNotIn('PYTHONPATH',env);self.assertNotIn('PYTHONHOME',env)
        self.assertEqual(env['EQ20_RUNNER_ENABLED'],'false')
        self.assertEqual(env['OTHER_ENABLED'],'false')
        self.assertEqual(env['SUPABASE_SERVICE_ROLE_KEY'],original['SUPABASE_SERVICE_ROLE_KEY'])
        self.assertEqual(original['EQ20_RUNNER_ENABLED'],'true')
        self.assertEqual(env['OPENBLAS_NUM_THREADS'],'1')

    def test_exact_bytes_load_but_changed_bytes_rejected(self):
        path,sha=self.fixture(); self.m.RUNNER_PATH=path;self.m.RUNNER_SHA256=sha
        module=self.m.load_pinned_runner(); self.assertEqual(module.__file__,str(path))
        self.assertTrue(callable(module.loop))
        path.write_text('raise AssertionError("must never execute")')
        with self.assertRaisesRegex(RuntimeError,'EXACT_ORIGINAL_HASH'):
            self.m.load_pinned_runner()

    def test_symlink_and_oversized_file_rejected(self):
        path,sha=self.fixture();link=self.root/'link.py';link.symlink_to(path)
        self.m.RUNNER_PATH=link;self.m.RUNNER_SHA256=sha
        with self.assertRaises(RuntimeError):self.m.load_pinned_runner()
        self.m.RUNNER_PATH=path;path.write_bytes(b' '*131073)
        with self.assertRaises(RuntimeError):self.m.load_pinned_runner()

    def test_single_start_and_stopped_start(self):
        env=dict(RENDER_SERVICE_ID=self.m.TARGET_SERVICE,EQ20_RUNNER_ENABLED='true')
        with patch.dict(self.m.os.environ,env), patch.object(self.m.threading,'Thread') as thread:
            self.assertTrue(self.m.start_background());self.assertFalse(self.m.start_background())
            self.assertEqual(thread.call_count,1)
        second=load(MODULE,'stopped_test');second.request_stop()
        with patch.dict(second.os.environ,env),patch.object(second.subprocess,'Popen') as popen:
            self.assertFalse(second.start_background());popen.assert_not_called()

    def test_manager_does_not_restart_failed_process(self):
        proc=MagicMock();proc.wait.return_value=1
        with patch.object(self.m.subprocess,'Popen',return_value=proc) as popen:
            self.m._manage_once()
        self.assertEqual(popen.call_count,1);self.assertEqual(proc.wait.call_count,1)
        cmd=popen.call_args.args[0]
        self.assertEqual(cmd[1:3],['-I','-S']);self.assertNotIn('-m',cmd)
        self.assertTrue(popen.call_args.kwargs['start_new_session'])
        self.assertTrue(popen.call_args.kwargs['close_fds'])

    def test_stop_during_launch_terminates_same_process(self):
        proc=MagicMock();proc.wait.return_value=0
        def launch(*a,**k):self.m._stop.set();return proc
        with patch.object(self.m.subprocess,'Popen',side_effect=launch):self.m._manage_once()
        proc.terminate.assert_called_once()

    def test_shutdown_does_not_spawn_or_kill_unknown_process(self):
        self.m.request_stop();self.assertTrue(self.m.join_shutdown(0))
        proc=MagicMock();proc.poll.return_value=0;self.m._process=proc
        self.m.request_stop();proc.terminate.assert_not_called()

    def test_parent_mismatch_rejected_before_prctl(self):
        with patch.object(self.m.os,'getppid',return_value=123),patch.object(self.m.ctypes,'CDLL') as libc:
            with self.assertRaises(RuntimeError):self.m.bind_parent_lifetime(124)
            libc.assert_not_called()

    def test_parent_death_registration_and_race_fail_closed(self):
        libc=MagicMock();libc.prctl.return_value=0
        with patch.object(self.m.os,'getppid',side_effect=[123,1]),patch.object(self.m.ctypes,'CDLL',return_value=libc):
            with self.assertRaises(RuntimeError):self.m.bind_parent_lifetime(123)
        self.assertEqual(libc.prctl.call_args.args,(1,int(signal.SIGTERM),0,0,0))

    def test_prctl_failure_rejected(self):
        libc=MagicMock();libc.prctl.return_value=-1
        with patch.object(self.m.os,'getppid',return_value=123),patch.object(self.m.ctypes,'CDLL',return_value=libc):
            with self.assertRaises(RuntimeError):self.m.bind_parent_lifetime(123)

    def launch_fixture(self, loop):
        path,sha=self.fixture(loop)
        code=("import types,pathlib,os; p=pathlib.Path("+repr(str(MODULE))+ ");m=types.ModuleType('fixture_isolation');m.__file__=str(p);exec(compile(p.read_bytes(),str(p),'exec'),m.__dict__);m.RUNNER_PATH=pathlib.Path("+repr(str(path))+");m.RUNNER_SHA256="+repr(sha)+";raise SystemExit(m.isolated_main(os.getppid()))")
        env=dict(os.environ,RENDER_SERVICE_ID=self.m.TARGET_SERVICE)
        return subprocess.Popen([sys.executable,'-I','-S','-u','-c',code],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)

    def test_real_isolated_process_graceful_stop(self):
        child=self.launch_fixture("def loop():\n print('READY',flush=True)\n _stop.wait(8)\n print('STOPPED',flush=True)\n")
        try:
            self.assertEqual(child.stdout.readline().strip(),'READY')
            child.send_signal(signal.SIGTERM);out,err=child.communicate(timeout=5)
            self.assertEqual(child.returncode,0);self.assertIn('STOPPED',out)
            self.assertIn('threads=1',err);self.assertIn('native_fingerprints_changed=false',err)
        finally:
            if child.poll() is None:child.kill();child.wait()

    def test_real_process_clock_excludes_parent_work(self):
        child=self.launch_fixture("import time,json,os\ndef loop():\n start=time.process_time();wall=time.monotonic();print('READY',flush=True)\n time.sleep(.35)\n print(json.dumps(dict(cpu=time.process_time()-start,wall=time.monotonic()-wall,pid=os.getpid())),flush=True)\n")
        try:
            self.assertEqual(child.stdout.readline().strip(),'READY')
            begin=time.process_time()
            while time.process_time()-begin<.15:sum(range(2000))
            out,err=child.communicate(timeout=3);result=json.loads(out.strip())
            self.assertEqual(child.returncode,0);self.assertLess(result['cpu'],.05)
            self.assertNotEqual(result['pid'],os.getpid())
        finally:
            if child.poll() is None:child.kill();child.wait()

    def test_real_native_runner_pin_is_unchanged_in_repository(self):
        path=ROOT/'app/eq20_runner_orchestrator.py'
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),self.m.RUNNER_SHA256)

    def test_outer_build_changes_only_bootstrap_routes(self):
        b=load(BUILDER,'builder_test')
        init=ROOT/'app/__init__.py';worker=ROOT/'app/worker.py'
        for path,blob,old,new,count in ((init,b.INIT_BLOB,b.OLD_IMPORT,b.NEW_IMPORT,1),(worker,b.WORKER_BLOB,b.OLD_LIFECYCLE,b.NEW_LIFECYCLE,2)):
            raw=path.read_bytes();changed=b.transform(raw,blob,old,new,count)
            self.assertEqual(changed.decode().replace(new,old).encode(),raw)
            with self.assertRaises(ValueError):b.transform(changed,blob,old,new,count)


if __name__=='__main__':unittest.main()
