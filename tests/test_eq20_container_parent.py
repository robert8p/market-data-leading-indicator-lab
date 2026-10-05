import os
from pathlib import Path
import subprocess
import sys
import types
import unittest
from unittest.mock import patch,MagicMock

ROOT=Path(__file__).resolve().parents[1]
MODULE=ROOT/'app/eq20_runner_process.py'

class ContainerParentTests(unittest.TestCase):
    def setUp(self):
        self.m=types.ModuleType('parent_test');self.m.__file__=str(MODULE)
        exec(compile(MODULE.read_bytes(),str(MODULE),'exec'),self.m.__dict__)
    def test_pid_one_is_valid_only_when_it_is_actual_parent(self):
        libc=MagicMock();libc.prctl.return_value=0
        with patch.object(self.m.os,'getppid',return_value=1),patch.object(self.m.ctypes,'CDLL',return_value=libc):
            self.m.bind_parent_lifetime(1)
        libc.prctl.assert_called_once()
        with patch.object(self.m.os,'getppid',return_value=2):
            with self.assertRaises(RuntimeError): self.m.bind_parent_lifetime(1)
    def test_nonpositive_or_boolean_parent_is_rejected(self):
        for pid in (0,-1,True,None):
            with self.subTest(pid=pid),self.assertRaises(RuntimeError):self.m.bind_parent_lifetime(pid)
    def test_real_failure_retains_safe_error_code_and_exits(self):
        result=subprocess.run([sys.executable,'-I','-S',str(MODULE),'--controller','0'],env=dict(os.environ,RENDER_SERVICE_ID=self.m.TARGET_SERVICE),capture_output=True,text=True,timeout=3)
        self.assertEqual(result.returncode,1)
        self.assertIn('code=ISOLATED_CONTROLLER_LINUX_PARENT_REQUIRED',result.stderr)
        self.assertIn('parent_pid=',result.stderr)
        self.assertNotIn('Traceback',result.stderr)

if __name__=='__main__':unittest.main()
