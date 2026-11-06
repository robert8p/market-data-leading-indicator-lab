from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

from app.eq17_verifier_recovery import NEW_FINAL
from app.eq17_verification_runner import _verifier_caller, verify


class CallerDiagnosticTests(unittest.TestCase):
    def run_trace(self, owner=True, function='verify'):
        # Exercise the exact generated exception-emission fragment with real frame objects.
        snippet = NEW_FINAL.split('  failure=dict', 1)[0]
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root/'src').mkdir()
            driver = root/'fixture.py'
            frame_path = (root/'src' if owner else root/'other')/'identity_primary_verify.py'
            opener = root/'src'/'positive9_verify_recovery.py'
            driver.write_text(
                "import os, pathlib, traceback\n"
                f"ROOT=pathlib.Path({td!r})\n"
                "ctx={}\n"
                "exec(compile(\"def read():\\n raise TimeoutError('https://private.scope/SECRET')\\n\","
                "'/stdlib/ssl.py','exec'),ctx)\n"
                f"exec(compile('def {function}():\\n read()\\n',{str(frame_path)!r},'exec'),ctx)\n"
                f"exec(compile('def main():\\n {function}()\\n',{str(opener)!r},'exec'),ctx)\n"
                "try:ctx['main']()\nexcept Exception as e:\n"+snippet+"  raise\n")
            records = []
            passed = verify([sys.executable,str(driver)],root,{},threading.Event(),records.append)
            self.assertFalse(passed)
            result = records[-1]
            self.assertEqual(result['returncode'],1)
            self.assertEqual(result['failure_location'],dict(file='ssl.py',line=2,function='read'))
            self.assertEqual(result['failure_class'],'TIMEOUTERROR')
            self.assertEqual(result['reason'],'EXECUTED_CHECK_FAILED')
            self.assertNotIn('SECRET',str(result))
            self.assertNotIn('https:',str(result))
            self.assertNotIn(td,str(result))
            self.assertNotIn('EQ17_VERIFIER_CALLER',str(result))
            return result['verifier_failure_location']

    def test_last_owned_frame_before_tls_leaf(self):
        self.assertEqual(self.run_trace(),dict(file='identity_primary_verify.py',function='verify',line=2))

    def test_same_filename_outside_pinned_root_is_ignored(self):
        self.assertEqual(self.run_trace(owner=False),dict(file='positive9_verify_recovery.py',function='main',line=2))

    def test_method_not_in_closed_vocabulary_is_ignored(self):
        self.assertEqual(self.run_trace(function='unreviewed'),dict(file='positive9_verify_recovery.py',function='main',line=2))

    def test_no_marker_from_text_spoofs_wrong_nonce_or_ambiguous_input(self):
        nonce='c'*32; prefix=b'EQ17_VERIFIER_CALLER '+nonce.encode()
        for raw in [b'  File "/x/src/identity_primary_verify.py", line 3, in verify\nTimeoutError:',
                    b'EQ17_VERIFIER_CALLER '+b'd'*32+b' 0 3',prefix+b' 99 3',prefix+b' 0 100000',
                    prefix+b' 0 0',prefix+b' 0 -3',prefix+b' 0 3 https://private',prefix+b' 0 3\n'+prefix+b' 0 4']:
            with self.subTest(raw=raw):self.assertIsNone(_verifier_caller(raw,nonce))

    def test_natural_success_does_not_emit_caller(self):
        def fake(command,**kwargs):
            marker='EQ17_VERIFIER_CALLER '+kwargs['env']['EQ17_VERIFIER_CALLER_NONCE']+' 0 3'
            return subprocess.CompletedProcess(command,0,stdout=marker.encode())
        records=[]
        self.assertTrue(verify([],'.',{},threading.Event(),records.append,runner=fake))
        self.assertNotIn('verifier_failure_location',records[-1])

    def test_non_timeout_failure_retains_original_behavior(self):
        def fake(command,**kwargs):
            marker='EQ17_VERIFIER_CALLER '+kwargs['env']['EQ17_VERIFIER_CALLER_NONCE']+' 0 3'
            return subprocess.CompletedProcess(command,1,stdout=(marker+'\nValueError: private\n').encode())
        records=[]
        self.assertFalse(verify([],'.',{},threading.Event(),records.append,runner=fake))
        self.assertEqual(records[-1]['failure_class'],'VALUEERROR')
        self.assertNotIn('verifier_failure_location',records[-1])

    def test_public_log_allowlists_only_short_location(self):
        source=(Path(__file__).resolve().parents[1]/'app/eq17_bootstrap.py').read_text()
        line=next(l for l in source.splitlines() if 'safe={k:value.get(k)' in l)
        self.assertIn("'verifier_failure_location'",line)
        self.assertNotIn('CALLER_NONCE',line)
