import subprocess,threading,unittest
from unittest.mock import Mock
from app.eq17_verification_runner import _diagnostic,verify

class VerificationRunner(unittest.TestCase):
    def run_case(self,code,output=b'',stop=None):
        records=[];runner=Mock(return_value=subprocess.CompletedProcess([],code,stdout=output))
        stop=stop or Mock(is_set=Mock(return_value=False),wait=Mock(return_value=False))
        passed=verify([],'.',{},stop,records.append,runner=runner,clock=lambda:0)
        return passed,records,runner
    def test_success_dispatches_completion(self):
        passed,records,runner=self.run_case(0,b'Ran 803 tests in 4.2s\n\nOK\n');self.assertTrue(passed);self.assertEqual(records[-1]['status'],'complete');self.assertEqual(records[-1]['tests'],803);self.assertTrue(records[-1]['suite_ok']);self.assertEqual(runner.call_count,1)
    def test_owner_handover_is_once_only(self):
        passed,records,runner=self.run_case(75);self.assertFalse(passed);self.assertEqual(runner.call_count,1);self.assertEqual(records[-1]['reason'],'OWNER_OR_SOURCE_HANDOVER')
    def test_non_transient_failure_stops(self):
        output=b'Traceback\n  File "/private/root/positive9_verify.py", line 141, in main\nAssertionError: secret payload omitted\n'
        passed,records,runner=self.run_case(1,output);self.assertFalse(passed);self.assertEqual(runner.call_count,1);self.assertEqual(records[-1]['reason'],'EXECUTED_CHECK_FAILED');self.assertEqual(records[-1]['failure_class'],'ASSERTIONERROR');self.assertEqual(records[-1]['failure_location'],{'file':'positive9_verify.py','line':141,'function':'main'});self.assertNotIn('secret',str(records[-1]).lower())
    def test_retryable_marker_is_allowlisted(self):
        output=(b'EQ17_VERIFIER_RETRYABLE {"error":"AssertionError","failure_location":{"file":"positive9_verify.py","function":"main","line":148},"reason":"Current-release owner unavailable for control baseline"}\n')
        passed,records,runner=self.run_case(75,output);self.assertFalse(passed)
        self.assertEqual(records[-1]['handover_reason'],'Current-release owner unavailable for control baseline')
        self.assertEqual(records[-1]['failure_class'],'ASSERTIONERROR')
        self.assertEqual(records[-1]['failure_location'],{'file':'positive9_verify.py','line':148,'function':'main'})
    def test_http_marker_is_bounded_metadata_only(self):
        output=(b'EQ17_VERIFIER_HTTP {"body_prefix_bytes":19,"body_prefix_sha256":"'+b'a'*64+b'","body_truncated":false,"path":"/api/higher-observation","retried":false,"status":503}\n')
        passed,records,runner=self.run_case(1,output);self.assertFalse(passed)
        self.assertEqual(records[-1]['http_failure'],{'path':'/api/higher-observation','status':503,'body_prefix_bytes':19,'body_prefix_sha256':'a'*64,'body_truncated':False,'retried':False})
    def test_shutdown_stops_before_launch(self):
        stop=threading.Event();stop.set();passed,records,runner=self.run_case(0,stop=stop);self.assertFalse(passed);runner.assert_not_called()
    def test_timeout_blocks_and_retains_reason(self):
        records=[];runner=Mock(side_effect=subprocess.TimeoutExpired('verifier',720))
        self.assertFalse(verify([],'.',{},threading.Event(),records.append,runner=runner));self.assertEqual(records[-1]['reason'],'VERIFIER_TIMEOUT')
    def test_diagnostic_is_bounded_metadata_only(self):
        value=_diagnostic(b'x'*70000+b'\nRan 514 tests in 1.0s\n\nOK\n',total=90000,digest='a'*64)
        self.assertEqual(value['tests'],514);self.assertTrue(value['suite_ok']);self.assertTrue(value['output_tail_truncated']);self.assertEqual(value['output_sha256'],'a'*64);self.assertNotIn('x'*100,str(value))

if __name__=='__main__':unittest.main()
