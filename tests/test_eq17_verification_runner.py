import subprocess,threading,unittest
from unittest.mock import Mock
from app.eq17_verification_runner import verify

class VerificationRunner(unittest.TestCase):
    def run_case(self,codes,stop=None):
        records=[];runner=Mock(side_effect=[subprocess.CompletedProcess([],c) for c in codes])
        stop=stop or Mock(is_set=Mock(return_value=False),wait=Mock(return_value=False))
        passed=verify([],'.',{},stop,records.append,runner=runner,clock=lambda:0)
        return passed,records,runner
    def test_success_dispatches_completion(self):
        passed,records,runner=self.run_case([0]);self.assertTrue(passed);self.assertEqual(records[-1]['status'],'complete');self.assertEqual(runner.call_count,1)
    def test_owner_handover_retries_sequentially(self):
        passed,records,runner=self.run_case([75,0]);self.assertTrue(passed);self.assertEqual(runner.call_count,2);self.assertIn('retry_pending',[r['status'] for r in records])
    def test_non_transient_failure_stops(self):
        passed,records,runner=self.run_case([1,0]);self.assertFalse(passed);self.assertEqual(runner.call_count,1);self.assertEqual(records[-1]['reason'],'FAILED_CHECK')
    def test_retry_budget_is_bounded(self):
        passed,records,runner=self.run_case([75,75,75]);self.assertFalse(passed);self.assertEqual(runner.call_count,3);self.assertEqual(records[-1]['reason'],'RETRY_BUDGET_EXHAUSTED')
    def test_shutdown_stops_before_launch(self):
        stop=threading.Event();stop.set();passed,records,runner=self.run_case([0],stop);self.assertFalse(passed);runner.assert_not_called()
    def test_timeout_blocks_and_retains_reason(self):
        records=[];runner=Mock(side_effect=subprocess.TimeoutExpired('verifier',720))
        self.assertFalse(verify([],'.',{},threading.Event(),records.append,runner=runner));self.assertEqual(records[-1]['reason'],'VERIFIER_TIMEOUT')

if __name__=='__main__':unittest.main()
