import unittest
from bounded_worker import Worker, RpcError


class WorkflowTests(unittest.TestCase):
    def worker(self, replies):
        w = Worker({"SUPABASE_URL": "https://example.supabase.co", "SUPABASE_SERVICE_ROLE_KEY": "test",
                    "RENDER_GIT_COMMIT": "revision", "RENDER_SERVICE_ID": "service"})
        calls = []
        def rpc(action, job=None, error=None):
            calls.append(action)
            result = replies.pop(0)
            if isinstance(result, Exception):
                raise result
            return result
        w.rpc = rpc
        return w, calls

    def test_guarded_claim_never_executes(self):
        w, calls = self.worker([{"state": "GATED"}])
        self.assertEqual(w.cycle(), "GATED")
        self.assertEqual(calls, ["CLAIM"])

    def test_checkpoint_ack(self):
        w, calls = self.worker([{"state": "CLAIMED", "job": {}}, {"state": "SAVED"}, {"state": "COMMITTED"}])
        self.assertEqual(w.cycle(), "COMMITTED")
        self.assertEqual(calls, ["CLAIM", "PROCESS", "ACK"])

    def test_uncertain_commit_requests_reconciliation(self):
        w, calls = self.worker([{"state": "CLAIMED", "job": {}}, {"state": "SAVED"}, RpcError("TRANSPORT_UNCERTAIN"), {"state": "ALREADY_COMPLETE"}])
        self.assertEqual(w.cycle(), "RECOVERY_PENDING")
        self.assertEqual(calls[-1], "FAIL")

    def test_interruption_never_acknowledges(self):
        w, calls = self.worker([{"state": "CLAIMED", "job": {}}, {"state": "INTERRUPT_CHECKPOINT_TEST"}])
        with self.assertRaises(SystemExit) as c:
            w.cycle()
        self.assertEqual(c.exception.code, 75)
        self.assertEqual(calls, ["CLAIM", "PROCESS"])

    def test_redirect_and_invalid_endpoint_rejected(self):
        for url in ["http://example.com", "https://user@example.com", "https://example.com/path"]:
            with self.assertRaises(RpcError):
                Worker({"SUPABASE_URL": url})

    def permit(self):
        return {"state": "WAIT", "wait": {"id": "wait-1", "fence": 1, "stop": 0,
                "ttl_seconds": 900, "delay_seconds": 15, "max_probes": 4}}

    def test_main_resumes_explicit_wait_and_commits(self):
        w, calls = self.worker([self.permit(), {"state": "RESUME"},
            {"state": "CLAIMED", "job": {}}, {"state": "SAVED"},
            {"state": "COMMITTED"}, {"state": "COMPLETE"}])
        w.sleep = lambda _: None
        parked = []
        w.park = lambda state: parked.append(state)
        w.main()
        self.assertEqual(calls, ["CLAIM", "CONTROL", "CLAIM", "PROCESS", "ACK", "CLAIM"])
        self.assertEqual(parked, ["COMPLETE"])

    def test_wait_exhaustion_is_finite_and_reported(self):
        w, calls = self.worker([self.permit() for _ in range(4)] + [{"state": "HOLD"}])
        w.sleep = lambda _: None
        self.assertFalse(w.resume_wait(self.permit()))
        self.assertEqual(calls, ["CONTROL"] * 4 + ["WAIT_EXHAUSTED"])

    def test_wait_does_not_resume_a_changed_fence_or_stop(self):
        for state in ["STOPPED", "HOLD", "BUDGET_HOLD", "CODE_GATED"]:
            w, calls = self.worker([{"state": state}])
            w.sleep = lambda _: None
            self.assertFalse(w.resume_wait(self.permit()))
            self.assertEqual(calls, ["CONTROL"])

    def test_invalid_or_repeated_wait_never_polls(self):
        w, calls = self.worker([])
        p = self.permit(); p["wait"]["max_probes"] = 500
        self.assertFalse(w.resume_wait(p))
        w.wait_episodes = 2
        self.assertFalse(w.resume_wait(self.permit()))
        self.assertEqual(calls, [])

    def test_signal_stop_prevents_a_wait_probe(self):
        w, calls = self.worker([])
        w.sleep = lambda _: setattr(w, "stopping", True)
        self.assertFalse(w.resume_wait(self.permit()))
        self.assertEqual(calls, [])

    def test_hard_holds_do_not_poll_or_restart(self):
        for state in ["GATED", "STOPPED", "HOLD", "BUDGET_HOLD", "CODE_GATED", "HTTP_401"]:
            w, calls = self.worker([{"state": state}])
            parked = []; w.park = lambda x: parked.append(x)
            w.main()
            self.assertEqual(calls, ["CLAIM"])
            self.assertEqual(parked, [state])


if __name__ == "__main__":
    unittest.main()
