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


if __name__ == "__main__":
    unittest.main()
