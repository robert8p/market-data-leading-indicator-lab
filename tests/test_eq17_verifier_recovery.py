import hashlib,tempfile,unittest
from pathlib import Path
from app.eq17_verifier_recovery import (OLD_PROVENANCE,NEW_PROVENANCE,
    OLD_RETRY,NEW_RETRY,OLD_HTTP,NEW_HTTP,OLD_ATTEMPT,NEW_ATTEMPT,
    OLD_FINAL,NEW_FINAL,patched_source,recovery_script)

class VerifierRecovery(unittest.TestCase):
    def fixture(self):
        return ("CHANGED=frozenset({'src/positive9_verify.py','tests/test_rule_retirement.py'})\n"
            "  if retryable:sys.exit(75)\n"+OLD_HTTP+"\n"+OLD_ATTEMPT+"\n"+OLD_FINAL+"\n")
    def test_exact_five_source_changes(self):
        source=self.fixture();digest=hashlib.sha256(source.encode()).hexdigest()
        recovered=patched_source(source,digest)
        self.assertEqual(source.count(OLD_PROVENANCE),1);self.assertEqual(recovered.count(NEW_PROVENANCE),1)
        self.assertEqual(source.count(OLD_RETRY),1);self.assertEqual(recovered.count(NEW_RETRY),1)
        self.assertEqual(source.count(OLD_HTTP),1);self.assertEqual(recovered.count(NEW_HTTP),1)
        self.assertEqual(source.count(OLD_ATTEMPT),1);self.assertEqual(recovered.count(NEW_ATTEMPT),1)
        self.assertEqual(source.count(OLD_FINAL),1);self.assertEqual(recovered.count(NEW_FINAL),1)
        self.assertEqual(recovered.replace(NEW_PROVENANCE,OLD_PROVENANCE).replace(NEW_RETRY,OLD_RETRY).replace(NEW_HTTP,OLD_HTTP).replace(NEW_ATTEMPT,OLD_ATTEMPT).replace(NEW_FINAL,OLD_FINAL),source)
    def test_mismatched_source_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError,'SOURCE_PIN_MISMATCH'):
            patched_source(self.fixture(),'0'*64)
    def test_release_mismatch_has_no_side_effect(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'src').mkdir()
            with self.assertRaisesRegex(RuntimeError,'RELEASE_MISMATCH'):
                with recovery_script(root,'0'*64):pass
            self.assertEqual(list((root/'src').iterdir()),[])

if __name__=='__main__':unittest.main()
