import hashlib,json,tempfile,unittest
from pathlib import Path
from app.eq17_verifier_recovery import (RELEASE,OLD_PROVENANCE,NEW_PROVENANCE,OLD_HTTP_GET,NEW_HTTP_GET,
    OLD_RETRY,NEW_RETRY,OLD_HTTP,NEW_HTTP,OLD_ATTEMPT,NEW_ATTEMPT,
    OLD_FINAL,NEW_FINAL,patched_source,recovery_script)

OLD_MEMBER_GUARD = "  if name not in CHANGED_BASELINE_FILES|retirement_files:assert current==body.encode(),name"
NEW_MEMBER_GUARD = "  exact_parent_delta={'tests/test_news_snapshot_generation.py': ['0498ea5c42bd49a917b4280970d9a484a30e5b41fe6225c1cab5e33144848e08', '0bd76d0b013bd057f2bf8c2a6a1cbf7303f56d91c26316526fb47366ef331fa8'], 'src/providers.py': ['cbc7fe7deff7838ec8434640b8ca3b0f472cebe9d245a4db6fc088af5090cc3f', 'd9b47a67611cb76ea4ccdcce38b9fe398b999142f78221855a0525398ed9f2c4'], 'src/preferred_reference.py': ['372a543e3b005dc7232602ccdf77c8ea1a911935537f7131559aa9671a3b18fa', '8ea3ccbd3ec973d62919d250a4696a57aafb12366a03282393384dee48c7cf49'], 'src/figi_reference.py': ['f2767f2005f24d1282f4d2a97e461351bef8f407d38ad5d1304cfce75b240b49', '6ed3096bae37f4bae542b24c46944a77fbfdd838570a127bf405154a82f82aca'], 'tests/test_source_repair.py': ['70670e91fca23e141d334064348c4afe09fca0760d0c0a699ad67b00a29d4bd7', 'ee72b6f95c464f9f16ff75b4cc8be6d979a579ce5053b293556f40e1f83a931c']}\n  if name in exact_parent_delta:\n   assert [hashlib.sha256(body.encode()).hexdigest(), hashlib.sha256(current).hexdigest()]==exact_parent_delta[name],name\n  elif name not in CHANGED_BASELINE_FILES|retirement_files:assert current==body.encode(),name"

class VerifierRecovery(unittest.TestCase):
    def fixture(self):
        return ("CHANGED=frozenset({'src/positive9_verify.py','tests/test_rule_retirement.py'})\n"
            +OLD_HTTP_GET+"\n"+OLD_MEMBER_GUARD+"\n  if retryable:sys.exit(75)\n"+OLD_HTTP+"\n"+OLD_ATTEMPT+"\n"+OLD_FINAL+"\n")
    def test_exact_seven_source_changes(self):
        source=self.fixture();digest=hashlib.sha256(source.encode()).hexdigest()
        recovered=patched_source(source,digest)
        changes=((OLD_PROVENANCE,NEW_PROVENANCE),(OLD_HTTP_GET,NEW_HTTP_GET),(OLD_MEMBER_GUARD,NEW_MEMBER_GUARD),
                 (OLD_RETRY,NEW_RETRY),(OLD_HTTP,NEW_HTTP),(OLD_ATTEMPT,NEW_ATTEMPT),(OLD_FINAL,NEW_FINAL))
        restored=recovered
        for old,new in changes:
            self.assertEqual(source.count(old),1)
            self.assertEqual(recovered.count(new),1)
            restored=restored.replace(new,old,1)
        self.assertEqual(restored,source)
    def test_member_guard_requires_one_exact_match(self):
        for label,guard in (('missing',''),('duplicated',OLD_MEMBER_GUARD+'\n'+OLD_MEMBER_GUARD),
                            ('changed',OLD_MEMBER_GUARD.replace('body.encode()','body'))):
            with self.subTest(label=label):
                source=self.fixture().replace(OLD_MEMBER_GUARD,guard)
                digest=hashlib.sha256(source.encode()).hexdigest()
                with self.assertRaisesRegex(RuntimeError,'^VERIFIER_EXACT_MEMBER_SCOPE_MISMATCH$'):
                    patched_source(source,digest)
    def test_mismatched_source_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError,'SOURCE_PIN_MISMATCH'):
            patched_source(self.fixture(),'0'*64)
    def test_release_matches_configured_recipe(self):
        recipe=json.loads((Path(__file__).resolve().parents[1]/'ops'/'eq17_gate_recipe_pin.json').read_text())
        self.assertEqual(RELEASE,recipe['expected_release'])
    def test_release_mismatch_has_no_side_effect(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'src').mkdir()
            with self.assertRaisesRegex(RuntimeError,'RELEASE_MISMATCH'):
                with recovery_script(root,'0'*64):pass
            self.assertEqual(list((root/'src').iterdir()),[])
    def test_source_mismatch_has_no_output_write(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);source=root/'src'/'positive9_verify.py'
            source.parent.mkdir();source.write_text('wrong source')
            with self.assertRaisesRegex(RuntimeError,'SOURCE_PIN_MISMATCH'):
                with recovery_script(root,RELEASE):pass
            self.assertEqual(list((root/'src').iterdir()),[source])
            self.assertEqual(source.read_text(),'wrong source')

if __name__=='__main__':unittest.main()


