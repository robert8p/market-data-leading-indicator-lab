import importlib.util,json,pathlib,unittest
spec=importlib.util.spec_from_file_location('integrity',pathlib.Path(__file__).resolve().parents[1]/'app'/'eq17_release_integrity.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
def fixture():
    f={'manifest.json':'{"rules":[]}','src/latency.py':'WORKERS=3\n'};groups=[]
    for family in (5,10,20,30,50):
        path='manifest.json' if family==5 else f'groups/{family}.json'
        if family!=5:f[path]='{"rules":[]}'
        groups.append(dict(family=family,manifest_path=path,manifest_hash=m.sha256(f[path])))
    f['research_groups.json']=json.dumps(dict(groups=groups),separators=(',',':'))
    obj=dict(files=f,manifest_hash=m.sha256(f['manifest.json']),research_groups_hash=m.sha256(f['research_groups.json']))
    raw=json.dumps(obj,sort_keys=True,separators=(',',':'));return raw,m.sha256(raw)
class TestIntegrity(unittest.TestCase):
    def test_valid(self):
        raw,pin=fixture();self.assertTrue(m.validate_release(raw,pin))
    def test_patched_release(self):
        raw,pin=fixture();packed,h=m.repack_release(raw,pin,{'manifest.json':'{"rules":[],"workers":4}','src/latency.py':'WORKERS=4\n'})
        self.assertTrue(m.validate_release(packed,h))
        a=json.loads(packed);self.assertEqual(json.loads(a['files']['research_groups.json'])['groups'][0]['manifest_hash'],m.sha256(a['files']['manifest.json']))
    def test_original_outage_reproduced(self):
        raw,_=fixture();a=json.loads(raw);a['files']['manifest.json']='{"rules":[],"workers":4}';a['manifest_hash']=m.sha256(a['files']['manifest.json'])
        corrupt=json.dumps(a,sort_keys=True,separators=(',',':'))
        with self.assertRaisesRegex(m.ReleaseIntegrityError,'GROUP_MANIFEST_HASH'):m.validate_release(corrupt,m.sha256(corrupt))
    def test_changed_source_attestation(self):
        raw,pin=fixture();p,h=m.repack_release(raw,pin,{'src/latency.py':'WORKERS=4\n'});a=json.loads(p);a['files']['src/latency.py']='corrupted';c=json.dumps(a,sort_keys=True,separators=(',',':'))
        with self.assertRaisesRegex(m.ReleaseIntegrityError,'VERIFIED_FILE_HASH'):m.validate_release(c,m.sha256(c))
    def test_outer_hash(self):
        raw,pin=fixture()
        with self.assertRaisesRegex(m.ReleaseIntegrityError,'OUTER_HASH'):m.validate_release(raw+' ',pin)
    def test_missing_main_manifest_fails_closed(self):
        raw, _ = fixture()
        a = json.loads(raw)
        del a['files']['manifest.json']
        broken = json.dumps(a, sort_keys=True, separators=(',', ':'))
        with self.assertRaisesRegex(m.ReleaseIntegrityError, 'TOP_LEVEL_MANIFEST_HASH'):
            m.validate_release(broken, m.sha256(broken))

    def test_missing_secondary_group_manifest_fails_closed(self):
        raw, _ = fixture()
        a = json.loads(raw)
        del a['files']['groups/10.json']
        broken = json.dumps(a, sort_keys=True, separators=(',', ':'))
        with self.assertRaisesRegex(m.ReleaseIntegrityError, 'GROUP_MANIFEST_HASH'):
            m.validate_release(broken, m.sha256(broken))

    def test_path_traversal(self):
        raw,_=fixture();a=json.loads(raw);a['files']['../escape']='bad';c=json.dumps(a,sort_keys=True,separators=(',',':'))
        with self.assertRaisesRegex(m.ReleaseIntegrityError,'UNSAFE_FILE'):m.validate_release(c,m.sha256(c))
if __name__=='__main__':unittest.main()
