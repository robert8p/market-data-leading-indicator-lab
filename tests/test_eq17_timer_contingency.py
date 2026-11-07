import ast
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import MagicMock, Mock, patch

from app import eq17_timer_contingency as gate

ROOT = Path(__file__).resolve().parents[1]
CURRENT = '1'*40

def terminal_record():
    return dict(status='blocked',reason='EXECUTED_CHECK_FAILED',attempt=1,returncode=1,
        release=gate.TIMER,git_commit=gate.ORIGINAL_GIT,checked=100.25,tests=None,suite_ok=False,
        output_bytes=12,output_sha256='2'*64,failure_class='FIXTURE',other='synthetic value')

def verified():
    return dict(status='verified',release=gate.TIMER,base_release=gate.BASE,builder_sha256=gate.BUILDER,
        tests_passed=True,suite_executed=True,reconstruct_only=False,tests_discovered=3,
        returncode=0,builder_returncode=0,controls_preserved=True,log_sha256='3'*64,
        started=100.0,git_commit=gate.ORIGINAL_GIT,tests_skipped=[])

def function(module,name,globals_):
    parsed=ast.parse((ROOT/'app'/module).read_text())
    node=next(n for n in parsed.body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module([node],type_ignores=[]),module,'exec'),globals_)
    return globals_[name]

class Contingency(unittest.TestCase):
    def setUp(self):
        # Deliberately synthetic, test-local pins. The operational payload is private.
        raw=json.dumps(terminal_record(),sort_keys=True).encode()
        projection=json.dumps({k:verified()[k] for k in gate.SUITE_FIELDS},
                              sort_keys=True,separators=(',',':')).encode()
        self.enterContext(patch.object(gate,'TERMINAL_HASH',hashlib.sha256(raw).hexdigest()))
        self.enterContext(patch.object(gate,'SUITE_FINGERPRINT',hashlib.sha256(projection).hexdigest()))

    def test_exact_bytes_and_every_terminal_field(self):
        original=terminal_record();raw=json.dumps(original,sort_keys=True)
        self.assertEqual(hashlib.sha256(raw.encode()).hexdigest(),gate.TERMINAL_HASH)
        gate.terminal(raw)
        for key in original:
            with self.subTest(key=key),self.assertRaises(RuntimeError):
                gate.terminal(json.dumps(dict(original,**{key:'mutated'}),sort_keys=True))
        for raw in [None,json.dumps(original,sort_keys=True,indent=2)]:
            with self.assertRaises(RuntimeError):gate.terminal(raw)

    def test_original_gate_or_reconstruction_only(self):
        record=verified()
        with patch.dict(os.environ,{'RENDER_GIT_COMMIT':CURRENT}):
            gate.tested(record)
            gate.tested(dict(record,git_commit=CURRENT,reused=True))
            for key in record:
                with self.subTest(key=key),self.assertRaises(RuntimeError):
                    gate.tested(dict(record,**{key:None}))
            for item in [dict(record,reconstruct_only=True),dict(record,tests_passed=1),
                         dict(record,git_commit=CURRENT),dict(record,tests_discovered=820)]:
                with self.assertRaises(RuntimeError):gate.tested(item)

    def test_new_guard_cannot_read_protected_controls(self):
        code=(ROOT/'app/eq17_timer_contingency.py').read_text()
        node=ast.parse(code)
        queries=[c.args[0].value for c in ast.walk(node) if isinstance(c,ast.Call)
                 and getattr(c.func,'attr',None)=='execute']
        self.assertEqual(queries,[
            'SELECT payload FROM live_eq17.state WHERE key=%s',
            "SELECT octet_length(payload), encode(sha256(convert_to(payload,'UTF8')),'hex') FROM live_eq17.deployment_artifacts WHERE hash=%s"])
        self.assertNotIn('live:paused',code)
        self.assertNotIn('qualification',code)
        self.assertNotIn('group_alerts',code)

    def test_guard_is_read_only_and_requires_publication(self):
        raw=json.dumps(terminal_record(),sort_keys=True)
        conn=Mock()
        def rows(published):
            return [Mock(fetchone=lambda:(raw,)),Mock(fetchone=lambda:published)]
        with patch.dict(os.environ,{'RENDER_SERVICE_ID':gate.SERVICE,'RENDER_GIT_COMMIT':CURRENT}):
            conn.execute.side_effect=rows((5744264,gate.TIMER))
            gate.check(conn,gate.TIMER,verified())
            for c in conn.execute.call_args_list:self.assertTrue(c.args[0].startswith('SELECT '))
            for published in [None,(0,gate.TIMER),(5744264,'0'*64)]:
                conn.execute.side_effect=rows(published)
                with self.assertRaisesRegex(RuntimeError,'PUBLISHED_BYTES'):gate.check(conn,gate.TIMER,verified())
            with patch.dict(os.environ,{'RENDER_GIT_COMMIT':gate.ORIGINAL_GIT}),self.assertRaisesRegex(RuntimeError,'DISTINCT_PUBLIC_REF'):
                gate.check(conn,gate.TIMER,verified())

    def test_predeploy_retains_fence_integrity_and_only_reconstructs(self):
        conn=Mock();conn.execute.return_value.fetchone.return_value=('candidate',)
        prior=verified();writes=[]
        options=dict(os=os,uuid=__import__('uuid'),time=__import__('time'),json=json,traceback=__import__('traceback'),
            SERVICE=gate.SERVICE,EXPECTED=gate.TIMER,BUILDER=gate.BUILDER,OP='fixture',
            PREFIX='engineering:timer:',CANDIDATE_KEY='engineering:timer:recipe',PROTECTED=['live:paused'],
            connection=lambda:nullcontext(conn),timer_return_check=Mock(),state=lambda *a:True,
            inputs=lambda c:('base','source'),candidate_proof=lambda c:prior,
            put=lambda c,k,v:writes.append((k,dict(v))),sha=lambda _:gate.TIMER,
            isolated_build=Mock(return_value=('candidate',{'release':gate.TIMER})))
        predeploy=function('eq17_gate_revision.py','predeploy',options)
        with patch.dict(os.environ,{'RENDER_SERVICE_ID':gate.SERVICE,'RENDER_GIT_COMMIT':CURRENT}):
            predeploy()
            options['isolated_build'].assert_called_once_with('base','source',reconstruct=True)
            self.assertTrue(any(v.get('reused') is True and v.get('controls_preserved') is True for _,v in writes))
            self.assertTrue(all(k!=gate.TERMINAL_KEY for k,_ in writes))
            self.assertTrue(any('build_lease' in str(c.args) and 'RETURNING key' in c.args[0] for c in conn.execute.call_args_list))
            self.assertTrue(any('FROM live_eq17.deployment_artifacts' in c.args[0] for c in conn.execute.call_args_list))
            self.assertFalse(any('INSERT INTO live_eq17.deployment_artifacts' in c.args[0] for c in conn.execute.call_args_list))
            options['state']=Mock(side_effect=[True,False])
            with self.assertRaisesRegex(RuntimeError,'BLOCKED'):predeploy()
            options['isolated_build'].reset_mock()
            options['state']=lambda *args:False
            with self.assertRaisesRegex(RuntimeError,'BLOCKED'):predeploy()
            options['isolated_build'].assert_not_called()
            options['state']=lambda *args:True
            prior.clear()
            with self.assertRaisesRegex(RuntimeError,'BLOCKED'):predeploy()
            options['isolated_build'].assert_not_called()

    def test_bootstrap_fails_before_children_and_never_executes_verification(self):
        for valid in (False,True):
            with self.subTest(valid=valid),tempfile.TemporaryDirectory() as tmp,patch.dict(os.environ,{'DATABASE_URL':'fixture-only'}):
                threads=[];c=Mock();c.execute.return_value.fetchone.return_value=('candidate',)
                py=Mock(connect=lambda *a,**k:nullcontext(c))
                def thread(target,args=(),daemon=True):
                    threads.append((target,args));return Mock()
                opt=dict(resolve_release=lambda:gate.TIMER,psycopg=py,_connection_info=lambda x:x,os=os,
                    json=json,pathlib=__import__('pathlib'),tempfile=Mock(mkdtemp=lambda **kw:tmp),
                    validate_release=lambda *a:{'files':{'manifest.json':'{}'},'manifest_hash':hashlib.sha256(b'{}').hexdigest()},
                    hashlib=hashlib,threading=Mock(Thread=thread),diagnostic_flush_loop=Mock(),supervise=Mock(),
                    timer_return_check=Mock(side_effect=None if valid else RuntimeError('stop')),
                    ReleaseIntegrityError=ValueError,boot_done=Mock())
                fn=function('eq17_bootstrap.py','start',opt)
                fn()
                if valid:
                    self.assertEqual([args[0] for _,args in threads if args],['server.py','runtime.py'])
                    self.assertEqual(len(threads),3)
                    self.assertFalse((Path(tmp)/'verification.done').exists())
                    for target,args in threads:
                        if args:self.assertEqual(args[2]['EQ17_VERIFY_DONE'],str(Path(tmp)/'verification.done'))
                else:self.assertEqual(threads,[])
                self.assertTrue(all(c.args[0].startswith('SELECT') for c in c.execute.call_args_list))
                opt['boot_done'].set.assert_called_once()
