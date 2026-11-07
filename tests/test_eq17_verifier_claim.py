import json
import re
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from app.eq17_verifier_claim import Attempt,PREFIX,PREDECESSORS
R='1'*64
G='2'*40

class Connection:
    """Execute real unique-insert and token CAS in isolated SQLite; translate only PostgreSQL projection syntax."""
    def __init__(self,file):
        self.db=sqlite3.connect(file,timeout=2,isolation_level=None);self.calls=[]
        self.db.create_function('regexp',2,lambda pattern,s:bool(re.fullmatch(pattern,s or '')))
        self.db.execute('CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,payload TEXT NOT NULL)')
    def execute(self,sql,params):
        self.calls.append((sql,params))
        sql=sql.replace('live_eq17.state','state').replace('%s','?').replace("payload::jsonb->>'","payload->>'").replace("payload::jsonb->'attempt'='1'::jsonb","payload->>'attempt'=1").replace(" ~ "," REGEXP ")
        return self.db.execute(sql,params)
    def close(self):self.db.close()

class OneShot(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);self.file=str(Path(t.name)/'test.sqlite3')
        self.c=Connection(self.file);self.addCleanup(self.c.close)
        for old in PREDECESSORS:self.c.db.execute('insert into state values (?,?)',(PREFIX+old,'SENTINEL'))
    def attempt(self):return Attempt(R,G,R)
    def test_only_new_key_and_one_insert_one_terminal_write(self):
        a=self.attempt();self.assertEqual(a.claim(self.c),'claimed');self.assertEqual(len(a.nonce),32)
        initial=a.current
        a.record(self.c,dict(status='running',attempt=1));self.assertEqual(a.current,initial)
        final=a.record(self.c,dict(status='blocked',reason='EXECUTED_CHECK_FAILED',returncode=1,attempt=1))
        self.assertEqual(final['attempt_nonce'],a.nonce);self.assertTrue(a.finished)
        with self.assertRaises(RuntimeError):a.record(self.c,dict(status='complete',attempt=1,returncode=0))
        self.assertEqual(self.attempt().claim(self.c),'terminal')
        for _,params in self.c.calls:
            self.assertEqual([x for x in params if isinstance(x,str) and x.startswith(PREFIX)],[PREFIX+R])
        self.assertEqual(sum(s.startswith('UPDATE') for s,_ in self.c.calls),1)
        for old in PREDECESSORS:self.assertEqual(self.c.db.execute('select payload from state where key=?',(PREFIX+old,)).fetchone(),('SENTINEL',))
    def test_restarts_cannot_steal_unknown_or_active_claim(self):
        a=self.attempt();self.assertEqual(a.claim(self.c),'claimed');self.assertEqual(self.attempt().claim(self.c),'held')
        self.c.db.execute('UPDATE state SET payload=? WHERE key=?',('{"status":"complete","attempt":1}',a.key))
        self.assertEqual(self.attempt().claim(self.c),'held')
        with self.assertRaises(RuntimeError):a.record(self.c,dict(status='complete',attempt=1,returncode=0))
        self.assertFalse(a.finished)
    def test_simultaneous_workers_only_one_launch(self):
        barrier=threading.Barrier(4);states=[];threads=[]
        def worker():
            c=Connection(self.file)
            try:barrier.wait(2);states.append(self.attempt().claim(c))
            finally:c.close()
        for i in range(4):
            t=threading.Thread(target=worker);threads.append(t);t.start()
        for t in threads:t.join(4);self.assertFalse(t.is_alive())
        self.assertCountEqual(states,['claimed','held','held','held'])
    def test_invalid_identifiers_and_forged_success(self):
        for old in PREDECESSORS:
            with self.assertRaises(ValueError):Attempt(old,G,old)
        for x in ((R,'bad',R),(R,G,'3'*64)):
            with self.assertRaises(ValueError):Attempt(*x)
        a=self.attempt();self.assertEqual(a.claim(self.c),'claimed')
        with self.assertRaises(RuntimeError):a.record(self.c,dict(status='complete',attempt=1,returncode=1))
        self.assertEqual(self.attempt().claim(self.c),'held')
    def test_unknown_terminal_never_becomes_retry(self):
        a=self.attempt();self.assertEqual(a.claim(self.c),'claimed')
        b=self.attempt();self.assertEqual(b.claim(self.c),'held')
        with self.assertRaises(RuntimeError):b.record(self.c,dict(status='complete',attempt=1,returncode=0))
        a.record(self.c,dict(status='blocked',reason='VERIFIER_TIMEOUT',attempt=1))
        self.assertEqual(self.attempt().claim(self.c),'terminal')

class PhaseAttribution(unittest.TestCase):
    def test_phase_is_tied_to_nonce_class_source_and_line(self):
        import subprocess
        from app.eq17_verification_runner import verify
        from app.eq17_verifier_recovery import HTTP_TIMEOUT_PHASES
        body=next(k for k,v in HTTP_TIMEOUT_PHASES.items() if v=='READ_RESPONSE_BODY')
        for line,idx,error,auth,expected in (
            (body-1,1,'TimeoutError',True,'OPEN_OR_RESPONSE_HEADERS'),(body,1,'TimeoutError',True,'READ_RESPONSE_BODY'),
            (body,0,'TimeoutError',True,None),(body+1,1,'TimeoutError',True,None),(body,1,'TimeoutError',False,None),
            (body,1,'ValueError',True,None)):
            def run(command,**kwargs):
                challenge=kwargs['env']['EQ17_VERIFIER_CALLER_NONCE'] if auth else 'wrong-nonce'
                output=f'EQ17_VERIFIER_CALLER {challenge} {idx} {line}\n{error}: PRIVATE_EXAMPLE\n'
                return subprocess.CompletedProcess(command,1,stdout=output.encode())
            records=[];self.assertFalse(verify([],'.',{},threading.Event(),records.append,runner=run))
            self.assertEqual(records[-1].get('verifier_timeout_phase'),expected);self.assertNotIn('PRIVATE_EXAMPLE',str(records))
