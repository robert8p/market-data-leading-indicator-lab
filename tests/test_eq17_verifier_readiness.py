"""Failure-injection tests of readiness and the real bootstrap verifier closure."""
import ast,contextlib,importlib.util,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('eq17_verifier_readiness',ROOT/'app/eq17_verifier_readiness.py')
gate=importlib.util.module_from_spec(spec);spec.loader.exec_module(gate)
R='a'*64

class ClockStop:
    def __init__(self):self.t=0;self.cancel=False
    def clock(self):return self.t
    def is_set(self):return self.cancel
    def wait(self,seconds):self.t+=seconds

class Connection:
    def __init__(self,fetch):self.fetch=fetch;self.sql=[]
    def __enter__(self):return self
    def __exit__(self,*a):return False
    def execute(self,sql):self.sql.append(sql);return self
    def fetchone(self):return self.fetch()

def row(**changes):
    values=dict(now=1000,owner='new',expires=1100,runtime_owner='new',release=R,state='DEGRADED_EVALUATING',warmup_owner='new',warmup_release=R,finished=999)
    values.update(changes);return tuple(values.values())

class ReadinessTests(unittest.TestCase):
    def test_wrong_owner_release_stale_lease_or_invalid_completion_never_admitted(self):
        for change in ({'owner':'old'},{'release':'b'*64},{'expires':1000},{'warmup_owner':'old'},{'warmup_release':'b'*64},{'finished':1001},{'finished':True},{'finished':None},{'now':float('nan')}):
            self.assertNotIn(gate.readiness(row(**change),R),('READY_MARKET_CLOSED','READY_WARMUP_COMPLETE'))
        self.assertEqual(gate.readiness(row(),R),'READY_WARMUP_COMPLETE')
        self.assertEqual(gate.readiness(row(state='MARKET_CLOSED_DEPENDENCIES_PENDING',finished=None),R),'READY_MARKET_CLOSED')
        self.assertEqual(gate.readiness(row(state='MARKET_CLOSED_DEPENDENCIES_PENDING',release='b'*64),R),'OWNER_NOT_READY')

    def test_measured_slow_warmup_waits_without_any_claim_write(self):
        stop=ClockStop();conn=Connection(lambda:row(finished=None if stop.t<468 else 999))
        result=gate.wait_ready(lambda:conn,R,stop,clock=stop.clock)
        self.assertEqual(result['status'],'ready');self.assertEqual(stop.t,468)
        self.assertFalse(result['claim_written'])
        self.assertTrue(all(sql.startswith(('SELECT','\nSELECT','SET LOCAL')) for sql in conn.sql))

    def test_missing_readiness_is_finite_and_shutdown_prevents_reads(self):
        stop=ClockStop();conn=Connection(lambda:None)
        result=gate.wait_ready(lambda:conn,R,stop,clock=stop.clock)
        self.assertEqual(result['status'],'held');self.assertLessEqual(stop.t,720)
        stop.cancel=True;before=len(conn.sql)
        result=gate.wait_ready(lambda:conn,R,stop,clock=stop.clock)
        self.assertEqual(result['status'],'stopped');self.assertEqual(len(conn.sql),before)

    def test_two_connection_failures_hold_without_exposing_exception(self):
        stop=ClockStop();calls=[]
        def connect():calls.append(1);raise OSError('private-dsn-secret')
        result=gate.wait_ready(connect,R,stop,clock=stop.clock)
        self.assertEqual(result['reason'],'READINESS_READ_UNAVAILABLE');self.assertEqual(len(calls),2)
        self.assertNotIn('secret',str(result))

    def test_ready_response_after_deadline_never_admitted(self):
        stop=ClockStop()
        def fetch():stop.t=721;return row()
        result=gate.wait_ready(lambda:Connection(fetch),R,stop,clock=stop.clock)
        self.assertEqual(result['reason'],'READINESS_DEADLINE')

    def bootstrap_calls(self,status,cancel_at=None):
        source=ast.parse((ROOT/'app/eq17_bootstrap.py').read_text())
        node=next(n for n in ast.walk(source) if isinstance(n,ast.FunctionDef) and n.name=='verify_once')
        calls=[];root=types.SimpleNamespace(__truediv__=lambda *a:None)
        class Root:
            def __truediv__(self,name):return types.SimpleNamespace(touch=lambda:calls.append('done'))
        class Attempt:
            def __init__(self,*args):self.finished=True
            def claim(self,conn):calls.append('claim');return 'terminal'
        stop=ClockStop()
        class Conn:
            def __enter__(self):
                if cancel_at=='connection_entry':stop.cancel=True
                return self
            def __exit__(self,*args):return False
        env=dict(PIN=R,os=types.SimpleNamespace(environ={'RENDER_GIT_COMMIT':'b'*40,'DATABASE_URL':'fixture'}),
                 psycopg=types.SimpleNamespace(connect=lambda *a,**k:Conn()),_connection_info=lambda x:x,
                 shutdown=stop,root=Root(),print=lambda *a,**k:None)
        exec(compile(ast.Module([node],[]),'actual_bootstrap_closure','exec'),env)
        mods={name:types.ModuleType(name) for name in ('app','app.eq17_verification_runner','app.eq17_verifier_recovery','app.eq17_verifier_claim','app.eq17_verifier_readiness')}
        mods['app.eq17_verification_runner'].verify=lambda *a,**k:calls.append('verify')
        mods['app.eq17_verifier_recovery'].RELEASE=R;mods['app.eq17_verifier_recovery'].recovery_script=lambda *a:contextlib.nullcontext('fixture')
        mods['app.eq17_verifier_claim'].Attempt=Attempt
        def wait(*a,**k):
            calls.append('readiness')
            if cancel_at=='ready_response':stop.cancel=True
            return {'status':status,'reason':'FIXTURE'}
        mods['app.eq17_verifier_readiness'].wait_ready=wait
        with patch.dict(sys.modules,mods):env['verify_once']()
        return calls

    def test_actual_bootstrap_claim_occurs_only_after_readiness(self):
        for status,expected in [('held',['readiness']),('stopped',['readiness']),('ready',['readiness','claim','done'])]:
            self.assertEqual(self.bootstrap_calls(status),expected)

    def test_shutdown_after_ready_or_during_connection_entry_never_claims(self):
        for boundary in ('ready_response','connection_entry'):
            with self.subTest(boundary=boundary):
                self.assertEqual(self.bootstrap_calls('ready',boundary),['readiness'])

if __name__=='__main__':unittest.main()
