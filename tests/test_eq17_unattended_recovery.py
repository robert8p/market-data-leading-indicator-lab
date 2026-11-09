"""Offline process and database failure injection, without importing live bootstrap."""
import ast
import pathlib
import sys
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

APP=pathlib.Path(__file__).resolve().parents[1]/'app'
sys.path.insert(0,str(APP))
from eq17_process_monitor import ExitJournal,make_event

def load(name,env):
    nodes=ast.parse((APP/'eq17_bootstrap.py').read_text()).body
    node=next(n for n in nodes if getattr(n,'name',None)==name)
    exec(compile(ast.Module([node],[]),str(APP/'eq17_bootstrap.py'),'exec'),env)
    return env[name]

class RecoveryTests(unittest.TestCase):
    def test_hung_insert_never_blocks_admission_of_next_exit(self):
        entered=threading.Event();release=threading.Event()
        class Connection:
            def __enter__(self):return self
            def __exit__(self,*args):return False
            def execute(self,sql,*args):
                if sql.startswith('INSERT'):entered.set();release.wait(2)
        journal=ExitJournal(Connection)
        first,second=[make_event('runtime.py',-9,'UNCLASSIFIED','a'*64,n,1) for n in (1,2)]
        journal.record(first)
        t=threading.Thread(target=journal.flush);t.start()
        try:
            self.assertTrue(entered.wait(1))
            done=threading.Event()
            def submit():journal.record(second);done.set()
            s=threading.Thread(target=submit);s.start()
            self.assertTrue(done.wait(.3));s.join()
        finally:release.set();t.join()
        self.assertEqual(list(journal.pending),[second])
        self.assertEqual(journal.flush(),1)
    def test_twelfth_child_starts_after_eleven_crashes_and_oom(self):
        attempts=[];sleeps=[]
        class Stop:
            def is_set(self):return len(attempts)>=12
            def wait(self,seconds):sleeps.append(seconds)
        def launch(*a,**k):
            attempts.append(1);return Mock()
        env=dict(time=SimpleNamespace(monotonic=lambda:sum(sleeps)),shutdown=Stop(),subprocess=SimpleNamespace(Popen=launch,DEVNULL=-1,PIPE=-2),
                 os=SimpleNamespace(sys=sys),child_lock=threading.Lock(),children={},child_started={},
                 inspect_child=lambda p:(-9,'UNCLASSIFIED'),child_journal=Mock(),PIN='b'*64,make_event=make_event,print=Mock())
        load('supervise',env)('runtime.py',APP,{})
        self.assertEqual(len(attempts),12);self.assertTrue(all(0<s<=60 for s in sleeps));self.assertLess(sum(sleeps),600)
    def test_real_sigkill_replaces_process_and_resumes_from_filesystem_marker(self):
        import subprocess,tempfile,time,os
        from eq17_process_monitor import inspect_child
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);(root/'src').mkdir()
            (root/'src'/'runtime.py').write_text("import os,signal,pathlib\np=pathlib.Path('prior')\nif not p.exists():\n p.write_text(str(os.getpid()));os.kill(os.getpid(),signal.SIGKILL)\npathlib.Path('recovered').write_text(str(os.getpid()))\n")
            stop=threading.Event();journal=ExitJournal(lambda:None)
            def inspect(p):
                outcome=inspect_child(p)
                if (root/'recovered').is_file():stop.set()
                return outcome
            env=dict(time=time,shutdown=stop,subprocess=subprocess,os=os,child_lock=threading.Lock(),children={},child_started={},
                     inspect_child=inspect,child_journal=journal,PIN='b'*64,make_event=make_event,print=Mock())
            th=threading.Thread(target=load('supervise',env),args=('runtime.py',root,os.environ.copy()))
            th.start()
            try:
                self.assertTrue(stop.wait(12));self.assertEqual(len(journal.pending),1)
                self.assertIn('"exit_status":-9',journal.pending[0][4])
                self.assertNotEqual((root/'prior').read_text(),(root/'recovered').read_text())
            finally:stop.set();th.join(3)
            self.assertFalse(th.is_alive())
    def test_health_requires_durable_progress_as_well_as_lease(self):
        conn=Mock();conn.__enter__=Mock(return_value=conn);conn.__exit__=Mock(return_value=False)
        conn.execute.return_value.fetchone.return_value=[False]
        env=dict(psycopg=SimpleNamespace(connect=lambda *a,**k:conn),_connection_info=lambda u:u,
                 os=SimpleNamespace(environ={'DATABASE_URL':'fixture'}),boot_started_wall=100,PIN='b'*64)
        self.assertFalse(load('_active_owner_evidence',env)())
        self.assertEqual(conn.execute.call_args_list[0].args[0],"SET LOCAL statement_timeout TO '2s'")
        sql=conn.execute.call_args_list[-1].args[0]
        self.assertIn("o.payload::jsonb->>'last_progress'",sql);self.assertIn('now())-600',sql)
        self.assertIn('l.expires >',sql);self.assertIn("e.payload::jsonb->>'release'=%s",sql)

if __name__=='__main__':unittest.main()
