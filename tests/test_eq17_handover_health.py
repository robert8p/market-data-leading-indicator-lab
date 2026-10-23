import ast,asyncio,pathlib,threading,unittest
from types import SimpleNamespace
from unittest.mock import Mock

class HandoverHealth(unittest.TestCase):
 def ready(self,age,owner=False,child_alive=True):
  # Load only the readiness function: never import bootstrap's live threads.
  path=pathlib.Path(__file__).parents[1]/'app/eq17_bootstrap.py'
  node=next(n for n in ast.parse(path.read_text()).body if isinstance(n,ast.AsyncFunctionDef) and n.name=='private_ready')
  class Client:
   def __init__(self,**kwargs):pass
   async def __aenter__(self):return self
   async def __aexit__(self,*args):pass
   async def get(self,url):return SimpleNamespace(status_code=200)
  env=dict(boot_done=SimpleNamespace(is_set=lambda:True),boot_error=None,time=SimpleNamespace(monotonic=lambda:10000+age),boot_started=10000,child_lock=threading.Lock(),children={k:SimpleNamespace(poll=lambda:None if child_alive else 1) for k in ('server.py','runtime.py')},child_started={k:9990 for k in ('server.py','runtime.py')},httpx=SimpleNamespace(AsyncClient=Client,RequestError=RuntimeError),owner_probe_lock=threading.Lock(),owner_probe_checked=0,owner_probe_good=0,_active_owner_evidence=Mock(return_value=owner),asyncio=asyncio)
  exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),env)
  return asyncio.run(env['private_ready']()),env['_active_owner_evidence']
 def test_drain_and_lease_handover_does_not_restart_healthy_children(self):
  result,probe=self.ready(195);self.assertTrue(result);probe.assert_not_called()
 def test_grace_expires_at_boundary_and_stale_owner_fails(self):
  self.assertTrue(self.ready(239.999)[0]);self.assertFalse(self.ready(240)[0])
 def test_current_owner_is_required_after_grace(self):
  self.assertTrue(self.ready(241,owner=True)[0]);self.assertFalse(self.ready(241,owner=False)[0])
 def test_dead_child_never_gets_handover_grace(self):
  self.assertFalse(self.ready(30,child_alive=False)[0])
