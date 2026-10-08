import copy,json,hashlib
from pathlib import Path
from app import remediation_sources_fomc_futures_v1 as f
from app import remediation_worker_v1 as w
from app import remediation_options_selftest_v1 as ot
events=[]
w.emit=lambda event,**kw:events.append((event,kw))
class RPC:
 def __init__(self): self.calls=[]
 def call(self,name,body):
  self.calls.append((name,body))
  return {"renewed":True} if name=="heartbeat" else {"status":"ok"}
env={"MASSIVE_API_KEY":"test-fixture-not-a-real-key","MARKET_DATA_REMEDIATION_FOMC_PROBE_VERSION":f.VERSION,
     "MARKET_DATA_REMEDIATION_MAX_REQUESTS":"999","MARKET_DATA_REMEDIATION_MAX_BATCHES":"999"}
rpc=RPC();worker=w.Worker(rpc,env)
assert worker.max_requests==2 and worker.max_batches==2
key=next(iter(f.SPECS));s=f.SPECS[key]
task={"batch_id":-1,"lease_token":"fixture","run_id":w.RUN_ID,"batch_key":key,"provider":"massive",
"source_type":"massive_candles","symbol":s[0],"interval_seconds":60,"start_ts":s[1],"end_ts":s[2],"attempts":1,
"request_json":{"probe_id":f.PROBE_ID,"required_futures_version":f.VERSION,"required_parser_version":f.VERSION,
"provider_calls_cap":1,"max_attempts":1,"price_status":"INCLUDED_NO_INCREMENTAL_CHARGE",
"maximum_incremental_cost_usd":0,"price_evidence":f.DOCS}}
assert worker.preflight(task)[0] is f
normal=w.Worker(RPC(),{"MASSIVE_API_KEY":"test-fixture-not-a-real-key"})
try: normal.preflight(task)
except w.WorkerFault as e: assert e.code=="finite_fomc_mode_task_mismatch"
else: raise AssertionError("disabled_fomc_fetch_admitted")
fx={**task,"batch_key":"ordinary-fx","symbol":"C:EURUSD","request_json":{
"required_parser_version":w.massive_compat.VERSION,"price_status":"INCLUDED_NO_INCREMENTAL_CHARGE","price_evidence":f.DOCS}}
assert normal.preflight(fx)[0] is w.massive_compat
try: worker.preflight(fx)
except w.WorkerFault as e: assert e.code=="finite_fomc_mode_task_mismatch"
else: raise AssertionError("fomc_mode_other_task_admitted")
def fail_fetch(*args): raise w.WorkerFault("fixture_transport",retryable=True)
worker.fetch=fail_fetch
worker.process(task)
failure=[p for n,p in rpc.calls if n=="fail"]
assert len(failure)==1 and failure[0]["retryable"] is False
assert w.SOURCE_CAPABILITIES["massive_candles"]==w.massive_compat.VERSION
assert w.SOURCE_CAPABILITIES["massive_fomc_futures"]==f.VERSION
assert ot.run_tests(w)
parser=f.self_test()
res={"parser":parser,"worker_integration_cases":7,"existing_options_runtime_suite_passed":True,
"network_calls":0,"source_requests":0,"module_sha256":hashlib.sha256(Path("app/remediation_sources_fomc_futures_v1.py").read_bytes()).hexdigest(),
"worker_sha256":hashlib.sha256(Path("app/remediation_worker_v1.py").read_bytes()).hexdigest()}
Path("validation.json").write_text(json.dumps(res,indent=2))
print(json.dumps(res))

