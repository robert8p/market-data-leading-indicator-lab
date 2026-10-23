"""Credentialless full-session workload. Never a prospective-live assertion."""
import sys,pathlib,time,json,sqlite3,random,hashlib,shutil,statistics,os,threading,tempfile,struct,zlib,argparse
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifact',required=True,type=pathlib.Path)
parser.add_argument('--release',required=True)
parser.add_argument('--output',required=True,type=pathlib.Path)
parser.add_argument('--seed',type=pathlib.Path)
args=parser.parse_args()
if any(os.environ.get(k) for k in ('EQ17_DATABASE_URL','DATABASE_URL','ALPACA_API_KEY','ALPACA_SECRET_KEY','MASSIVE_API_KEY')):
 raise RuntimeError('Run this isolated test without production credentials')
raw=args.artifact.read_bytes()
assert hashlib.sha256(raw).hexdigest()==args.release,'Artifact SHA mismatch'
artifact=json.loads(raw)
workspace=tempfile.TemporaryDirectory(prefix='eq17-full-session-')
root=pathlib.Path(workspace.name)/'release';root.mkdir()
for name,body in artifact['files'].items():
 path=root/name
 assert not pathlib.Path(name).is_absolute() and '..' not in pathlib.Path(name).parts
 path.parent.mkdir(parents=True,exist_ok=True);path.write_text(body)
sys.path[:0]=[str(root/'src'),str(root/'tests')]
# Fail closed if a private dependency unexpectedly attempts external I/O.
import socket
original_connect=socket.socket.connect
def isolated_connect(self,address):
 if isinstance(address,tuple):raise OSError('ISOLATED_ENDURANCE_EXTERNAL_NETWORK_DENIED')
 return original_connect(self,address)
socket.socket.connect=isolated_connect
from test_latency import fixture,OP
from families import ResearchEngine
from runtime import Cache
from positive9 import Observer
from store import Store
from latency_verify import CapacityStore,finish_capacity
from providers import iso
from core import Publication
from families import MACRO
import bls_primary
folder=args.output.resolve();folder.mkdir(parents=True,exist_ok=False)
# Active SQLite/WAL files are transient and must not be mirrored while open.
# Only append-only result logs and the closed evidence database are exported.
cache_folder=pathlib.Path(workspace.name)/'cache';cache_folder.mkdir()
if args.seed:shutil.copyfile(args.seed,cache_folder/'bars.sqlite')
else:
 from core import PreparedBar
 seed=Cache(str(cache_folder/'bars.sqlite'));rng=random.Random(217)
 cutoff=int(OP)+370*60;start=cutoff-43260
 for i in range(6059):
  symbol='S%05d'%i;rows=[];price=10+rng.random()*20
  for j in range(721):
   at=start+j*60
   if at>=cutoff or (i%2 and (j%7!=0 or j==720)) or (not i%2 and j%11==0):continue
   price*=1+rng.uniform(-.0001,.0001);volume=rng.randint(1,10000)
   b=PreparedBar(at,price,price*1.001,price*.999,price,volume,price,at+61)
   numeric=struct.pack('<q7dq',at,b.o,b.h,b.l,b.c,b.v,b.vw,b.received,b.revision)
   blob=struct.pack('<I',zlib.crc32(numeric))+numeric
   rows.append((symbol,at,json.dumps(dict(o=price,h=price*1.001,l=price*.999,c=price,v=volume,vw=price)),b.received,0,blob))
  seed.db.executemany('INSERT INTO bars VALUES (?,?,?,?,?,?)',rows);seed.cover(symbol,start,cutoff)
  if i%100==0:seed.flush()
 seed.flush();seed.db.close()
r=fixture(tuple('S%05d'%i for i in range(6059)));r.cache.db.close();r.cache=Cache(str(cache_folder/'bars.sqlite'))
r.db.close();evidence=Store(str(cache_folder/'evidence.sqlite'));r.db=CapacityStore(evidence,'controlled-full-session')
r.engine=ResearchEngine(evidence,'test');r.identity_primary_enabled=True;r.macro_primary_enabled=True;r.preferred_news_enabled=True;r.bls_alternative_enabled=True
r.positive9=Observer(evidence,scope='capacity_fixture');r.figi_records={};r.figi_ready=False
lease_stop=threading.Event()
def renew_isolated_owner():
 while not lease_stop.wait(10):evidence.lease(r.owner,time.time())
evidence.lease(r.owner,time.time());lease_thread=threading.Thread(target=renew_isolated_owner,daemon=True);lease_thread.start()
first=int(OP)+370*60;r.session=dict(date='CONTROLLED_SYNTHETIC_SESSION',open=first-960,close=first-960+23400)
# Synthetic positive and negative inputs, with clocks entirely inside this
# isolated simulation. Nothing here is a real source receipt or natural event.
for symbol in ('SPY','QQQ','IWM'):
 for minute in range(15):
  at=r.session['open']+minute*60;price=100+minute*.3
  r.cache.add(symbol,dict(t=iso(at),o=price,h=price*1.001,l=price*.999,c=price,v=1000,vw=price),at+61)
 r.cache.cover(symbol,r.session['open'],r.session['open']+900)
# Supply the complete opening support for the dense half; sparse absences stay.
for i,x in enumerate(r.universe):
 if i%2:continue
 for minute in range(15):
  at=r.session['open']+minute*60;price=10+(i%300)*.1+minute*.001
  r.cache.add(x['symbol'],dict(t=iso(at),o=price,h=price*1.001,l=price*.999,c=price,v=1000,vw=price),at+61)
r.cache.flush()
r.news_identities={x['symbol']:dict(symbol=x['symbol'],security=x['security'],valid_from=first-50000,identity_received=first-50000,last_observed=first,method='SYNTHETIC_FIXTURE') for x in r.universe}
r.news_publications=[Publication('synthetic-news-'+str(i),'SYNTHETIC_FIXTURE','NEWS',first-120,first-119,x['security'],True) for i,x in enumerate(r.universe) if i%10==0]
macro_families=set().union(*MACRO.values())
r.publications={family:Publication('synthetic-macro-'+family,'SYNTHETIC_FIXTURE',family,first-600,first-599) for family in macro_families if family not in ('CPI','EMPLOYMENT','JOLTS')}
r.bls_publications={family:Publication('synthetic-alternative-'+family,'SYNTHETIC_FIXTURE',family,first-600,first-599) for family in ('CPI','EMPLOYMENT','JOLTS')}
rng=random.Random(217);runs=[];queue=0;recovered=None;wall_started=time.time();cpu_started=time.process_time()
code={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in ['src/history_cache.py','src/runtime.py','src/latency_verify.py','src/core.py']}
for index in range(359):
 if time.time()-wall_started>7200:raise TimeoutError('Isolated endurance two-hour bound')
 t=first+index*60;evidence.lease(r.owner,time.time());began=time.monotonic();cpu=time.process_time()
 r.corporate_status=dict(available=True,received=t+1)
 coverage=dict(start=t-43260,end=t,received=t+1,complete=True)
 r.news_coverage=dict(coverage,ready=True)
 r.macro_status={family:dict(ready=True,received=t+1,coverage=coverage) for family in macro_families}
 r.bls_status=dict(ready=True,received=t+1,coverage=coverage,primary_contract_valid=True,primary_policy_hash=bls_primary.POLICY_HASH)
 for identity in r.news_identities.values():identity['last_observed']=t
 for i,x in enumerate(r.universe):
  # Half liquid each minute; half sparse, with original absence represented.
  if i%2 and (index+i)%7:continue
  price=10+(i%300)*.1+(index%19)*.001;v=100+rng.randrange(10000)
  r.cache.add(x['symbol'],dict(t=iso(t-60),o=price,h=price*1.001,l=price*.999,c=price,v=v,vw=price),t+1)
 for x in r.universe:r.cache.cover(x['symbol'],first-43260,t)
 r.cache.flush();ingested=time.monotonic()
 ctx=r.evaluation_context(t,persist_signals=False,capacity_replay=True,fixed_evaluation_time=t+2)
 ctx['source_snapshot']=r.source_snapshot(t);ctx['source_snapshot']['captured_at']=t+2
 ctx['activation_acquisition_complete']=True
 r.evaluate_batch(t,r.universe,ctx);evaluated=time.monotonic();finish_capacity(r,t,ctx);r.cache.flush();finished=time.monotonic()
 assert len(ctx['ages'])==6059 and len(ctx['rule_counts'])==90
 assert sum(ctx['input_complete'].values())>0,'Fixture did not exercise admissible inputs'
 assert sum(v['false']+v['true'] for v in ctx['rule_counts'].values())>0,'All rule results unavailable'
 assert all(sum(v.values())==6059 for v in ctx['rule_counts'].values())
 assert ctx.get('activation_publication_measured') is True
 assert not {k:v for k,v in ctx.items() if k.endswith('_error') and v}
 assert r.cache.history.size<=24*1024*1024
 service=finished-began;injected=180 if index==179 else 0
 queue=max(0,queue-60)+injected if index else 0
 start_age=queue;completion=queue+service+10;queue=completion
 if index>179 and start_age==0 and recovered is None:recovered=index
 run=dict(index=index,trigger=t,total_seconds=service,cpu_seconds=time.process_time()-cpu,ingestion_seconds=ingested-began,evaluation_seconds=evaluated-ingested,publication_seconds=finished-evaluated,processed_securities=len(ctx['ages']),trigger_bars=ctx['evaluated'],absent_trigger=ctx['without_trigger'],selected_rules=90,rule_counts=ctx['rule_counts'],input_complete=ctx['input_complete'],missing_counts=ctx['missing_counts'],history_bytes=r.cache.history.size,disk_hits=r.cache.history.disk_hits,rebuilds=r.cache.history.rebuilds,modeled_queue_age=start_age,modeled_completion_age=completion,injected_seconds=injected)
 runs.append(run)
 with (folder/'minutes.jsonl').open('a') as out:out.write(json.dumps(run,sort_keys=True)+'\n')
 if index%10==0:print(json.dumps({k:run[k] for k in ('index','total_seconds','evaluation_seconds','publication_seconds','trigger_bars','modeled_queue_age','disk_hits','rebuilds')}),flush=True)
values=sorted(x['total_seconds'] for x in runs)
assert all(any(r['input_complete'][rid]>0 for r in runs) for rid in runs[0]['input_complete']),'Some selected rules never exercised complete inputs'
result=dict(release=args.release,classification='CONTROLLED_SYNTHETIC_FULL_SESSION_NOT_LIVE',started=wall_started,finished=time.time(),cpu_seconds=time.process_time()-cpu_started,code_sha256=code,expected=359,observed=len(runs),missing=0,selected_rules=90,eligible=6059,timing=dict(p50=statistics.median(values),p95=values[int(.95*(len(values)-1))],maximum=max(values),within135=sum(x<=135 for x in values)),queue_model=dict(arrival_seconds=60,extra_network_allowance_seconds=10,injection_seconds=180,injection_index=179,recovered_index=recovered,max_queue=max(x['modeled_queue_age'] for x in runs),last_queue=runs[-1]['modeled_queue_age']),limits='Synthetic bars, benchmark support, receipt-timed news/macro/corporate fixtures; actual all-rule evaluation and publication code against isolated local SQLite. Queue model uses measured service plus explicit 10s network allowance, not measured Render/Postgres/provider latency. Does not establish live input completeness, timely capture, natural macro proof, hosted delivery or production acceptance.',passed=len(runs)==359 and recovered is not None and runs[-1]['modeled_queue_age']==0 and max(values)<60)
(folder/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
lease_stop.set();lease_thread.join();r.cache.db.close();evidence.close()
shutil.copyfile(cache_folder/'evidence.sqlite',folder/'isolated-evidence.sqlite')

workspace.cleanup()
raise SystemExit(0 if result['passed'] else 2)
