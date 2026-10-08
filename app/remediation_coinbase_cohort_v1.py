"""Bounded Coinbase HTTP concurrency with one serial database coordinator.

A generation is explicitly seeded/authorized by the coordinator. This module
never seeds tasks, rolls to another generation, resets attempts or mutates guards.
"""
import base64,concurrent.futures,gzip,hashlib,io,json,re,socket,threading,time
from urllib.request import Request,build_opener
from urllib.error import HTTPError,URLError
VERSION='coinbase_finite_fetch_cohort_20261008_v1'
MAX_HTTP_SLOTS=8
MAX_NATIVE_RESPONSE_BYTES=1024*1024

class SerializedRpc:
 def __init__(self,rpc):self.inner=rpc;self.mode=rpc.mode;self.lock=threading.RLock()
 def call(self,*args,**kwargs):
  with self.lock:return self.inner.call(*args,**kwargs)

class SharedRate:
 def __init__(self,stop,clock=time.monotonic):self.stop=stop;self.clock=clock;self.lock=threading.Lock();self.last=None;self.not_before=0
 def acquire(self,spacing):
  with self.lock:
   remaining=max(0,self.not_before-self.clock(),0 if self.last is None else spacing-(self.clock()-self.last))
   if remaining>0 and self.stop.wait(remaining):return False
   if self.stop.is_set():return False
   self.last=self.clock();return True
 def defer(self,seconds):
  with self.lock:self.not_before=max(self.not_before,self.clock()+seconds)

def create_worker(base,rpc,environ=None):
 class CoinbaseCohortWorker(base.Worker):
  def __init__(self,rpc,environ=None):
   super().__init__(SerializedRpc(rpc),environ)
   self.generation=self.env.get('MARKET_DATA_REMEDIATION_COHORT_GENERATION','')
   self.manifest_sha=self.env.get('MARKET_DATA_REMEDIATION_COHORT_MANIFEST_SHA256','')
   if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',self.generation) or not re.fullmatch(r'[a-f0-9]{64}',self.manifest_sha):raise base.WorkerFault('source_cohort_exact_generation_required')
   self.http_slots=min(8,max(1,int(self.env.get('MARKET_DATA_REMEDIATION_COHORT_CONCURRENCY','8'))))
   self.rate=SharedRate(self.stop);self.counter_lock=threading.Lock();self.rps=2.5
  def preflight(self,batch):
   req=batch.get('request_json') or {}
   if batch.get('provider')!='coinbase' or batch.get('source_type')!='coinbase_candles' or batch.get('interval_seconds')!=60 or req.get('cohort_generation_id')!=self.generation or req.get('cohort_manifest_sha256')!=self.manifest_sha or req.get('required_cohort_version')!=VERSION:raise base.WorkerFault('source_cohort_task_scope_mismatch')
   module,requests=super().preflight(batch)
   if len(requests)!=1 or requests[0]['role']!='candles':raise base.WorkerFault('source_cohort_one_native_request_required')
   return module,requests
  def retained_source(self,batch,request):
   artifact=batch.get('resume_source')
   if artifact is None:return None
   try:
    if artifact['source_url']!=request['url'] or artifact['role']!='primary' or artifact['http_status']!=200 or artifact['parser_version']!=self.source_module(batch).VERSION or artifact['credential_redactions']!=0 or artifact.get('provenance',{}).get('response_body_complete') is False:raise ValueError('contract')
    packed=base64.b64decode(artifact['compressed_base64'])
    if len(packed)>65536 or hashlib.sha256(packed).hexdigest()!=artifact['compressed_sha256']:raise ValueError('gzip hash')
    with gzip.GzipFile(fileobj=io.BytesIO(packed))as f:raw=f.read(MAX_NATIVE_RESPONSE_BYTES+1)
    if len(raw)>MAX_NATIVE_RESPONSE_BYTES or len(raw)!=artifact['original_bytes'] or hashlib.sha256(raw).hexdigest()!=artifact['stored_body_sha256'] or artifact['stored_body_sha256']!=artifact['source_sha256']:raise ValueError('source hash')
    return raw,artifact
   except (KeyError,ValueError,TypeError,OSError,EOFError):raise base.WorkerFault('source_cohort_retained_payload_integrity_rejected',source_invalid=True)
  def fetch_native(self,batch,request):
   # HTTP threads never make database calls or mutate the queue.
   base.validate_url('coinbase',request['url'])
   spacing=max(1/self.rps,(batch.get('request_json')or{}).get('minimum_source_request_spacing_seconds',0))
   if not self.rate.acquire(spacing):raise base.WorkerFault('worker_stopping',retryable=True)
   with self.counter_lock:
    if self.request_count>=self.max_requests:raise base.WorkerFault('process_request_ceiling_reached',retryable=True)
    self.request_count+=1
   begin=time.monotonic()
   request_obj=Request(request['url'],headers={'User-Agent':base.VERSION,'Accept-Encoding':'identity'},method='GET')
   try:
    try:response=build_opener(base.NoRedirect).open(request_obj,timeout=30)
    except HTTPError as error:response=error
    with response:body=response.read(MAX_NATIVE_RESPONSE_BYTES+1);status=response.status;headers=dict(response.headers)
   except (URLError,TimeoutError,socket.timeout):raise base.WorkerFault('source_transport_error',retryable=True)
   if status==429:
    retry=headers.get('Retry-After',headers.get('retry-after','60'))
    self.rate.defer(min(3600,max(1,float(retry))) if str(retry).replace('.','',1).isdigit() else 60)
   artifact=base.source_artifact(request['url'],status,headers,body,'primary' if status==200 else 'error',self.source_module(batch).VERSION,self.secrets)
   artifact['provenance'].update({'attempt':batch.get('attempts'),'cohort_version':VERSION,'cohort_generation_id':self.generation,'manifest_sha256':self.manifest_sha,'http_elapsed_seconds':round(time.monotonic()-begin,6),'database_commit_mode':'ONE_SERIAL_COORDINATOR','response_body_complete':len(body)<=MAX_NATIVE_RESPONSE_BYTES})
   return body,artifact
  def run_group(self,batches):
   pending={b['batch_id']:b for b in batches};pending_lock=threading.Lock();heart_stop=threading.Event();lost=set()
   def heartbeat():
    while not heart_stop.wait(20):
     with pending_lock:items=list(pending.values())
     for batch in items:
      try:
       result=self.rpc.call('heartbeat',self.identity(batch))
       if not result.get('renewed'):
        with pending_lock:lost.add(batch['batch_id'])
      except base.WorkerFault:
       with pending_lock:lost.add(batch['batch_id'])
   thread=threading.Thread(target=heartbeat,name='coinbase-cohort-leases',daemon=True);thread.start()
   start=time.monotonic();futures={};preflights={}
   try:
    with concurrent.futures.ThreadPoolExecutor(max_workers=self.http_slots,thread_name_prefix='coinbase-native-fetch') as executor:
     for batch in batches:
      try:
       module,requests=self.preflight(batch);preflights[batch['batch_id']]=(module,requests)
       retained=self.retained_source(batch,requests[0])
       if retained is None:future=executor.submit(self.fetch_native,batch,requests[0])
       else:
        future=concurrent.futures.Future();future.set_result(retained)
        base.emit('cohort_retained_source_resume',batch_id=batch['batch_id'],generation_id=self.generation,source_id=retained[1]['source_id'])
       futures[future]=batch
      except base.WorkerFault as error:
       future=concurrent.futures.Future();future.set_exception(error);futures[future]=batch
     for future in concurrent.futures.as_completed(futures):
      batch=futures[future];bid=batch['batch_id']
      try:result=future.result()
      except base.WorkerFault as error:result=error
      except Exception:result=base.WorkerFault('source_cohort_unexpected_fetch_error',retryable=False)
      with pending_lock:
       was_lost=bid in lost;pending.pop(bid,None)
      if was_lost:result=base.WorkerFault('worker_lease_or_stop',retryable=True)
      module,requests=preflights.get(bid,(None,[]))
      # Existing process handles HTTP status, raw-before-rows, parsing, finite
      # chunks, final reconciliation and failure history on this single thread.
      begin=time.monotonic();self.process(batch,prefetched={'module':module,'requests':requests,'result':result})
      base.emit('cohort_serial_commit_finished',batch_id=bid,generation_id=self.generation,elapsed_seconds=round(time.monotonic()-begin,6))
   finally:
    heart_stop.set();thread.join(timeout=2)
   base.emit('cohort_group_drained',generation_id=self.generation,group_tasks=len(batches),wall_seconds=round(time.monotonic()-start,6),request_count=self.request_count,batch_count=self.batch_count)
  def run(self):
   idle_report=0
   while not self.stop.is_set():
    slots=min(self.http_slots,self.max_batches-self.batch_count,self.max_requests-self.request_count)
    if slots<=0 or self.ceiling_reached:
     if time.monotonic()-idle_report>60:base.emit('bounded_cohort_process_idle',generation_id=self.generation,batch_count=self.batch_count,request_count=self.request_count);idle_report=time.monotonic()
     self.stop.wait(30);continue
    try:
     claim=self.rpc.call('claim',{'run_id':base.RUN_ID,'worker_id':self.worker_id,'source_capabilities':{**base.SOURCE_CAPABILITIES,'coinbase_fetch_cohort':VERSION},'coinbase_cohort':True,'cohort_generation_id':self.generation,'cohort_manifest_sha256':self.manifest_sha,'max_cohort_tasks':slots})
     if claim.get('status')!='cohort_claimed':
      if time.monotonic()-idle_report>60:base.emit('cohort_queue_idle',generation_id=self.generation,reason=claim.get('reason','no_claim'),batch_count=self.batch_count,request_count=self.request_count);idle_report=time.monotonic()
      self.stop.wait(30);continue
     batches=claim.get('batches',[])
     if claim.get('generation_id')!=self.generation or claim.get('manifest_sha256')!=self.manifest_sha or not isinstance(batches,list) or not 1<=len(batches)<=slots or len({b['batch_id']for b in batches})!=len(batches):raise base.WorkerFault('source_cohort_claim_response_invalid')
     self.rps=float(claim['rate_limit_rps'])
     if not 0<self.rps<=2.5:raise base.WorkerFault('source_cohort_rate_limit_invalid')
     self.batch_count+=len(batches);self.run_group(batches)
    except base.WorkerFault as error:base.emit('cohort_worker_waiting',code=error.code,generation_id=self.generation);self.stop.wait(30)
 return CoinbaseCohortWorker(rpc,environ)
