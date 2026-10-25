Exact47214 / claim48427. Producer delegated-agent:overview200_audit; primary independent reviewer.

New route only: ov4_* private tables/functions and population-top3000-overview-v4 worker. Existing Aug3 worker, resolution source consumer, family adapters, controller tick and scheduler are untouched.

State boundary:
1. Future separately reviewed source admission and ov4_start queues one authenticated worker wake. One private task per finite tranche is BLOCKED for the ordinary adapter dispatcher. A native turn creates a genuine pipeline_attempt and source lease, linked to the actual internal pg_net request id.
2. ov4_wake current guard serializes controls before task/attempt state. One native source call consumes one provider and one HTTP allowance. Each pg_net worker dispatch consumes another HTTP allowance. Finite tranche is <=100 provider calls and <=100 internal dispatches, <=200 total HTTP calls, <=1MiB/response and <=32MiB cumulative source/relations. Shared waits can spend internal dispatch budget and reduce attainable source calls; they cannot silently increase it.
3. Worker saves bounded received body bytes through an independent committed ov4_spool_save RPC before decoding/normalization. That RPC authenticates an immutable issued capability and current plain-snapshot owner/stop/policy/code. It never locks global control, task, attempt or provider lease rows. Later owner/stops reject new saves. Source bytes are FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE.
4. ov4_consume takes global controls, attempt, task then lease, verifies coherence, and commits one private date point plus genuine pipeline_commit. On success, the SAME transaction queues the next exact worker wake through pg_net. Postgres net requests send only after commit. An outer rollback removes queued requests. Failed consume retains an already committed spool body; stop/owner changes prohibit subsequent processing.
5. Actual or uncertain provider outcomes are never retried. Native known no-provider waits alone may queue another finite internal wake through ov4_defer. At tranche exhaustion or external uncertainty, existing executor performs measured review rather than a new controller.

Exact code binding: database bundle digest covers all ov4_* function definitions. Source admission and immutable deployment pin bind the canonical worker content SHA, not a revision label. Canonical worker SHA is computed by replacing only the CANONICAL_WORKER_SHA constant value with WORKER_SHA_PLACEHOLDER before SHA256. Independently read back actual deployed source/version/EZBR and contract.mjs hash before admission. A worker-supplied digest alone is not independent deployment evidence.

Private request manifest remains database-only. ov4_seed verifies exact47214 full details hash and its200-request manifest hash then appends immutable request records. It does not create any source admission or acquire data.

Proof files:
- contract.test.mjs:17 offline mocked/synthetic tests. These do not establish database durability.
- rollback.sql: actual PostgreSQL synthetic rollback,200 private request bindings, real route controls/attempt/task/commit/spool/queued-wake checks. Queue HTTP never commits. Must run inside BEGIN with route.sql then ROLLBACK.
- worker synthetic mode:200 fixtures,0provider/HTTP/database calls; verifies deployed module/request logic only.

A separately committed source spool followed by a genuinely failed later transaction must be observed on first separately admitted source execution (or a separately approved isolated committed fixture). Do not claim this from single-transaction rollback or mocked tests. First source activation must independently verify its first positive retained-body/point commits and automatic next wake; all200 request completion requires actual200 outputs or precisely classified source holds, never only code integration.

No interval, original historical receipt, company membership replacement, full-period release, CTX06 breadth, family acceptance or serving promotion is supplied by this route.


## worker.ts

```typescript
import postgres from 'npm:postgres@3.4.5';
import {boundedInput,boundedBody,validateRequest,validateEnvelope} from './contract.mjs';
// CANONICAL_WORKER_SHA excludes this exact constant's hex characters only.
const CANONICAL_WORKER_SHA='f2bbf487eca6f67fe4e27ea5091f0cee726e7c97e46b67299f776e691558a5e4';
const S='population_top3000_202608_202609_v1';
Deno.serve(async(req:Request)=>{
 if(req.method!=='POST')return Response.json({state:'METHOD_REJECTED'},{status:405});
 let b:any;try{b=await boundedInput(req.body);}catch{return Response.json({state:'INPUT_BOUND_OR_INVALID'},{status:400});}
 // No database/provider call in synthetic mode. Actual deployed endpoint proves module
 // imports and exact historical request logic; SQL rollback proves storage separately.
 if(b.mode==='synthetic'){
  const out=[];for(let n=1;n<=200;n++){const date='2026-08-'+String(1+(n%31)).padStart(2,'0'),ticker='FIXTURE'+n;
   const r={ticker,point_date:date,exact_url:`https://api.massive.com/v3/reference/tickers/${ticker}?date=${date}`,path:`/v3/reference/tickers/${ticker}`,request_state:'ADMITTED',request_manifest_sha256:'13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef',scope_sha256:'8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'};
   validateRequest(r);validateEnvelope({status:'OK',results:{ticker}},r);out.push(n);}
  return Response.json({state:'SYNTHETIC_200_BINDINGS_PASS',cases:out.length,provider_calls:0,source_http:0,worker_sha256:CANONICAL_WORKER_SHA,storage_proven:false});
 }
 if(!Number.isSafeInteger(b.admission_event_id)||typeof b.wake_token!=='string'||b.wake_token.length>100)return Response.json({state:'CAPABILITY_REQUIRED'},{status:403});
 const dbUrl=Deno.env.get('SUPABASE_DB_URL'),apiKey=(Deno.env.get('MASSIVE_API_KEY')||Deno.env.get('MASSIVE_KEY')||Deno.env.get('POLYGON_API_KEY')||Deno.env.get('POLYGON_KEY')||'').trim();
 if(!dbUrl||!apiKey)return Response.json({state:'CONFIGURATION_HOLD'},{status:503});
 const sql=postgres(dbUrl,{max:1,prepare:false,connect_timeout:3,connection:{application_name:'ref01-overview-v4',statement_timeout:'5000',lock_timeout:'750'}});
 const rpc=async(name:string,args:any[])=>{return(await sql.unsafe(`select ${S}.${name}(${args.map((_,i)=>'$'+(i+1)).join(',')}) result`,args))[0].result;};
 try{
  if(!await rpc('ov4_auth',[b.admission_event_id,b.wake_token,CANONICAL_WORKER_SHA]))return Response.json({state:'CAPABILITY_REJECTED'},{status:403});
  // Bounded native turn waits without holding any DB lock. No HTTP is sent on wait.
  await new Promise(r=>setTimeout(r,15000));
  const pre=await rpc('ov4_preflight',[b.admission_event_id]);
  if(pre.state==='BODY_RETAINED')return Response.json(await rpc('ov4_consume',[pre.request_key,b.admission_event_id]));
  if(pre.state!=='ELIGIBLE')return Response.json(pre);
  const c=await rpc('ov4_wake',[b.admission_event_id,b.wake_token,CANONICAL_WORKER_SHA]);if(c.state!=='CLAIMED'){if(['BUSY','QUIET_SHARED_PROVIDER_WAIT'].includes(c.state))return Response.json(await rpc('ov4_defer',[b.admission_event_id,b.wake_token,CANONICAL_WORKER_SHA]));return Response.json(c);}
  let response:Response,bytes:Uint8Array;
  try{response=await fetch(validateRequest(c.request),{headers:{Authorization:'Bearer '+apiKey},redirect:'error',signal:AbortSignal.timeout(20000)});bytes=await boundedBody(response);}catch{return Response.json(await rpc('ov4_unknown',[c.request.request_key,c.token,'NATIVE_RESPONSE_UNKNOWN_NO_RETRY']));}
  const sha=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))).map(x=>x.toString(16).padStart(2,'0')).join('');let raw='';for(const z of bytes)raw+=String.fromCharCode(z);
  // Independent RPC COMMIT precedes every normalization/FINISH/global-control lock.
  await rpc('ov4_spool_save',[c.request.request_key,c.token,JSON.stringify({body_base64:btoa(raw),bytes:bytes.length,sha256:sha,http_status:response.status,physical_http_count:1,representation:'FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE'}),CANONICAL_WORKER_SHA]);
  return Response.json(await rpc('ov4_consume',[c.request.request_key,b.admission_event_id]));
 }catch{return Response.json({state:'HELD_NO_AUTOMATIC_RETRY',body_retention_claimed:false},{status:503});}finally{await sql.end({timeout:1}).catch(()=>{});}
});

```


## contract.mjs

```javascript
export const REVISION='ref01-historical-overview-v4';
export const SCOPE='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd';
export const MANIFEST='13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef';
export const MAX_BYTES=1048576;
export class SourceFault extends Error {constructor(code,uncertain=false){super(code);this.code=code;this.uncertain=uncertain;}}
export function validateRequest(r){
 if(!r||r.scope_sha256!==SCOPE||r.request_manifest_sha256!==MANIFEST||r.request_state!=='ADMITTED')throw new SourceFault('EXACT_SOURCE_ADMISSION_REQUIRED');
 if(typeof r.ticker!=='string'||!r.ticker.match(/^[A-Za-z0-9._-]{1,64}$/)||typeof r.point_date!=='string'||!r.point_date.match(/^2026-(08-(0[1-9]|[12][0-9]|3[01])|09-(0[1-9]|[12][0-9]|30))$/))throw new SourceFault('EXACT_DATED_REQUEST_INVALID');
 const url=new URL('https://api.massive.com/v3/reference/tickers/'+encodeURIComponent(r.ticker));url.searchParams.set('date',r.point_date);
 if(r.exact_url!==url.toString()||r.path!==url.pathname)throw new SourceFault('EXACT_PATH_DATE_BINDING_CHANGED');
 return url;
}
export function validateEnvelope(body,r){
 if(!body||body.status!=='OK'||!body.results||typeof body.results!=='object'||Array.isArray(body.results)||body.results.ticker!==r.ticker)throw new SourceFault('OVERVIEW_SOURCE_IDENTITY_OR_STATUS_HOLD');
 if(body.next_url)throw new SourceFault('UNADMITTED_PAGINATION_HOLD');
 return {ticker:r.ticker,point_date:r.point_date,source_record:body.results,evidence_semantics:'DATE_QUALIFIED_VENDOR_OVERVIEW_POINT_NOT_INTERVAL_V4',original_historical_receipt_proven:false,intervals_qualified:false,family_accepted:false};
}
export async function boundedBody(response){
 if(!response.body)throw new SourceFault('SOURCE_EMPTY_BODY',true);
 const chunks=[];let bytes=0;const reader=response.body.getReader();
 try{while(true){const p=await reader.read();if(p.done)break;bytes+=p.value.length;if(bytes>MAX_BYTES){await reader.cancel();throw new SourceFault('SOURCE_RESPONSE_BOUND',true);}chunks.push(p.value);}}
 catch(e){if(e instanceof SourceFault)throw e;throw new SourceFault('SOURCE_STREAM_UNKNOWN',true);}
 const out=new Uint8Array(bytes);let off=0;for(const p of chunks){out.set(p,off);off+=p.length;}return out;
}
export async function executeOne(db,fetchImpl,key,admission,apiKey){
 // A cheap read-only preflight precedes any provider request or lock-heavy claim.
 const pre=await db.preflight(key,admission);
 if(pre.state==='BODY_RETAINED')return db.consume(key,admission);
 if(pre.state!=='ELIGIBLE')return pre;
 const claim=await db.claim(key,admission);
 if(claim.state!=='CLAIMED')return claim;
 const r=claim.request;let bytes,response;
 try{const url=validateRequest(r);response=await fetchImpl(url,{headers:{Authorization:'Bearer '+apiKey},redirect:'error',signal:AbortSignal.timeout(25000)});bytes=await boundedBody(response);}
 catch(e){await db.markUnknown(key,claim.token,e instanceof SourceFault?e.code:'SOURCE_EXTERNAL_OUTCOME_UNKNOWN');return {state:'UNKNOWN_EXTERNAL_OUTCOME_HELD',automatic_retry:false};}
 const bodySha=await db.hash(bytes);let binary='';for(const b of bytes)binary+=String.fromCharCode(b);
 const bodyBase64=btoa(binary);
 // Preserve acquired bytes before UTF8 decoding, JSON parsing or FINISH.
 await db.save(key,claim.token,{body_base64:bodyBase64,sha256:bodySha,bytes:bytes.length,http_status:response.status,http_date:response.headers.get('date'),request_id:response.headers.get('x-request-id'),worker_revision:REVISION,physical_http_count:1,representation:'FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE'});
 let text;
 try{text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);}catch{return {state:'BODY_RETAINED_UTF8_HOLD',automatic_retry:false};}
 // The durable save above is a separate committed RPC from normalization.
 if(!response.ok)return {state:'HTTP_ERROR_BODY_RETAINED',http_status:response.status,automatic_retry:false,no_listing_inferred:false};
 let body;try{body=JSON.parse(text);validateEnvelope(body,r);}catch{return {state:'BODY_RETAINED_QUALITY_HOLD',automatic_retry:false};}
 // A failing consumer leaves the durable body and never triggers a refetch.
 try{return await db.consume(key,admission);}catch{return {state:'BODY_RETAINED_CONSUMER_HOLD',automatic_retry:false};}
}
export async function boundedInput(body,maxBytes=2048){
 if(!body)throw new SourceFault('INPUT_INVALID');
 const reader=body.getReader(),chunks=[];let count=0;
 while(true){const item=await reader.read();if(item.done)break;count+=item.value.length;if(count>maxBytes){await reader.cancel();throw new SourceFault('INPUT_BOUND');}chunks.push(item.value);}
 const out=new Uint8Array(count);let off=0;for(const c of chunks){out.set(c,off);off+=c.length;}
 try{return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(out));}catch{throw new SourceFault('INPUT_INVALID');}
}

```


## rollback.sql

```sql
-- Run route.sql inside this SAME rollback transaction before this body.
-- net.http_post queue rows are never committed; pg_net sends only after commit.
-- No provider or external worker call is performed by this SQL test.
SET LOCAL statement_timeout='35s';SET LOCAL lock_timeout='1s';
SELECT population_top3000_202608_202609_v1.ov4_seed('f2bbf487eca6f67fe4e27ea5091f0cee726e7c97e46b67299f776e691558a5e4');
CREATE TEMP TABLE ov4_test_result(name text,result jsonb);
DO $t$
DECLARE d jsonb;review bigint;aid bigint;c jsonb;s jsonb;r record;tok text;b text;sha text;rid bigint;x jsonb;before_count int;i int;first_aid bigint;BEGIN
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_request)<>200 THEN RAISE EXCEPTION 'TEST_200_FAILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_request WHERE r->>'exact_url'<>'https://api.massive.com/v3/reference/tickers/'||(r->>'ticker')||'?date='||(r->>'point_date')) THEN RAISE EXCEPTION 'TEST_URL_BINDING_FAILED';END IF;
 INSERT INTO ov4_test_result VALUES('200_exact_private_bindings','{"pass":true}');
 d:=jsonb_build_object('owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','worker_sha256','f2bbf487eca6f67fe4e27ea5091f0cee726e7c97e46b67299f776e691558a5e4','bundle_sha256',population_top3000_202608_202609_v1.ov4_bundle(),'request_manifest_sha256','13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef','input_manifest_sha256','c6a21603b1015a3548de6e41b30ccdeb31c81b566f330e581d9ffc0a138cd57f','not_after',clock_timestamp()+interval '10 minutes','max_provider_calls',100,'max_http_calls',200,'max_source_bytes',33554432,'max_relation_bytes',33554432,'automatic_retry',false,'TEST_ONLY_ROLLBACK',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_REVIEW_V4',jsonb_build_object('approved',true,'producer','synthetic-test-producer','reviewer','synthetic-test-reviewer','proposal_sha256',encode(sha256(convert_to(d::text,'UTF8')),'hex'))) RETURNING event_id INTO review;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4',d||jsonb_build_object('review_event_id',review)) RETURNING event_id INTO aid;
 -- start durably queues exactly one wake; all queue effects roll back at outer boundary.
 x:=population_top3000_202608_202609_v1.ov4_start(aid);IF x->>'state'<>'ASYNC' THEN RAISE EXCEPTION 'TEST_START_FAILED %',x;END IF;
 x:=population_top3000_202608_202609_v1.ov4_start(aid);IF x->>'state'<>'ALREADY_STARTED_NO_DUPLICATE_WAKE' THEN RAISE EXCEPTION 'TEST_START_REPLAY';END IF;
 SELECT wake_token INTO tok FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_wake(aid,'stale',d->>'worker_sha256');RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='NEGATIVE_NOT_REJECTED' THEN RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_wake(aid,tok,repeat('0',64));RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='NEGATIVE_NOT_REJECTED' THEN RAISE;END IF;END;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp()-interval '1 second' WHERE singleton;
 c:=population_top3000_202608_202609_v1.ov4_wake(aid,tok,d->>'worker_sha256');IF c->>'state'<>'CLAIMED' THEN RAISE EXCEPTION 'TEST_NEEDS_QUIET_PROVIDER_SLOT %',c;END IF;
 b:=jsonb_build_object('status','OK','results',jsonb_build_object('ticker',c#>>'{request,ticker}','name','SYNTHETIC_FIXTURE_NOT_PROVIDER_FACT'))::text;sha:=encode(sha256(convert_to(b,'UTF8')),'hex');
 s:=jsonb_build_object('body_base64',encode(convert_to(b,'UTF8'),'base64'),'sha256',sha,'bytes',octet_length(b),'http_status',200,'physical_http_count',1,'representation','FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE');
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}','bad',s::text,d->>'worker_sha256');RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='NEGATIVE_NOT_REJECTED' THEN RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',(s||jsonb_build_object('sha256',repeat('0',64)))::text,d->>'worker_sha256');RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='NEGATIVE_NOT_REJECTED' THEN RAISE;END IF;END;
 x:=population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');
 x:=population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');IF x->>'state'<>'BODY_ALREADY_RETAINED' THEN RAISE EXCEPTION 'TEST_SPOOL_REPLAY_FAILED';END IF;
 -- Immutable source evidence cannot be changed even by normal privileged UPDATE.
 BEGIN UPDATE population_top3000_202608_202609_v1.ov4_spool SET status=404 WHERE k=c#>>'{request,request_key}';RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='NEGATIVE_NOT_REJECTED' THEN RAISE;END IF;END;
 -- Critical failure case: controls stop AFTER byte retention; FINISH rejects but spool survives.
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=true WHERE singleton;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='NEGATIVE_NOT_REJECTED' THEN RAISE;END IF;END;
 BEGIN PERFORM population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';EXCEPTION WHEN OTHERS THEN IF SQLERRM='NEGATIVE_NOT_REJECTED' THEN RAISE;END IF;END;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_spool WHERE k=c#>>'{request,request_key}' AND ov4_spool.sha=s->>'sha256') THEN RAISE EXCEPTION 'TEST_SPOOL_LOST';END IF;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stopped=false WHERE singleton;
 BEGIN
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET lease_until=clock_timestamp()-interval '1 second' WHERE task_id=(SELECT task_id FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid);
 PERFORM population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);RAISE EXCEPTION 'NEGATIVE_NOT_REJECTED';
 EXCEPTION WHEN OTHERS THEN IF SQLERRM='NEGATIVE_NOT_REJECTED' THEN RAISE;END IF;END;
 x:=population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);IF x->>'state'<>'POINT_RETAINED' THEN RAISE EXCEPTION 'TEST_POSITIVE_CONSUME_FAILED %',x;END IF;
 x:=population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);IF x->>'state'<>'POINT_ALREADY_RETAINED' THEN RAISE EXCEPTION 'TEST_POINT_REPLAY_FAILED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_cap c1 JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_commit p USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=a.task_id WHERE c1.k=c#>>'{request,request_key}' AND a.request_id IS NOT NULL AND a.execution_number=t.executions AND a.outcome='COMPLETE' AND p.new_committed_units=1 AND t.useful_committed_units=1) THEN RAISE EXCEPTION 'TEST_ATTEMPT_TASK_COMMIT_BINDING';END IF;
 IF (SELECT dispatches FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid)<>2 THEN RAISE EXCEPTION 'TEST_NEXT_WAKE_NOT_DURABLE_QUEUED';END IF;
 INSERT INTO ov4_test_result VALUES('positive_spool_replay_control_failure_commit_next_wake',jsonb_build_object('pass',true,'points',1,'queued_wakes',2,'provider_calls',0,'external_http',0,'test_reservations_rolled_back',true));
 first_aid:=aid;
 FOR i IN 2..200 LOOP
 IF i=101 THEN
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_REVIEW_V4',jsonb_build_object('approved',true,'producer','synthetic-test-producer','reviewer','synthetic-test-reviewer','proposal_sha256',encode(sha256(convert_to(d::text,'UTF8')),'hex'))) RETURNING event_id INTO review;
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4',d||jsonb_build_object('review_event_id',review)) RETURNING event_id INTO aid;
   x:=population_top3000_202608_202609_v1.ov4_start(aid);
   SELECT wake_token INTO tok FROM population_top3000_202608_202609_v1.ov4_batch WHERE admission=aid;
 END IF;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET next_provider_at=clock_timestamp()-interval '1 second' WHERE singleton;
 c:=population_top3000_202608_202609_v1.ov4_wake(aid,tok,d->>'worker_sha256');IF c->>'state'<>'CLAIMED' THEN RAISE EXCEPTION 'TEST_200_LOOP_CLAIM_FAILED ordinal% %',i,c;END IF;
 b:=jsonb_build_object('status','OK','results',jsonb_build_object('ticker',c#>>'{request,ticker}','name','SYNTHETIC_FIXTURE_NOT_PROVIDER_FACT'))::text;sha:=encode(sha256(convert_to(b,'UTF8')),'hex');
 s:=jsonb_build_object('body_base64',encode(convert_to(b,'UTF8'),'base64'),'sha256',sha,'bytes',octet_length(b),'http_status',200,'physical_http_count',1,'representation','FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE');
 x:=population_top3000_202608_202609_v1.ov4_spool_save(c#>>'{request,request_key}',c->>'token',s::text,d->>'worker_sha256');
 x:=population_top3000_202608_202609_v1.ov4_consume(c#>>'{request,request_key}',aid);IF x->>'state'<>'POINT_RETAINED' THEN RAISE EXCEPTION 'TEST_200_LOOP_CONSUME_FAILED ordinal% %',i,x;END IF;
 IF i=101 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_cap cp JOIN population_top3000_202608_202609_v1.pipeline_attempt at USING(attempt_id) WHERE cp.k=c#>>'{request,request_key}' AND at.execution_number=1 AND at.request_id IS NOT NULL) THEN RAISE EXCEPTION 'TEST_SECOND_TRANCHE_EXECUTION_OR_DISPATCH_BINDING';END IF;
 END LOOP;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_point)<>200 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ov4_spool)<>200 OR (SELECT sum(dispatches) FROM population_top3000_202608_202609_v1.ov4_batch)<>200 THEN RAISE EXCEPTION 'TEST_200_COUNTS_FAILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ov4_point WHERE point->>'intervals_qualified'<>'false' OR point->>'family_accepted'<>'false' OR point->>'original_historical_receipt_proven'<>'false') THEN RAISE EXCEPTION 'TEST_FALSE_QUALIFICATION';END IF;
 INSERT INTO ov4_test_result VALUES('200_points_two_finite_tranches',jsonb_build_object('pass',true,'synthetic_points',200,'genuine_rollback_attempts',200,'genuine_rollback_commits',200,'uncommitted_internal_wakes',200,'second_tranche_first_execution',1,'actual_provider_calls',0,'actual_external_http',0,'source_facts_qualified',false));

END;$t$;
SELECT * FROM ov4_test_result;
-- Caller ROLLBACK mandatory. Separate two-transaction spool durability test remains required.

```


## contract.test.mjs

```javascript
import test from 'node:test';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';
import {SCOPE,MANIFEST,REVISION,MAX_BYTES,validateRequest,validateEnvelope,executeOne,boundedInput} from './contract.mjs';
const make=(ticker='SYNTHETIC',point_date='2026-09-06')=>({ticker,point_date,scope_sha256:SCOPE,request_manifest_sha256:MANIFEST,request_state:'ADMITTED',path:'/v3/reference/tickers/'+encodeURIComponent(ticker),exact_url:'https://api.massive.com/v3/reference/tickers/'+encodeURIComponent(ticker)+'?date='+point_date});
const body=(ticker='SYNTHETIC')=>JSON.stringify({status:'OK',results:{ticker,cik:'0000000001',composite_figi:'SYNTHETIC0001',share_class_figi:'SYNTHETIC0002'}});
function fixture(overrides={}){const log=[],saved=[];const db={preflight:async()=>({state:saved.length?'BODY_RETAINED':'ELIGIBLE'}),claim:async()=>{log.push('claim');return{state:'CLAIMED',token:'synthetic_token',request:make()};},consume:async()=>{log.push('consume');return{state:'POINT_RETAINED'};},markUnknown:async(k,t,e)=>log.push('unknown:'+e),save:async(k,t,r)=>{log.push('save');saved.push(r);},hash:async(b)=>createHash('sha256').update(b).digest('hex'),...overrides};return{db,log,saved};}
const ok=async()=>new Response(body(),{status:200});
test('200 synthetic exact historical request URLs bind dates and path',()=>{for(let i=0;i<200;i++){const date=new Date(Date.UTC(2026,7,1+i%61)).toISOString().slice(0,10);assert.equal(validateRequest(make('SYNTHETIC'+i,date)).searchParams.get('date'),date);}});
test('stale manifest and unadmitted request rejected',()=>{assert.throws(()=>validateRequest({...make(),request_manifest_sha256:'0'.repeat(64)}));assert.throws(()=>validateRequest({...make(),request_state:'PROPOSED'}));});
test('path URL and invalid date rejection',()=>{assert.throws(()=>validateRequest({...make(),exact_url:'https://attacker.invalid/'}));assert.throws(()=>validateRequest(make('../SECRET')));assert.throws(()=>validateRequest(make('SYNTHETIC','2026-09-31')));});
test('quiet preflight does not claim or fetch',async()=>{const f=fixture({preflight:async()=>({state:'QUIET_EXACT_ADMISSION_WAIT'})});let n=0;assert.equal((await executeOne(f.db,async()=>{n++;},'k',1,'secret')).state,'QUIET_EXACT_ADMISSION_WAIT');assert.equal(n,0);assert.deepEqual(f.log,[]);});
test('retained body skips external acquisition',async()=>{const f=fixture({preflight:async()=>({state:'BODY_RETAINED'})});let n=0;await executeOne(f.db,async()=>{n++;},'k',1,'secret');assert.equal(n,0);assert.deepEqual(f.log,['consume']);});
test('native body saved before consumer with truthful single HTTP receipt',async()=>{const f=fixture();await executeOne(f.db,ok,'k',1,'secret');assert.deepEqual(f.log,['claim','save','consume']);assert.equal(f.saved[0].physical_http_count,1);assert.equal(f.saved[0].representation,'FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE');assert.equal(Buffer.from(f.saved[0].body_base64,'base64').toString(),body());});
test('FINISH/consumer failure retains body; replay consumes without another fetch',async()=>{let bad=true,n=0;const f=fixture({consume:async()=>{if(bad)throw new Error('synthetic failure');return{state:'POINT_RETAINED'};}});const fetcher=async()=>{n++;return ok();};assert.equal((await executeOne(f.db,fetcher,'k',1,'secret')).state,'BODY_RETAINED_CONSUMER_HOLD');assert.equal(f.saved.length,1);bad=false;assert.equal((await executeOne(f.db,fetcher,'k',1,'secret')).state,'POINT_RETAINED');assert.equal(n,1);});
test('invalid JSON and wrong ticker retain bodies and hold qualification',async()=>{for(const text of ['not JSON',body('WRONG')]){const f=fixture();assert.equal((await executeOne(f.db,async()=>new Response(text),'k',1,'secret')).state,'BODY_RETAINED_QUALITY_HOLD');assert.equal(f.saved.length,1);assert.ok(!f.log.includes('consume'));}});
test('uncertain transport holds without automatic retry or lease release',async()=>{const f=fixture();let n=0;const r=await executeOne(f.db,async()=>{n++;throw new Error('unknown');},'k',1,'secret');assert.equal(r.state,'UNKNOWN_EXTERNAL_OUTCOME_HELD');assert.equal(r.automatic_retry,false);assert.equal(n,1);assert.equal(f.saved.length,0);assert.ok(f.log.some(s=>s.startsWith('unknown:')));});
test('404/429 bodies retained with no absence inference and no retry',async()=>{for(const status of[404,429]){const f=fixture();const r=await executeOne(f.db,async()=>new Response('{}',{status}),'k',1,'secret');assert.equal(r.state,'HTTP_ERROR_BODY_RETAINED');assert.equal(r.automatic_retry,false);assert.equal(r.no_listing_inferred,false);assert.equal(f.saved.length,1);}});
test('1MiB response bound cancels acquisition and holds unknown external state',async()=>{const f=fixture();const r=await executeOne(f.db,async()=>new Response(new Uint8Array(MAX_BYTES+1)),'k',1,'secret');assert.equal(r.state,'UNKNOWN_EXTERNAL_OUTCOME_HELD');assert.equal(f.saved.length,0);});
test('invalid UTF8 exact bytes saved before decoding holds',async()=>{const f=fixture();const r=await executeOne(f.db,async()=>new Response(new Uint8Array([0xff,0xfe])),'k',1,'secret');assert.equal(r.state,'BODY_RETAINED_UTF8_HOLD');assert.equal(f.saved[0].body_base64,'//4=');});
test('fetch forbids redirects and carries one existing provider credential',async()=>{const f=fixture();await executeOne(f.db,async(u,o)=>{assert.equal(o.redirect,'error');assert.equal(o.headers.Authorization,'Bearer synthetic');assert.ok(o.signal);return ok();},'k',1,'synthetic');});
test('spool database failure never reports body retained or attempts a second fetch',async()=>{const f=fixture({save:async()=>{throw new Error('database unavailable');}});let n=0;await assert.rejects(()=>executeOne(f.db,async()=>{n++;return ok();},'k',1,'secret'));assert.equal(n,1);assert.ok(!f.log.includes('consume'));});
test('point projection never certifies intervals receipt or family',()=>{const r=validateEnvelope(JSON.parse(body()),make());assert.equal(r.family_accepted,false);assert.equal(r.intervals_qualified,false);assert.equal(r.original_historical_receipt_proven,false);});
test('chunked input bound cancels stream before unbounded allocation',async()=>{let cancelled=false;const stream=new ReadableStream({start(c){c.enqueue(new Uint8Array(2049));},cancel(){cancelled=true;}});await assert.rejects(()=>boundedInput(stream),e=>e.code==='INPUT_BOUND');assert.equal(cancelled,true);});
test('bounded valid input preserves JSON and rejects malformed UTF8',async()=>{const s=new Response('{"request_key":"synthetic"}').body;assert.equal((await boundedInput(s)).request_key,'synthetic');await assert.rejects(()=>boundedInput(new Response(new Uint8Array([255])).body));});

```

