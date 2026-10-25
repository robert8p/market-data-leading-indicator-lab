# Unintegrated historical OVERVIEW v4 preparation

No production approval, deployment, claim or source acquisition. Local synthetic tests: 17 passed.

## REVIEW_NOTES.md

```markdown
# Historical OVERVIEW v4 preparation — NOT INTEGRABLE

No implementation claim, production mutation, source acquisition, deployment, source admission or completion was performed for this draft.

The isolated JavaScript contract has 17 passing local synthetic tests. These cover 200 synthetic historical URLs, request/path/date binding, quiet preflight, retained-body reuse without fetching, byte-bounded receipt handling, save-before-consume call ordering, consumer failure, malformed JSON, wrong ticker, uncertain transport, 404/429, invalid UTF8, no inferred absence, no interval/family qualification and bounded chunked input. Mock save success is not durable-storage proof.

Primary review identified these required implementation gates:

1. `core.sql` is NOT an acceptable durable-receipt implementation yet. `ref01_overview_save_v4` still takes the global control/advisory path and calls a post-save guard. Control contention, admission expiry or an error can roll back the acquired body. The future implementation must prove a separately committed, private, byte-bounded receipt spool before contended FINISH/control processing. It must preserve current stops and ownership and must not claim durability from a mock RPC, worker RAM or ephemeral edge files. Any receipt-only quarantine after an already-dispatched request needs an explicitly reviewed fencing/accounting contract; no stop exception is supplied by this draft.
2. Lock order is inconsistent. The save route locks an attempt before control guarding, while the claim route locks controls before an attempt. The future unit must select one lock order and prove contention/deadlock behavior and prompt quiet preflight.
3. A `worker_revision` string is not an exact deployed-worker SHA binding. Pin the actual deployed worker source/bundle hash in the independently reviewed admission and authenticated runtime handshake before production.
4. Native HTTP accounting and registration are unfinished. `core.sql` currently increments provider and HTTP counters in a draft claim. The actual dispatcher/worker invocation ledger must be reviewed so every physical dispatch and provider call is counted once, including admitted waits/errors, without using the connector API's `UNKNOWN_CONNECTOR_INTERNALS` receipt for a known native call.
5. No real `pipeline_attempt`/task/commit route or registered automatic continuation exists. Implement and independently test genuine execution linkage; never fabricate an existing adapter's execution or equate local tests with useful production output.
6. No current-claim manifest seeding function, independently reviewed finite source admission, actual PostgreSQL rollback, deployed-worker synthetic test, recovery-manifest qualification or actual useful commit exists. A future exact assignment must do those before integration. The SQL is executable draft syntax, not validated PostgreSQL.
7. The 200 private request URLs and cases remain in DB evidence. The 200 synthetic requests in local tests are fixtures; they do not requalify factual identities, historical original receipts, listing intervals or any family.

Keep the existing Aug3 worker, current transport, scheduler/root claim, frozen cohort and every existing source/identity/lifecycle/session/family hold unchanged. Source acquisition remains unadmitted. The future claimed assignment's finite limits and repair counters apply; local preparation did not reset historical failures or admission windows.

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

## worker.ts

```typescript
import postgres from 'npm:postgres@3.4.5';
import {executeOne,REVISION,boundedInput} from './contract.mjs';
const S='population_top3000_202608_202609_v1';
// Deploy with verify_jwt=true. This draft is not authorized for production yet.
Deno.serve(async(req:Request)=>{
 if(req.method!=='POST')return Response.json({state:'METHOD_REJECTED'},{status:405});
 if(Number(req.headers.get('content-length')||0)>2048)return Response.json({state:'INPUT_BOUND'},{status:413});
 let b:any;try{b=await boundedInput(req.body);}catch(e:any){return Response.json({state:e?.code||'INPUT_INVALID'},{status:e?.code==='INPUT_BOUND'?413:400});}
 if(typeof b.request_key!=='string'||!/^[0-9a-f]{64}$/.test(b.request_key)||!Number.isSafeInteger(b.admission_event_id)||b.admission_event_id<=0)return Response.json({state:'EXACT_REQUEST_REQUIRED'},{status:400});
 const dbUrl=Deno.env.get('SUPABASE_DB_URL'),apiKey=(Deno.env.get('MASSIVE_API_KEY')||Deno.env.get('MASSIVE_KEY')||Deno.env.get('POLYGON_API_KEY')||Deno.env.get('POLYGON_KEY')||'').trim();
 if(!dbUrl||!apiKey)return Response.json({state:'EXISTING_CONFIGURATION_REQUIRED'},{status:503});
 const sql=postgres(dbUrl,{max:1,prepare:false,idle_timeout:2,connect_timeout:5,connection:{application_name:REVISION,statement_timeout:'8000',lock_timeout:'1500'}});
 const rpc=async(name:string,args:any[])=>{const p=args.map((_,i)=>'$'+(i+1)).join(',');return(await sql.unsafe(`select ${S}.${name}(${p}) as result`,args))[0].result;};
 const db={preflight:(k:string,a:number)=>rpc('ref01_overview_preflight_v4',[k,a]),claim:(k:string,a:number)=>rpc('ref01_overview_claim_v4',[k,a]),consume:(k:string,a:number)=>rpc('ref01_overview_consume_v4',[k,a]),markUnknown:(k:string,t:string,e:string)=>rpc('ref01_overview_unknown_v4',[k,t,e]),save:(k:string,t:string,r:any)=>rpc('ref01_overview_save_v4',[k,t,JSON.stringify(r)]),hash:async(v:Uint8Array)=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',v))).map(x=>x.toString(16).padStart(2,'0')).join('')};
 try{return Response.json(await executeOne(db,fetch,b.request_key,b.admission_event_id,apiKey));}catch{return Response.json({state:'BOUNDARY_OR_DATABASE_HOLD',automatic_retry:false},{status:503});}finally{await sql.end({timeout:2}).catch(()=>{});}
});

```

## core.sql

```sql
-- LOCAL PREPARATION ONLY: NOT EXECUTED, REVIEWED OR APPROVED FOR INTEGRATION.
-- Requires actual current-claim seeding, finite source admission, deployed-worker
-- synthetic proof, dispatcher/HTTP accounting integration and PostgreSQL rollback.
CREATE TABLE population_top3000_202608_202609_v1.ref01_overview_request_v4(
 request_key text PRIMARY KEY CHECK(request_key~'^[0-9a-f]{64}$'),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256='13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef'),
 ticker text NOT NULL,point_date date NOT NULL,exact_url text NOT NULL,content jsonb NOT NULL,content_sha256 text NOT NULL,
 UNIQUE(manifest_sha256,ticker,point_date),
 CHECK(ticker~'^[A-Za-z0-9._-]{1,64}$'),CHECK(point_date BETWEEN DATE '2026-08-01' AND DATE '2026-09-30'),
 CHECK(exact_url='https://api.massive.com/v3/reference/tickers/'||ticker||'?date='||point_date::text),
 CHECK(content_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex'))
);
CREATE TABLE population_top3000_202608_202609_v1.ref01_overview_attempt_v4(
 request_key text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ref01_overview_request_v4,
 admission_event_id bigint NOT NULL,token_sha256 text NOT NULL,owner_id text NOT NULL,owner_fence bigint NOT NULL,stop_generation bigint NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(),state text NOT NULL CHECK(state IN('CLAIMED','BODY_SAVED','COMPLETE','UNKNOWN_EXTERNAL_OUTCOME_HELD')),
 worker_revision text NOT NULL,body_sha256 text,unknown_reason text
);
CREATE TABLE population_top3000_202608_202609_v1.ref01_overview_body_v4(
 request_key text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ref01_overview_request_v4,
 body_bytes bytea NOT NULL,body_sha256 text NOT NULL,http_status integer NOT NULL CHECK(http_status BETWEEN 100 AND 599),
 content_bytes bigint NOT NULL CHECK(content_bytes BETWEEN 0 AND 1048576),received_at timestamptz NOT NULL DEFAULT clock_timestamp(),receipt jsonb NOT NULL,
 CHECK(content_bytes=octet_length(body_bytes)),CHECK(body_sha256=encode(sha256(body_bytes),'hex'))
);
CREATE TABLE population_top3000_202608_202609_v1.ref01_overview_point_v4(
 request_key text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ref01_overview_body_v4,
 body_sha256 text NOT NULL,content jsonb NOT NULL,record_sha256 text NOT NULL,
 CHECK(record_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex')),
 CHECK(content->>'evidence_semantics'='DATE_QUALIFIED_VENDOR_OVERVIEW_POINT_NOT_INTERVAL_V4'),
 CHECK(content->>'original_historical_receipt_proven'='false' AND content->>'intervals_qualified'='false' AND content->>'family_accepted'='false')
);
ALTER TABLE population_top3000_202608_202609_v1.ref01_overview_request_v4 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ref01_overview_attempt_v4 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ref01_overview_body_v4 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ref01_overview_point_v4 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_overview_request_v4,population_top3000_202608_202609_v1.ref01_overview_attempt_v4,population_top3000_202608_202609_v1.ref01_overview_body_v4,population_top3000_202608_202609_v1.ref01_overview_point_v4 FROM PUBLIC,anon,authenticated;

CREATE FUNCTION population_top3000_202608_202609_v1.ref01_overview_bundle_v4() RETURNS text LANGUAGE sql STABLE SET search_path='pg_catalog' AS $f$
SELECT encode(sha256(convert_to(string_agg(proname||':'||pg_get_functiondef(oid),E'\n' ORDER BY proname),'UTF8')),'hex') FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname IN('ref01_overview_bundle_v4','ref01_overview_guard_v4','ref01_overview_preflight_v4','ref01_overview_claim_v4','ref01_overview_save_v4','ref01_overview_unknown_v4','ref01_overview_consume_v4');
$f$;
CREATE FUNCTION population_top3000_202608_202609_v1.ref01_overview_guard_v4(p_admission bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE ctl jsonb;d jsonb;rv jsonb;BEGIN
ctl:=population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
SELECT details INTO d FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=p_admission AND event='REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4';
SELECT details INTO rv FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=(d->>'independent_review_event_id')::bigint AND event='REF01_OVERVIEW_FINITE_SOURCE_REVIEW_V4';
IF d IS NULL OR rv IS NULL OR rv->>'approved' IS DISTINCT FROM 'true' OR nullif(rv->>'reviewer','') IS NULL OR rv->>'reviewer' IS NOT DISTINCT FROM rv->>'producer' OR d->>'request_manifest_sha256' IS DISTINCT FROM '13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef' OR d->>'bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_overview_bundle_v4() OR d->>'worker_revision' IS DISTINCT FROM 'ref01-historical-overview-v4' OR d->>'owner_id' IS DISTINCT FROM ctl->>'owner_id' OR d->>'owner_fence' IS DISTINCT FROM '3' OR d->>'stop_generation' IS DISTINCT FROM '1' OR d->>'scope_sha256' IS DISTINCT FROM ctl->>'scope_sha256' OR d->>'not_after' IS NULL OR (d->>'not_after')::timestamptz<=clock_timestamp() OR d->>'max_provider_calls' IS DISTINCT FROM '200' OR d->>'max_http_invocations' IS DISTINCT FROM '200' OR d->>'max_source_bytes' IS DISTINCT FROM '33554432' OR d->>'max_relation_bytes' IS DISTINCT FROM '33554432' OR d->>'automatic_retries' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'OV4_EXACT_REVIEWED_FINITE_ADMISSION_REQUIRED';END IF;
IF rv->>'request_manifest_sha256' IS DISTINCT FROM d->>'request_manifest_sha256' OR rv->>'bundle_sha256' IS DISTINCT FROM d->>'bundle_sha256' OR rv->>'worker_revision' IS DISTINCT FROM d->>'worker_revision' THEN RAISE EXCEPTION 'OV4_REVIEW_BINDING_CHANGED';END IF;
IF (SELECT sum(pg_total_relation_size(oid)) FROM pg_class WHERE oid IN('population_top3000_202608_202609_v1.ref01_overview_request_v4'::regclass,'population_top3000_202608_202609_v1.ref01_overview_attempt_v4'::regclass,'population_top3000_202608_202609_v1.ref01_overview_body_v4'::regclass,'population_top3000_202608_202609_v1.ref01_overview_point_v4'::regclass))>33554432 THEN RAISE EXCEPTION 'OV4_CUMULATIVE_RELATION_BOUND';END IF;
RETURN d;
END $f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ref01_overview_preflight_v4(p_key text,p_admission bigint) RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path='pg_catalog' AS $f$
DECLARE d jsonb;BEGIN
-- Quiet read-only check: no row locks, no relation-wide aggregation, no HTTP.
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control a CROSS JOIN population_top3000_202608_202609_v1.pipeline_control p WHERE a.singleton AND p.singleton AND a.owner_id='top3000-completion-20261024-root-5db48b32d103' AND a.owner_fence=3 AND a.stop_generation=1 AND a.enabled AND NOT a.stopped AND a.hold_reason IS NULL AND p.enabled AND a.admitted_until>clock_timestamp() AND p.deadline>clock_timestamp() AND p.tick_count<p.max_ticks AND a.provider_requests<a.max_provider_requests AND a.http_invocations<a.max_http_invocations) THEN RETURN '{"state":"QUIET_CONTROL_OR_CAP_WAIT"}'::jsonb;END IF;
SELECT details INTO d FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=p_admission AND event='REF01_OVERVIEW_FINITE_SOURCE_ADMITTED_V4';
IF d IS NULL OR d->>'not_after' IS NULL OR (d->>'not_after')::timestamptz<=clock_timestamp() OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_overview_request_v4 WHERE request_key=p_key AND manifest_sha256=d->>'request_manifest_sha256') THEN RETURN '{"state":"QUIET_EXACT_ADMISSION_WAIT"}'::jsonb;END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_overview_body_v4 WHERE request_key=p_key) THEN RETURN '{"state":"BODY_RETAINED"}'::jsonb;END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_overview_attempt_v4 WHERE request_key=p_key) THEN RETURN '{"state":"EXISTING_ATTEMPT_HELD_NO_REISSUE"}'::jsonb;END IF;
RETURN '{"state":"ELIGIBLE"}'::jsonb;
END $f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ref01_overview_claim_v4(p_key text,p_admission bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE d jsonb;r record;a record;p record;tok text;k text;relation_bytes bigint;BEGIN
IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RETURN '{"state":"BUSY"}'::jsonb;END IF;
d:=population_top3000_202608_202609_v1.ref01_overview_guard_v4(p_admission);
SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ref01_overview_request_v4 WHERE request_key=p_key FOR UPDATE;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_overview_body_v4 WHERE request_key=p_key) THEN RETURN '{"state":"BODY_RETAINED"}'::jsonb;END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_overview_attempt_v4 WHERE request_key=p_key) THEN RETURN '{"state":"EXISTING_ATTEMPT_HELD_NO_REISSUE"}'::jsonb;END IF;
SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
IF a.next_provider_at>clock_timestamp() OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='LEASED' AND lease_until>clock_timestamp()) THEN RETURN '{"state":"QUIET_SHARED_PROVIDER_WAIT"}'::jsonb;END IF;
SELECT sum(pg_total_relation_size(oid)) INTO relation_bytes FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m');
IF a.provider_requests>=a.max_provider_requests OR a.http_invocations>=a.max_http_invocations OR a.stored_bytes+a.reserved_bytes+1048576>a.max_source_bytes OR relation_bytes>=(p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_overview_attempt_v4)>=200 OR coalesce((SELECT sum(content_bytes) FROM population_top3000_202608_202609_v1.ref01_overview_body_v4),0)+1048576>33554432 THEN RETURN '{"state":"FINITE_RESOURCE_BOUND"}'::jsonb;END IF;
IF (SELECT sum(pg_total_relation_size(oid)) FROM pg_class WHERE oid IN('population_top3000_202608_202609_v1.ref01_overview_request_v4'::regclass,'population_top3000_202608_202609_v1.ref01_overview_attempt_v4'::regclass,'population_top3000_202608_202609_v1.ref01_overview_body_v4'::regclass,'population_top3000_202608_202609_v1.ref01_overview_point_v4'::regclass))+3145728>33554432 THEN RETURN '{"state":"FINITE_RELATION_BOUND"}'::jsonb;END IF;
tok:=gen_random_uuid()::text||gen_random_uuid()::text;k:='ref01-overview-v4:'||p_key;
INSERT INTO population_top3000_202608_202609_v1.connector_probe_lease_v1(request_key,owner_id,owner_fence,stop_generation,lane,provider,request_sha256,reserved_bytes,reserved_usd) VALUES(k,a.owner_id,3,1,'REF01','Massive',r.content_sha256,1048576,0);
INSERT INTO population_top3000_202608_202609_v1.ref01_overview_attempt_v4(request_key,admission_event_id,token_sha256,owner_id,owner_fence,stop_generation,state,worker_revision) VALUES(p_key,p_admission,encode(sha256(convert_to(tok,'UTF8')),'hex'),a.owner_id,3,1,'CLAIMED','ref01-historical-overview-v4');
UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,http_invocations=http_invocations+1,reserved_bytes=reserved_bytes+1048576,next_provider_at=clock_timestamp()+interval '15 seconds' WHERE singleton;
RETURN jsonb_build_object('state','CLAIMED','token',tok,'request',jsonb_build_object('request_key',p_key,'request_manifest_sha256',r.manifest_sha256,'scope_sha256',a.scope_sha256,'request_state','ADMITTED','ticker',r.ticker,'point_date',r.point_date,'exact_url',r.exact_url,'path','/v3/reference/tickers/'||r.ticker));
END $f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ref01_overview_save_v4(p_key text,p_token text,p_receipt text) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE x record;d jsonb;j jsonb;b bytea;n bigint;h text;l record;old record;BEGIN
PERFORM pg_advisory_xact_lock(2026083000261022);
SELECT * INTO STRICT x FROM population_top3000_202608_202609_v1.ref01_overview_attempt_v4 WHERE request_key=p_key FOR UPDATE;
d:=population_top3000_202608_202609_v1.ref01_overview_guard_v4(x.admission_event_id);
IF x.token_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_token,'UTF8')),'hex') OR x.state NOT IN('CLAIMED','BODY_SAVED','COMPLETE') THEN RAISE EXCEPTION 'OV4_ATTEMPT_AUTH_OR_UNKNOWN_HOLD';END IF;
j:=p_receipt::jsonb;
IF octet_length(p_receipt)>1500000 OR j->>'worker_revision' IS DISTINCT FROM 'ref01-historical-overview-v4' OR j->>'physical_http_count' IS DISTINCT FROM '1' OR j->>'representation' IS DISTINCT FROM 'FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE' THEN RAISE EXCEPTION 'OV4_RECEIPT_BINDING';END IF;
b:=decode(j->>'body_base64','base64');n:=octet_length(b);h:=encode(sha256(b),'hex');
IF n>1048576 OR j->>'bytes' IS DISTINCT FROM n::text OR j->>'sha256' IS DISTINCT FROM h OR (j->>'http_status')::int NOT BETWEEN 100 AND 599 THEN RAISE EXCEPTION 'OV4_EXACT_SOURCE_BYTES_REQUIRED';END IF;
SELECT * INTO old FROM population_top3000_202608_202609_v1.ref01_overview_body_v4 WHERE request_key=p_key;
IF FOUND THEN IF old.body_bytes IS DISTINCT FROM b OR old.receipt IS DISTINCT FROM j-'body_base64' THEN RAISE EXCEPTION 'OV4_IMMUTABLE_BODY_CONFLICT';END IF;RETURN jsonb_build_object('state','BODY_ALREADY_RETAINED','sha256',h);END IF;
SELECT * INTO STRICT l FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE request_key='ref01-overview-v4:'||p_key AND released_at IS NULL FOR UPDATE;
IF coalesce((SELECT sum(content_bytes) FROM population_top3000_202608_202609_v1.ref01_overview_body_v4),0)+n>33554432 THEN RAISE EXCEPTION 'OV4_CUMULATIVE_SOURCE_BOUND';END IF;
INSERT INTO population_top3000_202608_202609_v1.ref01_overview_body_v4(request_key,body_bytes,body_sha256,http_status,content_bytes,receipt) VALUES(p_key,b,h,(j->>'http_status')::int,n,j-'body_base64');
UPDATE population_top3000_202608_202609_v1.ref01_overview_attempt_v4 SET state='BODY_SAVED',body_sha256=h WHERE request_key=p_key;
UPDATE population_top3000_202608_202609_v1.connector_probe_lease_v1 SET released_at=clock_timestamp(),receipt=j-'body_base64'||jsonb_build_object('state','RECEIVED_NATIVE_HTTP_BODY','incremental_cost_state','UNINVOICED_RESERVATION_RETAINED') WHERE request_key=l.request_key;
UPDATE population_top3000_202608_202609_v1.acquisition_control SET reserved_bytes=reserved_bytes-l.reserved_bytes,stored_bytes=stored_bytes+n WHERE singleton AND reserved_bytes>=l.reserved_bytes;
IF NOT FOUND THEN RAISE EXCEPTION 'OV4_RESERVATION_COUNTER_MISMATCH';END IF;
PERFORM population_top3000_202608_202609_v1.ref01_overview_guard_v4(x.admission_event_id);
RETURN jsonb_build_object('state','BODY_RETAINED','sha256',h,'bytes',n,'original_historical_receipt_proven',false);
END $f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ref01_overview_unknown_v4(p_key text,p_token text,p_reason text) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE x record;BEGIN
SELECT * INTO STRICT x FROM population_top3000_202608_202609_v1.ref01_overview_attempt_v4 WHERE request_key=p_key FOR UPDATE;
PERFORM population_top3000_202608_202609_v1.ref01_overview_guard_v4(x.admission_event_id);
IF x.token_sha256 IS DISTINCT FROM encode(sha256(convert_to(p_token,'UTF8')),'hex') OR x.state IS DISTINCT FROM 'CLAIMED' OR length(p_reason) NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'OV4_UNKNOWN_STATE_AUTH_REQUIRED';END IF;
UPDATE population_top3000_202608_202609_v1.ref01_overview_attempt_v4 SET state='UNKNOWN_EXTERNAL_OUTCOME_HELD',unknown_reason=p_reason WHERE request_key=p_key;
RETURN '{"state":"UNKNOWN_EXTERNAL_OUTCOME_HELD","lease_released":false,"automatic_retry":false}'::jsonb;
END $f$;

CREATE FUNCTION population_top3000_202608_202609_v1.ref01_overview_consume_v4(p_key text,p_admission bigint) RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE r record;b record;doc jsonb;c jsonb;h text;old record;BEGIN
PERFORM population_top3000_202608_202609_v1.ref01_overview_guard_v4(p_admission);
SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ref01_overview_request_v4 WHERE request_key=p_key;
SELECT * INTO STRICT b FROM population_top3000_202608_202609_v1.ref01_overview_body_v4 WHERE request_key=p_key FOR SHARE;
IF b.body_sha256 IS DISTINCT FROM encode(sha256(b.body_bytes),'hex') OR b.http_status<>200 THEN RETURN '{"state":"BODY_RETAINED_QUALITY_HOLD","no_listing_inferred":false}'::jsonb;END IF;
BEGIN doc:=convert_from(b.body_bytes,'UTF8')::jsonb;EXCEPTION WHEN OTHERS THEN RETURN '{"state":"BODY_RETAINED_ENCODING_OR_JSON_HOLD"}'::jsonb;END;
IF doc->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(doc->'results') IS DISTINCT FROM 'object' OR doc#>>'{results,ticker}' IS DISTINCT FROM r.ticker OR nullif(doc->>'next_url','') IS NOT NULL THEN RETURN '{"state":"BODY_RETAINED_IDENTITY_OR_STATUS_HOLD","no_listing_inferred":false}'::jsonb;END IF;
c:=jsonb_build_object('ticker',r.ticker,'point_date',r.point_date,'source_record',doc->'results','source_sha256',b.body_sha256,'request_key',p_key,'request_manifest_sha256',r.manifest_sha256,'evidence_semantics','DATE_QUALIFIED_VENDOR_OVERVIEW_POINT_NOT_INTERVAL_V4','original_historical_receipt_proven',false,'intervals_qualified',false,'family_accepted',false);
h:=encode(sha256(convert_to(c::text,'UTF8')),'hex');
INSERT INTO population_top3000_202608_202609_v1.ref01_overview_point_v4 VALUES(p_key,b.body_sha256,c,h) ON CONFLICT DO NOTHING;
SELECT * INTO STRICT old FROM population_top3000_202608_202609_v1.ref01_overview_point_v4 WHERE request_key=p_key;
IF old.content IS DISTINCT FROM c OR old.record_sha256 IS DISTINCT FROM h THEN RAISE EXCEPTION 'OV4_IMMUTABLE_POINT_CONFLICT';END IF;
UPDATE population_top3000_202608_202609_v1.ref01_overview_attempt_v4 SET state='COMPLETE' WHERE request_key=p_key AND admission_event_id=p_admission AND state IN('BODY_SAVED','COMPLETE');
IF NOT FOUND THEN RAISE EXCEPTION 'OV4_EXACT_SOURCE_ATTEMPT_REQUIRED';END IF;
PERFORM population_top3000_202608_202609_v1.ref01_overview_guard_v4(p_admission);
RETURN jsonb_build_object('state','POINT_RETAINED','request_key',p_key,'record_sha256',h,'family_accepted',false);
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_overview_bundle_v4(),population_top3000_202608_202609_v1.ref01_overview_guard_v4(bigint),population_top3000_202608_202609_v1.ref01_overview_preflight_v4(text,bigint),population_top3000_202608_202609_v1.ref01_overview_claim_v4(text,bigint),population_top3000_202608_202609_v1.ref01_overview_save_v4(text,text,text),population_top3000_202608_202609_v1.ref01_overview_unknown_v4(text,text,text),population_top3000_202608_202609_v1.ref01_overview_consume_v4(text,bigint) FROM PUBLIC,anon,authenticated;

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
