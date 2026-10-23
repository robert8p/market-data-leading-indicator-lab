// Actual isolated worker-handler tests. Fake DB/fetch only; no network or real credentials.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import {stripTypeScriptTypes} from 'node:module';
import {webcrypto,createHash} from 'node:crypto';
const input=fs.readFileSync(new URL('./population-top3000-minute-v1_DRAFT.ts',import.meta.url),'utf8').replace(/^import postgres from "npm:postgres@3\.4\.5";$/m,'const postgres=globalThis.mockPostgres;');
const source=stripTypeScriptTypes(input,{mode:'strip'});
const PATH='/v2/aggs/ticker/AAPL/range/1/minute/1788840000000/1788926399999';
const FIRST='https://api.massive.com'+PATH+'?adjusted=false&sort=asc&limit=50000';
const secret='NON_SECRET_MASSIVE_MINUTE_FIXTURE_91';
const env={SUPABASE_URL:'https://oxzabweahkoimtevbbny.supabase.co',SUPABASE_DB_URL:'postgres://mock.invalid/never-connect',MASSIVE_API_KEY:secret};
const baseClaim={state:'RESERVED',archive_key:'p01-massive-minute-v1:'+'b'.repeat(64),page_number:1,request_url:FIRST,max_response_bytes:8388608,max_runtime_ms:45000,family_id:'P01',partition_key:'massive-minute-AAPL-20260908-v1',source_symbol:'AAPL',trading_date:'2026-09-08',window_start_ms:1788840000000,window_end_exclusive_ms:1788926400000,adjusted:false,max_result_count:50000};
let handler,dbCalls=0,fetchCalls=0,claimAllowed=false,used=false,ctx={...baseClaim},commitFailure=false,committed=null,failure=null,status=200,responseText='',declaredLength=null,fetchThrow=false,requestSeen=null;
function mockPostgres(url,options){
 assert.equal(url,env.SUPABASE_DB_URL);assert.equal(options.max,1);assert.equal(options.prepare,false);dbCalls++;
 return {begin:async(fn)=>{
  const tx=async(strings,...values)=>{const q=strings.join('');
   if(q.includes('massive_minute_claim_v1')){if(!claimAllowed||used)throw new Error('DO_NOT_EXPOSE_CONTROL_ERROR');used=true;return [{result:{...ctx}}];}
   if(q.includes('massive_minute_commit_v1')){assert.equal(values[2].kind,'json-wrapper');if(commitFailure)throw new Error('DO_NOT_EXPOSE_UNKNOWN_COMMIT');committed=values[2].value;return [{result:{state:committed.next_url?'CONTINUE':'COMPLETE'}}];}
   if(q.includes('massive_minute_failure_v1')){assert.equal(values[2].kind,'json-wrapper');failure=values[2].value;return [{result:{state:'BLOCKED'}}];}return [];
  };
  tx.json=(value)=>{assert.equal(typeof value,'object');assert.notEqual(value,null);return {kind:'json-wrapper',value};};return fn(tx);
 },end:()=>Promise.resolve()};
}
const context=vm.createContext({mockPostgres,Deno:{env:{get:n=>env[n]},serve:fn=>handler=fn},crypto:webcrypto,TextEncoder,TextDecoder,Uint8Array,ArrayBuffer,AbortSignal,DOMException,Request,Response,ReadableStream,URL,setTimeout,clearTimeout,
 fetch:async(url,options)=>{fetchCalls++;requestSeen={url,options};const u=new URL(url);assert.equal(u.origin,'https://api.massive.com');assert.equal(u.pathname,PATH);assert.equal(u.searchParams.get('apiKey'),secret);assert.equal(options.redirect,'error');if(fetchThrow)throw new Error('DO_NOT_EXPOSE_FETCH_ERROR');const h={};if(declaredLength!==null)h['content-length']=String(declaredLength);return new Response(responseText,{status,headers:h});}});
vm.runInContext(source,context,{filename:'massive-minute-worker-transpiled.js'});
const envelope=(results,extras={})=>JSON.stringify({status:'OK',ticker:'AAPL',adjusted:false,results,...extras});
const raw=' { "status":"OK", "ticker":"AAPL", "adjusted":false, "results":[{"t":1788840000000,"o":0.12345678901234567890,"h":1,"l":0.00000000000000000001,"c":0.12345678901234567890,"v":9007199254740993,"n":0}], "resultsCount":1.0, "next_url":"https://api.massive.com'+PATH+'?cursor=NEXT%3D&apiKey='+secret+'", "request_id":"fixture" } ';
const clean=context.sanitizeEnvelope(raw,secret);
assert.equal(clean.result_count,1);assert.equal(clean.results_presence,'ARRAY');assert.equal(clean.next_url,'https://api.massive.com'+PATH+'?cursor=NEXT%3D');
assert.equal(clean.content_text,raw.replace('"https://api.massive.com'+PATH+'?cursor=NEXT%3D&apiKey='+secret+'"','"https://api.massive.com'+PATH+'?cursor=NEXT%3D"'));
assert.equal(clean.content_text.includes('9007199254740993'),true);assert.equal(clean.content_text.includes('0.12345678901234567890'),true);
assert.equal(context.sanitizeEnvelope(envelope([]),secret).result_count,0);
const explicitEmpty='{"status":"OK","ticker":"AAPL","adjusted":false,"resultsCount":0}';
assert.equal(context.sanitizeEnvelope(explicitEmpty,secret).results_presence,'OMITTED_WITH_EXPLICIT_ZERO');
assert.equal(context.sanitizeEnvelope(explicitEmpty,secret).content_text,explicitEmpty);
for(const bad of ['{"status":"OK","ticker":"AAPL","adjusted":false}',envelope(null,{resultsCount:0}),envelope([],{ticker:'MSFT'}),envelope([],{adjusted:true}),envelope([],{status:'ERROR'})])assert.throws(()=>context.sanitizeEnvelope(bad,secret),e=>e.code==='INVALID_SOURCE_ENVELOPE');
for(const count of [-1,0.5,'0',null])assert.throws(()=>context.sanitizeEnvelope(envelope([],{resultsCount:count}),secret),e=>e.code==='INVALID_SOURCE_COUNT');
assert.throws(()=>context.sanitizeEnvelope(envelope([{}],{resultsCount:0}),secret),e=>e.code==='INVALID_SOURCE_COUNT');
assert.throws(()=>context.sanitizeEnvelope(envelope(Array.from({length:50001},()=>({}))),secret),e=>e.code==='INVALID_SOURCE_COUNT');
for(const text of ['{"status":"OK","status":"OK"}',envelope([]).replace('"results":[]','"results":[{"t":1,"t":2}]'),envelope([]).replace('"results":[]','"results":[{"nested":{"a":1,"a":2}}]')])assert.throws(()=>context.sanitizeEnvelope(text,secret),e=>e.code==='DUPLICATE_SOURCE_KEY');
assert.throws(()=>context.sanitizeEnvelope(envelope([{hidden:secret}]),secret),e=>e.code==='SOURCE_CREDENTIAL_ECHO');
// Acquisition preserves malformed/out-of-range records for SQL quarantine; it never edits time or fabricates a bar.
const unusual=envelope([{t:1788839999999},{t:1788926400000},{t:1788840000001},{}]);assert.equal(context.sanitizeEnvelope(unusual,secret).content_text,unusual);
for(const url of ['https://evil.invalid'+PATH+'?cursor=X','https://api.massive.com'+PATH.replace('AAPL','MSFT')+'?cursor=X','https://api.massive.com'+PATH.replace('1788926399999','1788926400000')+'?cursor=X','https://u:p@api.massive.com'+PATH+'?cursor=X','https://api.massive.com'+PATH+'?cursor=X&cursor=Y','https://api.massive.com'+PATH+'?cursor=X#fragment'])assert.throws(()=>context.canonicalNext(url),e=>e.code==='INVALID_NEXT_URL');
for(const param of ['adjusted=true','sort=desc','limit=50001','ticker=MSFT'])assert.throws(()=>context.canonicalNext('https://api.massive.com'+PATH+'?cursor=X&'+param),e=>e.code==='INVALID_NEXT_URL');
assert.equal(context.canonicalNext('https://api.massive.com'+PATH+'?cursor=X&adjusted=false&sort=asc&limit=50000'),'https://api.massive.com'+PATH+'?cursor=X');
assert.throws(()=>context.validateRequestUrl(FIRST+'&apiKey=unsafe'),e=>e.code==='INVALID_NEXT_URL'||e.code==='UNADMITTED_SOURCE_URL');
await assert.rejects(()=>context.readLimited(new ReadableStream({start(c){c.enqueue(new Uint8Array(513));c.close();}}),512,Date.now()+1000),e=>e.code==='RESPONSE_TOO_LARGE');
const request=()=>new Request('https://owned.invalid/worker',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({attempt_id:'11111111-1111-4111-8111-111111111111',token:'a'.repeat(64)})});
let res=await handler(new Request('https://owned.invalid/worker',{method:'GET'}));assert.equal(res.status,405);assert.equal(dbCalls,0);assert.equal(fetchCalls,0);
res=await handler(new Request('https://owned.invalid/worker',{method:'POST',headers:{'Content-Type':'application/json'},body:'{"attempt_id":"bad","token":"bad"}'}));assert.equal(res.status,400);assert.equal(dbCalls,0);
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,0);assert.equal((await res.text()).includes('DO_NOT_EXPOSE'),false);
claimAllowed=true;used=false;ctx={...baseClaim,state:'BLOCKED'};res=await handler(request());assert.equal(fetchCalls,0);
ctx={...baseClaim};used=false;responseText=raw;res=await handler(request());assert.equal(res.status,200);let out=await res.json();assert.equal(out.state,'CONTINUE');assert.equal(fetchCalls,1);assert.equal(committed.content_text,clean.content_text);
assert.equal(committed.source_symbol,'AAPL');assert.equal(committed.trading_date,'2026-09-08');assert.equal(committed.window_start_ms,1788840000000);assert.equal(committed.window_end_exclusive_ms,1788926400000);
assert.equal(committed.results_presence,'ARRAY');assert.equal(committed.wire_sha256,createHash('sha256').update(raw).digest('hex'));assert.equal(committed.source_sha256,createHash('sha256').update(clean.content_text).digest('hex'));assert.notEqual(committed.wire_sha256,committed.source_sha256);assert.equal(JSON.stringify(committed).includes(secret),false);
for(const field of ['company_id','security_id','available_at','historical_received_at','sip_feed'])assert.equal(field in committed,false);
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,1);
used=false;ctx={...baseClaim,page_number:2,request_url:clean.next_url};responseText=explicitEmpty;res=await handler(request());assert.equal(res.status,200);assert.equal((await res.json()).state,'COMPLETE');assert.equal(fetchCalls,2);assert.equal(committed.page_number,2);assert.equal(committed.result_count,0);assert.equal(committed.results_presence,'OMITTED_WITH_EXPLICIT_ZERO');
for(const patch of [{family_id:'P02'},{partition_key:'massive-minute-AAPL-20260909-v1'},{source_symbol:'MSFT'},{trading_date:'2026-09-09'},{window_start_ms:1788840000001},{window_end_exclusive_ms:1788926399999},{adjusted:true},{max_result_count:50001},{page_number:101},{archive_key:'wrong'}]){
 used=false;ctx={...baseClaim,...patch};failure=null;const n=fetchCalls;res=await handler(request());assert.equal(res.status,200);assert.equal(failure.code,'INVALID_CLAIM');assert.equal(fetchCalls,n);
}
ctx={...baseClaim};used=false;status=403;failure=null;responseText='RAW_PROVIDER_ERROR_DO_NOT_EXPOSE';res=await handler(request());assert.equal(failure.failure_class,'AUTHENTICATION');assert.equal(JSON.stringify(failure).includes(responseText),false);
used=false;status=429;failure=null;res=await handler(request());assert.equal(failure.failure_class,'RATE_LIMIT');
used=false;status=200;declaredLength=8388609;responseText=envelope([]);failure=null;res=await handler(request());assert.equal(failure.code,'RESPONSE_TOO_LARGE');assert.equal(failure.fetch_started,true);assert.equal(failure.wire_sha256,null);declaredLength=null;
used=false;commitFailure=true;failure=null;res=await handler(request());assert.equal(res.status,503);out=await res.json();assert.equal(out.phase,'FINISH');assert.equal(out.receipt_recorded,false);assert.equal(failure,null);const afterUnknown=fetchCalls;res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,afterUnknown);commitFailure=false;
used=false;delete env.MASSIVE_API_KEY;failure=null;const beforeMissing=fetchCalls;res=await handler(request());assert.equal(fetchCalls,beforeMissing);assert.equal(failure.code,'SOURCE_CREDENTIAL_UNAVAILABLE');env.MASSIVE_API_KEY=secret;
used=false;fetchThrow=true;failure=null;res=await handler(request());assert.equal(failure.failure_class,'TRANSPORT_READ');assert.equal(failure.fetch_started,true);assert.equal(failure.wire_sha256,null);assert.equal((await res.text()).includes('DO_NOT_EXPOSE'),false);
console.log('PASS_LOCAL_MASSIVE_MINUTE_FIXED_AAPL_NY_DAY_AUTH_CURSOR_EXACT_NUMBERS_OPTIONAL_EMPTY_RESULTS_LIMITS_AND_UNKNOWN_EFFECT');
