// Local-only fixed-route contract tests. Fake DB and fetch; no provider requests.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import {stripTypeScriptTypes} from 'node:module';
import {webcrypto,createHash} from 'node:crypto';

const text=fs.readFileSync(new URL('./population-top3000-daily-v2_DRAFT.ts',import.meta.url),'utf8')
 .replace(/^import postgres from "npm:postgres@3\.4\.5";$/m,'const postgres=globalThis.mockPostgres;');
const source=stripTypeScriptTypes(text,{mode:'strip'});
const FIXED='https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/2026-08-03?adjusted=false&include_otc=false';
const calendar=JSON.parse(fs.readFileSync(new URL('./d02_daily_v2_calendar_contract.json',import.meta.url),'utf8'));
assert.equal(calendar.dates.length,42);assert.equal(new Set(calendar.dates).size,42);
assert.equal(createHash('sha256').update(calendar.dates.join('\n')).digest('hex'),calendar.calendar_date_list_sha256);
const fixedFor=date=>'https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'+date+'?adjusted=false&include_otc=false';
const secret='NON_SECRET_D02_FIXTURE_KEY_83';
const env={SUPABASE_URL:'https://oxzabweahkoimtevbbny.supabase.co',SUPABASE_DB_URL:'postgres://mock.invalid/never-connect',MASSIVE_API_KEY:secret};
const validClaim={state:'RESERVED',archive_key:'d02-daily-v2:'+'f'.repeat(64),request_url:FIXED,max_response_bytes:8388608,max_runtime_ms:45000,max_result_count:50000,family_id:'D02',partition_key:'daily-source-20260803-raw-no-otc-v2',trading_date:'2026-08-03',adjusted:false,include_otc:false,calendar_scope_id:calendar.calendar_scope_id,calendar_date_list_sha256:calendar.calendar_date_list_sha256,calendar_manifest_sha256:'c'.repeat(64)};
let handler,dbCalls=0,fetchCalls=0,claimAllowed=false,used=false,claim={...validClaim},commitFailure=false,committed=null,failure=null,failureCalls=0;
let status=200,responseText='',declaredLength=null,fetchThrow=false,responseBytes=null;
const calls=[];
function mockPostgres(url,options){
 assert.equal(url,env.SUPABASE_DB_URL);assert.equal(options.max,1);assert.equal(options.prepare,false);assert.equal(options.ssl,'require');assert.equal(options.debug,false);dbCalls++;
 return {begin:async(fn)=>{
  const tx=async(strings,...values)=>{const q=strings.join('');calls.push(q);
   if(q.includes('d02_daily_claim_v2')){if(!claimAllowed||used)throw Object.assign(new Error('RAW_DB_DETAIL_MUST_NOT_LEAVE'),{code:'P0001'});used=true;return [{result:{...claim}}];}
   if(q.includes('d02_daily_commit_v2')){assert.equal(values[2].kind,'json-wrapper');if(commitFailure)throw Object.assign(new Error('RAW_DB_DETAIL_MUST_NOT_LEAVE'),{code:'55P03'});committed=values[2].value;return [{result:{state:'COMPLETE'}}];}
   if(q.includes('d02_daily_failure_v2')){assert.equal(values[2].kind,'json-wrapper');failureCalls++;failure=values[2].value;return [{result:{state:'BLOCKED'}}];}
   assert.ok(q.includes('set local statement_timeout')||q.includes('set local lock_timeout'));
   return [];
  };
  tx.json=(value)=>{assert.equal(typeof value,'object');assert.notEqual(value,null);return {kind:'json-wrapper',value};};
  return fn(tx);
 },end:()=>Promise.resolve()};
}
const context=vm.createContext({mockPostgres,Deno:{env:{get:n=>env[n]},serve:fn=>{handler=fn;}},crypto:webcrypto,TextEncoder,TextDecoder,Uint8Array,ArrayBuffer,AbortSignal,DOMException,Request,Response,ReadableStream,URL,setTimeout,clearTimeout,
 fetch:async(url,options)=>{
  fetchCalls++;assert.equal(options.redirect,'error');assert.equal(options.method,'GET');assert.equal(options.headers.Accept,'application/json');
  const u=new URL(url);assert.equal(u.searchParams.get('apiKey'),secret);u.searchParams.delete('apiKey');assert.equal(u.toString(),fixedFor(claim.trading_date));
  if(fetchThrow)throw new Error('RAW_FETCH_SECRET_MUST_NOT_LEAVE');
  const headers={};if(declaredLength!==null)headers['content-length']=String(declaredLength);
  return new Response(responseBytes??responseText,{status,headers});
 }});
vm.runInContext(source,context,{filename:'d02-worker-transpiled.js'});

// Preserve decimal/large-integer spellings, Unicode and whitespace byte for byte.
const raw=' { "status":"OK", "adjusted":false, "queryCount":1, "resultsCount":1, "results":[{"T":"FIXTURE","o":99.5000000,"h":101.25,"l":98.5,"c":100.12345678901234567890,"v":9007199254740993,"vw":1.234e2,"t":1785729600000,"label":"é\\u0020price"}], "request_id":"fixture-request" }\n';
const clean=context.validateEnvelope(raw,secret);
assert.equal(clean.content_text,raw);assert.equal(clean.result_count,1);
assert.equal(context.validateEnvelope('{"status":"OK","adjusted":false,"results":[]}',secret).result_count,0);
assert.equal(context.validateEnvelope('{"status":"OK","adjusted":false,"results":[],"queryCount":12,"resultsCount":0,"next_url":null}',secret).result_count,0);
const invalid=[
 ['{"adjusted":false,"results":[]}','INVALID_SOURCE_ENVELOPE'],
 ['{"status":"ERROR","adjusted":false,"results":[]}','INVALID_SOURCE_ENVELOPE'],
 ['{"status":"OK","results":[]}','INVALID_SOURCE_ENVELOPE'],
 ['{"status":"OK","adjusted":true,"results":[]}','INVALID_SOURCE_ENVELOPE'],
 ['{"status":"OK","adjusted":"false","results":[]}','INVALID_SOURCE_ENVELOPE'],
 ['{"status":"OK","adjusted":false}','INVALID_SOURCE_ENVELOPE'],
 ['{"status":"OK","adjusted":false,"results":[null]}','INVALID_SOURCE_RECORD'],
 ['{"status":"OK","adjusted":false,"results":[[]]}','INVALID_SOURCE_RECORD'],
 ['{"status":"OK","adjusted":false,"results":[],"next_url":"https://api.massive.com/anything"}','UNEXPECTED_CONTINUATION'],
 ['{"status":"OK","adjusted":false,"results":[],"next_url":false}','UNEXPECTED_CONTINUATION'],
 ['{"status":"OK","adjusted":false,"results":[],"resultsCount":1}','INVALID_SOURCE_COUNT'],
 ['{"status":"OK","adjusted":false,"results":[],"queryCount":-1}','INVALID_SOURCE_COUNT'],
 ['{"status":"OK","adjusted":false,"results":[],"queryCount":1.5}','INVALID_SOURCE_COUNT'],
 ['{"status":"OK","adjusted":false,"results":[],"queryCount":"0"}','INVALID_SOURCE_COUNT'],
 ['{"status":"OK","adjusted":false,"results":[],"queryCount":10000000000}','INVALID_SOURCE_COUNT'],
 ['{"status":"OK","status":"OK","adjusted":false,"results":[]}','DUPLICATE_SOURCE_KEY'],
 ['{"status":"OK","adjusted":false,"results":[{"T":"A","T":"B"}]}','DUPLICATE_SOURCE_KEY'],
 ['{"status":"OK","adjusted":false,"results":[{"nest":{"x":1,"\\u0078":2}}]}','DUPLICATE_SOURCE_KEY'],
 ['{"status":"OK","adjusted":false,"results":[{"note":"'+secret+'"}]}','SOURCE_CREDENTIAL_ECHO'],
];
for(const [body,code] of invalid)assert.throws(()=>context.validateEnvelope(body,secret),e=>e.code===code,code);
assert.throws(()=>context.validateEnvelope(JSON.stringify({status:'OK',adjusted:false,results:Array.from({length:50001},()=>({}))}),secret),e=>e.code==='SOURCE_RECORD_LIMIT');
assert.equal(context.validateRequestUrl(FIXED,'2026-08-03'),FIXED);
for(const url of [FIXED.replace('2026-08-03','2026-08-04'),FIXED.replace('adjusted=false','adjusted=true'),FIXED.replace('include_otc=false','include_otc=true'),FIXED+'&cursor=X',FIXED+'&ticker=NVDA',FIXED.replace('api.massive.com','evil.invalid'),FIXED+'&apiKey='+secret]){
 assert.throws(()=>context.validateRequestUrl(url,'2026-08-03'),e=>e.code==='UNADMITTED_SOURCE_URL');
}
await assert.rejects(()=>context.readLimited(new ReadableStream({start(c){c.enqueue(new Uint8Array(513));c.close();}}),512,Date.now()+1000),e=>e.code==='RESPONSE_TOO_LARGE');
const request=()=>new Request('https://owned.invalid/worker',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({attempt_id:'11111111-1111-4111-8111-111111111111',token:'a'.repeat(64)})});
let res=await handler(new Request('https://owned.invalid/worker',{method:'GET'}));assert.equal(res.status,405);assert.equal(dbCalls,0);assert.equal(fetchCalls,0);
res=await handler(new Request('https://owned.invalid/worker',{method:'POST',headers:{'Content-Type':'application/json'},body:'{"attempt_id":"bad","token":"bad"}'}));assert.equal(res.status,400);assert.equal(dbCalls,0);
env.SUPABASE_URL='https://another-project.invalid';res=await handler(request());assert.equal(res.status,409);assert.equal(dbCalls,0);env.SUPABASE_URL='https://oxzabweahkoimtevbbny.supabase.co';
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,0);assert.equal((await res.text()).includes('RAW_DB_DETAIL'),false);
claimAllowed=true;claim={...validClaim,state:'BLOCKED'};used=false;res=await handler(request());assert.equal(res.status,200);assert.equal(fetchCalls,0);
for(const patch of [{family_id:'N03'},{partition_key:'arbitrary'},{trading_date:'2026-08-04'},{trading_date:'2026-09-07'},{calendar_scope_id:'wrong-scope'},{calendar_date_list_sha256:'e'.repeat(64)},{calendar_manifest_sha256:'invalid'},{adjusted:true},{include_otc:true},{max_result_count:50001},{archive_key:'unbound'},{request_url:FIXED+'&ticker=AAPL'}]){
 claim={...validClaim,...patch};used=false;failure=null;res=await handler(request());assert.equal(res.status,200);assert.ok(['INVALID_CLAIM','UNADMITTED_SOURCE_URL'].includes(failure.code));assert.equal(failure.fetch_started,false);assert.equal(fetchCalls,0);
}
claim={...validClaim};used=false;responseText=raw;failure=null;
res=await handler(request());assert.equal(res.status,200);let out=await res.json();assert.equal(out.state,'COMPLETE');assert.equal(out.cohort_certified,false);assert.equal(fetchCalls,1);
assert.equal(committed.trading_date,'2026-08-03');assert.equal(committed.content_text,raw);assert.equal(committed.wire_bytes,Buffer.byteLength(raw));assert.equal(committed.source_sha256,createHash('sha256').update(raw).digest('hex'));assert.equal(committed.source_sha256,committed.wire_sha256);assert.equal(committed.result_count,1);
assert.equal('next_url' in committed,false);assert.equal('page_number' in committed,false);assert.equal(JSON.stringify(committed).includes(secret),false);assert.equal(JSON.stringify(out).includes(secret),false);
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,1);
used=false;responseText='{"status":"OK","adjusted":false,"results":[]}';committed=null;res=await handler(request());out=await res.json();assert.equal(out.state,'COMPLETE');assert.equal(out.cohort_certified,false);assert.equal(committed.result_count,0);assert.equal(fetchCalls,2);
for(const [body,code] of invalid.slice(0,15)){
 used=false;responseText=body;failure=null;const before=fetchCalls;res=await handler(request());assert.equal(res.status,200);assert.equal(failure.code,code);assert.equal(failure.failure_class,'PARSER');assert.equal(fetchCalls,before+1);
}
used=false;status=403;responseText='SECRET_ERROR_BODY_NOT_RETAINED';failure=null;res=await handler(request());assert.equal(failure.failure_class,'AUTHENTICATION');assert.equal(JSON.stringify(failure).includes(responseText),false);
used=false;status=429;failure=null;res=await handler(request());assert.equal(failure.failure_class,'RATE_LIMIT');
used=false;status=500;failure=null;res=await handler(request());assert.equal(failure.failure_class,'TRANSPORT_READ');assert.equal(failure.http_status,500);
used=false;status=200;declaredLength=8388609;failure=null;res=await handler(request());assert.equal(failure.code,'RESPONSE_TOO_LARGE');assert.equal(failure.received_bytes,0);declaredLength=null;
// Fatal UTF-8 decode and BOMs cannot silently change retained body bytes.
for(const bad of [new Uint8Array([0xc3,0x28]),new TextEncoder().encode('\ufeff{"status":"OK","adjusted":false,"results":[]}')]){
 used=false;responseBytes=bad;failure=null;committed=null;res=await handler(request());assert.equal(res.status,200);assert.equal(failure.failure_class,'PARSER');assert.equal(committed,null);
}
responseBytes=null;responseText=raw;used=false;commitFailure=true;failure=null;const beforeFailures=failureCalls,beforeFetches=fetchCalls;
res=await handler(request());assert.equal(res.status,503);out=await res.json();assert.equal(out.phase,'FINISH');assert.equal(out.receipt_recorded,false);assert.equal(failureCalls,beforeFailures);assert.equal(fetchCalls,beforeFetches+1);assert.equal(JSON.stringify(out).includes('RAW_DB_DETAIL'),false);
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,beforeFetches+1);commitFailure=false;
used=false;delete env.MASSIVE_API_KEY;failure=null;res=await handler(request());assert.equal(failure.code,'SOURCE_CREDENTIAL_UNAVAILABLE');assert.equal(failure.fetch_started,false);assert.equal(fetchCalls,beforeFetches+1);env.MASSIVE_API_KEY=secret;
used=false;fetchThrow=true;failure=null;res=await handler(request());assert.equal(failure.failure_class,'TRANSPORT_READ');assert.equal(failure.fetch_started,true);assert.equal(failure.http_status,null);assert.equal((await res.text()).includes('RAW_FETCH_SECRET'),false);
assert.ok(calls.some(q=>q.includes('d02_daily_commit_v2')));assert.equal(calls.some(q=>q.includes('massive_news')||q.includes('massive_actions')),false);
// Every frozen session and exact partition is accepted; no calendar expansion is inferred.
for(const date of calendar.dates){
 const candidate={...validClaim,trading_date:date,request_url:fixedFor(date),partition_key:'daily-source-'+date.replaceAll('-','')+'-raw-no-otc-v2'};
 context.validateClaim(candidate);assert.equal(context.validateRequestUrl(candidate.request_url,date),fixedFor(date));
}
for(const date of ['2026-08-01','2026-08-02','2026-08-08','2026-09-07','2026-09-12','2026-07-31','2026-10-01','2026-09-31','2026-08-03T00:00:00Z','2026-08-03/../../secrets']){
 assert.throws(()=>context.validateClaim({...validClaim,trading_date:date,request_url:fixedFor(date),partition_key:'daily-source-'+date.replaceAll('-','')+'-raw-no-otc-v2'}),e=>e.code==='INVALID_CLAIM');
 assert.throws(()=>context.requestUrlFor(date),e=>e.code==='INVALID_CLAIM');
}
// Real handler flow at both ends and an interior session uses exactly one bounded URL.
fetchThrow=false;status=200;responseText=raw;failure=null;
for(const date of ['2026-08-04','2026-09-08','2026-09-30']){
 claim={...validClaim,trading_date:date,request_url:fixedFor(date),partition_key:'daily-source-'+date.replaceAll('-','')+'-raw-no-otc-v2'};
 used=false;committed=null;const before=fetchCalls;res=await handler(request());assert.equal(res.status,200);out=await res.json();
 assert.equal(fetchCalls,before+1);assert.equal(committed.trading_date,date);assert.equal(out.trading_date,date);assert.equal(committed.request_url,fixedFor(date));
 assert.equal(committed.content_text,raw);assert.equal(committed.source_sha256,committed.wire_sha256);
}
console.log('PASS_LOCAL_D02_DAILY_V2_FROZEN_42_SESSIONS_AUTH_REPLAY_EXACT_BYTES_COUNTS_ADJUSTMENT_NO_CONTINUATION_LIMITS_AND_UNKNOWN_EFFECT');
