// Local-only security/data-integrity tests. Uses fake DB and fetch; no real secrets or network.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import {stripTypeScriptTypes} from 'node:module';
import {webcrypto,createHash} from 'node:crypto';
const input=fs.readFileSync(new URL('./population-top3000-actions-n02-expansion_DRAFT.ts',import.meta.url),'utf8').replace(/^import postgres from "npm:postgres@3\.4\.5";$/m,'const postgres=globalThis.mockPostgres;');
const source=stripTypeScriptTypes(input,{mode:'strip'});
const FIRST='https://api.massive.com/stocks/v1/splits?execution_date.gte=2026-08-01&execution_date.lte=2026-09-30&sort=execution_date.asc&limit=1000';
const secret='NON_SECRET_MASSIVE_FIXTURE_89';
const env={SUPABASE_URL:'https://oxzabweahkoimtevbbny.supabase.co',SUPABASE_DB_URL:'postgres://mock.invalid/never-connect',MASSIVE_API_KEY:secret};
let handler,dbCalls=0,fetchCalls=0,claimAllowed=false,used=false,claimState='RESERVED',commitFailure=false,committed=null,failure=null;
let status=200,responseText='',declaredLength=null,fetchThrow=false,requestSeen=null;
let ctx={state:'RESERVED',archive_key:'fixture-archive',page_number:1,request_url:FIRST,max_response_bytes:8388608,max_runtime_ms:45000,family_id:'N03',partition_key:'splits-execution-202608-202609'};
function mockPostgres(url,options){
 assert.equal(url,env.SUPABASE_DB_URL);assert.equal(options.max,1);assert.equal(options.prepare,false);dbCalls++;
 return {begin:async(fn)=>{
  const tx=async(strings,...values)=>{const q=strings.join('');
   if(q.includes('massive_actions_route_claim_v2')){if(!claimAllowed||used)throw Object.assign(new Error('DO_NOT_RETURN_RAW_ERROR'),{code:'P0001'});used=true;return [{result:{...ctx,state:claimState}}];}
   if(q.includes('massive_actions_route_commit_v2')){assert.equal(values[2].kind,'json-wrapper');if(commitFailure)throw Object.assign(new Error('DO_NOT_RETURN_RAW_ERROR'),{code:'55P03'});committed=values[2].value;return [{result:{state:committed.next_url?'CONTINUE':'COMPLETE'}}];}
   if(q.includes('massive_actions_route_failure_v2')){assert.equal(values[2].kind,'json-wrapper');failure=values[2].value;return [{result:{state:'BLOCKED'}}];}
   return [];
  };
  tx.json=(value)=>{assert.equal(typeof value,'object');assert.notEqual(value,null);return {kind:'json-wrapper',value};};
  return fn(tx);
 },end:()=>Promise.resolve()};
}
const context=vm.createContext({mockPostgres,Deno:{env:{get:n=>env[n]},serve:fn=>{handler=fn;}},crypto:webcrypto,TextEncoder,TextDecoder,Uint8Array,ArrayBuffer,AbortSignal,DOMException,Request,Response,ReadableStream,URL,setTimeout,clearTimeout,
 fetch:async(url,options)=>{fetchCalls++;requestSeen={url,options};assert.equal(options.redirect,'error');const u=new URL(url);assert.equal(u.origin,'https://api.massive.com');assert.equal(u.pathname,ctx.family_id==='N02'?'/v3/reference/dividends':'/stocks/v1/splits');assert.equal(u.searchParams.get('apiKey'),secret);if(fetchThrow)throw new Error('RAW_FETCH_SECRET_DO_NOT_RETURN');const headers={};if(declaredLength!==null)headers['content-length']=String(declaredLength);return new Response(responseText,{status,headers});}});
vm.runInContext(source,context,{filename:'actions-worker-transpiled.js'});
const raw=' { "status":"OK", "results":[{"id":"fixture1","ticker":"TEST","execution_date":"2026-08-03","split_from":1,"split_to":2,"adjustment_type":"forward_split","large_integer":9007199254740993}], "next_url":"https://api.massive.com/stocks/v1/splits?cursor=NEXT%3D&apiKey='+secret+'", "request_id":"fixture-request" } ';
const clean=context.sanitizeEnvelope(raw,secret);
assert.equal(clean.content_text.includes(secret),false);
assert.equal(clean.content_text.includes('9007199254740993'),true);
assert.equal(clean.next_url,'https://api.massive.com/stocks/v1/splits?cursor=NEXT%3D');
assert.equal(clean.content_text,raw.replace('"https://api.massive.com/stocks/v1/splits?cursor=NEXT%3D&apiKey='+secret+'"','"https://api.massive.com/stocks/v1/splits?cursor=NEXT%3D"'));
assert.throws(()=>context.canonicalNext('https://evil.invalid/stocks/v1/splits?cursor=x'),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.canonicalNext('https://api.massive.com/v3/reference/tickers?cursor=x'),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.canonicalNext('https://api.massive.com/stocks/v1/splits?cursor=x&execution_date.gte=2020-01-01'),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.canonicalNext('https://u:p@api.massive.com/stocks/v1/splits?cursor=x'),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.canonicalNext('https://api.massive.com/stocks/v1/splits?cursor=x&cursor=y'),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.validateRequestUrl('https://api.massive.com/stocks/v1/splits?cursor=x&apiKey=not-retainable'),e=>e.code==='UNADMITTED_SOURCE_URL');
assert.throws(()=>context.sanitizeEnvelope('{"status":"OK","status":"OK","results":[]}',secret),e=>e.code==='DUPLICATE_SOURCE_KEY');
assert.throws(()=>context.sanitizeEnvelope('{"status":"OK","results":[{"nested":{"credential":"'+secret+'"}}]}',secret),e=>e.code==='SOURCE_CREDENTIAL_ECHO');
assert.throws(()=>context.sanitizeEnvelope(JSON.stringify({status:'OK',results:Array.from({length:1001},()=>({}))}),secret),e=>e.code==='INVALID_SOURCE_ENVELOPE');
await assert.rejects(()=>context.readLimited(new ReadableStream({start(c){c.enqueue(new Uint8Array(513));c.close();}}),512,Date.now()+1000),e=>e.code==='RESPONSE_TOO_LARGE');
const request=()=>new Request('https://owned.invalid/worker',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({attempt_id:'11111111-1111-4111-8111-111111111111',token:'a'.repeat(64)})});
let res=await handler(new Request('https://owned.invalid/worker',{method:'GET'}));assert.equal(res.status,405);assert.equal(dbCalls,0);assert.equal(fetchCalls,0);
res=await handler(new Request('https://owned.invalid/worker',{method:'POST',headers:{'Content-Type':'application/json'},body:'{"attempt_id":"bad","token":"bad"}'}));assert.equal(res.status,400);assert.equal(dbCalls,0);
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,0);assert.equal((await res.text()).includes('DO_NOT_RETURN_RAW_ERROR'),false);
claimAllowed=true;claimState='BLOCKED';used=false;res=await handler(request());assert.equal(res.status,200);assert.equal(fetchCalls,0);
claimState='RESERVED';used=false;responseText=raw;
res=await handler(request());assert.equal(res.status,200);let out=await res.json();assert.equal(out.state,'CONTINUE');assert.equal(fetchCalls,1);assert.equal(out.has_next_page,true);assert.equal(committed.content_text,clean.content_text);
assert.equal(committed.wire_sha256,createHash('sha256').update(raw).digest('hex'));assert.equal(committed.source_sha256,createHash('sha256').update(clean.content_text).digest('hex'));assert.notEqual(committed.wire_sha256,committed.source_sha256);
assert.equal(JSON.stringify(committed).includes(secret),false);assert.equal(JSON.stringify(out).includes(secret),false);
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,1);
// Page two uses retained continuation; one invocation never fetches two pages.
used=false;ctx={...ctx,page_number:2,request_url:clean.next_url};responseText='{"status":"OK","results":[]}';committed=null;
res=await handler(request());assert.equal(res.status,200);assert.equal((await res.json()).state,'COMPLETE');assert.equal(fetchCalls,2);assert.equal(committed.page_number,2);assert.equal(committed.next_url,null);
// Unsupported cursor causes a persisted parser failure, never a redirected request.
used=false;ctx={...ctx,page_number:1,request_url:FIRST};responseText='{"status":"OK","results":[],"next_url":"https://evil.invalid/leak"}';failure=null;
res=await handler(request());assert.equal(res.status,200);assert.equal(failure.failure_class,'PARSER');assert.equal(failure.code,'INVALID_NEXT_URL');assert.equal(fetchCalls,3);
// Auth and rate-limit failures do not self-retry and retain classified receipt only.
used=false;status=403;responseText='SECRET_ERROR_BODY_NOT_RETAINED';failure=null;
res=await handler(request());assert.equal(res.status,200);assert.equal(failure.failure_class,'AUTHENTICATION');assert.equal(fetchCalls,4);assert.equal(JSON.stringify(failure).includes(responseText),false);
used=false;status=429;failure=null;res=await handler(request());assert.equal(failure.failure_class,'RATE_LIMIT');assert.equal(fetchCalls,5);
// Oversized response declared before body consumption.
used=false;status=200;declaredLength=8388609;responseText='{"status":"OK","results":[]}';failure=null;
res=await handler(request());assert.equal(failure.code,'RESPONSE_TOO_LARGE');assert.equal(failure.received_bytes,0);assert.equal(fetchCalls,6);declaredLength=null;
// Unknown commit result: never post a second finish/failure and never refetch in that attempt.
used=false;commitFailure=true;failure=null;res=await handler(request());assert.equal(res.status,503);out=await res.json();assert.equal(out.phase,'FINISH');assert.equal(out.receipt_recorded,false);assert.equal(failure,null);assert.equal(fetchCalls,7);assert.equal(JSON.stringify(out).includes('DO_NOT_RETURN_RAW_ERROR'),false);
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,7);commitFailure=false;
// No configured key causes no fetch, but releases/resolves admitted attempt through failure RPC.
used=false;delete env.MASSIVE_API_KEY;failure=null;res=await handler(request());assert.equal(fetchCalls,7);assert.equal(failure.code,'SOURCE_CREDENTIAL_UNAVAILABLE');assert.equal(failure.failure_class,'AUTHENTICATION');env.MASSIVE_API_KEY=secret;
used=false;fetchThrow=true;failure=null;res=await handler(request());assert.equal(failure.failure_class,'TRANSPORT_READ');assert.equal(fetchCalls,8);assert.equal((await res.text()).includes('RAW_FETCH_SECRET'),false);

// Existing N03 behavior has passed unchanged through the new route wrappers above.
// N02 has two explicitly admitted query contracts, and absent status is accepted only with results[].
fetchThrow=false;used=false;status=200;failure=null;committed=null;
const exPartition='dividends-exdate-202608-202609';
const declarationPartition='dividends-declaration-202608-202609';
const EX='https://api.massive.com/v3/reference/dividends?ex_dividend_date.gte=2026-08-01&ex_dividend_date.lte=2026-09-30&order=asc&sort=ex_dividend_date&limit=1000';
const DECLARATION='https://api.massive.com/v3/reference/dividends?declaration_date.gte=2026-08-01&declaration_date.lte=2026-09-30&order=asc&sort=ex_dividend_date&limit=1000';
const exRoute=context.routeForClaim({family_id:'N02',partition_key:exPartition});
const declarationRoute=context.routeForClaim({family_id:'N02',partition_key:declarationPartition});
assert.equal(context.validateRequestUrl(EX,exRoute),EX);
assert.equal(context.validateRequestUrl(DECLARATION,declarationRoute),DECLARATION);
for(const invalid of ['{}','{"status":"OK"}','{"results":null}','{"results":{}}','{"status":null,"results":[]}','{"status":"ERROR","results":[]}']){
 assert.throws(()=>context.sanitizeEnvelope(invalid,secret,exRoute),e=>e.code==='INVALID_SOURCE_ENVELOPE');
}
assert.equal(context.sanitizeEnvelope('{"results":[]}',secret,exRoute).result_count,0);
assert.equal(context.sanitizeEnvelope('{"status":"OK","results":[]}',secret,declarationRoute).result_count,0);
assert.throws(()=>context.sanitizeEnvelope('{"results":[]}',secret),e=>e.code==='INVALID_SOURCE_ENVELOPE');
assert.throws(()=>context.sanitizeEnvelope('{"results":[],"results":[]}',secret,exRoute),e=>e.code==='DUPLICATE_SOURCE_KEY');
assert.throws(()=>context.canonicalNext('https://api.massive.com/stocks/v1/splits?cursor=x',exRoute),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.canonicalNext('https://api.massive.com/v3/reference/dividends?cursor=x'),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.canonicalNext('https://api.massive.com/v3/reference/dividends?cursor=x&declaration_date.gte=2026-08-01',exRoute),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.canonicalNext('https://api.massive.com/v3/reference/dividends?cursor=x&ex_dividend_date.gte=2026-08-01',declarationRoute),e=>e.code==='INVALID_NEXT_URL');
assert.throws(()=>context.canonicalNext('https://api.massive.com/v3/reference/dividends?cursor=x&declaration_date.gte=2025-08-01',declarationRoute),e=>e.code==='INVALID_NEXT_URL');
const dividendRaw=' {"results":[{"id":"fixture-dividend-1","ticker":"TEST","declaration_date":"2026-07-31","ex_dividend_date":"2026-08-03","cash_amount":1.2345678901234567890123456789,"large_integer":9007199254740993}], "next_url":"https://api.massive.com/v3/reference/dividends?cursor=N02%3D&ex_dividend_date.gte=2026-08-01&ex_dividend_date.lte=2026-09-30&order=asc&sort=ex_dividend_date&limit=1000&apiKey='+secret+'"} ';
const dividendClean=context.sanitizeEnvelope(dividendRaw,secret,exRoute);
assert.equal(dividendClean.content_text.includes('1.2345678901234567890123456789'),true);
assert.equal(dividendClean.content_text.includes('9007199254740993'),true);
assert.equal(dividendClean.content_text.includes(secret),false);
assert.equal(dividendClean.next_url,'https://api.massive.com/v3/reference/dividends?cursor=N02%3D');
const dividendNextOriginal=JSON.parse(dividendRaw).next_url;
assert.equal(dividendClean.content_text,dividendRaw.replace(JSON.stringify(dividendNextOriginal),JSON.stringify(dividendClean.next_url)));
// Ex-date page: exactly one fixed-source request, one typed JSON commit, precise stored numeric lexemes.
ctx={...ctx,family_id:'N02',partition_key:exPartition,request_url:EX,page_number:1};responseText=dividendRaw;
const beforeEx=fetchCalls;res=await handler(request());out=await res.json();
assert.equal(res.status,200);assert.equal(out.state,'CONTINUE');assert.equal(fetchCalls,beforeEx+1);
assert.equal(committed.content_text,dividendClean.content_text);
assert.equal(committed.wire_sha256,createHash('sha256').update(dividendRaw).digest('hex'));
assert.equal(committed.source_sha256,createHash('sha256').update(dividendClean.content_text).digest('hex'));
assert.equal(new URL(requestSeen.url).searchParams.get('ex_dividend_date.gte'),'2026-08-01');
assert.equal(new URL(requestSeen.url).searchParams.has('declaration_date.gte'),false);
res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,beforeEx+1);
// Opaque cursor scope is provided by the previously committed DB chain; it cannot switch route.
used=false;committed=null;ctx={...ctx,page_number:2,request_url:dividendClean.next_url};responseText='{"results":[]}';
res=await handler(request());assert.equal((await res.json()).state,'COMPLETE');assert.equal(fetchCalls,beforeEx+2);
assert.equal(committed.page_number,2);assert.equal(committed.next_url,null);
// Declaration-date union source uses its own fixed filter and accepts an explicit terminal empty page.
used=false;committed=null;ctx={...ctx,partition_key:declarationPartition,request_url:DECLARATION,page_number:1};responseText='{"status":"OK","results":[]}';
const beforeDeclaration=fetchCalls;res=await handler(request());assert.equal((await res.json()).state,'COMPLETE');
assert.equal(fetchCalls,beforeDeclaration+1);assert.equal(new URL(requestSeen.url).searchParams.get('declaration_date.gte'),'2026-08-01');
assert.equal(new URL(requestSeen.url).searchParams.has('ex_dividend_date.gte'),false);
// A missing results array is retained only as a failure receipt; it is never committed as zero observations.
used=false;committed=null;failure=null;responseText='{"status":"OK"}';
res=await handler(request());assert.equal(res.status,200);assert.equal(committed,null);assert.equal(failure.code,'INVALID_SOURCE_ENVELOPE');
// Contract mismatch fails before any provider fetch; no arbitrary route or cross-partition first URL.
used=false;failure=null;ctx={...ctx,partition_key:exPartition,request_url:DECLARATION};const beforeMismatch=fetchCalls;
res=await handler(request());assert.equal(res.status,200);assert.equal(fetchCalls,beforeMismatch);assert.equal(failure.code,'INVALID_NEXT_URL');
used=false;failure=null;ctx={...ctx,family_id:'N01',partition_key:'unknown'};
res=await handler(request());assert.equal(res.status,200);assert.equal(fetchCalls,beforeMismatch);assert.equal(failure.code,'INVALID_CLAIM');
// A known N02 page with an unacknowledged DB commit never produces a second source request or failure mutation.
used=false;failure=null;ctx={...ctx,family_id:'N02',partition_key:exPartition,request_url:EX};responseText='{"results":[]}';commitFailure=true;
res=await handler(request());out=await res.json();assert.equal(res.status,503);assert.equal(out.phase,'FINISH');assert.equal(failure,null);
const afterUnknown=fetchCalls;res=await handler(request());assert.equal(res.status,403);assert.equal(fetchCalls,afterUnknown);
assert.equal(JSON.stringify(out).includes(secret),false);assert.equal(JSON.stringify(out).includes('DO_NOT_RETURN_RAW_ERROR'),false);
console.log('PASS_LOCAL_N03_UNCHANGED_AND_N02_TWO_FIXED_ROUTES_EXACT_VALUES_EMPTY_PAGE_RULES_AUTH_REPLAY_AND_UNKNOWN_COMMIT');

