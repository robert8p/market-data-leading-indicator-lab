// LOCAL mocked source/DB only: no network, credentials, deployments or production data.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import {stripTypeScriptTypes} from 'node:module';
import {webcrypto,createHash} from 'node:crypto';
const file=new URL('./population-top3000-bars-v1_DRAFT.ts',import.meta.url);
const input=fs.readFileSync(file,'utf8').replace(/^import postgres from "npm:postgres@3\.4\.5";$/m,'const postgres=globalThis.mockPostgres;');
const code=stripTypeScriptTypes(input,{mode:'strip'});
const key='NON_SECRET_ALPACA_FIXTURE_ID_89',secret='NON_SECRET_ALPACA_FIXTURE_SECRET_73';
const env={SUPABASE_URL:'https://oxzabweahkoimtevbbny.supabase.co',SUPABASE_DB_URL:'postgres://mock.invalid/no-connection',ALPACA_API_KEY:key,ALPACA_API_SECRET:secret};
const spec={symbols:['AAPL','NVDA'],start_utc:'2026-09-08T04:00:00Z',end_exclusive_utc:'2026-09-09T04:00:00Z',end_inclusive_utc:'2026-09-09T03:59:59.999999Z'};
function requestURL(token=null){const u=new URL('https://data.alpaca.markets/v2/stocks/bars');for(const [k,v] of Object.entries({symbols:spec.symbols.join(','),timeframe:'1Min',start:spec.start_utc,end:spec.end_inclusive_utc,limit:'10000',adjustment:'raw',feed:'sip',sort:'asc',asof:'-',currency:'USD'}))u.searchParams.set(k,v);if(token!==null)u.searchParams.set('page_token',token);return u.toString();}
let claim={state:'RESERVED',archive_key:'alpaca-fixture',page_number:1,request_url:requestURL(),request_token:null,request_spec:spec,max_response_bytes:8388608,max_runtime_ms:45000,max_bars:10000,family_id:'P01',partition_key:'alpaca-sip-gap:'+ '1'.repeat(64)};
let handler,dbCalls=0,fetchCalls=0,allowClaim=false,used=false,claimState='RESERVED',commitFailure=false,committed=null,failure=null;
let httpStatus=200,responseText='',declaredLength=null,fetchThrow=false,requestSeen=null;
function mockPostgres(url,options){assert.equal(url,env.SUPABASE_DB_URL);assert.equal(options.max,1);assert.equal(options.prepare,false);dbCalls++;return {
 begin:async fn=>{const tx=async(strings,...values)=>{const q=strings.join('');
  if(q.includes('alpaca_sip_claim_v1')){if(!allowClaim||used)throw new Error('SECRET_SQL_ERROR_NEVER_EXPOSE');used=true;return [{result:{...claim,state:claimState}}];}
  if(q.includes('alpaca_sip_commit_v1')){assert.equal(values[2].type,'json-object');if(commitFailure)throw new Error('SECRET_SQL_ERROR_NEVER_EXPOSE');committed=values[2].value;return [{result:{state:committed.next_page_token===null?'COMPLETE':'CONTINUE'}}];}
  if(q.includes('alpaca_sip_failure_v1')){assert.equal(values[2].type,'json-object');failure=values[2].value;return [{result:{state:'BLOCKED'}}];}return [];
 };tx.json=value=>{assert.equal(typeof value,'object');assert.notEqual(value,null);return {type:'json-object',value};};return fn(tx);},end:()=>Promise.resolve()};}
const ctx=vm.createContext({mockPostgres,Deno:{env:{get:n=>env[n]},serve:fn=>{handler=fn;}},crypto:webcrypto,TextEncoder,TextDecoder,Uint8Array,ArrayBuffer,AbortSignal,DOMException,Request,Response,ReadableStream,URL,setTimeout,clearTimeout,
 fetch:async(url,options)=>{fetchCalls++;requestSeen={url,options};assert.equal(options.redirect,'error');const u=new URL(url);assert.equal(u.origin,'https://data.alpaca.markets');assert.equal(u.pathname,'/v2/stocks/bars');assert.equal(options.headers['APCA-API-KEY-ID'],key);assert.equal(options.headers['APCA-API-SECRET-KEY'],secret);assert.equal(url.includes(key)||url.includes(secret),false);if(fetchThrow)throw new Error('SECRET_FETCH_ERROR_NEVER_EXPOSE');const headers={'x-request-id':'fixture-request'};if(declaredLength!==null)headers['content-length']=String(declaredLength);return new Response(responseText,{status:httpStatus,headers});}});
vm.runInContext(code,ctx,{filename:'alpaca-bars-worker-transpiled.js'});
const raw=' {"bars":{"AAPL":[{"t":"2026-09-08T13:30:00Z","o":100.12345678901234567890,"h":101,"l":99,"c":100,"v":17.125,"n":9007199254740993,"vw":100.125}],"NVDA":[{"t":"2026-09-08T20:00:00Z","o":100,"h":101,"l":99,"c":100,"v":0,"n":0,"vw":100}]},"next_page_token":"Ab+/="} ';
const clean=ctx.sanitizeEnvelope(raw,[key,secret],spec.symbols);
assert.equal(clean.content_text,raw);assert.equal(clean.result_count,2);assert.equal(clean.next_page_token,'Ab+/=');
assert.ok(clean.content_text.includes('100.12345678901234567890'));assert.ok(clean.content_text.includes('9007199254740993'));
assert.equal(ctx.validateRequestUrl(requestURL(),claim),requestURL());
for(const bad of [requestURL().replace('data.alpaca.markets','evil.invalid'),requestURL().replace('feed=sip','feed=iex'),requestURL().replace('asof=-','asof=2026-08-03'),requestURL()+'&feed=sip',requestURL()+'&apiKey=bad'])assert.throws(()=>ctx.validateRequestUrl(bad,claim));
assert.throws(()=>ctx.sanitizeEnvelope('{"bars":{"AAPL":[{"c":1,"c":2}]},"next_page_token":null}',[key,secret],['AAPL']),e=>e.code==='DUPLICATE_SOURCE_KEY');
assert.throws(()=>ctx.sanitizeEnvelope('{"bars":{"OTHER":[]},"next_page_token":null}',[key,secret],['AAPL']),e=>e.code==='INVALID_SOURCE_SYMBOL');
assert.throws(()=>ctx.sanitizeEnvelope('{"bars":{}}',[key,secret],['AAPL']),e=>e.code==='INVALID_SOURCE_ENVELOPE');
assert.throws(()=>ctx.sanitizeEnvelope('{"bars":null,"next_page_token":null}',[key,secret],['AAPL']),e=>e.code==='INVALID_SOURCE_ENVELOPE');
assert.throws(()=>ctx.sanitizeEnvelope('{"bars":{},"next_page_token":""}',[key,secret],['AAPL']),e=>e.code==='INVALID_PAGE_TOKEN');
assert.throws(()=>ctx.sanitizeEnvelope('{"bars":{"AAPL":[{"debug":"'+secret+'"}]},"next_page_token":null}',[key,secret],['AAPL']),e=>e.code==='SOURCE_CREDENTIAL_ECHO');
assert.throws(()=>ctx.sanitizeEnvelope(JSON.stringify({bars:{AAPL:Array.from({length:6000},()=>({})),NVDA:Array.from({length:4001},()=>({}))},next_page_token:null}),[key,secret],spec.symbols),e=>e.code==='INVALID_SOURCE_COUNT');
assert.equal(ctx.sanitizeEnvelope('{"bars":{},"next_page_token":null}',[key,secret],['AAPL']).result_count,0);
await assert.rejects(()=>ctx.readLimited(new ReadableStream({start(c){c.enqueue(new Uint8Array(513));c.close();}}),512,Date.now()+1000),e=>e.code==='RESPONSE_TOO_LARGE');
const req=()=>new Request('https://owned.invalid/worker',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({attempt_id:'11111111-1111-4111-8111-111111111111',token:'a'.repeat(64)})});
let res=await handler(new Request('https://owned.invalid/worker',{method:'GET'}));assert.equal(res.status,405);assert.equal(dbCalls,0);assert.equal(fetchCalls,0);
res=await handler(req());assert.equal(res.status,403);assert.equal(fetchCalls,0);assert.equal((await res.text()).includes('SECRET_SQL_ERROR'),false);
allowClaim=true;claimState='BLOCKED';used=false;res=await handler(req());assert.equal(res.status,200);assert.equal(fetchCalls,0);
claimState='RESERVED';used=false;responseText=raw;res=await handler(req());assert.equal(res.status,200);let out=await res.json();assert.equal(out.state,'CONTINUE');assert.equal(fetchCalls,1);assert.equal(out.result_count,2);assert.equal(out.has_next_page,true);assert.equal(committed.content_text,raw);
assert.equal(committed.wire_sha256,createHash('sha256').update(raw).digest('hex'));assert.equal(committed.source_sha256,committed.wire_sha256);assert.equal(committed.wire_bytes,Buffer.byteLength(raw));assert.equal(JSON.stringify(committed).includes(key)||JSON.stringify(committed).includes(secret),false);
res=await handler(req());assert.equal(res.status,403);assert.equal(fetchCalls,1);
// A short page with a continuation must still continue; then a retained opaque cursor is used once.
used=false;claim={...claim,page_number:2,request_token:'Ab+/=',request_url:requestURL('Ab+/=')};responseText='{"bars":{},"next_page_token":null}';committed=null;
res=await handler(req());assert.equal(res.status,200);assert.equal((await res.json()).state,'COMPLETE');assert.equal(fetchCalls,2);assert.equal(new URL(requestSeen.url).searchParams.get('page_token'),'Ab+/=');
// Credential configuration is discovered only inside an admitted real task, never by a separate probe.
used=false;delete env.ALPACA_API_SECRET;failure=null;res=await handler(req());out=await res.json();assert.equal(fetchCalls,2);assert.equal(out.code,'MissingProviderKey');assert.equal(failure.code,'MISSING_PROVIDER_KEY');assert.equal(failure.fetch_started,false);assert.equal(JSON.stringify(out).includes('ALPACA_API'),false);env.ALPACA_API_SECRET=secret;
used=false;httpStatus=403;failure=null;responseText='DO_NOT_RETAIN_PROVIDER_AUTH_BODY';res=await handler(req());assert.equal(failure.failure_class,'AUTHENTICATION');assert.equal(fetchCalls,3);assert.equal(JSON.stringify(failure).includes(responseText),false);
used=false;httpStatus=429;failure=null;res=await handler(req());assert.equal(failure.failure_class,'RATE_LIMIT');assert.equal(fetchCalls,4);
used=false;httpStatus=200;declaredLength=8388609;responseText='{"bars":{},"next_page_token":null}';failure=null;res=await handler(req());assert.equal(failure.code,'RESPONSE_TOO_LARGE');assert.equal(failure.wire_sha256,null);assert.equal(failure.fetch_started,true);assert.equal(fetchCalls,5);declaredLength=null;
used=false;commitFailure=true;failure=null;res=await handler(req());assert.equal(res.status,503);out=await res.json();assert.equal(out.phase,'FINISH');assert.equal(out.receipt_recorded,false);assert.equal(failure,null);assert.equal(fetchCalls,6);res=await handler(req());assert.equal(res.status,403);assert.equal(fetchCalls,6);commitFailure=false;
used=false;fetchThrow=true;failure=null;res=await handler(req());assert.equal(failure.code,'FETCH_FAILED');assert.equal(failure.failure_class,'TRANSPORT_READ');assert.equal(fetchCalls,7);assert.equal((await res.text()).includes('SECRET_FETCH_ERROR'),false);fetchThrow=false;
// Supported alias pair remains internal, and a malformed source token never causes a second fetch.
used=false;delete env.ALPACA_API_KEY;delete env.ALPACA_API_SECRET;env.APCA_API_KEY_ID=key;env.APCA_API_SECRET_KEY=secret;failure=null;responseText='{"bars":{},"next_page_token":"Ab+/="}';res=await handler(req());assert.equal(failure.code,'REPEATED_PAGE_CURSOR');assert.equal(fetchCalls,8);
console.log('PASS_LOCAL_ALPACA_FIXED_SIP_RAW_ASOF_CURRENCY_SCOPE_TOTAL_LIMIT_EXACT_NUMBERS_OPAQUE_PAGINATION_AUTH_REPLAY_FAILURES_JSON_DRIVER');
