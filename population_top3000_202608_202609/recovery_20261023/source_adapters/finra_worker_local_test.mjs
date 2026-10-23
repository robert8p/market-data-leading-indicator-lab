// Local mock test only: no real secrets, database calls, or network access.
import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import {stripTypeScriptTypes} from 'node:module';
import {webcrypto,createHash} from 'node:crypto';
const input=fs.readFileSync(new URL('./population-top3000-evidence-v1_DRAFT.ts',import.meta.url),'utf8')
 .replace(/^import postgres from "npm:postgres@3\.4\.5";$/m,'const postgres=globalThis.mockPostgres;');
const source=stripTypeScriptTypes(input,{mode:'transform'});
const env={SUPABASE_URL:'https://oxzabweahkoimtevbbny.supabase.co',SUPABASE_DB_URL:'postgres://synthetic-fixture/unused'};
let handler;let connections=0;let fetches=0;let claimed=false;let allowed=false;let receipt=null;let httpStatus=200;let poisonUrl=false;let failSave=false;
const raw='Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\r\n20260803|SYNTH|10.125|0.125|20.25|B,Q,N\r\n1\r\n';
function mockPostgres(_url,options){connections++;assert.equal(options.max,1);assert.equal(options.prepare,false);
 const tx=async(strings,...values)=>{const query=strings.join('');
  if(query.includes('finra_claim_v1')){if(!allowed||claimed)return [{result:{state:'DENIED'}}];claimed=true;
   return [{result:{state:'CLAIMED',session_date:'2026-08-03',source_uri:poisonUrl?'https://untrusted.invalid/file':'https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260803.txt',
    max_response_bytes:8388608,provider_timeout_ms:25000,total_timeout_ms:45000}}];}
  if(query.includes('finra_receive_v1')){if(failSave)throw new Error('PRIVATE_ERROR_MUST_NOT_LEAK');
   assert.equal(typeof values[2],'object');assert.equal(values[2].__json,true);receipt=values[2].object;
   return [{result:{state:receipt.outcome==='RECEIVED'?'COMPLETE':'BLOCKED'}}];}return [];};
 tx.json=(object)=>{assert.equal(typeof object,'object');assert.equal(Array.isArray(object),false);return {__json:true,object};};
 return {begin:async(fn)=>fn(tx),end:()=>Promise.resolve()};
}
const context=vm.createContext({mockPostgres,Deno:{env:{get:n=>env[n]},serve:fn=>{handler=fn;}},crypto:webcrypto,
 TextEncoder,TextDecoder,Uint8Array,ArrayBuffer,AbortSignal,DOMException,Request,Response,ReadableStream,setTimeout,clearTimeout,
 fetch:async(url,options)=>{fetches++;assert.equal(claimed,true);assert.equal(url,'https://cdn.finra.org/equity/regsho/daily/CNMSshvol20260803.txt');
  assert.equal(options.redirect,'error');assert.equal(options.method,'GET');return new Response(raw,{status:httpStatus,headers:{'content-type':'text/plain','etag':'synthetic-fixture'}});}});
vm.runInContext(source,context,{filename:'finra-worker-transpiled.js'});
const body=JSON.stringify({attempt_id:'11111111-1111-4111-8111-111111111111',token:'b'.repeat(64)});
const request=(payload=body)=>new Request('https://owned.invalid/worker',{method:'POST',headers:{'content-type':'application/json'},body:payload});
let response=await handler(request('{"attempt_id":"bad"}'));assert.equal(response.status,400);assert.equal(connections,0);assert.equal(fetches,0);
response=await handler(request());assert.equal(response.status,403);assert.equal(fetches,0);
allowed=true;response=await handler(request());assert.equal(response.status,200);assert.equal(fetches,1);assert.equal(receipt.content_text,raw);
assert.equal(receipt.source_sha256,createHash('sha256').update(raw).digest('hex'));assert.equal(receipt.content_bytes,Buffer.byteLength(raw));
assert.equal(receipt.content_text.includes('10.125|0.125|20.25'),true);assert.equal(receipt.headers.etag,'synthetic-fixture');
response=await handler(request());assert.equal(response.status,403);assert.equal(fetches,1);
claimed=false;poisonUrl=true;response=await handler(request());assert.equal(response.status,409);assert.equal(fetches,1);
claimed=false;poisonUrl=false;httpStatus=404;response=await handler(request());assert.equal(response.status,200);assert.equal(receipt.outcome,'FAILED');
assert.equal(receipt.error_code,'HTTP_ERROR');assert.equal(receipt.http_status,404);assert.equal(receipt.content_text,undefined);assert.equal(fetches,2);
claimed=false;httpStatus=200;failSave=true;response=await handler(request());assert.equal(response.status,503);const failure=await response.json();
assert.equal(failure.requires_reconciliation,true);assert.equal(failure.automatic_retry,false);assert.equal(JSON.stringify(failure).includes('PRIVATE_ERROR'),false);assert.equal(fetches,3);
response=await handler(request());assert.equal(response.status,403);assert.equal(fetches,3);
await assert.rejects(()=>context.readLimited(new ReadableStream({start(c){c.enqueue(new Uint8Array(513));c.close();}}),512,Date.now()+1000,AbortSignal.timeout(1000)),e=>e.code==='RESPONSE_TOO_LARGE');
assert.equal(context.retryAfter('120'),120);assert.equal(context.retryAfter('invalid'),0);assert.equal(context.retryAfter('999999'),86400);
assert.throws(()=>context.fixedUrl('2026-08-03/../../evil'),e=>e.code==='CLAIM_SCOPE_INVALID');
console.log('PASS_FINRA_LOCAL_AUTH_FIXED_URL_BYTE_HASH_DECIMALS_JSON_OBJECT_SINGLE_REQUEST_404_AND_UNKNOWN_RECEIPT');
