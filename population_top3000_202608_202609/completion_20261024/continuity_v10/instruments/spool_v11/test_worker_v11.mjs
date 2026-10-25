import fs from 'node:fs';
import vm from 'node:vm';
import assert from 'node:assert/strict';
import {stripTypeScriptTypes} from 'node:module';
import {webcrypto} from 'node:crypto';
const original=fs.readFileSync(new URL('./core_v11.ts',import.meta.url),'utf8');
const source=stripTypeScriptTypes(original.replace('export function buildWorker','function buildWorker'));
const attempt='11111111-1111-4111-8111-111111111111',token='a'.repeat(64);
const raw='{"status":"OK","results":[{"ticker":"TEST","lexeme":1234567890123456789}],"request_id":"synthetic-only"}';
const url='https://api.massive.com/v3/reference/tickers?date=2026-09-30&sort=ticker&limit=1000&order=asc&active=true&locale=us&market=stocks';
function setup({failFinish=false,oversize=false,unapproved=false}={}){
 let handler;const log=[];let fetched=0;const spool=new Map();let fail=failFinish;
 const context={state:'RESERVED',archive_key:'ref01-date-archive-v4:'+'c'.repeat(64),page_number:1,request_url:url,max_response_bytes:8388608,max_runtime_ms:45000,family_id:'REF01',partition_key:'ref01-date-v4:'+'b'.repeat(64),request_key:'ref01-date-v4:'+'b'.repeat(64),as_of:'2026-09-30',active:true,population:'ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER',first_url:url,max_pages:14,request_manifest_sha256:'b'.repeat(64)};
 const tx=async(strings,...values)=>{
  const sql=strings.join('?');if(sql.startsWith('set local'))return [];
  if(values[0]!==attempt||values[1]!==token)throw new Error('AUTH_FAILED');
  if(sql.includes('spool_peek_v11')){log.push('peek');if(unapproved)throw new Error('NOT_REVIEWED');return[{result:spool.has(attempt)?{state:'RETAINED_RECEIPT',receipt:spool.get(attempt),source_sha256:spool.get(attempt).source_sha256}:{state:'NO_RETAINED_RECEIPT'}}];}
  if(sql.includes('date_claim_v4')){log.push('claim');return[{result:context}];}
  if(sql.includes('spool_put_v11')){log.push('spool');const receipt=values[3];spool.set(attempt,structuredClone(receipt));return[{result:{state:'DURABLY_RETAINED',source_sha256:receipt.source_sha256}}];}
  if(sql.includes('date_commit_v4')){log.push('finish');if(fail){fail=false;throw new Error('SYNTHETIC_FORCED_FINISH_LOCK_FAILURE');}assert.equal(values[2].content_text,raw);return[{result:{state:'COMPLETE'}}];}
  if(sql.includes('date_failure_v4')){log.push('failure');return[{result:{state:'BLOCKED'}}];}
  throw new Error('UNEXPECTED_SQL');
 };tx.json=x=>x;
 const db={begin:async f=>{log.push('begin');try{return await f(tx);}finally{log.push('end');}},end:async()=>{}};
 const sandbox={__postgres:()=>db,Deno:{env:{get:n=>({SUPABASE_URL:'https://oxzabweahkoimtevbbny.supabase.co',SUPABASE_DB_URL:'synthetic-db',MASSIVE_API_KEY:'synthetic-private-key'})[n]},serve:f=>{handler=f;}},crypto:webcrypto,TextEncoder,TextDecoder,Request,Response,URL,URLSearchParams,AbortSignal,setTimeout,clearTimeout,Date,ReadableStream,
 fetch:async()=>{fetched++;log.push('fetch');return new Response(raw,{status:200,headers:oversize?{'content-length':'8388609'}:{}});}};
 vm.runInNewContext(source+';globalThis.__handler=buildWorker({postgres:globalThis.__postgres,fetch,DenoEnv:Deno.env.get});',sandbox,{timeout:5000});handler=sandbox.__handler;
 const request=()=>new Request('https://worker.invalid/',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({attempt_id:attempt,token})});
 return {run:()=>handler(request()),spool,log,get fetched(){return fetched;}};
}
let checks=0;
const failure=setup({failFinish:true});let response=await failure.run();let body=await response.json();
assert.equal(response.status,503,JSON.stringify({body,log:failure.log}));assert.equal(body.phase,'FINISH');assert.equal(body.spool_retained,true);assert.equal(failure.spool.size,1);assert.equal(failure.fetched,1);assert.equal(failure.spool.get(attempt).content_text,raw);checks++;
assert(failure.log.indexOf('spool')<failure.log.indexOf('finish'));assert(failure.log.slice(failure.log.indexOf('spool'),failure.log.indexOf('finish')).includes('end'));checks++;
response=await failure.run();body=await response.json();assert.equal(response.status,200);assert.equal(body.retained_replay,true);assert.equal(body.provider_requested,false);assert.equal(failure.fetched,1);assert.equal(failure.log.filter(x=>x==='claim').length,1);assert.equal(failure.log.filter(x=>x==='failure').length,0);checks++;
const held=setup({unapproved:true});response=await held.run();assert.equal(response.status,503);assert.equal(held.fetched,0);assert.equal(held.spool.size,0);checks++;
const cap=setup({oversize:true});await cap.run();assert.equal(cap.spool.size,0);assert.equal(cap.log.includes('finish'),false);assert.equal(cap.log.includes('failure'),true);checks++;
const healthy=setup();response=await healthy.run();assert.equal(response.status,200);assert.equal(healthy.spool.size,1);assert.equal(healthy.fetched,1);assert.equal((await response.json()).spool_retained,true);checks++;
process.stdout.write(JSON.stringify({result:'PASS_ACTUAL_WORKER_SOURCE_SYNTHETIC',checks,provider_network_calls:0,sql_effects_mocked:true,raw_lexemes_preserved:true,finish_failure_retained:true,replay_without_fetch:true})+'\n');
