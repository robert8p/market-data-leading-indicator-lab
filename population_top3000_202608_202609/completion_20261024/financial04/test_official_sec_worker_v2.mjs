import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { stripTypeScriptTypes } from 'node:module';
import vm from 'node:vm';
import { webcrypto } from 'node:crypto';

const worker=stripTypeScriptTypes(readFileSync(new URL('./official_sec_worker_v2.ts',import.meta.url),'utf8').replace(/^import postgres.*\n/m,''));
const raw='<xbrl>é</xbrl>',rawBytes=new TextEncoder().encode(raw);
const base={state:'RESERVED',document_key:'financial-document-v1:'+'a'.repeat(64),request_manifest_sha256:'b'.repeat(64),
 request_url:'https://www.sec.gov/Archives/edgar/data/25475/000119312526330643/crda-20260630_htm.xml',cik:'0000025475',
 accession_number:'0001193125-26-330643',file_id:'crda-20260630_htm.xml',expected_file_bytes:rawBytes.length,
 max_response_bytes:8388608,max_runtime_ms:45000,maximum_provider_requests:1,family_id:'F03',partition_key:'financial-document-v1:'+'a'.repeat(64)};
async function run(claim=base,response=()=>new Response(rawBytes),method='POST'){
 let serve,fetches=0,commits=[],failures=[];
 const sql=async(strings,...values)=>{
  const text=strings.join('?');
  if(text.includes('financial_sec_claim_v2'))return [{result:claim}];
  if(text.includes('financial_sec_commit_v2')){commits.push(values[2]);return [{result:{state:'COMPLETE'}}];}
  if(text.includes('financial_sec_failure_v2')){failures.push(values[2]);return [{result:{state:'BLOCKED'}}];}
  return [];
 };sql.json=x=>x;
 const db={begin:f=>f(sql),end:async()=>{}};
 const ctx={postgres:()=>db,Response,Request,TextEncoder,TextDecoder,URL,ReadableStream,Uint8Array,AbortSignal,Date,Set,Map,Promise,
  crypto:webcrypto,setTimeout,clearTimeout,Deno:{env:{get:n=>n==='SUPABASE_URL'?'https://oxzabweahkoimtevbbny.supabase.co':n==='SUPABASE_DB_URL'?'redacted-test-url':undefined},serve:f=>{serve=f;}},
  fetch:async(url,opts)=>{fetches++;assert.equal(opts.redirect,'error');assert.equal(opts.headers['Accept-Encoding'],'identity');return response(url,opts);}};
 vm.runInNewContext(worker,ctx);
 const req=new Request('https://worker.invalid/',{method,headers:{'content-type':'application/json'},...(method==='POST'?{body:JSON.stringify({attempt_id:'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',token:'c'.repeat(64)})}:{})});
 const result=await serve(req);return {status:result.status,body:await result.json(),fetches,commits,failures};
}
let r=await run();assert.equal(r.fetches,1);assert.equal(r.commits.length,1);assert.equal(r.commits[0].content_text,raw);assert.equal(r.commits[0].source_sha256,r.commits[0].wire_sha256);assert.equal(r.commits[0].wire_bytes,rawBytes.length);
r=await run({state:'BLOCKED'});assert.equal(r.fetches,0);assert.equal(r.commits.length,0);
for(const request_url of ['http://www.sec.gov/Archives/edgar/data/25475/000119312526330643/crda-20260630_htm.xml','https://www.sec.gov@evil.invalid/a','https://127.0.0.1/a',base.request_url+'?x=1',base.request_url+'#x']){
 r=await run({...base,request_url});assert.equal(r.fetches,0);assert.equal(r.commits.length,0);assert.equal(r.failures.length,1);
}
r=await run(base,()=>new Response(null,{status:429,headers:{'retry-after':'12'}}));assert.equal(r.fetches,1);assert.equal(r.failures[0].failure_class,'RATE_LIMIT');assert.equal(r.failures[0].retry_after_seconds,12);
r=await run({...base,expected_file_bytes:rawBytes.length+1});assert.equal(r.commits.length,0);assert.equal(r.failures[0].code,'EXACT_BODY_PROOF_MISMATCH');
r=await run(base,()=>new Response(new Uint8Array([0xff,0xfe,0xfe])));assert.equal(r.commits.length,0);assert.equal(r.fetches,1);assert.equal(r.failures.length,1);
r=await run(base,()=>new Response(rawBytes,{headers:{'content-length':'8388609'}}));assert.equal(r.commits.length,0);assert.equal(r.failures[0].code,'RESPONSE_TOO_LARGE');
r=await run(base,()=>{throw new Error('synthetic fetch failure');});assert.equal(r.fetches,1);assert.equal(r.failures[0].code,'FETCH_FAILED');
r=await run(base,()=>new Response(rawBytes),'GET');assert.equal(r.status,405);assert.equal(r.fetches,0);
console.log('PASS: exact UTF8 source identity, fenced claim rejection, five URL escapes, rate limit, length mismatch, invalid UTF8, byte bound, no retry, method guard.');
