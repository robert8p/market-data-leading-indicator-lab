import assert from 'node:assert/strict';import {readFileSync} from 'node:fs';import {stripTypeScriptTypes} from 'node:module';import vm from 'node:vm';import {webcrypto} from 'node:crypto';
const src=stripTypeScriptTypes(readFileSync(new URL('./resource_worker_v4.ts',import.meta.url),'utf8').replace(/^import postgres.*\n/m,''));
const raw='node_filesystem_avail_bytes{mountpoint="/data",device="/dev/test"} 175000000000\nnode_filesystem_free_bytes{mountpoint="/data",device="/dev/test"} 180000000000\nnode_filesystem_size_bytes{mountpoint="/data",device="/dev/test"} 792000000000\n';
async function run({claim={state:'CLAIMED',probe_attempt:13,total_timeout_ms:30000},text=raw,status=200,credential='test-role'}={}){let serve,calls=0,finishes=[];const sql=async(strings,...values)=>{const s=strings.join('?');if(s.includes('resource_probe_claim_v4'))return[{result:claim}];if(s.includes('resource_probe_finish_v4')){finishes.push(values[2]);return[{result:{state:'RECORDED',outcome:values[2].outcome}}];}return[];};sql.json=x=>x;const db={begin:f=>f(sql),end:async()=>{}};
const ctx={postgres:()=>db,Response,Request,TextEncoder,TextDecoder,Uint8Array,AbortSignal,DOMException,Date,Set,Map,Promise,crypto:webcrypto,setTimeout,clearTimeout,btoa,Deno:{env:{get:n=>({'SUPABASE_URL':'https://oxzabweahkoimtevbbny.supabase.co','SUPABASE_DB_URL':'test-db','SUPABASE_SERVICE_ROLE_KEY':credential}[n])},serve:f=>serve=f},fetch:async(url,o)=>{calls++;assert.equal(url,'https://oxzabweahkoimtevbbny.supabase.co/customer/v1/privileged/metrics');assert.equal(o.method,'GET');assert.equal(o.redirect,'error');assert.equal(o.headers.Authorization,'Basic '+btoa('username:'+credential));return new Response(text,{status});}};
vm.runInNewContext(src,ctx);const response=await serve(new Request('https://worker.invalid',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({probe_id:'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',token:'b'.repeat(64)})}));return{result:await response.json(),calls,finishes};}
let r=await run();assert.equal(r.calls,1);assert.equal(r.finishes[0].outcome,'SUCCEEDED');assert.equal(r.finishes[0].metrics[0].available_bytes,175000000000);assert(!JSON.stringify(r).includes('test-role'));
r=await run({claim:{state:'DENIED'}});assert.equal(r.calls,0);
r=await run({claim:{state:'CLAIMED',probe_attempt:12,total_timeout_ms:30000}});assert.equal(r.calls,0);
r=await run({credential:''});assert.equal(r.calls,0);assert.equal(r.finishes[0].error_code,'METRICS_CREDENTIAL_UNAVAILABLE');
r=await run({status:403});assert.equal(r.calls,1);assert.equal(r.finishes[0].outcome,'FAILED');
r=await run({text:raw.replace('175000000000','900000000000')});assert.equal(r.finishes[0].error_code,'FILESYSTEM_METRICS_MISSING');
r=await run({text:raw+'node_filesystem_avail_bytes{mountpoint="/data",device="/dev/test"} 123\n'});assert.equal(r.finishes[0].error_code,'METRICS_PARSE_ERROR');
r=await run({text:'x'.repeat(2097153)});assert.equal(r.finishes[0].error_code,'METRICS_RESPONSE_TOO_LARGE');
r=await run({claim:{state:'CLAIMED',probe_attempt:13,total_timeout_ms:30000}});assert.equal(r.calls,1);
r=await run({claim:{state:'CLAIMED',probe_attempt:14,total_timeout_ms:30000}});assert.equal(r.calls,0);
r=await run({claim:{state:'CLAIMED',probe_attempt:13.5,total_timeout_ms:30000}});assert.equal(r.calls,0);
console.log('PASS 11 resource worker tests: exact slot13, old/denied capability no fetch, credentials not exposed, exact one owned-project GET, failure status, inconsistent metric values, duplicates, response bound.');
