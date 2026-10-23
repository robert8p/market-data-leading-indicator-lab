// Full handler fixture using actual postgres.js3.4.5 serializers at the SQL parameter boundary.
// Runs the known broken v2, the corrected v3, and the authenticated v3 HTTP fallback.
// No external request, DB connection or real credential is used.
import fs from 'node:fs';import vm from 'node:vm';import assert from 'node:assert/strict';
import {stripTypeScriptTypes} from 'node:module';import {webcrypto} from 'node:crypto';
import {Parameter,handleValue,serializers} from './driver_evidence/postgres-3.4.5/src/types.js';
const env={SUPABASE_URL:'https://oxzabweahkoimtevbbny.supabase.co',SUPABASE_DB_URL:'postgres://fixture/unused',
 SUPABASE_SECRET_KEYS:'{"default":"sb_secret_FIXTURE_NEVER_REAL"}',SEC_USER_AGENT:'PRIVATE_UA_FIXTURE',SEC_CONTACT_EMAIL:'private@example.invalid',MASSIVE_KEY:'PRIVATE_KEY_FIXTURE'};
const metrics='node_filesystem_avail_bytes{device="private_fixture",mountpoint="/db",fstype="ext4"} 40\nnode_filesystem_free_bytes{device="private_fixture",mountpoint="/db",fstype="ext4"} 50\nnode_filesystem_size_bytes{device="private_fixture",mountpoint="/db",fstype="ext4"} 100\n';
const body=JSON.stringify({probe_id:'11111111-1111-4111-8111-111111111111',token:'a'.repeat(64)});
const request=()=>new Request('https://owned.invalid/probe',{method:'POST',headers:{'Content-Type':'application/json'},body});
async function exercise(file,attempt,forceFinishFailure=false){
 let handler;let claimed=false;let fetchCount=0;let saved=null;let decodedType=null;
 const mockPostgres=()=>({begin:async(fn)=>{
  const tx=async(strings,...values)=>{
   const query=strings.join('');
   if(query.includes('resource_probe_claim_v1')){
    if(claimed)return [{result:{state:'DENIED'}}];claimed=true;
    return [{result:{state:'CLAIMED',probe_attempt:attempt,total_timeout_ms:30000}}];
   }
   if(query.includes('resource_probe_finish_v1')){
    const parameters=[];const types=[];handleValue(values[2],parameters,types,{transform:{undefined:undefined}});
    if(!types[0])types[0]=3802; // Actual ParameterDescription for this explicit ::jsonb argument.
    const wire=serializers[types[0]](parameters[0]);const decoded=JSON.parse(wire);decodedType=typeof decoded;
    if(decodedType!=='object'||decoded===null||Array.isArray(decoded))throw Object.assign(new Error('INVALID_SANITIZED_PROBE_RESULT'),{code:'P0001'});
    if(forceFinishFailure)throw Object.assign(new Error('RAW_PRIVATE_ERROR_FIXTURE'),{code:'55P03'});
    saved=decoded;return [{result:{state:'RECORDED',outcome:saved.outcome}}];
   }
   return [];
  };
  tx.json=value=>new Parameter(value,3802);return fn(tx);
 },end:()=>Promise.resolve()});
 const input=fs.readFileSync(new URL('./'+file,import.meta.url),'utf8').replace(/^import postgres from "npm:postgres@3\.4\.5";$/m,'const postgres=globalThis.mockPostgres;');
 const context=vm.createContext({mockPostgres,Deno:{env:{get:name=>env[name]},serve:fn=>handler=fn},crypto:webcrypto,TextEncoder,TextDecoder,Uint8Array,ArrayBuffer,AbortSignal,DOMException,Request,Response,ReadableStream,setTimeout,clearTimeout,btoa,
  fetch:async(url,options)=>{fetchCount++;assert.equal(url,'https://oxzabweahkoimtevbbny.supabase.co/customer/v1/privileged/metrics');assert.equal(options.redirect,'error');return new Response(metrics,{status:200});}});
 vm.runInContext(stripTypeScriptTypes(input,{mode:'strip'}),context);
 const response=await handler(request());const result=await response.json();
 const replay=await handler(request());assert.equal(replay.status,403);assert.equal(fetchCount,1);
 for(const forbidden of ['FIXTURE_NEVER_REAL','PRIVATE_UA_FIXTURE','private@example.invalid','PRIVATE_KEY_FIXTURE','private_fixture','RAW_PRIVATE_ERROR_FIXTURE']){
  assert.equal(JSON.stringify(result).includes(forbidden),false);assert.equal(JSON.stringify(saved).includes(forbidden),false);
 }
 return {status:response.status,result,saved,decodedType};
}
const broken=await exercise('population-top3000-resource-v2.ts',2);
assert.equal(broken.status,503);assert.equal(broken.result.phase,'FINISH');assert.equal(broken.result.sqlstate,'P0001');assert.equal(broken.decodedType,'string');assert.equal(broken.saved,null);
const fixed=await exercise('population-top3000-resource-v3.ts',3);
assert.equal(fixed.status,200);assert.equal(fixed.result.state,'RECORDED');assert.equal(fixed.decodedType,'object');assert.equal(fixed.saved.metrics[0].available_bytes,40);
const fallback=await exercise('population-top3000-resource-v3.ts',3,true);
assert.equal(fallback.status,503);assert.equal(fallback.result.phase,'FINISH');assert.equal(fallback.result.sqlstate,'55P03');assert.equal(fallback.result.validation_error,null);
assert.equal(fallback.saved,null);assert.equal(fallback.result.receipt_recorded,false);assert.equal(fallback.result.snapshot.outcome,'SUCCEEDED');assert.equal(fallback.result.snapshot.metrics[0].available_bytes,40);
assert.deepEqual(Object.keys(fallback.result.snapshot).sort(),['capabilities','elapsed_ms','error_code','metrics','metrics_http_status','observed_at','outcome','source_sha256'].sort());
assert.deepEqual(fallback.result.snapshot.capabilities,{sec_user_agent_configured:true,sec_contact_email_configured:true,massive_key_configured:true});
console.log('PASS_REAL_POSTGRES345_BOUNDARY_REPRODUCES_V2_FAILURE_AND_VERIFIES_V3_AND_SAFE_HTTP_FALLBACK');
