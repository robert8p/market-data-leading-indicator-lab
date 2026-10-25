import postgres from 'npm:postgres@3.4.5';
import {boundedInput,boundedBody,validateRequest,validateEnvelope} from './contract.mjs';
// Canonical SHA256: replace this complete declaration with the same declaration
// whose quoted value is WORKER_SHA_PLACEHOLDER, then hash the entire UTF8 file.
const CANONICAL_WORKER_SHA='b7d9783a20f8a6a94325eaf907aee2bd5d439184ad5e3b9ca3d223730c2215c4';
const S='population_top3000_202608_202609_v1';
Deno.serve(async(req:Request)=>{
 if(req.method!=='POST')return Response.json({state:'METHOD_REJECTED'},{status:405});
 let b:any;try{b=await boundedInput(req.body);}catch{return Response.json({state:'INPUT_BOUND_OR_INVALID'},{status:400});}
 // No database/provider call in synthetic mode. Actual deployed endpoint proves module
 // imports and exact historical request logic; SQL rollback proves storage separately.
 if(b.mode==='synthetic'){
  const out=[];for(let n=1;n<=200;n++){const date='2026-08-'+String(1+(n%31)).padStart(2,'0'),ticker='FIXTURE'+n;
   const r={ticker,point_date:date,exact_url:`https://api.massive.com/v3/reference/tickers/${ticker}?date=${date}`,path:`/v3/reference/tickers/${ticker}`,request_state:'ADMITTED',request_manifest_sha256:'13dafeef5c83990b6c73f8c8d9a7edcbfd81bff22b2f3e268e38d26d5c863bef',scope_sha256:'8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'};
   validateRequest(r);validateEnvelope({status:'OK',results:{ticker}},r);out.push(n);}
  return Response.json({state:'SYNTHETIC_200_BINDINGS_PASS',cases:out.length,provider_calls:0,source_http:0,worker_sha256:CANONICAL_WORKER_SHA,storage_proven:false});
 }
 if(!Number.isSafeInteger(b.admission_event_id)||typeof b.wake_token!=='string'||b.wake_token.length>100)return Response.json({state:'CAPABILITY_REQUIRED'},{status:403});
 const dbUrl=Deno.env.get('SUPABASE_DB_URL'),apiKey=(Deno.env.get('MASSIVE_API_KEY')||Deno.env.get('MASSIVE_KEY')||Deno.env.get('POLYGON_API_KEY')||Deno.env.get('POLYGON_KEY')||'').trim();
 if(!dbUrl||!apiKey)return Response.json({state:'CONFIGURATION_HOLD'},{status:503});
 const sql=postgres(dbUrl,{max:1,prepare:false,connect_timeout:3,connection:{application_name:'ref01-overview-v6',statement_timeout:'5000',lock_timeout:'750'}});
 const rpc=async(name:string,args:any[])=>{return(await sql.unsafe(`select ${S}.${name}(${args.map((_,i)=>'$'+(i+1)).join(',')}) result`,args))[0].result;};
 try{
  if(!await rpc('oq6_auth',[b.admission_event_id,b.wake_token,CANONICAL_WORKER_SHA]))return Response.json({state:'CAPABILITY_REJECTED'},{status:403});
  // Bounded native turn waits without holding any DB lock. No HTTP is sent on wait.
  await new Promise(r=>setTimeout(r,15000));
  const pre=await rpc('oq6_preflight',[b.admission_event_id]);
  if(pre.state==='BODY_RETAINED')return Response.json(await rpc('oq6_consume',[pre.request_key,b.admission_event_id]));
  if(pre.state!=='ELIGIBLE')return Response.json(pre);
  const c=await rpc('oq6_wake',[b.admission_event_id,b.wake_token,CANONICAL_WORKER_SHA]);if(c.state!=='CLAIMED'){if(['BUSY','QUIET_SHARED_PROVIDER_WAIT'].includes(c.state))return Response.json(await rpc('oq6_defer',[b.admission_event_id,b.wake_token,CANONICAL_WORKER_SHA]));return Response.json(c);}
  let response:Response,bytes:Uint8Array;
  try{response=await fetch(validateRequest(c.request),{headers:{Authorization:'Bearer '+apiKey},redirect:'error',signal:AbortSignal.timeout(20000)});bytes=await boundedBody(response);}catch{return Response.json(await rpc('oq6_unknown',[c.request.request_key,c.token,'NATIVE_RESPONSE_UNKNOWN_NO_RETRY']));}
  const sha=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))).map(x=>x.toString(16).padStart(2,'0')).join('');let raw='';for(const z of bytes)raw+=String.fromCharCode(z);
  // Independent RPC COMMIT precedes every normalization/FINISH/global-control lock.
  await rpc('oq6_spool_save',[c.request.request_key,c.token,JSON.stringify({body_base64:btoa(raw),bytes:bytes.length,sha256:sha,http_status:response.status,physical_http_count:1,representation:'FETCH_RESPONSE_BODY_BYTES_NOT_ORIGINAL_HTTP_WIRE'}),CANONICAL_WORKER_SHA]);
  return Response.json(await rpc('oq6_consume',[c.request.request_key,b.admission_event_id]));
 }catch{return Response.json({state:'HELD_NO_AUTOMATIC_RETRY',body_retention_claimed:false},{status:503});}finally{await sql.end({timeout:1}).catch(()=>{});}
});
