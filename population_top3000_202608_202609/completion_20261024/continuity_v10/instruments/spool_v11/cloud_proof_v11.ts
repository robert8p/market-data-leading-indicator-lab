import postgres from "npm:postgres@3.4.5";
import { buildWorker } from "./core_v11.ts";
const CORE_SHA="cfe13f0ac41f16f573e37257e99e971e1b5877897b44477d3cbcdf5b97cab1ea";
const RAW='{"status":"OK","results":[{"ticker":"SYNTHETIC_ONLY","lexeme":1234567890123456789}],"request_id":"fixture-only-not-market-evidence"}';
const FIXTURE_KEY="synthetic-fixture-key-not-provider-credential";
const reply=(x:unknown,status=200)=>Response.json(x,{status,headers:{"cache-control":"no-store"}});
Deno.serve(async(req:Request)=>{
 let sql:ReturnType<typeof postgres>|null=null;
 try{
  if(req.method!=="POST")return reply({error:"MethodNotAllowed"},405);
  if(req.headers.get("content-type")?.split(';')[0]!=="application/json")return reply({error:"InvalidRequest"},400);
  const reader=req.body?.getReader();if(!reader)return reply({error:"InvalidRequest"},400);
  const chunks:Uint8Array[]=[];let n=0;try{while(true){const r=await reader.read();if(r.done)break;n+=r.value.length;if(n>512)throw Error('BODY_CAP');chunks.push(r.value);}}finally{void reader.cancel().catch(()=>{});}
  const bytes=new Uint8Array(n);let offset=0;for(const c of chunks){bytes.set(c,offset);offset+=c.length;}
  const b=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
  if(!b||Object.keys(b).sort().join(',')!=="proof_id,token"||typeof b.proof_id!=="string"||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(b.proof_id)||typeof b.token!=="string"||!/^[0-9a-f]{64}$/.test(b.token))return reply({error:"InvalidRequest"},400);
  if(Deno.env.get('SUPABASE_URL')!=="https://oxzabweahkoimtevbbny.supabase.co")return reply({error:"ProjectMismatch"},409);
  const dbUrl=Deno.env.get('SUPABASE_DB_URL');if(!dbUrl)return reply({error:"FixtureControlUnavailable"},503);
  sql=postgres(dbUrl,{max:1,prepare:false,ssl:'require',connect_timeout:5,debug:false,onnotice:()=>{}});
  const realSql=sql;
  const call=async(method:string,payload:unknown=null)=>{
   const r=await realSql`select population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_call_v11(${b.proof_id}::uuid,${b.token}::text,${method}::text,${realSql.json(payload)}::jsonb) as result`;
   return r[0].result;
  };
  const start=await call('start');if(start.core_sha256!==CORE_SHA)throw Error('CORE_HASH_MISMATCH');
  const fakePostgres=()=>({begin:async(fn:any)=>await realSql.begin(async(realTx:any)=>{
   const tx:any=async(strings:TemplateStringsArray,...values:any[])=>{
    const query=strings.join('?');
    if(query==="set local statement_timeout='5s'"||query==="set local lock_timeout='2s'")return await realTx.unsafe(query);
    if(values[0]!==b.proof_id||values[1]!==b.token)throw Error('FIXTURE_ATTEMPT_BINDING');
    let method:string,payload:unknown=null;
    if(query.includes('ref01_spool_peek_v11('))method='peek';
    else if(query.includes('ref01_spool_put_v11(')){method='spool';payload=values[3];}
    else if(query.includes('ref01_date_claim_v4('))method='claim';
    else if(query.includes('ref01_date_commit_v4(')){method='commit';payload=values[2];}
    else throw Error('UNEXPECTED_WORKER_SQL');
    const rows=await realTx`select population_top3000_spool_fixture_v11.ref01_spool_cloud_fixture_call_v11(${b.proof_id}::uuid,${b.token}::text,${method}::text,${realTx.json(payload)}::jsonb) as result`;
    return rows;
   };tx.json=(x:unknown)=>x;return await fn(tx);
  }),end:async()=>{}});
  const syntheticFetch:typeof globalThis.fetch=async(input:any)=>{
   const u=new URL(String(input));if(u.origin!=="https://api.massive.com"||u.pathname!=="/v3/reference/tickers"||u.searchParams.get('apiKey')!==FIXTURE_KEY)throw Error('FIXTURE_URL_REJECTED');
   await call('fetch');return new Response(RAW,{status:200,headers:{'content-type':'application/json'}});
  };
  const handler=buildWorker({postgres:fakePostgres,fetch:syntheticFetch,DenoEnv:(name:string)=>['MASSIVE_API_KEY','MASSIVE_KEY','POLYGON_API_KEY','POLYGON_KEY'].includes(name)?FIXTURE_KEY:Deno.env.get(name)});
  const request=()=>new Request('https://fixture.invalid/',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({attempt_id:b.proof_id,token:b.token})});
  const first=await handler(request()),firstBody=await first.json();
  if(first.status!==503||firstBody.phase!=='FINISH'||firstBody.spool_retained!==true)throw Error('FIRST_FAILURE_NOT_OBSERVED');
  const retained=await call('summary');if(retained.content_text!==RAW||retained.fetches!==1||retained.claims!==1||retained.finishes!==0)throw Error('DURABLE_RECEIPT_NOT_PROVEN');
  await call('confirm_failure',firstBody);
  const second=await handler(request()),secondBody=await second.json();
  const final=await call('summary');
  if(second.status!==200||secondBody.retained_replay!==true||secondBody.provider_requested!==false||final.fetches!==1||final.claims!==1||final.finishes!==1||final.first_failure_verified!==true||final.receipt_sha256!==retained.receipt_sha256||final.content_text!==RAW)throw Error('REPLAY_NOT_PROVEN');
  return reply({result:'PASS_DEPLOYED_SHARED_CORE_REAL_POSTGRES_FIXTURE_DURABILITY',fixture_only:true,core_sha256:CORE_SHA,first_status:first.status,first_phase:firstBody.phase,second_status:second.status,retained_replay:true,source_sha256:final.source_sha256,receipt_sha256:final.receipt_sha256,synthetic_fetches:1,external_provider_http_calls:0,production_spool_functions_executed:false,private_fixture_sql_executed:true,production_first_one_consumed:false,market_facts_produced:false});
 }catch{return reply({error:'SyntheticRuntimeProofFailed',fixture_only:true,production_spool_functions_executed:false},503);}
 finally{if(sql)void sql.end({timeout:0}).catch(()=>{});}
});
