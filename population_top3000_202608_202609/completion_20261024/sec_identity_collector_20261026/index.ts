// Root deploys with verify_jwt=true; no source admission is installed by deployment.
import postgres from 'npm:postgres@3.4.5';
import {createWorker} from './worker.mjs';
import allowed from './allowlist_PRIVATE.json' with {type:'json'};
const PROJECT_URL='https://oxzabweahkoimtevbbny.supabase.co';
Deno.serve(async(req:Request)=>{
 if(Deno.env.get('SUPABASE_URL')!==PROJECT_URL)return Response.json({error:'PROJECT_MISMATCH'},{status:409});
 const uri=Deno.env.get('SUPABASE_DB_URL');if(!uri)return Response.json({error:'CONTROL_UNAVAILABLE'},{status:503});
 const db=postgres(uri,{max:1,prepare:false,ssl:'require',connect_timeout:5,idle_timeout:30,max_lifetime:60,debug:false,onnotice:()=>{}});
 const rpc=async(name:string,{attempt,token,payload}:any)=>db.begin(async(tx:any)=>{
  await tx`set local statement_timeout='5s'`;await tx`set local lock_timeout='2s'`;let r;
  if(name==='context')r=await tx`select population_top3000_202608_202609_v1.sec_identity_source_context_v1(${attempt}::uuid,${token}::text) as result`;
  else if(name==='claim')r=await tx`select population_top3000_202608_202609_v1.sec_identity_source_claim_v1(${attempt}::uuid,${token}::text,${payload.user_agent_sha256}::text) as result`;
  else if(name==='commit')r=await tx`select population_top3000_202608_202609_v1.sec_identity_source_commit_v1(${attempt}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
  else if(name==='failure')r=await tx`select population_top3000_202608_202609_v1.sec_identity_source_failure_v1(${attempt}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
  else throw Error('INVALID_RPC');return r[0].result;
 });
 try{return await createWorker({rpc,fetch,userAgent:Deno.env.get('SEC_USER_AGENT'),allowlist:new Set(allowed)})(req);}
 finally{void db.end({timeout:0}).catch(()=>{});}
});
