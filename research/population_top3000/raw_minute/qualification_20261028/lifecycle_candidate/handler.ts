import {runRawWorker} from './raw_minute_worker.ts';
type Db={begin:(fn:(tx:any)=>Promise<any>)=>Promise<any>;end:(options:{timeout:number})=>Promise<any>};
type Env=(name:string)=>string|undefined;
const PROJECT='https://oxzabweahkoimtevbbny.supabase.co';
const output=(value:unknown,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store','x-content-type-options':'nosniff'}});
async function bounded<T>(p:PromiseLike<T>,ms:number){let timer:ReturnType<typeof setTimeout>|undefined;try{return await Promise.race([Promise.resolve(p),new Promise<never>((_,reject)=>{timer=setTimeout(()=>reject(Error('CONTROL_DEADLINE')),Math.max(0,ms));})]);}finally{clearTimeout(timer);}}
export function makeHandler(env:Env,openDb:(url:string)=>Db,fetcher:typeof fetch=fetch){
 return async(req:Request)=>{
  const deadline=Date.now()+45000;let db:Db|undefined;
  try{
   if(req.method!=='POST')return output({error:'MethodNotAllowed'},405);
   if(env('SUPABASE_URL')!==PROJECT)return output({error:'ProjectMismatch'},409);
   if(req.headers.get('content-type')?.split(';')[0].trim().toLowerCase()!=='application/json')return output({error:'InvalidRequest'},400);
   if(Number(req.headers.get('content-length')||0)>512||!req.body)return output({error:'InvalidRequest'},413);
   const reader=req.body.getReader();let text='';let size=0;const decoder=new TextDecoder('utf-8',{fatal:true});
   try{while(true){const part=await bounded(reader.read(),Math.min(700,deadline-Date.now()));if(part.done)break;size+=part.value.length;if(size>512)throw Error();text+=decoder.decode(part.value,{stream:true});}text+=decoder.decode();}finally{void reader.cancel().catch(()=>{});}
   let input:any;try{input=JSON.parse(text);}catch{return output({error:'InvalidRequest'},400);}
   if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).sort().join(',')!=='attempt_id,token'||typeof input.attempt_id!=='string'||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(input.attempt_id)||typeof input.token!=='string'||!/^[0-9a-f]{64}$/.test(input.token))return output({error:'InvalidRequest'},400);
   const connection=env('SUPABASE_DB_URL');if(!connection)return output({error:'WorkerControlUnavailable'},503);
   db=openDb(connection);
   const call=async(kind:'claim'|'commit'|'failure',page?:unknown,reservation?:unknown)=>{
    if(Date.now()>=deadline-500)throw Error('CONTROL_DEADLINE');
    return await bounded(db!.begin(async(tx:any)=>{
     await tx`set local statement_timeout='5s'`;await tx`set local lock_timeout='500ms'`;await tx`set local idle_in_transaction_session_timeout='8s'`;
     if(kind!=='claim'){
      await tx`select pg_advisory_xact_lock(2026083000261022)`;
      const pin=kind==='commit'
       ? await tx`select encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.raw_minute_commit_v3(uuid,text,jsonb,jsonb)'::regprocedure),'UTF8')),'hex') = ${'fbd34f3f072a3e3a95c6f79b4f9dd4ca6e70b68bb4c376ca399987b9548f4191'} as matches`
       : await tx`select encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.raw_minute_failure_v3(uuid,text,text,jsonb)'::regprocedure),'UTF8')),'hex') = ${'d5c2f4b5a45c39ed225a1f4f4107d89244c8185925a8c8666a9061abd38fb77b'} as matches`;
      if(pin[0]?.matches!==true)throw Error('CALLBACK_CODE_CHANGED');
     }
     let rows;
     if(kind==='claim')rows=await tx`select population_top3000_202608_202609_v1.raw_minute_dispatch_v3(${input.attempt_id}::uuid,${input.token}::text) as result`;
     else if(kind==='commit')rows=await tx`select population_top3000_202608_202609_v1.raw_minute_commit_v3(${input.attempt_id}::uuid,${input.token}::text,${tx.json(page)}::jsonb,${tx.json(reservation)}::jsonb) as result`;
     else rows=await tx`select population_top3000_202608_202609_v1.raw_minute_failure_v3(${input.attempt_id}::uuid,${input.token}::text,${page}::text,${tx.json(reservation)}::jsonb) as result`;
     return rows[0].result;
    }),Math.min(6500,deadline-Date.now()-500));
   };
   const result=await bounded(runRawWorker(input,{claim:()=>call('claim'),commit:(_a,_t,p,r)=>call('commit',p,r),failure:(_a,_t,c,r)=>call('failure',c,r),credential:()=>{for(const name of ['MASSIVE_API_KEY','MASSIVE_KEY','POLYGON_API_KEY','POLYGON_KEY']){const value=env(name)?.trim();if(value)return value;}throw Error('SOURCE_CREDENTIAL_UNAVAILABLE');},fetcher}),deadline-Date.now()-100);
   return output(result);
  }catch{return output({state:'UNCONFIRMED',code:'WORKER_CONTROL_OUTCOME_REQUIRES_RECONCILIATION',receipt_recorded:false},503);}
  finally{if(db)void db.end({timeout:0}).catch(()=>{});}
 };
}
