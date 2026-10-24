// Private exact-byte official government source recovery. One bounded request per token.
import postgres from "npm:postgres@3.4.5";
const PROJECT="https://oxzabweahkoimtevbbny.supabase.co",MAX=8388608;
const reply=(x:unknown,s=200)=>Response.json(x,{status:s,headers:{"cache-control":"no-store"}});
async function hash(b:Uint8Array){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',b))].map(x=>x.toString(16).padStart(2,'0')).join('');}
async function read(stream:ReadableStream<Uint8Array>|null,max:number,deadline:number){if(!stream)throw Error('EMPTY');let n=0;const parts:Uint8Array[]=[];const r=stream.getReader();try{while(true){if(Date.now()>deadline)throw Error('TIME');let timer:ReturnType<typeof setTimeout>|undefined;let x:ReadableStreamReadResult<Uint8Array>;try{x=await Promise.race([r.read(),new Promise<never>((_,reject)=>{timer=setTimeout(()=>reject(Error('TIME')),Math.max(1,deadline-Date.now()));})]);}finally{if(timer!==undefined)clearTimeout(timer);}if(x.done)break;n+=x.value.length;if(n>max)throw Error('SIZE');parts.push(x.value);}const b=new Uint8Array(n);let o=0;for(const p of parts){b.set(p,o);o+=p.length;}return b;}finally{void r.cancel().catch(()=>{});}}
function base64(b:Uint8Array){let s='';for(let i=0;i<b.length;i+=32768)s+=String.fromCharCode(...b.subarray(i,i+32768));return btoa(s);}
Deno.serve(async(req:Request)=>{
 let db:ReturnType<typeof postgres>|null=null,c:any=null,id='',token='',phase='INPUT',status:number|null=null,fetchStarted=false;
 const deadline=Date.now()+45000;
 const call=async(kind:string,payload?:unknown)=>{if(!db)throw Error('DB');return await db.begin(async(tx)=>{await tx`set local statement_timeout='12s'`;await tx`set local lock_timeout='2s'`;let rows;
 if(kind==='claim')rows=await tx`select population_top3000_202608_202609_v1.ctx07_recovery_claim_v6(${id}::uuid,${token}) result`;
 else if(kind==='commit')rows=await tx`select population_top3000_202608_202609_v1.ctx07_recovery_commit_v6(${id}::uuid,${token},${tx.json(payload)}::jsonb) result`;
 else rows=await tx`select population_top3000_202608_202609_v1.ctx07_recovery_failure_v6(${id}::uuid,${token},${tx.json(payload)}::jsonb) result`;return rows[0].result;});};
 try{
 if(req.method!=='POST'||Deno.env.get('SUPABASE_URL')!==PROJECT)return reply({error:'InvalidContext'},400);
 const raw=await read(req.body,512,Date.now()+1000),x=JSON.parse(new TextDecoder().decode(raw));
 if(Object.keys(x).sort().join(',')!=='attempt_id,token'||!/^[0-9a-f-]{36}$/.test(x.attempt_id)||!/^[0-9a-f]{64}$/.test(x.token))return reply({error:'InvalidRequest'},400);
 id=x.attempt_id;token=x.token;const dbUrl=Deno.env.get('SUPABASE_DB_URL');if(!dbUrl)return reply({error:'DatabaseUnavailable'},503);
 db=postgres(dbUrl,{max:1,prepare:false,ssl:'require',connect_timeout:5,idle_timeout:30,max_lifetime:60,debug:false,onnotice:()=>{}});
 phase='CLAIM';c=await call('claim');if(c.state!=='RESERVED')return reply({state:c.state,provider_requested:false});
 const url=new URL(c.source_url);if(url.protocol!=='https:'||!['www.bls.gov','www.bea.gov','www.census.gov','www2.census.gov'].includes(url.hostname)||url.username||url.password||url.hash||c.max_bytes>MAX||c.max_bytes<1)throw Error('CLAIM');
 phase='FETCH';fetchStarted=true;const res=await fetch(url,{redirect:'error',signal:AbortSignal.timeout(Math.min(25000,deadline-Date.now()-12000)),headers:{'User-Agent':'Top3000 historical source verification (private research)','Accept':'text/html,application/pdf,application/octet-stream'}});status=res.status;
 if(status!==200){void res.body?.cancel();phase='FAILURE';const out=await call('failure',{http_status:status,fetch_started:true,code:'HTTP_'+status});return reply({state:out.state,http_status:status,receipt_recorded:true});}
 const declared=Number(res.headers.get('content-length')||0);if(declared>c.max_bytes){void res.body?.cancel();throw Error('SIZE');}
 const b=await read(res.body,c.max_bytes,deadline-12000);const h=await hash(b);phase='COMMIT';const out=await call('commit',{request_key:c.request_key,request_url:c.source_url,http_status:200,body_base64:base64(b),raw_sha256:h,retrieved_at:new Date().toISOString(),content_type:res.headers.get('content-type')||'application/octet-stream'});
 return reply({state:out.state,source_sha256:h,bytes:b.length,receipt_recorded:true});
 }catch{
 if(c?.state==='RESERVED'&&phase!=='COMMIT'&&phase!=='FAILURE'&&Date.now()<deadline-2000){try{const out=await call('failure',{http_status:status,fetch_started:fetchStarted,code:'TRANSPORT_OR_PARSE_FAILURE'});return reply({state:out.state,receipt_recorded:true});}catch{}}
 return reply({error:'WorkerReceiptUnconfirmed',phase,provider_requested:fetchStarted,receipt_recorded:false},503);
 }finally{if(db)void db.end({timeout:0}).catch(()=>{});}
});
