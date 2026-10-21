import postgres from "npm:postgres@3.4.5";
const VERSION="phase2-persistent-v1.0.1";
const AUTH_HASH="dd33639336a84139f03a556cb926ab13524e0daa85a22405a36a620873ad566d";
const S="research_eq10_fundamental_pit_20261020";
const sha=async(s:string)=>Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",new TextEncoder().encode(s)))).map(x=>x.toString(16).padStart(2,"0")).join("");
class Failure extends Error {constructor(public code:string,public retryable:boolean,public delay=60){super(code);}}
Deno.serve(async(req:Request)=>{
 if(req.method!=="POST")return new Response("Method not allowed",{status:405});
 const token=(req.headers.get("authorization")||"").replace(/^Bearer /,"");
 if(await sha(token)!==AUTH_HASH)return new Response("Unauthorized",{status:401});
 const execution=crypto.randomUUID(),isolate=Deno.env.get("SB_EXECUTION_ID")||null;
 const dbUrl=Deno.env.get("SUPABASE_DB_URL");
 if(!dbUrl)return Response.json({error:"DB_CONFIGURATION_MISSING"},{status:503});
 const sql=postgres(dbUrl,{max:1,prepare:false,idle_timeout:5,connect_timeout:8,connection:{application_name:"phase2_financial_worker_v1",statement_timeout:"12000",lock_timeout:"1500",max_parallel_workers_per_gather:"0"}});
 let job:any;
 const event=async(name:string,details:any={})=>{await sql.unsafe(`insert into ${S}.financial_worker_events_v1(job_id,event,execution_id,lease_token,details) values($1,$2,$3,$4,$5::text::jsonb)`,[job?.job_id||null,name,execution,job?.lease_token||null,JSON.stringify({...details,isolate,version:VERSION})]);};
 async function guard(tx:any) {
  const r=await tx.unsafe(`select j.job_id from ${S}.financial_worker_jobs_v1 j cross join ${S}.financial_worker_control_v1 c where j.job_id=$1 and j.lease_token=$2 and j.status='LEASED' and j.lease_expires_at>clock_timestamp() and c.lane='financials' and not c.stopped for update of j`,[job.job_id,job.lease_token]);
  if(!r.length)throw new Failure("LEASE_LOST_OR_STOPPED",false);
 }
 async function fetchMassive(path:string,asJson:boolean){
  if(!/^\/v1\/reference\/sec\/filings\/[0-9]{10}-[0-9]{2}-[0-9]{6}(\/files\/[^/?#]+)?$/.test(path))throw new Failure("ROUTE_NOT_ALLOWED",false);
  const key=(Deno.env.get("MASSIVE_API_KEY")||Deno.env.get("MASSIVE_KEY")||Deno.env.get("POLYGON_API_KEY")||Deno.env.get("POLYGON_KEY")||"").trim();
  if(!key)throw new Failure("MASSIVE_CREDENTIAL_MISSING",false);
  const started=new Date().toISOString();
  const r=await fetch("https://api.massive.com"+path,{headers:{Authorization:"Bearer "+key},redirect:"error",signal:AbortSignal.timeout(25000)});
  if(!r.ok){const status=r.status;await event("PROVIDER_ERROR",{route:path,status});throw new Failure("PROVIDER_HTTP_"+status,status===429||status>=500,Math.min(900,Math.max(60,Number(r.headers.get("retry-after"))||60)));}
  const reader=r.body!.getReader();let bytes=0;const chunks:Uint8Array[]=[];
  while(true){const v=await reader.read();if(v.done)break;bytes+=v.value.length;if(bytes>8*1024*1024){await reader.cancel();throw new Failure("DOCUMENT_REQUIRES_LARGER_MEMORY_LANE",false);}chunks.push(v.value);}
  const rawBytes=new Uint8Array(bytes);let off=0;for(const c of chunks){rawBytes.set(c,off);off+=c.length;}
  const text=new TextDecoder("utf-8",{fatal:true}).decode(rawBytes);
  const wireHash=Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",rawBytes))).map(x=>x.toString(16).padStart(2,"0")).join("");
  return {body:asJson?JSON.parse(text):text,evidence:{provider:"Massive",route:path,http_status:r.status,request_id:r.headers.get("x-request-id"),response_date:r.headers.get("date"),content_type:r.headers.get("content-type"),bytes,raw_bytes_sha256:wireHash,request_started_at:started,received_at:new Date().toISOString()}};
 }
 try{
  const claim=await sql.unsafe(`select ${S}.financial_worker_claim_v1($1,$2) claim`,[execution,VERSION]);
  if(claim[0].claim.state!=="CLAIMED"){await sql.end();return Response.json({state:claim[0].claim.state,execution,version:VERSION});}
  job=claim[0].claim.job;
  if(!/^\d{10}-\d{2}-\d{6}$/.test(job.accession_number)||!/^[a-zA-Z0-9_.-]+$/.test(job.file_id))throw new Failure("INVALID_DOCUMENT_KEY",false);
  const exists=await sql.unsafe(`select document_sha256,octet_length(raw_document) bytes from ${S}.financial_documents_v1 where accession_number=$1 and file_id=$2`,[job.accession_number,job.file_id]);
  if(!exists.length){
   const meta=await fetchMassive("/v1/reference/sec/filings/"+job.accession_number,true);
   const m=meta.body.results??meta.body;
   if(m.accession_number!==job.accession_number||!Array.isArray(m.entities))throw new Failure("PROVIDER_IDENTITY_MISMATCH",false);
   const allowed=m.main_file_url?new URL(m.main_file_url):null;
   if(!allowed||allowed.hostname!=="api.massive.com"||allowed.pathname!=="/v1/reference/sec/filings/"+job.accession_number+"/files/"+job.file_id)throw new Failure("PRIMARY_FILE_MISMATCH",false);
   const doc=await fetchMassive(allowed.pathname,false);
   if(!/<(?:[a-zA-Z0-9_-]+:)?html[\s>]/i.test(doc.body)||!doc.body.includes("inlineXBRL"))throw new Failure("NO_SUPPORTED_INLINE_XBRL",false);
   const hash=await sha(doc.body);
   await sql.begin(async tx=>{
    await guard(tx);
    await tx.unsafe(`select ${S}.financial_worker_save_metadata_v1($1::text::jsonb)`,[JSON.stringify(m)]);
    await tx.unsafe(`insert into ${S}.financial_evidence_v1(evidence_id,evidence_type,route,payload) values($1,'MASSIVE_METADATA_RETRIEVAL',$2,$3::text::jsonb) on conflict do nothing`,["worker:"+job.job_id+":metadata",meta.evidence.route,JSON.stringify({request:meta.evidence,response:meta.body})]);
    await tx.unsafe(`insert into ${S}.financial_documents_v1(accession_number,file_id,route,provider,raw_document,document_sha256,provider_metadata,collected_at) values($1,$2,$3,'Massive',$4,$5,$6::text::jsonb,$7) on conflict do nothing`,[job.accession_number,job.file_id,doc.evidence.route,doc.body,hash,JSON.stringify({...doc.evidence,version:VERSION}),doc.evidence.received_at]);
    await tx.unsafe(`update ${S}.financial_worker_jobs_v1 set checkpoint='DOCUMENT_SAVED',retrieval_count=retrieval_count+1,updated_at=clock_timestamp() where job_id=$1 and lease_token=$2`,[job.job_id,job.lease_token]);
   });
   await event("DOCUMENT_SAVED",{sha256:hash,bytes:doc.evidence.bytes,route:doc.evidence.route,received_at:doc.evidence.received_at});
  }else{await event("DOCUMENT_REUSED",{sha256:exists[0].document_sha256,bytes:exists[0].bytes});}
  if(job.interrupt_after_document&&!job.interruption_done){
   await sql.begin(async tx=>{await guard(tx);await tx.unsafe(`update ${S}.financial_worker_jobs_v1 set interruption_done=true,checkpoint='DOCUMENT_SAVED',updated_at=clock_timestamp() where job_id=$1 and lease_token=$2`,[job.job_id,job.lease_token]);});
   await event("INTENTIONAL_WORKER_INTERRUPTION",{boundary:"after durable document commit; before extraction; lease intentionally left unacknowledged"});
   await sql.end();return Response.json({state:"INTERRUPTED_FOR_RECOVERY_TEST",execution},{status:503});
  }
  let result:any;
  await sql.begin(async tx=>{
   await guard(tx);
   const parsed=await tx.unsafe(`select ${S}.extract_financial_document_v1($1,$2) result`,[job.accession_number,job.file_id]);
   if(parsed[0].result.error||Number(parsed[0].result.facts)===0)throw new Failure("EXTRACTION_FAILED",false);
   const valid=await tx.unsafe(`select count(*)::int total,count(*) filter(where extraction_state='PARSED')::int parsed,count(*) filter(where extraction_state='PARSED' and (entity_cik is null or not exists(select 1 from ${S}.financial_filing_metadata_v1 m,jsonb_array_elements(m.raw_payload->'entities') e where m.accession_number=f.accession_number and lpad(e#>>'{company_data,cik}',10,'0')=lpad(f.entity_cik,10,'0'))))::int identity_conflicts from ${S}.financial_fact_v1 f where accession_number=$1 and file_id=$2`,[job.accession_number,job.file_id]);
   if(valid[0].identity_conflicts>0)throw new Failure("EXTRACTED_ENTITY_MISMATCH",false);
   const derived=await tx.unsafe(`select ${S}.financial_reconstruct_metrics_v1($1,$2) result`,[job.accession_number,job.file_id]);
   result={...parsed[0].result,...valid[0],derived:derived[0].result,classification:"CONDITIONAL",original_provider_receipt_proven:false};
   await tx.unsafe(`update ${S}.financial_worker_jobs_v1 set status='COMPLETE',checkpoint='VERIFIED_PERSISTED',result=$3::text::jsonb,lease_expires_at=null,completed_at=clock_timestamp(),updated_at=clock_timestamp(),last_error_class=null,last_error=null where job_id=$1 and lease_token=$2`,[job.job_id,job.lease_token,JSON.stringify(result)]);
   await tx.unsafe(`insert into ${S}.financial_worker_events_v1(job_id,event,execution_id,lease_token,details) values($1,'BATCH_COMPLETE',$2,$3,$4::text::jsonb)`,[job.job_id,execution,job.lease_token,JSON.stringify({...result,isolate,version:VERSION})]);
  });
  await sql.end();return Response.json({state:"COMPLETE",execution,job_id:job.job_id,result});
 }catch(e){
  const f=e instanceof Failure?e:new Failure(e instanceof Error&&e.name==="TimeoutError"?"PROVIDER_TIMEOUT":"TRANSIENT_RUNTIME_OR_DATABASE_ERROR",true);
  if(job){
   try{
    const message=f.code;
    await sql.unsafe(`update ${S}.financial_worker_jobs_v1 set status=case when $3 and attempts<max_attempts then 'RETRY' else 'BLOCKED' end,last_error_class=$4,last_error=$4,next_attempt_at=clock_timestamp()+($5*interval '1 second'),lease_expires_at=null,updated_at=clock_timestamp() where job_id=$1 and lease_token=$2 and status='LEASED'`,[job.job_id,job.lease_token,f.retryable,message,Math.max(f.delay,Math.min(900,30*2**job.attempts))]);
    await event("ATTEMPT_FAILED",{error_class:f.code,retryable:f.retryable,error_type:e instanceof Error?e.name:"Unknown",sqlstate:typeof (e as any)?.code==="string"?(e as any).code:null});
    if(["MASSIVE_CREDENTIAL_MISSING","PROVIDER_HTTP_401","PROVIDER_HTTP_403"].includes(f.code))await sql.unsafe(`update ${S}.financial_worker_control_v1 set stopped=true,stop_reason=$1,updated_at=clock_timestamp() where lane='financials'`,[f.code]);
   }catch{}
  }
  await sql.end({timeout:2}).catch(()=>{});return Response.json({state:"FAILED",error_class:f.code,execution},{status:503});
 }
});