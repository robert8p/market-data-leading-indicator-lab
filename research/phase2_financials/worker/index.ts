import postgres from "npm:postgres@3.4.5";
const VERSION="phase2-persistent-v2.0.0";
const AUTH_HASH="dd33639336a84139f03a556cb926ab13524e0daa85a22405a36a620873ad566d";
const S="research_eq10_fundamental_pit_20261020";
const sha=async(s:string)=>Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",new TextEncoder().encode(s)))).map(x=>x.toString(16).padStart(2,"0")).join("");
class Failure extends Error {constructor(public code:string,public retryable:boolean,public delay=60){super(code);}}
class Interrupted extends Error {}
Deno.serve(async(req:Request)=>{
 if(req.method!=="POST")return new Response("Method not allowed",{status:405});
 if(await sha((req.headers.get("authorization")||"").replace(/^Bearer /,""))!==AUTH_HASH)return new Response("Unauthorized",{status:401});
 const execution=crypto.randomUUID(),isolate=Deno.env.get("SB_EXECUTION_ID")||null,started=Date.now(),deadline=started+40000;
 const dbUrl=Deno.env.get("SUPABASE_DB_URL");
 if(!dbUrl)return Response.json({error:"DB_CONFIGURATION_MISSING"},{status:503});
 const sql=postgres(dbUrl,{max:1,prepare:false,idle_timeout:5,connect_timeout:8,connection:{application_name:"phase2_financial_worker_v2",statement_timeout:"12000",lock_timeout:"1500",max_parallel_workers_per_gather:"0"}});
 let job:any,allowPage=true,maxUnits=4;const outcomes:any[]=[];
 const event=async(name:string,details:any={})=>{await sql.unsafe(`insert into ${S}.financial_worker_events_v1(job_id,event,execution_id,lease_token,details) values($1,$2,$3,$4,$5::text::jsonb)`,[job?.job_id||null,name,execution,job?.lease_token||null,JSON.stringify({...details,isolate,version:VERSION})]);};
 async function guard(tx:any){
  const r=await tx.unsafe(`select j.job_id from ${S}.financial_worker_jobs_v1 j cross join ${S}.financial_worker_control_v1 c where j.job_id=$1 and j.lease_token=$2 and j.status='LEASED' and j.lease_expires_at>clock_timestamp() and c.lane='financials' and not c.stopped for update of j`,[job.job_id,job.lease_token]);
  if(!r.length)throw new Failure("LEASE_LOST_OR_STOPPED",false);
 }
 async function fetchMassive(path:string,asJson:boolean,params:Record<string,string>={}){
  if(path!=="/v1/reference/sec/filings"&&!/^\/v1\/reference\/sec\/filings\/[0-9]{10}-[0-9]{2}-[0-9]{6}(\/files\/[^/?#]+)?$/.test(path))throw new Failure("ROUTE_NOT_ALLOWED",false);
  const key=(Deno.env.get("MASSIVE_API_KEY")||Deno.env.get("MASSIVE_KEY")||Deno.env.get("POLYGON_API_KEY")||Deno.env.get("POLYGON_KEY")||"").trim();
  if(!key)throw new Failure("MASSIVE_CREDENTIAL_MISSING",false);
  const url=new URL("https://api.massive.com"+path);
  for(const [k,v]of Object.entries(params)){if(!["cursor","limit","sort","order","filing_date.gte","filing_date.lte","type"].includes(k))throw new Failure("UNSUPPORTED_QUERY_KEY",false);url.searchParams.set(k,String(v));}
  const remaining=deadline-Date.now();if(remaining<1000)throw new Failure("REQUEST_TIME_BUDGET",true);
  const requestStarted=new Date().toISOString();
  const r=await fetch(url,{headers:{Authorization:"Bearer "+key},redirect:"error",signal:AbortSignal.timeout(Math.min(25000,remaining))});
  if(!r.ok){await event("PROVIDER_ERROR",{route:path,status:r.status});throw new Failure("PROVIDER_HTTP_"+r.status,r.status===429||r.status>=500,Math.min(900,Math.max(60,Number(r.headers.get("retry-after"))||60)));}
  const reader=r.body!.getReader();let bytes=0;const chunks:Uint8Array[]=[];
  while(true){const v=await reader.read();if(v.done)break;bytes+=v.value.length;if(bytes>8*1024*1024){await reader.cancel();throw new Failure("RESPONSE_REQUIRES_LARGER_MEMORY_LANE",false);}chunks.push(v.value);}
  const rawBytes=new Uint8Array(bytes);let off=0;for(const c of chunks){rawBytes.set(c,off);off+=c.length;}
  const text=new TextDecoder("utf-8",{fatal:true}).decode(rawBytes);
  const wireHash=Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",rawBytes))).map(x=>x.toString(16).padStart(2,"0")).join("");
  return {raw:text,body:asJson?JSON.parse(text):text,evidence:{provider:"Massive",route:path,params,http_status:r.status,request_id:r.headers.get("x-request-id"),response_date:r.headers.get("date"),content_type:r.headers.get("content-type"),bytes,raw_bytes_sha256:wireHash,request_started_at:requestStarted,received_at:new Date().toISOString()}};
 }
 function mainPath(m:any,accession:string){
  let u:URL;try{u=new URL(m.main_file_url);}catch{throw new Failure("PRIMARY_FILE_UNAVAILABLE",false);}
  if(u.protocol!=="https:"||u.hostname!=="api.massive.com"||u.search||u.hash||u.username||u.password||!new RegExp("^/v1/reference/sec/filings/"+accession+"/files/[A-Za-z0-9_.-]+[.]html?$").test(u.pathname))throw new Failure("UNSUPPORTED_PRIMARY_FILE_ROUTE",false);
  return u.pathname;
 }
 async function finish(tx:any,result:any,eventName="BATCH_COMPLETE"){
  await tx.unsafe(`update ${S}.financial_worker_jobs_v1 set status='COMPLETE',checkpoint='VERIFIED_PERSISTED',result=$3::text::jsonb,lease_expires_at=null,completed_at=clock_timestamp(),updated_at=clock_timestamp(),last_error_class=null,last_error=null where job_id=$1 and lease_token=$2`,[job.job_id,job.lease_token,JSON.stringify(result)]);
  await tx.unsafe(`update ${S}.financial_worker_control_v1 set last_useful_progress_at=clock_timestamp() where lane='financials'`);
  await tx.unsafe(`insert into ${S}.financial_worker_events_v1(job_id,event,execution_id,lease_token,details) values($1,$2,$3,$4,$5::text::jsonb)`,[job.job_id,eventName,execution,job.lease_token,JSON.stringify({...result,isolate,version:VERSION})]);
 }
 async function interruptIfRequested(checkpoint:string){
  if(job.interrupt_after_document&&!job.interruption_done){
   await sql.begin(async tx=>{await guard(tx);await tx.unsafe(`update ${S}.financial_worker_jobs_v1 set interruption_done=true,checkpoint=$3,updated_at=clock_timestamp() where job_id=$1 and lease_token=$2`,[job.job_id,job.lease_token,checkpoint]);});
   await event("INTENTIONAL_WORKER_INTERRUPTION",{boundary:"after durable "+checkpoint+"; lease intentionally unacknowledged"});throw new Interrupted();
  }
 }
 async function processPage(){
  const pageKey=job.job_payload.page_key,params=job.job_payload.params;
  if(!/^metadata_20230101_20260731_[0-9]{4,}$/.test(pageKey)||!params?.cursor)throw new Failure("INVALID_PAGE_JOB",false);
  const saved=await sql.unsafe(`select response_sha256 from ${S}.financial_metadata_pages_raw_v2 where page_key=$1`,[pageKey]);
  if(!saved.length){
   const page=await fetchMassive("/v1/reference/sec/filings",true,params);const body=page.body;
   if(!Array.isArray(body.results)||body.results.length>1000||body.status==="ERROR")throw new Failure("INVALID_PAGE_RESPONSE",false);
   let next:any=null;if(body.next_url){
    const u=new URL(body.next_url,"https://api.massive.com");
    if(u.protocol!=="https:"||u.hostname!=="api.massive.com"||u.pathname!=="/v1/reference/sec/filings"||!u.searchParams.get("cursor"))throw new Failure("INVALID_PAGINATION_ROUTE",false);
    next={cursor:u.searchParams.get("cursor")};if(next.cursor===params.cursor)throw new Failure("PAGINATION_CURSOR_NOT_ADVANCING",false);
   }
   const hash=await sha(page.raw);
   await sql.begin(async tx=>{await guard(tx);
    await tx.unsafe(`insert into ${S}.financial_metadata_pages_raw_v2(page_key,route,request_params,raw_response,response_sha256,provider_evidence,next_params,received_at)values($1,$2,$3::text::jsonb,$4,$5,$6::text::jsonb,$7::text::jsonb,$8) on conflict do nothing`,[pageKey,"/v1/reference/sec/filings",JSON.stringify(params),page.raw,hash,JSON.stringify({...page.evidence,provider_request_id:body.request_id,version:VERSION}),next?JSON.stringify(next):null,page.evidence.received_at]);
    await tx.unsafe(`update ${S}.financial_worker_jobs_v1 set checkpoint='PAGE_SAVED',retrieval_count=retrieval_count+1,updated_at=clock_timestamp() where job_id=$1 and lease_token=$2`,[job.job_id,job.lease_token]);
   });await event("METADATA_PAGE_SAVED",{page_key:pageKey,rows:body.results.length,sha256:hash,bytes:page.evidence.bytes});
  }else await event("METADATA_PAGE_REUSED",{page_key:pageKey,sha256:saved[0].response_sha256});
  await interruptIfRequested("PAGE_SAVED");
  const r=await sql.unsafe(`select ${S}.financial_worker_import_page_v2($1,$2) result`,[job.job_id,job.lease_token]);return r[0].result;
 }
 async function processLookup(){
  const response=await fetchMassive("/v1/reference/sec/filings/"+job.accession_number,true),m=response.body.results??response.body;
  if(m.accession_number!==job.accession_number||!Array.isArray(m.entities))throw new Failure("PROVIDER_IDENTITY_MISMATCH",false);
  let path:string|null=null,reason:string|null=null;try{path=mainPath(m,job.accession_number);}catch(e){reason=(e as Failure).code;}
  const result={metadata_saved:true,document_queued:!!path,hold_reason:reason};
  await sql.begin(async tx=>{await guard(tx);
   await tx.unsafe(`select ${S}.financial_worker_save_metadata_v1($1::text::jsonb)`,[JSON.stringify(m)]);
   await tx.unsafe(`insert into ${S}.financial_evidence_v1(evidence_id,evidence_type,route,payload) values($1,'MASSIVE_METADATA_RETRIEVAL',$2,$3::text::jsonb) on conflict do nothing`,["worker:"+job.job_id+":metadata",response.evidence.route,JSON.stringify({request:response.evidence,response:response.body})]);
   if(path){const file=path.split("/").pop()!;await tx.unsafe(`insert into ${S}.financial_worker_jobs_v1(job_id,accession_number,file_id,job_kind,priority) values($1,$2,$3,'DOCUMENT',10) on conflict do nothing`,["document:"+job.accession_number+":"+file,job.accession_number,file]);}
   else await tx.unsafe(`insert into ${S}.financial_ingest_issues_v2(issue_id,job_id,accession_number,reason,classification,evidence)values($1,$2,$3,$4,'EXCLUDED',$5::text::jsonb) on conflict do nothing`,[job.job_id+":primary",job.job_id,job.accession_number,reason,JSON.stringify({route:response.evidence.route})]);
   await finish(tx,result,"FILING_LOOKUP_COMPLETE");
  });return result;
 }
 async function processDocument(){
  if(!/^\d{10}-\d{2}-\d{6}$/.test(job.accession_number)||!/^[a-zA-Z0-9_.-]+$/.test(job.file_id))throw new Failure("INVALID_DOCUMENT_KEY",false);
  const saved=await sql.unsafe(`select document_sha256,octet_length(raw_document) bytes from ${S}.financial_documents_v1 where accession_number=$1 and file_id=$2`,[job.accession_number,job.file_id]);
  if(!saved.length){
   const existing=await sql.unsafe(`select raw_payload from ${S}.financial_filing_metadata_v1 where accession_number=$1`,[job.accession_number]);
   let m:any=existing[0]?.raw_payload,meta:any=null;
   if(!m){meta=await fetchMassive("/v1/reference/sec/filings/"+job.accession_number,true);m=meta.body.results??meta.body;}
   if(m.accession_number!==job.accession_number||!Array.isArray(m.entities))throw new Failure("PROVIDER_IDENTITY_MISMATCH",false);
   const path=mainPath(m,job.accession_number);if(path.split("/").pop()!==job.file_id)throw new Failure("PRIMARY_FILE_MISMATCH",false);
   const doc=await fetchMassive(path,false),hash=await sha(doc.body);
   await sql.begin(async tx=>{await guard(tx);
    if(meta){await tx.unsafe(`select ${S}.financial_worker_save_metadata_v1($1::text::jsonb)`,[JSON.stringify(m)]);await tx.unsafe(`insert into ${S}.financial_evidence_v1(evidence_id,evidence_type,route,payload)values($1,'MASSIVE_METADATA_RETRIEVAL',$2,$3::text::jsonb) on conflict do nothing`,["worker:"+job.job_id+":metadata",meta.evidence.route,JSON.stringify({request:meta.evidence,response:meta.body})]);}
    await tx.unsafe(`insert into ${S}.financial_documents_v1(accession_number,file_id,route,provider,raw_document,document_sha256,provider_metadata,collected_at) values($1,$2,$3,'Massive',$4,$5,$6::text::jsonb,$7) on conflict do nothing`,[job.accession_number,job.file_id,doc.evidence.route,doc.body,hash,JSON.stringify({...doc.evidence,version:VERSION,metadata_reused:!meta}),doc.evidence.received_at]);
    await tx.unsafe(`update ${S}.financial_worker_jobs_v1 set checkpoint='DOCUMENT_SAVED',retrieval_count=retrieval_count+1,updated_at=clock_timestamp() where job_id=$1 and lease_token=$2`,[job.job_id,job.lease_token]);
   });await event("DOCUMENT_SAVED",{sha256:hash,bytes:doc.evidence.bytes,route:doc.evidence.route});
   if(!doc.body.includes("inlineXBRL"))throw new Failure("NO_SUPPORTED_INLINE_XBRL",false);
  }else await event("DOCUMENT_REUSED",{sha256:saved[0].document_sha256,bytes:saved[0].bytes});
  await interruptIfRequested("DOCUMENT_SAVED");
  let result:any;
  await sql.begin(async tx=>{await guard(tx);
   const p=await tx.unsafe(`select ${S}.extract_financial_document_v1($1,$2) result`,[job.accession_number,job.file_id]);
   if(p[0].result.error||Number(p[0].result.facts)===0)throw new Failure("EXTRACTION_FAILED",false);
   const v=await tx.unsafe(`select count(*)::int total,count(*) filter(where extraction_state='PARSED')::int parsed,count(*) filter(where extraction_state='PARSED' and(entity_cik is null or not exists(select 1 from ${S}.financial_filing_metadata_v1 m,jsonb_array_elements(m.raw_payload->'entities') e where m.accession_number=f.accession_number and lpad(e#>>'{company_data,cik}',10,'0')=lpad(f.entity_cik,10,'0'))))::int identity_conflicts from ${S}.financial_fact_v1 f where accession_number=$1 and file_id=$2`,[job.accession_number,job.file_id]);
   if(v[0].identity_conflicts>0)throw new Failure("EXTRACTED_ENTITY_MISMATCH",false);
   const d=await tx.unsafe(`select ${S}.financial_reconstruct_metrics_v1($1,$2) result`,[job.accession_number,job.file_id]);
   result={...p[0].result,...v[0],derived:d[0].result,classification:"CONDITIONAL",original_provider_receipt_proven:false};
   await finish(tx,result);
  });return result;
 }
 async function failure(e:any){
  const f=e instanceof Failure?e:new Failure(e?.name==="TimeoutError"?"PROVIDER_TIMEOUT":"TRANSIENT_RUNTIME_OR_DATABASE_ERROR",true);
  if(job){
   const pgcode=typeof e?.code==="string"&&/^[0-9A-Z]{5}$/.test(e.code)?e.code:null;
   await sql.unsafe(`update ${S}.financial_worker_jobs_v1 set status=case when $3 and attempts<max_attempts then 'RETRY' else 'BLOCKED' end,last_error_class=$4,last_error=$4,next_attempt_at=clock_timestamp()+($5*interval '1 second'),lease_expires_at=null,updated_at=clock_timestamp() where job_id=$1 and lease_token=$2 and status='LEASED'`,[job.job_id,job.lease_token,f.retryable,f.code,Math.max(f.delay,Math.min(900,30*2**job.attempts))]);
   await event("ATTEMPT_FAILED",{error_class:f.code,retryable:f.retryable,error_type:e?.name,sqlstate:pgcode,detail:pgcode?String(e?.message).slice(0,240):null});
   if(["MASSIVE_CREDENTIAL_MISSING","PROVIDER_HTTP_401","PROVIDER_HTTP_403"].includes(f.code))await sql.unsafe(`update ${S}.financial_worker_control_v1 set stopped=true,stop_reason=$1,updated_at=clock_timestamp() where lane='financials'`,[f.code]);
  }
  return {state:"FAILED",job_id:job?.job_id,error_class:f.code};
 }
 try{
  await sql.unsafe(`select ${S}.financial_worker_refill_v2()`);
  for(let unit=0;unit<maxUnits&&Date.now()<deadline-8000;unit++){
   job=null;
   try{
    const c=await sql.unsafe(`select ${S}.financial_worker_claim_v2($1,$2,$3) claim`,[execution,VERSION,allowPage]);
    if(c[0].claim.state!=="CLAIMED"){outcomes.push({state:c[0].claim.state});break;}
    job=c[0].claim.job;maxUnits=c[0].claim.max_units;
    let result:any;
    if(job.job_kind==="METADATA_PAGE"){allowPage=false;result=await processPage();}
    else if(job.job_kind==="FILING_LOOKUP")result=await processLookup();
    else result=await processDocument();
    outcomes.push({state:"COMPLETE",job_id:job.job_id,kind:job.job_kind,result});
   }catch(e){
    if(e instanceof Interrupted){await sql.end();return Response.json({state:"INTERRUPTED_FOR_RECOVERY_TEST",execution,outcomes},{status:503});}
    outcomes.push(await failure(e));if((e as any)?.code==="LEASE_LOST_OR_STOPPED")break;
   }
  }
  await sql.unsafe(`select ${S}.financial_worker_refill_v2()`);
  await sql.end();return Response.json({state:"BATCH_FINISHED",execution,version:VERSION,outcomes,elapsed_ms:Date.now()-started});
 }catch(e){await sql.end({timeout:2}).catch(()=>{});return Response.json({state:"REQUEST_FAILED",execution,error_type:e instanceof Error?e.name:"Unknown"},{status:503});}
});
