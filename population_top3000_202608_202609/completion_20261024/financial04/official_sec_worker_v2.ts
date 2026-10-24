// Scoped official SEC original financial documents. Deployment root only. No provider credentials, retries or invented historical receipts.
import postgres from "npm:postgres@3.4.5";
const PROJECT_URL="https://oxzabweahkoimtevbbny.supabase.co";
const MAX_BYTES=8_388_608,MAX_RUNTIME_MS=45_000;
const encoder=new TextEncoder();
class WorkerError extends Error {code:string;constructor(code:string){super(code);this.code=code;}}
const output=(v:unknown,status=200)=>Response.json(v,{status,headers:{"cache-control":"no-store","x-content-type-options":"nosniff"}});
function left(deadline:number,reserve=0){return Math.max(0,deadline-Date.now()-reserve);}
async function timed<T>(p:PromiseLike<T>,ms:number):Promise<T>{
 if(ms<=0)throw new WorkerError("DEADLINE");let timer:number|undefined;
 try{return await Promise.race([Promise.resolve(p),new Promise<never>((_,reject)=>{timer=setTimeout(()=>reject(new WorkerError("DEADLINE")),ms);})]);}
 finally{if(timer!==undefined)clearTimeout(timer);}
}
async function sha(bytes:Uint8Array){return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",bytes)),b=>b.toString(16).padStart(2,"0")).join("");}
async function readLimited(stream:ReadableStream<Uint8Array>|null,cap:number,deadline:number,onBytes:(n:number)=>void=()=>{}){
 if(!stream)throw new WorkerError("EMPTY_BODY");const reader=stream.getReader();const chunks:Uint8Array[]=[];let length=0;
 try{while(true){const r=await timed(reader.read(),left(deadline));if(r.done)break;
  length+=r.value.length;onBytes(Math.min(length,cap));if(length>cap)throw new WorkerError("RESPONSE_TOO_LARGE");chunks.push(r.value);
 }const bytes=new Uint8Array(length);let off=0;for(const c of chunks){bytes.set(c,off);off+=c.length;}return bytes;}
 finally{void reader.cancel().catch(()=>{});}
}
type Claimed={state:string;document_key:string;request_manifest_sha256:string;request_url:string;cik:string;accession_number:string;file_id:string;expected_file_bytes:number;max_response_bytes:number;max_runtime_ms:number;maximum_provider_requests:number;family_id:string;partition_key:string};
function validateRequestUrl(c:Claimed){
 if(!/^[0-9]{10}$/.test(c.cik)||c.cik==="0000000000"||!/^[0-9]{10}-[0-9]{2}-[0-9]{6}$/.test(c.accession_number)||!/^[A-Za-z0-9][A-Za-z0-9._-]{0,200}\.(htm|html|xml)$/.test(c.file_id))throw new WorkerError("UNADMITTED_SOURCE_URL");
 const expected="https://www.sec.gov/Archives/edgar/data/"+c.cik.replace(/^0+/,"")+"/"+c.accession_number.replaceAll("-","")+"/"+c.file_id;
 if(c.request_url!==expected)throw new WorkerError("UNADMITTED_SOURCE_URL");return expected;
}
function failureClass(code:string,status:number|null){
 if(status===401||status===403||code==="SOURCE_CREDENTIAL_UNAVAILABLE")return "AUTHENTICATION";
 if(status===429)return "RATE_LIMIT";if(status===404)return "SOURCE_ABSENT";
 if(status!==null&&status>=500)return "TRANSPORT_READ";
 if(["DEADLINE","FETCH_FAILED","EMPTY_BODY"].includes(code))return "TRANSPORT_READ";
 return "PARSER";
}
Deno.serve(async(req:Request)=>{
 const started=Date.now(),deadline=started+MAX_RUNTIME_MS;
 let db:ReturnType<typeof postgres>|null=null,phase="VALIDATE",claimed:Claimed|null=null;
 let attemptId="",token="",status:number|null=null,receivedBytes=0,wireHash:string|null=null,fetchStarted=false;
 const txCall=async(name:"claim"|"commit"|"failure",payload?:unknown)=>{
  if(!db)throw new WorkerError("DATABASE_UNAVAILABLE");
  return await timed(db.begin(async(tx)=>{
   await tx`set local statement_timeout='5s'`;await tx`set local lock_timeout='2s'`;
   let rows;
   if(name==="claim")rows=await tx`select population_top3000_202608_202609_v1.financial_sec_claim_v2(${attemptId}::uuid,${token}::text) as result`;
   else if(name==="commit")rows=await tx`select population_top3000_202608_202609_v1.financial_sec_commit_v2(${attemptId}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
   else rows=await tx`select population_top3000_202608_202609_v1.financial_sec_failure_v2(${attemptId}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
   return rows[0].result;
  }),Math.min(8000,left(deadline,50)));
 };
 try{
  if(req.method!=="POST")return output({error:"MethodNotAllowed"},405);
  if(Deno.env.get("SUPABASE_URL")!==PROJECT_URL)return output({error:"ProjectMismatch"},409);
  if(req.headers.get("content-type")?.split(";")[0].trim().toLowerCase()!=="application/json")return output({error:"InvalidRequest"},400);
  if(Number(req.headers.get("content-length")||0)>512)return output({error:"InvalidRequest"},413);
  let body;try{body=JSON.parse(new TextDecoder("utf-8",{fatal:true}).decode(await readLimited(req.body,512,Math.min(deadline,Date.now()+700))));}catch{return output({error:"InvalidRequest"},400);}
  if(!body||typeof body!=="object"||Array.isArray(body)||Object.keys(body).sort().join(",")!=="attempt_id,token"||typeof body.attempt_id!=="string"||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(body.attempt_id)||typeof body.token!=="string"||!/^[0-9a-f]{64}$/.test(body.token))return output({error:"InvalidRequest"},400);
  attemptId=body.attempt_id;token=body.token;
  const dbUrl=Deno.env.get("SUPABASE_DB_URL");if(!dbUrl)return output({error:"WorkerControlUnavailable"},503);
  db=postgres(dbUrl,{max:1,prepare:false,ssl:"require",connect_timeout:5,idle_timeout:30,max_lifetime:60,debug:false,onnotice:()=>{}});
  phase="CLAIM";claimed=await txCall("claim");
  if(claimed?.state!=="RESERVED")return output({state:claimed?.state||"NOT_ADMITTED",provider_requested:false},200);
  if(claimed.family_id!=="F03"||claimed.partition_key!==claimed.document_key||!/^financial-document-v1:[0-9a-f]{64}$/.test(claimed.document_key)||!/^[0-9a-f]{64}$/.test(claimed.request_manifest_sha256)||claimed.max_response_bytes!==MAX_BYTES||claimed.max_runtime_ms!==MAX_RUNTIME_MS||claimed.maximum_provider_requests!==1||!Number.isSafeInteger(claimed.expected_file_bytes)||claimed.expected_file_bytes<1||claimed.expected_file_bytes>MAX_BYTES)throw new WorkerError("INVALID_CLAIM");
  const requestRedacted=validateRequestUrl(claimed);
  phase="FETCH";
  const fetchMs=Math.min(25000,left(deadline,12000));if(fetchMs<100)throw new WorkerError("DEADLINE");
  const fetchDeadline=Date.now()+fetchMs;fetchStarted=true;
  let response:Response;try{response=await fetch(requestRedacted,{method:"GET",headers:{"User-Agent":"Top3000-Company-Mission/1.0 (bounded original SEC document preservation)","Accept-Encoding":"identity",Accept:"application/xml,text/html;q=0.9,*/*;q=0.1"},redirect:"error",signal:AbortSignal.timeout(fetchMs)});}catch{throw new WorkerError("FETCH_FAILED");}
  status=response.status;
  if(status!==200){void response.body?.cancel().catch(()=>{});const retry=response.headers.get("retry-after");const retrySeconds=retry&&/^\d+$/.test(retry)?Math.min(3600,Math.max(1,Number(retry))):60;
   phase="FAILURE";const result=await txCall("failure",{code:"HTTP_"+status,failure_class:failureClass("HTTP_"+status,status),http_status:status,received_bytes:0,retry_after_seconds:retrySeconds,fetch_started:true,wire_sha256:null});
   return output({state:result.state,provider_http_status:status,receipt_recorded:true},200);
  }
  const declared=response.headers.get("content-length");if(declared&&Number(declared)>MAX_BYTES){void response.body?.cancel().catch(()=>{});throw new WorkerError("RESPONSE_TOO_LARGE");}
  const bytes=await readLimited(response.body,MAX_BYTES,fetchDeadline,n=>{receivedBytes=n;});wireHash=await sha(bytes);
  const raw=new TextDecoder("utf-8",{fatal:true,ignoreBOM:true}).decode(bytes);
  const sourceHash=await sha(encoder.encode(raw));
  if(sourceHash!==wireHash||encoder.encode(raw).length!==bytes.length||bytes.length!==claimed.expected_file_bytes)throw new WorkerError("EXACT_BODY_PROOF_MISMATCH");
  phase="FINISH";
  const result=await txCall("commit",{document_key:claimed.document_key,request_manifest_sha256:claimed.request_manifest_sha256,request_url:requestRedacted,content_text:raw,source_sha256:sourceHash,wire_sha256:wireHash,wire_bytes:receivedBytes,http_status:status,retrieved_at:new Date().toISOString(),elapsed_ms:Date.now()-started});
  return output({state:result.state,document_key:claimed.document_key,source_sha256:sourceHash,wire_bytes:receivedBytes,receipt_recorded:true},200);
 }catch(error){
  const allowed=new Set(["DEADLINE","EMPTY_BODY","SOURCE_CREDENTIAL_UNAVAILABLE","INVALID_NEXT_URL","UNADMITTED_SOURCE_URL","INVALID_SOURCE_JSON","DUPLICATE_SOURCE_KEY","SOURCE_NESTING_LIMIT","INVALID_SOURCE_ENVELOPE","INVALID_SOURCE_COUNT","INVALID_SOURCE_RECORD","SOURCE_CREDENTIAL_ECHO","REDACTED_RESPONSE_TOO_LARGE","RESPONSE_TOO_LARGE","INVALID_CLAIM","FETCH_FAILED","REPEATED_PAGE_CURSOR","EXACT_BODY_PROOF_MISMATCH"]);
  const code=error instanceof WorkerError&&allowed.has(error.code)?error.code:"CONTROL_OR_PARSE_FAILURE";
  if(claimed?.state==="RESERVED"&&phase!=="FINISH"&&phase!=="FAILURE"&&left(deadline)>1000){
   try{phase="FAILURE";const result=await txCall("failure",{code,failure_class:failureClass(code,status),http_status:status,received_bytes:receivedBytes,retry_after_seconds:60,fetch_started:fetchStarted,wire_sha256:wireHash});
    return output({state:result.state,code,receipt_recorded:true},200);
   }catch{/* Unknown DB effect remains in the fenced lease for reconciliation. */}
  }
  // No raw errors/URLs/payloads/environment values leave the worker.
  return output({error:phase==="CLAIM"?"AttemptNotAdmitted":"WorkerReceiptUnconfirmed",phase,code,receipt_recorded:false,provider_requested:fetchStarted},phase==="CLAIM"?403:503);
 }finally{if(db)void db.end({timeout:0}).catch(()=>{});}
});

