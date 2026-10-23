// Root deploys as population-top3000-evidence-v1 only after review/tests.
// verify_jwt=false requires the private DB capability check below BEFORE fetch.
// One fixed-origin FINRA request per atomically claimed attempt; no retries.
import postgres from "npm:postgres@3.4.5";
const PROJECT_URL="https://oxzabweahkoimtevbbny.supabase.co";
const MAX_BYTES=8_388_608;
const TOTAL_MS=45_000;
type Receipt={outcome:"RECEIVED"|"FAILED";http_status:number|null;received_at:string;
 content_text?:string;source_sha256?:string;content_bytes?:number;headers?:Record<string,string>;
 error_code?:string;retry_after_seconds:number};
class WorkerError extends Error {constructor(readonly code:string){super(code);}}
function json(body:unknown,status=200){return Response.json(body,{status,headers:{"cache-control":"no-store","x-content-type-options":"nosniff"}});}
function remaining(deadline:number,reserve=0){return Math.max(0,deadline-Date.now()-reserve);}
async function timed<T>(work:PromiseLike<T>,ms:number):Promise<T>{
 if(ms<=0)throw new WorkerError("DEADLINE");let timer:number|undefined;
 try{return await Promise.race([Promise.resolve(work),new Promise<never>((_,reject)=>{timer=setTimeout(()=>reject(new WorkerError("DEADLINE")),ms);})]);}
 finally{if(timer!==undefined)clearTimeout(timer);}
}
async function readLimited(stream:ReadableStream<Uint8Array>|null,max:number,deadline:number,signal:AbortSignal){
 if(!stream)throw new WorkerError("NETWORK_ERROR");const reader=stream.getReader();const chunks:Uint8Array[]=[];let total=0;
 const cancel=()=>{void reader.cancel().catch(()=>{});};signal.addEventListener("abort",cancel,{once:true});
 try{while(true){if(signal.aborted||Date.now()>=deadline)throw new WorkerError("PROVIDER_TIMEOUT");
  const next=await timed(reader.read(),remaining(deadline));if(next.done)break;
  total+=next.value.length;if(total>max)throw new WorkerError("RESPONSE_TOO_LARGE");chunks.push(next.value);
 }if(signal.aborted||Date.now()>=deadline)throw new WorkerError("PROVIDER_TIMEOUT");
 const bytes=new Uint8Array(total);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}return bytes;
 }finally{signal.removeEventListener("abort",cancel);cancel();}
}
async function sha256(bytes:Uint8Array){return Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",bytes)),b=>b.toString(16).padStart(2,"0")).join("");}
function retryAfter(value:string|null){if(!value)return 0;const seconds=/^[0-9]+$/.test(value)?Number(value):Math.ceil((Date.parse(value)-Date.now())/1000);
 return Number.isFinite(seconds)?Math.min(86400,Math.max(0,Math.ceil(seconds))):0;}
function fixedUrl(date:string){if(!/^2026-(08|09)-[0-9]{2}$/.test(date))throw new WorkerError("CLAIM_SCOPE_INVALID");
 return "https://cdn.finra.org/equity/regsho/daily/CNMSshvol"+date.replaceAll("-","")+".txt";}
async function collect(url:string,deadline:number):Promise<Receipt>{
 let status:number|null=null;let retry=0;
 try{const budget=Math.min(25000,remaining(deadline,10000));if(budget<=0)throw new WorkerError("PROVIDER_TIMEOUT");
  const fetchDeadline=Date.now()+budget;const signal=AbortSignal.timeout(budget);
  const response=await fetch(url,{method:"GET",headers:{Accept:"text/plain","Accept-Encoding":"identity"},redirect:"error",signal});
  status=response.status;retry=retryAfter(response.headers.get("retry-after"));
  if(status!==200){void response.body?.cancel().catch(()=>{});throw new WorkerError("HTTP_ERROR");}
  const declared=response.headers.get("content-length");if(declared&&Number(declared)>MAX_BYTES){void response.body?.cancel().catch(()=>{});throw new WorkerError("RESPONSE_TOO_LARGE");}
  const bytes=await readLimited(response.body,MAX_BYTES,fetchDeadline,signal);
  let text:string;try{text=new TextDecoder("utf-8",{fatal:true,ignoreBOM:true}).decode(bytes);}catch{throw new WorkerError("INVALID_UTF8");}
  const headers:Record<string,string>={};for(const name of ["content-type","content-encoding","etag","last-modified","date"]){const value=response.headers.get(name);if(value&&value.length<=1024)headers[name]=value;}
  return {outcome:"RECEIVED",http_status:status,received_at:new Date().toISOString(),content_text:text,
   source_sha256:await sha256(bytes),content_bytes:bytes.length,headers,retry_after_seconds:retry};
 }catch(error){const allowed=new Set(["HTTP_ERROR","RESPONSE_TOO_LARGE","PROVIDER_TIMEOUT","INVALID_UTF8","NETWORK_ERROR"]);
  const code=error instanceof WorkerError?(allowed.has(error.code)?error.code:error.code==="DEADLINE"?"PROVIDER_TIMEOUT":"UNEXPECTED_FAILURE"):
   error instanceof DOMException&&["AbortError","TimeoutError"].includes(error.name)?"PROVIDER_TIMEOUT":error instanceof TypeError?"NETWORK_ERROR":"UNEXPECTED_FAILURE";
  return {outcome:"FAILED",http_status:status,received_at:new Date().toISOString(),error_code:code,retry_after_seconds:retry};
 }
}
Deno.serve(async(req:Request)=>{
 const deadline=Date.now()+TOTAL_MS;let db:ReturnType<typeof postgres>|null=null;let claimed=false;
 let phase:"INPUT"|"CLAIM"|"FETCH"|"RECEIPT"="INPUT";
 try{
  if(req.method!=="POST")return json({error:"MethodNotAllowed"},405);
  if(Deno.env.get("SUPABASE_URL")!==PROJECT_URL)return json({error:"ProjectMismatch"},409);
  if(req.headers.get("content-type")?.split(";")[0].trim().toLowerCase()!=="application/json")return json({error:"InvalidRequest"},400);
  if(Number(req.headers.get("content-length")||0)>512)return json({error:"InvalidRequest"},413);
  let body:Record<string,unknown>;
  try{const inputDeadline=Date.now()+750;body=JSON.parse(new TextDecoder("utf-8",{fatal:true}).decode(await readLimited(req.body,512,inputDeadline,AbortSignal.timeout(750))));}
  catch{return json({error:"InvalidRequest"},400);}
  if(!body||typeof body!=="object"||Array.isArray(body)||Object.keys(body).sort().join(",")!=="attempt_id,token"
   ||typeof body.attempt_id!=="string"||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(body.attempt_id)
   ||typeof body.token!=="string"||!/^[0-9a-f]{64}$/.test(body.token))return json({error:"InvalidRequest"},400);
  const databaseUrl=Deno.env.get("SUPABASE_DB_URL");if(!databaseUrl)return json({error:"ControlUnavailable"},503);
  db=postgres(databaseUrl,{max:1,prepare:false,ssl:"require",connect_timeout:5,idle_timeout:30,max_lifetime:60,debug:false,onnotice:()=>{}});
  const attempt=body.attempt_id;const token=body.token;phase="CLAIM";
  const claim=await timed(db.begin(async(tx)=>{
   await tx`set local statement_timeout='3s'`;await tx`set local lock_timeout='2s'`;
   const rows=await tx`select population_top3000_202608_202609_v1.finra_claim_v1(${attempt}::uuid,${token}::text) as result`;
   return rows[0].result as {state:string;session_date:string;source_uri:string;max_response_bytes:number;provider_timeout_ms:number;total_timeout_ms:number};
  }),Math.min(7500,remaining(deadline,30000)));
  if(claim.state!=="CLAIMED")return json({error:"AttemptNotAdmitted"},403);
  claimed=true;
  const url=fixedUrl(claim.session_date);
  if(claim.source_uri!==url||claim.max_response_bytes!==MAX_BYTES||claim.provider_timeout_ms!==25000||claim.total_timeout_ms!==TOTAL_MS)
   return json({error:"ClaimScopeMismatch",requires_reconciliation:true},409);
  phase="FETCH";const receipt=await collect(url,deadline);phase="RECEIPT";
  const recorded=await timed(db.begin(async(tx)=>{
   await tx`set local statement_timeout='7s'`;await tx`set local lock_timeout='2s'`;
   // Pass the object to the driver's JSON helper. Never JSON.stringify a JSONB parameter.
   const rows=await tx`select population_top3000_202608_202609_v1.finra_receive_v1(${attempt}::uuid,${token}::text,${tx.json(receipt)}::jsonb) as result`;
   return rows[0].result as {state:string};
  }),Math.min(8500,remaining(deadline,100)));
  if(!["COMPLETE","BLOCKED","ALREADY_RECORDED"].includes(recorded.state))return json({error:"ReceiptNotConfirmed",requires_reconciliation:true},503);
  return json({state:recorded.state,attempt_id:attempt,provider_outcome:receipt.outcome,automatic_retry:false});
 }catch{
  // A claimed request with unknown receipt stays under its durable provider lease.
  // The existing expiry reconciler blocks uncertain effects; never fetch again here.
  return json({error:"ControlUnavailable",phase,requires_reconciliation:claimed,automatic_retry:false},503);
 }finally{if(db)void db.end({timeout:0}).catch(()=>{});}
});
