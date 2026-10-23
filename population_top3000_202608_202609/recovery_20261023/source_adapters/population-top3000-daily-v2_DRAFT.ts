// Deploy only after root review as population-top3000-daily-v2, verify_jwt=false.
// D02 v2: one grouped daily source request for one of 42 frozen sessions. No pagination or source retries.
// Exact UTF-8 response body is retained; this worker does not certify company membership or session-close semantics.
// No provider retries, arbitrary endpoints, new subscriptions, historical receipt invention or public logs.
import postgres from "npm:postgres@3.4.5";
const PROJECT_URL="https://oxzabweahkoimtevbbny.supabase.co";
const PROVIDER_ORIGIN="https://api.massive.com";
const CALENDAR_SCOPE_ID="corrected_company_scope_8e6ca1178f03822c";
const CALENDAR_DATE_LIST_SHA256="188cc82e93e2c3446d9fba86cec682b996913997f56a2263d3fd680475998368";
const TRADING_DATES=Object.freeze(["2026-08-03","2026-08-04","2026-08-05","2026-08-06","2026-08-07","2026-08-10","2026-08-11","2026-08-12","2026-08-13","2026-08-14","2026-08-17","2026-08-18","2026-08-19","2026-08-20","2026-08-21","2026-08-24","2026-08-25","2026-08-26","2026-08-27","2026-08-28","2026-08-31","2026-09-01","2026-09-02","2026-09-03","2026-09-04","2026-09-08","2026-09-09","2026-09-10","2026-09-11","2026-09-14","2026-09-15","2026-09-16","2026-09-17","2026-09-18","2026-09-21","2026-09-22","2026-09-23","2026-09-24","2026-09-25","2026-09-28","2026-09-29","2026-09-30"]);
const ALLOWED_DATES=new Set(TRADING_DATES);
const PATH_PREFIX="/v2/aggs/grouped/locale/us/market/stocks/";
const MAX_RECORDS=50_000;
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
function sourceKey(){for(const n of ["MASSIVE_API_KEY","MASSIVE_KEY","POLYGON_API_KEY","POLYGON_KEY"]){const k=Deno.env.get(n)?.trim();if(k)return k;}throw new WorkerError("SOURCE_CREDENTIAL_UNAVAILABLE");}
function admittedDate(value:unknown):value is string{return typeof value==="string"&&ALLOWED_DATES.has(value);}
function requestUrlFor(tradingDate:unknown){
 if(!admittedDate(tradingDate))throw new WorkerError("INVALID_CLAIM");
 return PROVIDER_ORIGIN+PATH_PREFIX+tradingDate+"?adjusted=false&include_otc=false";
}
function validateRequestUrl(value:unknown,tradingDate:unknown){
 const expected=requestUrlFor(tradingDate);
 if(value!==expected)throw new WorkerError("UNADMITTED_SOURCE_URL");
 return expected;
}
function credentialPresent(value:unknown,key:string,depth=0):boolean{
 if(depth>64)throw new WorkerError("SOURCE_NESTING_LIMIT");
 if(typeof value==="string")return value.includes(key)||value.includes(encodeURIComponent(key));
 if(Array.isArray(value))return value.some(v=>credentialPresent(v,key,depth+1));
 if(value&&typeof value==="object")return Object.entries(value).some(([k,v])=>credentialPresent(k,key,depth+1)||credentialPresent(v,key,depth+1));
 return false;
}
// Reject duplicate keys at every JSON object depth before PostgreSQL jsonb can collapse them.
function assertUniqueObjectKeys(text:string){
 let i=0;const space=()=>{while(i<text.length&&/\s/.test(text[i]))i++;};
 const quoted=()=>{const start=i++;while(i<text.length){if(text[i]==="\\"){i+=2;continue;}if(text[i++]==='"')return text.slice(start,i);}throw new WorkerError("INVALID_SOURCE_JSON");};
 const value=(depth:number):void=>{
  if(depth>64)throw new WorkerError("SOURCE_NESTING_LIMIT");space();
  if(text[i]==="{"){i++;space();const seen=new Set<string>();if(text[i]==="}"){i++;return;}
   while(i<text.length){space();if(text[i]!=='"')throw new WorkerError("INVALID_SOURCE_JSON");const key=JSON.parse(quoted());
    if(seen.has(key))throw new WorkerError("DUPLICATE_SOURCE_KEY");seen.add(key);space();if(text[i++]!==":")throw new WorkerError("INVALID_SOURCE_JSON");value(depth+1);space();
    if(text[i]==="}"){i++;return;}if(text[i++]!==",")throw new WorkerError("INVALID_SOURCE_JSON");}
  }else if(text[i]==="["){i++;space();if(text[i]==="]"){i++;return;}while(i<text.length){value(depth+1);space();if(text[i]==="]"){i++;return;}if(text[i++]!==",")throw new WorkerError("INVALID_SOURCE_JSON");}}
  else if(text[i]==='"'){quoted();return;}
  else{const start=i;while(i<text.length&&!/[\s,}\]]/.test(text[i]))i++;if(start===i)throw new WorkerError("INVALID_SOURCE_JSON");return;}
  throw new WorkerError("INVALID_SOURCE_JSON");
 };value(0);space();if(i!==text.length)throw new WorkerError("INVALID_SOURCE_JSON");
}
function validateEnvelope(raw:string,key:string){
 assertUniqueObjectKeys(raw);
 let obj:Record<string,unknown>;try{obj=JSON.parse(raw);}catch{throw new WorkerError("INVALID_SOURCE_JSON");}
 if(!obj||typeof obj!=="object"||Array.isArray(obj)||obj.status!=="OK"||obj.adjusted!==false||!Array.isArray(obj.results))throw new WorkerError("INVALID_SOURCE_ENVELOPE");
 if(obj.results.length>MAX_RECORDS)throw new WorkerError("SOURCE_RECORD_LIMIT");
 if(obj.results.some(r=>r===null||typeof r!=="object"||Array.isArray(r)))throw new WorkerError("INVALID_SOURCE_RECORD");
 if(obj.next_url!==undefined&&obj.next_url!==null&&obj.next_url!=="")throw new WorkerError("UNEXPECTED_CONTINUATION");
 for(const name of ["queryCount","resultsCount"]){
  if(name in obj&&(!Number.isSafeInteger(obj[name])||Number(obj[name])<0||Number(obj[name])>9_999_999_999))throw new WorkerError("INVALID_SOURCE_COUNT");
 }
 if("resultsCount" in obj&&obj.resultsCount!==obj.results.length)throw new WorkerError("INVALID_SOURCE_COUNT");
 if(credentialPresent(obj,key))throw new WorkerError("SOURCE_CREDENTIAL_ECHO");
 // No JSON.stringify of source data: preserve every source number/string/whitespace lexeme.
 return {content_text:raw,result_count:obj.results.length,request_id:typeof obj.request_id==="string"?obj.request_id.slice(0,160):null};
}
type Claimed={state:string;archive_key:string;request_url:string;max_response_bytes:number;max_runtime_ms:number;max_result_count:number;family_id:string;partition_key:string;trading_date:string;adjusted:boolean;include_otc:boolean;calendar_scope_id:string;calendar_date_list_sha256:string;calendar_manifest_sha256:string};
function validateClaim(claimed:Claimed){
 if(claimed.family_id!=="D02"||!admittedDate(claimed.trading_date)
  ||claimed.partition_key!=="daily-source-"+claimed.trading_date.replaceAll("-","")+"-raw-no-otc-v2"
  ||claimed.calendar_scope_id!==CALENDAR_SCOPE_ID||claimed.calendar_date_list_sha256!==CALENDAR_DATE_LIST_SHA256
  ||typeof claimed.calendar_manifest_sha256!=="string"||!/^[0-9a-f]{64}$/.test(claimed.calendar_manifest_sha256)
  ||claimed.max_response_bytes!==MAX_BYTES||claimed.max_runtime_ms!==MAX_RUNTIME_MS||claimed.max_result_count!==MAX_RECORDS
  ||claimed.adjusted!==false||claimed.include_otc!==false||typeof claimed.archive_key!=="string"
  ||!/^d02-daily-v2:[0-9a-f]{64}$/.test(claimed.archive_key))throw new WorkerError("INVALID_CLAIM");
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
   if(name==="claim")rows=await tx`select population_top3000_202608_202609_v1.d02_daily_claim_v2(${attemptId}::uuid,${token}::text) as result`;
   else if(name==="commit")rows=await tx`select population_top3000_202608_202609_v1.d02_daily_commit_v2(${attemptId}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
   else rows=await tx`select population_top3000_202608_202609_v1.d02_daily_failure_v2(${attemptId}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
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
  validateClaim(claimed);
  const requestRedacted=validateRequestUrl(claimed.request_url,claimed.trading_date);
  phase="FETCH";const key=sourceKey();const requestUrl=new URL(requestRedacted);requestUrl.searchParams.set("apiKey",key);
  const fetchMs=Math.min(25000,left(deadline,12000));if(fetchMs<100)throw new WorkerError("DEADLINE");
  const fetchDeadline=Date.now()+fetchMs;fetchStarted=true;
  let response:Response;try{response=await fetch(requestUrl.toString(),{method:"GET",headers:{Accept:"application/json"},redirect:"error",signal:AbortSignal.timeout(fetchMs)});}catch{throw new WorkerError("FETCH_FAILED");}
  status=response.status;
  if(status!==200){void response.body?.cancel().catch(()=>{});const retry=response.headers.get("retry-after");const retrySeconds=retry&&/^\d+$/.test(retry)?Math.min(3600,Math.max(1,Number(retry))):60;
   phase="FAILURE";const result=await txCall("failure",{code:"HTTP_"+status,failure_class:failureClass("HTTP_"+status,status),http_status:status,received_bytes:0,retry_after_seconds:retrySeconds,fetch_started:true,wire_sha256:null});
   return output({state:result.state,provider_http_status:status,receipt_recorded:true},200);
  }
  const declared=response.headers.get("content-length");if(declared&&Number(declared)>MAX_BYTES){void response.body?.cancel().catch(()=>{});throw new WorkerError("RESPONSE_TOO_LARGE");}
  const bytes=await readLimited(response.body,MAX_BYTES,fetchDeadline,n=>{receivedBytes=n;});wireHash=await sha(bytes);
  const raw=new TextDecoder("utf-8",{fatal:true,ignoreBOM:true}).decode(bytes);const clean=validateEnvelope(raw,key);
  const sourceHash=await sha(encoder.encode(clean.content_text));
  if(sourceHash!==wireHash)throw new WorkerError("SOURCE_BYTES_CHANGED");
  phase="FINISH";
  const result=await txCall("commit",{archive_key:claimed.archive_key,trading_date:claimed.trading_date,request_url:requestRedacted,content_text:clean.content_text,source_sha256:sourceHash,wire_sha256:wireHash,wire_bytes:receivedBytes,http_status:status,retrieved_at:new Date().toISOString(),result_count:clean.result_count,provider_request_id:clean.request_id,elapsed_ms:Date.now()-started});
  return output({state:result.state,trading_date:claimed.trading_date,source_sha256:sourceHash,result_count:clean.result_count,receipt_recorded:true,cohort_certified:false},200);
 }catch(error){
  const allowed=new Set(["DEADLINE","EMPTY_BODY","SOURCE_CREDENTIAL_UNAVAILABLE","UNADMITTED_SOURCE_URL","INVALID_SOURCE_JSON","DUPLICATE_SOURCE_KEY","SOURCE_NESTING_LIMIT","INVALID_SOURCE_ENVELOPE","INVALID_SOURCE_COUNT","INVALID_SOURCE_RECORD","SOURCE_CREDENTIAL_ECHO","RESPONSE_TOO_LARGE","INVALID_CLAIM","FETCH_FAILED","UNEXPECTED_CONTINUATION","SOURCE_RECORD_LIMIT","SOURCE_BYTES_CHANGED"]);
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
