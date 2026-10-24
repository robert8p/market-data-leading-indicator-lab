// Scoped CTX02 historical full-reference census; no cohort, strike, DTE or deliverable filter.
// One admitted request per attempt; existing provider limiter, retained page chain and successor pipeline.
// Preserves source numeric lexemes including nanoseconds; never infers no-trade or original receipt.
import postgres from "npm:postgres@3.4.5";
const PROJECT_URL="https://oxzabweahkoimtevbbny.supabase.co";
const PROVIDER_ORIGIN="https://api.massive.com";

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
type Claimed={state:string;archive_key:string;page_number:number;request_url:string;max_response_bytes:number;max_runtime_ms:number;family_id:string;partition_key:string;request_key:string;as_of:string;expired:boolean;population:string;first_url:string;max_pages:number;request_manifest_sha256:string};
function sourceContract(c:Claimed){
 if(!/^2026-(08|09)-[0-9]{2}$/.test(c.as_of)||new Date(c.as_of).toISOString().slice(0,10)!==c.as_of||c.as_of<"2026-08-01"||c.as_of>"2026-09-30"||c.expired!==false||c.population!=="ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER")throw new WorkerError("INVALID_CLAIM");
 const fixed:Record<string,string>={as_of:c.as_of,expired:"false",limit:"1000",sort:"ticker",order:"asc"};
 const path="/v3/reference/options/contracts";
 const first=PROVIDER_ORIGIN+path+"?"+new URLSearchParams(fixed).toString();
 if(c.first_url!==first||c.max_response_bytes!==1048576)throw new WorkerError("INVALID_CLAIM");
 return {path,fixed,first,limit:1000};
}
function canonicalNext(value:unknown,c:Claimed):string|null{
 if(value===null||value===undefined||value==="")return null;
 if(typeof value!=="string"||value.length>16384)throw new WorkerError("INVALID_NEXT_URL");
 const spec=sourceContract(c);let u:URL;try{u=new URL(value);}catch{throw new WorkerError("INVALID_NEXT_URL");}
 if(u.origin!==PROVIDER_ORIGIN||u.pathname!==spec.path||u.username||u.password||u.hash)throw new WorkerError("INVALID_NEXT_URL");
 const names=new Set<string>();for(const [k,v] of u.searchParams){
  if(names.has(k))throw new WorkerError("INVALID_NEXT_URL");names.add(k);
  if(k==="apiKey"||k==="api_key"||k==="apikey")continue;
  if(k==="cursor")continue;
  if(!(k in spec.fixed)||v!==spec.fixed[k])throw new WorkerError("INVALID_NEXT_URL");
 }
 const cursor=u.searchParams.get("cursor");if(!cursor||cursor.length>8192||/[\u0000-\u0020\u007f]/.test(cursor))throw new WorkerError("INVALID_NEXT_URL");
 const out=new URL(PROVIDER_ORIGIN+spec.path);out.searchParams.set("cursor",cursor);return out.toString();
}
function validateRequestUrl(c:Claimed){
 const spec=sourceContract(c);if(c.page_number===1&&c.request_url===spec.first)return c.request_url;
 if(c.page_number===1)throw new WorkerError("UNADMITTED_SOURCE_URL");
 const normalized=canonicalNext(c.request_url,c);
 if(normalized===null||normalized!==c.request_url)throw new WorkerError("UNADMITTED_SOURCE_URL");
 return c.request_url;
}
// Find top-level JSON value spans without changing any result number/string lexeme.
// The exact stored content differs from the wire only at the credential-bearing next_url value.
function topLevelValues(text:string):Map<string,{start:number,end:number}>{
 let i=0;const skip=()=>{while(/\s/.test(text[i]||"")&&i<text.length)i++;};
 const stringEnd=()=>{if(text[i]!=='"')throw new WorkerError("INVALID_SOURCE_JSON");i++;while(i<text.length){if(text[i]==="\\"){i+=2;continue;}if(text[i++]==='"')return;}throw new WorkerError("INVALID_SOURCE_JSON");};
 skip();if(text[i++]!=="{")throw new WorkerError("INVALID_SOURCE_JSON");const spans=new Map<string,{start:number,end:number}>();
 while(true){skip();if(text[i]==="}"){i++;break;}const keyStart=i;stringEnd();const key=JSON.parse(text.slice(keyStart,i));skip();if(text[i++]!==":")throw new WorkerError("INVALID_SOURCE_JSON");skip();const start=i;
  let depth=0;while(i<text.length){const c=text[i];if(c==='"'){stringEnd();continue;}if(c==="{"||c==="["){depth++;i++;continue;}if(c==="}"||c==="]"){if(depth===0)break;depth--;i++;continue;}if(c===","&&depth===0)break;i++;}
  let end=i;while(end>start&&/\s/.test(text[end-1]))end--;
  if(spans.has(key))throw new WorkerError("DUPLICATE_SOURCE_KEY");spans.set(key,{start,end});skip();
  if(text[i]===","){i++;continue;}if(text[i]==="}"){i++;break;}throw new WorkerError("INVALID_SOURCE_JSON");
 }skip();if(i!==text.length)throw new WorkerError("INVALID_SOURCE_JSON");return spans;
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
function sanitizeEnvelope(raw:string,key:string,c:Claimed){
 assertUniqueObjectKeys(raw);
 let obj:Record<string,unknown>;try{obj=JSON.parse(raw);}catch{throw new WorkerError("INVALID_SOURCE_JSON");}
 if(!obj||typeof obj!=="object"||Array.isArray(obj)||obj.status!=="OK"||!Array.isArray(obj.results)||obj.results.length>sourceContract(c).limit)throw new WorkerError("INVALID_SOURCE_ENVELOPE");
 if(obj.results.some(r=>r===null||typeof r!=="object"||Array.isArray(r)))throw new WorkerError("INVALID_SOURCE_RECORD");
 const spans=topLevelValues(raw);const next=canonicalNext(obj.next_url,c);let text=raw;
 const span=spans.get("next_url");if(span)text=raw.slice(0,span.start)+JSON.stringify(next)+raw.slice(span.end);
 if(credentialPresent(JSON.parse(text),key))throw new WorkerError("SOURCE_CREDENTIAL_ECHO");
 if(encoder.encode(text).length>c.max_response_bytes)throw new WorkerError("REDACTED_RESPONSE_TOO_LARGE");
 return {content_text:text,next_url:next,result_count:obj.results.length,request_id:typeof obj.request_id==="string"?obj.request_id.slice(0,160):null};
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
   if(name==="claim")rows=await tx`select population_top3000_202608_202609_v1.ctx02_census_claim_v1(${attemptId}::uuid,${token}::text) as result`;
   else if(name==="commit")rows=await tx`select population_top3000_202608_202609_v1.ctx02_census_commit_v1(${attemptId}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
   else rows=await tx`select population_top3000_202608_202609_v1.ctx02_census_failure_v1(${attemptId}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
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
  if(claimed.family_id!=="CTX02"||claimed.partition_key!==claimed.request_key||!/^ctx02-census-v1:[0-9a-f]{64}$/.test(claimed.request_key)||!/^ctx02-census-archive-v1:[0-9a-f]{64}$/.test(claimed.archive_key)||!/^[0-9a-f]{64}$/.test(claimed.request_manifest_sha256)||claimed.max_runtime_ms!==MAX_RUNTIME_MS||!Number.isSafeInteger(claimed.page_number)||!Number.isSafeInteger(claimed.max_pages)||claimed.page_number<1||claimed.page_number>claimed.max_pages||claimed.max_pages>100000)throw new WorkerError("INVALID_CLAIM");
  const requestRedacted=validateRequestUrl(claimed);
  phase="FETCH";const key=sourceKey();const requestUrl=new URL(requestRedacted);requestUrl.searchParams.set("apiKey",key);
  const fetchMs=Math.min(25000,left(deadline,12000));if(fetchMs<100)throw new WorkerError("DEADLINE");
  const fetchDeadline=Date.now()+fetchMs;fetchStarted=true;
  let response:Response;try{response=await fetch(requestUrl.toString(),{method:"GET",headers:{Accept:"application/json"},redirect:"error",signal:AbortSignal.timeout(fetchMs)});}catch{throw new WorkerError("FETCH_FAILED");}
  status=response.status;
  if(status!==200){void response.body?.cancel().catch(()=>{});const retry=response.headers.get("retry-after");const retrySeconds=retry&&/^\d+$/.test(retry)?Math.min(3600,Math.max(status===429?60:1,Number(retry))):60;
   phase="FAILURE";const result=await txCall("failure",{code:"HTTP_"+status,failure_class:failureClass("HTTP_"+status,status),http_status:status,received_bytes:0,retry_after_seconds:retrySeconds,fetch_started:true,wire_sha256:null});
   return output({state:result.state,provider_http_status:status,receipt_recorded:true},200);
  }
  const declared=response.headers.get("content-length");if(declared&&Number(declared)>claimed.max_response_bytes){void response.body?.cancel().catch(()=>{});throw new WorkerError("RESPONSE_TOO_LARGE");}
  const bytes=await readLimited(response.body,claimed.max_response_bytes,fetchDeadline,n=>{receivedBytes=n;});wireHash=await sha(bytes);
  const raw=new TextDecoder("utf-8",{fatal:true,ignoreBOM:true}).decode(bytes);const clean=sanitizeEnvelope(raw,key,claimed);
  const sourceHash=await sha(encoder.encode(clean.content_text));
  if(clean.next_url===requestRedacted)throw new WorkerError("REPEATED_PAGE_CURSOR");
  phase="FINISH";
  const result=await txCall("commit",{archive_key:claimed.archive_key,request_manifest_sha256:claimed.request_manifest_sha256,page_number:claimed.page_number,request_url:requestRedacted,next_url:clean.next_url,content_text:clean.content_text,source_sha256:sourceHash,wire_sha256:wireHash,wire_bytes:receivedBytes,http_status:status,retrieved_at:new Date().toISOString(),result_count:clean.result_count,provider_request_id:clean.request_id,elapsed_ms:Date.now()-started});
  return output({state:result.state,page_number:claimed.page_number,source_sha256:sourceHash,result_count:clean.result_count,has_next_page:clean.next_url!==null,receipt_recorded:true},200);
 }catch(error){
  const allowed=new Set(["DEADLINE","EMPTY_BODY","SOURCE_CREDENTIAL_UNAVAILABLE","INVALID_NEXT_URL","UNADMITTED_SOURCE_URL","INVALID_SOURCE_JSON","DUPLICATE_SOURCE_KEY","SOURCE_NESTING_LIMIT","INVALID_SOURCE_ENVELOPE","INVALID_SOURCE_COUNT","INVALID_SOURCE_RECORD","SOURCE_CREDENTIAL_ECHO","REDACTED_RESPONSE_TOO_LARGE","RESPONSE_TOO_LARGE","INVALID_CLAIM","FETCH_FAILED","REPEATED_PAGE_CURSOR"]);
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

