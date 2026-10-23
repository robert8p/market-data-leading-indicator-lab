// Deploy only after root review as population-top3000-bars-v1, verify_jwt=false.
// Shared P01/P02 Alpaca SIP/raw compact-source lane. One page before live review; one provider request per fenced attempt.
// No credential probe: credentials are read only for an admitted real source request. No automatic retries or source overwrites.
import postgres from "npm:postgres@3.4.5";
const PROJECT_URL="https://oxzabweahkoimtevbbny.supabase.co";
const PROVIDER_ORIGIN="https://data.alpaca.markets";
const PATH="/v2/stocks/bars";
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
function sourceCredentials(){
 // Match the alias contract already present in the existing precursor ingestion configuration.
 // Values remain internal and are read only after the attempt capability is claimed.
 const first=(names:string[])=>names.map(name=>Deno.env.get(name)?.trim()).find(Boolean);
 const id=first(["ALPACA_API_KEY","APCA_API_KEY_ID","ALPACA_KEY_ID","ALPACA_API_KEY_ID"]);
 const secret=first(["ALPACA_API_SECRET","APCA_API_SECRET_KEY","ALPACA_SECRET_KEY","ALPACA_API_SECRET_KEY"]);
 if(id&&secret)return {id,secret};
 throw new WorkerError("MISSING_PROVIDER_KEY");
}
function validateRequestUrl(value:unknown,claim:Claimed){
 if(typeof value!=="string"||value.length>24000)throw new WorkerError("UNADMITTED_SOURCE_URL");
 let u:URL;try{u=new URL(value);}catch{throw new WorkerError("UNADMITTED_SOURCE_URL");}
 if(u.origin!==PROVIDER_ORIGIN||u.pathname!==PATH||u.username||u.password||u.hash)throw new WorkerError("UNADMITTED_SOURCE_URL");
 const spec=claim.request_spec;
 if(!spec||!Array.isArray(spec.symbols)||spec.symbols.length<1||spec.symbols.length>50||spec.symbols.some(s=>typeof s!=="string"||!/^[A-Z0-9][A-Z0-9.-]{0,31}$/.test(s))||new Set(spec.symbols).size!==spec.symbols.length)throw new WorkerError("INVALID_CLAIM");
 const required:Record<string,string>={symbols:spec.symbols.join(","),timeframe:"1Min",start:spec.start_utc,end:spec.end_inclusive_utc,limit:"10000",adjustment:"raw",feed:"sip",sort:"asc",asof:"-",currency:"USD"};
 if(!/^2026-(08|09)-[0-9]{2}T04:00:00Z$/.test(spec.start_utc)||!/^2026-(08|09|10)-[0-9]{2}T03:59:59[.]999999Z$/.test(spec.end_inclusive_utc)||!/^2026-(08|09|10)-[0-9]{2}T04:00:00Z$/.test(spec.end_exclusive_utc))throw new WorkerError("INVALID_CLAIM");
 const start=Date.parse(spec.start_utc),end=Date.parse(spec.end_exclusive_utc);
 if(!Number.isFinite(start)||!Number.isFinite(end)||start<Date.parse("2026-08-01T04:00:00Z")||end>Date.parse("2026-10-01T04:00:00Z")||end<=start||end-start>7*86400000||Date.parse(spec.end_inclusive_utc)!==end-1)throw new WorkerError("INVALID_CLAIM");
 const token=claim.request_token;if(token!==null){if(typeof token!=="string"||!token||token.length>8192||/[\u0000-\u0020\u007f]/.test(token))throw new WorkerError("INVALID_CLAIM");required.page_token=token;}
 const names=new Set<string>();for(const [k,v] of u.searchParams){if(names.has(k)||!(k in required)||v!==required[k])throw new WorkerError("UNADMITTED_SOURCE_URL");names.add(k);}
 if(names.size!==Object.keys(required).length)throw new WorkerError("UNADMITTED_SOURCE_URL");return value;
}
// Local JSON scanner utility; provider source text is retained without reserialization.
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
type Claimed={state:string;archive_key:string;page_number:number;request_url:string;request_token:string|null;max_response_bytes:number;max_runtime_ms:number;max_bars:number;family_id:string;partition_key:string;request_spec:{symbols:string[];start_utc:string;end_inclusive_utc:string;end_exclusive_utc:string}};
function sanitizeEnvelope(raw:string,keys:string[],symbols:string[]){
 assertUniqueObjectKeys(raw);let obj:Record<string,unknown>;try{obj=JSON.parse(raw);}catch{throw new WorkerError("INVALID_SOURCE_JSON");}
 if(!obj||typeof obj!=="object"||Array.isArray(obj)||!obj.bars||typeof obj.bars!=="object"||Array.isArray(obj.bars)||!("next_page_token" in obj))throw new WorkerError("INVALID_SOURCE_ENVELOPE");
 let count=0;const wanted=new Set(symbols);
 for(const [symbol,rows] of Object.entries(obj.bars)){
  if(!wanted.has(symbol)||!Array.isArray(rows))throw new WorkerError("INVALID_SOURCE_SYMBOL");
  count+=rows.length;if(count>10000)throw new WorkerError("INVALID_SOURCE_COUNT");
  if(rows.some(r=>r===null||typeof r!=="object"||Array.isArray(r)))throw new WorkerError("INVALID_SOURCE_RECORD");
 }
 const token=obj.next_page_token;
 if(token!==null&&(typeof token!=="string"||!token||token.length>8192||/[\u0000-\u0020\u007f]/.test(token)))throw new WorkerError("INVALID_PAGE_TOKEN");
 if(keys.some(k=>credentialPresent(obj,k)))throw new WorkerError("SOURCE_CREDENTIAL_ECHO");
 if(encoder.encode(raw).length>MAX_BYTES)throw new WorkerError("RESPONSE_TOO_LARGE");
 // Do not JSON.stringify the provider body: every number and source timestamp lexeme stays exact.
 return {content_text:raw,next_page_token:token as string|null,result_count:count};
}
function failureClass(code:string,status:number|null){
 if(status===401||status===403||code==="MISSING_PROVIDER_KEY")return "AUTHENTICATION";
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
   if(name==="claim")rows=await tx`select population_top3000_202608_202609_v1.alpaca_sip_claim_v1(${attemptId}::uuid,${token}::text) as result`;
   else if(name==="commit")rows=await tx`select population_top3000_202608_202609_v1.alpaca_sip_commit_v1(${attemptId}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
   else rows=await tx`select population_top3000_202608_202609_v1.alpaca_sip_failure_v1(${attemptId}::uuid,${token}::text,${tx.json(payload)}::jsonb) as result`;
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
  if(claimed.family_id!=="P01"||!claimed.partition_key.startsWith("alpaca-sip-gap:")||claimed.max_response_bytes!==MAX_BYTES||claimed.max_runtime_ms!==MAX_RUNTIME_MS||claimed.max_bars!==10000||!Number.isSafeInteger(claimed.page_number)||claimed.page_number<1||claimed.page_number>64)throw new WorkerError("INVALID_CLAIM");
  const requestRedacted=validateRequestUrl(claimed.request_url,claimed);
  phase="FETCH";const credentials=sourceCredentials();const requestUrl=new URL(requestRedacted);
  const fetchMs=Math.min(25000,left(deadline,12000));if(fetchMs<100)throw new WorkerError("DEADLINE");
  const fetchDeadline=Date.now()+fetchMs;fetchStarted=true;
  let response:Response;try{response=await fetch(requestUrl.toString(),{method:"GET",headers:{Accept:"application/json","APCA-API-KEY-ID":credentials.id,"APCA-API-SECRET-KEY":credentials.secret},redirect:"error",signal:AbortSignal.timeout(fetchMs)});}catch{throw new WorkerError("FETCH_FAILED");}
  status=response.status;
  if(status!==200){void response.body?.cancel().catch(()=>{});const retry=response.headers.get("retry-after");const retrySeconds=retry&&/^\d+$/.test(retry)?Math.min(3600,Math.max(1,Number(retry))):60;
   phase="FAILURE";const result=await txCall("failure",{code:"HTTP_"+status,failure_class:failureClass("HTTP_"+status,status),http_status:status,received_bytes:0,retry_after_seconds:retrySeconds,fetch_started:true,wire_sha256:null});
   return output({state:result.state,provider_http_status:status,receipt_recorded:true},200);
  }
  const declared=response.headers.get("content-length");if(declared&&Number(declared)>MAX_BYTES){void response.body?.cancel().catch(()=>{});throw new WorkerError("RESPONSE_TOO_LARGE");}
  const bytes=await readLimited(response.body,MAX_BYTES,fetchDeadline,n=>{receivedBytes=n;});wireHash=await sha(bytes);
  const raw=new TextDecoder("utf-8",{fatal:true,ignoreBOM:true}).decode(bytes);const clean=sanitizeEnvelope(raw,[credentials.id,credentials.secret],claimed.request_spec.symbols);
  const sourceHash=await sha(encoder.encode(clean.content_text));
  if(clean.next_page_token!==null&&clean.next_page_token===claimed.request_token)throw new WorkerError("REPEATED_PAGE_CURSOR");
  phase="FINISH";
  const result=await txCall("commit",{archive_key:claimed.archive_key,page_number:claimed.page_number,request_url:requestRedacted,next_page_token:clean.next_page_token,content_text:clean.content_text,source_sha256:sourceHash,wire_sha256:wireHash,wire_bytes:receivedBytes,http_status:status,retrieved_at:new Date().toISOString(),result_count:clean.result_count,provider_request_id:response.headers.get("x-request-id")?.slice(0,160)||null,elapsed_ms:Date.now()-started});
  return output({state:result.state,page_number:claimed.page_number,source_sha256:sourceHash,result_count:clean.result_count,has_next_page:clean.next_page_token!==null,receipt_recorded:true},200);
 }catch(error){
  const allowed=new Set(["DEADLINE","EMPTY_BODY","MISSING_PROVIDER_KEY","INVALID_PAGE_TOKEN","INVALID_SOURCE_SYMBOL","UNADMITTED_SOURCE_URL","INVALID_SOURCE_JSON","DUPLICATE_SOURCE_KEY","SOURCE_NESTING_LIMIT","INVALID_SOURCE_ENVELOPE","INVALID_SOURCE_COUNT","INVALID_SOURCE_RECORD","SOURCE_CREDENTIAL_ECHO","REDACTED_RESPONSE_TOO_LARGE","RESPONSE_TOO_LARGE","INVALID_CLAIM","FETCH_FAILED","REPEATED_PAGE_CURSOR"]);
  const code=error instanceof WorkerError&&allowed.has(error.code)?error.code:"CONTROL_OR_PARSE_FAILURE";
  if(claimed?.state==="RESERVED"&&phase!=="FINISH"&&phase!=="FAILURE"&&left(deadline)>1000){
   try{phase="FAILURE";const result=await txCall("failure",{code,failure_class:failureClass(code,status),http_status:status,received_bytes:receivedBytes,retry_after_seconds:60,fetch_started:fetchStarted,wire_sha256:wireHash});
    return output({state:result.state,code:code==="MISSING_PROVIDER_KEY"?"MissingProviderKey":code,receipt_recorded:true},200);
   }catch{/* Unknown DB effect remains in the fenced lease for reconciliation. */}
  }
  // No raw errors/URLs/payloads/environment values leave the worker.
  return output({error:phase==="CLAIM"?"AttemptNotAdmitted":"WorkerReceiptUnconfirmed",phase,code:code==="MISSING_PROVIDER_KEY"?"MissingProviderKey":code,receipt_recorded:false,provider_requested:fetchStarted},phase==="CLAIM"?403:503);
 }finally{if(db)void db.end({timeout:0}).catch(()=>{});}
});
