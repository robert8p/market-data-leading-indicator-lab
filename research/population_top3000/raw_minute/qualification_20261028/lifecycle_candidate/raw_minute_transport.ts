// Generalized RAW-only transport core. Candidate; not deployed. No normalizer calls.
// Provider numeric lexemes are preserved. Original pilot remains unchanged.
export type Manifest={schema:string;cohort_sha256:string;candidate_company_id:string;symbol:string;start_ms:number;end_exclusive_ms:number;multiplier:number;timespan:string;adjusted:boolean;sort:string;limit:number;max_pages:number;max_response_bytes:number;qualification:string;normalized_price_activation:boolean;request_sha256:string};
const PROJECT_URL="https://oxzabweahkoimtevbbny.supabase.co";
const PROVIDER_ORIGIN="https://api.massive.com";

const pathFor=(m:Manifest)=>"/v2/aggs/ticker/"+encodeURIComponent(m.symbol)+"/range/1/minute/"+m.start_ms+"/"+(m.end_exclusive_ms-1);
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
function canonicalNext(value:unknown,m:Manifest):string|null{
 const PATH=pathFor(m);
 if(value===null||value===undefined||value==="")return null;
 if(typeof value!=="string"||value.length>16384||/[\u0000-\u001f\u007f]/.test(value))throw new WorkerError("INVALID_NEXT_URL");
 let u:URL;try{u=new URL(value);}catch{throw new WorkerError("INVALID_NEXT_URL");}
 if(!value.startsWith(PROVIDER_ORIGIN+PATH+"?")||u.origin!==PROVIDER_ORIGIN||u.pathname!==PATH||u.username||u.password||u.hash||value.slice(value.indexOf("?")+1).split("&").some(p=>!p.includes("=")))throw new WorkerError("INVALID_NEXT_URL");
 const fixed:Record<string,string>={adjusted:"false",sort:"asc",limit:"50000"};
 const names=new Set<string>();for(const [k,v] of u.searchParams){
  if(names.has(k))throw new WorkerError("INVALID_NEXT_URL");names.add(k);
  if(k==="apiKey"||k==="api_key"||k==="apikey")continue;
  if(k==="cursor")continue;
  if(!(k in fixed)||v!==fixed[k])throw new WorkerError("INVALID_NEXT_URL");
 }
 const cursor=u.searchParams.get("cursor");if(!cursor||cursor.length>8192||/[\u0000-\u0020\u007f]/.test(cursor))throw new WorkerError("INVALID_NEXT_URL");
 return PROVIDER_ORIGIN+PATH+"?cursor="+encodeURIComponent(cursor).replace(/[!'()*]/g,c=>"%"+c.charCodeAt(0).toString(16).toUpperCase()).replace(/%20/g,"+");
}
function validateRequestUrl(value:unknown,m:Manifest){
 const FIRST_URL=PROVIDER_ORIGIN+pathFor(m)+"?adjusted=false&sort=asc&limit=50000";
 if(typeof value!=="string")throw new WorkerError("UNADMITTED_SOURCE_URL");
 if(value===FIRST_URL)return value;
 const normalized=canonicalNext(value,m);
 if(normalized===null||normalized!==value)throw new WorkerError("UNADMITTED_SOURCE_URL");
 return value;
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
// Read timestamp token text rather than rounded JS numbers: 1.0 and 1e0 are
// Decimal tokens in the reference parser, not integer timestamp observations.
function timestampWarnings(raw:string,m:Manifest){
 const span=topLevelValues(raw).get('results');let tokens:string[]=[];
 if(span){
  const a=raw.slice(span.start,span.end);let start=-1,depth=0,quoted=false,escaped=false;
  for(let i=1;i<a.length-1;i++){
   const c=a[i];
   if(quoted){if(escaped)escaped=false;else if(c==='\\')escaped=true;else if(c==='"')quoted=false;continue;}
   if(c==='"'){quoted=true;continue;}
   if(c==='{'){if(depth===0)start=i;depth++;}
   else if(c==='}'){
    depth--;if(depth===0){const row=a.slice(start,i+1);const t=topLevelValues(row).get('t');tokens.push(t?row.slice(t.start,t.end):'');}
   }
  }
 }
 const good:number[]=[];
 for(const token of tokens){
  if(!/^-?(0|[1-9][0-9]*)$/.test(token))continue;
  const t=Number(token);if(Number.isSafeInteger(t)&&t>=m.start_ms&&t<m.end_exclusive_ms&&t%60000===0)good.push(t);
 }
 return {observation_timestamp_errors:tokens.length-good.length,duplicate_timestamps:good.length-new Set(good).size,source_rows_in_timestamp_order:good.length===tokens.length&&good.every((t,i)=>i===0||good[i-1]<=t)};
}
function sanitizeEnvelope(raw:string,key:string,m:Manifest){
 assertUniqueObjectKeys(raw);
 let obj:Record<string,unknown>;try{obj=JSON.parse(raw);}catch{throw new WorkerError("INVALID_SOURCE_JSON");}
 if(!obj||typeof obj!=="object"||Array.isArray(obj)||obj.status!=="OK"||obj.ticker!==m.symbol||obj.adjusted!==false)throw new WorkerError("INVALID_SOURCE_ENVELOPE");
 let results:unknown[],presence:string;
 if(Array.isArray(obj.results)){results=obj.results;presence="ARRAY";}
 else if(!Object.prototype.hasOwnProperty.call(obj,"results")&&obj.resultsCount===0){results=[];presence="OMITTED_WITH_EXPLICIT_ZERO";}
 else throw new WorkerError("INVALID_SOURCE_ENVELOPE");
 if(results.length>50000)throw new WorkerError("INVALID_SOURCE_COUNT");
 for(const name of ["queryCount","resultsCount"]){
  if(Object.prototype.hasOwnProperty.call(obj,name)){
   const n=obj[name];if(typeof n!=="number"||!Number.isSafeInteger(n)||n<0||(name==="resultsCount"&&n!==results.length))throw new WorkerError("INVALID_SOURCE_COUNT");
  }
 }
 if(results.some(r=>r===null||typeof r!=="object"||Array.isArray(r)))throw new WorkerError("INVALID_SOURCE_RECORD");
 const spans=topLevelValues(raw);const next=canonicalNext(obj.next_url,m);let text=raw;
 const span=spans.get("next_url");if(span)text=raw.slice(0,span.start)+JSON.stringify(next)+raw.slice(span.end);
 if(credentialPresent(JSON.parse(text),key))throw new WorkerError("SOURCE_CREDENTIAL_ECHO");
 if(encoder.encode(text).length>m.max_response_bytes)throw new WorkerError("REDACTED_RESPONSE_TOO_LARGE");
 return {content_text:text,next_url:next,result_count:results.length,results_presence:presence,request_id:typeof obj.request_id==="string"?obj.request_id.slice(0,160):null,...timestampWarnings(raw,m)};
}

const FIELDS=["schema","cohort_sha256","candidate_company_id","symbol","start_ms","end_exclusive_ms","multiplier","timespan","adjusted","sort","limit","max_pages","max_response_bytes","qualification","normalized_price_activation","request_sha256"].sort().join(",");
export async function validateManifest(m:Manifest){
 if(!m||typeof m!=="object"||Array.isArray(m)||Object.keys(m).sort().join(",")!==FIELDS)throw new WorkerError("MANIFEST_SHAPE");
 if(m.schema!=="RAW_MINUTE_MONTH_V1"||m.cohort_sha256!=="2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec"||typeof m.candidate_company_id!=="string"||!/^CIK:[0-9]{10}$/.test(m.candidate_company_id)||typeof m.symbol!=="string"||!/^[A-Za-z0-9][A-Za-z0-9.\^-]{0,31}$/.test(m.symbol))throw new WorkerError("MANIFEST_SCOPE");
 if(!Number.isSafeInteger(m.start_ms)||!Number.isSafeInteger(m.end_exclusive_ms)||![[1785556800000,1788235200000],[1788235200000,1790827200000]].some(([a,b])=>a===m.start_ms&&b===m.end_exclusive_ms))throw new WorkerError("MANIFEST_WINDOW");
 if(m.multiplier!==1||m.timespan!=="minute"||m.adjusted!==false||m.sort!=="asc"||m.limit!==50000||!Number.isSafeInteger(m.max_pages)||m.max_pages<1||m.max_pages>4||!Number.isSafeInteger(m.max_response_bytes)||m.max_response_bytes<1||m.max_response_bytes>MAX_BYTES)throw new WorkerError("MANIFEST_BOUNDS");
 if(m.qualification!=="RAW_PROVIDER_SOURCE_ONLY_IDENTITY_LIFECYCLE_NOT_CERTIFIED"||m.normalized_price_activation!==false)throw new WorkerError("MANIFEST_QUALIFICATION");
 const unsigned=Object.fromEntries(Object.keys(m).filter(k=>k!=="request_sha256").sort().map(k=>[k,m[k as keyof Manifest]]));
 if(await sha(encoder.encode(JSON.stringify(unsigned)))!==m.request_sha256)throw new WorkerError("MANIFEST_HASH");
}
export async function retainRaw(bytes:Uint8Array,m:Manifest,key:string,currentUrl:string){
 await validateManifest(m);validateCredential(key);
 if(!(bytes instanceof Uint8Array)||bytes.length<1||bytes.length>m.max_response_bytes)throw new WorkerError("RESPONSE_SIZE");
 const raw=new TextDecoder("utf-8",{fatal:true,ignoreBOM:true}).decode(bytes);
 const clean=sanitizeEnvelope(raw,key,m);if(clean.next_url===currentUrl)throw new WorkerError("REPEATED_CURSOR");
 return {...clean,source_sha256:await sha(encoder.encode(clean.content_text)),wire_sha256:await sha(bytes),wire_bytes:bytes.length,normalized_values_created:0,original_historical_receipt_verified:false,identity_lifecycle_qualified:false,family_accepted:false,no_trade_inferred:false};
}
function validateCredential(key:string){if(typeof key!=="string"||key.length<8||/[\u0000-\u0020\u007f]/.test(key))throw new WorkerError("CREDENTIAL_UNAVAILABLE");}
export type Reservation={manifest_sha256:string;request_url:string;page_number:number;attempt_id:string;reserved_bytes:number;expiry_epoch:number;reserved:boolean;owner_fence:number;stop_generation:number};
export async function acquireRaw(m:Manifest,r:Reservation,key:string,commit:(page:unknown,reservation:Reservation)=>Promise<unknown>,failure:(code:string,reservation:Reservation)=>Promise<unknown>,fetcher:typeof fetch=fetch,now:()=>number=Date.now){
 await validateManifest(m);
 if(!r||r.reserved!==true||r.manifest_sha256!==m.request_sha256||r.owner_fence!==3||r.stop_generation!==1||r.reserved_bytes!==m.max_response_bytes||!Number.isFinite(r.expiry_epoch)||r.expiry_epoch*1000<=now()+2000||typeof r.attempt_id!=="string"||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(r.attempt_id)||!Number.isSafeInteger(r.page_number)||r.page_number<1||r.page_number>m.max_pages)throw new WorkerError("CURRENT_RESERVATION_REQUIRED");
 const first=PROVIDER_ORIGIN+pathFor(m)+"?adjusted=false&sort=asc&limit=50000";
 if(typeof r.request_url!=="string"||!r.request_url||(r.page_number===1?r.request_url!==first:canonicalNext(r.request_url,m)!==r.request_url))throw new WorkerError("PAGE_URL_REQUIRED");
 validateCredential(key);const target=new URL(r.request_url);target.searchParams.set("apiKey",key);
 const deadline=Math.min(r.expiry_epoch*1000-1000,now()+25000);const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),Math.max(1,deadline-now()));
 let response:Response;let bytes:Uint8Array;
 const recordFailure=async(code:string)=>{try{await failure(code,r);}catch{throw new WorkerError("FAILURE_RECORD_OUTCOME_REQUIRES_RECONCILIATION");}};
 try{
  try{response=await fetcher(target.toString(),{method:"GET",headers:{Accept:"application/json"},redirect:"manual",signal:controller.signal});}catch{throw new WorkerError("TRANSPORT_OUTCOME_REQUIRES_RECONCILIATION");}
  if(!(response instanceof Response))throw new WorkerError("INVALID_TRANSPORT_RECEIPT_REQUIRES_RECONCILIATION");
  if(response.status>=300&&response.status<400||response.redirected){void response.body?.cancel().catch(()=>{});await recordFailure("REDIRECT_REJECTED");throw new WorkerError("REDIRECT_REJECTED");}
  if(response.status!==200){void response.body?.cancel().catch(()=>{});await recordFailure("HTTP_"+response.status);throw new WorkerError("HTTP_"+response.status);}
  try{bytes=await readLimited(response.body,m.max_response_bytes,deadline);}catch{throw new WorkerError("TRANSPORT_OUTCOME_REQUIRES_RECONCILIATION");}
  let page;try{page=await retainRaw(bytes,m,key,r.request_url);}catch(e){const code=e instanceof WorkerError?e.code:"INVALID_SOURCE_JSON";await recordFailure(code);throw new WorkerError(code);}
  try{if(now()>=r.expiry_epoch*1000)throw new WorkerError("LEASE_EXPIRED");return await commit(page,r);}catch{throw new WorkerError("COMMIT_OUTCOME_REQUIRES_RECONCILIATION");}
 }finally{clearTimeout(timer);controller.abort();}
}
export {canonicalNext,validateRequestUrl,sanitizeEnvelope,assertUniqueObjectKeys,WorkerError};
