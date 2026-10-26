// New mission capability v9: deploy as population-top3000-resource-v9 with verify_jwt=false.
// Explicitly admitted cumulative reusable finite ordinals18..41. Original three v1 attempts remain immutable.
// The private, single-use DB capability is mandatory before any privileged metrics fetch.
// No logging, provider requests, arbitrary URLs, storage changes or automatic retries.
import postgres from "npm:postgres@3.4.5";

const PROJECT_URL = "https://oxzabweahkoimtevbbny.supabase.co";
const METRICS_URL = PROJECT_URL + "/customer/v1/privileged/metrics";
const MAX_RESPONSE_BYTES = 2_097_152;
const TOTAL_TIMEOUT_MS = 30_000;
const encoder = new TextEncoder();
type Capabilities = {sec_user_agent_configured:boolean;sec_contact_email_configured:boolean;massive_key_configured:boolean};
type Metric = {filesystem_id:string;mountpoint:string;available_bytes:number;free_bytes:number;size_bytes:number};
type ProbeResult = {outcome:"SUCCEEDED"|"FAILED";observed_at:string;metrics_http_status:number|null;source_sha256:string|null;metrics:Metric[];error_code:string|null;elapsed_ms:number;capabilities:Capabilities};
class ProbeError extends Error { readonly code:string; constructor(code:string) { super(code);this.code=code; } }
const json = (value:unknown,status=200) => Response.json(value,{status,headers:{"cache-control":"no-store","x-content-type-options":"nosniff"}});
function remaining(deadline:number,reserve=0) { return Math.max(0,Math.floor(deadline-Date.now()-reserve)); }
async function digest(bytes:Uint8Array):Promise<string> {
 const h=await crypto.subtle.digest("SHA-256",bytes);
 return Array.from(new Uint8Array(h),b=>b.toString(16).padStart(2,"0")).join("");
}
async function timed<T>(promise:PromiseLike<T>,milliseconds:number):Promise<T> {
 if(milliseconds<=0) throw new ProbeError("DEADLINE");
 let timer:number|undefined;
 try {return await Promise.race([Promise.resolve(promise),new Promise<never>((_,reject)=>{timer=setTimeout(()=>reject(new ProbeError("DEADLINE")),milliseconds);})]);}
 finally {if(timer!==undefined)clearTimeout(timer);}
}
async function readLimited(stream:ReadableStream<Uint8Array>|null,limit:number,deadline:number,signal:AbortSignal):Promise<Uint8Array> {
 if(!stream) throw new ProbeError("METRICS_PARSE_ERROR");
 const reader=stream.getReader();const chunks:Uint8Array[]=[];let total=0;
 const cancel=()=>{void reader.cancel().catch(()=>{});};signal.addEventListener("abort",cancel,{once:true});
 try {
  while(true) {
   if(signal.aborted||Date.now()>=deadline)throw new ProbeError("METRICS_READ_TIMEOUT");
   const next=await timed(reader.read(),remaining(deadline));if(next.done)break;
   total+=next.value.length;if(total>limit)throw new ProbeError("METRICS_RESPONSE_TOO_LARGE");
   chunks.push(next.value);
  }
  if(signal.aborted||Date.now()>=deadline)throw new ProbeError("METRICS_READ_TIMEOUT");
  const bytes=new Uint8Array(total);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}return bytes;
 } finally {signal.removeEventListener("abort",cancel);cancel();}
}
function configured(name:string):boolean {return !!Deno.env.get(name)?.trim();}
function capabilities():Capabilities {return {
 sec_user_agent_configured:configured("SEC_USER_AGENT"),sec_contact_email_configured:configured("SEC_CONTACT_EMAIL"),
 massive_key_configured:["MASSIVE_API_KEY","MASSIVE_KEY","POLYGON_API_KEY","POLYGON_KEY"].some(configured)
};}
function metricCredential():string {
 // Existing project convention: preferred secret key, then legacy service-role key.
 const raw=Deno.env.get("SUPABASE_SECRET_KEYS");
 if(raw)try {const keys=JSON.parse(raw);if(keys&&typeof keys==="object"&&!Array.isArray(keys)){
  const candidate=typeof keys.default==="string"?keys.default:Object.values(keys).find(k=>typeof k==="string"&&k.startsWith("sb_secret_"));
  if(typeof candidate==="string"&&candidate.startsWith("sb_secret_"))return candidate;
 }}catch{/* Never surface environment content. */}
 const legacy=Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");if(legacy?.trim())return legacy;
 throw new ProbeError("METRICS_CREDENTIAL_UNAVAILABLE");
}
function label(labels:string,name:"mountpoint"|"device"|"fstype"):string {
 const match=labels.match(new RegExp('(?:^|,)\\s*'+name+'="((?:\\\\.|[^"\\\\])*)"'));
 return match?match[1].replace(/\\(\\|"|n)/g,(_,char:string)=>char==="n"?"\n":char):"";
}
async function parseMetrics(text:string,deadline:number):Promise<Metric[]> {
 const groups=new Map<string,{mountpoint:string;available_bytes?:number;free_bytes?:number;size_bytes?:number}>();
 for(const line of text.split("\n")) {
  if(Date.now()>=deadline)throw new ProbeError("METRICS_READ_TIMEOUT");
  if(line.length>4096)continue;
  const match=line.match(/^[a-zA-Z_:][a-zA-Z0-9_:]*filesystem_(avail|free|size)_bytes(?:\{([^\n]*)\})?\s+([0-9.eE+\-]+)(?:\s+[0-9]+)?\s*$/);
  if(!match)continue;
  const value=Number(match[3]);if(!Number.isSafeInteger(value)||value<0)continue;
  const labels=match[2]||"";const mountpoint=label(labels,"mountpoint");
  if(mountpoint.length>160||/[\u0000-\u001f\u007f]/.test(mountpoint))continue;
  const identity=JSON.stringify(Array.from(labels.matchAll(/([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"/g),m=>[m[1],m[2]]).sort((a,b)=>a[0].localeCompare(b[0])));
  if(!groups.has(identity)&&groups.size>=64)continue;
  const group=groups.get(identity)||{mountpoint};
  const key=match[1]==="avail"?"available_bytes":match[1]==="free"?"free_bytes":"size_bytes";
  if(group[key]!==undefined&&group[key]!==value)throw new ProbeError("METRICS_PARSE_ERROR");
  group[key]=value;groups.set(identity,group);
 }
 const output:Metric[]=[];
 for(const [identity,group] of groups) {
  if(Date.now()>=deadline)throw new ProbeError("METRICS_READ_TIMEOUT");
  const {available_bytes:a,free_bytes:f,size_bytes:s}=group;
  if(a===undefined||f===undefined||s===undefined||s<=0||a>f||f>s)continue;
  output.push({filesystem_id:await digest(encoder.encode(identity)),mountpoint:group.mountpoint,available_bytes:a,free_bytes:f,size_bytes:s});
  if(output.length===32)break;
 }
 return output.sort((a,b)=>a.filesystem_id.localeCompare(b.filesystem_id));
}
async function collect(started:number,deadline:number,caps:Capabilities):Promise<ProbeResult> {
 let status:number|null=null;let sourceHash:string|null=null;
 try {
  const key=metricCredential();const fetchBudget=remaining(deadline,8500);
  if(fetchBudget<100)throw new ProbeError("METRICS_READ_TIMEOUT");
  const fetchDeadline=Date.now()+fetchBudget;const signal=AbortSignal.timeout(fetchBudget);
  const response=await fetch(METRICS_URL,{method:"GET",headers:{Authorization:"Basic "+btoa("username:"+key)},signal,redirect:"error"});
  status=response.status;
  if(status!==200){void response.body?.cancel().catch(()=>{});throw new ProbeError("METRICS_HTTP_ERROR");}
  const declared=response.headers.get("content-length");
  if(declared&&Number(declared)>MAX_RESPONSE_BYTES){void response.body?.cancel().catch(()=>{});throw new ProbeError("METRICS_RESPONSE_TOO_LARGE");}
  const bytes=await readLimited(response.body,MAX_RESPONSE_BYTES,fetchDeadline,signal);
  sourceHash=await digest(bytes);
  const text=new TextDecoder("utf-8",{fatal:true}).decode(bytes);
  const metrics=await parseMetrics(text,fetchDeadline);
  if(metrics.length===0)throw new ProbeError("FILESYSTEM_METRICS_MISSING");
  return {outcome:"SUCCEEDED",observed_at:new Date().toISOString(),metrics_http_status:status,source_sha256:sourceHash,metrics,error_code:null,elapsed_ms:Date.now()-started,capabilities:caps};
 }catch(error){
  const allowed=new Set(["METRICS_CREDENTIAL_UNAVAILABLE","METRICS_HTTP_ERROR","METRICS_RESPONSE_TOO_LARGE","METRICS_READ_TIMEOUT","METRICS_PARSE_ERROR","FILESYSTEM_METRICS_MISSING"]);
  const code=error instanceof ProbeError&&allowed.has(error.code)?error.code:
   error instanceof DOMException&&["AbortError","TimeoutError"].includes(error.name)?"METRICS_READ_TIMEOUT":
   error instanceof TypeError?"METRICS_PARSE_ERROR":"UNEXPECTED_FAILURE";
  return {outcome:"FAILED",observed_at:new Date().toISOString(),metrics_http_status:status,source_sha256:sourceHash,metrics:[],error_code:code,elapsed_ms:Date.now()-started,capabilities:caps};
 }
}
function safeSnapshot(value:ProbeResult|null):ProbeResult|null {
 if(!value||!["SUCCEEDED","FAILED"].includes(value.outcome)||!Number.isFinite(Date.parse(value.observed_at))
  ||!Number.isInteger(value.elapsed_ms)||value.elapsed_ms<0||value.elapsed_ms>TOTAL_TIMEOUT_MS
  ||(value.metrics_http_status!==null&&(!Number.isInteger(value.metrics_http_status)||value.metrics_http_status<100||value.metrics_http_status>599))
  ||(value.source_sha256!==null&&!/^[0-9a-f]{64}$/.test(value.source_sha256))||!Array.isArray(value.metrics)||value.metrics.length>32)return null;
 const errors=new Set(["METRICS_CREDENTIAL_UNAVAILABLE","METRICS_HTTP_ERROR","METRICS_RESPONSE_TOO_LARGE","METRICS_READ_TIMEOUT","METRICS_PARSE_ERROR","FILESYSTEM_METRICS_MISSING","UNEXPECTED_FAILURE"]);
 if(value.outcome==="SUCCEEDED"&&(value.metrics_http_status!==200||value.source_sha256===null||value.error_code!==null||value.metrics.length===0))return null;
 if(value.outcome==="FAILED"&&(!errors.has(value.error_code||"")||value.metrics.length!==0))return null;
 const flags=value.capabilities;if(!flags||[flags.sec_user_agent_configured,flags.sec_contact_email_configured,flags.massive_key_configured].some(x=>typeof x!=="boolean"))return null;
 const metrics:Metric[]=[];
 for(const row of value.metrics){
  if(!/^[0-9a-f]{64}$/.test(row.filesystem_id)||typeof row.mountpoint!=="string"||row.mountpoint.length>160||/[\u0000-\u001f\u007f]/.test(row.mountpoint)
   ||[row.available_bytes,row.free_bytes,row.size_bytes].some(x=>!Number.isSafeInteger(x))||row.available_bytes<0||row.free_bytes<row.available_bytes||row.size_bytes<=0||row.size_bytes<row.free_bytes)return null;
  metrics.push({filesystem_id:row.filesystem_id,mountpoint:row.mountpoint,available_bytes:row.available_bytes,free_bytes:row.free_bytes,size_bytes:row.size_bytes});
 }
 if(new Set(metrics.map(row=>row.filesystem_id)).size!==metrics.length)return null;
 return {outcome:value.outcome,observed_at:value.observed_at,metrics_http_status:value.metrics_http_status,source_sha256:value.source_sha256,
  metrics,error_code:value.error_code,elapsed_ms:value.elapsed_ms,capabilities:{sec_user_agent_configured:flags.sec_user_agent_configured,
   sec_contact_email_configured:flags.sec_contact_email_configured,massive_key_configured:flags.massive_key_configured}};
}
Deno.serve(async(req:Request)=>{
 const started=Date.now();const deadline=started+TOTAL_TIMEOUT_MS;
 let db:ReturnType<typeof postgres>|null=null;
 let phase:"VALIDATE"|"CLAIM"|"METRICS"|"FINISH"="VALIDATE";
 let capabilityClaimed=false;let capturedSnapshot:ProbeResult|null=null;
 try {
  if(req.method!=="POST")return json({error:"MethodNotAllowed"},405);
  if(Deno.env.get("SUPABASE_URL")!==PROJECT_URL)return json({error:"ProjectMismatch"},409);
  if(req.headers.get("content-type")?.split(";")[0].trim().toLowerCase()!=="application/json")return json({error:"InvalidRequest"},400);
  const declared=req.headers.get("content-length");if(declared&&Number(declared)>512)return json({error:"InvalidRequest"},413);
  let body:Record<string,unknown>;
  try {const bodyDeadline=Math.min(deadline,Date.now()+600);const signal=AbortSignal.timeout(Math.max(1,bodyDeadline-Date.now()));
   body=JSON.parse(new TextDecoder("utf-8",{fatal:true}).decode(await readLimited(req.body,512,bodyDeadline,signal)));
  }catch{return json({error:"InvalidRequest"},400);}
  if(!body||typeof body!=="object"||Array.isArray(body)||Object.keys(body).sort().join(",")!=="probe_id,token"
   ||typeof body.probe_id!=="string"||!(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(body.probe_id))
   ||typeof body.token!=="string"||!(/^[0-9a-f]{64}$/.test(body.token)))return json({error:"InvalidRequest"},400);
  const databaseUrl=Deno.env.get("SUPABASE_DB_URL");if(!databaseUrl)return json({error:"ProbeControlUnavailable"},503);
  db=postgres(databaseUrl,{max:1,prepare:false,ssl:"require",connect_timeout:5,idle_timeout:30,max_lifetime:60,debug:false,onnotice:()=>{}});
  const probeId=body.probe_id;const token=body.token;
  phase="CLAIM";
  const claim=await timed(db.begin(async(tx)=>{
   await tx`set local statement_timeout='6s'`;await tx`set local lock_timeout='5s'`;
   const rows=await tx`select population_top3000_202608_202609_v1.resource_probe_claim_v9(${probeId}::uuid,${token}::text) as result`;
   return rows[0].result as {state:string;probe_attempt:number;total_timeout_ms:number};
  }),Math.min(7500,remaining(deadline,9000)));
  if(claim.state!=="CLAIMED"||(!Number.isInteger(claim.probe_attempt)||(claim.probe_attempt<18||claim.probe_attempt>41))||claim.total_timeout_ms!==TOTAL_TIMEOUT_MS)return json({error:"ProbeNotAdmitted"},403);
  capabilityClaimed=true;
  phase="METRICS";
  const snapshot=await collect(started,deadline,capabilities());
  capturedSnapshot=snapshot;
  if(remaining(deadline)<150||snapshot.elapsed_ms>TOTAL_TIMEOUT_MS)return json({error:"ProbeDeadlineExceeded",probe_id:probeId,receipt_recorded:false,snapshot:safeSnapshot(capturedSnapshot)},504);
  phase="FINISH";
  const finished=await timed(db.begin(async(tx)=>{
   await tx`set local statement_timeout='6s'`;await tx`set local lock_timeout='5s'`;
   const rows=await tx`select population_top3000_202608_202609_v1.resource_probe_finish_v9(${probeId}::uuid,${token}::text,${tx.json(snapshot)}::jsonb) as result`;
   return rows[0].result as {state:string;outcome?:string};
  }),Math.min(7500,remaining(deadline,50)));
  if(finished.state!=="RECORDED"&&finished.state!=="ALREADY_RECORDED")return json({error:"ProbeReceiptNotAdmitted",probe_id:probeId,receipt_recorded:false,snapshot:safeSnapshot(capturedSnapshot)},409);
  return json({state:finished.state,probe_id:probeId,outcome:finished.outcome});
 }catch(error){
  // Return only a fixed phase, elapsed duration and allowlisted SQLSTATE; never raw errors, inputs or credentials.
  const allowSqlStates=new Set(["57014","55P03","40001","40P01","08000","08001","08003","08004","08006","08007","08P01","57P01","57P02","57P03","53300","53400","23514","22023","22P02","42883","42501","P0001","22007"]);
  const candidate=error&&typeof error==="object"&&"code" in error?(error as {code:unknown}).code:null;
  const sqlstate=typeof candidate==="string"&&allowSqlStates.has(candidate)?candidate:null;
  const control_code=error instanceof ProbeError&&error.code==="DEADLINE"?"DEADLINE":"CONTROL_ERROR";
  const allowedValidators=new Set(["INVALID_SANITIZED_PROBE_RESULT","INVALID_SANITIZED_CAPABILITY_FLAGS","INCOMPLETE_SUCCESSFUL_PROBE_RESULT","INVALID_PROBE_FAILURE_CODE","INVALID_FILESYSTEM_METRIC","DUPLICATE_FILESYSTEM_METRIC"]);
  const message=error&&typeof error==="object"&&"message" in error?(error as {message:unknown}).message:null;
  const validation_error=typeof message==="string"&&allowedValidators.has(message)?message:null;
  const fallback=capabilityClaimed&&phase==="FINISH"?safeSnapshot(capturedSnapshot):null;
  return json({error:"ProbeControlUnavailable",phase,sqlstate,control_code,validation_error,elapsed_ms:Math.max(0,Date.now()-started),
   receipt_recorded:false,snapshot:fallback},503);
 }
 finally {if(db)void db.end({timeout:0}).catch(()=>{});}
});
