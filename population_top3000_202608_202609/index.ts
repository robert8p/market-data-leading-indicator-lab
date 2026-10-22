import postgres from "postgres";

const S="population_top3000_202608_202609_v1";
const VERSION="top3000-source-v1.0.0";
const EXPECTED_SCOPE_SHA256="8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd";
const MAX_BYTES=8*1024*1024;
class Failure extends Error {constructor(public code:string,public retry=false,public delay=60,public evidence:Record<string,unknown>={}){super(code);}}
const hash=async(bytes:Uint8Array)=>Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256",bytes))).map(x=>x.toString(16).padStart(2,"0")).join("");
const pause=(ms:number)=>new Promise(resolve=>setTimeout(resolve,ms));

export function sourceUrl(job:any):URL {
 if(job.snapshot_date!=="2026-08-03")throw new Failure("JOB_SCOPE_MISMATCH");
 const url=new URL("https://api.massive.com"+(job.kind==="CENSUS"?"/v3/reference/tickers":"/v3/reference/tickers/"+encodeURIComponent(job.ticker)));
 if(job.kind==="CENSUS"){
  if(job.params.cursor){
   if(typeof job.params.cursor!=="string"||job.params.cursor.length>8192)throw new Failure("CURSOR_INVALID");
   url.searchParams.set("cursor",job.params.cursor);
  }else{
   for(const [k,v]of Object.entries({market:"stocks",locale:"us",active:"true",date:"2026-08-03",limit:"1000",sort:"ticker",order:"asc"}))url.searchParams.set(k,v);
  }
 }else if(job.kind==="OVERVIEW"){
  if(typeof job.ticker!=="string"||job.ticker.length>64||!job.ticker.length)throw new Failure("TICKER_INVALID");
  url.searchParams.set("date","2026-08-03");
 }else throw new Failure("JOB_KIND_NOT_ALLOWED");
 return url;
}

export function nextCursor(body:any,job:any):string|null {
 if(!body.next_url)return null;
 if(job.kind!=="CENSUS"||typeof body.next_url!=="string")throw new Failure("UNEXPECTED_PAGINATION");
 const u=new URL(body.next_url);
 if(!["api.massive.com","api.polygon.io"].includes(u.hostname)||u.protocol!=="https:"||u.pathname!=="/v3/reference/tickers"||u.username||u.password||u.hash)throw new Failure("PAGINATION_ORIGIN_MISMATCH");
 const cursor=u.searchParams.get("cursor");
 if(!cursor||cursor.length>8192||cursor===job.params.cursor)throw new Failure("PAGINATION_CURSOR_INVALID");
 // Never store a provider credential embedded in a pagination URL.
 for(const key of [...u.searchParams.keys()])if(/^(apikey|api_key|key|token|authorization)$/i.test(key))u.searchParams.delete(key);
 body.next_url=u.toString();
 return cursor;
}

async function fetchSource(job:any,key:string){
 const url=sourceUrl(job),started=new Date().toISOString();
 let response:Response;
 try{response=await fetch(url,{headers:{Authorization:"Bearer "+key},redirect:"error",signal:AbortSignal.timeout(25000)});}catch(e){throw new Failure(e instanceof Error&&e.name==="TimeoutError"?"PROVIDER_TIMEOUT_UNCERTAIN":"PROVIDER_TRANSPORT_UNCERTAIN",true,60);}
 const received=new Date().toISOString();
 const evidence:Record<string,unknown>={provider:"Massive",route:url.pathname,request_params:Object.fromEntries(url.searchParams),request_started_at:started,received_at:received,http_status:response.status,http_date:response.headers.get("date"),request_id:response.headers.get("x-request-id"),code_version:VERSION,original_historical_receipt_proven:false};
 if(!response.ok){
  await response.body?.cancel();
  const ra=response.headers.get("retry-after");let delay=ra?Number(ra):60;
  if(!Number.isFinite(delay))delay=ra?Math.ceil((Date.parse(ra)-Date.now())/1000):60;
  if(!Number.isFinite(delay))delay=60;
  if(delay>2147483646)throw new Failure("RETRY_AFTER_OUTSIDE_SUPPORTED_BOUND",false,60,{...evidence,retry_after:ra});
  throw new Failure("PROVIDER_HTTP_"+response.status,response.status===429||response.status>=500,Math.max(1,Math.ceil(delay)),{...evidence,retry_after:ra});
 }
 if(!response.body)throw new Failure("EMPTY_RESPONSE_BODY",false,60,evidence);
 const reader=response.body.getReader();const chunks:Uint8Array[]=[];let count=0;
 try{
  while(true){const item=await reader.read();if(item.done)break;count+=item.value.byteLength;if(count>MAX_BYTES){await reader.cancel();throw new Failure("RESPONSE_BYTE_LIMIT",false,60,{...evidence,observed_bytes:count});}chunks.push(item.value);}
 }catch(e){if(e instanceof Failure)throw e;throw new Failure("RESPONSE_STREAM_UNCERTAIN",true,60,evidence);}
 const bytes=new Uint8Array(count);let offset=0;for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
 let raw:string,body:any;
 try{raw=new TextDecoder("utf-8",{fatal:true}).decode(bytes);body=JSON.parse(raw);}catch{throw new Failure("RESPONSE_JSON_OR_UTF8_INVALID",false,60,evidence);}
 if(!body||typeof body!=="object"||Array.isArray(body))throw new Failure("RESPONSE_SHAPE_INVALID",false,60,evidence);
 const originalNext=body.next_url;const cursor=nextCursor(body,job);
 // Exact received wire hash is retained. Only an embedded credential in next_url is redacted.
 const redacted=originalNext!==body.next_url;
 const storedRaw=redacted?JSON.stringify(body):raw;
 const storedBytes=new TextEncoder().encode(storedRaw);
 if(storedBytes.length>MAX_BYTES)throw new Failure("STORED_RESPONSE_BYTE_LIMIT",false,60,evidence);
 return {raw:storedRaw,sha:await hash(storedBytes),evidence:{...evidence,provider_request_id:body.request_id??null,next_cursor:cursor,wire_bytes:count,wire_sha256:await hash(bytes),stored_bytes:storedBytes.length,redaction:redacted?"NEXT_URL_CREDENTIAL_QUERY_REMOVED":"NONE"}};
}

export async function handler(req:Request):Promise<Response>{
 if(req.method!=="POST")return Response.json({state:"METHOD_REJECTED"},{status:405});
 if(Number(req.headers.get("content-length")||0)>4096)return Response.json({state:"INPUT_LIMIT"},{status:413});
 let b:any;
 try{
  if(!req.body)throw new Error();
  const reader=req.body.getReader();let input="",size=0;const decoder=new TextDecoder("utf-8",{fatal:true});
  while(true){const part=await reader.read();if(part.done)break;size+=part.value.length;if(size>4096){await reader.cancel();return Response.json({state:"INPUT_LIMIT"},{status:413});}input+=decoder.decode(part.value,{stream:true});}
  input+=decoder.decode();b=JSON.parse(input);
 }catch{return Response.json({state:"INVALID_INPUT"},{status:400});}
 if(typeof b.attempt_id!=="string"||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(b.attempt_id)||typeof b.token!=="string"||!/^[0-9a-f]{64}$/.test(b.token))return Response.json({state:"AUTHENTICATION_REQUIRED"},{status:401});
 const dbUrl=Deno.env.get("SUPABASE_DB_URL");if(!dbUrl)return Response.json({state:"DATABASE_CONFIGURATION_MISSING"},{status:503});
 const sql=postgres(dbUrl,{max:1,prepare:false,idle_timeout:2,connect_timeout:5,connection:{application_name:VERSION,statement_timeout:"8000",lock_timeout:"1500",max_parallel_workers_per_gather:"0"}});
 let claimed=false;
 try{
  let r:any;
  for(let tries=0;tries<3;tries++){
   r=(await sql.unsafe(`select ${S}.claim_v1($1,$2) as result`,[b.attempt_id,b.token]))[0].result;
   if(r.state!=="RATE_WAIT")break;
   await pause(Math.max(1,Math.min(550,Number(r.milliseconds)||500)));
  }
  if(r.state!=="CLAIMED")return Response.json({state:r.state});
  claimed=true;
  if(r.scope_sha256!==EXPECTED_SCOPE_SHA256)throw new Failure("CODE_SCOPE_HASH_MISMATCH");
  if(!r.cached){
   const key=(Deno.env.get("MASSIVE_API_KEY")||Deno.env.get("MASSIVE_KEY")||Deno.env.get("POLYGON_API_KEY")||Deno.env.get("POLYGON_KEY")||"").trim();
   if(!key)throw new Failure("PROVIDER_CREDENTIAL_MISSING");
   const source=await fetchSource(r.job,key);
   const saved=(await sql.unsafe(`select ${S}.save_source_v1($1,$2,$3,$4,$5::text::jsonb) as result`,[b.attempt_id,b.token,source.raw,source.sha,JSON.stringify(source.evidence)]))[0].result;
   if(saved.state==="INTERRUPT_CHECKPOINT_TEST")return Response.json({state:saved.state},{status:503});
  }
  const finished=(await sql.unsafe(`select ${S}.finish_v1($1,$2) as result`,[b.attempt_id,b.token]))[0].result;
  return Response.json(finished);
 }catch(e:any){
  const pgcode=typeof e?.code==="string"?e.code:null;
  const known:Record<string,string>={"55P03":"DATABASE_LOCK_CONFLICT","57014":"DATABASE_STATEMENT_TIMEOUT","53300":"DATABASE_CONNECTION_LIMIT","28P01":"DATABASE_AUTHENTICATION","42501":"DATABASE_PERMISSION"};
  const f=e instanceof Failure?e:new Failure(known[pgcode||""]||(pgcode&&/^(22|23|42|P0001)/.test(pgcode)?"DATABASE_CODE_INPUT_OR_FENCE":"DATABASE_TRANSPORT_UNCERTAIN"),["55P03","57014","53300"].includes(pgcode||"")||!pgcode,60,{sqlstate:pgcode});
  if(claimed){try{const result=(await sql.unsafe(`select ${S}.fail_v1($1,$2,$3,$4,$5,$6::text::jsonb) as result`,[b.attempt_id,b.token,f.code,f.retry,f.delay,JSON.stringify(f.evidence)]))[0].result;return Response.json(result,{status:result.state==="ALREADY_COMMITTED"?200:503});}catch{/* Expired/changed owners cannot mutate the old attempt. Dispatcher reconciles its lease. */}}
  return Response.json({state:"REQUEST_FAILED",error_class:f.code},{status:503});
 }finally{await sql.end({timeout:2}).catch(()=>{});}
}

Deno.serve(handler);
