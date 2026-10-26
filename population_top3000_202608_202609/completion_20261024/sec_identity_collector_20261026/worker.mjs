// Exact SEC original-byte preservation. No source admission or mapping acceptance.
export class WorkerError extends Error { constructor(code){ super(code);this.code=code; } }
const E=new TextEncoder(), MAX=8388608;
const fail=c=>{throw new WorkerError(c);};
export async function sha(b){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',b))].map(x=>x.toString(16).padStart(2,'0')).join('');}
export function validateManifest(m,allowlist){
 if(!m||!/^\d{10}$/.test(m.cik)||m.cik==='0000000000'||m.max_bytes!==MAX||m.max_runtime_ms!==45000||!/^\w[\w:.-]{0,250}$/.test(m.request_key)||!/^[a-f0-9]{64}$/.test(m.user_agent_sha256))fail('INVALID_MANIFEST');
 let expected;
 if(m.kind==='SEC_SUBMISSIONS'){if(m.accession!==null&&m.accession!==undefined)fail('INVALID_MANIFEST');expected=`https://data.sec.gov/submissions/CIK${m.cik}.json`;}
 else if(m.kind==='SEC_FULL_SUBMISSION'){if(!/^\d{10}-\d{2}-\d{6}$/.test(m.accession))fail('INVALID_MANIFEST');expected=`https://www.sec.gov/Archives/edgar/data/${m.cik.replace(/^0+/,'')}/${m.accession}.txt`;}
 else fail('INVALID_MANIFEST');
 if(m.request_url!==expected||!allowlist.has(expected))fail('UNADMITTED_ROUTE');return expected;
}
export async function readBounded(stream,cap,deadline,now=Date.now,onBytes=()=>{}){
 if(!stream)fail('EMPTY_BODY');const reader=stream.getReader(),chunks=[];let n=0;
 try{for(;;){const ms=deadline-now();if(ms<=0)fail('DEADLINE');let timer;let r;
  try{r=await Promise.race([reader.read(),new Promise((_,rej)=>{timer=setTimeout(()=>rej(new WorkerError('DEADLINE')),ms);})]);}finally{clearTimeout(timer);}
  if(r.done)break;if(!(r.value instanceof Uint8Array))fail('INVALID_STREAM');n+=r.value.length;onBytes(n);if(n>cap)fail('RESPONSE_TOO_LARGE');chunks.push(r.value);
 }const b=new Uint8Array(n);let at=0;for(const c of chunks){b.set(c,at);at+=c.length;}return b;
 }finally{void reader.cancel().catch(()=>{});}
}
export function validateWire(bytes,m){
 if(bytes.length===0||bytes.length>m.max_bytes)fail('INVALID_BODY_SIZE');
 if(bytes.length>=3&&bytes[0]===239&&bytes[1]===187&&bytes[2]===191)fail('BOM_REJECTED');
 let text;try{text=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes);}catch{fail('INVALID_UTF8');}
 const round=E.encode(text);if(round.length!==bytes.length||round.some((b,i)=>b!==bytes[i]))fail('WIRE_ROUNDTRIP');
 if(m.kind==='SEC_SUBMISSIONS'){let j;try{j=JSON.parse(text);}catch{fail('INVALID_SEC_JSON');}if(!j||Array.isArray(j)||typeof j!=='object'||!['string','number'].includes(typeof j.cik)||!/^\d{1,10}$/.test(String(j.cik))||String(j.cik).padStart(10,'0')!==m.cik)fail('SEC_CIK_MISMATCH');}
 else{
  const starts=[...text.matchAll(/<SEC-HEADER>/g)];if(starts.length!==1)fail('SEC_HEADER_MISMATCH');const end=text.indexOf('</SEC-HEADER>',starts[0].index);if(end<0||end-starts[0].index>262144)fail('SEC_HEADER_MISMATCH');
  const header=text.slice(starts[0].index,end);const acc=[...header.matchAll(/^ACCESSION NUMBER:\s*(\d{10}-\d{2}-\d{6})\s*$/gm)];if(acc.length!==1||acc[0][1]!==m.accession)fail('SEC_ACCESSION_MISMATCH');
  const ciks=[...header.matchAll(/^\s*CENTRAL INDEX KEY:\s*(\d{1,10})\s*$/gm)].map(x=>x[1].padStart(10,'0'));if(!ciks.includes(m.cik))fail('SEC_CIK_MISMATCH');
 }return text;
}
function b64(bytes){let s='';for(let i=0;i<bytes.length;i+=32768)s+=String.fromCharCode(...bytes.subarray(i,i+32768));return btoa(s);}
const response=(body,status=200)=>Response.json(body,{status,headers:{'cache-control':'no-store','x-content-type-options':'nosniff'}});
export function createWorker({rpc,fetch:fetchSource,userAgent,allowlist,now=Date.now}){
 return async req=>{
  const started=now(),deadline=started+45000;let phase='INPUT',reserved=false,fetchStarted=false,attempt,token,status=null,received=0,wire=null,wireHash=null,manifest;
  const call=async(name,payload)=>{const ms=Math.min(8000,deadline-now());if(ms<=0)fail('DEADLINE');let timer;try{return await Promise.race([rpc(name,{attempt,token,payload}),new Promise((_,rej)=>{timer=setTimeout(()=>rej(new WorkerError('DEADLINE')),ms);})]);}finally{clearTimeout(timer);}};
  try{
   if(req.method!=='POST')return response({error:'METHOD'},405);
   if(req.headers.get('content-type')?.split(';')[0].trim().toLowerCase()!=='application/json')return response({error:'INPUT'},400);
   const bytes=await readBounded(req.body,512,Math.min(deadline,now()+1000),now);let input;try{input=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));}catch{fail('INVALID_INPUT');}
   if(!input||Array.isArray(input)||Object.keys(input).sort().join(',')!=='attempt_id,token'||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(input.attempt_id)||!/^[a-f0-9]{64}$/.test(input.token))fail('INVALID_INPUT');
   attempt=input.attempt_id;token=input.token;
   if(typeof userAgent!=='string'||userAgent.length>512||/[\r\n]/.test(userAgent)||!/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/.test(userAgent))fail('CONTACT_NOT_CONFIGURED');
   phase='CONTEXT';manifest=await call('context');validateManifest(manifest,allowlist);const ua=await sha(E.encode(userAgent));if(ua!==manifest.user_agent_sha256)fail('CONTACT_HASH_MISMATCH');
   phase='CLAIM';const claimed=await call('claim',{user_agent_sha256:ua});if(claimed?.state!=='RESERVED')return response({state:claimed?.state||'NOT_ADMITTED',provider_requested:false});reserved=true;
   validateManifest(claimed,allowlist);for(const k of ['request_key','kind','cik','accession','request_url','max_bytes','max_runtime_ms','user_agent_sha256'])if(claimed[k]!==manifest[k])fail('CLAIM_CHANGED');manifest=claimed;
   phase='FETCH';const ms=Math.min(25000,deadline-now()-12000);if(ms<=0)fail('DEADLINE');fetchStarted=true;
   const res=await fetchSource(manifest.request_url,{method:'GET',redirect:'error',headers:{'User-Agent':userAgent,'Accept-Encoding':'identity',Accept:manifest.kind==='SEC_SUBMISSIONS'?'application/json':'text/plain'},signal:AbortSignal.timeout(ms)});status=res.status;
   if(res.redirected)fail('REDIRECT_REJECTED');if(status!==200){void res.body?.cancel().catch(()=>{});fail('HTTP_STATUS');}
   const enc=res.headers.get('content-encoding');if(enc&&enc.toLowerCase()!=='identity'){void res.body?.cancel().catch(()=>{});fail('CONTENT_ENCODING');}
   const ct=res.headers.get('content-type');if(!ct||!(manifest.kind==='SEC_SUBMISSIONS'?/^(application\/json)(?:;|$)/i:/^(text\/plain|application\/octet-stream)(?:;|$)/i).test(ct)){void res.body?.cancel().catch(()=>{});fail('CONTENT_TYPE');}
   const length=res.headers.get('content-length');if(length&&(!/^\d+$/.test(length)||Number(length)>MAX)){void res.body?.cancel().catch(()=>{});fail('RESPONSE_TOO_LARGE');}
   wire=await readBounded(res.body,MAX,Math.min(deadline-10000,now()+ms),now,n=>{received=n;});wireHash=await sha(wire);validateWire(wire,manifest);
   const receipt={wire_base64:b64(wire),wire_sha256:wireHash,wire_bytes:wire.length,request_url:manifest.request_url,http_status:200,retrieved_at:new Date(now()).toISOString(),elapsed_ms:now()-started,content_type:ct,content_encoding:enc||'identity'};
   phase='COMMIT';const result=await call('commit',receipt);return response({state:result?.state||'RECEIPT_UNCONFIRMED',receipt_recorded:['COMMITTED','ALREADY_COMMITTED'].includes(result?.state),wire_sha256:wireHash,wire_bytes:wire.length});
  }catch(e){const code=e instanceof WorkerError?e.code:'CONTROL_OR_TRANSPORT_FAILURE';
   if(reserved&&phase!=='COMMIT'&&deadline-now()>1000){try{const payload={code,fetch_started:fetchStarted,http_status:status,received_bytes:received,effects_known:!fetchStarted||wire!==null,wire_complete:wire!==null};if(wire){payload.wire_base64=b64(wire);payload.wire_sha256=wireHash;payload.wire_bytes=wire.length;}phase='FAILURE';const r=await call('failure',payload);return response({state:r?.state||'UNKNOWN_EFFECTS_HELD',code,receipt_recorded:true,provider_requested:fetchStarted});}catch{/* Keep the fenced lease for unknown effects. */}}
   return response({error:'RECEIPT_UNCONFIRMED',phase,code,provider_requested:fetchStarted,receipt_recorded:false},503);
  }
 };
}
