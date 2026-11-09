'use strict';
// Independent observer. Only the fixed public HTTPS login and three ordinary report routes are reachable.
const fs = require('node:fs');
const path = require('node:path');
const https = require('node:https');
const crypto = require('node:crypto');
const zlib = require('node:zlib');
const {performance} = require('node:perf_hooks');

const ORIGIN = 'https://market-data-lab-web.onrender.com';
const PREFIX = '/eq17/api/';
const LOGIN = PREFIX+'login', STATUS = PREFIX+'activation-status', REPORT = PREFIX+'activation-report?session=2026-11-09', P9 = PREFIX+'positive9?scope=live';
const PREFLIGHT = [STATUS, REPORT, P9];
const ROOT='/var/data/eq17/evidence', DATE='2026-11-09';
const FIRST=Date.parse(DATE+'T14:46:00Z'), BEGIN=FIRST-600000, END=FIRST+358*60000+300000;
const SIZE=8*1024*1024, DEADLINE=30000;
const manifest='7a440f36b9b4cfeaeb09fc2477c49b4ce17a4a953d4030df320f72c2f27f1140';
const retirementDigest='fc3339912cb85d5ee9ac8344b39a236053443897f9128a7ac79763d564545fda';
const expectedAdmission='137345ad42c89ce437138a3b438d4c6ae9abb58294c3b5515804d43f9c9be6d6';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const ensure=(ok,msg)=>{if(!ok)throw new Error(msg);};
const json=(f)=>JSON.parse(fs.readFileSync(f));
const mkdir=(d)=>fs.mkdirSync(d,{recursive:true,mode:0o700});
const immutable=(f,b)=>{
  mkdir(path.dirname(f));
  if(fs.existsSync(f)){ensure(fs.readFileSync(f).equals(b),'immutable_path_conflict');return;}
  fs.writeFileSync(f,b,{flag:'wx',mode:0o600});
};
const object=(f,o)=>immutable(f,Buffer.from(JSON.stringify(o)+'\n'));
const files=(d)=>fs.existsSync(d)?fs.readdirSync(d,{withFileTypes:true}).filter(x=>x.isFile()).map(x=>x.name).sort():[];
const dirs=(d)=>fs.existsSync(d)?fs.readdirSync(d,{withFileTypes:true}).filter(x=>x.isDirectory()).map(x=>x.name).sort():[];
const numbered=n=>String(n).padStart(4,'0');
const stamp=()=>new Date().toISOString();
const log=(event,data={})=>console.log(JSON.stringify({observer:event,utc:stamp(),...data}));
function admission(raw){
  ensure(sha(raw)===expectedAdmission,'admission_hash_mismatch');
  const a=JSON.parse(raw);
  ensure(a.production_serving===true && a.owner_verified===true && a.schema==='eq17.production_serving.v1','invalid_serving_attestation');
  return a;
}
function readConfig(){
  const raw=Buffer.from(process.env.RECORDER_BOUND_ADMISSION_B64||'','base64');
  ensure(Boolean(process.env.EQ17_OBSERVER_PASSWORD),'login_secret_unconfigured');
  const generation=process.env.RECORDER_RECOVERY_ID||'initial';
  ensure(/^[A-Za-z0-9_-]{1,48}$/.test(generation),'invalid_recovery_generation');
  return {raw,a:admission(raw),generation};
}
function ordinary(route,cookie,loginPassword,deadlineMs=DEADLINE){
  const login=route===LOGIN;
  ensure(login ? Boolean(loginPassword)&&!cookie : PREFLIGHT.includes(route)&&Boolean(cookie)&&!loginPassword,'route_not_permitted');
  const start=Date.now(),mono=performance.now();
  const payload=login?Buffer.from(JSON.stringify({password:loginPassword})):null;
  const headers={Accept:'application/json','Accept-Encoding':'identity','Cache-Control':'no-cache','User-Agent':'EQ17 independent Render HTTPS observer/2'};
  if(login){headers['Content-Type']='application/json'; headers['Content-Length']=payload.length;}
  else headers.Cookie=cookie;
  return new Promise((resolve,reject)=>{
    let timedOut=false;
    const req=https.request(ORIGIN+route,{method:login?'POST':'GET',headers,agent:false,maxHeaderSize:32768,rejectUnauthorized:true},res=>{
      let n=0;const chunks=[];
      res.on('data',chunk=>{n+=chunk.length;if(n>SIZE){req.destroy(new Error('body_too_large'));return;} chunks.push(chunk);});
      res.on('error',()=>reject(new Error('response_interrupted')));
      res.on('end',()=>{
        try{
          ensure(res.complete,'incomplete_http_message');
          if(login){
            ensure(res.statusCode===200,'login_status_'+res.statusCode);
            const session=(res.headers['set-cookie']||[]).filter(v=>v.startsWith('eq17='));
            ensure(session.length===1&&/;\s*HttpOnly(?:;|$)/i.test(session[0])&&/;\s*Secure(?:;|$)/i.test(session[0])&&/;\s*SameSite=Strict(?:;|$)/i.test(session[0]),'invalid_login_cookie');
            const value=session[0].split(';',1)[0];
            ensure(/^eq17=[-A-Za-z0-9_=]+\.[0-9a-f]{64}$/.test(value),'invalid_session_format');
            resolve(value); return;
          }
          ensure(!Object.keys(res.headers).some(k=>/^(set-cookie2?|authorization|proxy-authorization|authentication-info|proxy-authentication-info)$/i.test(k)),'unexpected_secret_response_header');
          const end=Date.now(),body=Buffer.concat(chunks);
          const sensitive=[cookie.slice(5),process.env.EQ17_OBSERVER_PASSWORD].filter(Boolean);
          ensure(!sensitive.some(x=>body.includes(Buffer.from(x))||res.rawHeaders.some(v=>v.includes(x))),'unexpected_secret_response');
          const receipt={url:ORIGIN+route,start_ms:start,end_ms:end,elapsed_ms:Math.ceil(performance.now()-mono),status:res.statusCode,
            cache_control:res.headers['cache-control']||'',content_type:res.headers['content-type']||'',service_worker:false,
            body_sha256:sha(body),bytes:body.length,raw_headers:res.rawHeaders,http_version:res.httpVersion,
            vantage:'RENDER_INDEPENDENT_HTTPS',transport:'node:https/TLS',content_encoding:res.headers['content-encoding']||'identity'};
          resolve({receipt,body});
        }catch(e){reject(e);}
      });
    });
    const timer=setTimeout(()=>{timedOut=true;req.destroy();},Math.min(deadlineMs,route===P9?20000:DEADLINE));
    req.on('error',e=>reject(new Error(timedOut?'whole_get_timeout':e.message==='body_too_large'?'body_too_large':'transport_request_failed')));
    req.on('close',()=>clearTimeout(timer));
    req.end(payload);
  });
}
function quality({receipt:r,body}){
  ensure(r.status===200 && r.cache_control==='no-store' && r.service_worker===false && r.content_encoding==='identity' &&
    r.content_type.toLowerCase().split(';')[0].trim()==='application/json' && r.bytes===body.length && r.bytes>0 && r.bytes<=SIZE && r.body_sha256===sha(body),'wire_evidence_invalid');
  ensure([r.start_ms,r.end_ms,r.elapsed_ms].every(Number.isSafeInteger)&&r.elapsed_ms>=0 &&r.elapsed_ms<=DEADLINE&&r.end_ms>=r.start_ms && r.end_ms-r.start_ms<=DEADLINE && Math.abs(r.end_ms-r.start_ms-r.elapsed_ms)<=250,'observer_clock_mismatch');
  ensure(Array.isArray(r.raw_headers)&&r.raw_headers.length%2===0&&r.raw_headers.every(v=>typeof v==='string'),'invalid_raw_header_array');
  function one(name,value){const found=r.raw_headers.filter((k,i)=>i%2===0&&k.toLowerCase()===name);return found.length===1&&r.raw_headers[r.raw_headers.indexOf(found[0])+1]===value;}
  ensure(one('cache-control',r.cache_control)&&one('content-type',r.content_type),'actual_no_store_or_type_header_mismatch');
  return JSON.parse(body);
}
function validatePreflight(items,a){
  ensure(items.length===3&&items.every((x,i)=>x.receipt.url===ORIGIN+PREFLIGHT[i]),'preflight_route_mismatch');
  const [s,r,p]=items.map(quality);
  ensure(s.release===a.release&&s.scope==='CURRENT_RELEASE_RESEARCH'&&s.timing.maximum_seconds===300&&s.controls.paused===true&&s.controls.valid_armed_intent===false&&s.alerts_enabled_rules===0&&s.independent_nine.ready===true&&s.independent_nine.requested===true&&s.independent_nine.enabled===false,'preflight_status_failed');
  ensure(r.release===a.release&&r.session.session_date===DATE&&r.session.state==='WAITING'&&r.session.counts.expected===359&&r.session.counts.passed===0,'preflight_calendar_failed');
  ensure(p.scope==='live'&&p.historical===false&&p.manifest_hash===manifest&&p.paused===true&&p.deployed_acceptance.passed===true&&p.deployed_acceptance.release===a.release&&p.deployed_acceptance.manifest_hash===manifest&&p.alert_control.release===a.release&&p.alert_control.manifest_hash===manifest&&p.alert_control.ready===true&&p.alert_control.requested===true&&p.alert_control.enabled===false,'preflight_positive9_failed');
  ensure(s.controls.manual_pause===true&&s.controls.revision===2&&s.controls.action==='USER_STOP'
    &&Array.isArray(p.tracking?.retired_rule_ids)&&sha(JSON.stringify([...p.tracking.retired_rule_ids].sort()))===retirementDigest
    &&items[2].receipt.elapsed_ms<=20000&&items[2].receipt.end_ms-items[2].receipt.start_ms<=20000,'preflight_controls_or_report_budget');
  const gen=Date.parse(s.generated_utc),w=items[0].receipt;
  ensure(Number.isFinite(gen)&&Math.abs(gen-w.end_ms)<=2000+w.elapsed_ms,'server_client_clock_check_failed');
  return {release:a.release,signed_in_responses:3,statuses:items.map(({receipt:x})=>({url:x.url,status:x.status,bytes:x.bytes,body_sha256:x.body_sha256,elapsed_ms:x.elapsed_ms,cache_control:x.cache_control,raw_header_count:x.raw_headers.length/2}))};
}
function store(d,n,{receipt,body}){
  if(body){
    const f=path.join(d,receipt.body_sha256+'.json.gz');
    if(fs.existsSync(f)){ensure(sha(zlib.gunzipSync(fs.readFileSync(f),{maxOutputLength:SIZE}))===receipt.body_sha256,'stored_body_changed');}
    else immutable(f,zlib.gzipSync(body));
  }
  object(path.join(d,numbered(n)+'.receipt.json'),receipt);
}
function readStored(d,n){
  const receipt=json(path.join(d,numbered(n)+'.receipt.json'));
  ensure(/^[0-9a-f]{64}$/.test(receipt.body_sha256),'invalid_digest');
  const body=zlib.gunzipSync(fs.readFileSync(path.join(d,receipt.body_sha256+'.json.gz')),{maxOutputLength:SIZE});
  ensure(sha(body)===receipt.body_sha256&&body.length===receipt.bytes,'persisted_body_mismatch');
  return {receipt,body};
}
function safeCode(e){
  const msg=String(e.message||'');
  return /^[a-z0-9_]{1,90}$/.test(msg)?msg:'recorder_error';
}
async function timedObservation(d,n,route,cookie,flush,transport=ordinary){
  const start=Date.now();let w;
  try{w=await transport(route,cookie);}
  catch(e){store(d,n,{receipt:{url:ORIGIN+route,start_ms:start,end_ms:Date.now(),error:safeCode(e),vantage:'RENDER_INDEPENDENT_HTTPS'}});await flush();throw e;}
  store(d,n,w);await flush();return w;
}
function prefix(root){return path.join(root,'bindings',expectedAdmission,DATE);}
function receipts(d){return files(d).filter(n=>/^\d{4}\.receipt\.json$/.test(n));}
function inspect(root,cfg){
  let validated=null,checked=0;
  const base=prefix(root),pd=path.join(base,'preflight'),cap=path.join(base,'capture');
  for(const d of dirs(pd)){
    const here=path.join(pd,d);
    for(const n of receipts(here)){const r=json(path.join(here,n));if(!r.error){readStored(here,Number(n.slice(0,4)));checked++;}}
    if(fs.existsSync(path.join(here,'verified.json'))){
      ensure(sha(fs.readFileSync(path.join(here,'admission.json')))===expectedAdmission,'preflight_admission_changed');
      const w=[1,2,3].map(i=>readStored(here,i));
      const result=validatePreflight(w,cfg.a);
      const v=json(path.join(here,'verified.json'));
      ensure(JSON.stringify(v.result)===JSON.stringify(result),'persisted_preflight_changed');
      validated={attempt:d,verified_sha256:sha(fs.readFileSync(path.join(here,'verified.json'))),result};
    }
  }
  for(const n of receipts(cap)){const r=json(path.join(cap,n));if(!r.error){readStored(cap,Number(n.slice(0,4)));checked++;}}
  return {validated,complete_bodies_reopened:checked};
}
async function main(){
 process.umask(0o077);
 const cfg=readConfig(),boot=crypto.randomUUID(),root=ROOT,base=prefix(root);
 const {openEvidence}=require('./evidence.cjs');
 const evidence=await openEvidence(root);
 const flush=()=>evidence.flush();
 let stopping=false;let wake;let auth=null;let stopReview=null;
 const invalidate=()=>{auth=null;};
 const session=()=>auth||(auth=ordinary(LOGIN,null,process.env.EQ17_OBSERVER_PASSWORD).catch(e=>{invalidate();throw e;}));
 const beginReviews=()=>{
   if(stopReview||!fs.existsSync(path.join(base,'armed.json')))return;
   stopReview=require('./review.cjs').start({base,boot,release:cfg.a.release,now:Date.now,write:object,flush,session,invalidate,reading:quality,safe:safeCode,log,
     stopping:()=>stopping||fs.existsSync(path.join(base,'holds',cfg.generation+'.json')),
     wire:(d,n,name,cookie,limit)=>timedObservation(d,n,({status:STATUS,positive9:P9,report:REPORT})[name],cookie,flush,(route,c)=>ordinary(route,c,null,limit))});
 };
 const stop=()=>{stopping=true;if(wake)wake();};
 process.once('SIGTERM',stop);process.once('SIGINT',stop);
 const pause=async ms=>{if(stopping||ms<=0)return;await new Promise(resolve=>{const t=setTimeout(resolve,Math.min(ms,60000));wake=()=>{clearTimeout(t);resolve();};});wake=null;};
 const until=async t=>{while(!stopping&&Date.now()<t)await pause(t-Date.now());};
 const idle=async()=>{while(!stopping)await pause(60000);};
 const bootMeta={boot,utc:stamp(),pid:process.pid,service_id:process.env.RENDER_SERVICE_ID||null,instance:process.env.RENDER_INSTANCE_ID||null,source_revision:process.env.RENDER_GIT_COMMIT||null,vantage:'RENDER_INDEPENDENT_HTTPS'};
 try{
   const prior=files(path.join(base,'boots')).length,seen=inspect(root,cfg);
   object(path.join(base,'boots',boot+'.json'),bootMeta);await flush();
   if(prior){
     const recovery={...bootMeta,prior_boots:prior,complete_bodies_reopened:seen.complete_bodies_reopened,
       previous_preflight:seen.validated?.attempt||null,verified_sha256:seen.validated?.verified_sha256||null};
     object(path.join(base,'recovery',boot+'.json'),recovery);await flush();log('recovery',recovery);
   }
   log('started',bootMeta);
   if(fs.existsSync(path.join(base,'terminal.json'))){log('preserving_terminal_evidence');beginReviews();await idle();return;}
   if(fs.existsSync(path.join(base,'holds',cfg.generation+'.json'))){log('preserving_hold',{generation:cfg.generation});await idle();return;}
   let good=seen.validated;
   if(!good){
     if(Date.now()>=FIRST)throw new Error('unqualified_after_first_cutoff');
     // A finite trial budget persists across process restarts and requires operator reconciliation thereafter.
     const pd=path.join(base,'preflight');
     const priorTrials=dirs(pd).filter(x=>x.startsWith(cfg.generation+'-'));
     if(priorTrials.some(x=>{const f=path.join(pd,x,'failure.json');return fs.existsSync(f)&&/^login_status_(401|403|429)$/.test(json(f).error);}))throw new Error('authentication_hold');
     for(let trial=priorTrials.length;trial<3&&!good&&!stopping;trial++){
       const id=cfg.generation+'-'+String(trial+1).padStart(2,'0')+'-'+boot,d=path.join(pd,id);
       const meta={boot,attempt:id,started:stamp(),vantage:bootMeta.vantage};
       immutable(path.join(d,'admission.json'),cfg.raw);object(path.join(d,'start.json'),meta);await flush();
       try{
         const cookie=await ordinary(LOGIN,null,process.env.EQ17_OBSERVER_PASSWORD);
         const items=[];
         for(const [i,route] of PREFLIGHT.entries())items.push(await timedObservation(d,i+1,route,cookie,flush));
         const result=validatePreflight(items,cfg.a);
         object(path.join(d,'verified.json'),{...meta,completed:stamp(),result});await flush();
         good=inspect(root,cfg).validated;ensure(good,'preflight_readback_failed');log('preflight_verified',good);
       }catch(e){
         const error=safeCode(e);object(path.join(d,'failure.json'),{...meta,utc:stamp(),error});await flush();log('preflight_failed',{attempt:id,error});
         if(/^login_status_(401|403|429)$/.test(error)||trial===2||Date.now()+60000>=FIRST)throw e;
         await until(Date.now()+60000);
       }
     }
   }
   if(stopping)return;
   ensure(good,'no_preflight_receipt');
   const cap=path.join(base,'capture'),book=path.join(base,'bookings');
   immutable(path.join(cap,'admission.json'),cfg.raw);
   if(!fs.existsSync(path.join(base,'armed.json'))){
     object(path.join(base,'armed.json'),{...bootMeta,begin:new Date(BEGIN).toISOString(),first:new Date(FIRST).toISOString(),end:new Date(END).toISOString(),preflight:good.attempt,sha256:good.verified_sha256});
   }
   await flush();log('armed',{boot,preflight:good.attempt,release:cfg.a.release,begin:new Date(BEGIN).toISOString(),end:new Date(END).toISOString()});
   beginReviews();
   const interrupted=files(book).filter(n=>/^\d{4}\.json$/.test(n)&&!fs.existsSync(path.join(cap,n.slice(0,4)+'.receipt.json')));
   for(const n of interrupted){const marker=json(path.join(book,n));store(cap,Number(n.slice(0,4)),{receipt:{url:ORIGIN+REPORT,start_ms:marker.start_ms,end_ms:Date.now(),error:'previous_process_interrupted'}});}
   if(interrupted.length)await flush();
   await until(BEGIN); if(stopping)return;
   let failures=0;
   for(let n=1;n<=Math.ceil((END-BEGIN)/60000)&&!stopping;n++){
     const target=BEGIN+(n-1)*60000;
     if(Date.now()>=Math.min(END,target+60000)||fs.existsSync(path.join(book,numbered(n)+'.json')))continue;
     await until(target);if(stopping||Date.now()>=END)break;
     // Never attribute a late request to an earlier cutoff. Time in each receipt is actual.
     if(Date.now()>=target+60000)continue;
     const started=Date.now();object(path.join(book,numbered(n)+'.json'),{slot:n,boot,start_ms:started,target_ms:target});await flush();
     let w;
     try{
       const cookie=await session();
       w=await timedObservation(cap,n,REPORT,cookie,flush);
       if(w.receipt.status===401){invalidate();throw new Error('session_expired');}
       const b=quality(w);ensure(b.release===cfg.a.release,'release_changed');
       failures=0;
     }catch(e){
       if(!fs.existsSync(path.join(cap,numbered(n)+'.receipt.json')))store(cap,n,{receipt:{url:ORIGIN+REPORT,start_ms:started,end_ms:Date.now(),error:safeCode(e)}});
       object(path.join(base,'capture-errors',numbered(n)+'.json'),{utc:stamp(),boot,slot:n,error:safeCode(e)});await flush();
       if(safeCode(e)==='release_changed'||/^login_status_(401|403|429)$/.test(safeCode(e))||++failures>=2)throw e;
     }
     log('capture',{boot,slot:n,status:w?.receipt.status||0,body_sha256:w?.receipt.body_sha256||null});
   }
   if(stopping)return;
   await until(END);if(stopping)return;
   const terminal={...bootMeta,finished:stamp(),state:'collection_window_closed',attempts:receipts(cap).length,acceptance_claimed:false};
   object(path.join(base,'terminal.json'),terminal);await flush();log('finished',terminal);await idle();
 }catch(e){
   const hold=path.join(base,'holds',cfg.generation+'.json');
   if(!fs.existsSync(hold)){object(hold,{...bootMeta,finished:stamp(),state:'held',error:safeCode(e),acceptance_claimed:false});await flush();}
   log('held',{boot,error:safeCode(e)});await idle();
 }finally{if(stopReview)await stopReview();await evidence.close();}
}
module.exports={admission,validatePreflight,quality,inspect,store,readStored,timedObservation,ordinary,ORIGIN,STATUS,REPORT,P9,LOGIN,BEGIN,FIRST,END,expectedAdmission};
if(require.main===module)main().catch(e=>{log('boot_error',{error:safeCode(e)});process.exitCode=1;});
