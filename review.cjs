'use strict';
const fs=require('node:fs'),path=require('node:path');
// Fixed read-only appointments. No data here can enter the genuine capture directory.
const SLOTS=[
 {name:'mid',start:Date.parse('2026-11-10T15:20:05Z'),end:Date.parse('2026-11-10T15:21:55Z')},
 {name:'close',start:Date.parse('2026-11-10T21:02:00Z'),end:Date.parse('2026-11-10T21:04:55Z')}
];
function room(base) {
 const s=fs.statfsSync(base);
 if(s.bavail*s.bsize<64*1024*1024)throw Error('review_disk_reserve');
}
async function runSlot(slot,ctx) {
 const {base,boot,release,now,write,flush,wire,session,invalidate,reading,stopping,log}=ctx;
 const d=path.join(base,'reviews',slot.name);
 const exists=f=>fs.existsSync(path.join(d,f));
 const save=async (f,v)=>{write(path.join(d,f),v);await flush();};
 if(exists('complete.json')||exists('failure.json'))return;
 if(now()<slot.start)return;
 const meta={name:slot.name,boot,release,requested_window:[slot.start,slot.end],purpose:'INDEPENDENT_RETROSPECTIVE_REVIEW'};
 if(exists('booked.json')){
   await save('failure.json',{...meta,error:'previous_review_interrupted',end_ms:now()});return;
 }
 if(now()>=slot.end){
   await save('failure.json',{...meta,error:'review_window_missed',end_ms:now()});return;
 }
 // Fail closed before issuing requests if the immutable booking cannot be exported.
 await save('booked.json',{...meta,start_ms:now()});
 try {
   room(base);
   const routeNames=slot.name==='mid'?['status','positive9']:['report','status','positive9'];
   let attempt=1;
   while(attempt<=2) {
     const label='attempt-'+attempt,folder=path.join(d,label);
     const errors=[];let expired=false;
     if(stopping()||now()>=slot.end)throw Error('review_window_closed');
     await save(label+'/start.json',{...meta,attempt,start_ms:now()});
     const cookie=await session();
     for(const [i,name] of routeNames.entries()) {
       if(stopping()||now()>=slot.end)throw Error('review_window_closed');
       room(base);
       const observation=await wire(folder,i+1,name,cookie,Math.min(slot.end-now(),name==='positive9'?20000:30000));
       if(observation.receipt.status===401){invalidate();expired=true;errors.push('http_401');break;}
       if(observation.receipt.status===403||observation.receipt.status===429)throw Error('review_access_hold');
       try{reading(observation);}catch(e){errors.push(ctx.safe(e));}
     }
     if(expired||errors.length) {
       await save(label+'/failure.json',{...meta,attempt,errors,end_ms:now()});
       if(expired&&attempt===1&&now()+55000<slot.end){attempt++;continue;}
       throw Error('incomplete_review_receipts');
     }
     if(now()>slot.end)throw Error('review_window_closed');
     await save('complete.json',{...meta,attempt,routes:routeNames,ended_ms:now()});
     log('review_readback',{kind:slot.name,attempt,boot});return;
   }
 }catch(e){
   await save('failure.json',{...meta,error:ctx.safe(e),end_ms:now()});
   log('review_failed',{kind:slot.name,error:ctx.safe(e),boot});
 }
}
function start(ctx){
 let halted=false,active=Promise.resolve();
 const timers=SLOTS.map(slot=>setTimeout(()=>{
   if(halted||ctx.stopping())return;
   active=active.then(()=>runSlot(slot,{...ctx,stopping:()=>halted||ctx.stopping()})).catch(e=>{
     // Log a bounded diagnostic even when the private disk itself is unavailable.
     ctx.log('review_export_failed',{kind:slot.name,error:ctx.safe(e),boot:ctx.boot});
   });
 },Math.max(0,slot.start-ctx.now())));
 return async ()=>{halted=true;for(const t of timers)clearTimeout(t);await active;};
}
module.exports={SLOTS,runSlot,start};
