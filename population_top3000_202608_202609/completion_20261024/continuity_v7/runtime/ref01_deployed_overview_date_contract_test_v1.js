// Extracted sourceUrl from deployed population-top3000-source-v1 indexSHA48c161842379569faddf3f560aa3eb5c714e3be86fe1eb815c56f96b7866439d.
// Synthetic offline contract test; no provider calls or source qualification.
const assert=require('node:assert/strict'); class Failure extends Error {constructor(code){super(code);this.code=code;}}
function sourceUrl(job) {
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


const dates=Array.from({length:37},(_,i)=>new Date(Date.UTC(2026,7,1+i)).toISOString().slice(0,10)).filter(d=>d!=='2026-08-03'); for(const d of dates){assert.throws(()=>sourceUrl({kind:'OVERVIEW',snapshot_date:d,ticker:'SYNTHETIC',params:{date:d}}),e=>e.code==='JOB_SCOPE_MISMATCH');}
assert.equal(sourceUrl({kind:'OVERVIEW',snapshot_date:'2026-08-03',ticker:'SYNTHETIC',params:{date:'2026-09-06'}}).toString(),'https://api.massive.com/v3/reference/tickers/SYNTHETIC?date=2026-08-03');
console.log(JSON.stringify({result:'PASS_RETAINED_DEPLOYED_SOURCEURL_DATE_GUARD_PROOF',non_anchor_dates_rejected:dates.length,job_params_cannot_override_date:true,provider_calls:0}));
