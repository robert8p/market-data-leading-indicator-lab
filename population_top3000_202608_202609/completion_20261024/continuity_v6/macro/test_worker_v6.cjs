// Runs exact reviewed worker after Node's syntax-only TypeScript stripping; no network/DB.
const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const {stripTypeScriptTypes}=require('node:module');
const src=fs.readFileSync(__dirname+'/macro_source_worker_v6.ts','utf8').replace('import postgres from "npm:postgres@3.4.5";','');
const code=stripTypeScriptTypes(src,{mode:'strip'});
const aid='11111111-2222-3333-4444-555555555555',token='a'.repeat(64);
async function run(opts={}){
 let handler,calls=[],network=0,commit;
 const claim={state:'RESERVED',source_url:'https://www.bls.gov/news.release/archives/synthetic.htm',max_bytes:8388608,request_key:'synthetic',...opts.claim};
 const tx=async(strings,...args)=>{const sql=strings.join('?');if(sql.startsWith('set local'))return[];
 const kind=sql.includes('claim_v6')?'claim':sql.includes('commit_v6')?'commit':'failure';calls.push({kind,payload:args[2]});
 if(kind==='claim')return[{result:claim}];if(kind==='commit'){commit=args[2];if(opts.rejectCommit)throw Error('CONTROLLER_OWNERSHIP_CHANGED');return[{result:{state:'COMPLETE'}}];}
 return[{result:{state:args[2]?.http_status===null&&args[2]?.fetch_started?'UNCERTAIN':'BLOCKED'}}];};tx.json=x=>x;
 const db={begin:async(fn)=>fn(tx),end:async()=>{}};
 const ctx={Deno:{env:{get:k=>k==='SUPABASE_URL'?'https://oxzabweahkoimtevbbny.supabase.co':'postgres://synthetic'},serve:fn=>handler=fn},postgres:()=>db,Response,Request,URL,TextDecoder,Uint8Array,ReadableStream,AbortSignal,btoa,crypto:require('node:crypto').webcrypto,Date,JSON,Object,Number,Error,setTimeout,clearTimeout,
 fetch:async(url,options)=>{network++;assert.equal(options.redirect,'error');assert.ok(options.signal);if(opts.fetchError)throw Error('synthetic');return new Response(opts.body??new Uint8Array([0,1,2,0x7f,0x80,0xff]),{status:opts.status??200,headers:opts.headers??{'content-type':'application/octet-stream'}});}};
 vm.runInNewContext(code,ctx,{timeout:1000});const req=new Request('https://worker.invalid',{method:'POST',body:opts.stalledInput?new ReadableStream({start(){}}):JSON.stringify(opts.input??{attempt_id:aid,token}),duplex:'half'});const response=await handler(req);return{body:await response.json(),status:response.status,calls,network,commit};
}
(async()=>{
 let r=await run();assert.equal(r.status,200);assert.equal(r.network,1);assert.equal(Buffer.from(r.commit.body_base64,'base64').toString('hex'),'0001027f80ff');assert.equal(r.commit.raw_sha256,require('node:crypto').createHash('sha256').update(Buffer.from([0,1,2,0x7f,0x80,0xff])).digest('hex'));assert.equal(r.calls.map(x=>x.kind).join(','),'claim,commit');
 r=await run({claim:{state:'WAIT'}});assert.equal(r.network,0);assert.equal(r.calls.length,1);
 r=await run({status:403});assert.equal(r.network,1);assert.equal(r.calls.map(x=>x.kind).join(','),'claim,failure');assert.equal(r.calls[1].payload.http_status,403);assert.equal(r.commit,undefined);
 r=await run({fetchError:true});assert.equal(r.body.state,'UNCERTAIN');assert.equal(r.calls[1].payload.http_status,null);assert.equal(r.commit,undefined);
 r=await run({rejectCommit:true});assert.equal(r.status,503);assert.equal(r.body.receipt_recorded,false);assert.equal(r.calls.map(x=>x.kind).join(','),'claim,commit'); // uncertain post-fetch commit never repeated or released
 r=await run({body:new Uint8Array(8388609)});assert.equal(r.commit,undefined);assert.equal(r.calls.map(x=>x.kind).join(','),'claim,failure');
 r=await run({headers:{'content-length':'8388609'}});assert.equal(r.commit,undefined);assert.equal(r.calls.map(x=>x.kind).join(','),'claim,failure');
 r=await run({claim:{source_url:'https://unapproved.invalid/source'}});assert.equal(r.network,0);assert.equal(r.commit,undefined);
 r=await run({claim:{max_bytes:8388609}});assert.equal(r.network,0);
 r=await run({input:{attempt_id:aid,token,extra:true}});assert.equal(r.status,400);assert.equal(r.network,0);assert.equal(r.calls.length,0);
 const start=Date.now();r=await run({stalledInput:true});assert.equal(r.status,503);assert.equal(r.network,0);assert.ok(Date.now()-start<2500);
 console.log(JSON.stringify({result:'PASS_WORKER_V6',tests:11,external_requests:0,actual_worker_sha256:require('node:crypto').createHash('sha256').update(fs.readFileSync(__dirname+'/macro_source_worker_v6.ts')).digest('hex'),coverage:['exact binary bytes and digest','reservation wait','403 no retry','unknown transport outcome','post-fetch owner/stop rejection no retry','8MiB streamed cap','8MiB declared cap','official URL guard','admitted maximum','request shape','stalled input deadline']}));
})().catch(e=>{console.error(e);process.exitCode=1;});
