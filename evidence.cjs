'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),z=require('node:zlib');
const {setTimeout:delay}=require('node:timers/promises');
const HASH=b=>crypto.createHash('sha256').update(b).digest('hex');
const ZERO='0'.repeat(64), MOUNT='/var/data/eq17';
const sync=p=>{const fd=fs.openSync(p,fs.constants.O_RDONLY);try{fs.fsyncSync(fd);}finally{fs.closeSync(fd);}};
function entries(dir,prefix=''){
 const out=[];
 for(const e of fs.readdirSync(dir,{withFileTypes:true})){
  const rel=prefix+e.name;
  if(!/^[A-Za-z0-9._/-]+$/.test(rel)||e.name.startsWith('.'))throw Error('unsafe_journal_member');
  if(e.isDirectory())out.push(...entries(path.join(dir,e.name),rel+'/'));
  else if(e.isFile())out.push(rel);else throw Error('nonregular_journal_member');
 }return out.sort();
}
function box(files,key){
 const bytes=z.gzipSync(Buffer.from(JSON.stringify({v:1,files})));
 const aes=crypto.randomBytes(32),iv=crypto.randomBytes(12),c=crypto.createCipheriv('aes-256-gcm',aes,iv);
 const ct=Buffer.concat([c.update(bytes),c.final()]);
 const ek=crypto.publicEncrypt({key,oaepHash:'sha256',padding:crypto.constants.RSA_PKCS1_OAEP_PADDING},aes);
 return Buffer.from(JSON.stringify({v:1,ek:ek.toString('base64'),iv:iv.toString('base64'),tag:c.getAuthTag().toString('base64'),ct:ct.toString('base64')}));
}
function phase(paths){
 if(paths.some(x=>x.includes('/holds/')))return 'held';
 if(paths.some(x=>x.endsWith('/terminal.json')))return 'close';
 if(paths.some(x=>x.includes('/recovery/')))return 'recovery';
 if(paths.some(x=>x.endsWith('/armed.json')))return 'armed';
 if(paths.some(x=>x.endsWith('/verified.json')))return 'preflight';
 if(paths.some(x=>x.includes('/capture/')||x.includes('/bookings/')))return 'mid';
 return 'boot';
}
async function emitLine(line,stream){
 if(!stream.write(line+'\n'))await new Promise((ok,fail)=>{const onerror=e=>{stream.off('drain',ondrain);fail(e);},ondrain=()=>{stream.off('error',onerror);ok();};stream.once('error',onerror);stream.once('drain',ondrain);});
}
async function journal(dir,root,key,stream=process.stdout){
 fs.mkdirSync(dir,{recursive:true,mode:0o700});fs.mkdirSync(root,{recursive:true,mode:0o700});
 const known=new Map(),groups=[];let tail=ZERO;
 for(const name of fs.readdirSync(root).filter(n=>/^g-\d{6}$/.test(n)).sort()){
  const d=path.join(root,name),m=JSON.parse(fs.readFileSync(path.join(d,'meta.json'))),sealed=fs.readFileSync(path.join(d,'box'));
  if(m.seq!==groups.length+1||m.prev!==tail||m.id!==HASH(sealed)||m.key!==keyFingerprint(key))throw Error('durable_chain_mismatch');
  for(const f of m.files){
   if(!/^[A-Za-z0-9_-][A-Za-z0-9._/-]*$/.test(f.path)||f.path.split('/').some(n=>!n||n==='.'||n==='..')||known.has(f.path))throw Error('durable_path_conflict');
   const p=path.join(dir,f.path),st=fs.lstatSync(p);
   if(!st.isFile()||st.size!==f.size||HASH(fs.readFileSync(p))!==f.sha256)throw Error('durable_content_mismatch');
   known.set(f.path,{...f,mtime:st.mtimeMs});
  }tail=m.id;groups.push({...m,d});
 }
 let chain=Promise.resolve(),errored=null;
 async function emit(m){
  const raw=fs.readFileSync(path.join(m.d,'box'));
  if(HASH(raw)!==m.id)throw Error('outbox_digest_mismatch');
  const b64=raw.toString('base64'),n=Math.ceil(b64.length/1500);
  for(let i=0;i<n;i++){await emitLine('EQ17BOX '+m.id+' '+(i+1)+'/'+n+' '+b64.slice(i*1500,(i+1)*1500),stream);if((i+1)%24===0)await delay(520);}
  await emitLine('EQ17COMMIT '+m.seq+' '+m.id+' '+m.prev,stream);
  await emitLine('EQ17HEAD '+m.phase+' '+m.seq+' '+m.id,stream);
  if(!fs.existsSync(path.join(m.d,'emitted'))){fs.writeFileSync(path.join(m.d,'emitted'),'local stdout accepted\n',{mode:0o600});sync(path.join(m.d,'emitted'));sync(m.d);}
 }
 const enqueue=m=>{chain=chain.then(()=>emit(m)).catch(()=>{errored=Error('ciphertext_export_failed');});};
 for(const m of groups)if(!fs.existsSync(path.join(m.d,'emitted')))enqueue(m);
 const re=process.env.RECORDER_REEXPORT_RANGE;
 if(re){
  const range=/^(\d+)-(\d+)$/.exec(re);if(!range||+range[1]<1||+range[2]<+range[1]||+range[2]-+range[1]>2000)throw Error('invalid_reexport_range');
  for(const m of groups)if(m.seq>=+range[1]&&m.seq<=+range[2])enqueue(m);
 }
 function flush(){
  if(errored)throw errored;
  const added=[];
  for(const p of entries(dir)){
   const fp=path.join(dir,p),st=fs.statSync(fp),previous=known.get(p);
   if(previous){
    if(st.size!==previous.size||(st.mtimeMs!==previous.mtime&&HASH(fs.readFileSync(fp))!==previous.sha256))throw Error('immutable_file_changed');
    continue;
   }
   if(st.size>12*1024*1024)throw Error('single_evidence_file_bound');
   const body=fs.readFileSync(fp);sync(fp);sync(path.dirname(fp));
   added.push({path:p,sha256:HASH(body),size:body.length,mtime:st.mtimeMs,body_b64:body.toString('base64')});
  }
  if(!added.length)return;
  const seq=groups.length+1, tmp=path.join(root,'.pending-'+crypto.randomUUID()),d=path.join(root,'g-'+String(seq).padStart(6,'0'));
  fs.mkdirSync(tmp,{mode:0o700});
  const sealed=box(added.map(({path,sha256,body_b64})=>({path,sha256,body_b64})),key);
  const id=HASH(sealed);let p=phase(added.map(f=>f.path));
  if(p==='close' && added.some(f=>f.path.endsWith('/terminal.json')&&JSON.parse(Buffer.from(f.body_b64,'base64')).state==='held'))p='held';
  const m={v:1,seq,prev:tail,id,key:keyFingerprint(key),phase:p,files:added.map(({path,sha256,size})=>({path,sha256,size}))};
  fs.writeFileSync(path.join(tmp,'box'),sealed,{mode:0o600});fs.writeFileSync(path.join(tmp,'meta.json'),JSON.stringify(m),{mode:0o600});
  sync(path.join(tmp,'box'));sync(path.join(tmp,'meta.json'));sync(tmp);fs.renameSync(tmp,d);sync(root);
  for(const f of added)known.set(f.path,{sha256:f.sha256,size:f.size,mtime:f.mtime});tail=id;
  const group={...m,d};groups.push(group);enqueue(group);
 }
 return {flush,close:async()=>{await chain;if(errored)throw errored;}};
}
function keyFingerprint(key){return HASH(key.export({type:'spki',format:'der'}));}
async function openEvidence(dir){
 if(dir!==MOUNT+'/evidence'||!process.env.RENDER_SERVICE_ID||fs.realpathSync(MOUNT)!==MOUNT)throw Error('required_render_evidence_mount');
 if(!fs.readFileSync('/proc/self/mountinfo','utf8').split('\n').some(line=>line.split(' ')[4]===MOUNT))throw Error('persistent_disk_not_mounted');
 const key=crypto.createPublicKey(Buffer.from(process.env.RECORDER_PUBLIC_KEY_B64||'','base64'));
 if(key.asymmetricKeyType!=='rsa'||key.asymmetricKeyDetails.modulusLength<3072||keyFingerprint(key)!==process.env.RECORDER_KEY_SHA256)throw Error('wrong_archive_recipient');
 // Entrypoint holds flock for the lifetime of this process. Refuse an unguarded entry.
 const lock=fs.statSync('/proc/self/fd/9'),expected=fs.statSync(MOUNT+'/.recorder.lock');
 if(lock.dev!==expected.dev||lock.ino!==expected.ino)throw Error('missing_disk_lease');
 return journal(dir,MOUNT+'/sealed',key);
}
module.exports={openEvidence,journal,keyFingerprint};
