"""Private deterministic gzip transport. Start with tty=False; send one JSON line per message."""
from pathlib import Path
import sys,json,gzip,hashlib,io,os
if sys.stdin.isatty():
 import tty
 tty.setraw(sys.stdin.fileno())
R=Path(__file__).resolve().parent;C=json.loads((R/'contract.json').read_text());LIMIT=128*1024*1024
(R/'chunks').mkdir(exist_ok=True)
def diskbytes():return sum(p.stat().st_size for p in R.rglob('*')if p.is_file())
def immut(path,data):
 if path.exists():
  if path.read_bytes()!=data:raise ValueError('IMMUTABLE_FILE_CONFLICT:'+path.name)
  return 'ALREADY_SAVED'
 if diskbytes()+len(data)>LIMIT:raise ValueError('LOCAL_128MIB_BOUND')
 tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_bytes(data);os.replace(tmp,path);return'SAVED'
def joined_hash(items):return hashlib.sha256('\n'.join(items).encode()).hexdigest()
print(json.dumps({'ready':True,'private_data_echo':False}),flush=True)
for raw in sys.stdin:
 try:
  m=json.loads(raw);op=m['op']
  if op=='transport_probe':
   b=m['synthetic_text'].encode();assert len(b)<=1048576;buf=io.BytesIO()
   with gzip.GzipFile(filename='',fileobj=buf,mode='wb',mtime=0)as g:g.write(b)
   assert gzip.decompress(buf.getvalue())==b
   print(json.dumps({'op':op,'synthetic_only':True,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest(),'no_disk_write':True,'private_data_echo':False}),flush=True);continue
  if op=='quit':print(json.dumps({'closed':True}),flush=True);break
  if op=='manifest':
   e=m['export'];assert e['row_count']==126000 and e['date_count']==42
   b=json.dumps(e,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode();state=immut(R/'server_manifest.json',b)
   print(json.dumps({'op':op,'state':state,'file_sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)}),flush=True);continue
  if op!='chunk':raise ValueError('UNKNOWN_TRANSPORT_OPERATION')
  e=m['export'];i=int(e['chunk_index']);assert 0<=i<21
  assert e['dates']==C['dates'][i*2:i*2+2]and e['row_count']==6000 and len(e['records'])==6000
  hashes=[];keys=[]
  for q in e['records']:
   assert hashlib.sha256(q['consumer_text'].encode()).hexdigest()==q['consumer_sha256']
   assert hashlib.sha256(q['base_point_text'].encode()).hexdigest()==q['base_point_sha256']
   # Numeric cells are not parsed here; their exact canonical lexemes stay within strings.
   z=json.loads(q['consumer_text']);keys.append((z['session_date'],z['company_ordinal']));hashes.append(q['consumer_sha256'])
  assert keys==[(d,n)for d in e['dates']for n in range(1,3001)]
  assert joined_hash(hashes)==e['ordered_consumer_row_sha_manifest']
  buf=io.BytesIO()
  with gzip.GzipFile(filename='',fileobj=buf,mode='wb',mtime=0,compresslevel=6)as g:
   for q in e['records']:g.write((json.dumps(q,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode())
  b=buf.getvalue();path=R/'chunks'/f'chunk_{i:02d}.jsonl.gz';state=immut(path,b)
  receipt={'op':op,'index':i,'state':state,'rows':6000,'file_sha256':hashlib.sha256(b).hexdigest(),'gzip_bytes':len(b),'ordered_consumer_row_sha_manifest':e['ordered_consumer_row_sha_manifest'],'private_data_echo':False}
  immut(R/'chunks'/f'chunk_{i:02d}_receipt.json',(json.dumps(receipt,sort_keys=True)+'\n').encode())
  print(json.dumps(receipt),flush=True)
 except Exception as exc:
  print(json.dumps({'error':type(exc).__name__,'reason':str(exc),'private_data_echo':False}),flush=True)
