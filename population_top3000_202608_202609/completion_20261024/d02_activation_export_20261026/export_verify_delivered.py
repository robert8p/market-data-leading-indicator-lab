"""Standalone offline verifier: python verify_delivered.py path/to/delivery.zip"""
import sys,json,csv,zipfile,io,hashlib
from decimal import Decimal
from collections import defaultdict,Counter
from itertools import zip_longest
D=Decimal
def parse(s):return json.loads(s,parse_float=D)
def sha(b):return hashlib.sha256(b).hexdigest()
def pg_jsonb(v):
 if v is None:return'null'
 if v is True:return'true'
 if v is False:return'false'
 if isinstance(v,(D,int)):return format(v,'f')if isinstance(v,D)else str(v)
 if isinstance(v,str):return json.dumps(v,ensure_ascii=False)
 if isinstance(v,list):return'['+', '.join(pg_jsonb(x)for x in v)+']'
 return'{'+', '.join(pg_jsonb(k)+': '+pg_jsonb(v[k])for k in sorted(v,key=lambda k:(len(k.encode()),k.encode())))+'}'
def csv_cell(v):
 if v is None:return'\\N'
 if v is True:return'true'
 if v is False:return'false'
 return format(v,'f')if isinstance(v,D)else str(v)
class JoinedDigest:
 def __init__(self):self.h=hashlib.sha256();self.n=0
 def add(self,s):
  if self.n:self.h.update(b'\n')
  self.h.update(s.encode());self.n+=1
 def hexdigest(self):return self.h.hexdigest()
with zipfile.ZipFile(sys.argv[1])as z:
 assert z.testzip()is None
 c=parse(z.read('contract.json'));m=parse(z.read('independent_server_manifest.json'));r=parse(z.read('verification_receipt.json'))
 for name,want in r['members'].items():
  h=hashlib.sha256();size=0
  with z.open(name)as f:
   while b:=f.read(1024*1024):h.update(b);size+=len(b)
  assert h.hexdigest()==want['sha256']and size==want['bytes']
 cp={}
 for line in z.read('checkpoint_provenance.jsonl').decode().splitlines():
  item=parse(line);p=parse(item['checkpoint_canonical_text']);assert sha(item['checkpoint_payload_text'].encode())==item['checkpoint_sha256']==p['checkpoint_sha256'];cp[p['session_date']]=p
 assert sorted(cp)==c['dates']
 whole=JoinedDigest();perdate=defaultdict(JoinedDigest);base=defaultdict(JoinedDigest);counts=Counter();presence=Counter();n=0;companies={}
 with io.TextIOWrapper(z.open('consumer_canonical.jsonl'),encoding='utf-8')as jf,io.TextIOWrapper(z.open('consumer_typed.csv'),encoding='utf-8',newline='')as cf:
  reader=csv.DictReader(cf);assert reader.fieldnames==c['columns']
  for line,saved in zip_longest(jf,reader):
   assert line is not None and saved is not None;v=parse(line);assert set(v)==set(c['columns'])and all(saved[k]==csv_cell(v[k])for k in c['columns'])
   assert(v['session_date'],v['company_ordinal'])==(c['dates'][n//3000],n%3000+1)
   key=v['company_ordinal'];companies.setdefault(key,(v['company_id'],v['ticker']));assert companies[key]==(v['company_id'],v['ticker'])
   assert all(v[k]is False for k in c['false_flags'])and v['preliminary']is True and v['full_applicable_instrument_expected_units']is None
   assert v['overlay_manifest_sha256']==m['overlay_manifest_sha256']
   h=sha(line.rstrip('\n').encode());whole.add(h);perdate[v['session_date']].add(h);base[v['session_date']].add(pg_jsonb({k:v[k]for k in c['base_columns']}));counts[v['point_source_state']]+=1;presence[v['source_presence_state']]+=1;n+=1
 assert n==126000 and len(companies)==3000 and len({v[0]for v in companies.values()})==3000
 assert whole.hexdigest()==m['ordered_consumer_row_sha_manifest']
 for d in m['dates']:assert perdate[d['session_date']].hexdigest()==d['consumer_row_sha_manifest']and base[d['session_date']].hexdigest()==cp[d['session_date']]['value_manifest_sha256']
 assert dict(counts)==m['point_source_state_counts']and dict(presence)==m['source_presence_state_counts']
print(json.dumps({'result':'PASS','rows':n,'dates':42,'companies':3000,'csv_cells':n*len(c['columns']),'family_certified':False,'server_consumer_row_sha_manifest':whole.hexdigest()}))
