"""Build only from previously admitted, independently pinned consumer captures; never queries DB."""
from pathlib import Path
from decimal import Decimal,localcontext
from collections import Counter,defaultdict
import json,csv,gzip,hashlib,zipfile,io,os
from itertools import zip_longest
R=Path(__file__).resolve().parent;C=json.loads((R/'contract.json').read_text());M=json.loads((R/'server_manifest.json').read_text());D=Decimal
MAX_DISK=128*1024*1024;MAX_ZIP=50*1024*1024
FALSE=C['false_flags'];BASE=C['base_columns'];COLS=C['columns'];NUM=set(C['numeric_columns']);INT=set(C['integer_columns']);BINARY=set(C['hash_columns'])
def sha(b):return hashlib.sha256(b).hexdigest()
def parse(s):return json.loads(s,parse_float=D)
def pg_jsonb(v):
 if v is None:return'null'
 if v is True:return'true'
 if v is False:return'false'
 if isinstance(v,(D,int)):return format(v,'f')if isinstance(v,D)else str(v)
 if isinstance(v,str):return json.dumps(v,ensure_ascii=False)
 if isinstance(v,list):return'['+', '.join(pg_jsonb(x)for x in v)+']'
 if isinstance(v,dict):return'{'+', '.join(pg_jsonb(k)+': '+pg_jsonb(v[k])for k in sorted(v,key=lambda k:(len(k.encode()),k.encode())))+'}'
 raise TypeError(type(v).__name__)
def csv_cell(v):
 if v is None:return'\\N'
 if v is True:return'true'
 if v is False:return'false'
 if isinstance(v,D):return format(v,'f')
 assert v!='\\N','CSV_NULL_SENTINEL_COLLISION';return str(v)
def iter_records():
 for i in range(21):
  p=R/'chunks'/f'chunk_{i:02d}.jsonl.gz';rc=json.loads((R/'chunks'/f'chunk_{i:02d}_receipt.json').read_text());assert sha(p.read_bytes())==rc['file_sha256']
  n=0
  with gzip.open(p,'rt',encoding='utf-8')as f:
   for line in f:n+=1;yield json.loads(line)
  assert n==6000
class JoinedDigest:
 def __init__(self):self.h=hashlib.sha256();self.n=0
 def add(self,s):
  if self.n:self.h.update(b'\n')
  self.h.update(s.encode());self.n+=1
 def hexdigest(self):return self.h.hexdigest()
assert M['row_count']==126000 and M['date_count']==42
assert [x['session_date']for x in M['dates']]==C['dates']
completion=parse(M['completion_canonical_text']);assert completion['status']=='COMPLETE_INDEPENDENTLY_VERIFIED'and completion['expected_rows']==126000 and completion['expected_dates']==42
assert completion['cohort_sha256']==C['cohort_sha256']and completion['membership_acceptance_sha256']==C['membership_acceptance_sha256']
assert completion['verified_grid_manifest_sha256']==M['overlay_manifest_sha256']
checkpoints={};checkpoint_lines=[]
for item in M['checkpoints']:
 cp=parse(item['checkpoint_canonical_text']);payload=parse(item['checkpoint_payload_text']);assert sha(item['checkpoint_payload_text'].encode())==item['checkpoint_sha256']==cp['checkpoint_sha256']
 assert cp['session_date']==payload['date']and cp['value_manifest_sha256']==payload['value_manifest_sha256']and cp['rows']==3000
 checkpoints[cp['session_date']]=cp;checkpoint_lines.append(json.dumps(item,ensure_ascii=False,sort_keys=True,separators=(',',':')))
assert sorted(checkpoints)==C['dates']
whole=JoinedDigest();bydate=defaultdict(JoinedDigest);basebydate=defaultdict(JoinedDigest);counts=Counter();presence=Counter();datecounts=defaultdict(Counter);datepresence=defaultdict(Counter);mapping={};date_n=Counter();nullcells=Counter();n=0
for q in iter_records():
 text=q['consumer_text'];assert sha(text.encode())==q['consumer_sha256'];assert sha(q['base_point_text'].encode())==q['base_point_sha256']
 row=parse(text);assert set(row)==set(COLS)
 dt=row['session_date'];ordinal=row['company_ordinal'];assert(dt,ordinal)==(C['dates'][n//3000],n%3000+1)
 cid=row['company_id'];assert isinstance(cid,str)and cid
 if ordinal not in mapping:mapping[ordinal]=(cid,row['ticker'])
 assert mapping[ordinal]==(cid,row['ticker'])
 for flag in FALSE:assert row[flag]is False
 assert row['preliminary']is True and row['full_applicable_instrument_expected_units']is None
 assert row['source_basis']==C['source_basis']and row['overlay_manifest_sha256']==M['overlay_manifest_sha256']
 state=row['point_source_state'];nr=row['source_rows'];assert isinstance(nr,int)and nr>=0
 ps='ABSENT_FROM_RETAINED_SOURCE'if nr==0 else'PRESENT_SINGLE'if nr==1 else'DUPLICATE_SOURCE_TICKER';assert row['source_presence_state']==ps
 linked=state=='VERIFIED_POINT_AND_SOURCE_LINK';assert row['dated_anchor_point_and_source_value_link_verified']is linked
 for col in NUM:
  v=row[col];assert v is None or isinstance(v,(D,int))and not isinstance(v,bool)
 for col in INT:
  v=row[col];assert v is None or isinstance(v,int)and not isinstance(v,bool)
 for col in BINARY:
  v=row[col];assert v is None or isinstance(v,str)and len(v)==66 and v.startswith('\\x')and all(c in'0123456789abcdef'for c in v[2:])
 if linked:
  assert nr==1 and row['raw_close']is not None and row['raw_close']>0 and row['raw_volume']is not None and row['raw_volume']>=0
  assert row['source_row']is not None and row['source_row']>0 and row['identity_source_ordinal']is not None and row['identity_source_ordinal']>0
  assert all(row[k]is not None for k in BINARY)
 else:assert all(row[k]is None for k in['raw_close','raw_volume','provider_vwap'])
 with localcontext()as ctx:
  ctx.prec=250;derived=None if row['raw_volume']is None or row['provider_vwap']is None else D(row['raw_volume'])*D(row['provider_vwap'])
 assert row['source_vwap_times_volume']==derived
 base=pg_jsonb({k:row[k]for k in BASE});assert base==q['base_point_text'],'BASE_CANONICAL_PROJECTION_MISMATCH'
 whole.add(q['consumer_sha256']);bydate[dt].add(q['consumer_sha256']);basebydate[dt].add(base)
 counts[state]+=1;presence[ps]+=1;datecounts[dt][state]+=1;datepresence[dt][ps]+=1;date_n[dt]+=1
 for k,v in row.items():nullcells[k]+=v is None
 n+=1
assert n==126000 and len(mapping)==3000 and len({v[0]for v in mapping.values()})==3000
assert whole.hexdigest()==M['ordered_consumer_row_sha_manifest']
for d in M['dates']:
 dt=d['session_date'];assert date_n[dt]==3000 and bydate[dt].hexdigest()==d['consumer_row_sha_manifest']
 assert basebydate[dt].hexdigest()==checkpoints[dt]['value_manifest_sha256']
 assert dict(datecounts[dt])==checkpoints[dt]['state_counts']['point_source_state']and dict(datepresence[dt])==checkpoints[dt]['state_counts']['source_presence_state']
assert dict(counts)==M['point_source_state_counts']and dict(presence)==M['source_presence_state_counts']
# Private files are streamed into ZIP; plaintext 126000-row members are never written to disk.
readme='''D02 DATED ANCHOR / RETAINED DAILY SOURCE OVERLAY — WORKING EXPORT\n\nThis separately versioned package contains the complete frozen-anchor grid: 3,000 accepted companies x 42 target US trading sessions in August–September 2026 = 126,000 company/date rows, including every held row. It is NOT an all-applicable-instrument denominator. The full-applicable-instrument expected count remains NULL.\n\nVERIFIED_POINT_AND_SOURCE_LINK means the dated frozen-anchor point and retained provider source values are linked and mechanically checked; it does not certify independent economic accuracy, regular-session basis, corporate-action applicability, original historical receipt, strict point-in-time availability, full-period instrument lifecycle or family acceptance. All seven corresponding qualification flags remain FALSE. preliminary remains TRUE.\n\nIdentity hash representations are deliberately distinct. For 3 August, identity_record_sha256 hashes the canonical raw vendor-reference JSONB record; for later dates it hashes the normalized reference wrapper. point_record_sha256 hashes the canonical dated/anchor point wrapper. identity_source_sha256 identifies the actual single source page/body, not the legacy 27c818... multi-page source-set manifest. Each checkpoint source_proof.source_relation identifies acquisition_raw or source_document so the representation can be traced. These hashes must not be treated as interchangeable.\n\nHeld source values remain JSON null / CSV \\N, never zero-filled or silently dropped. Source-presence states distinguish absence from the retained source, one source occurrence and duplicate ticker occurrences; absence is not proof of no trading or economic nonexistence. source_vwap_times_volume is a mechanically derived product, not independently certified dollar turnover.\n\nconsumer_canonical.jsonl preserves exact PostgreSQL canonical consumer JSON records. CSV contains the same 30 typed fields. Bytea hashes use PostgreSQL \\x hexadecimal representation. Decimal numeric lexemes and NULLs were checked without binary floating-point conversion. Provenance contains all 42 immutable checkpoint payloads and hashes, source/calendar pins and source proofs. Verification compares a separate server whole/per-date consumer row-hash manifest and every projected base-point canonical record to each checkpoint value manifest.\n\nAn offline verifier is included: extract verify_delivered.py and run python verify_delivered.py against this ZIP. It rechecks all 126000 rows and 3780000 CSV cells without network or database access.\n\nThis archive does not replace or alter older working workbooks, ZIPs, frozen readers or releases. It contains selected consumer values and private lineage metadata, not original provider wire bodies. No new source calls or final-family certification were performed for this export.\n'''
descriptions={'company_id':'Frozen accepted company identifier; this grid is not all applicable instruments','ticker':'Frozen accepted anchor ticker, not proof of a full listing interval','raw_close':'Unadjusted retained grouped provider close, or NULL while held','raw_volume':'Retained grouped share volume; fractional provider values preserved','provider_vwap':'Retained provider grouped VWAP, nullable','source_vwap_times_volume':'Exact mechanical VWAP times share volume, not independent dollar-turnover accuracy','source_rows':'Actual retained raw-source ticker occurrence count','source_presence_state':'Retained-source occurrence classification; absence is not no-trade proof','point_source_state':'Dated point/source linkage or explicit held reason','source_row':'One-based occurrence ordinal in retained source body, nullable','identity_source_sha256':'Actual single identity-source page/body SHA256 as PostgreSQL bytea hex; this differs from the legacy 27c818... multi-page source-set manifest. checkpoint.source_proof.source_relation distinguishes acquisition_raw from source_document.','identity_source_ordinal':'One-based identity source occurrence ordinal, nullable','identity_record_sha256':'Date-dependent hash representation: Aug3 is canonical raw vendor-reference JSONB-record SHA256; later dates are normalized reference-wrapper SHA256. These are distinct representations, preserved without relabelling, nullable while held.','point_record_sha256':'SHA256 of canonical dated/anchor point wrapper, distinct from the raw vendor-reference or normalized reference-wrapper hash; nullable.','raw_record_sha256':'SHA256 of retained provider daily source record, nullable','overlay_manifest_sha256':'Activated 42-date grid manifest; distinct from consumer export row-hash digest','full_applicable_instrument_expected_units':'Unknown all-applicable-instrument denominator, preserved NULL'}
dictionary={'columns':[],'null_csv_sentinel':'\\N','qualification':C['qualifications'],'canonical_jsonb_recipe':'Object keys by UTF8 byte length then byte order; exact numeric decimal text. Recipe was tested byte-for-byte against every server base-point record.'}
for k in COLS:
 typ='numeric'if k in NUM else'integer_or_NULL'if k in INT else'boolean'if k in FALSE+['preliminary','dated_anchor_point_and_source_value_link_verified']else'bytea_hex_or_NULL'if k in BINARY else'text_or_NULL'
 dictionary['columns'].append({'name':k,'type':typ,'description':descriptions.get(k,'Exact activated working consumer field'),'qualification':('FALSE: independent substantive qualification remains unproven'if k in FALSE else'Dated point and retained source link only; not economic/family certification'if k=='dated_anchor_point_and_source_value_link_verified'else'Unknown all-applicable-instrument denominator, preserved NULL'if k=='full_applicable_instrument_expected_units'else'Preserved activated working consumer field')})
receipt={'row_count':n,'date_count':42,'company_count':3000,'cell_count':n*len(COLS),'all_consumer_rows_and_csv_cells_verified':True,'server_consumer_row_sha_manifest':whole.hexdigest(),'overlay_manifest_sha256':M['overlay_manifest_sha256'],'point_source_state_counts':dict(counts),'source_presence_state_counts':dict(presence),'null_cell_counts':dict(nullcells),'all_42_checkpoint_value_manifests_recomputed':True,'all_126000_base_canonical_projections_byte_exact':True,'source_calls':0,'http_calls':0,'provider_calls':0,'family_accepted':False,'old_files_unchanged':True,'qualifications':C['qualifications']}
# Reserve room for max50MiB ZIP before opening it.
existing=sum(p.stat().st_size for p in R.rglob('*')if p.is_file());assert existing+MAX_ZIP<=MAX_DISK,'INSUFFICIENT_128MIB_DISK_ROOM'
zpath=R/'Top3000_D02_Dated_Source_Overlay_Working_20261026.zip';assert not zpath.exists(),'FINAL_ZIP_ALREADY_EXISTS_IMMUTABLE';tmp=zpath.with_suffix('.zip.tmp');members={}
with zipfile.ZipFile(tmp,'w',zipfile.ZIP_DEFLATED,compresslevel=6)as z:
 h=hashlib.sha256();size=0
 with z.open('consumer_canonical.jsonl','w',force_zip64=True)as f:
  for q in iter_records():b=(q['consumer_text']+'\n').encode();f.write(b);h.update(b);size+=len(b)
 members['consumer_canonical.jsonl']={'bytes':size,'sha256':h.hexdigest()}
 h=hashlib.sha256();size=0;buf=io.StringIO(newline='');writer=csv.writer(buf,lineterminator='\n')
 with z.open('consumer_typed.csv','w',force_zip64=True)as f:
  for values in [COLS]:writer.writerow(values);b=buf.getvalue().encode();f.write(b);h.update(b);size+=len(b);buf.seek(0);buf.truncate(0)
  for q in iter_records():row=parse(q['consumer_text']);writer.writerow([csv_cell(row[k])for k in COLS]);b=buf.getvalue().encode();f.write(b);h.update(b);size+=len(b);buf.seek(0);buf.truncate(0)
 members['consumer_typed.csv']={'bytes':size,'sha256':h.hexdigest()}
 small={'checkpoint_provenance.jsonl':('\n'.join(checkpoint_lines)+'\n').encode(),'independent_server_manifest.json':(R/'server_manifest.json').read_bytes(),'dictionary.json':(json.dumps(dictionary,indent=2)+'\n').encode(),'README.txt':readme.encode(),'verify_delivered.py':(R/'verify_delivered.py').read_bytes(),'contract.json':(R/'contract.json').read_bytes()}
 for name,b in small.items():z.writestr(name,b);members[name]={'bytes':len(b),'sha256':sha(b)}
 receipt['members']=members;z.writestr('verification_receipt.json',(json.dumps(receipt,indent=2)+'\n').encode())
assert tmp.stat().st_size<MAX_ZIP,'FINAL_ZIP_50MIB_BOUND'
assert sum(p.stat().st_size for p in R.rglob('*')if p.is_file())<=MAX_DISK,'LOCAL_DISK_128MIB_BOUND'
# Reopen actual immutable byte artifact and audit every CSV cell against canonical JSONL.
with zipfile.ZipFile(tmp)as z:
 assert z.testzip()is None
 for name,want in members.items():
  h=hashlib.sha256();size=0
  with z.open(name)as f:
   while b:=f.read(1024*1024):h.update(b);size+=len(b)
  assert {'bytes':size,'sha256':h.hexdigest()}==want
 with io.TextIOWrapper(z.open('consumer_canonical.jsonl'),encoding='utf-8')as jf,io.TextIOWrapper(z.open('consumer_typed.csv'),encoding='utf-8',newline='')as cf:
  reader=csv.DictReader(cf);assert reader.fieldnames==COLS;nn=0
  for line,saved in zip_longest(jf,reader):
   assert line is not None and saved is not None;row=parse(line);assert all(saved[k]==csv_cell(row[k])for k in COLS);nn+=1
  assert nn==126000 and next(reader,None)is None and jf.readline()==''
os.replace(tmp,zpath);receipt['zip']={'path':str(zpath),'bytes':zpath.stat().st_size,'sha256':sha(zpath.read_bytes())};receipt['reopened_zip_all_cells_checked']=True;receipt['local_disk_bytes']=sum(p.stat().st_size for p in R.rglob('*')if p.is_file())
(R/'actual_export_receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({'row_count':n,'zip':receipt['zip'],'all_cells':n*len(COLS),'all_checks_passed':True}))
