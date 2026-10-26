from pathlib import Path
import json,re,hashlib,csv,zipfile,sys
from decimal import Decimal
from datetime import datetime
sys.path.insert(0,str(Path('d02_overlay_export_20261026_compact').resolve()))
from compact_codec import pg_jsonb
P=Path('ctx07_june_only_export_84044');O=P/'deliverable';O.mkdir(exist_ok=True)
H=lambda b:hashlib.sha256(b).hexdigest()
r=json.loads((P/'ACTUAL_SERVER_RESULT_PRIVATE.json').read_text(),parse_float=Decimal)
w=json.loads(r['content'][0]['text'],parse_float=Decimal)
d=json.loads(re.search(r'\n(\[.*\])\n',w['result'],re.S)[1],parse_float=Decimal)[0]['payload']
t=d['canonical_text'];e=json.loads(t,parse_float=Decimal);sha='8bd15cd681e52d5273690cb346534a89c408fab30ef4d6c0129b87c84db7b0fe'
assert d['record_count']==1 and d['record_id']==d['canonical_sha256']==H(t.encode())==sha
assert H(pg_jsonb(e).encode())==sha and d['source_pin_exists'] is True
assert d['june_before']==[] and len(d['june_at'])==1 and d['june_at'][0]==e
assert e['retained_numeric']==Decimal('37.60') and e['extraction']['value_lexeme']=='37.60' and e['record_role']=='revision' and e['historical_received_at'] is None
flags=['strict_original_receipt_qualified','document_charset_qualified','full_vintage_denominator_qualified','full_family_accepted','final_delivery']
assert all(e[x] is False for x in flags)
j=d['july_check'];assert len(j)==1 and j[0]['value']==Decimal('37.62') and j[0]['role']=='headline'
assert datetime.fromisoformat(j[0]['reference_period'].replace('Z','+00:00'))==datetime.fromisoformat('2026-07-01T04:00:00+00:00')
fields=['record_id','source_id','factor_name','reference_period','publication_supported_available_at','retained_numeric','retained_units','record_role','revision_version','source_sha256','literal_proof_sha256','qualification','historical_received_at']+flags+['canonical_sha256','extraction_json']
assert len(fields)==20
row={f:e.get(f) for f in fields};row.update(record_id=d['record_id'],canonical_sha256=sha,extraction_json=pg_jsonb(e['extraction']))
def cell(v):
 if v is None:return 'NULL'
 if isinstance(v,bool):return str(v).lower()
 if isinstance(v,Decimal):return format(v,'f')
 return str(v)
(O/'canonical_records.jsonl').write_bytes(t.encode()+b'\n')
with (O/'june_revision.csv').open('w',newline='') as f:
 w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerow({k:cell(row[k]) for k in fields})
with (O/'june_revision.csv').open(newline='') as f:
 got=list(csv.DictReader(f));assert len(got)==1 and all(got[0][k]==cell(row[k]) for k in fields)
def write(name,obj):(O/name).write_text(pg_jsonb(obj)+'\n')
write('dictionary.json',{'columns':[{'name':f,'representation':('Decimal USD per hour; exact lexical scale retained' if f=='retained_numeric' else 'UTF8 canonical JSON object' if f=='extraction_json' else 'NULL literal for unavailable historical receipt' if f=='historical_received_at' else 'lowercase boolean; false is unqualified' if f in flags else 'UTF8 text'),'nullable':f=='historical_received_at'} for f in fields],'canonical_SHA':'SHA256 of exact PostgreSQL JSONB UTF8 text, without newline; separate from JSONL file hash','dataset_rows':1,'CSV_cells':20})
write('provenance.json',{'source_id':e['source_id'],'source_sha256':e['source_sha256'],'source_retrieved_at':e['source_retrieved_at'],'literal_proof_sha256':e['literal_proof_sha256'],'extraction':e['extraction'],'source_proof_basis':'REUSE actual bounded literal83771 and repairedPG50/independent post84040; no fresh whole-body rescan','current_stored_source_pin_exists':d['source_pin_exists'],'fresh_body_rehash':False,'historical_received_at':None,'old_held_evidence_unchanged':True,'all79_observations21_dictionary_units_preserved':True})
write('asof_verification.json',{'observed_at':d['observed_at'],'June_reference':'2026-06-01T04:00:00Z','publication':'2026-08-07T12:30:00Z','checks':[{'case':'June BEFORE','known_at':'2026-08-07T12:29:59.999999Z','expected_count':0,'actual_count':0},{'case':'June AT','known_at':'2026-08-07T12:30:00Z','expected_count':1,'actual_count':1,'expected_numeric':Decimal('37.60'),'actual_numeric':e['retained_numeric'],'record_id':sha,'record_role':'revision'},{'case':'July exact-period separation CHECK ONLY','reference_period':j[0]['reference_period'],'expected_numeric':Decimal('37.62'),'actual_numeric':j[0]['value'],'record_role':j[0]['role'],'included_in_dataset':False}]})
write('verification.json',{'assignment_event_id':84044,'claim_event_id':84060,'server_observed_at':d['observed_at'],'records':1,'columns':20,'CSV_cells':20,'canonical_sha256':sha,'canonical_SHA_matches':True,'producer_all_cells_match':True,'before_at_reference_partition_pass':True,'source_tests':'REUSE actualPG50 and literal proof; no new full-body audit','independent_file_review':'REQUIRED_BEFORE_SAVE','source_HTTP_provider_data_DDL_tasks_code_changes':0,'qualification':e['qualification'],'historical_received_at':None,**{k:False for k in flags}})
(O/'README.md').write_text('# June 2026 earnings revision component\n\nOne June observation: $37.60 per hour, marked preliminary in the retained table and published on 7 August 2026 at 12:30 UTC. The reference-period reader returns no June record before publication and this revision at publication. July $37.62 is checked separately and is not included in the dataset.\n\nQualification is numerical publication-time reconstruction only. Original historical receipt, source charset, complete vintages, strict point-in-time eligibility and full-family acceptance remain unqualified. historical_received_at is NULL. The source was retrieved in October; publication reconstruction does not establish original historical possession. All original 79 observations and 21 dictionary units remain obligations. The old three employment observations and seven JOLTS observations are not re-exported, and their files and original held evidence remain unchanged.\n\nCSV: one row, 20 columns. NULL is explicit missing receipt, false means unqualified, Decimal values retain exact scale, extraction_json contains canonical JSON. The JSONL retains exact server PostgreSQL JSONB text; the record hash excludes its newline and differs from the JSONL file hash. The manifest hashes the seven other members and excludes itself. As-of numerical values are JSON numbers, not strings.\n')
names=sorted(x.name for x in O.iterdir() if x.name!='manifest.json');assert len(names)==7
write('manifest.json',{'records':1,'CSV_cells':20,'canonical_sha256':sha,'qualification':e['qualification'],'family_accepted':False,'final_delivery':False,'files':{n:{'bytes':len((O/n).read_bytes()),'sha256':H((O/n).read_bytes())} for n in names}})
Z=P/'Top3000_Macro_June_Earnings_Revision_Publication_Reconstruction_Component_20261026.zip';assert not Z.exists()
with zipfile.ZipFile(Z,'x',compression=zipfile.ZIP_DEFLATED) as z:
 for f in sorted(O.iterdir()):z.write(f,f.name)
with zipfile.ZipFile(Z) as z:assert z.testzip() is None and len(z.namelist())==8
receipt={'path':str(Z.resolve()),'bytes':Z.stat().st_size,'sha256':H(Z.read_bytes()),'records':1,'CSV_cells':20,'actual_saved':False,'independent_approval_pending':True};(P/'PRODUCER_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
