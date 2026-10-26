from pathlib import Path
from decimal import Decimal
import csv,json,hashlib,zipfile
O=Path('ctx07_jolts_component_export_20261026');P=Path('ctx07_jolts_component_delivery_design/ACTUAL_83604_SERVER_PROOF.json');report=json.loads(P.read_text(),parse_float=Decimal)['result']
def sha(b):return hashlib.sha256(b).hexdigest()
def plain(v):
 if isinstance(v,Decimal):return str(v)
 if v is None:return 'NULL'
 if isinstance(v,bool):return str(v).lower()
 return str(v)
assert report['source_count']==4 and report['evidence_count']==7 and report['asof_case_count']==14 and report['asof_errors']==0 and report['evidence_errors']==0 and report['scope_table_extra_evidence_count']==0
assert all(x['body_hash_ok']for x in report['source_bodies'])
records=report['canonical_rows'];texts=[x['canonical_record_text']for x in records]
assert len(set(x['observation_id']for x in records))==7
for x in records:assert sha(x['canonical_record_text'].encode())==x['record_sha256']
assert sha('\n'.join(texts).encode())==report['ordered_dataset_values_sha256']
(O/'jolts7_canonical_evidence.jsonl').write_text('\n'.join(texts)+'\n')
rows=[json.loads(t,parse_float=Decimal)for t in texts]
fields=['observation_id','factor_name','record_role','reference_period','retained_numeric','retained_units','publication_supported_available_at','source_id','source_sha256','evidence_sha256','extracted_numeric','numeric_lexeme','matched_byte_start','matched_byte_length','matched_bytes_sha256','strict_original_receipt_qualified','full_family_accepted','document_charset_qualified']
with(O/'jolts7_publication_reconstruction.csv').open('w',newline='')as f:
 w=csv.DictWriter(f,fieldnames=fields);w.writeheader()
 for e,pin in zip(rows,records):
  x=e['extraction'];out={k:e.get(k)for k in fields};out.update(evidence_sha256=pin['record_sha256'],extracted_numeric=x['value'],numeric_lexeme=x['lexeme'],matched_byte_start=x['matched_byte_start'],matched_byte_length=x['matched_byte_length'],matched_bytes_sha256=x['matched_bytes_sha256'],document_charset_qualified=x['document_charset_qualified']);w.writerow({k:plain(v)for k,v in out.items()})
# Sidecars retain exact JSON values/numbers from server proof using original JSON lexical representation.
original=json.loads(P.read_text())['result']
for name,key in [('four_source_manifest.json','source_bodies'),('asof14_consumer_matrix.json','asof_matrix')]:
 (O/name).write_text(json.dumps(original[key],ensure_ascii=False,indent=2)+'\n')
readme='''# JOLTS: seven scoped publication reconstruction observations\n\nThis separate component contains seven already independently reviewed numerical/publication-time reconstructions from four retained BLS sources, plus fourteen actual before/at-publication consumer checks. Units are millions of jobs. Original headline and later revision records remain distinct, including equal rounded values. May 2026 warmup is necessary earlier support; target releases are August–September 2026.\n\nThese are publication-supported retrospective reconstructions. They do not establish original historical receipt, strict point-in-time feed availability, a complete vintage denominator, document charset qualification, full macro-family acceptance, or completion of the wider mission. Those flags remain false. The original 79-observation and 21-dictionary-unit requirements remain intact; this export assesses only seven scoped observations.\n\n`jolts7_canonical_evidence.jsonl` contains exact PostgreSQL JSONB record text, ordered by observation_id, one UTF-8 record per line. Each line hash is in manifest.json. The ordered dataset hash joins seven records with newline separators and no trailing newline; the JSONL file hash includes its final newline. These are deliberately separate digest contracts.\n\nThe CSV uses explicit NULL for missing values and lowercase true/false booleans. Values retain Decimal semantics; no missing value is zero-filled. Original source bodies remain privately retained in the database and are not included. Their hashes, actual retrieval times, lengths and byte-range provenance are included. No provider requests or source downloads occurred for this export.\n\nProducer check: 26 October 2026 03:44:58 UTC. Assignment83604, claim83606. Four source hashes, seven evidence hashes and fourteen actual as-of cases passed. A separate independent final file review and actual saved-file receipt are required; this file build alone is not a delivery receipt.\n'''
(O/'README.md').write_text(readme)
with(O/'dictionary.csv').open('w',newline='')as f:
 w=csv.writer(f);w.writerow(['field','meaning']);
 for k in fields:w.writerow([k,{'retained_numeric':'Published headline/revision numeric value; millions of jobs.','publication_supported_available_at':'Official publication-header time; not original historical receipt.','evidence_sha256':'SHA256 of exact PostgreSQL JSONB record text.','source_sha256':'SHA256 of retained original body bytes.','matched_byte_start':'One-based original byte offset.','document_charset_qualified':'Always false for this byte-projection component.'}.get(k,'Exact scoped canonical evidence field; see README and canonical record.')])
manifest={'component':'CTX07_EXACT7_JOLTS_QUALIFIED_COMPONENT_ASOF_CANONICAL_DELIVERY_V1','assignment_event_id':83604,'claim_event_id':83606,'actual_consumer_observed_at':original['observed_at'],'observations':7,'sources':4,'asof_boundary_cases':14,'server_audit_sha256':json.loads(P.read_text())['audit_sha256'],'ordered_dataset_values_sha256':report['ordered_dataset_values_sha256'],'canonical_order':'observation_id_ASC','row_hashes':[{'observation_id':x['observation_id'],'sha256':x['record_sha256']}for x in records],'qualification_label':report['qualification_label'],'strict_original_receipt_qualified':False,'full_vintage_denominator_qualified':False,'document_charset_qualified':False,'family_accepted':False,'final_delivery':False,'new_provider_requests':0,'new_HTTP':0,'new_source_bytes':0,'files':{p.name:{'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())}for p in sorted(O.iterdir())if p.is_file()and p.suffix in('.jsonl','.csv','.json','.md')and p.name not in('manifest.json','verification.json')}}
(O/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
# Verify every CSV field against canonical JSON without floating-point conversion.
with(O/'jolts7_publication_reconstruction.csv').open(newline='')as f:csvrows=list(csv.DictReader(f))
assert len(csvrows)==7
for c,e,pin in zip(csvrows,rows,records):
 x=e['extraction'];expected={k:e.get(k)for k in fields};expected.update(evidence_sha256=pin['record_sha256'],extracted_numeric=x['value'],numeric_lexeme=x['lexeme'],matched_byte_start=x['matched_byte_start'],matched_byte_length=x['matched_byte_length'],matched_bytes_sha256=x['matched_bytes_sha256'],document_charset_qualified=x['document_charset_qualified']);assert c=={k:plain(v)for k,v in expected.items()}
assert all(m['canonical_match']for m in original['asof_matrix'])
verification={'status':'PRODUCER_FULL_CELL_HASH_MATRIX_PASS_PENDING_INDEPENDENT_FILE_REVIEW_AND_SAVE','canonical_records':7,'CSV_cells_checked':7*len(fields),'row_hash_errors':0,'CSV_cell_errors':0,'asof_cases':14,'asof_errors':0,'source_body_hash_errors':0,'ordered_dataset_hash_match':True,'flags_false_preserved':True,'source_calls':0,'actual_saved_file_receipt':False}
(O/'verification.json').write_text(json.dumps(verification,indent=2)+'\n')
zpath=O/'Top3000_Macro_JOLTS_Publication_Reconstruction_Component_20261026.zip'
files=[p for p in O.iterdir()if p.is_file()and p.suffix in('.jsonl','.csv','.json','.md')and not p.name.startswith('ACTUAL')]
with zipfile.ZipFile(zpath,'w',zipfile.ZIP_DEFLATED,compresslevel=9)as z:
 for p in sorted(files):z.write(p,p.name)
with zipfile.ZipFile(zpath)as z:assert z.testzip()is None
assert zpath.stat().st_size<=2097152
receipt={'ZIP_path':str(zpath.resolve()),'ZIP_bytes':zpath.stat().st_size,'ZIP_sha256':sha(zpath.read_bytes()),'manifest_sha256':sha((O/'manifest.json').read_bytes()),'canonical_JSONL_bytes':(O/'jolts7_canonical_evidence.jsonl').stat().st_size,'canonical_JSONL_file_sha256':sha((O/'jolts7_canonical_evidence.jsonl').read_bytes()),'ordered_dataset_values_sha256':report['ordered_dataset_values_sha256'],'producer_verification':verification,'actual_saved':False}
(O/'PRODUCER_BUILD_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
