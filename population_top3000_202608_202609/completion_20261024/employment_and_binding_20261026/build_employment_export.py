from pathlib import Path
from decimal import Decimal
import argparse, csv, hashlib, json, zipfile

ap = argparse.ArgumentParser()
ap.add_argument('proof')
ap.add_argument('output')
args = ap.parse_args()
proof = json.loads(Path(args.proof).read_text(), parse_float=Decimal)
report = proof['result']
out = Path(args.output)
out.mkdir(parents=True, exist_ok=True)
def sha(b): return hashlib.sha256(b).hexdigest()
def scalar(v):
    if v is None: return 'NULL'
    if isinstance(v, bool): return str(v).lower()
    return str(v)
def sidecar_json(v):
    if isinstance(v, Decimal): return str(v)
    if isinstance(v, dict): return '{'+','.join(json.dumps(k,ensure_ascii=False)+':'+sidecar_json(x) for k,x in v.items())+'}'
    if isinstance(v, list): return '['+','.join(sidecar_json(x) for x in v)+']'
    return json.dumps(v,ensure_ascii=False)
assert report['source_count'] == 1 and report['evidence_count'] == 3
assert report['verified_factor_scope_extras'] == 0 and report['held_fourth_revision_count'] == 1
assert report['evidence_errors'] == report['asof_errors'] == 0
assert report['asof_case_count'] == 6
assert all(x['body_hash_ok'] for x in report['source_bodies'])
records = report['canonical_rows']
assert len({x['observation_id'] for x in records}) == 3
assert [x['observation_id'] for x in records] == sorted(x['observation_id'] for x in records)
texts = [x['canonical_record_text'] for x in records]
for r in records: assert sha(r['canonical_record_text'].encode()) == r['record_sha256']
assert sha('\n'.join(texts).encode()) == report['ordered_dataset_values_sha256']
rows = [json.loads(t, parse_float=Decimal) for t in texts]
(out/'employment3_canonical_evidence.jsonl').write_text('\n'.join(texts)+'\n')
fields = ['observation_id','factor_name','record_role','reference_period','retained_numeric','retained_units','publication_supported_available_at','source_id','source_sha256','evidence_sha256','extracted_numeric','numeric_lexeme','source_text_sha256','source_location_start','publication_header','reference_period_header','historical_received_at','strict_original_receipt_qualified','full_family_accepted','document_charset_qualified']
def csvrow(e, pin):
    x=e['extraction']; r={k:e.get(k) for k in fields}
    r.update(evidence_sha256=pin['record_sha256'],extracted_numeric=x['value'],numeric_lexeme=x['lexeme'],source_text_sha256=x['source_text_sha256'],source_location_start=x['source_location_start'],publication_header=x['publication_header'],reference_period_header=x['reference_period_header'],document_charset_qualified=False)
    return {k:scalar(v) for k,v in r.items()}
with (out/'employment3_publication_reconstruction.csv').open('w',newline='') as f:
    w=csv.DictWriter(f,fieldnames=fields); w.writeheader()
    for e,pin in zip(rows,records): w.writerow(csvrow(e,pin))
with (out/'employment3_publication_reconstruction.csv').open(newline='') as f:
    actual=list(csv.DictReader(f))
assert actual == [csvrow(e,pin) for e,pin in zip(rows,records)]
for name,key in [('source_manifest.json','source_bodies'),('asof6_consumer_matrix.json','asof_matrix')]:
    (out/name).write_text(sidecar_json(report[key])+'\n')
assert all(m['canonical_match'] for m in report['asof_matrix'])
for e in rows:
    assert e['strict_original_receipt_qualified'] is False and e['full_family_accepted'] is False
    assert e['historical_received_at'] is None
readme=f'''# July employment: three scoped publication reconstructions

This component contains three existing independently reviewed July 2026 headline observations from the BLS release published 7 August 2026 at 12:30 UTC: unemployment 4.1 percent, labor-force participation 61.4 percent, and average hourly earnings 37.62 USD/hour. It includes six actual before/at-publication consumer checks. It excludes the fourth July revision observation, whose extraction remains held. The original 79 observations and 21 dictionary units remain required; the prior seven-observation JOLTS component is a separate unchanged delivery.

These are publication-supported retrospective reconstructions. Original historical receipt, strict point-in-time feed availability, full vintage coverage, document charset qualification, full macro-family acceptance and final mission delivery remain unqualified. All corresponding flags are false; historical_received_at remains NULL. Source retrieval on 24 October is preserved separately from the publication-header time.

Canonical JSONL lines are exact PostgreSQL JSONB record text, ordered by observation_id. The ordered dataset digest joins records with newline separators and no final newline; the JSONL file includes a final newline, so its file digest differs. CSV values retain Decimal semantics, explicit NULL and lowercase boolean text. The source location is a one-based character position in HTML-stripped, whitespace-normalized text, NOT a raw byte offset. Source-text SHA hashes that normalized text; source_sha256 hashes the retained original body bytes. Original source bodies are retained privately and are not included.

Observed server check: {report['observed_at']}. Assignment {report['assignment_event_id']}, claim {report['claim_event_id']}. This producer build requires independent whole-file review and an actual saved-file receipt before it is called delivered.
'''
(out/'README.md').write_text(readme)
meanings={'source_location_start':'One-based normalized HTML-stripped, whitespace-collapsed text character position; not raw byte offset.','source_text_sha256':'SHA256 of normalized source text used by the extraction rule.','source_sha256':'SHA256 of retained original source-body bytes.','publication_supported_available_at':'Official publication-header time; not original historical receipt.','evidence_sha256':'SHA256 of exact PostgreSQL JSONB evidence text.','document_charset_qualified':'False: charset qualification is not certified by this component.'}
with (out/'dictionary.csv').open('w',newline='') as f:
    w=csv.writer(f);w.writerow(['field','meaning'])
    for k in fields:w.writerow([k,meanings.get(k,'Exact scoped canonical evidence field; see README and canonical JSONL.')])
verification={'status':'PRODUCER_PASS_PENDING_INDEPENDENT_REVIEW_AND_ACTUAL_SAVE','canonical_rows':3,'CSV_cells_checked':3*len(fields),'source_body_hash_errors':0,'evidence_hash_errors':0,'CSV_cell_errors':0,'asof_cases':6,'asof_errors':0,'held_fourth_revision_preserved':True,'strict_original_receipt_qualified':False,'full_vintage_denominator_qualified':False,'document_charset_qualified':False,'family_accepted':False,'final_delivery':False,'actual_saved_file_receipt':False}
(out/'verification.json').write_text(json.dumps(verification,indent=2)+'\n')
members=[p for p in sorted(out.iterdir()) if p.is_file() and p.suffix in ('.json','.csv','.jsonl','.md') and p.name not in ('manifest.json','PRODUCER_BUILD_RECEIPT.json','independent_ACTUAL_EMPLOYMENT3_FILE_ASOF_review.json')]
manifest={'component':'CTX07_EXACT3_JULY_EMPLOYMENT_QUALIFIED_COMPONENT_ASOF_CANONICAL_DELIVERY_V1','assignment_event_id':report['assignment_event_id'],'claim_event_id':report['claim_event_id'],'observed_at':report['observed_at'],'canonical_rows':3,'CSV_cells':60,'sources':1,'asof_cases':6,'server_audit_sha256':proof['audit_sha256'],'ordered_dataset_values_sha256':report['ordered_dataset_values_sha256'],'canonical_order':'observation_id_ASC','row_hashes':[{'observation_id':r['observation_id'],'sha256':r['record_sha256']}for r in records],'qualification_label':report['qualification_label'],'family_accepted':False,'final_delivery':False,'strict_original_receipt_qualified':False,'files':{p.name:{'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())}for p in members}}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
members.append(out/'manifest.json')
zpath=out/'Top3000_Macro_July_Employment_Publication_Reconstruction_Component_20261026.zip'
with zipfile.ZipFile(zpath,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as z:
    for p in sorted(members):z.write(p,p.name)
with zipfile.ZipFile(zpath) as z: assert z.testzip() is None and len(z.namelist())==8
assert zpath.stat().st_size < 8388608
receipt={'ZIP_path':str(zpath.resolve()),'ZIP_bytes':zpath.stat().st_size,'ZIP_sha256':sha(zpath.read_bytes()),'manifest_sha256':sha((out/'manifest.json').read_bytes()),'canonical_JSONL_file_sha256':sha((out/'employment3_canonical_evidence.jsonl').read_bytes()),'producer_verification':verification,'actual_saved':False}
(out/'PRODUCER_BUILD_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
