from pathlib import Path
import json,re,hashlib,sys
R=Path(__file__).resolve().parent
b=json.loads(Path(sys.argv[1]).read_text());required=['assignment_event_id','claim_event_id','assignment_details_sha256','view_definition_sha256','overlay_manifest_sha256','activation_review_event_id'];assert set(required)<=set(b)
for k in ['assignment_details_sha256','view_definition_sha256','overlay_manifest_sha256']:assert re.fullmatch('[0-9a-f]{64}',b[k])
for k in ['assignment_event_id','claim_event_id','activation_review_event_id']:assert isinstance(b[k],int)and b[k]>0
mapping={'__ASSIGNMENT_ID__':str(b['assignment_event_id']),'__CLAIM_ID__':str(b['claim_event_id']),'__DETAILS_SHA256__':b['assignment_details_sha256'],'__VIEW_SHA256__':b['view_definition_sha256'],'__GRID_SHA256__':b['overlay_manifest_sha256'],'__ACTIVATION_REVIEW_ID__':str(b['activation_review_event_id'])}
def bind(s):
 for k,v in mapping.items():s=s.replace(k,v)
 assert not re.search('__[^_]+__',s),'UNBOUND_LITERAL';return s
out=R/'bound';out.mkdir(exist_ok=True);files={}
for source in [R/f'chunk_{i:02d}_UNBOUND.sql'for i in range(21)]:
 data=bind(source.read_text()).encode();p=out/source.name.replace('_UNBOUND','');assert not p.exists()or p.read_bytes()==data;p.write_bytes(data);files[p.name]=hashlib.sha256(data).hexdigest()
s=bind((R/'guard_TEMPLATE.sql').read_text()+(R/'manifest_TEMPLATE.sql').read_text());p=out/'manifest.sql';assert not p.exists()or p.read_text()==s;p.write_text(s);files[p.name]=hashlib.sha256(s.encode()).hexdigest()
p=R/'binding.json';assert not p.exists()or json.loads(p.read_text())==b;p.write_text(json.dumps(b,indent=2)+'\n');(out/'sql_file_hashes.json').write_text(json.dumps(files,indent=2)+'\n');print(json.dumps({'prepared_only':True,'bound_queries':22,'binding':b,'sql_hashes_sha256':hashlib.sha256((out/'sql_file_hashes.json').read_bytes()).hexdigest()}))
