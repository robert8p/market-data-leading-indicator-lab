from pathlib import Path
import json,re,hashlib,sys
R=Path(__file__).resolve().parent;b=json.loads(Path(sys.argv[1]).read_text())
for k in['assignment_details_sha256','view_definition_sha256','overlay_manifest_sha256']:assert re.fullmatch('[0-9a-f]{64}',b[k])
M=json.loads((R/'server_manifest.json').read_text());assert hashlib.sha256((R/'server_manifest.json').read_bytes()).hexdigest()=='4615cdaa4fb911ed071e05ba91350b65c53fe7b9ee8e2cdc21ce1d877c9da802'
assert M['view_sha256']==b['view_definition_sha256']and M['overlay_manifest_sha256']==b['overlay_manifest_sha256']and json.loads(M['completion_canonical_text'])['independent_review_event_id']==b['activation_review_event_id']
mp={'__ASSIGNMENT_ID__':str(b['assignment_event_id']),'__CLAIM_ID__':str(b['claim_event_id']),'__DETAILS_SHA256__':b['assignment_details_sha256'],'__VIEW_SHA256__':b['view_definition_sha256'],'__GRID_SHA256__':b['overlay_manifest_sha256'],'__ACTIVATION_REVIEW_ID__':str(b['activation_review_event_id'])}
o=R/'bound';o.mkdir(exist_ok=True);hashes={}
for i in range(21):
 s=(R/f'chunk_{i:02d}_UNBOUND.sql').read_text()
 for k,v in mp.items():s=s.replace(k,v)
 assert not re.search(r'__[A-Z0-9_]+__',s)
 p=o/f'chunk_{i:02d}.sql';assert not p.exists()or p.read_text()==s;p.write_text(s);hashes[p.name]=hashlib.sha256(s.encode()).hexdigest()
p=R/'binding.json';assert not p.exists()or json.loads(p.read_text())==b;p.write_text(json.dumps(b,indent=2)+'\n');(o/'sql_hashes.json').write_text(json.dumps(hashes,indent=2)+'\n');print(json.dumps({'prepared_only':True,'queries':21,'retained_manifest_sha256':hashlib.sha256((R/'server_manifest.json').read_bytes()).hexdigest()}))
