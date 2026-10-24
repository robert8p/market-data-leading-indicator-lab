import json,pathlib,sys
p=pathlib.Path(__file__).parent
claim,run=sys.argv[1:3]
h=(p/'integration_header_v1.sql').read_text().replace('REPLACE_CLAIM_EVENT_ID',claim).replace('REPLACE_RUN_ID',run)
files=['f05_retained_candidate_v1.sql','dependency_wake_v1.sql','continuation_dispatch_v6.sql','continuation_claim_v6.sql','register_successors_v1.sql','register_runtime_v1.sql']
body=';\n'.join((p/f).read_text() for f in files)
(p/'actual_rollback.sql').write_text(h+'\n'+body+'\n'+(p/'rollback_body_v1.sql').read_text())
(p/'integration_prefix.sql').write_text(h+'\n'+body+'\n')
print(json.dumps({'rollback':str(p/'actual_rollback.sql'),'bytes':len((h+body).encode())}))
