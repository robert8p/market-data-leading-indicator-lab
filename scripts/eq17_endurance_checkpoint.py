"""Read-only SQLite snapshot of this isolated endurance test, never production.

The immutable evidence snapshot selects the completed prefix. Bars are copied
afterwards so every completed minute's history exists. Any subsequently fetched
bars keep their original timestamps; the runtime's cutoff filtering still applies.
"""
import argparse,datetime,hashlib,json,pathlib,sqlite3,time

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--cache',required=True,type=pathlib.Path)
p.add_argument('--minutes',required=True,type=pathlib.Path)
p.add_argument('--artifact',required=True,type=pathlib.Path)
p.add_argument('--release',required=True)
p.add_argument('--output',required=True,type=pathlib.Path)
a=p.parse_args()
assert a.cache.resolve().parent.name.startswith('eq17-full-session-')
assert hashlib.sha256(a.artifact.read_bytes()).hexdigest()==a.release
a.output.mkdir(parents=True,exist_ok=False)
deadline=time.monotonic()+120
def digest(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for block in iter(lambda:f.read(1048576),b''):h.update(block)
 return h.hexdigest()
def copy(name):
 source=sqlite3.connect('file:'+str((a.cache/name).resolve())+'?mode=ro',uri=True)
 target=sqlite3.connect(a.output/name)
 def progress(status,remaining,total):
  if time.monotonic()>deadline:raise TimeoutError('CHECKPOINT_120_SECOND_BOUND')
 source.backup(target,pages=4096,progress=progress,sleep=.01)
 source.close();target.close()
copy('evidence.sqlite')
db=sqlite3.connect(a.output/'evidence.sqlite')
assert all(scope in ('test_capacity','capacity_fixture','test') for scope, in db.execute('SELECT DISTINCT scope FROM evidence'))
counts=db.execute("SELECT kind,count(*) FROM evidence WHERE scope='test_capacity' AND kind LIKE 'capacity_activation_capacity_%' GROUP BY kind").fetchall()
assert len(counts)==4 and len({n for _,n in counts})==1,'Snapshot caught an unfinished publication; retain attempt and retry later'
n=counts[0][1];assert 0<n<359
cutoffs=[row[0] for row in db.execute("SELECT event_time FROM evidence WHERE scope='test_capacity' AND kind='capacity_activation_capacity_minute' ORDER BY event_time")]
db.close()
raw=a.minutes.read_bytes();lines=raw[:raw.rfind(b'\n')+1].splitlines();rows=[json.loads(x) for x in lines[:n]]
assert len(rows)==n and [x['index'] for x in rows]==list(range(n)) and [x['trigger'] for x in rows]==cutoffs
(a.output/'minutes.jsonl').write_bytes(b'\n'.join(lines[:n])+b'\n')
copy('bars.sqlite')
proof=dict(classification='ISOLATED_ENDURANCE_CHECKPOINT',release=a.release,observed=n,
 checked_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_log_modified_epoch=a.minutes.stat().st_mtime,complete_log_matches_snapshot=(len(lines)==n),sha256={name:digest(a.output/name) for name in ('evidence.sqlite','bars.sqlite','minutes.jsonl')},
 source_modified=False,original_information_clocks_preserved=True,live_session_proven=False,
 limitations='Consistent completed evidence prefix, followed by a later bar-cache snapshot. Future-dated bars retain their original receipt/start and must remain excluded by runtime cutoffs. Any work after this prefix remains in the original attempt and is not silently deleted.')
(a.output/'checkpoint.json').write_text(json.dumps(proof,indent=2));print(json.dumps(proof),flush=True)
