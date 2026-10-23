"""Full immutable publication/revalidation protocol using isolated replay bodies.

Never a scanner or prospective-live proof. Original RECOVERY_OR_REPLAY bodies
remain failed for live admission. No production credentials or database writes.
"""
import argparse,hashlib,json,pathlib,sqlite3,sys,tempfile,time,statistics,shutil

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--artifact',required=True,type=pathlib.Path)
parser.add_argument('--release',required=True)
parser.add_argument('--evidence',required=True,type=pathlib.Path)
parser.add_argument('--minutes',required=True,type=pathlib.Path)
parser.add_argument('--output',required=True,type=pathlib.Path)
args=parser.parse_args()
import os,socket
assert not any(os.environ.get(k) for k in ('EQ17_DATABASE_URL','DATABASE_URL','ALPACA_API_KEY','ALPACA_SECRET_KEY','MASSIVE_API_KEY'))
raw=args.artifact.read_bytes();assert hashlib.sha256(raw).hexdigest()==args.release
source_sha=hashlib.sha256(args.evidence.read_bytes()).hexdigest()
work=tempfile.TemporaryDirectory(prefix='eq17-publication-test-');base=pathlib.Path(work.name)
root=base/'release';root.mkdir()
for name,body in json.loads(raw)['files'].items():
    path=root/name;assert not pathlib.Path(name).is_absolute() and '..' not in pathlib.Path(name).parts
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(body)
sys.path[:0]=[str(root/'src')]
def no_network(*args,**kwargs):raise OSError('ISOLATED_PUBLICATION_EXTERNAL_NETWORK_DENIED')
socket.socket.connect=no_network
from store import Store,digest
import activation as a
from eq17_endurance_metrics import assess

minutes=[json.loads(line) for line in args.minutes.read_text().splitlines()]
assert len(minutes)==359 and [r['index'] for r in minutes]==list(range(359))
source=sqlite3.connect('file:'+str(args.evidence.resolve())+'?mode=ro',uri=True)
rows=source.execute("SELECT event_time,payload FROM evidence WHERE scope='test_capacity' AND kind='capacity_activation_capacity_minute' ORDER BY event_time").fetchall()
source.close();assert len(rows)==359 and len({r[0] for r in rows})==359
args.output.mkdir(parents=True,exist_ok=False)
dbpath=base/'protocol.sqlite';db=Store(str(dbpath));owner='isolated-publication-protocol'
db.put_state('live:paused',True);db.lease(owner,time.time())
results=[];all_ids=[];cutoffs=[];wall_started=time.time();deadline=time.monotonic()+1200
restart_preserved=False
for index,(cutoff,payload) in enumerate(rows):
    if time.monotonic()>deadline:raise TimeoutError('PUBLICATION_PROTOCOL_1200_SECOND_BOUND')
    original=json.loads(payload)
    assert original['origin']=='RECOVERY_OR_REPLAY' and original.get('live_session_proven') is False
    assert len(original['expected_securities'])==6059 and len(original['counts'])==90
    assert cutoff==minutes[index]['trigger']
    b=original['bindings'];session=original['session'];cutoffs.append(cutoff)
    db.lease(owner,time.time());started=time.monotonic()
    if index==180:
        before=a.fingerprints(db,all_ids);db.close();db=Store(str(dbpath))
        assert a.fingerprints(db,all_ids)==before
        db.lease(owner,time.time());restart_preserved=True
    body=dict(original)
    with db.atomic():
        body['fence']=a.fence(db,owner,time.time());body['owner']=owner
        eid=a.append(db,'minute',digest([b,session['date'],int(cutoff)]),body,time.time())
    with db.atomic():
        a.fence(db,owner,time.time())
        fid=a.append(db,'minute_finish',eid,dict(minute=eid,finished=time.time()),time.time())
        vid=a.seal_minute(db,eid,b,session,cutoff,time.time())
    with db.atomic():
        a.fence(db,owner,time.time());cid=a.complete_minute(db,eid,time.time())
    published=time.monotonic()
    sealed=a.read(db,vid,'minute_verdict',time.time())
    assert 'NOT_LIVE' in sealed['errors'],'Replay must not become live admission'
    all_ids.extend([eid,fid,vid,cid])
    # Existing runtime reassesses every30s: two complete accumulated verifications
    # per one-minute arrival, including raw fingerprints and immutable receipts.
    for _ in range(2):
        verdicts=a.minute_verdicts(db,b,session,cutoffs,time.time())
        assert len(verdicts)==index+1
        assert all(v['errors'] for _,v in verdicts),'No replay minute may pass live admission'
    verified=time.monotonic()
    fingerprint=a.fingerprints(db,[eid])[eid]
    with db.atomic():assert a.append(db,'minute',digest([b,session['date'],int(cutoff)]),body,time.time())==eid
    assert a.fingerprints(db,[eid])[eid]==fingerprint
    run=dict(index=index,cutoff=cutoff,publication_seconds=published-started,
        two_accumulated_verifications_seconds=verified-published,total_seconds=verified-started,
        original_live_errors=sealed['errors'],scope='ISOLATED_REPLAY_PROTOCOL')
    results.append(run)
    with (args.output/'minutes.jsonl').open('a') as out:out.write(json.dumps(run)+'\n')
    if index%60==0:print(json.dumps(run),flush=True)

before=a.fingerprints(db,all_ids)
class ExpectedRollback(Exception):pass
tamper_detected=False
try:
    with db.atomic():
        db.execute('UPDATE evidence SET payload=? WHERE id=?',('{}',all_ids[0]))
        invalid=a.minute_verdicts(db,b,session,[cutoffs[0]],time.time())[0][1]
        tamper_detected='MISSING_OR_CHANGED_FIRST_EVALUATION' in invalid['errors']
        assert tamper_detected
        raise ExpectedRollback()
except ExpectedRollback:pass
assert a.fingerprints(db,all_ids)==before
assert db.execute("SELECT count(*) FROM evidence WHERE scope=?",(a.SCOPE,)).fetchone()[0]==359*4
combined=[dict(original,total_seconds=original['total_seconds']+extra['total_seconds']) for original,extra in zip(minutes,results)]
assessment=assess(combined)
proof=dict(release=args.release,classification='ISOLATED_FULL_PUBLICATION_PROTOCOL_NOT_LIVE',
    source_evidence_sha256=source_sha,started=wall_started,finished=time.time(),expected=359,observed=len(results),
    exact_activation_code_sha256=hashlib.sha256((root/'src/activation.py').read_bytes()).hexdigest(),
    full_protocol_records=359*4,duplicate_records_added=0,restart_preserved=restart_preserved,
    tamper_detected=bool(tamper_detected),original_replay_failures_retained=True,
    mean_added_seconds=statistics.mean(r['total_seconds'] for r in results),
    maximum_added_seconds=max(r['total_seconds'] for r in results),
    combined_conservative_throughput=assessment,passed=bool(restart_preserved and tamper_detected and len(results)==359),
    prospective_live_minutes=0,production_database_latency_measured=False,browser_delivery_measured=False,
    limits='Adds full immutable publication plus two whole accumulated receipt/fingerprint verifications per minute to the already measured runtime/capacity workload. This conservatively duplicates some publication work. Uses isolated SQLite; cannot establish PostgreSQL/network, natural sources, prospective clocks or browser delivery.')
(args.output/'result.json').write_text(json.dumps(proof,indent=2));db.close()
shutil.copyfile(dbpath,args.output/'isolated-protocol.sqlite');work.cleanup()
print(json.dumps(proof),flush=True)
raise SystemExit(0 if proof['passed'] and assessment['passed'] else 2)
