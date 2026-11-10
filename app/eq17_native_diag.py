"""One bounded diagnostic on an existing private input; no service boot, publication, or acceptance."""
import ctypes
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import resource
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from app.eq17_native_archive import OLD_OPERATION, combined, read_piece, check_pin as pin_record
from app.eq17_native_receipts import slot_record, started_record, terminal_record

SERVICE='srv-d9jm320u01pc73fhincg'
PREFIX='engineering:eq17-session-recovery-601e1acecf3a-20261110:'
RELEASE='601e1acecf3af6c27b8e6bbec37baeb3f6d694a7f496e5e0c762bf6ce4bc3736'
FORWARD='51d5e5adff5809ccb76c74c5c75fb9c9acbc0bba'
BUILDER='b905c8d4dda768cbbfeffb92b23742dc789c687599a2ed4408b7614c4ca30963'
BASE='1214cbba624020d9ab7f8c9122e577f75944c42cd5d0db8bc78901e949d32012'
HOSTED_LOG='af0bd243e321784e02fad4c7bf147cfcc4bd26ed9c1438b72e1c5aeca651af3f'
ORIGINAL_RECEIPT='67c542cd96c251b112688a38b9c15b3772d95faae15905c615fc9f7bd6ecee51'
PREVIOUS_DIAGNOSTIC='1f2a1b113a5284c9098dc118a07ede853cdbb081'
PREVIOUS_TERMINAL='ea699a7e1bcd7c035264300b585e7bde3619938a0809af5cc4f742dfb488a6e0'
CHUNK=98304
MAX_PACKET=24*1024*1024
MAX_EXTRACTED=128*1024*1024
LIMIT_SECONDS=1799
ROOT=Path(__file__).resolve().parents[1]
class DiagnosticFailure(ValueError):
    pass
def check(ok,code):
    if not ok:raise DiagnosticFailure(code)
def failure_code(exc):
    if isinstance(exc,DiagnosticFailure):return str(exc)
    if type(exc) is ValueError and str(exc) in ('RESULT_SCHEMA','TRANSPORT_SCHEMA','ARCHIVE_PIN','DUPLICATE_JSON','BAD_JSON_NUMBER'):
        return str(exc)
    return 'NATIVE_DIAGNOSTIC_ERROR'
def h(raw):return hashlib.sha256(raw).hexdigest()
def canonical(v):return json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)
def hexsha(x,n=64):return isinstance(x,str) and re.fullmatch('[a-f0-9]{'+str(n)+'}',x) is not None
def load_json(data):
    def pairs(p):
        v={}
        for k,val in p:
            check(k not in v,'DUPLICATE_JSON');v[k]=val
        return v
    def bad(v):raise ValueError('BAD_JSON_NUMBER')
    return json.loads(data,object_pairs_hook=pairs,parse_constant=bad)
def parent_guard():
    libc=ctypes.CDLL(None,use_errno=True)
    # PR_SET_DUMPABLE controls same-UID ptrace and access to sensitive /proc/PID files.
    check(libc.prctl(4,0,0,0,0)==0 and libc.prctl(3,0,0,0,0)==0,'PARENT_GUARD')
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    check(os.geteuid()!=0 and sys.version_info[:2]==(3,12),'RUNTIME_IDENTITY')
def key_for(pin,commit):
    check(hexsha(commit,40) and commit!=FORWARD,'DIAGNOSTIC_GIT')
    return PREFIX+'native_trial_result:'+pin['sha256']+':'+commit
def admission(pin,commit):
    return dict(schema='eq17.native_predeploy.v3',phase='authorized',service=SERVICE,source=RELEASE,
                forward_commit=FORWARD,diagnostic_commit=commit,packet=pin['sha256'],arm_ms=25,
                branch_role='diagnostic_only',production_publication=False,base_input=pin['base']['sha256'],overlay_input=pin['overlay']['sha256'],
                retry_input=pin['retry']['sha256'],previous_diagnostic=PREVIOUS_DIAGNOSTIC,previous_terminal=PREVIOUS_TERMINAL)
def claim(conn,pin,commit,t0):
    key=key_for(pin,commit);permitted=admission(pin,commit)
    value=dict(permitted,phase='running',attempt=uuid.uuid4().hex,started_wall=time.time(),t0_ns=str(t0))
    row=conn.execute("UPDATE live_eq17.state SET payload=%s WHERE key=%s AND payload::jsonb=%s::jsonb RETURNING key",
                     (canonical(value),key,canonical(permitted))).fetchone()
    check(row and row[0]==key,'MISSING_OR_PREVIOUS_ATTEMPT')
    return key,value
def save(conn,key,state):
    row=conn.execute("UPDATE live_eq17.state SET payload=%s WHERE key=%s AND payload::jsonb->>'phase'='running' AND payload::jsonb->>'attempt'=%s AND payload::jsonb->>'diagnostic_commit'=%s RETURNING key",
                     (canonical(state),key,state['attempt'],state['diagnostic_commit'])).fetchone()
    check(row and row[0]==key,'ATTEMPT_FENCE')
def read_private(conn,pin):
    previous='engineering:eq17-identity-readiness-1214cbba6240-20261109:native_trial_result:bc0ecbc2dfba474c161239231a037fe1f589478ac9a4b06e7e98969ce4266b96:'+PREVIOUS_DIAGNOSTIC
    row=conn.execute("SELECT encode(sha256(convert_to(payload,'UTF8')),'hex') FROM live_eq17.state WHERE key=%s AND payload::jsonb->>'phase'='evidence_ready'",(previous,)).fetchone()
    check(row and row[0]==PREVIOUS_TERMINAL,'PREVIOUS_TERMINAL')
    row=conn.execute("SELECT payload FROM live_eq17.state WHERE key=%s",(PREFIX+'candidate_by_recipe:'+BUILDER,)).fetchone()
    check(row,'NO_HOSTED_PROOF');p=load_json(row[0])
    expected=dict(status='verified',git_commit=FORWARD,release=RELEASE,base_release=BASE,builder_sha256=BUILDER,
                  tests_passed=True,suite_executed=True,reconstruct_only=False,controls_preserved=True,tests_discovered=922,
                  returncode=0,builder_returncode=0,log_sha256=HOSTED_LOG,files=213,bytes=6204868)
    expected.update(schema='eq17-identity-readiness/v1',execution='BOUNDED_PREDEPLOY_SUBPROCESS_NOT_SCANNER',
                    tests_skipped=['test_position_return_display.PositionReturnDisplayTests.test_position_cards_preserve_evidence_and_match_metrics_certainty'])
    check(all(type(p.get(k)) is type(v) and p[k]==v for k,v in expected.items()) and not p.get('reused',False),'HOSTED_PROOF_MISMATCH')
    published=conn.execute("SELECT EXISTS (SELECT 1 FROM live_eq17.deployment_artifacts WHERE hash=%s AND octet_length(payload)=6023063 AND encode(sha256(convert_to(payload,'UTF8')),'hex')=%s), EXISTS (SELECT 1 FROM live_eq17.deployment_artifacts WHERE hash IN (%s,%s))",(BASE,BASE,RELEASE,'c145be13120ab8cbb20b956db7fa942304b7428015bfab260e9655fb9956bed3')).fetchone()
    check(published==(True,False),'PUBLICATION_BOUNDARY')
    row=conn.execute("SELECT payload FROM live_eq17.state WHERE key=%s",(PREFIX+'candidate_payload:'+RELEASE,)).fetchone()
    check(row,'CANDIDATE_MISSING');candidate=row[0].encode();check(len(candidate)==6204868 and h(candidate)==RELEASE,'CANDIDATE_BYTES')
    return candidate,(read_piece(conn,OLD_OPERATION,pin['base']),read_piece(conn,'engineering:eq17-pending-retirement-cb383a5e04b9-20261107:',pin['overlay']),read_piece(conn,PREFIX,pin['retry']))
def path_ok(name):
    path=PurePosixPath(name)
    return bool(name) and path.as_posix()==name and not path.is_absolute() and '..' not in path.parts and '\\' not in name and re.fullmatch(r'[a-zA-Z0-9_.\-/]+',name) is not None
def extract(bundle,pin,temp,candidate):
    from app.eq17_release_integrity import validate_release
    checked=validate_release(candidate.decode(),RELEASE);tree=temp/'candidate/tree';tree.mkdir(parents=True)
    (temp/'candidate/candidate.json').write_bytes(candidate)
    check(len(checked['files'])==213,'CANDIDATE_SIZE')
    for n,body in checked['files'].items():
        check(path_ok(n),'CANDIDATE_PATH');f=tree/n;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(body.encode())
    packet=temp/'packet';packet.mkdir()
    for name,body in combined(*bundle,pin).items():
        f=packet/name;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(body)
    return tree,packet
def child_env(temp,t0):
    return dict(PATH='/usr/local/bin:/usr/bin:/bin',LANG='C.UTF-8',LC_ALL='C.UTF-8',HOME=str(temp/'work'),
       TMPDIR=str(temp/'work'),PYTHONDONTWRITEBYTECODE='1',PYTHONUNBUFFERED='1',MALLOC_ARENA_MAX='2',
       EQ17_T0_NS=str(t0),EQ17_ARM_MS='25',EQ17_WORK_DIR=str(temp/'work'),EQ17_CANDIDATE_ROOT=str(temp/'candidate/tree'),
       EQ17_CANDIDATE_ENVELOPE=str(temp/'candidate/candidate.json'),EQ17_RESULT_DIR=str(temp/'result'))
def checkpoint(path,limit,dir_fd=None):
    # Only explicit regular files from the pinned private writer; no symlink traversal.
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=dir_fd)
    try:
        st=os.fstat(fd);check(stat.S_ISREG(st.st_mode) and st.st_nlink==1 and 0<st.st_size<=limit,'RESULT_FILE')
        with os.fdopen(fd,'rb',closefd=False) as f: raw=f.read(limit+1)
        check(len(raw)==st.st_size and len(raw)<=limit,'RESULT_SIZE');return raw
    finally:os.close(fd)
def safe_guard(raw,guard_codes):
    result={}
    for line in raw.splitlines():
        abi=re.fullmatch(rb'EQ17_LANDLOCK_ABI ([0-9]{1,3})',line)
        links=re.fullmatch(rb'EQ17_PRIVATE_LINKS inert=([0-9]{1,4}) lowered=([0-9]{1,4}) remaining_up=0 nonloopback_routes=0',line)
        block=re.fullmatch(rb'EQ17_GUARD_BLOCK ([a-z0-9_]{1,48}) errno=([0-9]{1,4})',line)
        passed=re.fullmatch(rb'EQ17_GUARD_PASS mode=(unix_abi9|private_netns) external_file_denied capabilities_zero empty_sysv',line)
        if abi:result['landlock_abi']=int(abi[1])
        if links:result['private_links']={'inert':int(links[1]),'lowered':int(links[2]),'remaining_up':0,'nonloopback_routes':0}
        if block:
            if block[1].decode() in guard_codes:result.update(block=block[1].decode(),errno=int(block[2]))
            break
        if passed:result['mode']=passed[1].decode();result['guard_pass']=True;break
    return result
def drain_guard(pipe,evidence,guard_codes):
    # Seal the launcher's initial marker, then discard the private worker's stderr.
    with pipe:
        while 'guard_pass' not in evidence and 'block' not in evidence:
            line=pipe.readline(256)
            if not line:break
            entry=safe_guard(line,guard_codes)
            if not entry:break
            evidence.update(entry)
        while pipe.read(8192):pass
def child(temp,pin,t0,conn,key,state):
    from app.eq17_native_transport_receipt import transport_record,GUARD_CODES as guard_codes
    exe=Path('/usr/local/libexec/eq17-native-restrict');work=temp/'work';out=temp/'result'
    work.mkdir();out.mkdir()
    source=Path('/usr/local/share/eq17-native/restrict.c');check(h(source.read_bytes())==pin['restrict_sha256'],'LAUNCHER_SOURCE')
    args=[str(exe),str(temp),'/usr/local/bin/python','-B',str(temp/'packet/native_transport/run_fixture.py')]
    rows={}; seen={}
    data=temp/'packet/data'
    oracle=load_json((data/'oracle.json').read_bytes())
    market=load_json((data/'market.json').read_bytes())
    events=load_json((data/'observer-events.json').read_bytes())
    check(len(oracle)==30,'ORACLE_SHAPE')
    p=subprocess.Popen(args,env=child_env(temp,t0),cwd=temp/'packet',stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,close_fds=True,start_new_session=True)
    out_fd=os.open(out,os.O_DIRECTORY|os.O_NOFOLLOW|os.O_RDONLY)
    markers={}
    drain=threading.Thread(target=drain_guard,args=(p.stderr,markers,guard_codes),daemon=True)
    drain.start()
    try:
        while True:
            rc=p.poll()
            elapsed=(time.monotonic_ns()-t0)/1e9
            changed=False
            guard=dict(markers)
            if guard and guard!=state.get('isolation'):state['isolation']=guard;changed=True
            for name,validate,limit in [('started',lambda x:started_record(x,t0),3072),('transport',transport_record,8192)]:
                try:raw=checkpoint(name+'.json',limit,out_fd)
                except FileNotFoundError:continue
                d=h(raw)
                if seen.get(name)!=d:
                    state[name]=validate(load_json(raw));seen[name]=d;changed=True
            for i in range(30):
                if i not in rows:
                    try:raw=checkpoint(f'slot-{i:02d}.json',3072,out_fd)
                    except FileNotFoundError:continue
                    v=slot_record(load_json(raw),i,t0)
                    check(v['oracle_sha256']==h(canonical(oracle[i]).encode()) and
                          v['expected_trigger_bars']==market['eligible_symbols_with_trigger_bar_by_cutoff'][str(v['cutoff'])],'ORACLE_MISMATCH')
                    rows[i]=v;changed=True
            state['slots']=[rows[i] for i in sorted(rows)]
            if changed:
                state['elapsed_s']=round(elapsed,3);save(conn,key,state)
            if rc is not None:
                drain.join(timeout=.2)
                guard=dict(markers)
                if guard:state['isolation']=guard
                state['child_returncode']=int(rc)
                try:raw=checkpoint('terminal.json',3072,out_fd)
                except FileNotFoundError:pass
                else:state['terminal']=terminal_record(load_json(raw),state['slots'],t0,time.monotonic_ns())
                check(guard.get('guard_pass') is True,'NATIVE_GUARD_BLOCKED')
                check(guard.get('mode')!='private_netns' or 'private_links' in guard,'NATIVE_TOPOLOGY_EVIDENCE')
                tr=state.get('transport',{})
                if state.get('terminal',{}).get('phase')=='blocked':raise DiagnosticFailure(state['terminal']['reason'])
                if tr.get('status')=='BLOCKED':raise DiagnosticFailure('FIXTURE_'+tr['failure'])
                check(rc==0,'NATIVE_TRIAL_FAILED')
                if state.get('terminal',{}).get('gate'):
                    v=state['terminal']
                    check(v['event_sha256']==events['semantic_sha256'] and v['primary_sha256']==events['primary_sha256'] and
                          v['positive9_events']==events['count'] and v['primary_events']==len(events['primary_events']) and
                          v['position_count']==sum(x[2] for x in events['position_counts']),'EVENT_ORACLE')
                check(state.get('started') and state.get('terminal',{}).get('gate') is True and tr.get('status')=='COMPLETE' and tr.get('mode')==guard['mode'],'INCOMPLETE_EVIDENCE')
                return
            check(elapsed<LIMIT_SECONDS,'DEADLINE')
            time.sleep(.25)
    finally:
        os.close(out_fd)
        try:os.killpg(p.pid,signal.SIGTERM)
        except ProcessLookupError:pass
        try:p.wait(timeout=1)
        except subprocess.TimeoutExpired:
            try:os.killpg(p.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            p.wait(timeout=1)
def main(t0):
    parent_guard()
    import psycopg
    from app.db import _connection_info
    from app.database_url import normalise_custom_supabase_pooler_route
    pin=load_json((ROOT/'ops/eq17-native/pin.json').read_bytes());pin_record(pin)
    commit=os.environ.get('RENDER_GIT_COMMIT','');check(os.environ.get('RENDER_SERVICE_ID')==SERVICE and hexsha(commit,40),'DIAGNOSTIC_SERVICE')
    # Parent owns this one bounded connection. The sandbox child has no inherited descriptors or runtime environment.
    with psycopg.connect(_connection_info(normalise_custom_supabase_pooler_route(os.environ['DATABASE_URL'])),autocommit=True,connect_timeout=5,prepare_threshold=None) as conn:
        key,state=claim(conn,pin,commit,t0)
        directory=None
        try:
            candidate,bundle=read_private(conn,pin)
            directory=tempfile.TemporaryDirectory(prefix='eq17-native-')
            root=Path(directory.name);extract(bundle,pin,root,candidate)
            del candidate,bundle
            child(root,pin,t0,conn,key,state)
            check((time.monotonic_ns()-t0)/1e9<1800,'DEADLINE')
            state['phase']='evidence_ready'
        except Exception as exc:
            state['phase']='failed'
            state['failure']=failure_code(exc)
        state['elapsed_s']=round((time.monotonic_ns()-t0)/1e9,3)
        try:
            save(conn,key,state)
        finally:
            if directory is not None:directory.cleanup()
    print('EQ17_NATIVE_TRIAL_RECORDED',state['phase'],len(state.get('slots',[])),flush=True)
