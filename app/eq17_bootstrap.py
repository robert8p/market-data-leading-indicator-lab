"""Isolated private application mount. Public source contains no research rules.

Release payloads are hash-pinned and read using existing server database access.
The original application's routes, middleware, workers and holds are unchanged.
"""
import os,json,hashlib,pathlib,tempfile,subprocess,threading,time,atexit,re,asyncio
import psycopg,httpx
from app.db import _connection_info
from app.b001_web import app as original_app
from app.eq17_release_integrity import ReleaseIntegrityError,validate_release
from app.eq17_process_monitor import ExitJournal,inspect_child,make_event
from app.eq17_gate_revision import resolve_release

PIN='7b4727c990213d44dfc0532f3f939c36706423a3b33362c8dc2f0b654bff6c26'
children={}
child_started={}
child_lock=threading.Lock()
boot_done=threading.Event()
shutdown=threading.Event()
boot_error=None
boot_started=time.monotonic()
owner_probe_lock=threading.Lock()
owner_probe_checked=0.0
owner_probe_good=0.0
boot_started_wall=time.time()
child_journal=ExitJournal(lambda:psycopg.connect(_connection_info(os.environ['DATABASE_URL']),connect_timeout=3))
def stop():
    shutdown.set()
    with child_lock:active=list(children.values())
    for p in active:
        if p.poll() is None:p.terminate()
atexit.register(stop)

def diagnostic_flush_loop():
    # Persistent database receipt is retried while the web service remains live.
    while not shutdown.wait(20):
        done=child_journal.flush()
        if done:print('EQ17_CHILD_DIAGNOSTICS_RETAINED count='+str(done),flush=True)

def supervise(script,root,env):
    if script not in ('server.py','runtime.py'):raise ValueError('unexpected child')
    failures=0
    while not shutdown.is_set():
        reason='UNCLASSIFIED';status=-1;started=time.monotonic()
        try:
            p=subprocess.Popen([os.sys.executable,str(root/'src'/script)],cwd=root,env=env,
                stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,text=True,errors='replace',bufsize=1)
            with child_lock:children[script]=p;child_started[script]=time.monotonic()
            status,reason=inspect_child(p)
        except (OSError,ValueError):
            reason='LAUNCH_EXCEPTION'
        finally:
            with child_lock:children.pop(script,None);child_started.pop(script,None)
        if not shutdown.is_set():
            elapsed=time.monotonic()-started
            failures=1 if elapsed>=300 else min(failures+1,10)
            event=make_event(script,status,reason,PIN,failures,elapsed)
            child_journal.record(event)
            print('EQ17_CHILD_EXIT component='+script+' status='+str(status)+
                  ' reason='+reason+' diagnostic_id='+event[0],flush=True)
            shutdown.wait(min(60,2**min(failures,6)))


def start():
    global boot_error,PIN
    try:
        # Reconstruct from pinned baseline + pinned recipe, then require the
        # exact predeploy regression proof. A mutable DB pointer is not trust.
        PIN=resolve_release()
        with psycopg.connect(_connection_info(os.environ['DATABASE_URL']),connect_timeout=15) as c:
            row=c.execute('SELECT payload FROM live_eq17.deployment_artifacts WHERE hash=%s',(PIN,)).fetchone()
        if not row:raise ReleaseIntegrityError('RELEASE_NOT_FOUND')
        artifact=validate_release(row[0],PIN);root=pathlib.Path(tempfile.mkdtemp(prefix='eq17-'))
        for name,content in artifact['files'].items():
            path=pathlib.PurePosixPath(name)
            if path.is_absolute() or '..' in path.parts:raise RuntimeError('artifact path')
            target=root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(content)
        if hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest()!=artifact['manifest_hash']:raise RuntimeError('manifest verification')
        env=os.environ.copy();env.update(EQ17_DATABASE_URL=_connection_info(os.environ['DATABASE_URL']),EQ17_RELEASE_SHA256=PIN,EQ17_MANIFEST_SHA256=artifact['manifest_hash'],EQ17_BIND='127.0.0.1',PORT='10001',EQ17_VERIFY_DONE=str(root/'verification.done'))
        # Bound glibc arena proliferation in threaded children. This limits
        # allocator retention, not application allocations or retained evidence.
        env['MALLOC_ARENA_MAX']='2'
        del row,artifact,content
        try:
            import gc,ctypes
            gc.collect();ctypes.CDLL(None).malloc_trim(0)
        except (OSError,AttributeError):pass
        threading.Thread(target=diagnostic_flush_loop,daemon=True).start()
        for script in ['server.py','runtime.py']:threading.Thread(target=supervise,args=(script,root,env),daemon=True).start()
        def verify_once():
            # The existing verifier already runs regression, authenticated UI,
            # macro adoption, retained identities and actual runtime timing.
            # Reuse it rather than starting a second verification controller.
            from app.eq17_verification_runner import verify
            from app.eq17_verifier_recovery import RELEASE,recovery_script
            from app.eq17_verifier_claim import Attempt
            try:attempt=Attempt(PIN,os.environ.get('RENDER_GIT_COMMIT'),RELEASE)
            except ValueError:
                print('EQ17_HOSTED_VERIFICATION_HELD',flush=True)
                return
            def connection():
                return psycopg.connect(_connection_info(os.environ['DATABASE_URL']),connect_timeout=5)
            def record(value):
                with connection() as conn:value=attempt.record(conn,value)
                if value.get('status') in ('complete','blocked','stopped'):
                    safe={k:value.get(k) for k in ('status','reason','handover_reason','http_failure','returncode','tests','suite_ok','failure_class','failure_location','verifier_failure_location','verifier_timeout_phase','output_bytes','output_sha256','output_tail_truncated') if value.get(k) is not None}
                    print('EQ17_HOSTED_VERIFICATION_RESULT '+json.dumps(safe,sort_keys=True,separators=(',',':')),flush=True)
            try:
                # Readiness is a read-only precondition, before the durable once-only claim.
                # A cold-start wait must not consume or reset a natural verifier attempt.
                from app.eq17_verifier_readiness import wait_ready
                readiness=wait_ready(connection,PIN,shutdown)
                if readiness['status']!='ready':
                    print('EQ17_HOSTED_VERIFICATION_PREFLIGHT '+readiness['reason'],flush=True)
                    return
                if shutdown.is_set():return
                with connection() as conn:
                    if shutdown.is_set():return
                    state=attempt.claim(conn)
                if state=='terminal':
                    (root/'verification.done').touch()
                    return
                if state!='claimed':
                    print('EQ17_HOSTED_VERIFICATION_HELD',flush=True)
                    return
                with recovery_script(root,PIN) as verifier:
                    passed=verify([os.sys.executable,str(verifier)],root,env,shutdown,record)
                if not passed:print('EQ17_HOSTED_VERIFICATION_BLOCKED component=positive9_verify.py',flush=True)
            except Exception:
                # No output, connects or re-execution from an unknown outcome.
                print('EQ17_HOSTED_VERIFICATION_EXECUTION_UNCERTAIN',flush=True)
                return
            if attempt.finished:(root/'verification.done').touch()
        threading.Thread(target=verify_once,daemon=True).start()
        print('EQ17_RELEASE_CHAIN_VALIDATED release='+PIN[:12],flush=True)
    except ReleaseIntegrityError as e:
        boot_error=e.code;print('EQ17_STARTUP_BLOCKED reason='+e.code,flush=True)
    except Exception as e:
        # No exception messages: database DSNs and credentials must never enter logs.
        boot_error=type(e).__name__;print('EQ17_STARTUP_BLOCKED reason='+type(e).__name__,flush=True)
    finally:
        boot_done.set()

threading.Thread(target=start,daemon=True).start()


def _active_owner_evidence():
    # Requires a recently renewed scanner owner, independently of web health.
    try:
        with psycopg.connect(_connection_info(os.environ['DATABASE_URL']),connect_timeout=2) as conn:
            conn.execute("SET LOCAL statement_timeout TO '2s'")
            row=conn.execute("""
                SELECT l.expires > extract(epoch from now())
                  AND o.payload::jsonb->>'owner'=l.owner
                  AND (o.payload::jsonb->>'heartbeat')::double precision >
                      extract(epoch from now())-50
                  AND (o.payload::jsonb->>'last_progress')::double precision >
                      extract(epoch from now())-600
                  AND EXISTS (
                    SELECT 1 FROM live_eq17.operational_events e
                    WHERE e.kind='runtime_started'
                      AND e.created >= %s
                      AND e.payload::jsonb->>'owner'=l.owner
                      AND e.payload::jsonb->>'release'=%s
                  )
                FROM live_eq17.owner_lease l
                JOIN live_eq17.state o ON o.key='live:owner'
                WHERE l.name='scanner'
            """,(boot_started_wall-2,PIN)).fetchone()
        return bool(row and row[0])
    except Exception:
        return False

async def private_ready():
    global owner_probe_checked,owner_probe_good
    if not boot_done.is_set() or boot_error:return False
    now=time.monotonic()
    with child_lock:
        stable=all(name in children and children[name].poll() is None and
                   now-child_started.get(name,now)>=6.0 for name in ('server.py','runtime.py'))
    if not stable:return False
    try:
        async with httpx.AsyncClient(timeout=1.5,trust_env=False) as client:
            result=await client.get('http://127.0.0.1:10001/health')
        if result.status_code!=200:return False
    except (httpx.RequestError,ValueError):
        return False
    # During zero-downtime handover the previous instance holds the lease.
    # Thereafter verify actual ownership, not only a living process.
    # Render drained the old instance for 130s and the successor acquired its
    # normal 45s lease by 195s. A 120s readiness grace caused a platform restart
    # after that successful handover. Bound transport-only grace at 240s; all
    # scanner writes and acceptance still require the original live fence.
    if now-boot_started<240:return True
    with owner_probe_lock:
        due=now-owner_probe_checked>=15
        if due:owner_probe_checked=now
    if due:
        ok=await asyncio.to_thread(_active_owner_evidence)
        if ok:
            with owner_probe_lock:owner_probe_good=time.monotonic()
    with owner_probe_lock:return time.monotonic()-owner_probe_good<=50

async def app(scope,receive,send):
    if scope['type']=='http' and scope['path'] in ('/health','/eq17/healthz'):
        ready=await private_ready()
        payload=json.dumps({'status':'ok' if ready else 'unavailable','component':'eq17',
            'release':PIN[:12],'ready':ready}).encode()
        await send({'type':'http.response.start','status':200 if ready else 503,
            'headers':[(b'content-type',b'application/json'),(b'cache-control',b'no-store')]})
        await send({'type':'http.response.body','body':payload});return
    if scope['type']!='http' or not (scope['path']=='/eq17' or scope['path'].startswith('/eq17/')):
        return await original_app(scope,receive,send)
    if scope['path']=='/eq17':
        await send({'type':'http.response.start','status':307,'headers':[(b'location',b'/eq17/')]});await send({'type':'http.response.body','body':b''});return
    body=b''
    while True:
        part=await receive()
        if part['type']=='http.disconnect':return
        body+=part.get('body',b'')
        if len(body)>65536:
            await send({'type':'http.response.start','status':413,'headers':[]});await send({'type':'http.response.body','body':b''});return
        if not part.get('more_body'):break
    headers={k.decode():v.decode() for k,v in scope['headers'] if k.lower() not in {b'connection',b'transfer-encoding',b'content-length'}}
    query=scope.get('query_string',b'').decode();path=scope['path'][5:] or '/'
    try:
        async with httpx.AsyncClient(timeout=25,trust_env=False) as client:
            r=await client.request(scope['method'],'http://127.0.0.1:10001'+path+('?' +query if query else ''),headers=headers,content=body)
        response_headers=[(k.encode(),v.encode()) for k,v in r.headers.multi_items() if k.lower() not in {'connection','transfer-encoding','content-length','content-encoding'}]
        await send({'type':'http.response.start','status':r.status_code,'headers':response_headers});await send({'type':'http.response.body','body':r.content})
    except Exception:
        await send({'type':'http.response.start','status':503,'headers':[(b'content-type',b'application/json'),(b'cache-control',b'no-store')]});await send({'type':'http.response.body','body':b'{"error":"Private scanner unavailable; existing application remains independent"}'})


# HTTP response transport only: scanner runtime and evidence paths are untouched.
from app.egress_http import EQ17ApiGzip
app = EQ17ApiGzip(app)
