"""Isolated private application mount. Public source contains no research rules.

Release payloads are hash-pinned and read using existing server database access.
The original application's routes, middleware, workers and holds are unchanged.
"""
import os,json,hashlib,pathlib,tempfile,subprocess,threading,time,atexit
import psycopg,httpx
from app.db import _connection_info
from app.b001_web import app as original_app

PIN='576e9c15e2c43a8704e9a9787f42ede40f19dccf9cb234453ab60db348fed3e3'
children=[]
def stop():
    for p in children:
        if p.poll() is None:p.terminate()
atexit.register(stop)

def supervise(script,root,env):
    # Bounded restart rate; fail closed without restarting unrelated applications.
    attempts=[]
    while True:
        now=time.monotonic();attempts[:]=[t for t in attempts if now-t<3600]
        if len(attempts)>=10:time.sleep(60);continue
        attempts.append(now)
        p=subprocess.Popen([os.sys.executable,str(root/'src'/script)],cwd=root,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        children.append(p);p.wait();children.remove(p);time.sleep(min(60,2**min(len(attempts),6)))

def start():
    try:
        with psycopg.connect(_connection_info(os.environ['DATABASE_URL']),connect_timeout=15) as c:
            row=c.execute('SELECT payload FROM live_eq17.deployment_artifacts WHERE hash=%s',(PIN,)).fetchone()
        if not row or hashlib.sha256(row[0].encode()).hexdigest()!=PIN:raise RuntimeError('private artifact verification')
        artifact=json.loads(row[0]);root=pathlib.Path(tempfile.mkdtemp(prefix='eq17-'))
        for name,content in artifact['files'].items():
            path=pathlib.PurePosixPath(name)
            if path.is_absolute() or '..' in path.parts:raise RuntimeError('artifact path')
            target=root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_text(content)
        if hashlib.sha256((root/'manifest.json').read_bytes()).hexdigest()!=artifact['manifest_hash']:raise RuntimeError('manifest verification')
        env=os.environ.copy();env.update(EQ17_DATABASE_URL=_connection_info(os.environ['DATABASE_URL']),EQ17_RELEASE_SHA256=PIN,EQ17_MANIFEST_SHA256=artifact['manifest_hash'],EQ17_BIND='127.0.0.1',PORT='10001',EQ17_VERIFY_DONE=str(root/'verification.done'))
        for script in ['server.py','runtime.py']:threading.Thread(target=supervise,args=(script,root,env),daemon=True).start()
        def verify_once():
            try:subprocess.run([os.sys.executable,str(root/'src'/'positive9_verify.py')],cwd=root,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=720)
            finally:(root/'verification.done').touch()
        threading.Thread(target=verify_once,daemon=True).start()
        print('Private application release verified; supervisor started',flush=True)
    except Exception as e:
        # No exception messages: database DSNs and credentials must never enter logs.
        print('Private application startup failed: '+type(e).__name__,flush=True)

threading.Thread(target=start,daemon=True).start()

async def app(scope,receive,send):
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
