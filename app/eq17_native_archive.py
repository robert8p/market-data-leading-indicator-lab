"""Materialize a sparse, whole-hash verified overlay over the existing immutable trial corpus."""
import base64
import hashlib
import io
import json
from pathlib import PurePosixPath
import re
import stat
import zipfile

OLD_OPERATION='engineering:eq17-primary-retirement-groups-c145be13120a-20261107:'
BASE='9584b2e219678c662da5851863cab1a03c980940d24a685101048e9ed0446e8c'
CHUNK=98304
MAX_PACKET=24*1024*1024
MAX_DATA=128*1024*1024
def need(ok):
    if not ok:raise ValueError('ARCHIVE_PIN')
def digest(b):return hashlib.sha256(b).hexdigest()
def valid_sha(v):return type(v) is str and re.fullmatch('[0-9a-f]{64}',v) is not None
def validate_piece_pin(pin):
    need(type(pin) is dict and set(pin)=={'sha256','bytes','chunks'} and valid_sha(pin['sha256']))
    need(type(pin['bytes']) is int and 0<pin['bytes']<=MAX_PACKET and type(pin['chunks']) is list)
    need(len(pin['chunks'])==(pin['bytes']+CHUNK-1)//CHUNK and all(valid_sha(h) for h in pin['chunks']))
def check_pin(pin):
    need(type(pin) is dict and set(pin)=={'schema','base','overlay','sha256','files','runner_sha256','restrict_sha256'})
    need(pin['schema']=='eq17.diagnostic.overlay.v1' and all(valid_sha(pin[k]) for k in ('sha256','runner_sha256','restrict_sha256')))
    for p in (pin['base'],pin['overlay']):validate_piece_pin(p)
    need(pin['base']['sha256']==BASE and pin['base']['bytes']==7949137 and pin['overlay']['sha256']!=BASE)
    need(pin['overlay']['bytes']<=2*1024*1024 and type(pin['files']) is int and 1<=pin['files']<=256)
def read_piece(conn,prefix,p):
    keys=[prefix+'native_trial_input:'+p['sha256']+':'+format(i,'05d') for i in range(len(p['chunks']))]
    rows=conn.execute("SELECT key,payload::jsonb #>> '{}' FROM live_eq17.state WHERE key=ANY(%s) AND jsonb_typeof(payload::jsonb)='string' ORDER BY key",(keys,)).fetchall()
    need([r[0] for r in rows]==keys)
    pieces=[]
    for i,(_,encoded) in enumerate(rows):
        n=min(CHUNK,p['bytes']-i*CHUNK)
        need(isinstance(encoded,str) and len(encoded)==4*((n+2)//3))
        raw=base64.b64decode(encoded,validate=True);need(len(raw)==n and digest(raw)==p['chunks'][i]);pieces.append(raw)
    blob=b''.join(pieces);need(len(blob)==p['bytes'] and digest(blob)==p['sha256']);return blob
def safe_path(n):
    p=PurePosixPath(n)
    return type(n) is str and bool(n) and p.as_posix()==n and not p.is_absolute() and '..' not in p.parts and re.fullmatch(r'[a-zA-Z0-9_.\-/]+',n) is not None
def members(raw,expected):
    need(digest(raw)==expected)
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        infos=z.infolist();names=[x.filename for x in infos]
        need(len(infos)==len(set(names)) and len(infos)<=256 and sum(x.file_size for x in infos)<=MAX_DATA)
        ret={}
        for x in infos:
            kind=stat.S_IFMT(x.external_attr>>16)
            need(safe_path(x.filename) and not x.is_dir() and kind in (0,stat.S_IFREG) and x.flag_bits&1==0)
            ret[x.filename]=z.read(x)
        return ret
def combined(base,overlay,pin):
    check_pin(pin)
    files=members(base,pin['base']['sha256']);changes=members(overlay,pin['overlay']['sha256'])
    need(0<len(changes)<=32 and all(v!=files.get(n) for n,v in changes.items()))
    files.update(changes)
    need(len(files)==pin['files'] and sum(map(len,files.values()))<=MAX_DATA)
    manifest={n:digest(v) for n,v in files.items()}
    raw=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
    need(digest(raw)==pin['sha256'] and manifest['native_transport/run_fixture.py']==pin['runner_sha256'])
    return files
