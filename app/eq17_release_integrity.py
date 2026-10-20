"""Private EQ17 release packaging and fail-closed, full-chain verification.

Implementation hashes inside historical manifests are reference metadata; only
verified_files contains newly attested source files. No private rules here.
"""
import hashlib,json,copy
from pathlib import PurePosixPath
FAMILIES={5,10,20,30,50}
class ReleaseIntegrityError(ValueError):
    def __init__(self,code):self.code=code;super().__init__(code)
def sha256(s):return hashlib.sha256(s.encode('utf-8')).hexdigest()
def obj(s,code):
    try:d=json.loads(s)
    except (TypeError,ValueError) as e:raise ReleaseIntegrityError(code) from e
    if not isinstance(d,dict):raise ReleaseIntegrityError(code)
    return d
def safe_path(s):
    if not isinstance(s,str) or not s or '\\' in s:return False
    p=PurePosixPath(s)
    return not p.is_absolute() and p.as_posix()==s and '..' not in p.parts
def validate_release(raw,pin):
    if not isinstance(raw,str) or not isinstance(pin,str) or sha256(raw)!=pin:raise ReleaseIntegrityError('OUTER_HASH')
    a=obj(raw,'ARTIFACT_JSON');f=a.get('files')
    if not isinstance(f,dict) or not f:raise ReleaseIntegrityError('FILES_MISSING')
    if any(not safe_path(k) or not isinstance(v,str) for k,v in f.items()):raise ReleaseIntegrityError('UNSAFE_FILE')
    for path,key in [('manifest.json','manifest_hash'),('research_groups.json','research_groups_hash')]:
        if path not in f or sha256(f[path])!=a.get(key):raise ReleaseIntegrityError('TOP_LEVEL_'+key.upper())
    groups=obj(f['research_groups.json'],'GROUPS_JSON').get('groups')
    if not isinstance(groups,list) or len(groups)!=len(FAMILIES):raise ReleaseIntegrityError('GROUP_COUNT')
    seen=set()
    for g in groups:
        if not isinstance(g,dict) or type(g.get('family')) is not int:raise ReleaseIntegrityError('GROUP_INVALID')
        family=g['family'];path=g.get('manifest_path')
        if family not in FAMILIES or family in seen:raise ReleaseIntegrityError('GROUP_FAMILY')
        seen.add(family)
        if not safe_path(path) or path not in f or sha256(f[path])!=g.get('manifest_hash'):raise ReleaseIntegrityError('GROUP_MANIFEST_HASH')
    verified=a.get('verified_files',{})
    if not isinstance(verified,dict):raise ReleaseIntegrityError('VERIFIED_FILE_FORMAT')
    for path,expected in verified.items():
        if not safe_path(path) or path not in f or sha256(f[path])!=expected:raise ReleaseIntegrityError('VERIFIED_FILE_HASH')
    return a
def repack_release(raw,pin,replacements):
    """Update dependent group pins, outer pins and explicitly changed source pins."""
    a=copy.deepcopy(validate_release(raw,pin));f=a['files']
    if not isinstance(replacements,dict) or not replacements:raise ReleaseIntegrityError('NO_CHANGES')
    for k,v in replacements.items():
        if not safe_path(k) or k not in f or not isinstance(v,str) or k=='research_groups.json':raise ReleaseIntegrityError('REPLACEMENT_DENIED')
    f.update(replacements)
    groups=obj(f['research_groups.json'],'GROUPS_JSON')
    for g in groups['groups']:g['manifest_hash']=sha256(f[g['manifest_path']])
    f['research_groups.json']=json.dumps(groups,ensure_ascii=False,separators=(',',':'))
    a['manifest_hash']=sha256(f['manifest.json']);a['research_groups_hash']=sha256(f['research_groups.json'])
    a['verified_files']=dict(a.get('verified_files',{}),**{k:sha256(v) for k,v in replacements.items()})
    packed=json.dumps(a,ensure_ascii=False,sort_keys=True,separators=(',',':'))
    new_pin=sha256(packed);validate_release(packed,new_pin)
    return packed,new_pin
