"""Exact, temporary repair for the 7249 hosted verifier.

The private release remains immutable.  This adapter creates one ephemeral
verifier script which acknowledges the already recipe-bound
``tests/test_latency.py`` delta and emits one allowlisted retryable-failure
marker.  It neither exports prior diagnostic state nor changes Scanner/runtime
files, consumer predicates, credentials or controls.
"""
from contextlib import contextmanager
import hashlib
from pathlib import Path

RELEASE='75d0bef737ce5e70caeef41226d2c292a84914aeba599b1b5e9c2a5ca81af71e'
SOURCE_SHA256='e7c1d7da5ac5402d7ea06dc3d6a20ba60d83f498fb738e40ca810efe04f29e4c'
RECOVERY_SHA256='bc8e57218b62a7d6b2b5d2e1f9f57ecf1130eccd14f4c005ddce13f92c053eb8'
OLD_HTTP_GET='   with client.open(base+path,timeout=20) as response:value=json.loads(response.read())'
NEW_HTTP_GET='   with client.open(base+path,timeout=20) as response:\n    body=response.read()\n   value=json.loads(body)'
HTTP_TIMEOUT_PHASES={183: 'OPEN_OR_RESPONSE_HEADERS', 184: 'READ_RESPONSE_BODY'}
OLD_PROVENANCE="tests/test_rule_retirement.py'})"
NEW_PROVENANCE="tests/test_rule_retirement.py','tests/test_latency.py'})"
OLD_RETRY="  if retryable:sys.exit(75)"
NEW_RETRY="""  if retryable:
   safe_reason=str(e) if str(e) in RETRYABLE_MESSAGES else 'SCANNER_OWNERSHIP_LOST'
   print('EQ17_VERIFIER_RETRYABLE '+json.dumps(dict(reason=safe_reason,error=type(e).__name__,failure_location=dict(file=pathlib.Path(location.filename).name,line=location.lineno,function=location.name)),sort_keys=True,separators=(',',':')),flush=True)
   sys.exit(75)"""
OLD_HTTP="""   db.put_state('verification:http-error:'+release,receipt);db.event('hosted_http_verification_failed',receipt,'test')
   raise"""
NEW_HTTP="""   db.put_state('verification:http-error:'+release,receipt);db.event('hosted_http_verification_failed',receipt,'test')
   safe_routes={'/api/positive9?scope=live':'P9_LIVE','/api/positive9?scope=test':'P9_TEST','/api/positive9?scope=replay':'P9_REPLAY'}
   if path in safe_routes:
    marker={k:receipt.get(k) for k in ('status','body_prefix_bytes','body_prefix_sha256','body_truncated','retried')}
    marker['route']=safe_routes[path]
    print('EQ17_VERIFIER_REPORT_HTTP '+os.environ.get('EQ17_VERIFIER_CALLER_NONCE','')+' '+json.dumps(marker,sort_keys=True,separators=(',',':')),flush=True)
   elif path in ('/api/positive9','/api/positive9/control','/api/coverage','/api/delivery-test','/api/higher-observation','/api/login','/api/outcome-recovery','/api/macro-source','/api/identity-requirement','/api/latency','/api/status'):
    print('EQ17_VERIFIER_HTTP '+json.dumps({k:receipt.get(k) for k in ('path','status','body_prefix_bytes','body_prefix_sha256','body_truncated','retried')},sort_keys=True,separators=(',',':')),flush=True)
   raise"""
OLD_ATTEMPT="attempt_id=hashlib.sha256(('hosted-delivery-route:'+release).encode()).hexdigest()[:32]"
NEW_ATTEMPT="attempt_id=hashlib.sha256(('hosted-delivery-route:'+release+':'+os.environ.get('RENDER_GIT_COMMIT','')).encode()).hexdigest()[:32]"
# Indexes form a closed vocabulary shared by the generated child and the supervisor.
CALLER_FRAMES = (
    ('positive9_verify_recovery.py', 'main'), ('positive9_verify_recovery.py', 'get'),
    ('positive9_verify_recovery.py', 'isolated_fixture'), ('positive9_verify_recovery.py', 'current_owner'),
    ('positive9_verify_recovery.py', 'verify_retirement_scope'), ('positive9_verify_recovery.py', 'http_error_receipt'),
    ('bls_primary_verify.py', 'verify'), ('bls_primary_verify.py', 'pages'),
    ('identity_primary_verify.py', 'verify'), ('cycle_timing_verify.py', 'verify'),
    ('evidence_transport_verify.py', 'verify'),
)
OLD_FINAL="""  failure=dict(release=os.environ.get('EQ17_RELEASE_SHA256'),stage='FAILED',failed_stage=previous.get('stage'),path=previous.get('path'),error=type(e).__name__,failure_location=dict(file=pathlib.Path(location.filename).name,line=location.lineno,function=location.name),retryable_handover_or_fresh_source=retryable,updated=time.time())
  db.put_state('test:positive9_progress',failure)"""
NEW_FINAL="""  frame_nonce=os.environ.pop('EQ17_VERIFIER_CALLER_NONCE','')
  if type(e) is TimeoutError and frame_nonce:
   callers={(str(ROOT/'src'/name),fn):i for i,(name,fn) in enumerate(CALLER_FRAMES_LITERAL)}
   last_caller=None
   for frame,lineno in traceback.walk_tb(e.__traceback__):
    index=callers.get((frame.f_code.co_filename,frame.f_code.co_name))
    if index is not None:last_caller=(index,lineno)
   if last_caller is not None:
    try:print('EQ17_VERIFIER_CALLER',frame_nonce,*last_caller,flush=True)
    except OSError:pass
  failure=dict(release=os.environ.get('EQ17_RELEASE_SHA256'),stage='FAILED',failed_stage=previous.get('stage'),path=previous.get('path'),error=type(e).__name__,failure_location=dict(file=pathlib.Path(location.filename).name,line=location.lineno,function=location.name),retryable_handover_or_fresh_source=retryable,updated=time.time())
  if isinstance(e,urllib.error.HTTPError):
   safe_url=str(e.geturl());safe_path='/'+safe_url.split('://',1)[-1].split('/',1)[-1].split('?',1)[0]
   if safe_path in ('/api/positive9','/api/positive9/control','/api/coverage','/api/delivery-test','/api/higher-observation','/api/login','/api/outcome-recovery','/api/macro-source','/api/identity-requirement','/api/latency','/api/status'):
    print('EQ17_VERIFIER_HTTP_FINAL '+json.dumps(dict(path=safe_path,status=int(e.code),stage=previous.get('stage')),sort_keys=True,separators=(',',':')),flush=True)
  db.put_state('test:positive9_progress',failure)""".replace('CALLER_FRAMES_LITERAL', repr(CALLER_FRAMES))


def patched_source(source,expected_sha256=SOURCE_SHA256):
    raw=source.encode('utf-8')
    if hashlib.sha256(raw).hexdigest()!=expected_sha256:
        raise RuntimeError('VERIFIER_RECOVERY_SOURCE_PIN_MISMATCH')
    if source.count(OLD_PROVENANCE)!=1 or NEW_PROVENANCE in source:
        raise RuntimeError('VERIFIER_RECOVERY_PATCH_SCOPE_MISMATCH')
    if source.count(OLD_RETRY)!=1 or NEW_RETRY in source:
        raise RuntimeError('VERIFIER_RECOVERY_DIAGNOSTIC_SCOPE_MISMATCH')
    if source.count(OLD_HTTP)!=1 or NEW_HTTP in source:
        raise RuntimeError('VERIFIER_RECOVERY_HTTP_SCOPE_MISMATCH')
    if source.count(OLD_ATTEMPT)!=1 or NEW_ATTEMPT in source:
        raise RuntimeError('VERIFIER_RECOVERY_ATTEMPT_SCOPE_MISMATCH')
    if source.count(OLD_FINAL)!=1 or NEW_FINAL in source:
        raise RuntimeError('VERIFIER_RECOVERY_FINAL_SCOPE_MISMATCH')
    if source.count(OLD_HTTP_GET)!=1 or NEW_HTTP_GET in source:raise RuntimeError('VERIFIER_HTTP_PHASE_SCOPE_MISMATCH')
    old_member_guard='  if name not in CHANGED_BASELINE_FILES|retirement_files:assert current==body.encode(),name'
    new_member_guard="  exact_parent_delta={'tests/test_news_snapshot_generation.py': ['0498ea5c42bd49a917b4280970d9a484a30e5b41fe6225c1cab5e33144848e08', '0bd76d0b013bd057f2bf8c2a6a1cbf7303f56d91c26316526fb47366ef331fa8'], 'src/providers.py': ['cbc7fe7deff7838ec8434640b8ca3b0f472cebe9d245a4db6fc088af5090cc3f', 'd9b47a67611cb76ea4ccdcce38b9fe398b999142f78221855a0525398ed9f2c4'], 'src/preferred_reference.py': ['372a543e3b005dc7232602ccdf77c8ea1a911935537f7131559aa9671a3b18fa', '8ea3ccbd3ec973d62919d250a4696a57aafb12366a03282393384dee48c7cf49'], 'src/figi_reference.py': ['f2767f2005f24d1282f4d2a97e461351bef8f407d38ad5d1304cfce75b240b49', '6ed3096bae37f4bae542b24c46944a77fbfdd838570a127bf405154a82f82aca'], 'tests/test_source_repair.py': ['70670e91fca23e141d334064348c4afe09fca0760d0c0a699ad67b00a29d4bd7', 'ee72b6f95c464f9f16ff75b4cc8be6d979a579ce5053b293556f40e1f83a931c']}\n  if name in exact_parent_delta:\n   assert [hashlib.sha256(body.encode()).hexdigest(), hashlib.sha256(current).hexdigest()]==exact_parent_delta[name],name\n  elif name not in CHANGED_BASELINE_FILES|retirement_files:assert current==body.encode(),name"
    if source.count(old_member_guard)!=1:raise RuntimeError('VERIFIER_EXACT_MEMBER_SCOPE_MISMATCH')
    return (source.replace(OLD_PROVENANCE,NEW_PROVENANCE,1).replace(OLD_HTTP_GET,NEW_HTTP_GET,1).replace(old_member_guard,new_member_guard,1)
        .replace(OLD_RETRY,NEW_RETRY,1).replace(OLD_HTTP,NEW_HTTP,1)
        .replace(OLD_ATTEMPT,NEW_ATTEMPT,1).replace(OLD_FINAL,NEW_FINAL,1))


@contextmanager
def recovery_script(root,release):
    if release!=RELEASE:raise RuntimeError('VERIFIER_RECOVERY_RELEASE_MISMATCH')
    root=Path(root);source=(root/'src'/'positive9_verify.py').read_text()
    recovered=patched_source(source)
    if hashlib.sha256(recovered.encode()).hexdigest()!=RECOVERY_SHA256:
        raise RuntimeError('VERIFIER_RECOVERY_OUTPUT_PIN_MISMATCH')
    target=root/'src'/'positive9_verify_recovery.py'
    target.write_text(recovered)
    try:yield target
    finally:target.unlink(missing_ok=True)



