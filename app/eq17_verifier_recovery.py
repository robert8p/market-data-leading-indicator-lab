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

RELEASE='7249ed762d724633cd841ca2fc1397b84558ef5872367b9dc26a5e7720ddd923'
SOURCE_SHA256='9611f7b55b57aa82122e7d30867533f8bd9a6e274c84d7554a18012b83032f0a'
RECOVERY_SHA256='1e6c94ec2d04c8070074a4ace53c8710c1df3153ae663ac9f332f30cac51be34'
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
   print('EQ17_VERIFIER_HTTP '+json.dumps({k:receipt.get(k) for k in ('path','status','body_prefix_bytes','body_prefix_sha256','body_truncated','retried')},sort_keys=True,separators=(',',':')),flush=True)
   raise"""


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
    return (source.replace(OLD_PROVENANCE,NEW_PROVENANCE,1)
        .replace(OLD_RETRY,NEW_RETRY,1).replace(OLD_HTTP,NEW_HTTP,1))


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
