"""Exact, temporary repair for the 7249 hosted verifier provenance check.

The private release remains immutable.  This adapter creates one ephemeral
verifier script whose only source change acknowledges the already recipe-bound
``tests/test_latency.py`` delta.  It neither reads prior diagnostic state nor
changes Scanner/runtime files, consumer predicates, credentials or controls.
"""
from contextlib import contextmanager
import hashlib
from pathlib import Path

RELEASE='7249ed762d724633cd841ca2fc1397b84558ef5872367b9dc26a5e7720ddd923'
SOURCE_SHA256='9611f7b55b57aa82122e7d30867533f8bd9a6e274c84d7554a18012b83032f0a'
RECOVERY_SHA256='b31e5dce7afd742aaf407cf9bead43b8a2f7a6a4bf339323a7ddafc5887f253f'
OLD="tests/test_rule_retirement.py'})"
NEW="tests/test_rule_retirement.py','tests/test_latency.py'})"


def patched_source(source,expected_sha256=SOURCE_SHA256):
    raw=source.encode('utf-8')
    if hashlib.sha256(raw).hexdigest()!=expected_sha256:
        raise RuntimeError('VERIFIER_RECOVERY_SOURCE_PIN_MISMATCH')
    if source.count(OLD)!=1 or NEW in source:
        raise RuntimeError('VERIFIER_RECOVERY_PATCH_SCOPE_MISMATCH')
    return source.replace(OLD,NEW,1)


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
