"""Bounded private release build. No strategy source or credentials are public.

The original artifact and reviewed private recipe are both SHA256-pinned.
Predeploy tests run without service credentials. Startup reconstructs the same
candidate from those immutable inputs rather than trusting a mutable DB pin.
No scanner, entry control or scheduler is started by this module.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import traceback
import psycopg
from app.db import _connection_info
from app.eq17_release_integrity import validate_release, ReleaseIntegrityError

BASE = '7b4727c990213d44dfc0532f3f939c36706423a3b33362c8dc2f0b654bff6c26'
_RECIPE = json.loads((Path(__file__).resolve().parents[1]/'ops/eq17_gate_recipe_pin.json').read_text())
BUILDER = _RECIPE['builder_sha256']
assert _RECIPE['base_release'] == BASE and len(BUILDER) == 64
OP = 'eq17-gates-135-75-20261021'
assert _RECIPE['operation'] == OP and _RECIPE['activation_authorised'] is False
PREFIX = 'engineering:' + OP + ':'
SERVICE = 'srv-d9jm320u01pc73fhincg'
LOCK = 17351021135
PROTECTED = ['live:paused', 'research:activation_v1:intent',
             'research:activation_v1:control', 'research:positive9:v1:alert_control'] + [
                 f'live:group_alerts:{f}' for f in (5,10,20,30,50)]


def sha(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def connection():
    return psycopg.connect(_connection_info(os.environ['DATABASE_URL']),
                           connect_timeout=15, autocommit=True)


def state(conn, key):
    row = conn.execute('SELECT payload FROM live_eq17.state WHERE key=%s', (key,)).fetchone()
    return json.loads(row[0]) if row else None


def put(conn, key, value):
    conn.execute('INSERT INTO live_eq17.state(key,payload) VALUES (%s,%s) '
                 'ON CONFLICT(key) DO UPDATE SET payload=excluded.payload',
                 (key, json.dumps(value, sort_keys=True, allow_nan=False)))


def inputs(conn):
    row = conn.execute('SELECT payload FROM live_eq17.deployment_artifacts WHERE hash=%s',
                       (BASE,)).fetchone()
    if not row:
        raise ReleaseIntegrityError('GATE_BASE_RELEASE_MISSING')
    validate_release(row[0], BASE)
    builder = state(conn, PREFIX+'builder') or {}
    source = builder.get('source', '')
    if sha(source) != BUILDER or builder.get('sha256') != BUILDER or builder.get('base_release') != BASE:
        raise ReleaseIntegrityError('GATE_BUILDER_PIN_MISMATCH')
    return row[0], source


def isolated_build(raw, source, reconstruct=False):
    """Only loopback networking is permitted; production credentials are absent."""
    with tempfile.TemporaryDirectory(prefix='eq17-gate-build-') as directory:
        root = Path(directory)
        (root/'base.json').write_text(raw)
        (root/'builder.py').write_text(source)
        guard = root/'guard'; guard.mkdir()
        (guard/'sitecustomize.py').write_text(
            "import socket\n"
            "_connect=socket.socket.connect\n"
            "def connect(self,address):\n"
            " if isinstance(address,tuple) and str(address[0]) not in ('127.0.0.1','::1','localhost'):\n"
            "  raise OSError('ISOLATED_TEST_EXTERNAL_NETWORK_DENIED')\n"
            " return _connect(self,address)\n"
            "socket.socket.connect=connect\n")
        env = {k:v for k,v in os.environ.items() if k in ('PATH','LANG','LC_ALL')}
        env.update(HOME=directory, TMPDIR=directory, PYTHONPATH=str(guard),
                   PYTHONDONTWRITEBYTECODE='1', MALLOC_ARENA_MAX='2')
        args = [sys.executable, str(root/'builder.py'), str(root/'base.json'),
                str(root/'candidate'), str(root/'proof.json'), str(root/'candidate.json')]
        if reconstruct:
            args.append('--reconstruct-only')
        process = subprocess.Popen(args, cwd=root, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True, start_new_session=True)
        try:
            stdout, stderr = process.communicate(timeout=90 if reconstruct else 330)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate()
            raise RuntimeError('ISOLATED_BUILD_TIMEOUT') from None
        proof = json.loads((root/'proof.json').read_text()) if (root/'proof.json').exists() else {}
        proof.update(builder_sha256=BUILDER, base_release=BASE,
                     builder_returncode=process.returncode,
                     builder_output=(stdout+'\n'+stderr)[-16000:])
        if process.returncode or not (root/'candidate.json').exists():
            return None, proof
        candidate = (root/'candidate.json').read_text()
        release = sha(candidate)
        artifact = validate_release(candidate, release)
        before = json.loads(raw)['files']
        frozen = ['manifest.json','research_groups.json','src/core.py'] + [
            g['manifest_path'] for g in json.loads(before['research_groups.json'])['groups']]
        if any(artifact['files'][p] != before[p] for p in frozen):
            raise ReleaseIntegrityError('GATE_FROZEN_STRATEGY_CHANGED')
        proof['release'] = release
        return candidate, proof


def predeploy():
    if os.environ.get('RENDER_SERVICE_ID') != SERVICE:
        return
    with connection() as conn:
        if not conn.execute('SELECT pg_try_advisory_lock(%s)', (LOCK,)).fetchone()[0]:
            raise RuntimeError('GATE_BUILD_ALREADY_OWNED')
        try:
            raw, source = inputs(conn)
            previous = state(conn, PREFIX+'candidate') or {}
            if previous.get('tests_passed') is True and previous.get('builder_sha256') == BUILDER:
                candidate, reconstructed = isolated_build(raw, source, reconstruct=True)
                if candidate and reconstructed.get('release') == previous.get('release'):
                    print('EQ17_GATE_PREDEPLOY_REUSED release='+previous['release'], flush=True)
                    return
            before = {k:state(conn,k) for k in PROTECTED}
            if before['live:paused'] is not True:
                raise RuntimeError('GATE_CHANGE_REQUIRES_EXISTING_PAUSE')
            put(conn, PREFIX+'build', dict(status='in_progress',builder_sha256=BUILDER,
                started=time.time(),git_commit=os.environ.get('RENDER_GIT_COMMIT'),
                execution='BOUNDED_PREDEPLOY_SUBPROCESS_NOT_SCANNER'))
            candidate, proof = isolated_build(raw, source)
            proof.update(checked=time.time(),git_commit=os.environ.get('RENDER_GIT_COMMIT'),
                         controls_preserved=before=={k:state(conn,k) for k in PROTECTED})
            if candidate is None or proof.get('tests_passed') is not True or not proof['controls_preserved']:
                proof['status']='failed'
                put(conn, PREFIX+'build:'+BUILDER, proof)
                put(conn, PREFIX+'build', proof)
                raise RuntimeError('GATE_PREDEPLOY_TESTS_FAILED_PRIVATE_EVIDENCE_RETAINED')
            release=proof['release']
            conn.execute('INSERT INTO live_eq17.deployment_artifacts(hash,payload) VALUES (%s,%s) '
                         'ON CONFLICT(hash) DO NOTHING', (release,candidate))
            proof['status']='verified'
            put(conn, PREFIX+'candidate:'+release, proof)
            put(conn, PREFIX+'candidate', proof)
            put(conn, PREFIX+'build', proof)
            print('EQ17_GATE_PREDEPLOY_VERIFIED release='+release, flush=True)
        except Exception as error:
            current=state(conn,PREFIX+'build') or {}
            if current.get('status')!='failed':
                put(conn,PREFIX+'build',dict(status='failed',builder_sha256=BUILDER,
                    checked=time.time(),error_type=type(error).__name__,
                    detail=str(error)[:600],traceback=traceback.format_exc()[-4000:],
                    production_release_selected=False))
            raise RuntimeError('EQ17_GATE_PREDEPLOY_BLOCKED_SEE_PRIVATE_CHECKPOINT') from None
        finally:
            conn.execute('SELECT pg_advisory_unlock(%s)', (LOCK,))


def resolve_release():
    """Re-derive the pin from reviewed inputs; a DB status alone is insufficient."""
    with connection() as conn:
        raw,source=inputs(conn)
        proof=state(conn,PREFIX+'candidate') or {}
    if proof.get('tests_passed') is not True or proof.get('builder_sha256')!=BUILDER:
        raise ReleaseIntegrityError('GATE_EXACT_RECIPE_TEST_PROOF_MISSING')
    candidate,reconstructed=isolated_build(raw,source,reconstruct=True)
    if not candidate or reconstructed.get('release')!=proof.get('release'):
        raise ReleaseIntegrityError('GATE_RECONSTRUCTED_RELEASE_MISMATCH')
    return reconstructed['release']


if __name__=='__main__':
    predeploy()
