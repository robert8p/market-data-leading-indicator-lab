"""One exact prior timer and one exact terminal failure. All database calls here are reads."""
import hashlib
import json
import os
import re

TIMER = 'badf62a10ca7f5c7cf2a4249b07d8b64870d4ad2ff72de4c0b35c5b29440fb37'
ORIGINAL_GIT = '5ffb98b7ade063f91e20ab47b06e5de8be13a3cd'
BASE = '3386c5d4fb61639bdeb5f1f6276aa2e24832314652c48512aaba3e9a20fc2023'
BUILDER = '371db21944e2bcff9b1f41b522942235f2643a1760414561e46cd2748ffecb1a'
SERVICE = 'srv-d9jm320u01pc73fhincg'
TERMINAL_KEY = 'engineering:eq17-backlog-20261023:hosted_supervision:' + TIMER
TERMINAL_HASH = '67c542cd96c251b112688a38b9c15b3772d95faae15905c615fc9f7bd6ecee51'
SUITE_FIELDS = ('base_release', 'builder_returncode', 'builder_sha256', 'controls_preserved', 'log_sha256', 'reconstruct_only', 'release', 'returncode', 'started', 'status', 'suite_executed', 'tests_discovered', 'tests_passed', 'tests_skipped')
SUITE_FINGERPRINT = 'ce4335479f7e4e4b0b0bc4b49f6ce0281811a96a654b3f4a65fe59daf4fc0d5c'

def require(ok,code):
    if not ok:raise RuntimeError('TIMER_CONTINGENCY_'+code)

def same_fields(record, expected):
    return isinstance(record,dict) and all(type(record.get(k)) is type(v) and record[k]==v for k,v in expected.items())

def terminal(raw):
    require(isinstance(raw,str) and hashlib.sha256(raw.encode('utf-8')).hexdigest()==TERMINAL_HASH,'RECEIPT_BYTES')
    require(same_fields(json.loads(raw),dict(release=TIMER,git_commit=ORIGINAL_GIT,
        status='blocked',attempt=1,returncode=1,reason='EXECUTED_CHECK_FAILED',suite_ok=False)), 'RECEIPT_IDENTITY')

def tested(proof):
    require(isinstance(proof,dict) and all(key in proof for key in SUITE_FIELDS),'ORIGINAL_GATE')
    protected=json.dumps({key:proof[key] for key in SUITE_FIELDS},sort_keys=True,
                         separators=(',',':'),allow_nan=False).encode()
    require(hashlib.sha256(protected).hexdigest()==SUITE_FINGERPRINT,'ORIGINAL_GATE')
    # Reuse updates the wrapper commit/check time, while preserving the original
    # test bytes, run start, test count, exit codes and log digest above.
    require(proof.get('git_commit')==ORIGINAL_GIT or
        (proof.get('reused') is True and proof.get('git_commit')==os.getenv('RENDER_GIT_COMMIT')), 'GATE_PROVENANCE')

def check(conn, release, proof):
    require(release==TIMER and os.getenv('RENDER_SERVICE_ID')==SERVICE,'SERVICE_OR_RELEASE')
    commit=os.getenv('RENDER_GIT_COMMIT','')
    require(re.fullmatch('[0-9a-f]{40}',commit) and commit!=ORIGINAL_GIT,'DISTINCT_PUBLIC_REF')
    tested(proof)
    raw=conn.execute('SELECT payload FROM live_eq17.state WHERE key=%s',(TERMINAL_KEY,)).fetchone()
    terminal(raw[0] if raw else None)
    publication=conn.execute("SELECT octet_length(payload), encode(sha256(convert_to(payload,'UTF8')),'hex') FROM live_eq17.deployment_artifacts WHERE hash=%s",(TIMER,)).fetchone()
    require(publication==(5744264,TIMER),'PUBLISHED_BYTES')
