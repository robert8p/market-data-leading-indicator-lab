"""Once-only, bounded continuation of the existing hosted verifier.

The runner never stores child output.  It drains the complete stream, retains a
small in-memory tail only long enough to derive an allowlisted diagnostic, and
kills the verifier's owned process group on timeout.  This is neither a scanner
nor a second verification controller.
"""
from collections import deque
import hashlib
import json
import math
import os
import re
import secrets
import signal
import subprocess
import threading
import time

from app.eq17_verifier_recovery import CALLER_FRAMES,HTTP_TIMEOUT_PHASES

_TAIL_BYTES=65536
_FINALIZE_SECONDS=2
_V2_DETAILS_BYTES=32768
_V2_DETAIL_FIELDS=frozenset('''operations python_observed psycopg_observed package_reproducibility_claimed
 connection_or_configuration_error observed_at forward_acceptance hosted_capacity_verified
 access artifact admission migrations runtime http role rls_tables exact_policy_count public_and_browser_denied
 schema_create attestation_write controls_write effective_inherited_privileges_checked files_verified verified_files
 reference_files_unchanged reference_parent_release reference_files release_sha256 manifest_sha256 source_commit
 configuration_sha256 deployment_attestation_id controls_id admitted_at status count bundle_sha256 file_hashes
 historical_applied_release_preserved owner coordinator_fence runtime_started_event runtime_started_at progress_at
 state session_id sources calendar universe batch_id received payload_sha256 independent_readback controls snapshot_id
 research_observations entries_held main_alerts positive9_alerts broker_execution source_limitation authenticated_html
 catalogue_rules serving_release scope projection_as_of observed_status observed_progress observed_coverage
 http_200_is_operational_acceptance browser_rendering_verified reason operational_verified last_cutoff published_at
 lag_seconds freshness_status streams main positive9 current_cutoff evaluated expected_securities last_completed_cutoff
 cutoffs completed expected missed pending blocked failed registered unregistered roster_complete overdue_pending
 securities basis evaluations true false unavailable total unit factor_evaluations recovery receipts'''.split())
_V2_DETAIL_TEXT={
 'operations':{'READ_ONLY'}, 'role':{'mdl_web_20260824'}, 'scope':{'research'},
 'status':{'qualified','deployed','warmup','not_scheduled','degraded','complete','observing','completed',
           'pending','blocked','failed','missed','not_due'}, 'freshness_status':{'unknown','recorded'},
 'unit':{'rule-security-cutoff decisions'},
 'basis':{'Minimum coverage across the current due cutoff of both streams',
          'Original receipts only; recovery is separate',
          'Recovery receipts preserve the original terminal cutoff status'},
 'source_limitation':{'Bound source payloads and independent readback only; factor completeness and forward acceptance remain unverified.'},
 'reason':{'No session has been registered in the requested scope. Operational readiness is unknown.',
           'The registered calendar has no expected cutoffs.', 'The registered market session has not opened.',
           'No original evaluation receipt exists for this session.',
           'The original cutoff roster completed; operational certification is a separate evidence requirement.',
           'The scanner ownership lease is absent or expired. Completed receipts are retained.',
           'Original receipts are being recorded. Full operational verification has not been established by this projection.'},
}


def _safe_v2_detail(value,key='',depth=0):
    """Only documented counters, hashes and fixed descriptions can be retained."""
    if depth>9:return False
    if isinstance(value,dict):
        return len(value)<=64 and all(type(k) is str and
            (re.fullmatch(r'00[1-7]_[a-z_]+\.sql',k) if key=='file_hashes' else k in _V2_DETAIL_FIELDS)
            and _safe_v2_detail(v,'sha256' if key=='file_hashes' else k,depth+1) for k,v in value.items())
    if value is None or type(value) is bool:return True
    if type(value) in (int,float):return math.isfinite(value) and abs(value)<=1e18
    if type(value) is not str or len(value)>512:return False
    if key in _V2_DETAIL_TEXT:
        return value in _V2_DETAIL_TEXT[key] or (key=='reason' and re.fullmatch(
            r'[0-9]{1,18} failed, overdue or unregistered cutoff units remain visible\. Recovery does not erase the original result\.',value) is not None)
    if key in {'python_observed','psycopg_observed'}:return re.fullmatch(r'[0-9][0-9A-Za-z.+-]{0,63}',value) is not None
    if key in {'state','connection_or_configuration_error'}:return re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}',value) is not None
    if key in {'last_cutoff','published_at','current_cutoff','last_completed_cutoff','projection_as_of'}:
        return re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)',value) is not None
    size=40 if key=='source_commit' else 32 if key in {'owner','runtime_started_event'} else 64
    return (key.endswith('sha256') or key in {'reference_parent_release','release_sha256','serving_release',
        'deployment_attestation_id','controls_id','snapshot_id','batch_id','session_id','owner','runtime_started_event','source_commit'}) and re.fullmatch('[a-f0-9]{'+str(size)+'}',value) is not None


def _v2_details(output,proof):
    """Retain one bounded parsed object bound to the already validated nonce proof."""
    raw=output if isinstance(output,bytes) else str(output).encode()
    prefix=b'EQ17_V2_HOSTED_DETAILS '
    lines=[line[len(prefix):] for line in raw.splitlines() if line.startswith(prefix)]
    if len(lines)!=1 or len(lines[0])>_V2_DETAILS_BYTES:return None
    def unique(pairs):
        if len(pairs)!=len(dict(pairs)):raise ValueError('Duplicate detail key')
        return dict(pairs)
    try:
        value=json.loads(lines[0],object_pairs_hook=unique)
        encoded=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        if (not isinstance(value,dict) or value.get('operations')!='READ_ONLY' or not _safe_v2_detail(value)
                or len(encoded)>_V2_DETAILS_BYTES or hashlib.sha256(encoded).hexdigest()!=proof['evidence_sha256']):return None
        if proof['passed'] and (any(not isinstance(value.get(key),dict) or not value[key] for key in
            ('access','artifact','admission','migrations','runtime','http'))
            or any(key not in value for key in ('python_observed','psycopg_observed','observed_at',
                'package_reproducibility_claimed','forward_acceptance','hosted_capacity_verified'))):return None
        return value
    except (ValueError,TypeError,KeyError,RecursionError,OverflowError):return None
_SAFE_ERRORS=frozenset({
    'AssertionError','ImportError','KeyError','MemoryError','ModuleNotFoundError',
    'OSError','OperationalError','PermissionError','RuntimeError','SyntaxError',
    'TimeoutError','TypeError','ValueError',
})
_SAFE_HANDOVER_REASONS=frozenset({
    'ACTIVATION_OWNER_LOST','CURRENT_RELEASE_OWNER_UNAVAILABLE',
    'Current-release owner unavailable for control baseline',
    'Current owner warmup did not complete',
    'Fresh current-owner identity source unavailable',
    'Current-release owner and full fresh source response unavailable',
    'Could not capture actual receipt-cutoff snapshot',
    'SCANNER_OWNERSHIP_LOST',
})


class _Capture:
    def __init__(self,limit=_TAIL_BYTES):
        self.limit=limit;self.tail=deque();self.tail_bytes=0
        self.total=0;self.digest=hashlib.sha256()

    def add(self,chunk):
        self.total+=len(chunk);self.digest.update(chunk);self.tail.append(chunk);self.tail_bytes+=len(chunk)
        while self.tail_bytes>self.limit and self.tail:
            first=self.tail.popleft();excess=self.tail_bytes-self.limit
            if len(first)>excess:
                first=first[excess:];self.tail.appendleft(first);self.tail_bytes-=excess;break
            self.tail_bytes-=len(first)

    def value(self):return b''.join(self.tail)


_PUBLIC_HTTP_ROUTES=frozenset(('/api/positive9','/api/positive9/control','/api/coverage','/api/delivery-test','/api/higher-observation','/api/login','/api/outcome-recovery','/api/macro-source','/api/identity-requirement','/api/latency','/api/status'))
_REPORT_ROUTES=frozenset(('P9_LIVE','P9_TEST','P9_REPLAY'))
_REPORT_PHASES=('connect','transaction','status','positions','events','totals','queue','state','other_read','finish')
_REPORT_CAUSES=('deadline','connect_timeout','pool_limit','statement_cancel','database_unavailable','driver')
_REPORT_BUCKETS=('under_1s','1_to_3s','3_to_8s','8_to_16s','16s_or_more')

def _report_detail(size,hash_value):
    for phase in _REPORT_PHASES:
        for cause in _REPORT_CAUSES:
            for elapsed in _REPORT_BUCKETS:
                detail={'phase':phase,'cause':cause,'elapsed':elapsed}
                raw=json.dumps({'error':'Live report database temporarily unavailable','diagnostic':detail},sort_keys=True,separators=(',',':')).encode()
                if len(raw)==size and hashlib.sha256(raw).hexdigest()==hash_value:return detail
    return None

def _report_http(output,nonce):
    if not isinstance(nonce,str) or not re.fullmatch('[a-f0-9]{32}',nonce):return None
    pattern=rb'^EQ17_VERIFIER_REPORT_HTTP '+nonce.encode()+rb' (\{[^\r\n]{1,384}\})$'
    prefix=b'EQ17_VERIFIER_REPORT_HTTP '+nonce.encode()+b' '
    lines=[line for line in output.splitlines() if line.startswith(prefix)]
    if len(lines)!=1:return None
    matches=re.findall(pattern,lines[0])
    if len(matches)!=1:return None
    try:
        # Duplicate keys are ambiguous even when the last value is syntactically valid.
        def unique(pairs):
            if len(pairs)!=len(dict(pairs)):raise ValueError('duplicate')
            return dict(pairs)
        v=json.loads(matches[0],object_pairs_hook=unique)
        if set(v)!={'route','status','body_prefix_bytes','body_prefix_sha256','body_truncated','retried'}:return None
        route=v['route'];status=v['status'];size=v['body_prefix_bytes'];digest=v['body_prefix_sha256']
        if (not isinstance(route,str) or route not in _REPORT_ROUTES or type(status) is not int or not 400<=status<=599
                or type(size) is not int or not 0<=size<=2048 or not isinstance(digest,str) or not re.fullmatch('[a-f0-9]{64}',digest)
                or type(v['body_truncated']) is not bool or v['retried'] is not False):return None
        result={'path':'/api/positive9',**v}
        if route=='P9_LIVE' and status==503 and not v['body_truncated']:
            detail=_report_detail(size,digest)
            if detail:result['report_diagnostic']=detail
        return result
    except (TypeError,ValueError,KeyError):return None

def _diagnostic(output,total=None,digest=None,nonce=None):
    raw=output if isinstance(output,bytes) else str(output or '').encode('utf-8','replace')
    text=raw.decode('utf-8','replace');tests=[int(x) for x in re.findall(r'Ran (\d+) tests?',text)]
    frames=re.findall(r'File "[^"]*?([^/\\"]+)", line (\d+), in ([A-Za-z_][A-Za-z0-9_]*)',text)
    errors=[x for x in re.findall(r'^([A-Za-z][A-Za-z0-9_]*):',text,re.MULTILINE) if x in _SAFE_ERRORS]
    handover={};markers=re.findall(r'(?m)^EQ17_VERIFIER_RETRYABLE (\{[^\n]*\})$',text)
    if markers:
        try:
            candidate=json.loads(markers[-1]);reason=candidate.get('reason');error=candidate.get('error');location=candidate.get('failure_location')
            if reason in _SAFE_HANDOVER_REASONS:handover['handover_reason']=reason
            if error in _SAFE_ERRORS:handover['failure_class']=error.upper()
            if (isinstance(location,dict) and re.fullmatch(r'[A-Za-z0-9_.-]+',str(location.get('file','')))
                and isinstance(location.get('line'),int) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',str(location.get('function','')))):
                handover['failure_location']={'file':location['file'],'line':location['line'],'function':location['function']}
        except (TypeError,ValueError,json.JSONDecodeError):pass
    http_failure=_report_http(raw,nonce);markers=re.findall(r'(?m)^EQ17_VERIFIER_HTTP (\{[^\n]*\})$',text)
    if http_failure is None and markers:
        try:
            candidate=json.loads(markers[-1]);path=candidate.get('path');status=candidate.get('status');size=candidate.get('body_prefix_bytes');digest_value=candidate.get('body_prefix_sha256')
            if (isinstance(path,str) and path in _PUBLIC_HTTP_ROUTES
                and isinstance(status,int) and 100<=status<=599 and isinstance(size,int) and 0<=size<=2048
                and isinstance(digest_value,str) and re.fullmatch(r'[0-9a-f]{64}',digest_value)
                and isinstance(candidate.get('body_truncated'),bool) and candidate.get('retried') is False):
                http_failure={k:candidate[k] for k in ('path','status','body_prefix_bytes','body_prefix_sha256','body_truncated','retried')}
        except (TypeError,ValueError,json.JSONDecodeError):pass
    if http_failure is None:
        markers=re.findall(r'(?m)^EQ17_VERIFIER_HTTP_FINAL (\{[^\n]*\})$',text)
        if markers:
            try:
                candidate=json.loads(markers[-1]);path=candidate.get('path');status=candidate.get('status');stage=candidate.get('stage')
                if (isinstance(path,str) and path in _PUBLIC_HTTP_ROUTES
                    and isinstance(status,int) and 100<=status<=599
                    and isinstance(stage,str) and re.fullmatch(r'[A-Z0-9_]+',stage)):
                    http_failure={'path':path,'status':status,'stage':stage}
            except (TypeError,ValueError,json.JSONDecodeError):pass
    return dict(
        output_bytes=int(len(raw) if total is None else total),
        output_sha256=digest or hashlib.sha256(raw).hexdigest(),
        output_tail_truncated=(total is not None and total>len(raw)),
        tests=max(tests,default=None),suite_ok=bool(re.search(r'(?m)^OK$',text)),
        failure_class=handover.get('failure_class') or (errors[-1].upper() if errors else None),
        failure_location=handover.get('failure_location') or (dict(file=frames[-1][0],line=int(frames[-1][1]),function=frames[-1][2]) if frames else None),
        handover_reason=handover.get('handover_reason'),
        http_failure=http_failure,
    )


def _verifier_caller(output, nonce):
    # Only the child with this invocation's nonce can supplement the standard traceback.
    pattern = rb'^EQ17_VERIFIER_CALLER ' + nonce.encode('ascii') + rb' ([0-9]{1,2}) ([1-9][0-9]{0,4})$'
    frames = re.findall(pattern, output, re.MULTILINE) if isinstance(output, bytes) else []
    if len(frames) != 1:
        return None
    index, line = map(int, frames[0])
    if index >= len(CALLER_FRAMES):
        return None
    filename, function = CALLER_FRAMES[index]
    return dict(file=filename, line=line, function=function)


def _run(command,cwd,env,timeout):
    process=subprocess.Popen(command,cwd=cwd,env=env,stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,start_new_session=True)
    capture=_Capture()
    def drain():
        for chunk in iter(lambda:process.stdout.read(4096),b''):capture.add(chunk)
    reader=threading.Thread(target=drain,daemon=True);reader.start()
    try:code=process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
        process.wait(timeout=_FINALIZE_SECONDS);reader.join(timeout=_FINALIZE_SECONDS)
        raise
    reader.join(timeout=_FINALIZE_SECONDS)
    if reader.is_alive():
        try:os.killpg(process.pid,signal.SIGKILL)
        except ProcessLookupError:pass
        reader.join(timeout=_FINALIZE_SECONDS)
        if process.stdout:process.stdout.close()
        raise subprocess.TimeoutExpired(command,timeout)
    return subprocess.CompletedProcess(command,code,stdout=capture.value()),capture


def verify(command,cwd,env,stop,record,runner=None,clock=time.monotonic,*,require_v2_proof=False):
    """Run exactly once; owner/source handover is reported, never retried here."""
    if stop.is_set():record(dict(status='stopped',attempt=1,reason='STOP_CONTROL'));return False
    deadline=clock()+720;record(dict(status='running',attempt=1,maximum_seconds=720))
    nonce=secrets.token_hex(16);env={**env,'EQ17_VERIFIER_CALLER_NONCE':nonce}
    try:
        if runner is None:result,capture=_run(command,cwd,env,720)
        else:
            result=runner(command,cwd=cwd,env=env,stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,timeout=720)
            capture=None
    except subprocess.TimeoutExpired:
        record(dict(status='blocked',attempt=1,reason='VERIFIER_TIMEOUT',elapsed_seconds=max(0,720-(deadline-clock()))));return False
    diagnostic=_diagnostic(result.stdout,total=capture.total if capture else None,
        digest=capture.digest.hexdigest() if capture else None,nonce=nonce)
    if require_v2_proof:
        from app.eq17_v2_supervision import software_proof
        proof=software_proof(result.stdout,nonce,env.get('EQ17_RELEASE_SHA256'),
                             env.get('RENDER_GIT_COMMIT',env.get('EQ17_SOURCE_COMMIT')))
        if proof is None or (result.returncode==0 and proof['passed'] is not True):
            record(dict(status='blocked',attempt=1,reason='V2_SOFTWARE_PROOF_MISSING_OR_INVALID',
                        returncode=int(result.returncode),**diagnostic));return False
        diagnostic['v2_software_proof']=proof
        details=_v2_details(result.stdout,proof)
        if details is None:
            record(dict(status='blocked',attempt=1,reason='V2_SOFTWARE_DETAILS_MISSING_OR_INVALID',
                        returncode=int(result.returncode),**diagnostic));return False
        diagnostic['v2_software_details']=details
    if result.returncode==0:
        record(dict(status='complete',attempt=1,returncode=0,**diagnostic));return True
    caller=_verifier_caller(result.stdout, nonce) if diagnostic['failure_class']=='TIMEOUTERROR' else None
    if caller is not None:
        diagnostic['verifier_failure_location']=caller
        if caller['file']=='positive9_verify_recovery.py' and caller['function']=='get':
            phase=HTTP_TIMEOUT_PHASES.get(caller['line'])
            if phase:diagnostic['verifier_timeout_phase']=phase
    reason='OWNER_OR_SOURCE_HANDOVER' if result.returncode==75 else 'EXECUTED_CHECK_FAILED'
    record(dict(status='blocked',attempt=1,reason=reason,returncode=int(result.returncode),**diagnostic));return False


