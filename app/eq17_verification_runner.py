"""Once-only, bounded continuation of the existing hosted verifier.

The runner never stores child output.  It drains the complete stream, retains a
small in-memory tail only long enough to derive an allowlisted diagnostic, and
kills the verifier's owned process group on timeout.  This is neither a scanner
nor a second verification controller.
"""
from collections import deque
import hashlib
import json
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


def _diagnostic(output,total=None,digest=None):
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
    http_failure=None;markers=re.findall(r'(?m)^EQ17_VERIFIER_HTTP (\{[^\n]*\})$',text)
    if markers:
        try:
            candidate=json.loads(markers[-1]);path=candidate.get('path');status=candidate.get('status');size=candidate.get('body_prefix_bytes');digest_value=candidate.get('body_prefix_sha256')
            if (isinstance(path,str) and re.fullmatch(r'/api/[A-Za-z0-9_./?-]+',path)
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
                if (isinstance(path,str) and re.fullmatch(r'/api/[A-Za-z0-9_./?-]+',path)
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


def verify(command,cwd,env,stop,record,runner=None,clock=time.monotonic):
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
        digest=capture.digest.hexdigest() if capture else None)
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
