"""Build an exact, guard-preserving runner diagnostic image.

This patch does not resume work, change limits, grant evidence access or
substitute thread CPU for the existing process-wide CPU guard. Runtime
activation still requires matching native pins and all existing admission.
"""
from __future__ import annotations
import hashlib
from pathlib import Path

BASE_SHA256 = 'fd7e92d4b236f0d23ffdabaab68870659b03412255ffa0f9ee04025d9c5b53ab'

OLD_MEMORY = '''def memory_safe(pid=None):
    try:
        limit = int(Path('/sys/fs/cgroup/memory.max').read_text())
        used = int(Path('/sys/fs/cgroup/memory.current').read_text())
        if used > min(limit*85//100,450*1024*1024):
            return False
        if pid:
            try:
                lines=Path('/proc/%s/status'%pid).read_text().splitlines()
            except FileNotFoundError:
                return True  # Reaped child; inspect its durable receipt.
            values=[int(x.split()[1])*1024 for x in lines if x.startswith('VmRSS:')]
            if not values:
                state=next((x for x in lines if x.startswith('State:')), '')
                return bool(re.search(r'\\b[ZX]\\b', state))
            rss=values[0]
            if rss>256*1024*1024:
                return False
        return True
    except (OSError,ValueError,StopIteration):
        return False
'''

NEW_MEMORY = '''def runner_memory_observation(pid=None):
    """Expose the original memory decision; do not change its acceptance set."""
    report = dict(safe=False, reason='MEMORY_TELEMETRY_UNAVAILABLE',
                  cgroup_used_bytes=None, cgroup_limit_bytes=None,
                  cgroup_guard_bytes=None, child_rss_bytes=None)
    try:
        limit = int(Path('/sys/fs/cgroup/memory.max').read_text())
        used = int(Path('/sys/fs/cgroup/memory.current').read_text())
        ceiling = min(limit*85//100,450*1024*1024)
        report.update(cgroup_used_bytes=used,cgroup_limit_bytes=limit,cgroup_guard_bytes=ceiling)
        if used > ceiling:
            report['reason']='CGROUP_MEMORY_GUARD'
            return report
        if pid:
            try:
                lines=Path('/proc/%s/status'%pid).read_text().splitlines()
            except FileNotFoundError:
                # Preserve the old boolean behavior, but do not call this a
                # verified reap. The actual Popen return code is reported later.
                report.update(safe=True,reason='PROC_STATUS_MISSING_LEGACY_ACCEPTANCE')
                return report
            values=[int(x.split()[1])*1024 for x in lines if x.startswith('VmRSS:')]
            if not values:
                state=next((x for x in lines if x.startswith('State:')), '')
                accepted=bool(re.search(r'\\b[ZX]\\b',state))
                report.update(safe=accepted,reason='TERMINAL_STATE_WITHOUT_RSS' if accepted else 'CHILD_RSS_UNAVAILABLE')
                return report
            rss=values[0];report['child_rss_bytes']=rss
            if rss>256*1024*1024:
                report['reason']='CHILD_RSS_GUARD'
                return report
        report.update(safe=True,reason='MEMORY_SAFE')
        return report
    except (OSError,ValueError,StopIteration):
        return report


def memory_safe(pid=None):
    return runner_memory_observation(pid)['safe']


def runner_guard_observation(elapsed, process_cpu, thread_cpu, pid):
    """Same ordered guards and thresholds; thread CPU is diagnostic only."""
    report=dict(wall_seconds=elapsed,parent_process_cpu_seconds=process_cpu,
                parent_thread_cpu_seconds=thread_cpu,guard_reason=None,
                memory=None,parent_cpu_guard_scope='PROCESS_WIDE_UNCHANGED')
    if elapsed>150:
        report['guard_reason']='RUNNER_WALL_GUARD'
    elif process_cpu>3:
        report['guard_reason']='RUNNER_PARENT_PROCESS_CPU_GUARD'
    else:
        report['memory']=runner_memory_observation(pid)
        if not report['memory']['safe']:
            report['guard_reason']='RUNNER_'+report['memory']['reason']
    return report


_RUNNER_PHASES=frozenset(('MARKET_INPUTS_START','MARKET_INPUTS_READY',
    'CORRECTED_PART_FETCH_START','CORRECTED_PART_FETCH_VERIFIED','BINDING_SEGMENT_RETURNED'))


def record_runner_phase(attempt,phase,part_no=None):
    """One bounded metadata marker. No values, credentials or source payloads."""
    if not isinstance(attempt,str) or not re.fullmatch(r'[0-9a-f-]{36}',attempt):
        raise ValueError('DIAGNOSTIC_ATTEMPT_ID_REJECTED')
    if phase not in _RUNNER_PHASES or (part_no is not None and
            (type(part_no) is not int or not 0<=part_no<7590)):
        raise ValueError('DIAGNOSTIC_PHASE_REJECTED')
    value=dict(version=1,attempt_id=attempt,phase=phase,part_no=part_no,
               observed_unix_seconds=time.time(),stage_completion_claimed=False)
    try:
        atomic(ROOT/('runner_phase_'+attempt+'.json'),json.dumps(value,sort_keys=True).encode())
    except OSError:
        # Instrumentation cannot change source contents or confer a pass.
        LOG.warning('EQ20 runner phase marker unavailable')


def read_runner_phase(attempt):
    """Read only this attempt's at-most-1024-byte allowlisted metadata."""
    path=ROOT/('runner_phase_'+attempt+'.json')
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size>1024:
            return None
        with path.open('rb') as handle:
            raw=handle.read(1025)
        if len(raw)>1024:
            return None
        value=json.loads(raw)
        if not isinstance(value,dict) or set(value)!={'version','attempt_id','phase',
                'part_no','observed_unix_seconds','stage_completion_claimed'}:
            return None
        stamp=value['observed_unix_seconds'];part=value['part_no']
        if (value['version']!=1 or value['attempt_id']!=attempt or
                value['phase'] not in _RUNNER_PHASES or value['stage_completion_claimed'] is not False or
                type(stamp) not in (int,float) or not 0<stamp<1e12 or
                (part is not None and (type(part) is not int or not 0<=part<7590))):
            return None
        return value
    except (OSError,ValueError,TypeError):
        return None
'''

OLD_LOOP = '''        begin=time.monotonic()
        parent_cpu_begin=time.process_time()
        while process.poll() is None:
            time.sleep(1)
            if process.poll() is not None:
                break
            if time.monotonic()-begin>150 or time.process_time()-parent_cpu_begin>3 or not memory_safe(process.pid):
                os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10);break
            checked=rpc.call('heartbeat',owner,fence,dict(attempt_id=attempt,child_pid=process.pid))
            if not checked.get('continue'):
                os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10);break
'''
NEW_LOOP = '''        begin=time.monotonic()
        parent_cpu_begin=time.process_time()
        thread_clock=getattr(time,'thread_time',None)
        thread_cpu_begin=thread_clock() if thread_clock else None
        termination_reason=None
        observation=None
        while process.poll() is None:
            time.sleep(1)
            if process.poll() is not None:
                break
            observation=runner_guard_observation(time.monotonic()-begin,
                time.process_time()-parent_cpu_begin,
                thread_clock()-thread_cpu_begin if thread_clock else None,process.pid)
            if observation['guard_reason'] is not None:
                termination_reason=observation['guard_reason']
                os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10);break
            checked=rpc.call('heartbeat',owner,fence,dict(attempt_id=attempt,child_pid=process.pid))
            if not checked.get('continue'):
                termination_reason='RUNNER_HEARTBEAT_CONTINUATION_CLOSED'
                os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=10);break
'''

OLD_RESULT = """        if job['action']=='QA_INTERRUPT' and process.returncode==86:
"""
NEW_RESULT = """        diagnostics=dict(version=1,attempt_id=attempt,
            observed_unix_seconds=time.time(),guard_observation=observation,
            termination_reason=termination_reason or 'NO_SUPERVISOR_KILL_OBSERVED',
            exit_code=process.returncode,child_receipt_present=receipt_path.exists(),
            last_phase=read_runner_phase(attempt),historical_kill_cause_inferred=False,
            resource_limits_changed=False,thread_cpu_used_for_admission=False)
        result['supervisor_diagnostics']=diagnostics
        try:
            atomic(ROOT/('runner_diagnostic_'+attempt+'.json'),json.dumps(diagnostics,sort_keys=True).encode())
        except OSError:
            LOG.warning('EQ20 runner diagnostic file unavailable')
        if not result.get('success'):
            LOG.warning('EQ20 runner termination reason=%s exit_code=%s observation=%s',
                diagnostics['termination_reason'],process.returncode,
                json.dumps(observation,sort_keys=True))
        if job['action']=='QA_INTERRUPT' and process.returncode==86:
"""

OLD_PREPARE = '''    if action == 'PREPARE_BINDING':
        inputs = market_inputs(config)
        prepared = extension.prepare(inputs, config, auxiliary, owner, fence, attempt, ROOT)
'''
NEW_PREPARE = '''    if action == 'PREPARE_BINDING':
        record_runner_phase(attempt,'MARKET_INPUTS_START')
        inputs = market_inputs(config)
        record_runner_phase(attempt,'MARKET_INPUTS_READY')
        prepared = extension.prepare(inputs, config, auxiliary, owner, fence, attempt, ROOT)
        record_runner_phase(attempt,'BINDING_SEGMENT_RETURNED')
'''

OLD_FETCH = '''        transport = super().call
        return corrected_unit_part(
            lambda chunk: transport(op, owner, fence, dict(args, chunk_no=chunk)),
            number, self.manifest_sha256,
            ROOT/'corrected_parts'/attempt/('part_%s.jsonl'%number))
'''
NEW_FETCH = '''        transport = super().call
        record_runner_phase(attempt,'CORRECTED_PART_FETCH_START',number)
        result = corrected_unit_part(
            lambda chunk: transport(op, owner, fence, dict(args, chunk_no=chunk)),
            number, self.manifest_sha256,
            ROOT/'corrected_parts'/attempt/('part_%s.jsonl'%number))
        record_runner_phase(attempt,'CORRECTED_PART_FETCH_VERIFIED',number)
        return result
'''

REPLACEMENTS=((OLD_MEMORY,NEW_MEMORY),(OLD_LOOP,NEW_LOOP),
              (OLD_RESULT,NEW_RESULT),(OLD_PREPARE,NEW_PREPARE),(OLD_FETCH,NEW_FETCH))


def patched_bytes(original: bytes) -> bytes:
    if hashlib.sha256(original).hexdigest()!=BASE_SHA256:
        raise ValueError('EXACT_RUNNER_DIAGNOSTIC_PREDECESSOR_REQUIRED')
    text=original.decode('utf8')
    for before,after in REPLACEMENTS:
        if text.count(before)!=1:
            raise ValueError('EXACT_SINGLE_DIAGNOSTIC_PATCH_SITE_REQUIRED')
        text=text.replace(before,after,1)
    restored=text
    for before,after in reversed(REPLACEMENTS):
        if restored.count(after)!=1:
            raise ValueError('DIAGNOSTIC_PATCH_REVERSE_PROOF_FAILED')
        restored=restored.replace(after,before,1)
    if restored.encode()!=original:
        raise ValueError('UNREGISTERED_RUNNER_CHANGE_REJECTED')
    compile(text,'eq20_runner_orchestrator.py','exec')
    return text.encode('utf8')


def main() -> None:
    path=Path(__file__).resolve().parents[1]/'app'/'eq20_runner_orchestrator.py'
    raw=patched_bytes(path.read_bytes())
    path.write_bytes(raw)
    if path.read_bytes()!=raw:
        raise ValueError('RUNNER_DIAGNOSTIC_BUILD_READBACK_FAILED')
    print('EQ20_DIAGNOSTIC_RUNNER_IMAGE_SHA256='+hashlib.sha256(raw).hexdigest())


if __name__=='__main__':
    main()
