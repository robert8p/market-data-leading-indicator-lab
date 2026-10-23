"""Bounded continuation of the existing hosted verifier; never a scanner."""
import subprocess,time

def verify(command,cwd,env,stop,record,runner=subprocess.run,clock=time.monotonic):
    deadline=clock()+1200
    for attempt in range(1,4):
        remaining=deadline-clock()
        if stop.is_set() or remaining<=0:
            record(dict(status='stopped',attempt=attempt,reason='STOP_OR_DEADLINE'));return False
        record(dict(status='running',attempt=attempt,remaining_seconds=remaining))
        try:
            result=runner(command,cwd=cwd,env=env,stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,timeout=min(720,remaining))
            code=result.returncode
        except subprocess.TimeoutExpired:
            record(dict(status='blocked',attempt=attempt,reason='VERIFIER_TIMEOUT'));return False
        if code==0:
            record(dict(status='complete',attempt=attempt));return True
        # Exit 75 is emitted only for enumerated ownership/fresh-source waits.
        # A failed assertion or unknown error is never retried as transient.
        if code!=75 or attempt==3:
            record(dict(status='blocked',attempt=attempt,reason='FAILED_CHECK' if code!=75 else 'RETRY_BUDGET_EXHAUSTED',returncode=code));return False
        record(dict(status='retry_pending',attempt=attempt,reason='OWNER_OR_SOURCE_HANDOVER',returncode=code))
        if stop.wait(min(10,max(0,deadline-clock()))):
            record(dict(status='stopped',attempt=attempt,reason='STOP_CONTROL'));return False
    return False
