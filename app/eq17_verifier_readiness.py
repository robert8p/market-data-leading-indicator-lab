"""Read-only readiness before a new release's original once-only verifier claim.

Never write/reset a claim or transfer a scanner lease. The existing verifier's
720-second execution limit and all of its post-claim owner checks remain intact.
"""
import math
import re
import time

MAX_WAIT_SECONDS=720
READ_RESERVE_SECONDS=8
SQL="""
SELECT extract(epoch FROM clock_timestamp())::double precision,
       l.owner,l.expires,
       r.payload::jsonb->>'owner',r.payload::jsonb->>'release',r.payload::jsonb->>'state',
       w.payload::jsonb->>'owner',w.payload::jsonb->>'release',w.payload::jsonb->'finished'
FROM live_eq17.owner_lease l
LEFT JOIN live_eq17.state r ON r.key='live:runtime'
LEFT JOIN live_eq17.state w ON w.key='live:warmup_complete'
WHERE l.name='scanner'
"""

def readiness(row,release):
    if not row or len(row)!=9:return 'OWNER_NOT_READY'
    now,owner,expires,runtime_owner,runtime_release,state,warmup_owner,warmup_release,finished=row
    if type(now) not in (int,float) or type(expires) not in (int,float) or not math.isfinite(now) or not math.isfinite(expires):
        return 'OWNER_NOT_READY'
    if not owner or runtime_owner!=owner or runtime_release!=release or expires<=now:return 'OWNER_NOT_READY'
    # This preserves the existing reporting_ready contract: closed-market
    # reports do not require an intraday history preload.
    if state=='MARKET_CLOSED_DEPENDENCIES_PENDING':return 'READY_MARKET_CLOSED'
    if (warmup_owner==owner and warmup_release==release and type(finished) in (int,float)
            and math.isfinite(finished) and 0<finished<=now):return 'READY_WARMUP_COMPLETE'
    return 'WARMUP_NOT_READY'

def wait_ready(connection,release,stop,*,maximum_seconds=MAX_WAIT_SECONDS,clock=time.monotonic):
    if type(release) is not str or re.fullmatch('[a-f0-9]{64}',release) is None:raise ValueError('INVALID_RELEASE')
    if type(maximum_seconds) not in (int,float) or not math.isfinite(maximum_seconds) or not READ_RESERVE_SECONDS<=maximum_seconds<=MAX_WAIT_SECONDS:
        raise ValueError('INVALID_PREFLIGHT_BOUND')
    started=clock();deadline=started+maximum_seconds;failures=0;checks=0;reason='OWNER_NOT_READY'
    def result(status,code):return dict(status=status,reason=code,checks=checks,elapsed_seconds=max(0,clock()-started),claim_written=False)
    while not stop.is_set():
        if deadline-clock()<=READ_RESERVE_SECONDS:return result('held','READINESS_DEADLINE')
        try:
            with connection() as conn:
                conn.execute("SET LOCAL statement_timeout TO '2s'")
                row=conn.execute(SQL).fetchone()
            checks+=1;failures=0;reason=readiness(row,release)
        except Exception:
            failures+=1
            if failures>=2:return result('held','READINESS_READ_UNAVAILABLE')
            reason='READINESS_READ_UNAVAILABLE'
        if stop.is_set():return result('stopped','SHUTDOWN')
        if clock()>deadline:return result('held','READINESS_DEADLINE')
        if reason in ('READY_MARKET_CLOSED','READY_WARMUP_COMPLETE'):return result('ready',reason)
        stop.wait(min(2,max(0,deadline-clock()-READ_RESERVE_SECONDS)))
    return result('stopped','SHUTDOWN')
