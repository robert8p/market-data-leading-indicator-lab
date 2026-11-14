"""Read-only v2 supervisor bindings. No rule data or qualification authority."""
from datetime import datetime, timedelta
import hashlib
import json
import math
import re
from zoneinfo import ZoneInfo
import zlib

ENTRYPOINTS={'runtime':'v2.production.main','dashboard':'v2.entrypoints.server_main'}
REQUIRED_PROOF=('software_chain','migration_hashes_verified','runtime_readiness',
                'access_controls','authenticated_http','controls_preserved')
UNCLAIMED=('forward_acceptance','hosted_capacity_verified','source_completeness_verified','trading_authority_granted')
MAX_SOURCE_BYTES=8*1024*1024


def _hash(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _sha(value,length=64):
    return type(value) is str and re.fullmatch('[a-f0-9]{'+str(length)+'}',value) is not None


def _finite(value):
    return type(value) in (int,float) and math.isfinite(value)


def artifact_mode(artifact):
    """Called only after the exact outer release and all member pins validate."""
    if 'v2_entrypoints' not in artifact:
        return 'legacy'
    if artifact['v2_entrypoints']!=ENTRYPOINTS or not all(
            name in artifact.get('files',{}) for name in ('v2/production.py','v2/entrypoints.py','v2/hosted_verify.py')):
        raise ValueError('V2_SUPERVISOR_METADATA_MISMATCH')
    return 'v2'


def _source(row,kind,owner,fence,now):
    if not row or len(row)!=11:
        raise ValueError('V2_SOURCE_BINDING_MISSING')
    identity,actual_kind,scope,actual_owner,actual_fence,received,digest,compressed,size,status,finished=row
    if (not _sha(identity) or actual_kind!=kind or scope!='research' or actual_owner!=owner or actual_fence!=fence
            or not _finite(received) or not 0<received<=now or status!='completed'
            or not _finite(finished) or not received<=finished<=now
            or type(size) is not int or not 0<size<=MAX_SOURCE_BYTES or not _sha(digest)):
        raise ValueError('V2_SOURCE_BINDING_MISMATCH')
    decoder=zlib.decompressobj()
    raw=decoder.decompress(bytes(compressed),size+1)
    if (len(raw)!=size or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail
            or hashlib.sha256(raw).hexdigest()!=digest):
        raise ValueError('V2_SOURCE_HASH_MISMATCH')
    value=json.loads(raw)
    if not isinstance(value,dict) or value.get('verified') is not True or value.get('complete') is not True:
        raise ValueError('V2_SOURCE_INCOMPLETE')
    return value


def v2_readiness(row,sources,receipt,release,commit,not_before):
    """Software readiness only; missing factors never become complete coverage."""
    if not row or len(row)!=9:
        return 'V2_OWNER_NOT_READY'
    now,owner,expires,runtime_raw,coordinator_owner,fence,coordinator_expires,started,heartbeat_raw=row
    if (not _finite(now) or not owner or not _finite(expires) or expires<=now
            or owner!=coordinator_owner or type(fence) is not int or fence<=0
            or not _finite(coordinator_expires) or coordinator_expires<=now
            or not _finite(started) or not not_before<=started<=now):
        return 'V2_OWNER_NOT_READY'
    try:
        runtime=json.loads(runtime_raw);heartbeat=json.loads(heartbeat_raw)
        if (runtime.get('owner')!=owner or runtime.get('release')!=release or runtime.get('source_commit')!=commit
                or runtime.get('engine')!='EQ17_V2' or heartbeat.get('owner')!=owner
                or not _finite(runtime.get('progress_at')) or not now-120<=runtime['progress_at']<=now
                or not _finite(heartbeat.get('heartbeat')) or not now-50<=heartbeat['heartbeat']<=now
                or not _finite(heartbeat.get('last_progress')) or not now-600<=heartbeat['last_progress']<=now):
            return 'V2_OWNER_NOT_READY'
        calendar_id=runtime.get('calendar_evidence_ref');universe_id=runtime.get('universe_evidence_ref')
        calendar=_source(sources.get(calendar_id),'calendar',owner,fence,now)
        universe=_source(sources.get(universe_id),'universe',owner,fence,now)
        records=calendar.get('sessions')
        if not isinstance(records,list) or not 3<=len(records)<=370:
            return 'V2_CALENDAR_NOT_READY'
        days=[]
        for record in records:
            day=datetime.fromisoformat(record['session_date']).date()
            if day.isoformat()!=record['session_date'] or type(record.get('is_trading_day')) is not bool:
                return 'V2_CALENDAR_NOT_READY'
            if not isinstance(record.get('provider_record'),dict) or _hash(record['provider_record'])!=record.get('provider_record_sha256'):
                return 'V2_CALENDAR_NOT_READY'
            if record['is_trading_day']:
                if (type(record.get('open_utc')) is not int or type(record.get('close_utc')) is not int
                        or not record['open_utc']<record['close_utc']):
                    return 'V2_CALENDAR_NOT_READY'
            elif record.get('open_utc') is not None or record.get('close_utc') is not None:
                return 'V2_CALENDAR_NOT_READY'
            days.append(day)
        if days!=[days[0]+timedelta(days=i) for i in range(len(days))]:
            return 'V2_CALENDAR_NOT_READY'
        today=datetime.fromtimestamp(now,ZoneInfo('America/New_York')).date().isoformat()
        day=next((x for x in records if x['session_date']==today),None)
        ids=universe.get('security_ids')
        if (not day or universe.get('session_date')!=today or not isinstance(ids,list) or not ids
                or any(not isinstance(x,str) or not x for x in ids) or len(set(ids))!=len(ids)
                or _hash(universe.get('identity_records',ids))!=universe.get('universe_sha256')):
            return 'V2_UNIVERSE_NOT_READY'
        if runtime.get('state')=='V2_CALENDAR_WAIT':
            closed=(not day['is_trading_day'] or now<day['open_utc']-3600 or now>=day['close_utc']+60)
            if (closed and runtime.get('provider_record_sha256')==day['provider_record_sha256']
                    and runtime.get('live_acceptance') is False and runtime.get('cutoff_progress') is False):
                return 'READY_V2_CALENDAR_WAIT'
            return 'V2_CALENDAR_NOT_READY'
        if runtime.get('state')=='V2_RESEARCH_HELD':
            return 'V2_RESEARCH_HELD'
        if not day['is_trading_day'] or not receipt or len(receipt)!=12:
            return 'V2_CUTOFF_NOT_READY'
        created,summary_raw,count,status,finished,cutoff,deadline,universe_hash,calendar_hash,scope,expected,session_id=receipt
        summary=json.loads(summary_raw)
        if (not _finite(created) or not now-360<=created<=now or status!='completed'
                or not _finite(finished) or not cutoff<=finished<=min(deadline,now)
                or not now-360<=cutoff<=now or count!=expected or expected!=len(ids)
                or scope!='research' or session_id!=runtime.get('session_id')
                or universe_hash!=universe.get('universe_sha256') or calendar_hash!=day['provider_record_sha256']
                or summary.get('release_sha256')!=release or summary.get('source_commit')!=commit
                or summary.get('calendar_evidence_ref')!=calendar_id or summary.get('universe_evidence_ref')!=universe_id
                or summary.get('observation_mode')!='FORWARD_PRODUCTION' or summary.get('recovery') is not False):
            return 'V2_CUTOFF_NOT_READY'
        return 'READY_V2_CURRENT_RECEIPT'
    except (ValueError,TypeError,KeyError,OverflowError,zlib.error):
        return 'V2_EVIDENCE_NOT_READY'


V2_SQL="""
SELECT extract(epoch FROM clock_timestamp())::double precision,l.owner,l.expires,r.payload,
       c.owner,c.fence,c.expires,
       (SELECT max(e.created) FROM live_eq17.operational_events e
        WHERE e.kind='runtime_started' AND e.payload::jsonb->>'owner'=l.owner
          AND e.payload::jsonb->>'release'=%s),h.payload
FROM live_eq17.owner_lease l
LEFT JOIN live_eq17.state r ON r.key='live:runtime'
LEFT JOIN live_eq17.state h ON h.key='live:owner'
LEFT JOIN live_eq17_v2.coordinator_lease c ON c.name='scanner'
WHERE l.name='scanner'
"""


def read_v2_readiness(conn,release,commit,not_before):
    row=conn.execute(V2_SQL,(release,)).fetchone()
    if not row or not row[3]:
        return 'V2_OWNER_NOT_READY'
    try:
        state=json.loads(row[3]);refs=(state.get('calendar_evidence_ref'),state.get('universe_evidence_ref'))
    except (ValueError,TypeError):
        return 'V2_EVIDENCE_NOT_READY'
    source_rows=conn.execute("""
        SELECT b.id,b.kind,b.scope,b.owner,b.fence,b.received,b.payload_hash,b.payload,b.uncompressed_bytes,c.status,c.finished
        FROM live_eq17_v2.source_batches b LEFT JOIN live_eq17_v2.source_consumers c
          ON c.batch_id=b.id AND c.consumer='independent_observer'
        WHERE b.id IN (%s,%s)
    """,refs).fetchall()
    receipt=conn.execute("""
        SELECT r.created,r.summary,r.security_count,j.status,j.finished,c.runtime_cutoff_utc,c.deadline,
               s.universe_hash,s.calendar_hash,s.scope,s.expected_securities,s.id
        FROM live_eq17_v2.evaluation_receipts r JOIN live_eq17_v2.jobs j ON j.id=r.job_id
        JOIN live_eq17_v2.expected_cutoffs c ON c.id=r.cutoff_id
        JOIN live_eq17_v2.sessions s ON s.id=c.session_id
        WHERE r.owner=%s AND r.fence=%s AND r.is_recovery=0 AND j.kind='LIVE_EVALUATION' AND s.id=%s
        ORDER BY r.created DESC LIMIT 1
    """,(row[1],row[5],state.get('session_id'))).fetchone()
    return v2_readiness(row,{r[0]:r for r in source_rows},receipt,release,commit,not_before)


def software_proof(output,nonce,release,commit):
    """Accept one invocation-bound, deliberately limited software proof."""
    if not _sha(nonce,32) or not _sha(release) or not _sha(commit,40):
        return None
    raw=output if isinstance(output,bytes) else str(output).encode()
    prefix=b'EQ17_V2_HOSTED_PROOF '+nonce.encode()+b' '
    lines=[x[len(prefix):] for x in raw.splitlines() if x.startswith(prefix)]
    if len(lines)!=1 or len(lines[0])>8192:
        return None
    def unique(pairs):
        if len(pairs)!=len(dict(pairs)):raise ValueError('Duplicate proof key')
        return dict(pairs)
    try:
        value=json.loads(lines[0],object_pairs_hook=unique)
        keys={'schema','passed','release_sha256','source_commit','configuration_sha256','evidence_sha256','failures',*REQUIRED_PROOF,*UNCLAIMED}
        if (set(value)!=keys or value['schema']!='eq17.v2.hosted-software-proof/1'
                or value['release_sha256']!=release or value['source_commit']!=commit
                or not _sha(value['configuration_sha256']) or not _sha(value['evidence_sha256'])
                or type(value['passed']) is not bool or any(type(value[x]) is not bool for x in REQUIRED_PROOF+UNCLAIMED)
                or any(value[x] for x in UNCLAIMED) or not isinstance(value['failures'],list) or len(value['failures'])>16
                or any(type(x) is not str or re.fullmatch('[A-Z][A-Z0-9_]{0,95}',x) is None for x in value['failures'])):
            return None
        if value['passed'] != (all(value[x] for x in REQUIRED_PROOF) and not value['failures']):
            return None
        return value
    except (ValueError,TypeError,KeyError):
        return None
