"""Bounded provider capture on the existing EQ20 mission timer.

The database owns every dated job, quota permit, source revision and first-alert
CAS. The parent owns exact process quiescence and a complete I/O journal. A
successful scientific output never substitutes for the separately authenticated
population, exposure, inference, entitlement or full-horizon resource gates.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import base64
import fcntl
import hashlib
import importlib.util
import json
import logging
import math
import os
from pathlib import Path
import re
import resource
import signal
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
import zlib

VERSION='EQ20_PROSPECTIVE_CAPTURE_RUNTIME_V3'
RPC_NAME='eq20_prospective_capture_pipeline_v1'
ROOT=Path('/tmp/astra-eq20-w10/prospective_capture')
SHARED_METADATA_ROOT=Path('/tmp/astra-eq20-w10/prospective_metadata')
MAX_REQUEST=2*1024*1024
MAX_REPLY=12*1024*1024
MAX_FILE=256*1024*1024
MAX_JOURNAL=1024*1024
SQL_TAIL=3
CONTROL_LIMIT=7
ENTRYPOINT=Path(__file__).resolve()
LOG=logging.getLogger(__name__)
_lock=threading.Lock()
_stop=threading.Event()
_last_poll=0.
_last_delay=300.
_last_activation_key=None
_modules={}
_ALLOWED={'probe','status','funded_probe','claim','next_work','terminal','terminal_replay','claim_reconcile','close_conservative',
 'page_begin','page_part','page_commit','population_commit','source_prefix_batch',
 'decision_commit','capsule_begin','capsule_part','capsule_commit'}

class Closed(ValueError):
    pass

def require(value,reason):
    if not value:raise Closed(reason)

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()

def sha(raw):return hashlib.sha256(raw).hexdigest()

def object_hash(value):return sha(canonical(value))

def exact_hash(value):
    require(isinstance(value,str) and re.fullmatch(r'[0-9a-f]{64}',value),'CAPTURE_EXACT_SHA_REQUIRED')
    return value

def utc(value):
    require(isinstance(value,str),'CAPTURE_EXACT_TIMESTAMP_REQUIRED')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    require(result.tzinfo is not None,'CAPTURE_EXPLICIT_TIMEZONE_REQUIRED')
    return result.astimezone(timezone.utc)

def boot_id():return Path('/proc/sys/kernel/random/boot_id').read_text().strip()

def load(name,expected=None):
    path=Path(__file__).with_name(name+'.py')
    require(path.is_file() and (expected is None or sha(path.read_bytes())==exact_hash(expected)),
            'CAPTURE_INSTALLED_CODE_PIN_MISMATCH')
    if name not in _modules:
        spec=importlib.util.spec_from_file_location('capture_runtime_'+name,path)
        value=importlib.util.module_from_spec(spec);sys.modules[spec.name]=value;spec.loader.exec_module(value)
        _modules[name]=value
    return _modules[name]

def guards():return load('eq20_source_supervisor')

def accounting():return load('eq20_prospective_accounting_v3')

def atomic(path,value,*,immutable=True):
    raw=value if isinstance(value,bytes) else canonical(value)
    require(len(raw)<=MAX_FILE and not path.is_symlink(),'CAPTURE_PHYSICAL_FILE_OR_SYMLINK_BOUND')
    if path.exists() and immutable:
        require(path.read_bytes()==raw,'CAPTURE_IMMUTABLE_LOCAL_CONFLICT');return
    require(guards().scratch_safe(len(raw)),'EXISTING_SHARED_SCRATCH_CEILING')
    path.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temporary.open('xb') as handle:
            handle.write(raw);handle.flush();os.fsync(handle.fileno())
        os.replace(temporary,path)
        descriptor=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(descriptor)
        finally:os.close(descriptor)
    finally:temporary.unlink(missing_ok=True)

def launch(command):
    """Retain the object even if a post-Popen initialization step fails."""
    child=guards().ReapedChild.__new__(guards().ReapedChild)
    try:
        child.__init__(command)
        return child
    except BaseException as error:
        if hasattr(child,'pid'):
            child.stop()
            if getattr(child,'finished',False):
                child.launch_error=type(error).__name__
                return child
        raise

class SourceRevisionCache:
    """Evictable, hash-checked raw revisions; never an evidence authority.

    Every cached value was acknowledged by the authoritative decision CAS.
    The pipeline still authenticates its combined row-vector against the next
    native source manifest. Losing this cache merely causes a bounded rebuild.
    """
    MAX_VALUE=8*1024*1024
    DEFAULT_TOTAL=128*1024*1024

    def __init__(self,path=None,*,maximum_bytes=DEFAULT_TOTAL,scratch_safe=None):
        self.path=Path(path) if path is not None else ROOT/'source_cache'
        require(type(maximum_bytes) is int and self.MAX_VALUE<=maximum_bytes<=self.DEFAULT_TOTAL,
                'CAPTURE_CACHE_FINITE_LIMIT_REQUIRED')
        self.maximum_bytes=maximum_bytes
        self.scratch_safe=scratch_safe or guards().scratch_safe
        self.path.mkdir(parents=True,exist_ok=True,mode=0o700)
        database=self.path/'index.sqlite'
        require(not database.is_symlink() and (not database.exists() or database.stat().st_size<8*1024*1024),
                'CAPTURE_CACHE_INDEX_BOUND')
        self.db=sqlite3.connect(database,timeout=.25)
        self.db.execute('PRAGMA journal_mode=DELETE')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS entries (cache_key TEXT PRIMARY KEY, bytes INTEGER NOT NULL, used REAL NOT NULL)')
        self.db.commit()

    @staticmethod
    def key(activation,session,security):
        require(all(isinstance(x,str) and 0<len(x)<=512 for x in (activation,session,security)),
                'CAPTURE_CACHE_EXACT_SCOPE_REQUIRED')
        require(re.fullmatch(r'\d{4}-\d{2}-\d{2}',session),'CAPTURE_CACHE_SESSION_DATE')
        return object_hash([activation,session,security])

    def discard(self,key):
        (self.path/(key+'.z')).unlink(missing_ok=True)
        self.db.execute('DELETE FROM entries WHERE cache_key=?',(key,));self.db.commit()

    def load(self,activation_key,session_date,security_id):
        key=self.key(activation_key,session_date,security_id)
        path=self.path/(key+'.z')
        try:
            require(path.is_file() and not path.is_symlink() and path.stat().st_size<=self.MAX_VALUE,
                    'CAPTURE_CACHE_PAYLOAD_BOUND')
            compressed=path.read_bytes();decoder=zlib.decompressobj()
            raw=decoder.decompress(compressed,self.MAX_VALUE+1)
            require(len(raw)<=self.MAX_VALUE and decoder.eof and not decoder.unused_data and not decoder.unconsumed_tail,
                    'CAPTURE_CACHE_DECODE_BOUND')
            envelope=json.loads(raw)
            require(envelope.get('scope')==[activation_key,session_date,security_id]
                    and envelope.get('value_sha256')==object_hash(envelope.get('value')),
                    'CAPTURE_CACHE_HASH_OR_SCOPE_MISMATCH')
            self.db.execute('UPDATE entries SET used=? WHERE cache_key=?',(time.time(),key));self.db.commit()
            return envelope['value']
        except (OSError,ValueError,zlib.error,Closed):
            self.discard(key);return None

    def store(self,activation_key,session_date,security_id,value):
        key=self.key(activation_key,session_date,security_id)
        require(isinstance(value,dict) and type(value.get('source_sequence')) is int
                and value['source_sequence']>=0 and type(value.get('decision_index')) is int
                and value['decision_index']>=0 and isinstance(value.get('source_rows'),list),
                'CAPTURE_CACHE_ACKNOWLEDGED_PREFIX_SCHEMA')
        exact_hash(value.get('source_rows_sha256'));exact_hash(value.get('raw_prefix_sha256'))
        for row in value['source_rows']:
            require(isinstance(row,dict) and isinstance(row.get('row_evidence_text'),str)
                    and sha(row['row_evidence_text'].encode())==exact_hash(row.get('row_sha256')),
                    'CAPTURE_CACHE_EXACT_RAW_REVISION_HASH')
        raw=canonical({'scope':[activation_key,session_date,security_id],
                       'value_sha256':object_hash(value),'value':value})
        if len(raw)>self.MAX_VALUE:return False
        payload=zlib.compress(raw,1)
        previous=self.db.execute('SELECT bytes FROM entries WHERE cache_key=?',(key,)).fetchone()
        total=self.db.execute('SELECT coalesce(sum(bytes),0) FROM entries').fetchone()[0]
        needed=max(0,len(payload)-(previous[0] if previous else 0))
        while total+needed>self.maximum_bytes:
            victim=self.db.execute('SELECT cache_key,bytes FROM entries WHERE cache_key<>? ORDER BY used LIMIT 1',(key,)).fetchone()
            if victim is None:return False
            self.discard(victim[0]);total-=victim[1]
        if not self.scratch_safe(len(payload)+2*1024*1024):return False
        atomic(self.path/(key+'.z'),payload,immutable=False)
        self.db.execute('INSERT INTO entries(cache_key,bytes,used) VALUES(?,?,?) ON CONFLICT(cache_key) DO UPDATE SET bytes=excluded.bytes,used=excluded.used',
                        (key,len(payload),time.time()));self.db.commit()
        return True

    def close(self):self.db.close()


class PendingPageCache:
    """Exact first-received bodies until their immutable page commit ACK.

    A missing cache entry is never authority to replace a partly uploaded page
    with newly fetched bytes. The pipeline must reconcile that immutable page
    or retain the identified gap. There is at most one active producer slice.
    """
    MAX_RAW=2*1024*1024
    MAX_PENDING=16

    def __init__(self,path=None):
        self.path=Path(path) if path is not None else ROOT/'pending_pages'
        self.path.mkdir(parents=True,exist_ok=True,mode=0o700)

    def load(self,request_sha256):
        exact_hash(request_sha256);path=self.path/(request_sha256+'.json')
        if not path.exists():return None
        value=read(path,3*self.MAX_RAW)
        require(value.get('request_sha256')==request_sha256 and isinstance(value.get('receipt'),dict)
                and value.get('receipt_sha256')==object_hash(value['receipt']),
                'EXACT_PENDING_PROVIDER_PAGE_RECEIPT_REQUIRED')
        receipt=value['receipt'];raw=base64.b64decode(receipt['raw_payload_base64'],validate=True)
        require(len(raw)<=self.MAX_RAW and receipt.get('raw_bytes')==len(raw)
                and receipt.get('raw_sha256')==sha(raw)
                and receipt.get('request_sha256')==request_sha256,
                'EXACT_PENDING_PROVIDER_PAGE_BYTES_REQUIRED')
        payload=json.loads(raw,parse_constant=lambda x:{'untrusted_nonfinite_json_literal':x})
        return receipt,payload

    def store(self,request_sha256,receipt,payload=None):
        exact_hash(request_sha256)
        require(isinstance(receipt,dict) and receipt.get('request_sha256')==request_sha256,
                'PENDING_PAGE_EXACT_REQUEST_REQUIRED')
        raw=base64.b64decode(receipt.get('raw_payload_base64',''),validate=True)
        require(0<len(raw)<=self.MAX_RAW and receipt.get('raw_bytes')==len(raw)
                and receipt.get('raw_sha256')==sha(raw),'PENDING_PAGE_RAW_READBACK_REQUIRED')
        path=self.path/(request_sha256+'.json')
        require(path.exists() or len(list(self.path.glob('*.json')))<self.MAX_PENDING,
                'FINITE_PENDING_PROVIDER_PAGE_CACHE_EXHAUSTED')
        atomic(path,{'request_sha256':request_sha256,'receipt_sha256':object_hash(receipt),'receipt':receipt})
        self.load(request_sha256)
        return True

    def remove(self,request_sha256):
        exact_hash(request_sha256)
        (self.path/(request_sha256+'.json')).unlink(missing_ok=True)

    def pending(self):
        paths=sorted(self.path.glob('*.json'))
        require(len(paths)<=self.MAX_PENDING,'FINITE_PENDING_PROVIDER_PAGE_CACHE_EXHAUSTED')
        result=[]
        for path in paths:
            receipt,_=self.load(path.stem)
            result.append({'request_sha256':path.stem,'raw_sha256':receipt['raw_sha256']})
        return result

    def acknowledge(self,requested,response):
        require(isinstance(response,list) and len(response)<=self.MAX_PENDING,
                'BOUNDED_PROVIDER_PAGE_COMMIT_READBACK_REQUIRED')
        pairs={(item['request_sha256'],item['raw_sha256']) for item in requested}
        observed=set()
        for item in response:
            require(isinstance(item,dict),'EXACT_PROVIDER_PAGE_COMMIT_READBACK_REQUIRED')
            pair=(exact_hash(item.get('request_sha256')),exact_hash(item.get('raw_sha256')))
            require(pair in pairs and pair not in observed,'PROVIDER_PAGE_COMMIT_READBACK_SCOPE_MISMATCH')
            observed.add(pair)
        for request_sha,_ in observed:self.remove(request_sha)


class PendingDecisionObservations:
    """Keep actually observed first-alert ACKs until native receipt readback."""
    MAX_PENDING=64
    MAX_OBSERVATION=512*1024
    MAX_PAGE=512*1024

    def __init__(self,path=None):
        self.path=Path(path) if path is not None else ROOT/'decision_observations'
        self.path.mkdir(parents=True,exist_ok=True,mode=0o700)

    def store(self,observation):
        require(isinstance(observation,dict)
                and observation.get('version')=='EQ20_CAPTURE_DECISION_COMMIT_OBSERVATION_V1'
                and len(canonical(observation))<=self.MAX_OBSERVATION,
                'BOUNDED_ACTUAL_DECISION_ACK_OBSERVATION_REQUIRED')
        key=exact_hash(observation.get('observation_sha256'))
        require(key==object_hash({k:v for k,v in observation.items() if k!='observation_sha256'}),
                'EXACT_DECISION_ACK_OBSERVATION_HASH_REQUIRED')
        path=self.path/(key+'.json')
        require(path.exists() or len(list(self.path.glob('*.json')))<self.MAX_PENDING,
                'FINITE_PENDING_DECISION_ACK_QUEUE_EXHAUSTED')
        atomic(path,observation)
        return key

    def pending(self,activation_key):
        paths=sorted(self.path.glob('*.json'))
        require(len(paths)<=self.MAX_PENDING,'FINITE_PENDING_DECISION_ACK_QUEUE_EXHAUSTED')
        result=[];size=2
        for path in paths:
            value=read(path,self.MAX_OBSERVATION)
            require(value.get('observation_sha256')==path.stem
                    and path.stem==object_hash({k:v for k,v in value.items() if k!='observation_sha256'}),
                    'EXACT_DECISION_ACK_OBSERVATION_HASH_REQUIRED')
            if value.get('activation_key')!=activation_key:continue
            length=len(canonical(value))+1
            if result and size+length>self.MAX_PAGE:break
            require(size+length<=self.MAX_PAGE,'ONE_DECISION_ACK_PIGGYBACK_BOUND')
            result.append(value);size+=length
        return result

    def acknowledge(self,requested,response):
        require(isinstance(response,list) and len(response)<=self.MAX_PENDING,
                'BOUNDED_DECISION_OBSERVATION_NATIVE_ACK_REQUIRED')
        keys={x['observation_sha256'] for x in requested};seen=set()
        for item in response:
            require(isinstance(item,dict),'ACTUAL_DECISION_OBSERVATION_NATIVE_ACK_REQUIRED')
            key=exact_hash(item.get('observation_sha256'))
            require(key in keys and key not in seen,'DECISION_OBSERVATION_NATIVE_ACK_SCOPE_MISMATCH')
            seen.add(key)
        for key in seen:(self.path/(key+'.json')).unlink(missing_ok=True)

def read(path,maximum=MAX_JOURNAL):
    require(path.is_file() and not path.is_symlink() and 0<path.stat().st_size<=maximum,
            'CAPTURE_DURABLE_RECEIPT_BOUND')
    result=json.loads(path.read_bytes());require(isinstance(result,dict),'CAPTURE_DURABLE_OBJECT_REQUIRED')
    return result

def stopping():
    mission=sys.modules.get('app.eq20_mission_continuation')
    return _stop.is_set() or (mission is not None and getattr(mission,'_stop',_stop).is_set())

def has_pending_local_cycle():
    return (ROOT/'active_cycle.json').exists() or (ROOT/'carry.json').exists()


def metadata_pool_stopped(activation_key):
    require(isinstance(activation_key,str) and 0<len(activation_key)<=256,'ACTUAL_METADATA_ACTIVATION_SCOPE_REQUIRED')
    path=SHARED_METADATA_ROOT/(object_hash(activation_key)+'.json')
    if not path.exists():return False
    state=read(path)
    require(state.get('activation_key')==activation_key,'SHARED_METADATA_STOP_SCOPE_MISMATCH')
    return state.get('metadata_pool_exhausted') is True or state.get('remaining_normal_probe_starts')==0


def record_metadata_ack(activation_key,ack):
    require(isinstance(ack,dict) and ack.get('funded') is True
            and ack.get('activation_key')==activation_key,'ACTUAL_SCOPED_METADATA_FUNDING_ACK_REQUIRED')
    remaining=ack.get('remaining_normal_probe_starts')
    require(type(remaining) is int and 0<=remaining<=318 and type(ack.get('metadata_pool_exhausted')) is bool,
            'ACTUAL_FINITE_METADATA_POOL_BALANCE_REQUIRED')
    path=SHARED_METADATA_ROOT/(object_hash(activation_key)+'.json')
    prior=read(path) if path.exists() else {}
    if prior:
        require(prior.get('activation_key')==activation_key,'SHARED_METADATA_STOP_SCOPE_MISMATCH')
        remaining=min(remaining,prior['remaining_normal_probe_starts'])
    value={'activation_key':activation_key,'remaining_normal_probe_starts':remaining,
        'metadata_pool_exhausted':ack['metadata_pool_exhausted'] or remaining==0 or prior.get('metadata_pool_exhausted',False),
        'actual_observed_ack':ack,'actual_observed_ack_sha256':object_hash(ack),'prior_charges_or_stops_reset':False}
    atomic(path,value,immutable=False)
    return value


def probe_schedule_path(activation_key):
    return ROOT/'probe_schedules'/(object_hash(activation_key)+'.json')


def peer_has_pending_cycle():
    # The two adapters deliberately share one foreground owner and finite pool.
    # An initial peer probe must not close a healthy local measured carry.
    name='prospective_consumer_v3' if RPC_NAME=='eq20_prospective_capture_pipeline_v1' else 'prospective_capture'
    peer=ROOT.parent/name
    return (peer/'active_cycle.json').exists() or (peer/'carry.json').exists()


def record_probe_schedule(activation_key,probe):
    """Persist the actual funded schedule without treating it as admission proof."""
    path=probe_schedule_path(activation_key)
    prior=read(path) if path.exists() else {}
    require(not prior or (prior.get('activation_key')==activation_key
            and prior.get('rpc_name')==RPC_NAME),'CAPTURE_PERSISTED_SCHEDULE_SCOPE')
    counts=prior.get('dependency_observations',{})
    require(isinstance(counts,dict) and len(counts)<=320
            and all(re.fullmatch('[0-9a-f]{64}',key) and type(value) is int and 1<=value<=3
                    for key,value in counts.items()),'CAPTURE_FINITE_DEPENDENCY_HISTORY')
    schedule={'version':'EQ20_FUNDED_PROBE_SCHEDULE_V1','activation_key':activation_key,
        'rpc_name':RPC_NAME,'response_sha256':object_hash(probe),
        'metadata_accounting_ack':probe.get('metadata_accounting_ack'),
        'dependency_observations':dict(counts),'host_instance':socket.gethostname(),
        'boot_id':boot_id(),'received_monotonic':time.monotonic(),
        'received_at':datetime.now(timezone.utc).isoformat(),'stopped':False}
    try:
        ack=probe.get('metadata_accounting_ack',{})
        require(ack.get('funded') is True and ack.get('activation_key')==activation_key,
                'ACTUAL_FUNDED_SCHEDULE_SCOPE_REQUIRED')
        basis=probe.get('schedule_basis')
        require(basis in ('REGISTERED_OFFICIAL_SESSION','FINITE_DEPENDENCY_RETRY'),
                'ACTUAL_NATIVE_PROBE_SCHEDULE_REQUIRED')
        due=utc(probe.get('next_due_at'));server=utc(probe.get('server_time'))
        delay=(due-server).total_seconds()
        require(0<delay<=370*86400,'BOUNDED_ACTUAL_NATIVE_NEXT_DUE_REQUIRED')
        schedule.update(schedule_basis=basis,next_due_at=due.isoformat(),
            server_time=server.isoformat(),next_monotonic=schedule['received_monotonic']+delay,
            next_poll_seconds=delay)
        if basis=='FINITE_DEPENDENCY_RETRY':
            fingerprint=exact_hash(probe.get('dependency_fingerprint'))
            maximum=probe.get('maximum_dependency_probe_retries')
            require(type(maximum) is int and 0<=maximum<=2,'FINITE_NATIVE_DEPENDENCY_RETRY_BOUND')
            observed=min(3,counts.get(fingerprint,0)+1)
            schedule['dependency_observations'][fingerprint]=observed
            require(len(schedule['dependency_observations'])<=320,'FINITE_DEPENDENCY_HISTORY_EXHAUSTED')
            schedule.update(dependency_fingerprint=fingerprint,
                maximum_dependency_probe_retries=maximum,stopped=observed>maximum,
                stop_reason='FINITE_UNCHANGED_DEPENDENCY_PROBE_RETRIES_EXHAUSTED' if observed>maximum else None)
    except Exception as error:
        # An acknowledged but malformed no-work schedule cannot turn into an
        # unbounded sequence of newly funded probes after restart.
        schedule.update(stopped=True,stop_reason='ACTUAL_NATIVE_PROBE_SCHEDULE_INVALID',
                        schedule_error=str(error)[:180])
    atomic(path,schedule,immutable=False)
    return schedule


def probe_schedule_wait(activation_key):
    path=probe_schedule_path(activation_key)
    if not path.exists():return None
    schedule=read(path)
    require(schedule.get('activation_key')==activation_key and schedule.get('rpc_name')==RPC_NAME,
            'CAPTURE_PERSISTED_SCHEDULE_SCOPE')
    if schedule.get('stopped') is True:
        return {'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':schedule.get('stop_reason'),
            'next_poll_seconds':86400,'new_rpc_or_reservation_started':False,
            'schedule_response_sha256':schedule['response_sha256']}
    if schedule.get('host_instance')==socket.gethostname() and schedule.get('boot_id')==boot_id():
        remaining=float(schedule['next_monotonic'])-time.monotonic()
    else:
        # Cross-host UTC is only a wake-up hint. The server still authenticates
        # every permitted request, official work window and resource gate.
        remaining=(utc(schedule['next_due_at'])-datetime.now(timezone.utc)).total_seconds()
    if remaining>0:
        return {'state':'AWAITING_ELIGIBLE_EVIDENCE','reason':'ACTUAL_REGISTERED_NEXT_PROBE_NOT_YET_DUE',
            'next_due_at':schedule['next_due_at'],'next_poll_seconds':remaining,
            'new_rpc_or_reservation_started':False,'schedule_response_sha256':schedule['response_sha256']}
    return None


def require_initial_claim_funding(cycle,ack):
    require(isinstance(ack,dict) and ack.get('funded') is True
            and all(ack.get(key)==cycle.get(key) for key in
                    ('attempt_id','activation_key','invocation_id','owner','host_instance','boot_id'))
            and ack.get('prepaid_seconds')==36 and ack.get('slots')==3 and ack.get('seconds_per_slot')==12
            and ack.get('scope')=='INITIAL_CLAIM_AND_TWO_EXACT_NO_RESERVATION_RECONCILIATIONS'
            and ack.get('conservative_no_refund') is True and ack.get('within_actual_activation_predebit') is True,
            'ACTUAL_INITIAL_CLAIM_AND_EXACT_RECOVERY_PREDEBIT_REQUIRED')

def module_pins():
    return {key:sha(Path(__file__).with_name(name+'.py').read_bytes()) for key,name in (
      ('module_sha256','eq20_prospective_capture_runtime'),('capture_core_sha256','eq20_prospective_capture'),
      ('pipeline_module_sha256','eq20_prospective_capture_pipeline'),('incremental_module_sha256','eq20_prospective_incremental'),
      ('accounting_module_sha256','eq20_prospective_accounting_v3'))}

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise Closed('CAPTURE_RPC_REDIRECT_REJECTED')

class RPC:
    """Only this private RPC; exact transmitted bytes are persisted before I/O."""
    def direct(self,document,*,transport_receipt=None):
        require(document.get('p_op') in _ALLOWED,'CAPTURE_RPC_OPERATION_REJECTED')
        helper=load('eq20_rpc_admission_v3','72fda15472338d0b49afc9ab342bfec4b91de743d76d2fd0c0b0293b1d91fdff')
        return helper.send(document,target_rpc='public.'+RPC_NAME+'(text,text,jsonb)',
            post=self._post,canonical_bytes=canonical,host_instance=socket.gethostname(),boot_id=boot_id(),
            read_only=document['p_op'] in ('probe','status'),transport_receipt=transport_receipt)

    def _post(self,rpc_name,raw):
        base=os.environ.get('SUPABASE_URL','').rstrip('/');key=os.environ.get('SUPABASE_SERVICE_ROLE_KEY','').strip()
        require(base=='https://oxzabweahkoimtevbbny.supabase.co' and key,'PRIVATE_CONFIGURATION_REQUIRED')
        require(len(raw)<=MAX_REQUEST,'CAPTURE_RPC_REQUEST_BOUND')
        request=urllib.request.Request(base+'/rest/v1/rpc/'+rpc_name,data=raw,
            headers={'Authorization':'Bearer '+key,'apikey':key,'Content-Type':'application/json'},method='POST')
        try:
            with urllib.request.build_opener(NoRedirect()).open(request,timeout=1.7) as response:
                body=response.read(MAX_REPLY+1)
        except urllib.error.HTTPError as error:
            raise Closed('CAPTURE_RPC_HTTP_'+str(error.code)) from None
        except (urllib.error.URLError,TimeoutError,ConnectionError):
            raise Closed('CAPTURE_RPC_TRANSPORT_FAILURE') from None
        require(len(body)<=MAX_REPLY,'CAPTURE_RPC_RESPONSE_BOUND')
        value=json.loads(body);require(isinstance(value,dict),'CAPTURE_RPC_RESPONSE_OBJECT')
        return value

    def call(self,op,owner,args,*,journal=None,recovery_slot=None):
        require(op in _ALLOWED,'CAPTURE_RPC_OPERATION_REJECTED')
        argument=dict(args)
        document={'p_op':op,'p_owner':owner,'p_args':argument};raw=canonical(document)
        require(len(raw)<=MAX_REQUEST,'CAPTURE_RPC_REQUEST_BOUND')
        key=uuid.uuid4().hex;request_path=ROOT/('rpc_'+key+'.json');reply_path=ROOT/('rpc_reply_'+key+'.json')
        call_id=journal.begin(op,sha(raw),transport='REAPED_HTTP_HELPER',recovery_slot=recovery_slot) if journal else key
        atomic(request_path,raw)
        if op=='claim':atomic(ROOT/('claim_request_'+args['invocation_id']+'.json'),raw)
        began=time.monotonic();cpu=time.process_time();child=None;success=False;value=None;result={}
        try:
            child=launch([sys.executable,'-I','-S',str(ENTRYPOINT),'--rpc-helper',key])
            while not child.reap():
                if time.monotonic()-began>2.1 or time.process_time()-cpu>1:
                    break
                time.sleep(.02)
            child.stop()
            require(child.termination_proof=='SPECIFIC_CHILD_WAIT4' and child.cpu is not None,
                    'CAPTURE_HTTP_HELPER_ACTUAL_WAIT4_REQUIRED')
            result=read(reply_path,MAX_REPLY+1024) if reply_path.exists() else {}
            require(child.exit_code==0 and result.get('success') is True,
                    result.get('error','CAPTURE_HTTP_HELPER_NO_COMPLETE_REPLY'))
            value=result['response'];success=True
            return value
        finally:
            if child is not None:child.stop()
            finished=time.monotonic();elapsed=finished-began
            proof={'operation':op,'request_canonical_utf8':raw.decode(),'request_sha256':sha(raw),
                'attempt_id':args.get('attempt_id'),
                'host_instance':socket.gethostname(),'boot_id':boot_id(),'response_readback_verified':success,
                'process_identity':None if child is None else child.original_identity,
                'process_finished':child is not None and child.termination_proof=='SPECIFIC_CHILD_WAIT4',
                'process_termination_proof':None if child is None else child.termination_proof,
                'helper_cpu_seconds':None if child is None else child.cpu,'rpc_elapsed_seconds':elapsed,
                'parent_cpu_seconds':time.process_time()-cpu,'finished_at':datetime.now(timezone.utc).isoformat(),
                'research_child_launched':False}
            proof['transport_receipt']=result.get('transport_receipt') if isinstance(result,dict) else None
            proof['unknown_mint_may_remain_inert']=not success
            proof['permitted_workload_only_tail']=True
            if success:
                proof['response_sha256']=object_hash(value)
                if op=='claim':
                    atomic(ROOT/('claim_server_reply_'+args['invocation_id']+'.json'),value)
                elif op=='funded_probe':
                    atomic(ROOT/('probe_server_reply_'+args['invocation_id']+'.json'),value)
            else:
                # This is a physical wait, not an assertion that an unknown call
                # completed. A pending journal always retains the full charge.
                time.sleep(SQL_TAIL)
                proof['sql_tail_waited_seconds']=SQL_TAIL
            proof['control_bound_exceeded']=(elapsed+(0 if child is None or child.cpu is None else child.cpu)
                +time.process_time()-cpu+(0 if success else SQL_TAIL)>CONTROL_LIMIT)
            atomic(ROOT/('rpc_proof_'+key+'.json'),proof)
            if op=='claim':atomic(ROOT/('claim_helper_'+args['invocation_id']+'.json'),proof)
            elif op=='funded_probe':atomic(ROOT/('probe_helper_'+args['invocation_id']+'.json'),proof)
            if success and journal:
                journal.finish(call_id,response_sha256=object_hash(value),rpc_elapsed_seconds=elapsed,
                    helper_cpu_seconds=child.cpu,process_identity=child.original_identity,
                    termination_proof=child.termination_proof,sql_tail_seconds=0,
                    transport_receipt=result.get('transport_receipt'))
            request_path.unlink(missing_ok=True);reply_path.unlink(missing_ok=True)
            require(not proof['control_bound_exceeded'],'CAPTURE_ACTUAL_CONTROL_ENVELOPE_EXCEEDED')


def journal_for(cycle):
    return accounting().CallJournal(ROOT/('calls_'+cycle['attempt_id']+'.json'),
        {'attempt_id':cycle['attempt_id'],'owner':cycle['owner'],'host_instance':cycle['host_instance'],
         'boot_id':cycle['boot_id'],'meter_id':cycle['meter_id']},scratch_safe=guards().scratch_safe)

def hydrate_job(claim):
    compact=claim.get('job');require(isinstance(compact,dict),'ACTUAL_SERVER_CAPTURE_JOB_REQUIRED')
    job_text=claim.get('job_evidence_text')
    require(isinstance(job_text,str) and len(job_text.encode())<=256*1024
            and sha(job_text.encode())==exact_hash(claim.get('job_sha256'))
            and json.loads(job_text)==compact,'ACTUAL_COMPACT_JOB_NATIVE_READBACK_REQUIRED')
    context=claim.get('execution_context')
    require(isinstance(context,dict) and isinstance(context.get('evidence_text'),str)
            and len(context['evidence_text'].encode())<=MAX_REPLY
            and sha(context['evidence_text'].encode())==exact_hash(context.get('sha256'))
            and context['sha256']==compact.get('execution_context_sha256'),
            'ACTUAL_IMMUTABLE_CAPTURE_CONTEXT_READBACK_REQUIRED')
    decoded=json.loads(context['evidence_text'])
    require(isinstance(decoded,dict),'CAPTURE_CONTEXT_OBJECT_REQUIRED')
    protected={'attempt_id','host_instance','host_boot_id','owner','fence','admission_nonce',
        'activation_key','session_date','work_key','window_start_at','window_end_at','not_before',
        'deadline_at','regular_open','regular_close','maximum_wall_seconds','maximum_governed_seconds'}
    require(not(protected & set(decoded)) and all(decoded[k]==v for k,v in compact.items() if k in decoded),
            'CAPTURE_CONTEXT_CANNOT_OVERRIDE_FIXED_JOB_SCOPE')
    runtime=claim.get('runtime_binding')
    require(isinstance(runtime,dict),'ACTUAL_ACTIVE_CAPTURE_RUNTIME_BINDING_REQUIRED')
    runtime_fields={'attempt_id','host_instance','host_boot_id','owner','fence','admission_nonce',
        'accounting_module_sha256','request_timeout_verification','accounting_contract_reference',
        'normal_governed_seconds','total_held_seconds','verified_available_capture_governed_seconds',
        'actual_allocation_scope','chain_id','chain_closure_pool','resource_reservation_verified'}
    require(set(runtime)<=runtime_fields and all(runtime[k]==v for k,v in compact.items() if k in runtime),
            'CAPTURE_RUNTIME_BINDING_CANNOT_OVERRIDE_FIXED_JOB_SCOPE')
    job=dict(decoded,**compact);job.update(runtime)
    return job


def validate_job(claim):
    require(claim.get('state')=='RUNNING' and claim.get('normal_governed_seconds')==30
            and claim.get('total_held_seconds')==54,'ACTUAL_FINITE_V3_CAPTURE_RESERVATION_REQUIRED')
    job=hydrate_job(claim)
    for name,value in module_pins().items():require(job.get(name)==value,'CAPTURE_REGISTERED_COMPONENT_PIN_MISMATCH')
    require(claim.get('host_instance')==socket.gethostname() and job.get('host_instance')==socket.gethostname()
            and job.get('host_boot_id')==boot_id(),'CAPTURE_CURRENT_HOST_AND_BOOT_REQUIRED')
    require(claim.get('attempt_id')==job.get('attempt_id') and claim.get('work_key')==job.get('work_key')
            and claim.get('activation_key')==job.get('activation_key'),'CAPTURE_EXACT_ACTIVE_JOB_IDENTITY')
    proof=job.get('request_timeout_verification',{})
    require(proof.get('verified_actual_http_request') is True and proof.get('query_timeout_seconds')==2
            and proof.get('request_start_deadline_ms')==500 and proof.get('post_helper_sql_tail_seconds')==3
            and proof.get('per_request_server_permit_required') is True
            and proof.get('client_clock_error_not_used_for_admission') is True
            and proof.get('clock_observation_is_historical_only') is True
            and proof.get('host_instance')==socket.gethostname() and proof.get('host_boot_id')==boot_id(),
            'ACTUAL_CURRENT_HOST_REQUEST_TIMEOUT_PROOF_REQUIRED')
    exact_hash(proof.get('artifact_sha256'))
    pipeline=load('eq20_prospective_capture_pipeline',job['pipeline_module_sha256'])
    contract,plan,_,_=pipeline.validate_plan(job)
    resource_proof=pipeline.resolve(job.get('full_horizon_feasibility_binding'),
        'SUCCESSOR_CAPTURE_FULL_HORIZON_FEASIBILITY','VERIFIED')
    require(job.get('actual_allocation_scope')=='INITIAL_FULL_HORIZON_PREALLOCATION',
            'ACTUAL_FULL_HORIZON_PREALLOCATION_SCOPE_REQUIRED')
    core=load('eq20_prospective_capture',job['capture_core_sha256'])
    core.validate_acquisition_projection(resource_proof,available_seconds=job.get('verified_available_capture_governed_seconds'))
    return job,contract,plan,pipeline,core


def run_slice(claim,cycle,*,rpc=None):
    """Persist actual page/first-alert/capsule progress in one finite window."""
    job,contract,plan,pipeline,core=validate_job(claim)
    job['_contract']=contract;job['_plan']=plan
    remaining=cycle['child_governed_seconds'];wall=cycle['child_wall_seconds']
    budget=core.GovernedBudget(remaining,wall)
    journal=journal_for(cycle)
    raw_rpc=rpc or RPC()
    def transport(op,args):
        budget.before(1.9)
        document={'p_op':op,'p_owner':cycle['owner'],'p_args':dict(args)}
        request_sha=object_hash(document)
        call=journal.begin(op,request_sha,transport='DIRECT_IN_REAPED_RESEARCH_CHILD')
        began=time.monotonic()
        transport_receipt={}
        value=budget.io('DATABASE_'+op,1.9,lambda:raw_rpc.direct(document,transport_receipt=transport_receipt))
        journal.finish(call,response_sha256=object_hash(value),rpc_elapsed_seconds=time.monotonic()-began,
                       helper_cpu_seconds=0,process_identity=None,termination_proof=None,sql_tail_seconds=0,
                       transport_receipt=transport_receipt)
        return value
    provider=core.ProviderClient(budget,journal=journal)
    incremental_module=load('eq20_prospective_incremental',job['incremental_module_sha256'])
    incremental=incremental_module.prepare_incremental(contract,job['source_producer_bindings'],job['incremental_release_binding'])
    source_cache=SourceRevisionCache()
    page_cache=PendingPageCache()
    observations=PendingDecisionObservations()
    completed=0;attempted=0;last=None;last_committed=None
    try:
        while attempted<job.get('maximum_actions',4096):
            budget.before(2.)
            pending_pages=page_cache.pending()
            pending_observations=observations.pending(job['activation_key'])
            work=transport('next_work',{'attempt_id':job['attempt_id'],'work_key':job['work_key'],
                'expected_cursor_sha256':None if last is None else last.get('cursor_sha256'),
                'pending_provider_pages':pending_pages,'observed_decision_commits':pending_observations})
            page_cache.acknowledge(pending_pages,work.get('committed_provider_page_receipts',[]))
            observations.acknowledge(pending_observations,work.get('committed_decision_observation_receipts',[]))
            if work.get('state') in ('WINDOW_COMPLETE','SESSION_COMPLETE','AWAITING_NEXT_DATED_WINDOW'):
                last=work;break
            if work.get('state')=='WAIT_SOURCE_AVAILABILITY':
                budget.wait_until(utc(work['not_before']));continue
            require(work.get('state')=='RUNNING' and isinstance(work.get('work'),dict),
                    'ACTUAL_SERVER_SELECTED_CAPTURE_ACTION_REQUIRED')
            last=pipeline.perform_action(job,work['work'],rpc=transport,provider=provider,capture=core,
                                         incremental=incremental,source_cache=source_cache,page_cache=page_cache)
            require(isinstance(last,dict),'CAPTURE_ACTION_COMMITTED_RESPONSE_REQUIRED')
            if last.get('decision_commit_observation') is not None:
                observations.store(last['decision_commit_observation'])
            attempted+=1
            if last.get('committed_progress') is True:
                completed+=1;last_committed=last
            elif last.get('state')=='AWAITING_ELIGIBLE_EVIDENCE':
                # A wait or a successful read is not committed stage output.
                # Yield to the fixed future window instead of spinning.
                break
    except core.SliceComplete:
        pass
    finally:
        source_cache.close()
    exported=journal.export()
    return {'state':'RUNNING','completed_actions':completed,'attempted_actions':attempted,
        'last_committed_transition':last_committed,'last_action_result':last,
        'governed_work':budget.receipt(),'calls_sha256':exported['calls_sha256'],
        'protected_outcomes_accessed_only_under_registered_activation':True,
        'research_objective_achieved':False}


def cycle_call_totals(journal):
    exported=journal.export()
    total=sum(Decimal(str(c['rpc_elapsed_seconds']))+Decimal(str(c['helper_cpu_seconds']))+
              Decimal(str(c['sql_tail_seconds'])) for c in exported['calls'])
    return float(total)


def carry_control_fits(cycle,journal):
    """The old54 prepays only12 for all work before a new reservation."""
    if cycle.get('parent_pid')!=os.getpid() or journal.data.get('pending') is not None:return False
    try:
        parent_cpu=time.process_time()-cycle['parent_cpu_start']
        actual=parent_cpu+cycle_call_totals(journal)
        return math.isfinite(actual) and parent_cpu>=0 and actual+CONTROL_LIMIT<=12
    except (KeyError,TypeError,ValueError):
        return False


def terminal_args(cycle,physical,result,journal):
    try:
        calls=journal.export();complete=True
    except Exception:
        calls={'calls':journal.data.get('calls',[]),'pending':journal.data.get('pending'),
               'unknown_call_count':1,'all_started_calls_accounted':False,'all_sql_tails_closed':False};complete=False
    receipt={'version':VERSION,'attempt_id':cycle['attempt_id'],'activation_key':cycle['activation_key'],
        'owner':cycle['owner'],'host_instance':cycle['host_instance'],'boot_id':cycle['boot_id'],
        'module_sha256':cycle['module_sha256'],'state':result.get('state','BLOCKED_BY_IDENTIFIED_DEPENDENCY'),
        'result':result,'physical_quiescence':physical,'process_finished':True,
        'process_termination_proof':physical['termination_proof'],'process_identity':physical.get('process_identity'),
        'child_cpu_seconds':physical['cpu_seconds'],'child_wall_seconds':physical['wall_seconds'],
        'parent_cpu_seconds':(time.process_time()-cycle['parent_cpu_start'])
            if cycle.get('parent_pid')==os.getpid() and time.process_time()>=cycle['parent_cpu_start'] else None,
        'call_census':calls,'all_started_calls_accounted':complete,'research_objective_achieved':False,
        'unknown_work_retains_full54':not complete or physical.get('cpu_seconds') is None
            or cycle.get('parent_pid')!=os.getpid(),'prior_v2_debits_changed':False}
    require(len(canonical(receipt))<MAX_REQUEST-4096,'CAPTURE_TERMINAL_RECEIPT_BOUND')
    return {'attempt_id':cycle['attempt_id'],'receipt':receipt}


def execute_claim(rpc,cycle,claim):
    require(claim.get('attempt_id')==cycle['attempt_id'],'CAPTURE_CLAIM_ATTEMPT_READBACK_MISMATCH')
    cycle=dict(cycle,activation_key=claim['activation_key'],claim=claim)
    # The durable server claim is recorded before any local gate that can fail.
    # A rejected job is still an admitted reservation requiring settlement.
    atomic(ROOT/'active_cycle.json',cycle,immutable=False)
    atomic(ROOT/('claim_received_'+cycle['invocation_id']+'.json'),
        {'attempt_id':cycle['attempt_id'],'invocation_id':cycle['invocation_id'],
         'job_sha256':claim['job_sha256'],'claim_sha256':object_hash(claim)})
    journal=journal_for(cycle)
    rejection=None
    try:
        job,_,_,_,_=validate_job(claim)
        available=30-cycle_call_totals(journal)-(time.process_time()-cycle['parent_cpu_start'])-7.5
        wall=min(150.,float(job['maximum_wall_seconds']),
                 (utc(job['deadline_at'])-datetime.now(timezone.utc)).total_seconds()-3.)
        require(available>2 and wall>2,'FINITE_CAPTURE_WINDOW_HEADROOM_EXHAUSTED')
        cycle.update(child_governed_seconds=min(available,30.),child_wall_seconds=wall)
        atomic(ROOT/('cycle_'+cycle['attempt_id']+'.json'),cycle)
        atomic(ROOT/'active_cycle.json',cycle,immutable=False)
    except Exception as error:
        rejection=str(error)[:180]
    if stopping() or rejection is not None:
        physical={'process_finished':True,'termination_proof':'EXACT_OWNED_NO_RESEARCH_LAUNCH',
            'launch_intent_absent':True,'cpu_seconds':0.,'wall_seconds':0.,'process_identity':None,
            'admission_nonce':claim.get('admission_nonce',claim.get('runtime_binding',{}).get('admission_nonce')),
            'claim_sha256':object_hash(claim),'claim_received_marker_sha256':
                sha((ROOT/('claim_received_'+cycle['invocation_id']+'.json')).read_bytes()),
            'claim_helper_sha256':sha((ROOT/('claim_helper_'+cycle['invocation_id']+'.json')).read_bytes())
                if (ROOT/('claim_helper_'+cycle['invocation_id']+'.json')).exists() else None}
        result={'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                'reason':'EXPLICIT_STOP_PRESERVED' if stopping() else rejection,
                'no_research_child_launched':True}
    else:
        atomic(ROOT/('launch_'+cycle['attempt_id']+'.json'),{'attempt_id':cycle['attempt_id'],
            'job_sha256':claim['job_sha256'],'host_instance':cycle['host_instance'],'boot_id':cycle['boot_id']})
        began=time.monotonic();process=None;child_error=None
        try:
            process=launch([sys.executable,'-I','-S',str(ENTRYPOINT),'--capture-child',cycle['attempt_id']])
            if process.original_identity is not None:atomic(ROOT/('process_'+cycle['attempt_id']+'.json'),process.original_identity)
            while not process.reap():
                if time.monotonic()-began>wall:break
                # A socket read is bounded as a whole operation, not merely by
                # urllib's per-packet timeout. The source journal is the intent.
                try:
                    pending=read(ROOT/('calls_'+cycle['attempt_id']+'.json')).get('pending')
                except Exception:pending=None
                if pending and pending.get('transport')=='DIRECT_IN_REAPED_RESEARCH_CHILD' \
                        and time.monotonic()-pending.get('started_monotonic',time.monotonic())>2.1:
                    break
                time.sleep(.05)
        except BaseException as error:
            # Keep exact physical termination even if publishing the identity
            # or reading the control journal fails after launch.
            child_error=type(error).__name__
        finally:
            if process is not None:process.stop()
        require(process is not None and process.termination_proof=='SPECIFIC_CHILD_WAIT4' and process.cpu is not None,
                'ACTUAL_CAPTURE_CHILD_WAIT4_REQUIRED')
        child_wall=time.monotonic()-began
        physical={'process_finished':True,'termination_proof':process.termination_proof,
            'process_identity':process.original_identity,'cpu_seconds':process.cpu,'wall_seconds':child_wall,
            'exit_code':process.exit_code}
        if process.exit_code!=0 or child_error is not None:
            time.sleep(SQL_TAIL)
            physical['sql_tail_waited_seconds']=SQL_TAIL
        atomic(ROOT/('physical_'+cycle['attempt_id']+'.json'),physical)
        output=ROOT/('result_'+cycle['attempt_id']+'.json')
        if child_error is not None:
            result={'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                    'reason':'CAPTURE_POST_LAUNCH_CONTROL_FAILURE','error_type':child_error}
        else:
            try:
                result=read(output,MAX_REQUEST) if output.exists() else {
                    'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':'BOUNDED_CAPTURE_CHILD_NO_RECEIPT'}
            except Exception as error:
                result={'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY',
                        'reason':'CAPTURE_CHILD_RECEIPT_READBACK_FAILED','error_type':type(error).__name__}
        journal=journal_for(cycle)  # Child atomically appended actual call receipts.
    atomic(ROOT/('physical_'+cycle['attempt_id']+'.json'),physical)
    args=terminal_args(cycle,physical,result,journal)
    atomic(ROOT/('terminal_'+cycle['attempt_id']+'.json'),args)
    return complete_terminal(rpc,cycle,args,physical,journal)


def complete_terminal(rpc,cycle,args,physical,journal,*,recovery_slot=None):
    # A pending child call is retained. Recovery transport gets a separate,
    # finite journal rather than deleting the original unknown intent.
    usable=journal if journal.data.get('pending') is None else None
    actual_args=dict(args)
    if recovery_slot is not None:
        actual_args.update(recovery_slot=recovery_slot,recovery_invocation=uuid.uuid4().hex)
    ack=rpc.call('terminal',cycle['owner'],actual_args,journal=usable,recovery_slot=recovery_slot)
    require(ack.get('state') in ('QUIESCENT','VERIFIED','RUNNING') and ack.get('attempt_id')==cycle['attempt_id'],
            'ACTUAL_CAPTURE_TERMINAL_ACK_REQUIRED')
    terminal_sha=exact_hash(ack.get('terminal_receipt_sha256'));physical_sha=exact_hash(ack.get('physical_quiescence_sha256'))
    atomic(ROOT/('terminal_ack_'+cycle['attempt_id']+'.json'),ack)
    cut_cpu=time.process_time();cut_monotonic=time.monotonic()
    next_meter=None
    rollover_headroom=False
    if usable is not None and type(physical.get('cpu_seconds')) in (int,float) and cycle.get('parent_pid')==os.getpid():
        prefix=physical['cpu_seconds']+cut_cpu-cycle['parent_cpu_start']+cycle_call_totals(usable)
        rollover_headroom=math.isfinite(prefix) and 0<=prefix and prefix+12<=54
    if rollover_headroom:
        # Admit the closure work before proof construction/fsync. Everything
        # after this exact cut belongs to the next funded meter only if the
        # server atomically accepts the rollover; otherwise the old full54 stays.
        next_meter={'version':'EQ20_PROSPECTIVE_LOCAL_NEXT_METER_V3','meter_id':uuid.uuid4().hex,
            'attempt_id':str(uuid.uuid4()),'owner':cycle['owner'],'host_instance':cycle['host_instance'],
            'boot_id':cycle['boot_id'],'parent_pid':os.getpid(),'parent_cpu_start':cut_cpu,
            'monotonic_start':cut_monotonic,'predecessor_attempt_id':cycle['attempt_id']}
        atomic(ROOT/'next_meter.json',next_meter,immutable=False)
    cut={'meter_id':cycle['meter_id'],'attempt_id':cycle['attempt_id'],'host_instance':cycle['host_instance'],
        'boot_id':cycle['boot_id'],'terminal_ack_sha256':object_hash(ack),'all_old_calls_closed':usable is not None,
        'all_children_reaped':True,'parent_cpu_counter_at_cut':str(cut_cpu),'monotonic_at_cut':str(cut_monotonic)}
    closed=None
    if rollover_headroom:
        try:
            closed=accounting().build_closed_cycle(journal=usable,
                scope={k:cycle[k] for k in ('attempt_id','activation_key','owner','host_instance','boot_id','module_sha256')},
                research_child=physical,terminal_ack_sha256=object_hash(ack),terminal_sha256=terminal_sha,
                physical_sha256=physical_sha,parent_cpu_seconds=cut_cpu-cycle['parent_cpu_start'],
                recovery_parent_cpu_seconds=0,meter_cut=cut,terminal_response=ack)
        except Exception:closed=None
    carry={'attempt_id':cycle['attempt_id'],'activation_key':cycle['activation_key'],'owner':cycle['owner'],
        'host_instance':cycle['host_instance'],'boot_id':cycle['boot_id'],'parent_pid':os.getpid(),
        'terminal_receipt_sha256':terminal_sha,'physical_quiescence_sha256':physical_sha,'closed_cycle':closed,
        'next_meter':next_meter,'all_actual_charges_preserved':True}
    atomic(ROOT/'carry.json',carry,immutable=False)
    (ROOT/'active_cycle.json').unlink(missing_ok=True)
    if closed is None:
        return conservative_close(rpc,cycle['owner'],carry)
    return dict(ack,committed=bool(result_committed(args['receipt'].get('result',{}))),
        next_poll_seconds=1 if closed is not None else 30,research_objective_achieved=False)


def result_committed(result):
    return type(result.get('completed_actions')) is int and result['completed_actions']>0


def finish_claim_reconciliation(rpc,cycle,result):
    """Replay an actually observed native ACK without spending another slot."""
    if result.get('claim_closed') is not True:return result
    if result.get('reservation_created') is False:
        if (ROOT/'carry.json').exists():
            prior=read(ROOT/'carry.json',MAX_REPLY)
            # The native no-new-cycle reconciliation closes the old chain in
            # this same prepaid request. A second closure could exceed it.
            require(result.get('closed') is True and result.get('closed_attempt_id')==prior['attempt_id']
                    and result.get('closed_terminal_receipt_sha256')==prior['terminal_receipt_sha256']
                    and result.get('closed_physical_quiescence_sha256')==prior['physical_quiescence_sha256'],
                    'ACTUAL_RECONCILED_PREDECESSOR_CHAIN_CLOSURE_REQUIRED')
            (ROOT/'carry.json').unlink(missing_ok=True);(ROOT/'next_meter.json').unlink(missing_ok=True)
            prune_acknowledged_cycle(prior['attempt_id'])
        (ROOT/'active_cycle.json').unlink(missing_ok=True)
        prune_acknowledged_cycle(cycle['attempt_id'])
        return result
    require(result.get('attempt_id')==cycle['attempt_id']
            and isinstance(result.get('activation_key'),str),
            'ACTUAL_RECONCILED_CAPTURE_RESERVATION_SCOPE_REQUIRED')
    carry={'attempt_id':cycle['attempt_id'],'activation_key':result['activation_key'],
        'owner':cycle['owner'],'host_instance':cycle['host_instance'],'boot_id':cycle['boot_id'],
        'module_sha256':cycle['module_sha256'],'parent_pid':None,'closed_cycle':None,'next_meter':None,
        'terminal_receipt_sha256':exact_hash(result.get('terminal_receipt_sha256')),
        'physical_quiescence_sha256':exact_hash(result.get('physical_quiescence_sha256')),
        'chain_pointer':result.get('chain_pointer'),'all_actual_charges_preserved':True}
    atomic(ROOT/'carry.json',carry,immutable=False)
    (ROOT/'active_cycle.json').unlink(missing_ok=True)
    return conservative_close(rpc,carry['owner'],carry)


def reconcile_active(rpc,cycle):
    """Never launch another child to repair an unknown prior acknowledgement."""
    require(cycle.get('host_instance')==socket.gethostname() and cycle.get('boot_id')==boot_id(),
            'PRIOR_HOST_CAPTURE_QUIESCENCE_REQUIRED')
    if cycle.get('phase')=='PROBE_PENDING':
        require(not (ROOT/('launch_'+cycle['attempt_id']+'.json')).exists()
                and not (ROOT/('claim_request_'+cycle['invocation_id']+'.json')).exists(),
                'CAPTURE_METADATA_RECOVERY_CANNOT_CROSS_CLAIM_BOUNDARY')
        if (ROOT/'carry.json').exists():
            carry=read(ROOT/'carry.json',MAX_REPLY)
            result=conservative_close(rpc,carry['owner'],carry)
            if result.get('closed') is True:
                (ROOT/'active_cycle.json').unlink(missing_ok=True)
                prune_acknowledged_cycle(cycle['attempt_id'])
            return result
        count_path=ROOT/('probe_recovery_'+cycle['invocation_id']+'.json')
        counter=read(count_path) if count_path.exists() else {'used':0}
        require(type(counter.get('used')) is int and counter['used']<2,
                'FINITE_CAPTURE_METADATA_RECONCILIATION_EXHAUSTED')
        original=read(ROOT/('probe_request_'+cycle['invocation_id']+'.json'),MAX_REQUEST)
        args=dict(original['p_args'],invocation_id=uuid.uuid4().hex,
                  reconcile_probe_invocation_id=cycle['invocation_id'],probe_recovery_slot=counter['used'],
                  original_probe_helper_termination=read(ROOT/('probe_helper_'+cycle['invocation_id']+'.json'),MAX_REQUEST))
        atomic(count_path,{'used':counter['used']+1},immutable=False)
        require(original.get('p_op')=='funded_probe','EXACT_FUNDED_CAPTURE_PROBE_INTENT_REQUIRED')
        result=rpc.call('funded_probe',cycle['owner'],args)
        require(result.get('metadata_reconciliation_completed') is True
                and result.get('reconciled_probe_invocation_id')==cycle['invocation_id']
                and result.get('metadata_accounting_ack',{}).get('funded') is True,
                'ACTUAL_CAPTURE_METADATA_RECONCILIATION_ACK_REQUIRED')
        record_metadata_ack(original['p_args']['activation_key'],result['metadata_accounting_ack'])
        (ROOT/'active_cycle.json').unlink(missing_ok=True)
        prune_acknowledged_cycle(cycle['attempt_id'])
        (ROOT/('probe_request_'+cycle['invocation_id']+'.json')).unlink(missing_ok=True)
        count_path.unlink(missing_ok=True)
        return dict(result,next_poll_seconds=30)
    terminal=ROOT/('terminal_'+cycle['attempt_id']+'.json')
    if terminal.exists():
        args=read(terminal,MAX_REQUEST);physical=args['receipt']['physical_quiescence']
        replay=ROOT/('terminal_recovery_'+cycle['attempt_id']+'.json')
        state=read(replay) if replay.exists() else {'used':0}
        require(type(state['used']) is int and state['used']<2,'FINITE_CAPTURE_TERMINAL_REPLAYS_EXHAUSTED')
        slot=state['used'];atomic(replay,{'used':slot+1},immutable=False)
        return complete_terminal(rpc,cycle,args,physical,journal_for(cycle),recovery_slot=slot)
    # A durable unacknowledged admission can be closed only using its exact
    # helper wait4 proof, and only when neither receipt nor launch marker exists.
    invocation=cycle['invocation_id']
    observed_claim=ROOT/('claim_server_reply_'+invocation+'.json')
    if observed_claim.exists() and not (ROOT/('claim_received_'+invocation+'.json')).exists():
        claim=read(observed_claim,MAX_REPLY)
        if claim.get('state')=='RUNNING':
            require(claim.get('attempt_id')==cycle['attempt_id'],'ACTUAL_CAPTURE_OBSERVED_CLAIM_IDENTITY')
            cycle=dict(cycle,claim=claim,activation_key=claim['activation_key'])
            atomic(ROOT/'active_cycle.json',cycle,immutable=False)
            atomic(ROOT/('claim_received_'+invocation+'.json'),{'attempt_id':cycle['attempt_id'],
                'invocation_id':invocation,'job_sha256':claim['job_sha256'],'claim_sha256':object_hash(claim)})
        elif claim.get('reservation_created') is False:
            (ROOT/'active_cycle.json').unlink(missing_ok=True)
            prune_acknowledged_cycle(cycle['attempt_id'])
            return claim
    request_path=ROOT/('claim_request_'+invocation+'.json');proof_path=ROOT/('claim_helper_'+invocation+'.json')
    if not (ROOT/('claim_received_'+invocation+'.json')).exists() and not (ROOT/('launch_'+cycle['attempt_id']+'.json')).exists():
        request=read(request_path,MAX_REQUEST);proof=read(proof_path,MAX_REQUEST)
        require(proof.get('request_sha256')==object_hash(request) and proof.get('process_finished') is True
                and proof.get('process_termination_proof')=='SPECIFIC_CHILD_WAIT4'
                and proof.get('response_readback_verified') is False
                and type(proof.get('sql_tail_waited_seconds')) in (int,float)
                and proof['sql_tail_waited_seconds']>=SQL_TAIL,'ACTUAL_LOST_CAPTURE_CLAIM_HELPER_PROOF_REQUIRED')
        saved_ack=ROOT/('claim_reconcile_ack_'+cycle['attempt_id']+'.json')
        if saved_ack.exists():
            return finish_claim_reconciliation(rpc,cycle,read(saved_ack,MAX_REPLY))
        counter_path=ROOT/('terminal_recovery_'+cycle['attempt_id']+'.json')
        counter=read(counter_path) if counter_path.exists() else {'used':0}
        require(type(counter.get('used')) is int and 0<=counter['used']<2,
                'FINITE_CAPTURE_CLAIM_RECONCILIATIONS_EXHAUSTED')
        args={'attempt_id':cycle['attempt_id'],'invocation_id':invocation,'host_instance':socket.gethostname(),
            'host_boot_id':boot_id(),'original_claim_canonical_utf8':canonical(request).decode(),
            'original_claim_sha256':object_hash(request),'helper_termination':proof,
            'no_research_launch_marker':True,'claim_received_marker_absent':True,
            'recovery_slot':counter['used'],'recovery_invocation':uuid.uuid4().hex}
        atomic(ROOT/('claim_reconcile_request_'+cycle['attempt_id']+'_'+str(counter['used'])+'.json'),args)
        atomic(counter_path,{'used':counter['used']+1},immutable=False)
        result=rpc.call('claim_reconcile',cycle['owner'],args,recovery_slot=counter['used'])
        if result.get('claim_closed') is True:
            atomic(ROOT/('claim_reconcile_ack_'+cycle['attempt_id']+'.json'),result)
        return finish_claim_reconciliation(rpc,cycle,result)
    launch_path=ROOT/('launch_'+cycle['attempt_id']+'.json')
    if not launch_path.exists():
        received=read(ROOT/('claim_received_'+invocation+'.json'))
        require(received.get('claim_sha256')==object_hash(cycle.get('claim')),
                'ACTUAL_CAPTURE_CLAIM_READBACK_REQUIRED')
        physical={'process_finished':True,'termination_proof':'EXACT_OWNED_NO_RESEARCH_LAUNCH',
            'launch_intent_absent':True,'cpu_seconds':0.,'wall_seconds':0.,'process_identity':None,
            'admission_nonce':cycle['claim'].get('admission_nonce',cycle['claim'].get('runtime_binding',{}).get('admission_nonce')),
            'claim_sha256':received['claim_sha256'],
            'claim_received_marker_sha256':object_hash(received),
            'claim_helper_sha256':sha((ROOT/('claim_helper_'+invocation+'.json')).read_bytes())
                if (ROOT/('claim_helper_'+invocation+'.json')).exists() else None}
    else:
        # The exact recorded process identity is checked before any signal.
        # Missing wait4 CPU remains unknown and can never earn measured credit.
        identity=read(ROOT/('process_'+cycle['attempt_id']+'.json'),8192)
        proof=guards().quiesce_recorded_child(identity,guards().ParentBudget(),reserve_rpc=False)
        require(proof.get('process_finished') is True,'ACTUAL_RECORDED_CAPTURE_CHILD_QUIESCENCE_REQUIRED')
        time.sleep(SQL_TAIL)
        physical={'process_finished':True,'termination_proof':proof['proof'],
            'process_identity':identity,'cpu_seconds':None,'wall_seconds':None,
            'measurement_verified':False,'sql_tail_waited_seconds':SQL_TAIL,
            'recorded_identity_reconciled':True}
    result={'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':'CAPTURE_INTERRUPTED_EXACT_PROCESS_RECOVERED',
            'measurement_verified':False,'unknown_work_retains_full54':True,'research_objective_achieved':False}
    args=terminal_args(cycle,physical,result,journal_for(cycle))
    atomic(terminal,args)
    # Use the first already-funded recovery slot. This settles a quiescent
    # original attempt and never runs its scientific child again.
    return reconcile_active(rpc,cycle)


def prune_acknowledged_cycle(attempt):
    """Only call after a server settlement, or an observed read-only no-claim."""
    require(re.fullmatch(r'[0-9a-f-]{36}',attempt or ''),'CAPTURE_PRUNE_EXACT_ATTEMPT')
    for prefix in ('cycle_','calls_','launch_','process_','physical_','result_','terminal_',
                   'terminal_ack_','terminal_recovery_','closure_','closure_recovery_','claim_reconcile_ack_'):
        (ROOT/(prefix+attempt+'.json')).unlink(missing_ok=True)
    for path in ROOT.glob('claim_reconcile_request_'+attempt+'_[01].json'):
        path.unlink(missing_ok=True)
    for prefix in ('claim_received_','claim_request_','claim_helper_','claim_server_reply_','probe_request_',
                   'probe_helper_','probe_server_reply_','rpc_proof_'):
        for path in ROOT.glob(prefix+'*.json'):
            try:
                row=read(path,MAX_REQUEST+8192)
                bound=row.get('attempt_id') or row.get('p_args',{}).get('attempt_id')
                if bound==attempt:path.unlink(missing_ok=True)
            except (OSError,ValueError,Closed):
                continue


def conservative_close(rpc,owner,carry):
    """Exactly two prepaid closure requests; no budget is created by cleanup."""
    attempt=carry['attempt_id'];state_path=ROOT/('closure_recovery_'+attempt+'.json')
    state=read(state_path) if state_path.exists() else {'used':0,'closure_invocation':uuid.uuid4().hex}
    require(type(state.get('used')) is int and 0<=state['used']<2,'FINITE_CAPTURE_CLOSURE_POOL_EXHAUSTED')
    args={k:carry[k] for k in ('attempt_id','activation_key','terminal_receipt_sha256','physical_quiescence_sha256')}
    args.update(closure_invocation=state['closure_invocation'],closure_slot=state['used'],
                host_instance=socket.gethostname(),host_boot_id=boot_id())
    atomic(ROOT/('closure_'+attempt+'.json'),args,immutable=False)
    atomic(state_path,dict(state,used=state['used']+1),immutable=False)
    result=rpc.call('close_conservative',owner,args,recovery_slot=state['used'])
    if result.get('closed') is True and result.get('attempt_id')==attempt:
        (ROOT/'carry.json').unlink(missing_ok=True);(ROOT/'next_meter.json').unlink(missing_ok=True)
        prune_acknowledged_cycle(attempt)
    return result


def supervise_once(rpc,owner,*,scheduled_at,trigger,activation_key=None):
    require(trigger in ('PERSISTENT_WORKER_STARTUP','PERSISTENT_WORKER_TIMER'),'OBSERVED_PERSISTENT_CAPTURE_TRIGGER_REQUIRED')
    ROOT.mkdir(parents=True,exist_ok=True,mode=0o700)
    if (ROOT/'active_cycle.json').exists():return reconcile_active(rpc,read(ROOT/'active_cycle.json',MAX_REPLY))
    carry=read(ROOT/'carry.json',MAX_REPLY) if (ROOT/'carry.json').exists() else None
    if carry is not None and (carry.get('closed_cycle') is None or carry.get('parent_pid')!=os.getpid()
                             or carry.get('host_instance')!=socket.gethostname() or carry.get('boot_id')!=boot_id()):
        return conservative_close(rpc,owner,carry)
    meter=(carry['next_meter'] if carry is not None else {'meter_id':uuid.uuid4().hex,'attempt_id':str(uuid.uuid4()),
        'parent_cpu_start':time.process_time(),'monotonic_start':time.monotonic()})
    actual_activation=carry['activation_key'] if carry is not None else activation_key
    require(isinstance(actual_activation,str) and 0<len(actual_activation)<=256,
            'ACTUAL_ACKNOWLEDGED_CAPTURE_ACTIVATION_REQUIRED_BEFORE_POLL')
    if carry is None and metadata_pool_stopped(actual_activation):
        return {'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':'FINITE_ACTIVATION_METADATA_POOL_EXHAUSTED',
                'next_poll_seconds':300,'new_rpc_or_reservation_started':False}
    if carry is None:
        if peer_has_pending_cycle():
            return {'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':'OWNED_PEER_CYCLE_MUST_FINISH_FIRST',
                    'next_poll_seconds':1,'new_rpc_or_reservation_started':False}
        waiting=probe_schedule_wait(actual_activation)
        if waiting is not None:return waiting
    cycle=dict(meter,owner=owner,host_instance=socket.gethostname(),boot_id=boot_id(),parent_pid=os.getpid(),
        phase='PROBE_PENDING',activation_key=actual_activation,
        module_sha256=sha(ENTRYPOINT.read_bytes()),invocation_id=uuid.uuid4().hex)
    atomic(ROOT/'active_cycle.json',cycle,immutable=False)
    journal=journal_for(cycle)
    arguments=dict(module_pins(),version=VERSION,host_instance=socket.gethostname(),host_boot_id=boot_id(),
        attempt_id=cycle['attempt_id'],meter_id=cycle['meter_id'],invocation_id=cycle['invocation_id'],
        scheduled_at=scheduled_at,trigger=trigger,activation_key=actual_activation,
        probe_funding_mode='NEXT_REGISTERED_CYCLE_METER' if carry is not None else 'INITIAL_ACTIVATION_PREPAID_METADATA')
    prior=None
    if carry is not None:
        prior=accounting().prepare_rollover(carry['closed_cycle'],next_meter_id=cycle['meter_id'],
            next_attempt_id=cycle['attempt_id'],next_owner=owner,next_host=socket.gethostname(),
            next_boot_id=boot_id(),meter_started_before_closure=True)
        arguments['prior_settlement']=prior
        if not carry_control_fits(cycle,journal):
            (ROOT/'active_cycle.json').unlink(missing_ok=True)
            return conservative_close(rpc,carry['owner'],carry)
    atomic(ROOT/('probe_request_'+cycle['invocation_id']+'.json'),
           {'p_op':'funded_probe','p_owner':owner,'p_args':arguments})
    probe=rpc.call('funded_probe',owner,arguments,journal=journal)
    metadata_ack=probe.get('metadata_accounting_ack',{})
    require(metadata_ack.get('funded') is True,'ACTUAL_FINITE_CAPTURE_PROBE_FUNDING_REQUIRED')
    if carry is None:record_metadata_ack(actual_activation,metadata_ack)
    pending=probe.get('pending_quiescent_cycle')
    if pending is not None:
        require(isinstance(pending,dict) and pending.get('activation_key')==actual_activation,
                'ACTUAL_PENDING_CAPTURE_CYCLE_SCOPE_REQUIRED')
        for field in ('terminal_receipt_sha256','physical_quiescence_sha256'):exact_hash(pending.get(field))
        closure=dict(pending,owner=pending['owner_id'],closed_cycle=None,parent_pid=None,
                     next_meter=None,all_actual_charges_preserved=True)
        atomic(ROOT/'carry.json',closure,immutable=False)
        (ROOT/'active_cycle.json').unlink(missing_ok=True)
        return conservative_close(rpc,closure['owner'],closure)
    if probe.get('state')!='READY_TO_CLAIM':
        # The funded probe cannot read observations or start a child; its
        # immutable metadata accounting acknowledgement is required above.
        # A previous funded cycle remains held; no unobserved credit is claimed.
        schedule=record_probe_schedule(actual_activation,probe)
        (ROOT/'active_cycle.json').unlink(missing_ok=True)
        if carry is not None:
            # A long idle interval must not accumulate unallocated control work
            # behind an unused successor meter. The finite chain pool closes it.
            return conservative_close(rpc,owner,carry)
        prune_acknowledged_cycle(cycle['attempt_id'])
        return dict(probe,next_poll_seconds=schedule.get('next_poll_seconds',86400),
                    probe_schedule_stopped=schedule['stopped'])
    claim_args=dict(arguments,activation_key=probe['activation_key'],work_key=probe['work_key'],
        job_sha256=probe['job_sha256'],accounting_contract_reference=probe['accounting_contract_reference'],prior_settlement=prior)
    if carry is None:
        initial_ack=probe.get('initial_control_accounting_ack')
        require_initial_claim_funding(cycle,initial_ack)
        cycle['initial_control_accounting_ack']=initial_ack
        claim_args['initial_control_accounting_ack']=initial_ack
    if carry is not None and not carry_control_fits(cycle,journal):
        (ROOT/'active_cycle.json').unlink(missing_ok=True)
        return conservative_close(rpc,carry['owner'],carry)
    cycle['activation_key']=probe['activation_key'];cycle['phase']='CLAIM_REQUESTED'
    atomic(ROOT/'active_cycle.json',cycle,immutable=False)
    claim=rpc.call('claim',owner,claim_args,journal=journal)
    if claim.get('state')!='RUNNING':
        require(claim.get('reservation_created') is False,'UNKNOWN_CAPTURE_CLAIM_STATE_REQUIRES_RECONCILIATION')
        (ROOT/'active_cycle.json').unlink(missing_ok=True)
        if carry is not None:return conservative_close(rpc,owner,carry)
        prune_acknowledged_cycle(cycle['attempt_id'])
        return claim
    if carry is not None and claim.get('prior_settlement_committed') is True:
        require(claim.get('prior_attempt_id')==carry['attempt_id']
                and claim.get('prior_closed_cycle_sha256')==object_hash(carry['closed_cycle']),
                'CAPTURE_EXACT_ROLLOVER_SETTLEMENT_ACK_REQUIRED')
        prune_acknowledged_cycle(carry['attempt_id'])
    return execute_claim(rpc,cycle,claim)


def timer_tick(owner,*,scheduled_at=None,trigger='PERSISTENT_WORKER_STARTUP',rpc=None,activation_key=None):
    global _last_poll,_last_delay,_last_activation_key
    if stopping() or not _lock.acquire(blocking=False):return {'state':'CAPTURE_STOPPED_OR_LOCAL_OWNER_ACTIVE'}
    handle=None
    try:
        pending=(ROOT/'active_cycle.json').exists() or (ROOT/'carry.json').exists()
        if not pending and activation_key==_last_activation_key and time.monotonic()-_last_poll<_last_delay:
            return {'state':'CAPTURE_BOUNDED_POLL_INTERVAL','next_poll_seconds':_last_delay,
                    'new_rpc_or_reservation_started':False}
        _last_poll=time.monotonic();_last_activation_key=activation_key
        ROOT.mkdir(parents=True,exist_ok=True,mode=0o700)
        handle=(ROOT/'owner.lock').open('a+b');fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        result=supervise_once(rpc or RPC(),owner,scheduled_at=scheduled_at,trigger=trigger,activation_key=activation_key)
        delay=float(result.get('next_poll_seconds',30))
        require(math.isfinite(delay) and 0<=delay<=370*86400,'BOUNDED_NATIVE_OR_LOCAL_POLL_DELAY')
        _last_delay=max(1,delay)
        return result
    except Exception as error:
        LOG.warning('EQ20 prospective capture dependency: %s',type(error).__name__)
        _last_delay=30
        return {'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':str(error)[:180],'next_poll_seconds':30}
    finally:
        if handle is not None:handle.close()
        _lock.release()


def _rpc_helper(key):
    require(re.fullmatch(r'[0-9a-f]{32}',key),'CAPTURE_HELPER_ID')
    signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.setitimer(signal.ITIMER_REAL,2)
    signal.signal(signal.SIGPROF,signal.SIG_DFL);signal.setitimer(signal.ITIMER_PROF,.5)
    resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
    resource.setrlimit(resource.RLIMIT_FSIZE,(MAX_FILE,MAX_FILE));guards().prohibit_descendants()
    transport_receipt={}
    try:
        value=RPC().direct(read(ROOT/('rpc_'+key+'.json'),MAX_REQUEST),transport_receipt=transport_receipt);result={'success':True,'response':value}
    except Exception as error:result={'success':False,'error':str(error)[:180]}
    result['transport_receipt']=transport_receipt
    atomic(ROOT/('rpc_reply_'+key+'.json'),result)
    return 0 if result['success'] else 1

def _capture_child(attempt):
    require(re.fullmatch(r'[0-9a-f-]{36}',attempt),'CAPTURE_CHILD_ATTEMPT_ID')
    cycle=read(ROOT/('cycle_'+attempt+'.json'),MAX_REPLY)
    wall=float(cycle['child_wall_seconds']);cpu=float(cycle['child_governed_seconds'])
    require(0<wall<=150 and 0<cpu<=30,'CAPTURE_CHILD_EXISTING_LIMITS')
    signal.signal(signal.SIGALRM,signal.SIG_DFL);signal.setitimer(signal.ITIMER_REAL,wall)
    resource.setrlimit(resource.RLIMIT_CPU,(max(1,math.ceil(cpu)),max(1,math.ceil(cpu))))
    resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
    resource.setrlimit(resource.RLIMIT_FSIZE,(MAX_FILE,MAX_FILE));os.nice(10);guards().prohibit_descendants()
    try:result=run_slice(cycle['claim'],cycle)
    except Exception as error:result={'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':str(error)[:180],
        'research_objective_achieved':False}
    atomic(ROOT/('result_'+attempt+'.json'),result)
    return 0 if result['state'] in ('RUNNING','VERIFIED') else 1

def request_stop():_stop.set()

def join_shutdown(timeout=0):return not _lock.locked()

if __name__=='__main__':
    if len(sys.argv)==3 and sys.argv[1]=='--rpc-helper':raise SystemExit(_rpc_helper(sys.argv[2]))
    if len(sys.argv)==3 and sys.argv[1]=='--capture-child':raise SystemExit(_capture_child(sys.argv[2]))
    raise SystemExit('Only the bounded registered capture entry points are permitted')
