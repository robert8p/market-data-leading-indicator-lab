"""Server-clock admission permits inside an already bounded HTTP operation.

This module owns no credentials, network client, thread, reservation or retry.
The caller's existing exact helper/child limit covers both HTTP requests. The
server consumes a permit atomically against the exact logical request and
rechecks its server deadline after mutation locks; local wall clocks grant no
admission authority. Unknown mint responses are never looked up or reconstructed.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import re
import time

VERSION='EQ20_RPC_SERVER_ADMISSION_PERMIT_V3'
PERMIT_RPC='eq20_request_permit_v3'
TRANSPORT_FIELDS=frozenset({'request_start_deadline_at','_eq20_request_permit'})
MAX_LOGICAL_BYTES=1792*1024
MAX_HTTP_BYTES=2*1024*1024

class AdmissionClosed(ValueError):
    pass

def require(test,reason):
    if not test:raise AdmissionClosed(reason)

def sha(raw):return hashlib.sha256(raw).hexdigest()

def logical_request(document):
    require(isinstance(document,dict) and set(document)=={'p_op','p_owner','p_args'}
            and isinstance(document['p_args'],dict),'EXACT_RPC_LOGICAL_ENVELOPE_REQUIRED')
    require(isinstance(document['p_op'],str) and 0<len(document['p_op'])<=80
            and isinstance(document['p_owner'],str) and 0<len(document['p_owner'])<=160,
            'EXACT_RPC_OPERATION_OWNER_REQUIRED')
    return dict(document,p_args={key:value for key,value in document['p_args'].items()
                                if key not in TRANSPORT_FIELDS})

def send(logical_document,*,target_rpc,post,canonical_bytes,host_instance,boot_id,
         read_only=False,transport_receipt=None,before_send=None):
    """Send one logical call using the supplied existing fixed-host transport.

    ``post(rpc_name, raw_bytes)`` makes exactly one HTTP call. ``canonical_bytes``
    is supplied by the caller so existing durable logical hashes do not change.
    ``before_send(stage, raw_bytes)`` may persist wire intent before each request.
    A provided receipt is updated before and after every subcall, including an
    unknown response. Elapsed time is measured by the surrounding funded caller.
    """
    require(isinstance(target_rpc,str) and re.fullmatch(r'public\.(eq20_[a-z0-9_]+)\(text,text,jsonb\)',target_rpc),
            'EXACT_REGISTERED_RPC_SIGNATURE_REQUIRED')
    require(isinstance(host_instance,str) and 0<len(host_instance)<=160
            and isinstance(boot_id,str) and re.fullmatch(r'[0-9a-f-]{36}',boot_id),
            'EXACT_RPC_HOST_BOOT_SCOPE_REQUIRED')
    document=logical_request(logical_document);raw=canonical_bytes(document)
    require(isinstance(raw,bytes) and 0<len(raw)<=MAX_LOGICAL_BYTES,'RPC_LOGICAL_REQUEST_BOUND')
    require(json.loads(raw)==document,'CALLER_CANONICAL_ENCODING_CHANGED_LOGICAL_REQUEST')
    target_name=target_rpc.split('.')[1].split('(')[0]
    receipt=transport_receipt if transport_receipt is not None else {}
    receipt.update(version=VERSION,target_rpc=target_rpc,logical_request_sha256=sha(raw),
                   calls=[],all_started_calls_accounted=False,client_clock_used_for_admission=False)

    def request(stage,name,body):
        wire=canonical_bytes(body)
        require(0<len(wire)<=MAX_HTTP_BYTES,'RPC_HTTP_ENVELOPE_BOUND')
        entry={'stage':stage,'rpc_name':name,'request_sha256':sha(wire),
               'request_bytes':len(wire),'state':'STARTED_RESPONSE_UNKNOWN'}
        receipt['calls'].append(entry)
        if before_send is not None:before_send(stage,wire)
        began=time.monotonic()
        try:
            value=post(name,wire)
            require(isinstance(value,dict),'RPC_RESPONSE_OBJECT_REQUIRED')
            response=canonical_bytes(value)
            entry.update(state='RESPONSE_READ_BACK',response_sha256=sha(response))
            return value
        finally:entry['elapsed_seconds']=time.monotonic()-began

    if read_only:
        require(document['p_op'] in ('status','probe'),'ONLY_PURE_METADATA_OPS_ARE_PERMIT_EXEMPT')
        result=request('READ_ONLY_METADATA',target_name,document)
        receipt['all_started_calls_accounted']=True
        return result

    mint={'p_owner':document['p_owner'],'p_args':{'target_rpc':target_rpc,
        'host_instance':host_instance,'boot_id':boot_id,
        'request_document':document,'request_sha256':sha(raw)}}
    response=request('PERMIT_MINT',PERMIT_RPC,mint)
    require(response.get('ready') is True and isinstance(response.get('permit'),dict),
            'ACTUAL_SERVER_ISSUED_ADMISSION_PERMIT_REQUIRED')
    permit=response['permit']
    require(permit.get('host_instance')==host_instance and permit.get('boot_id')==boot_id
            and permit.get('target_rpc')==target_rpc and permit.get('request_sha256')==sha(raw)
            and isinstance(permit.get('permit_id'),str) and re.fullmatch(r'[0-9a-f-]{36}',permit['permit_id'])
            and isinstance(permit.get('request_jsonb_sha256'),str)
            and re.fullmatch(r'[0-9a-f]{64}',permit['request_jsonb_sha256']),
            'SERVER_PERMIT_EXACT_LOGICAL_SCOPE_MISMATCH')
    try:
        issued=datetime.fromisoformat(permit['issued_at'].replace('Z','+00:00'))
        expires=datetime.fromisoformat(permit['expires_at'].replace('Z','+00:00'))
        require(issued.tzinfo is not None and expires.tzinfo is not None
                and 0<(expires-issued).total_seconds()<=.5,'SERVER_PERMIT_FINITE_500MS_WINDOW_REQUIRED')
    except (KeyError,AttributeError,TypeError,ValueError):
        raise AdmissionClosed('SERVER_PERMIT_EXACT_TIMESTAMPS_REQUIRED') from None
    receipt['permit']=dict(permit)
    final=dict(document,p_args=dict(document['p_args'],_eq20_request_permit=dict(permit),
                                    request_start_deadline_at=permit['expires_at']))
    result=request('PERMITTED_MUTATION',target_name,final)
    receipt['all_started_calls_accounted']=True
    return result
