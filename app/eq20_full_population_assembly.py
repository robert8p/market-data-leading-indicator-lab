"""Bounded compact-source assembly in the existing, accounted source child.

Immutable source rows already reside in the governed database. A partial unit
SQLite is a cache of that ledger: a replacement host reads its exact prefix again
with actual readback acknowledgements. Only the final immutable shard is uploaded,
so partial checkpoints do not repeatedly copy an increasing full SQLite file.
"""
from __future__ import annotations
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import zlib

VERSION='EQ20_FULL_POPULATION_ASSEMBLY_V1'
CHUNK=131072
MAX_FILE=32*1024*1024
ROLES=('contract','base_engine','bound_engine','source_policy','scope','manifest')
PRIVATE_FILES={'contract':'research_contract_v1.json','base_engine':'w10_discovery_runner.py',
 'bound_engine':'w10_scope_bound_runner.py','source_policy':'w10_source_unit_policy_exact.py',
 'scope':'w10_frozen_scope_v1.json'}
PINS={'contract':'a3b1fa43d92ba5315005697da952f9574d44f18a1290c1992edb9027f2ee1b34',
 'base_engine':'00e6a4c6e6245b4a8e2a398561fa8a51a01dc502273aaebbd446bd25cc7af346',
 'bound_engine':'5730d2f79ab96557b25679dd984a3c56bb7864c7a1b5b0d34b97c7528c815006',
 'source_policy':'323c48c56e816106096da4148c3b223ae7d2ffe5bb7170df20eb8c9704342abf',
 'scope':'bb797e6337663bfc7cc08c54d083bb8e1711a52fc97ee793e5a19095e90c079f'}


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def digest(raw):return hashlib.sha256(raw).hexdigest()


def need(value,reason):
    if not value:raise ValueError(reason)


def _hash(path):
    need(path.is_file() and not path.is_symlink() and path.stat().st_size<=MAX_FILE,'SOURCE_ASSEMBLY_OWNED_FILE_BOUND')
    h=hashlib.sha256()
    with path.open('rb') as stream:
        while raw:=stream.read(CHUNK):h.update(raw)
    return h.hexdigest()


def _local_root(root,epoch):
    need(isinstance(epoch,str) and re.fullmatch(r'[0-9a-f]{64}',epoch),'SOURCE_ASSEMBLY_EPOCH_REQUIRED')
    path=Path(root)/('assembly_'+epoch)
    need(not path.is_symlink(),'SOURCE_ASSEMBLY_SYMLINK')
    path.mkdir(mode=0o700,parents=True,exist_ok=True)
    return path


def _metadata(name,raw,packed):
    need(0<len(raw)<=MAX_FILE and 0<len(packed)<=MAX_FILE,'SOURCE_ASSEMBLY_TRANSFER_BOUND')
    return dict(name=name,raw_sha256=digest(raw),raw_bytes=len(raw),blob_sha256=digest(packed),
                encoded_bytes=len(packed),chunks=(len(packed)+CHUNK-1)//CHUNK,codec='zlib')


def upload(job,owner,rpc,*,name,purpose,raw,checkpoint):
    """Every accepted piece persists; the next invocation skips exact committed pieces."""
    packed=zlib.compress(raw,3)
    extension='sqlite' if purpose=='DICTIONARY' else 'py' if checkpoint.get('role') in {'base_engine','bound_engine','source_policy'} else 'json'
    meta=_metadata(name+'.'+extension,raw,packed)
    started=rpc('assembly_transfer_begin',owner,dict(attempt_id=job['attempt_id'],name=name,purpose=purpose,file=meta,checkpoint=checkpoint))
    need(isinstance(started.get('transfer_id'),str),'SOURCE_ASSEMBLY_TRANSFER_RESERVATION_REQUIRED')
    if started.get('complete') is True:return dict(committed=True,complete=True,file=meta)
    present=started.get('committed_parts',[])
    need(isinstance(present,list) and all(type(i)is int and 0<=i<meta['chunks'] for i in present),'SOURCE_ASSEMBLY_TRANSFER_CURSOR')
    for number in range(meta['chunks']):
        if number in present:continue
        value=packed[number*CHUNK:(number+1)*CHUNK]
        ack=rpc('assembly_transfer_part',owner,dict(attempt_id=job['attempt_id'],transfer_id=started['transfer_id'],part_no=number,
            payload_base64=base64.b64encode(value).decode(),payload_sha256=digest(value)))
        need(ack.get('committed') is True and ack.get('part_no')==number,'SOURCE_ASSEMBLY_PART_ACK_REQUIRED')
    # Verify actual locally encoded/decoded bytes; the server independently hashes
    # every stored part and the complete ordered compressed payload.
    need(zlib.decompress(packed)==raw,'SOURCE_ASSEMBLY_RAW_READBACK_REQUIRED')
    return rpc('assembly_transfer_commit',owner,dict(attempt_id=job['attempt_id'],transfer_id=started['transfer_id'],
        raw_sha256=digest(raw),raw_readback_verified=True))


def _dictionary(job,owner,rpc,root,scratch_safe):
    epoch=job['assembly_job']['epoch_id'];directory=_local_root(root,epoch)
    status=rpc('assembly_unit_batch',owner,dict(attempt_id=job['attempt_id']))
    prefix=status.get('prefix');need(isinstance(prefix,str) and re.fullmatch(r'[0-9a-f]',prefix),'SOURCE_DICTIONARY_PREFIX_REQUIRED')
    path=directory/('units_'+prefix+'.sqlite')
    need(not path.is_symlink(),'SOURCE_DICTIONARY_SYMLINK')
    # This ceiling includes SQLite's worst-case rollback copy and the compressed
    # final upload. Its admission precedes all page materialisation.
    need(scratch_safe(96*1024*1024),'SOURCE_DICTIONARY_SHARED_SCRATCH_ADMISSION')
    if not path.exists():
        with sqlite3.connect(path) as con:
            con.execute('PRAGMA page_size=4096');con.execute('PRAGMA journal_mode=DELETE')
            con.execute('CREATE TABLE units(unit_id TEXT PRIMARY KEY,unit_key TEXT)')
            con.execute('CREATE TABLE checkpoint(singleton INTEGER PRIMARY KEY CHECK(singleton=1),payload TEXT NOT NULL)')
            con.execute('INSERT INTO checkpoint VALUES(1,?)',(canonical(dict(last_unit='',unit_count=0,pending=None)).decode(),))
    need(path.stat().st_size<=MAX_FILE,'SOURCE_DICTIONARY_LOCAL_SIZE_BOUND')
    with sqlite3.connect(path) as con:
        state=json.loads(con.execute('SELECT payload FROM checkpoint WHERE singleton=1').fetchone()[0])
        need(con.execute('SELECT count(*) FROM units').fetchone()[0]==state['unit_count'],'SOURCE_DICTIONARY_ACTUAL_ROWCOUNT_REQUIRED')
    if state.get('pending'):
        pending=state['pending'];pending['sqlite_readback_sha256']=_hash(path)
        ack=rpc('assembly_dictionary_checkpoint',owner,dict(attempt_id=job['attempt_id'],sqlite_readback_verified=True,**pending))
        need(ack.get('committed') is True,'SOURCE_DICTIONARY_EXACT_PAGE_ACK_REQUIRED')
        with sqlite3.connect(path) as con:
            state['pending']=None;con.execute('UPDATE checkpoint SET payload=? WHERE singleton=1',(canonical(state).decode(),))
        status=rpc('assembly_unit_batch',owner,dict(attempt_id=job['attempt_id'],after_unit=state['last_unit']))
    previous=status.get('previous',{})
    if state['last_unit']!=status.get('after_unit'):
        status=rpc('assembly_unit_batch',owner,dict(attempt_id=job['attempt_id'],after_unit=state['last_unit']))
        previous=status.get('previous',{})
    need(state['last_unit']<=previous.get('last_unit',''),'SOURCE_DICTIONARY_LOCAL_CURSOR_AHEAD_WITHOUT_PENDING')
    if previous.get('build_complete') is True and state['last_unit']==previous.get('last_unit'):
        need(state['unit_count']==previous['unit_count'],'SOURCE_DICTIONARY_COMPLETE_COUNT_REQUIRED')
        # Local checkpoint metadata is cleared deterministically before sealing.
        # The global dictionary consumer reads only the immutable units table.
        raw=path.read_bytes()
        result=upload(job,owner,rpc,name='units_'+prefix,purpose='DICTIONARY',raw=raw,
          checkpoint=dict(prefix=prefix,last_unit=state['last_unit'],unit_count=state['unit_count']))
        if result.get('complete') is True:
            path.unlink()
        return dict(result,state='RUNNING')
    rows=status.get('units');need(isinstance(rows,list) and len(rows)<=256,'SOURCE_UNIT_BATCH_BOUND')
    last=state['last_unit'];seen=set()
    for row in rows:
        need(isinstance(row,dict) and isinstance(row.get('unit_id'),str)
             and re.fullmatch(prefix+'[0-9a-f]{63}',row['unit_id']) and row['unit_id']>last
             and isinstance(row.get('unit_key_text'),str) and len(row['unit_key_text'].encode())<=8192
             and digest(row['unit_key_text'].encode())==row['unit_id'] and row['unit_id']not in seen,'SOURCE_EXACT_UNIT_PAGE_REQUIRED')
        # JSON must be the same compact canonical content certified by the
        # original source-unit policy; no source-unit identity is reconstructed.
        need(canonical(json.loads(row['unit_key_text'])).decode()==row['unit_key_text'],'SOURCE_CANONICAL_UNIT_KEY_REQUIRED')
        last=row['unit_id'];seen.add(last)
    pending=dict(after_unit=state['last_unit'],last_unit=last,units=rows)
    with sqlite3.connect(path) as con:
        for row in rows:con.execute('INSERT INTO units VALUES(?,?)',(row['unit_id'],row['unit_key_text']))
        state=dict(last_unit=last,unit_count=state['unit_count']+len(rows),pending=pending)
        con.execute('UPDATE checkpoint SET payload=? WHERE singleton=1',(canonical(state).decode(),))
    need(path.stat().st_size<=MAX_FILE,'SOURCE_DICTIONARY_LOCAL_SIZE_BOUND')
    ack=rpc('assembly_dictionary_checkpoint',owner,dict(attempt_id=job['attempt_id'],sqlite_readback_sha256=_hash(path),sqlite_readback_verified=True,**pending))
    need(ack.get('committed') is True,'SOURCE_DICTIONARY_ACTUAL_COMMIT_REQUIRED')
    with sqlite3.connect(path) as con:
        state['pending']=None;con.execute('UPDATE checkpoint SET payload=? WHERE singleton=1',(canonical(state).decode(),))
    return dict(state='RUNNING',committed=True,unit_rows_committed=len(rows),protected_outcomes_accessed=False)


def manifests(reply):
    """Bind actual transport/denominators to an independently reviewed input policy."""
    context=reply['context'];pages=reply['partition_pages'];shards=reply['unit_dictionary_shards']
    need(isinstance(context,dict) and isinstance(pages,list) and len(pages)==187 and isinstance(shards,list) and len(shards)==16,
         'SOURCE_COMPLETE_COMPACT_ASSEMBLY_REQUIRED')
    registration=json.loads(json.dumps(context['preparation_registration']))
    readiness=registration['source_readiness'];contract=registration['contract']
    manifest=json.loads(json.dumps(context['manifest_template']))
    counts={};decisions={};next_row=next_index=0
    population={name:0 for name in ('VERIFIED_PRIMARY','UNRESOLVED_MEMBERSHIP','KNOWN_NONPRIMARY')}
    classes={name:0 for name in ('CERTIFIED_ARCHIVED_FINAL_BAR_PROXY','CERTIFIED_PUBLICATION_REPLAY','UNCERTIFIED_SOURCE_ABSTENTION')}
    gaps={name:0 for name in ('missing_raw_sessions','unresolved_identity_sessions','unknown_security_type_sessions')}
    candidates=0
    for page in pages:
        need(page['first_row']==next_row and page['first_index']==next_index,'SOURCE_FINAL_PAGE_CURSOR_GAP')
        need(not(set(counts)&set(page['sessions_by_date'])),'SOURCE_DUPLICATE_DAY_INDEX_PAGE')
        counts.update(page['sessions_by_date']);decisions.update(page['decisions_by_date'])
        for name in population:population[name]+=page['population_disposition_counts'][name]
        for name in classes:classes[name]+=page['source_evidence_class_counts'][name]
        for name in gaps:gaps[name]+=page['gap_counts'][name]
        candidates+=page['candidate_frame_sessions']
        next_row=page['last_row']+1;next_index=page['last_index']+1
    need(next_row==reply['expected_session_count'] and next_index==reply['partition_count']
         and sum(counts.values())==next_row,'SOURCE_FINAL_DENOMINATOR_REQUIRED')
    # Scientific fields are inherited only from the separately registered input
    # binding, never set to true because transport or compilation succeeded.
    need(readiness['expected_session_count']==next_row and readiness['session_dates']==sorted(counts)
         and manifest['contract_sha256']==PINS['contract'],'SOURCE_INDEPENDENT_DENOMINATOR_BINDING_REQUIRED')
    need(readiness['verified_primary_sessions']==population['VERIFIED_PRIMARY']
         and readiness['unresolved_membership_sessions']==population['UNRESOLVED_MEMBERSHIP']
         and readiness['known_nonprimary_sessions']==population['KNOWN_NONPRIMARY']
         and readiness['candidate_frame_sessions']==candidates==sum(population.values())
         and all(readiness[name]==value for name,value in gaps.items())
         and readiness['source_evidence_class_counts']==classes,'SOURCE_ACTUAL_POPULATION_AND_EVIDENCE_LEDGER_COUNTS_REQUIRED')
    common=dict(input_format='TECHNICAL11_PLUS_COMPACT_CORRECTED_SOURCE_V1',partition_pages=pages,
        partition_count=next_index,expected_session_count=next_row,expected_decision_count=sum(decisions.values()),
        unit_dictionary_shards=shards,partition_membership_root_sha256=digest(canonical(pages)),
        source_corpus_sha256=reply['source_corpus_sha256'],fixed_sensitivity_receipt_key=reply['clock_receipt_key'],
        fixed_sensitivity_receipt_sha256=reply['clock_receipt_sha256'])
    manifest.update(common,expected_session_count_by_date=counts,expected_decision_count_by_date=decisions)
    readiness.update(common)
    manifest['schema']='EQ20_FP01_PAGED_COMPACT_V2'
    need(manifest.get('clock_policy_sha256')==readiness.get('clock_policy_sha256')==contract.get('development_clock_policy_sha256'),
         'SOURCE_REGISTERED_CLOCK_POLICY_REQUIRED')
    need(manifest.get('source_evidence_class_counts')==readiness.get('source_evidence_class_counts'),
         'SOURCE_EXACT_INDEPENDENT_EVIDENCE_CLASS_COUNTS_REQUIRED')
    return manifest,registration


def step(job,owner,rpc,root,private_root,scratch_safe):
    reply=rpc('assembly_step',owner,dict(attempt_id=job['attempt_id']))
    phase=reply.get('phase')
    if reply.get('complete') is True or reply.get('committed') is True:return dict(reply,state='RUNNING')
    if phase=='DICTIONARY':return _dictionary(job,owner,rpc,root,scratch_safe)
    if phase=='COMMON':
        role=reply.get('role');need(role in ROLES,'SOURCE_COMMON_ROLE_REQUIRED')
        if role=='manifest':raw=canonical(manifests(reply)[0])
        else:
            path=Path(private_root)/PRIVATE_FILES[role]
            need(path.is_file() and not path.is_symlink() and path.stat().st_size<=8*1024*1024,'SOURCE_COMMON_ORIGINAL_BYTES_REQUIRED')
            raw=path.read_bytes();need(digest(raw)==PINS[role],'SOURCE_COMMON_EXACT_ORIGINAL_PIN')
        return dict(upload(job,owner,rpc,name='common_'+role,purpose='COMMON',raw=raw,checkpoint=dict(role=role)),state='RUNNING')
    if phase=='PREPARED':
        manifest,registration=manifests(reply);files=reply['input_files']
        roles={item['role']:item for item in files};need(set(roles)==set(ROLES),'SOURCE_COMMON_SIX_FILE_READBACK_REQUIRED')
        need(roles['manifest']['raw_sha256']==digest(canonical(manifest)),'SOURCE_FINAL_ACTUAL_MANIFEST_PIN_REQUIRED')
        registration['source_readiness'].update(input_files=files,source_manifest_sha256=roles['manifest']['raw_sha256'])
        registration['contract']['source_manifest_sha256']=roles['manifest']['raw_sha256']
        return rpc('assembly_step',owner,dict(attempt_id=job['attempt_id'],registration=registration,complete_manifest_constructed=True))
    need(reply.get('state')=='BLOCKED_BY_IDENTIFIED_DEPENDENCY' or reply.get('ready') is False,'SOURCE_ASSEMBLY_EXPLICIT_STATE_REQUIRED')
    return reply
