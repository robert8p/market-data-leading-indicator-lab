"""Synthetic transport/cursor tests; these do not certify market throughput."""
from copy import deepcopy
from pathlib import Path
import base64
import importlib.util
import json
import tempfile
import time
import unittest
from unittest.mock import patch
import zlib

ROOT=Path(__file__).resolve().parents[1]
def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);obj=importlib.util.module_from_spec(spec);spec.loader.exec_module(obj);return obj
b=module('batch_test_module',ROOT/'app/eq20_prospective_batch.py')
f=module('batch_fixture_kernel',ROOT/'tests/test_eq20_prospective_kernel.py')
k=f.k


class Host:
    VERSION='SYNTHETIC_BATCH_TEST'
    def __init__(self,root,db): self.ROOT=root;self.db=db
    def RPC(self): return self.db
    def guards(self): return self
    def scratch_safe(self,n): return n<=b.MAX_SCRATCH
    def load_local(self,name):
        if name=='eq20_prospective_kernel':return k
        raise AssertionError(name)


class Database:
    def __init__(self,job,segments,payloads):
        self.job=job;self.segments=segments;self.payloads=payloads;self.calls=[];self.outputs={}
        self.cursor=dict(job['progress']['cursor']);self.lose_ack=False
    def readback(self,e):
        raw=b.canonical(e).decode();return {'receipt_evidence_text':raw,'receipt_sha256':b.sha(raw.encode())}
    def direct(self,op,owner,args):
        self.calls.append((op,args)); self.assert_attempt=args['attempt_id']
        if op=='input_part':
            content=self.payloads[args['compressed_sha256']];offset=args['part_no']*b.CHUNK;raw=content[offset:offset+b.CHUNK]
            e={'work_sha256':self.job['work_reference']['implementation_sha256'],'compressed_sha256':args['compressed_sha256'],
               'part_no':args['part_no'],'bytes':len(raw),'part_sha256':b.sha(raw)}
            return dict(self.readback(e),payload_base64=base64.b64encode(raw).decode(),part_sha256=b.sha(raw))
        if op=='commit_outputs':
            if self.cursor==args['expected_cursor']:
                for output in args['outputs']:
                    if output['ordinal'] in self.outputs: raise AssertionError('duplicate output')
                    self.outputs[output['ordinal']]=output
                self.cursor=dict(args['next_cursor'])
            elif self.cursor!=args['next_cursor']: raise AssertionError('CAS mismatch')
            e={'work_sha256':self.job['work_reference']['implementation_sha256'],'cursor':self.cursor,
               'receipt_identities':[[r['ordinal'],r['security_id'],r['receipt_sha256']] for r in args['outputs']]}
            if self.lose_ack:
                self.lose_ack=False;raise ValueError('CONSUMER_RPC_TRANSPORT_FAILURE')
            return dict(self.readback(e),committed=True)
        raise AssertionError(op)


def fixture(count=5,split=2):
    contract,payload=f.fixture();rows=[]
    for i in range(count):
        row=deepcopy(payload);row['security_id']='S'+str(i).zfill(5);row['features']['security_id']=row['security_id'];rows.append(row)
    population=f.population(contract,rows)
    segments=[];contents={}
    for index,start in enumerate(range(0,count,split)):
        group=rows[start:start+split];raw=b''.join(b.canonical({'ordinal':start+i,'payload':v})+b'\n' for i,v in enumerate(group));compressed=zlib.compress(raw)
        desc={'segment_index':index,'first_ordinal':start,'last_ordinal':start+len(group)-1,
              'first_security_id':group[0]['security_id'],'last_security_id':group[-1]['security_id'],'codec':'zlib',
              'compressed_sha256':b.sha(compressed),'compressed_bytes':len(compressed),'raw_sha256':b.sha(raw),'raw_bytes':len(raw)}
        segments.append(desc);contents[desc['compressed_sha256']]=compressed
    work={'session_date':rows[0]['session_date'],'population':population,'input_stream':{'segments':segments}}
    job={'action':'PROCESS_SECURITY_BATCH','owner':'synthetic_owner','attempt_id':'00000000-0000-0000-0000-000000000001','activation_key':'SYNTHETIC',
         'work_reference':{'artifact_key':'SYNTHETIC_BATCH','implementation_sha256':b.sha(b.canonical(work))},
         'progress':{'cursor':{'next_ordinal':0,'segment_index':0,'raw_offset':0}},'_deadline':time.monotonic()+60,
         '_feasibility':{'maximum_security_wall_seconds':.1,'maximum_segment_decode_wall_seconds':.1}}
    return contract,rows,work,job,Database(job,segments,contents)


def producer(host,job,contract,raw):
    return k.process_security_session(contract,raw),{'raw_source_sha256':b.sha(b.canonical(raw))}


class BatchTest(unittest.TestCase):
    def test_full_natural_stream_all_outputs_and_cursor_committed(self):
        c,rows,w,j,db=fixture()
        with tempfile.TemporaryDirectory() as temp,patch.object(b,'produce',side_effect=producer):
            result=b.run_batch(Host(Path(temp),db),j,c,w)
        self.assertEqual(result['state'],'VERIFIED');self.assertEqual(len(db.outputs),len(rows))
        self.assertEqual(db.cursor,{'next_ordinal':len(rows),'segment_index':len(w['input_stream']['segments']),'raw_offset':0})
        self.assertEqual([b.decode_receipt(v) for _,v in sorted(db.outputs.items())],[k.process_security_session(c,p) for p in rows])

    def test_lost_commit_ack_then_server_cursor_resume_does_not_replay_security(self):
        c,rows,w,j,db=fixture();db.lose_ack=True;seen=[]
        def track(host,job,contract,raw):seen.append(raw['security_id']);return producer(host,job,contract,raw)
        with tempfile.TemporaryDirectory() as temp,patch.object(b,'produce',side_effect=track):
            host=Host(Path(temp),db)
            with self.assertRaisesRegex(ValueError,'TRANSPORT_FAILURE'):b.run_batch(host,j,c,w)
            self.assertEqual(len(db.outputs),2)
            resumed=deepcopy(j);resumed['progress']['cursor']=dict(db.cursor);resumed['_deadline']=time.monotonic()+60
            b.run_batch(host,resumed,c,w)
        self.assertEqual(seen,[p['security_id'] for p in rows]);self.assertEqual(len(db.outputs),5)

    def test_completed_output_partial_work_survives_interruption(self):
        c,rows,w,j,db=fixture(4,4);seen=[]
        def interrupted(host,job,contract,raw):
            if len(seen)==2:raise b.PreparationYield('COMMITTED_BOUNDED_PREPARATION_OR_WORK_YIELD')
            seen.append(raw['security_id']);return producer(host,job,contract,raw)
        with tempfile.TemporaryDirectory() as temp,patch.object(b,'produce',side_effect=interrupted),patch.object(b,'MAX_COMMIT',1):
            result=b.run_batch(Host(Path(temp),db),j,c,w)
        self.assertEqual(result['state'],'RUNNING');self.assertEqual(db.cursor['next_ordinal'],2);self.assertEqual(len(db.outputs),2)

    def test_chunk_cache_resumes_verified_parts(self):
        c,rows,w,j,db=fixture(3,3)
        with tempfile.TemporaryDirectory() as temp,patch.object(b,'CHUNK',37):
            host=Host(Path(temp),db);base=host.ROOT/'cache';segment=w['input_stream']['segments'][0]
            path=b.hydrate(host,j,segment,base);before=len(db.calls)
            self.assertEqual(b.sha(path.read_bytes()),segment['raw_sha256'])
            self.assertEqual(b.hydrate(host,j,segment,base),path);self.assertEqual(len(db.calls),before)

    def test_corrupt_input_and_unregistered_extra_record_fail_before_any_commit(self):
        for mutation in ('corrupt','extra'):
            c,rows,w,j,db=fixture(1,1);seg=w['input_stream']['segments'][0]
            if mutation=='corrupt':
                raw=db.payloads[seg['compressed_sha256']];db.payloads[seg['compressed_sha256']]=raw[:-1]+bytes([raw[-1]^1])
            else:
                original=zlib.decompress(db.payloads[seg['compressed_sha256']]);raw=original+original;compressed=zlib.compress(raw)
                seg.update(raw_bytes=len(raw),raw_sha256=b.sha(raw),compressed_bytes=len(compressed),compressed_sha256=b.sha(compressed));db.payloads[seg['compressed_sha256']]=compressed
            with tempfile.TemporaryDirectory() as temp,patch.object(b,'produce',side_effect=producer):
                with self.assertRaises((ValueError,zlib.error)):b.run_batch(Host(Path(temp),db),j,c,w)
            self.assertEqual(db.outputs,{})

    def test_unknown_missing_or_duplicate_security_never_drops_denominator(self):
        c,rows,w,j,db=fixture(1,1);seg=w['input_stream']['segments'][0]
        record=json.loads(zlib.decompress(db.payloads[seg['compressed_sha256']]))
        record['payload']['security_id']='UNREGISTERED';raw=b.canonical(record)+b'\n';compressed=zlib.compress(raw)
        seg.update(raw_bytes=len(raw),raw_sha256=b.sha(raw),compressed_bytes=len(compressed),compressed_sha256=b.sha(compressed));db.payloads[seg['compressed_sha256']]=compressed
        with tempfile.TemporaryDirectory() as temp,patch.object(b,'produce',side_effect=producer):
            with self.assertRaisesRegex(ValueError,'EXACT_SECURITY_ORDINAL'):b.run_batch(Host(Path(temp),db),j,c,w)
        self.assertEqual(db.outputs,{})

    def test_cache_incarnation_is_stable_across_attempts_and_changes_only_after_loss(self):
        c,rows,w,j,db=fixture(1,1)
        with tempfile.TemporaryDirectory() as temp:
            host=Host(Path(temp),db);first=b.cache_incarnation(host,j)
            next_job={key:value for key,value in j.items() if not key.startswith('_cache_')}
            self.assertEqual(b.cache_incarnation(host,next_job),first)
            (host.ROOT/'cache_incarnations'/(j['work_reference']['implementation_sha256']+'.json')).unlink()
            lost_job={key:value for key,value in j.items() if not key.startswith('_cache_')}
            self.assertNotEqual(b.cache_incarnation(host,lost_job),first)

    def test_original_bar_mode_precedes_execution_and_uses_frozen_family(self):
        c,rows,w,j,db=fixture(1,1);c['reference_outcome_mode']='ORIGINAL_CERTIFIED_MINUTE_BAR_COVERAGE_V1'
        raw=rows[0];raw.pop('candidate_family_sha256');raw['execution_market']={'synthetic':True};calls=[]
        class Source:
            def produce_features(self,contract,payload,binding):calls.append('features');return {'features':raw['features'],'provenance':{'test':True}}
            def build_reference_labels(self,contract,features,payload,binding):
                calls.append('reference');self.assert_raw=payload is raw
                if not self.assert_raw:raise AssertionError('wrong A input mode')
                return {'reference_labels':raw['reference_labels']}
        class Execution:
            def build_execution_labels(self,contract,features,market,indices):
                calls.append('execution');return {'execution_labels':raw['execution_labels'],'receipt_sha256':'1'*64}
        source=Source();execution=Execution()
        class ProducerHost:
            def load_local(self,name,expected=None):
                return {'eq20_prospective_features':source,'eq20_execution_replay':execution,'eq20_prospective_kernel':k}[name]
        j.update(_source_module_sha256='2'*64,_execution_module_sha256='3'*64,source_producer_bindings={'test':True})
        result,proof=b.produce(ProducerHost(),j,c,raw)
        self.assertEqual(calls,['features','reference','execution']);self.assertEqual(result['candidate_family_sha256'],c['candidate_family_sha256'])
        self.assertEqual(proof['raw_source_sha256'],b.sha(b.canonical(raw)))

    def test_compressed_output_is_bound_to_original_kernel_receipt(self):
        c,rows,w,j,db=fixture(1,1);receipt=k.process_security_session(c,rows[0])
        item=b.compact_output(receipt,{'raw_source_sha256':'a'*64},0,0,0,1)
        self.assertEqual(b.decode_receipt(item),receipt)
        item['receipt_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'KERNEL_RECEIPT_HASH'):b.decode_receipt(item)

if __name__=='__main__':unittest.main()
