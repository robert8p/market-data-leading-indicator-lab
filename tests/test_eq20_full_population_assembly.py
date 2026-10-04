"""Real bounded SQLite construction and interrupted transport; synthetic units only."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import random
import sqlite3
import sys
import tempfile
import unittest
import zlib
from datetime import date,timedelta

APP=Path(__file__).parents[1]/'app'
spec=importlib.util.spec_from_file_location('source_assembly_test',APP/'eq20_full_population_assembly.py')
assembly=importlib.util.module_from_spec(spec);spec.loader.exec_module(assembly)

class YieldForTest(RuntimeError):pass

class DictionaryRPC:
    def __init__(self,rows):
        self.rows=rows;self.last='';self.count=0;self.build_complete=False
        self.transfer=None;self.parts={};self.part_calls=[];self.fail_once=True;self.finished=None;self.readbacks=0
    def __call__(self,op,owner,args):
        if op=='assembly_step':return dict(phase='DICTIONARY')
        if op=='assembly_unit_batch':
            after=args.get('after_unit',self.last)
            rows=[r for r in self.rows if r['unit_id']>after and (after==self.last or r['unit_id']<=self.last)][:256]
            return dict(phase='DICTIONARY',epoch_id='a'*64,prefix='0',after_unit=after,units=rows,
                batch_complete=len(rows)<256 and after==self.last,
                previous=dict(last_unit=self.last,unit_count=self.count,build_complete=self.build_complete))
        if op=='assembly_dictionary_checkpoint':
            expected=[r for r in self.rows if r['unit_id']>args['after_unit'] and (args['after_unit']==self.last or r['unit_id']<=self.last)][:256]
            assert args['units']==expected
            assert args['sqlite_readback_verified'] is True and len(args['sqlite_readback_sha256'])==64
            if args['after_unit']==self.last and not self.build_complete:
                self.last=args['last_unit'];self.count+=len(expected);self.build_complete=len(expected)<256
            self.readbacks+=1
            return dict(committed=True)
        if op=='assembly_transfer_begin':
            value={name:args[name] for name in ('name','purpose','file','checkpoint')}
            if self.transfer is None:self.transfer=value
            else:assert self.transfer==value,'Restored immutable SQLite changed bytes'
            return dict(transfer_id='00000000-0000-0000-0000-000000000001',committed_parts=sorted(self.parts),complete=self.finished is not None)
        if op=='assembly_transfer_part':
            number=args['part_no']
            if number==1 and self.fail_once:
                self.fail_once=False;raise YieldForTest('bounded child yielded after a committed chunk')
            raw=base64.b64decode(args['payload_base64']);assert assembly.digest(raw)==args['payload_sha256']
            assert number not in self.parts,'Already committed piece was replayed'
            self.parts[number]=raw;self.part_calls.append(number)
            return dict(committed=True,part_no=number)
        if op=='assembly_transfer_commit':
            meta=self.transfer['file'];packed=b''.join(self.parts[i] for i in range(meta['chunks']))
            assert assembly.digest(packed)==meta['blob_sha256']
            raw=zlib.decompress(packed)
            assert len(raw)==meta['raw_bytes'] and assembly.digest(raw)==meta['raw_sha256']==args['raw_sha256']
            assert args['raw_readback_verified'] is True
            self.finished=raw
            return dict(committed=True,complete=True,file=meta)
        raise AssertionError(op)

class AssemblyTests(unittest.TestCase):
    def test_actual_assembler_manifest_passes_v2_page_plan_and_retains_exclusions(self):
        def load(name,path):
            spec=importlib.util.spec_from_file_location(name,path)
            result=importlib.util.module_from_spec(spec);sys.modules[name]=result;spec.loader.exec_module(result);return result
        mission=load('assembly_manifest_mission',APP/'eq20_mission_continuation.py')
        adapter=load('assembly_manifest_adapter',APP/'eq20_fp01_artifact_adapter_v2.py');adapter._MISSION=mission
        dates=[(date(2025,9,2)+timedelta(days=i)).isoformat() for i in range(187)]
        pages=[]
        for i,day in enumerate(dates):
            pages.append(dict(artifact_key='SYNTHETIC_SOURCE_PAGE_'+day,sha256='a'*64,membership_sha256='b'*64,
             first_index=i,last_index=i,first_row=2*i,last_row=2*i+1,first_key=[day,'A'],last_key=[day,'B'],
             session_count=2,decision_count=640,sessions_by_date={day:2},decisions_by_date={day:640},
             population_disposition_counts=dict(VERIFIED_PRIMARY=1,UNRESOLVED_MEMBERSHIP=1,KNOWN_NONPRIMARY=1),
             source_evidence_class_counts=dict(CERTIFIED_ARCHIVED_FINAL_BAR_PROXY=1,CERTIFIED_PUBLICATION_REPLAY=0,UNCERTIFIED_SOURCE_ABSTENTION=1),
             gap_counts=dict(missing_raw_sessions=1,unresolved_identity_sessions=1,unknown_security_type_sessions=1),candidate_frame_sessions=3))
        shards=[dict(prefix=p,file=dict(name='units_'+p+'.sqlite',raw_sha256='c'*64,blob_sha256='d'*64,
                    raw_bytes=4096,encoded_bytes=512,chunks=1,codec='zlib')) for p in '0123456789abcdef']
        clock=mission.object_hash(adapter.DEVELOPMENT_CLOCK_POLICY)
        classes=dict(CERTIFIED_ARCHIVED_FINAL_BAR_PROXY=187,CERTIFIED_PUBLICATION_REPLAY=0,UNCERTIFIED_SOURCE_ABSTENTION=187)
        readiness=dict(expected_session_count=374,session_dates=dates,verified_primary_sessions=187,
            unresolved_membership_sessions=187,known_nonprimary_sessions=187,candidate_frame_sessions=561,
            missing_raw_sessions=187,unresolved_identity_sessions=187,unknown_security_type_sessions=187,
            source_evidence_class_counts=classes,clock_policy_sha256=clock)
        reply=dict(context=dict(preparation_registration=dict(contract=dict(development_clock_policy_sha256=clock),source_readiness=readiness),
              manifest_template=dict(contract_sha256=assembly.PINS['contract'],clock_policy_sha256=clock,source_evidence_class_counts=classes)),
            partition_pages=pages,unit_dictionary_shards=shards,source_corpus_sha256='e'*64,clock_receipt_key='SYNTHETIC_CLOCK',
            clock_receipt_sha256='f'*64,expected_session_count=374,partition_count=187)
        manifest,registration=assembly.manifests(reply)
        actual_pages,actual_counts,actual_shards=adapter._partition_plan(manifest,registration['source_readiness'])
        self.assertEqual(actual_pages,pages);self.assertEqual(sum(actual_counts.values()),374)
        self.assertEqual(set(actual_shards),set('0123456789abcdef'))
        self.assertEqual(registration['source_readiness']['known_nonprimary_sessions'],187)
        self.assertEqual(registration['source_readiness']['unresolved_membership_sessions'],187)
        # Mismatching actual and independently bound populations cannot be fixed
        # by silently dropping unknown or known non-primary census records.
        reply['context']['preparation_registration']['source_readiness']['candidate_frame_sessions']=560
        with self.assertRaisesRegex(ValueError,'POPULATION_AND_EVIDENCE_LEDGER'):
            assembly.manifests(reply)

    def test_real_dictionary_restores_after_host_loss_and_skips_committed_transport(self):
        rng=random.Random(20261004);rows=[]
        for number in range(100):
            payload=''.join(rng.choice('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(5000))
            nonce=0
            while True:
                raw=assembly.canonical(dict(source='SYNTHETIC_ONLY',number=number,payload=payload,nonce=nonce))
                key=assembly.digest(raw)
                if key.startswith('0'):break
                nonce+=1
            rows.append(dict(unit_id=key,unit_key_text=raw.decode()))
        rows.sort(key=lambda row:row['unit_id'])
        rpc=DictionaryRPC(rows);job=dict(attempt_id='00000000-0000-0000-0000-000000000002',assembly_job=dict(epoch_id='a'*64))
        with tempfile.TemporaryDirectory() as old, tempfile.TemporaryDirectory() as restored:
            first=assembly.step(job,'synthetic',rpc,old,old,lambda n:n==96*1024*1024)
            self.assertTrue(first['committed']);self.assertEqual(rpc.count,100)
            with self.assertRaises(YieldForTest):assembly.step(job,'synthetic',rpc,old,old,lambda n:True)
            self.assertEqual(rpc.part_calls,[0]);self.assertIsNone(rpc.finished)
            # Replacement host reconstructs the exact partial dictionary from its
            # immutable source-unit ledger. It does not recompile any market input.
            second=assembly.step(job,'synthetic',rpc,restored,restored,lambda n:True)
            self.assertTrue(second['committed']);self.assertEqual(rpc.count,100)
            result=assembly.step(job,'synthetic',rpc,restored,restored,lambda n:True)
            self.assertTrue(result['complete']);self.assertEqual(rpc.part_calls.count(0),1)
            path=Path(restored)/'final.sqlite';path.write_bytes(rpc.finished)
            with sqlite3.connect(path) as con:
                actual=con.execute('SELECT unit_id,unit_key FROM units ORDER BY unit_id').fetchall()
            self.assertEqual(actual,[(r['unit_id'],r['unit_key_text']) for r in rows])
            self.assertGreaterEqual(rpc.readbacks,2)
    def test_scratch_admission_precedes_sqlite_creation(self):
        rpc=DictionaryRPC([]);job=dict(attempt_id='synthetic',assembly_job=dict(epoch_id='a'*64))
        with tempfile.TemporaryDirectory() as root:
            with self.assertRaisesRegex(ValueError,'SHARED_SCRATCH_ADMISSION'):
                assembly.step(job,'synthetic',rpc,root,root,lambda n:False)
            self.assertEqual(list(Path(root).rglob('*.sqlite')),[])
    def test_code_common_files_retain_python_extension_and_exact_original_pin(self):
        raw=b'print("SYNTHETIC")\n';rpc=DictionaryRPC([]);rpc.fail_once=False
        result=assembly.upload({'attempt_id':'synthetic'},'synthetic',lambda op,o,a:self._upload_code_rpc(op,a,raw),name='common_base_engine',purpose='COMMON',raw=raw,checkpoint=dict(role='base_engine'))
        self.assertTrue(result['committed'])
    def _upload_code_rpc(self,op,args,raw):
        if op=='assembly_transfer_begin':
            self.assertEqual(args['file']['name'],'common_base_engine.py')
            return dict(transfer_id='synthetic',committed_parts=[])
        if op=='assembly_transfer_part':return dict(committed=True,part_no=args['part_no'])
        if op=='assembly_transfer_commit':return dict(committed=True,complete=True)
        self.fail(op)

if __name__=='__main__':unittest.main()
