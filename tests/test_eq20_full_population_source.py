"""Synthetic source construction and original-runtime integration; no market IO."""
import base64
import copy
import hashlib
import importlib.util
import json
import os
import random
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import patch
import zlib

APP=Path(__file__).parents[1]/'app'
WORK=APP.parents[1]

def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    value=importlib.util.module_from_spec(spec);sys.modules[name]=value;spec.loader.exec_module(value)
    return value

source=module('full_source_test',APP/'eq20_full_population_source.py')

class SourceOperationalTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.patches=[patch.object(source,'ROOT',self.root),patch.object(source,'_stop',__import__('threading').Event()),
          patch.object(source,'_child_metrics',None)]
        for p in self.patches:p.start()
    def tearDown(self):
        for p in reversed(self.patches):p.stop()
        self.temp.cleanup()
    def test_real_nested_merger_fields_and_pagination_are_retained(self):
        record={'id':'synthetic','acquiree_symbol':'OLD','acquirer_symbol':'NEW','cash':5}
        result=source.parse_alpaca_corporate_action_page({'corporate_actions':{'cash_mergers':[record]},'next_page_token':'synthetic-page-2'},page_sha256='a'*64,captured_at='2026-10-04T00:00:00Z')
        self.assertEqual(result['actions'][0]['provider_record'],record)
        self.assertEqual(result['actions'][0]['observed_symbol_fields'],{'acquiree_symbol':'OLD','acquirer_symbol':'NEW'})
        self.assertFalse(result['transport_page_complete']);self.assertFalse(result['historical_completeness_certified'])
        self.assertFalse(result['source_admitted'])
    def test_flat_or_unknown_actions_cannot_silently_drop_records(self):
        for value in ({'cash_mergers':[{}]},{'corporate_actions':{'unregistered':[{}]}}):
            with self.assertRaises(source.SourceGateClosed):
                source.parse_alpaca_corporate_action_page(value,page_sha256='a'*64,captured_at='2026-10-04T00:00:00Z')
    def test_waiting_w10_never_launches_source_child(self):
        calls=[]
        def rpc(op,owner,args):calls.append(op);return {'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY','reason':'REAL_W10_COMPLETE_REQUIRED'}
        # The execution workspace has a host-mounted /proc that deliberately
        # fails production namespace checks. This metadata-only fixture supplies
        # identity; the live runtime still requires its real proc namespace.
        with patch.object(source.source_guards(),'process_identity',return_value=dict(pid=42,start_ticks=1,process_group=42,boot_id='synthetic')),\
             patch.object(source.source_guards(),'scratch_safe',return_value=True),\
             patch.object(source,'execute_source_child',side_effect=AssertionError('unexpected source launch')):
            value=source.run_once('synthetic',scheduled_at=time.time(),trigger='PERSISTENT_WORKER_TIMER',rpc=rpc)
        self.assertEqual(calls,['tick']);self.assertEqual(value['reason'],'REAL_W10_COMPLETE_REQUIRED')
    def test_original_host_identity_is_mandatory_before_any_recovery_rpc(self):
        with self.assertRaisesRegex(source.SourceGateClosed,'PRIOR_HOST_TERMINATION'):
            source.recover_source_child({'host_instance':'not-this-host'},'synthetic',lambda *a: self.fail('RPC before quiescence'))
    def test_denied_maintenance_reservation_cannot_kill_a_real_owned_child(self):
        import subprocess,socket
        child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(10)'],start_new_session=True)
        attempt='00000000-0000-0000-0000-000000000003'
        try:
            identity=dict(pid=child.pid,start_ticks=1,process_group=child.pid,boot_id='synthetic')
            (self.root/('process_'+attempt+'.json')).write_bytes(source.canonical(identity))
            calls=[]
            def denied(op,owner,args):
                calls.append(op);return dict(state='BLOCKED_BY_IDENTIFIED_DEPENDENCY',reason='FINITE_ALLOCATION_EXHAUSTED')
            with patch.object(source.source_guards(),'assert_proc_namespace'),patch.object(source.source_guards(),'process_identity',return_value=identity),\
                 patch.object(source.source_guards(),'quiesce_recorded_child',side_effect=AssertionError('Quiescence before reservation')):
                result=source.recover_source_child(dict(attempt_id=attempt,host_instance=socket.gethostname(),original_owner='synthetic-old'),'synthetic-new',denied)
            self.assertEqual(calls,['recovery_reserve']);self.assertEqual(result['reason'],'FINITE_ALLOCATION_EXHAUSTED')
            self.assertIsNone(child.poll(),'Unreserved recovery signalled a live process')
        finally:
            child.kill();child.wait()
    def test_exact_unlaunched_reservation_reconciles_without_inventing_a_pid(self):
        import socket
        attempt='00000000-0000-0000-0000-000000000004';invocation='1'*32
        args=dict(invocation_id=invocation)
        request_hash=source.sha(source.canonical(dict(op='tick',owner='synthetic-old',args=args)))
        identity=dict(pid=42,start_ticks=1,process_group=42,boot_id='synthetic')
        intent=dict(owner='synthetic-old',args=args,request_sha256=request_hash,parent_identity=identity,host_instance=socket.gethostname())
        helper=dict(operation='tick',request_sha256=request_hash,process_finished=True,process_termination_proof='SPECIFIC_CHILD_WAIT4',sql_tail_waited_seconds=3)
        (self.root/('intent_'+invocation+'.json')).write_bytes(source.canonical(intent))
        (self.root/('intent_rpc_proof_'+invocation+'.json')).write_bytes(source.canonical(helper))
        calls=[]
        def rpc(op,owner,args):
            calls.append((op,args))
            if op=='recovery_reserve':return dict(reserved_governed_seconds=30,recovery_id='synthetic-recovery')
            return dict(committed=True)
        with patch.object(source.time,'sleep') as wait,patch.object(source.source_guards(),'assert_proc_namespace'),\
             patch.object(source.source_guards(),'process_identity',return_value=identity):
            result=source.recover_source_child(dict(attempt_id=attempt,host_instance=socket.gethostname(),original_owner='synthetic-old',
                original_invocation_id=invocation,original_request_sha256=request_hash),'synthetic-new',rpc)
        self.assertTrue(result['committed']);self.assertEqual([x[0] for x in calls],['recovery_reserve','recovery_proof','recovery_terminal'])
        self.assertEqual(calls[0][1]['proof']['proof_scope'],'EXACT_UNLAUNCHED_RESERVATION')
        self.assertEqual(calls[-1][1]['receipt']['process_identity']['kind'],'UNLAUNCHED_RESERVATION')
        self.assertNotIn('pid',calls[-1][1]['receipt']['process_identity']);wait.assert_called_once_with(3)
        (self.root/('launch_intent_'+attempt+'.json')).write_text('{}')
        with self.assertRaisesRegex(source.SourceGateClosed,'NO_LAUNCH_INTENT_PROOF'),\
             patch.object(source.source_guards(),'assert_proc_namespace'):
            source.recover_source_child(dict(attempt_id=attempt,host_instance=socket.gethostname(),original_owner='synthetic-old',
                original_invocation_id=invocation,original_request_sha256=request_hash),'synthetic-new',lambda *a:self.fail('Reserve despite ambiguous launch'))
    def test_owned_cleanup_is_bounded_and_rejects_symlink(self):
        attempt='00000000-0000-0000-0000-000000000001'
        own=self.root/('partition_'+attempt);own.mkdir();(own/'unit_registry.sqlite').write_bytes(b'synthetic')
        other=self.root/'unrelated';other.mkdir();(other/'keep').write_bytes(b'preserve')
        source.cleanup_owned_partition(attempt)
        self.assertFalse(own.exists());self.assertEqual((other/'keep').read_bytes(),b'preserve')
        own.mkdir();(own/'unit_registry.sqlite').symlink_to(other/'keep')
        with self.assertRaisesRegex(source.SourceGateClosed,'OWNED_CLEANUP'):
            source.cleanup_owned_partition(attempt)
        self.assertEqual((other/'keep').read_bytes(),b'preserve')
    def test_private_manifest_rejects_traversal_before_file_read(self):
        files=[dict(name='ok%d.py'%i,bytes=1,sha256='a'*64) for i in range(9)]
        files[0]['name']='../outside.py'
        with self.assertRaisesRegex(source.SourceGateClosed,'MANIFEST_FILE'):
            source._code_ready(dict(bundle_sha256='a'*64,files=files))
    def test_incomplete_rpc_census_cannot_erase_known_cpu_overrun(self):
        result=source.known_accounting_floor(25.25,1.0,None,None,'synthetic')
        self.assertTrue(result['known_resource_overrun']);self.assertFalse(result['complete_measurement_claimed'])
        self.assertEqual(result['minimum_charge_seconds'],33.25)
        unknown=source.known_accounting_floor(None,0,None,None,'synthetic')
        self.assertEqual(unknown['minimum_charge_seconds'],30);self.assertFalse(unknown['known_resource_overrun'])
    def test_actual_source_transport_uses_exact_server_permit_without_client_clock_admission(self):
        boot='11111111-1111-1111-1111-111111111111'
        identity=dict(pid=42,start_ticks=1,process_group=42,boot_id=boot)
        calls=[];original_args=dict(attempt_id='00000000-0000-0000-0000-000000000001',fixed_input='synthetic')
        def post(base,key,name,raw):
            self.assertEqual(base,'https://oxzabweahkoimtevbbny.supabase.co')
            self.assertLessEqual(len(raw),2*1024*1024)
            body=json.loads(raw);calls.append((name,body))
            if name=='eq20_request_permit_v3':
                logical=body['p_args']['request_document']
                self.assertEqual(body['p_args']['request_sha256'],source.sha(source.canonical(logical)))
                self.assertEqual(logical['p_args']['original_request_sha256'],source.sha(source.canonical(dict(op='compiled_commit',owner='synthetic',args=original_args))))
                # The server's synthetic epoch is deliberately unlike this
                # machine's clock: only the server consumes/rechecks expiry.
                permit=dict(permit_id='22222222-2222-2222-2222-222222222222',host_instance='synthetic-host',boot_id=boot,
                    target_rpc=body['p_args']['target_rpc'],request_sha256=body['p_args']['request_sha256'],
                    request_jsonb_sha256='b'*64,issued_at='2000-01-01T00:00:00Z',expires_at='2000-01-01T00:00:00.500000Z')
                return dict(ready=True,permit=permit)
            self.assertEqual(name,source.RPC_NAME)
            self.assertEqual(body['p_args']['request_start_deadline_at'],'2000-01-01T00:00:00.500000Z')
            self.assertEqual(body['p_args']['request_helper_identity'],identity)
            self.assertEqual({k:v for k,v in body['p_args'].items() if k not in ('_eq20_request_permit','request_start_deadline_at')},calls[0][1]['p_args']['request_document']['p_args'])
            return dict(committed=True,receipt_key='synthetic-committed')
        with patch.dict(os.environ,{'SUPABASE_URL':'https://oxzabweahkoimtevbbny.supabase.co','SUPABASE_SERVICE_ROLE_KEY':'synthetic-only'}),\
             patch.object(source.source_guards(),'process_identity',return_value=identity),patch.object(source.socket,'gethostname',return_value='synthetic-host'),\
             patch.object(source,'_post_fixed',side_effect=post),patch.object(source,'_atomic',side_effect=AssertionError('No scratch publication between permit and send')):
            result=source._direct_unmetered('compiled_commit','synthetic',original_args)
        self.assertTrue(result['committed']);self.assertEqual([v[0] for v in calls],['eq20_request_permit_v3',source.RPC_NAME])
        self.assertEqual(original_args,dict(attempt_id='00000000-0000-0000-0000-000000000001',fixed_input='synthetic'))
    def test_unknown_permit_response_never_sends_source_mutation_or_retries(self):
        identity=dict(pid=42,start_ticks=1,process_group=42,boot_id='11111111-1111-1111-1111-111111111111')
        calls=[]
        def unknown(base,key,name,raw):
            calls.append(name);raise source.SourceGateClosed('SYNTHETIC_UNKNOWN_MINT_RESPONSE')
        with patch.dict(os.environ,{'SUPABASE_URL':'https://oxzabweahkoimtevbbny.supabase.co','SUPABASE_SERVICE_ROLE_KEY':'synthetic-only'}),\
             patch.object(source.source_guards(),'process_identity',return_value=identity),patch.object(source,'_post_fixed',side_effect=unknown):
            with self.assertRaisesRegex(source.SourceGateClosed,'UNKNOWN_MINT_RESPONSE'):
                source._direct_unmetered('tick','synthetic',dict(fixed_input='synthetic'))
        self.assertEqual(calls,['eq20_request_permit_v3'])
    def test_compact_request_oversize_closes_before_any_mint(self):
        identity=dict(pid=42,start_ticks=1,process_group=42,boot_id='11111111-1111-1111-1111-111111111111')
        with patch.dict(os.environ,{'SUPABASE_URL':'https://oxzabweahkoimtevbbny.supabase.co','SUPABASE_SERVICE_ROLE_KEY':'synthetic-only'}),\
             patch.object(source.source_guards(),'process_identity',return_value=identity),patch.object(source,'_post_fixed',side_effect=AssertionError('Oversize admission')):
            with self.assertRaisesRegex(source.SourceGateClosed,'SOURCE_COMPACT_TRANSFER_BOUND'):
                source._direct_unmetered('compiled_commit','synthetic',dict(payload='x'*source.MAX_REQUEST))
    def test_actual_specialist_pages_resume_from_hash_checked_cache_without_source_replay(self):
        from decimal import Decimal
        binding=dict(kinds=['FUNDAMENTAL_FACT'],maximum_issuer_payload_bytes=8*1024*1024)
        binding_text=json.dumps(binding,separators=(', ',': '));binding_hash=source.sha(binding_text.encode())
        fact_text='{"source_key":"SYNTHETIC_FACT","value":1.25}'
        row=dict(payload_text=fact_text,payload_sha256=source.sha(fact_text.encode()))
        requests=[];first_failure=[True]
        def rpc(op,owner,args):
            requests.append((op,args.get('page_no')))
            if op=='specialist_manifest':return dict(binding_text=binding_text,binding_sha256=binding_hash,kinds=binding['kinds'])
            if op=='specialist_page':
                number=args['page_no']
                if number==1 and first_failure[0]:first_failure[0]=False;raise source.SourceGateClosed('SYNTHETIC_INTERRUPTED_PAGE')
                payload=json.dumps([row] if number==0 else [],separators=(', ',': '))
                return dict(binding_sha256=binding_hash,kind='FUNDAMENTAL_FACT',page_no=number,payload_text=payload,
                    payload_sha256=source.sha(payload.encode()),records=1 if number==0 else 0,
                    previous_cursor=None if number==0 else ['SYNTHETIC_FACT','cash'],last_cursor=['SYNTHETIC_FACT','cash'],terminal_empty_page=number==1)
            if op=='specialist_ack':return dict(committed=True,payload_sha256=args['payload_sha256'])
            if op=='specialist_verify':return dict(verified=True,capture_receipt_sha256='c'*64)
            self.fail(op)
        item=dict(local_ordinal=0,admission=dict(specialist_inputs=dict(issuer_cik='1',family_admission={'FUNDAMENTAL':True})))
        with patch.object(source.source_guards(),'scratch_safe',return_value=True):
            with self.assertRaisesRegex(source.SourceGateClosed,'INTERRUPTED_PAGE'):
                source.captured_specialist_inputs(item,{'attempt_id':'synthetic'},'synthetic',rpc)
            output,receipt=source.captured_specialist_inputs(item,{'attempt_id':'synthetic'},'synthetic',rpc)
        self.assertEqual(requests.count(('specialist_page',0)),1)
        self.assertEqual(requests.count(('specialist_ack',0)),2)
        self.assertEqual(output['admission']['specialist_inputs']['facts'][0]['value'],Decimal('1.25'))
        self.assertTrue(output['admission']['specialist_inputs']['capture_complete'])
        self.assertEqual(receipt['binding_sha256'],binding_hash)

class ExactOriginalCompilerTests(unittest.TestCase):
    def setUp(self):
        self.private=WORK/'eq20_full_population_private_bundle'
        if not (self.private/'eq20_full_population_compiler_v1.py').is_file():
            self.skipTest('Exact private code is supplied only in the governed integration workspace')
        self.cmodule=module('full_source_private_test',self.private/'eq20_full_population_compiler_v1.py')
        self.compiler=self.cmodule.Compiler(self.private)
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
    def tearDown(self):
        if hasattr(self,'temp'):self.temp.cleanup()
    def fixture(self,ticker='SYN',ordinal=0,codec=None):
        raw=json.dumps(codec,sort_keys=True,separators=(', ',': ')) if codec is not None else 'null'
        member=[ticker,'synthetic-identity','CS','UNRESOLVED',[],None,None,None,'SYNTHETIC']
        member_text=json.dumps(member,separators=(', ',': '))
        return dict(session_date='2025-09-02',regular_open='2025-09-02T13:30:00+00:00',regular_close='2025-09-02T20:00:00+00:00',
         member=member,member_payload_text=member_text,member_ordinal=ordinal,
         member_sha256=hashlib.sha256(member_text.encode()).hexdigest(),raw_payload_text=raw,raw_payload_sha256=hashlib.sha256(raw.encode()).hexdigest(),pointer_sha256='b'*64,
         population_certificate_sha256='2'*64,calendar={'2025-09-02':dict(certified=True,is_trading_day=True,
          regular_open='2025-09-02T13:30:00+00:00',regular_close='2025-09-02T20:00:00+00:00')})
    def test_exact_missing_identity_emits_all_original_grid_rows_and_real_compact_bytes(self):
        value=self.compiler.compile(self.fixture())
        self.assertEqual(value['state'],'COMPILED_PROVISIONAL_AWAITING_PIT_ADMISSION')
        self.assertEqual(len(value['technical']['decisions']),320)
        self.assertTrue(all(x['state']=='UNRESOLVED' for x in value['labels']['decisions']))
        self.assertNotIn('membership_ordinal',value['technical'])
        self.assertEqual(value['technical']['source_provenance']['membership_ordinal'],0)
        self.assertEqual(value['technical']['source_provenance']['source_evidence_class'],'UNCERTIFIED_SOURCE_ABSTENTION')
        files=self.cmodule.partition_bytes([value],self.root)
        self.assertEqual(set(files),{'features','labels','unit_registry'})
        self.assertGreater(len(files['features']),300000)
        with sqlite3.connect(self.root/'unit_registry.sqlite') as connection:
            row=json.loads(connection.execute('SELECT payload FROM members').fetchone()[0])
        self.assertEqual(len(row['corrected_source']['segments'][0]['features']),106)
        for raw in files.values():self.assertEqual(zlib.decompress(zlib.compress(raw,3)),raw)
    def test_raw_hash_and_protected_dates_fail_closed(self):
        bad=self.fixture();bad['raw_payload_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'RAW_NATIVE_TEXT_HASH'):
            self.compiler.compile(bad)
        bad=self.fixture();bad['session_date']='2026-06-01'
        with self.assertRaisesRegex(ValueError,'ORIGINAL_DISCOVERY_DATES'):
            self.compiler.compile(bad)
    def test_source_frame_proxy_cannot_be_promoted(self):
        bad=self.fixture();bad['admission']=dict(state='VERIFIED',primary_ordinary_common_certified=True,
         point_in_time_identity_certified=True,raw_payload_sha256=bad['raw_payload_sha256'],member_sha256=bad['member_sha256'],
         source_quality_review_verified=True,evidence_class='ADMITTED_SOURCE_SCOPED_ARCHIVED_FINAL_BAR_PROXY')
        with self.assertRaisesRegex(ValueError,'PRIMARY_CLASS_AND_REVIEWED'):
            self.compiler.compile(bad)
    def admitted_fixture(self):
        codec=dict(codec_version='eq20_raw_array_v1',base_ts='2025-09-02T00:00:00-04:00',
            capture_end_exclusive='2025-09-03T00:00:00-04:00',
            columns=['ts_offset_seconds','session_label','open','high','low','close','volume','trade_count','vwap','provenance_index'],
            provenance_columns=['loaded_by_job_id','source_loaded_at'],
            provenance=[['SYNTHETIC_ONLY','2025-09-03T00:00:00Z']],
            rows=[[34200+i*60,'REGULAR',100,100+(i%17)/100,99.5,100,10+i%3,2,100,0] for i in range(390)])
        value=self.fixture(codec=codec)
        value['admission']=dict(state='VERIFIED',primary_ordinary_common_certified=True,point_in_time_identity_certified=True,
            raw_payload_sha256=value['raw_payload_sha256'],member_sha256=value['member_sha256'],source_quality_review_verified=True,
            evidence_class='CERTIFIED_ARCHIVED_FINAL_BAR_PROXY',archive_proxy_justification_reviewed=True,
            frozen_latency_sensitivities_registered=True,identity_certified=True,comparable_price_units=True,
            eligible_regular_trades_certified=True,certificate_sha256='c'*64,
            specialist_inputs=dict(capture_complete=True,facts=[],events=[]),coverage=[dict(start=value['regular_open'],
                end=value['regular_close'],kind='CAPTURE_COMPLETE',evidence_id='SYNTHETIC_ONLY')],
            proxy_justification_sha256='8'*64,fixed_sensitivity_receipt_sha256='9'*64)
        return value
    def test_immutable_caches_preserve_exact_original_features_labels_and_quality_defects(self):
        slow=self.cmodule.Compiler(self.private,memoize_immutable_runtime=False)
        for variant in ('valid','duplicate_latest','invalid_latest','later_touch'):
            value=self.admitted_fixture();codec=json.loads(value['raw_payload_text'])
            if variant=='duplicate_latest':codec['rows'].insert(10,copy.deepcopy(codec['rows'][9]))
            elif variant=='invalid_latest':codec['rows'][9][2]=-1
            elif variant=='later_touch':codec['rows'][280][3]=125
            value['raw_payload_text']=json.dumps(codec,sort_keys=True,separators=(', ',': '))
            value['raw_payload_sha256']=hashlib.sha256(value['raw_payload_text'].encode()).hexdigest()
            value['admission']['raw_payload_sha256']=value['raw_payload_sha256']
            if variant=='duplicate_latest':
                # Keep the original390-row safety bound while testing the exact
                # duplicated latest reference and missing final observation.
                codec['rows'].pop()
                value['raw_payload_text']=json.dumps(codec,sort_keys=True,separators=(', ',': '))
                value['raw_payload_sha256']=hashlib.sha256(value['raw_payload_text'].encode()).hexdigest()
                value['admission']['raw_payload_sha256']=value['raw_payload_sha256']
            expected=slow.compile(value);actual=self.compiler.compile(value)
            self.assertEqual(self.cmodule.canonical(actual),self.cmodule.canonical(expected),variant)
            self.assertFalse(actual['technical']['source_provenance']['publication_replay_certified'])
            self.assertFalse(actual['technical']['source_provenance']['execution_certified'])
            if variant=='valid':self.assertEqual(actual['labels']['decisions'][0]['state'],'NONQUALIFY')
            if variant=='later_touch':self.assertEqual(actual['labels']['decisions'][0]['state'],'QUALIFY')
    def test_indexed_fastpath_keeps_gaps_no_trade_missing_capture_and_partial_touch_semantics(self):
        slow=self.cmodule.Compiler(self.private,memoize_immutable_runtime=False)
        from datetime import datetime,timedelta
        for variant in ('gaps','no_trade_gap','missing_capture','partial_touch','nonmonotone_receipts'):
            value=self.admitted_fixture();codec=json.loads(value['raw_payload_text'])
            if variant=='gaps':
                rng=random.Random(20261004)
                codec['rows']=[row for i,row in enumerate(codec['rows']) if i not in {0,9,389} and rng.random()>.07]
            if variant=='no_trade_gap':
                codec['rows'].pop(100)
                value['admission']['coverage'].append(dict(start='2025-09-02T15:10:00+00:00',end='2025-09-02T15:11:00+00:00',kind='NO_ELIGIBLE_TRADES',evidence_id='SYNTHETIC_ONLY'))
            if variant=='missing_capture':
                value['admission']['coverage'].append(dict(start='2025-09-02T18:00:00+00:00',end='2025-09-02T18:01:00+00:00',kind='MISSING_CAPTURE',evidence_id='SYNTHETIC_ONLY'))
            if variant=='partial_touch':codec['rows'][10][3]=125
            if variant=='nonmonotone_receipts':
                value['admission']['evidence_class']='CERTIFIED_PUBLICATION_REPLAY'
                base=datetime.fromisoformat(codec['base_ts'])
                receipts={}
                for i,row in enumerate(codec['rows']):
                    at=base+timedelta(seconds=row[0]);receipts[at.isoformat()]=(at+timedelta(seconds=95 if i!=9 else 600)).isoformat()
                value['admission']['bar_receipts']=receipts
                value['admission']['actual_receipt_revision_history_verified']=True
            value['raw_payload_text']=json.dumps(codec,sort_keys=True,separators=(', ',': '))
            value['raw_payload_sha256']=hashlib.sha256(value['raw_payload_text'].encode()).hexdigest()
            value['admission']['raw_payload_sha256']=value['raw_payload_sha256']
            self.assertEqual(self.cmodule.canonical(self.compiler.compile(value)),self.cmodule.canonical(slow.compile(value)),variant)
    def test_actual_clock_sensitivity_calls_original_features_without_labels(self):
        source_row=self.admitted_fixture()
        with patch.object(self.compiler.engine,'reference_label',side_effect=AssertionError('clock QA read labels')):
            receipt=self.compiler.clock_sensitivity(source_row,source_corpus_sha256='a'*64)
        self.assertEqual([item['archive_delay_seconds'] for item in receipt['checks']],[35,5,65,125])
        self.assertEqual(len({item['decision_grid_sha256'] for item in receipt['checks']}),1)
        self.assertEqual(receipt['checks'][3]['reference_state_counts'],{'STALE':320})
        self.assertFalse(receipt['labels_accessed']);self.assertFalse(receipt['synthetic_only'])
        self.assertTrue(receipt['actual_fixed_sensitivity_checks_executed'])
        self.assertEqual(receipt['original_contract_raw_sha256'],'a3b1fa43d92ba5315005697da952f9574d44f18a1290c1992edb9027f2ee1b34')
    def test_stale_extreme_decimal_prefix_is_never_evaluated(self):
        from datetime import datetime,timedelta,timezone
        from decimal import Decimal
        eng=self.compiler.engine;opened=datetime(2025,9,2,13,30,tzinfo=timezone.utc)
        session=eng.Session('SYNTHETIC_ONLY',opened,opened+timedelta(minutes=390))
        bars=[eng.Bar(opened+timedelta(minutes=i),opened+timedelta(minutes=i+1),Decimal(100),Decimal(100),Decimal(100),Decimal(100),
            Decimal('1e999998'),True,True,True,source_available_at=opened+timedelta(minutes=i+4)) for i in range(20)]
        policy=eng.ClockPolicy();expected,_=self.compiler.compact.causal_features(session,bars,policy=policy)
        actual=self.compiler.fastpath.construct(eng,self.compiler.compact,session,bars,[],policy,identity_certified=True,comparable_price_units=True)
        self.assertEqual(actual[0],expected);self.assertEqual({row['reference_state'] for row in expected},{'STALE'})
    def test_known_nonprimary_is_audited_and_removed_only_from_primary_stream(self):
        value=self.fixture(ticker='CPZ')
        value['population']=dict(state='VERIFIED',member_sha256=value['member_sha256'],disposition='KNOWN_NONPRIMARY',population_certificate_sha256='2'*64)
        compiled=self.compiler.compile(value)
        self.assertEqual(compiled['state'],'EXCLUDED_KNOWN_NONPRIMARY')
        self.assertEqual(compiled['key'],['2025-09-02','CPZ'])
        self.assertIsNone(compiled['technical']);self.assertIsNone(compiled['labels'])
        files=self.cmodule.partition_bytes([compiled],self.root)
        self.assertEqual(files['features'],b'');self.assertEqual(files['labels'],b'')
        with sqlite3.connect(self.root/'unit_registry.sqlite') as con:self.assertEqual(con.execute('SELECT count(*) FROM members').fetchone()[0],0)
    def test_actual_compiler_unit_registry_and_v2_adapter_validate_together(self):
        tmod=module('full_source_adapter_fixture',Path(__file__).with_name('test_eq20_fp01_artifact_adapter_v2.py'))
        fixture=tmod.ArtifactAdapterTests('test_compact_partitions_use_original_validator_and_keep_unknown_members')
        fixture.setUp()
        try:
            manifest,descriptors=fixture.streaming_fixture()
            mission=tmod.mission;readiness=fixture.job['registration']['source_readiness'];contract=fixture.job['registration']['contract']
            scope=self.compiler.scope
            manifest['features']={name:dict(role='PREDICTOR',approval='CERTIFIED',temporal_certified=True,
              implementation_sha256='3'*64,source_version_key='synthetic',units='SYNTHETIC')
              for name in list(scope['technical_features'])+list(scope['feature_grammar'])}
            counts={'CERTIFIED_ARCHIVED_FINAL_BAR_PROXY':0,'CERTIFIED_PUBLICATION_REPLAY':0,'UNCERTIFIED_SOURCE_ABSTENTION':4}
            manifest['source_evidence_class_counts']=counts;readiness['source_evidence_class_counts']=counts
            readiness.update(verified_primary_sessions=0,unresolved_membership_sessions=4,missing_raw_sessions=4,
                unresolved_identity_sessions=4,unknown_security_type_sessions=4)
            for index,ids in enumerate((('A','B'),('C','D'))):
                members=[self.compiler.compile(self.fixture(ticker,index*2+i)) for i,ticker in enumerate(ids)]
                directory=self.root/str(index);directory.mkdir()
                files=self.cmodule.partition_bytes(members,directory)
                descriptors[index]['files']=[dict(fixture.rpc.source('real_%s_%d.%s'%(role,index,'sqlite' if role=='unit_registry' else 'jsonl'),raw),role=role) for role,raw in files.items()]
            source_files={item['role']:item for item in readiness['input_files']}
            source_files['scope']=dict(fixture.rpc.source('real_scope.json',(self.private/'w10_frozen_scope_v1.json').read_bytes()),role='scope')
            source_files['source_policy']=dict(fixture.rpc.source('real_source_policy.py',(self.private/'w10_source_unit_policy_exact.py').read_bytes()),role='source_policy')
            source_files['manifest']=dict(fixture.rpc.source('real_manifest.json',mission.canonical_bytes(manifest)),role='manifest')
            readiness['input_files']=list(source_files.values())
            contract['template_source_scope_sha256']=source_files['scope']['raw_sha256']
            contract['source_policy_sha256']=source_files['source_policy']['raw_sha256']
            contract['source_manifest_sha256']=source_files['manifest']['raw_sha256']
            loaded=fixture.load_streaming();rows=list(loaded['inputs'].sessions('2025-09-01','2026-05-31'))
            self.assertEqual([row[0]['security_id'] for row in rows],['A','B','C','D'])
            self.assertTrue(all(len(row[0]['decisions'])==320 for row in rows))
            self.assertTrue(all(len(row[0]['decisions'][0]['features'])==117 for row in rows))
            self.assertTrue(all(label['state']=='UNRESOLVED' for _,labels in rows for label in labels['decisions']))
            loaded['inputs']._w10_source_registry.detach()
        finally:
            fixture.doCleanups();fixture.tearDown()

if __name__=='__main__':unittest.main()
