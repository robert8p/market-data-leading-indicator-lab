"""Negative transport/admission checks for the governed pre-release QA lane.

The separately archived private-engine harness exercises actual 6-context engine
and SQLite byte restore equivalence. These tests target failure paths and scope.
"""
import base64
import copy
import importlib.util
import json
from pathlib import Path
import socket
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid
import zlib

PATH=Path(__file__).resolve().parents[1]/'app'/'eq20_fp01_prerelease_qa.py'
SPEC=importlib.util.spec_from_file_location('tested_fp01_prerelease_qa',PATH)
qa=importlib.util.module_from_spec(SPEC);sys.modules[SPEC.name]=qa;SPEC.loader.exec_module(qa)


def checkpoint():
    return dict(stage_commits={},completed_folds=[],trial_records=0,active_stage=None,
                protected_outcomes_accessed=False)


def manifest():
    cp=checkpoint();files=[]
    for name,body in [('checkpoint.json',dict(cp,checkpoint_sha256=qa.object_hash(cp))),('wave_scope.json',{'synthetic':True})]:
        raw=qa.canonical_bytes(body);packed=zlib.compress(raw)
        files.append(dict(name=name,raw_bytes=len(raw),raw_sha256=qa.digest(raw),encoded_bytes=len(packed),
            blob_sha256=qa.digest(packed),parts=[dict(part_no=0,bytes=len(packed),sha256=qa.digest(packed))]))
    value=dict(codec='FILES_ZLIB_PARTS_V1',synthetic_only=True,files=files,sealed_files=[],checkpoint=cp,
        raw_bytes=sum(v['raw_bytes'] for v in files),encoded_bytes=sum(v['encoded_bytes'] for v in files),
        files_manifest_sha256=qa.object_hash(files),checkpoint_sha256=qa.object_hash(cp),
        canonical_checkpoint_utf8=qa.canonical_bytes(cp).decode(),work_vector=qa.qa_work(cp))
    value['raw_sha256']=qa.object_hash([{k:v for k,v in item.items() if k in ('name','raw_bytes','raw_sha256')} for item in files])
    value['payload_sha256']=qa.object_hash(value)
    return value


def rehash(value):
    value['files_manifest_sha256']=qa.object_hash(value['files'])
    value['payload_sha256']=qa.object_hash({k:v for k,v in value.items() if k!='payload_sha256'})
    return value


def request_bound():
    return dict(artifact_key='SYNTHETIC_BOUND_FIXTURE_ONLY',artifact_sha256='9'*64,
        verified_actual_http_request=True,query_timeout_seconds=2,request_start_deadline_ms=500,
        maximum_clock_error_ms=100,post_helper_sql_tail_seconds=3,
        per_request_server_permit_required=True,client_clock_error_not_used_for_admission=True,
        clock_observation_is_historical_only=True,
        late_queued_request_termination_inferred_from_helper_exit=False,host_instance=socket.gethostname(),
        host_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip())


def registered_job():
    return dict(action='PRERELEASE_QA',admission_role='PRERELEASE_QA',qa_admission_verified=True,
        release_verified=False,resource_reservation_verified=True,qa_wrapper_sha256=qa.digest(PATH.read_bytes()),
        reserved_cpu_seconds=30,host_instance=socket.gethostname(),release_artifact_sha256='a'*64,
        request_timeout_verification=request_bound(),
        qa_state=dict(revision=0,phase='COMMON_READBACK'),registration={'contract':dict(wave='FP01',
        maximum_cpu_seconds=7200,maximum_scratch_bytes=2147483648,dates=['2025-09-01','2026-05-31'],
        confirmation_or_holdout_access_allowed=False,engine_sha256=qa.BASE_SHA,bound_engine_sha256=qa.BOUND_SHA,
        source_policy_sha256=qa.POLICY_SHA,template_source_scope_sha256=qa.SCOPE_SHA,
        source_manifest_sha256='b'*64,population_manifest_sha256='c'*64,source_adapter_sha256='d'*64,
        external_quantiles_sha256='e'*64)})


class SnapshotGateTests(unittest.TestCase):
    def test_valid_small_streaming_manifest(self):
        value=manifest();self.assertEqual(qa.decode_snapshot(value),value)

    def test_named_traversal_rejected_even_with_manifest_rehashed(self):
        value=manifest();value['files'][0]['name']='../checkpoint.json';rehash(value)
        with self.assertRaisesRegex(qa.GateClosed,'FILENAME'):qa.decode_snapshot(value)

    def test_duplicate_name_rejected_even_with_manifest_rehashed(self):
        value=manifest();value['files'][1]['name']=value['files'][0]['name'];rehash(value)
        with self.assertRaisesRegex(qa.GateClosed,'FILENAME'):qa.decode_snapshot(value)

    def test_part_gap_rejected(self):
        value=manifest();value['files'][0]['parts'][0]['part_no']=1;rehash(value)
        with self.assertRaisesRegex(qa.GateClosed,'PART_RANGE'):qa.decode_snapshot(value)

    def test_expansion_bound_rejected_before_decompression(self):
        value=manifest();value['files'][0]['raw_bytes']=qa.MAX_QA_FILE+1;rehash(value)
        with self.assertRaisesRegex(qa.GateClosed,'FILE_BOUND'):qa.decode_snapshot(value)

    def test_claimed_raw_file_set_hash_must_match(self):
        value=manifest();value['raw_sha256']='f'*64;rehash(value)
        with self.assertRaisesRegex(qa.GateClosed,'RAW_FILE_SET_HASH'):qa.decode_snapshot(value)

    def test_checkpoint_canonical_text_cannot_change(self):
        value=manifest();value['canonical_checkpoint_utf8']='{}';rehash(value)
        with self.assertRaisesRegex(qa.GateClosed,'CANONICAL_CHECKPOINT'):qa.decode_snapshot(value)

    def test_work_vector_cannot_claim_uncommitted_records(self):
        value=manifest();value['work_vector'][2]=6000;rehash(value)
        with self.assertRaisesRegex(qa.GateClosed,'LOGICAL_READBACK'):qa.decode_snapshot(value)

    def test_original_checkpoint_checksum_is_verified(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'checkpoint.json';cp=checkpoint()
            path.write_bytes(qa.canonical_bytes(dict(cp,checkpoint_sha256=qa.object_hash(cp))))
            self.assertEqual(qa._read_checkpoint_body(path),cp)
            cp['trial_records']=1;path.write_bytes(qa.canonical_bytes(dict(cp,checkpoint_sha256='f'*64)))
            with self.assertRaisesRegex(qa.GateClosed,'CHECKSUM'):qa._read_checkpoint_body(path)


class AdmissionGateTests(unittest.TestCase):
    def test_separate_qa_admission_is_accepted(self):
        qa.validate_job(registered_job())

    def test_scientific_release_role_is_rejected(self):
        job=registered_job();job['release_verified']=True
        with self.assertRaisesRegex(qa.GateClosed,'SEPARATE_ADMISSION'):qa.validate_job(job)

    def test_qa_does_not_borrow_another_wave(self):
        job=registered_job();job['registration']['contract']['wave']='FP02'
        with self.assertRaisesRegex(qa.GateClosed,'SEPARATE_WAVE'):qa.validate_job(job)

    def test_foreign_host_receipt_is_rejected(self):
        job=registered_job();job['host_instance']='unobserved-previous-host'
        with self.assertRaisesRegex(qa.GateClosed,'CURRENT_HOST'):qa.validate_job(job)

    def test_untested_request_timeout_is_not_an_execution_bound(self):
        job=registered_job();job['request_timeout_verification']['verified_actual_http_request']=False
        with self.assertRaisesRegex(qa.GateClosed,'REQUEST_TIMEOUT'):qa.validate_job(job)

    def test_helper_death_does_not_make_a_queued_request_quiescent(self):
        job=registered_job();job['request_timeout_verification']['late_queued_request_termination_inferred_from_helper_exit']=True
        with self.assertRaisesRegex(qa.GateClosed,'REQUEST_TIMEOUT'):qa.validate_job(job)

    def test_query_only_two_second_tail_is_rejected(self):
        job=registered_job();job['request_timeout_verification']['post_helper_sql_tail_seconds']=2
        with self.assertRaisesRegex(qa.GateClosed,'REQUEST_TIMEOUT'):qa.validate_job(job)

    def test_historical_clock_error_is_not_an_admission_assumption(self):
        job=registered_job();job['request_timeout_verification']['maximum_clock_error_ms']=101
        qa.validate_job(job)

    def test_actual_per_request_server_permit_is_mandatory(self):
        job=registered_job();job['request_timeout_verification']['per_request_server_permit_required']=False
        with self.assertRaisesRegex(qa.GateClosed,'SERVER_PERMIT'):qa.validate_job(job)

    def test_a_past_clock_observation_cannot_authorize_future_work(self):
        job=registered_job();job['request_timeout_verification']['client_clock_error_not_used_for_admission']=False
        with self.assertRaisesRegex(qa.GateClosed,'SERVER_PERMIT'):qa.validate_job(job)

    def test_larger_budget_is_rejected(self):
        job=registered_job();job['registration']['contract']['maximum_cpu_seconds']=7201
        with self.assertRaisesRegex(qa.GateClosed,'SCOPE_AND_RESOURCES'):qa.validate_job(job)


class ServerPermitTransportTests(unittest.TestCase):
    """Exercise the actual shared codec through the actual QA direct caller."""
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.directory=Path(self.temp.name)
        self.root=patch.object(qa,'ROOT',self.directory);self.root.start()
        self.path=patch.object(qa,'_rpc_transport_path',self.directory/'transport.json');self.path.start()
        self.scans=[]
        self.guards=patch.object(qa,'_source_guards',SimpleNamespace(
            scratch_safe=lambda extra=0:self.scans.append(extra) is None));self.guards.start()
        self.rpc=object.__new__(qa.MissionRPC);self.calls=[]

    def tearDown(self):
        self.guards.stop();self.path.stop();self.root.stop();self.temp.cleanup()

    def post(self,name,raw):
        body=json.loads(raw);self.calls.append((name,raw,len(self.scans)))
        if name=='eq20_request_permit_v3':
            args=body['p_args']
            self.permit=dict(permit_id='11111111-1111-4111-8111-111111111111',
                host_instance=args['host_instance'],boot_id=args['boot_id'],target_rpc=args['target_rpc'],
                request_sha256=args['request_sha256'],request_jsonb_sha256='a'*64,
                issued_at='2026-10-04T16:00:00.000000Z',expires_at='2026-10-04T16:00:00.500000Z')
            return dict(ready=True,permit=self.permit)
        return {'committed':True,'synthetic_only':True}

    def test_server_permit_preserves_ascii_logical_hash_and_does_not_scan_between_requests(self):
        args={'text':'\u0394','value':7,'request_start_deadline_at':'UNTRUSTED_OLD_CLIENT_CLOCK',
              '_eq20_request_permit':{'old':True}};before=copy.deepcopy(args)
        self.rpc._post=self.post
        result=self.rpc.direct_call('terminal_commit','SYNTHETIC_OWNER',args)
        self.assertTrue(result['committed']);self.assertEqual(args,before)
        self.assertEqual([v[0] for v in self.calls],['eq20_request_permit_v3',qa.RPC_NAME])
        logical=dict(p_op='terminal_commit',p_owner='SYNTHETIC_OWNER',p_args={'text':'\u0394','value':7})
        self.assertIn(b'\\u0394',qa.canonical_bytes(logical))
        mint=json.loads(self.calls[0][1]);final=json.loads(self.calls[1][1])
        self.assertEqual(mint['p_args']['request_document'],logical)
        self.assertEqual(mint['p_args']['request_sha256'],qa.object_hash(logical))
        self.assertEqual(final['p_args']['_eq20_request_permit'],self.permit)
        self.assertEqual(final['p_args']['request_start_deadline_at'],self.permit['expires_at'])
        self.assertEqual(self.calls[0][2],self.calls[1][2])
        receipt=json.loads((self.directory/'transport.json').read_bytes())
        self.assertEqual(receipt['logical_request_sha256'],qa.object_hash(logical))
        self.assertTrue(receipt['all_started_calls_accounted'])
        self.assertFalse(receipt['client_clock_used_for_admission'])
        self.assertEqual([v['state'] for v in receipt['calls']],['RESPONSE_READ_BACK']*2)

    def test_unknown_mint_does_not_send_mutation_or_retry(self):
        def missing_ack(name,raw):
            self.calls.append((name,raw));raise TimeoutError('synthetic unknown mint')
        self.rpc._post=missing_ack
        with self.assertRaises(TimeoutError):self.rpc.direct_call('tick','SYNTHETIC_OWNER',{'revision':1})
        self.assertEqual([v[0] for v in self.calls],['eq20_request_permit_v3'])
        receipt=json.loads((self.directory/'transport.json').read_bytes())
        self.assertFalse(receipt['all_started_calls_accounted'])
        self.assertEqual(receipt['calls'][0]['state'],'STARTED_RESPONSE_UNKNOWN')
        self.assertNotIn('permit',receipt)

    def test_status_is_one_explicitly_read_only_call(self):
        self.rpc._post=self.post
        self.rpc.direct_call('status','SYNTHETIC_OWNER',{})
        self.assertEqual([v[0] for v in self.calls],[qa.RPC_NAME])
        receipt=json.loads((self.directory/'transport.json').read_bytes())
        self.assertEqual([v['stage'] for v in receipt['calls']],['READ_ONLY_METADATA'])
        self.assertNotIn('permit',receipt)


class ScientificComparisonTests(unittest.TestCase):
    def fit_document(self,count=0):
        return dict(stage_key='0_fit',kind='quantiles',processed_sessions=12,
            result=dict(snapshot={'external_store':{'finalized_fields':count,'sequence':12,'chain_sha256':'a'*64}},
                        evidence={'unit_count':12,'retained_value_hash':'b'*64}))

    def test_only_proven_cache_counter_is_normalized(self):
        baseline=self.fit_document(0);resumed=self.fit_document(1)
        self.assertNotEqual(qa.object_hash(baseline),qa.object_hash(resumed))
        self.assertEqual(qa.fit_stage_semantic_hash(baseline,'0_fit',83),qa.fit_stage_semantic_hash(resumed,'0_fit',83))
        self.assertEqual(baseline['result']['snapshot']['external_store']['finalized_fields'],0)
        self.assertEqual(resumed['result']['snapshot']['external_store']['finalized_fields'],1)

    def test_actual_unit_evidence_changes_are_never_ignored(self):
        baseline=self.fit_document(0);changed=self.fit_document(1);changed['result']['evidence']['unit_count']=11
        self.assertNotEqual(qa.fit_stage_semantic_hash(baseline,'0_fit',83),qa.fit_stage_semantic_hash(changed,'0_fit',83))

    def test_source_chain_changes_are_never_ignored(self):
        baseline=self.fit_document();changed=self.fit_document(1)
        changed['result']['snapshot']['external_store']['chain_sha256']='c'*64
        self.assertNotEqual(qa.fit_stage_semantic_hash(baseline,'0_fit',83),qa.fit_stage_semantic_hash(changed,'0_fit',83))

    def test_unknown_or_unbounded_cache_counter_is_rejected(self):
        for count in (None,-1,84,True):
            with self.subTest(count=count),self.assertRaisesRegex(qa.GateClosed,'CACHE_COUNTER'):
                qa.fit_stage_semantic_hash(self.fit_document(count),'0_fit',83)


class DurableRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.directory=Path(self.temp.name)
        self.root_patch=patch.object(qa,'ROOT',self.directory);self.root_patch.start()
        self.guards_patch=patch.object(qa,'_source_guards',SimpleNamespace(scratch_safe=lambda extra=0:True));self.guards_patch.start()
        qa._stop.clear()

    def tearDown(self):
        qa._stop.clear();self.guards_patch.stop();self.root_patch.stop();self.temp.cleanup()

    def test_stop_before_child_has_durable_no_child_terminal_and_exact_replay(self):
        attempt=str(uuid.uuid4());owner='render_eq20_mission_test';calls=[]
        def unknown_ack(op,who,args):
            calls.append((op,who,args));raise RuntimeError('simulated_unknown_ack')
        qa._stop.set()
        with self.assertRaisesRegex(RuntimeError,'unknown_ack'):
            qa.execute_reserved_child(SimpleNamespace(call=unknown_ack),owner,dict(attempt_id=attempt,reserved_cpu_seconds=30))
        saved=json.loads((self.directory/('terminal_'+attempt+'.json')).read_bytes())
        self.assertIs(saved['args']['receipt']['process_started'],False)
        self.assertIsNone(saved['args']['receipt']['process_identity'])
        recovery=dict(attempt_id=attempt,reserved_cpu_seconds=30,recovery_id=str(uuid.uuid4()),
                      host_instance=socket.gethostname(),original_owner=owner,request_timeout_verification=request_bound())
        def ack(op,who,args):calls.append((op,who,args));return {'settled':True}
        self.assertEqual(qa.recover_recorded_attempt(SimpleNamespace(call=ack),recovery),{'settled':True})
        op,who,args=calls[-1];self.assertEqual(op,'terminal_recover');self.assertEqual(who,owner)
        self.assertEqual(args['receipt'],saved['args']['receipt'])
        self.assertEqual(args['recovery_receipt']['process_termination_proof'],'QA_SAVED_NO_CHILD_TERMINAL_RECEIPT')
        self.assertEqual(args['recovery_receipt']['terminal_file_sha256'],qa.digest((self.directory/('terminal_'+attempt+'.json')).read_bytes()))

    def test_missing_request_bound_after_reservation_commits_no_child_receipt(self):
        attempt=str(uuid.uuid4());calls=[]
        def call(op,owner,args):calls.append((op,owner,args));return {'settled':True}
        with patch.object(qa,'_last_rpc_proof',None):
            result=qa.execute_reserved_child(SimpleNamespace(call=call),'owner',dict(attempt_id=attempt,reserved_cpu_seconds=30))
        self.assertEqual(result,{'settled':True})
        self.assertIs(calls[0][2]['receipt']['process_started'],False)
        self.assertIn('REQUEST_TIMEOUT',calls[0][2]['receipt']['error'])
        self.assertTrue((self.directory/('terminal_'+attempt+'.json')).is_file())

    def test_admitted_control_overrun_never_launches_research_child(self):
        attempt=str(uuid.uuid4());calls=[]
        def call(op,owner,args):calls.append((op,owner,args));return {'settled':True}
        proof=dict(operation='tick',control_bound_exceeded=True,control_governed_prefix_seconds=7.1)
        with patch.object(qa,'_last_rpc_proof',proof):
            result=qa.execute_reserved_child(SimpleNamespace(call=call),'owner',dict(attempt_id=attempt,reserved_cpu_seconds=30))
        self.assertEqual(result,{'settled':True});receipt=calls[0][2]['receipt']
        self.assertIs(receipt['process_started'],False)
        self.assertEqual(receipt['admission_control_termination'],proof)
        self.assertEqual(receipt['error'],'QA_OBSERVED_ADMISSION_CONTROL_BOUND_VIOLATION')

    def lost_admission_fixture(self):
        attempt=str(uuid.uuid4());invocation=uuid.uuid4().hex;owner='render_eq20_mission_test'
        arguments=dict(invocation_id=invocation,host_instance=socket.gethostname(),version=qa.VERSION)
        request=dict(op='tick',owner=owner,args=arguments)
        proof=dict(operation='tick',request_sha256=qa.object_hash(request),response_return_authorized=False,
            process_finished=True,process_termination_proof='SPECIFIC_CHILD_WAIT4',host_instance=socket.gethostname(),
            process_identity=dict(pid=12345,process_group=12345,start_ticks=900,
                boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip()))
        qa.atomic_file(self.directory/('admission_request_'+invocation+'.json'),qa.canonical_bytes(request))
        qa.atomic_file(self.directory/('admission_proof_'+invocation+'.json'),qa.canonical_bytes(proof))
        job=dict(attempt_id=attempt,reserved_cpu_seconds=30,recovery_id=str(uuid.uuid4()),
            original_owner=owner,original_invocation_id=invocation,original_tick_arguments=arguments,
            host_instance=socket.gethostname(),request_timeout_verification=request_bound())
        return job,proof

    def test_lost_admission_ack_proves_no_child_from_exact_control_handoff(self):
        job,proof=self.lost_admission_fixture();calls=[]
        def call(op,owner,args):calls.append((op,owner,args));return {'settled':True}
        with patch.object(qa.time,'sleep') as sleeper:
            result=qa.recover_recorded_attempt(SimpleNamespace(call=call),job)
        self.assertEqual(result,{'settled':True});sleeper.assert_called_once_with(qa.SQL_TAIL_SECONDS)
        receipt=calls[0][2]['recovery_receipt']
        self.assertEqual(receipt['process_termination_proof'],'QA_UNDELIVERED_ADMISSION_ACTUAL_HELPER_WAIT4')
        self.assertEqual(receipt['admission_helper_termination'],proof)
        self.assertIs(calls[0][2]['receipt']['process_started'],False)
        self.assertTrue((self.directory/('terminal_'+job['attempt_id']+'.json')).is_file())

    def test_a_return_authorized_admission_cannot_infer_no_child_from_missing_pid(self):
        job,proof=self.lost_admission_fixture();proof['response_return_authorized']=True
        path=self.directory/('admission_proof_'+job['original_invocation_id']+'.json');path.write_bytes(qa.canonical_bytes(proof))
        with self.assertRaisesRegex(qa.GateClosed,'UNDELIVERED_ADMISSION_HELPER'):
            qa.recover_recorded_attempt(None,job)

    def test_received_marker_prevents_lost_ack_no_child_recovery(self):
        job,proof=self.lost_admission_fixture()
        qa.atomic_file(self.directory/('admission_received_'+job['original_invocation_id']+'.json'),b'{}')
        with self.assertRaisesRegex(qa.GateClosed,'RECEIVED_OR_CHILD_LAUNCH'):
            qa.recover_recorded_attempt(None,job)

    def test_admission_request_must_match_the_actual_server_invocation(self):
        job,proof=self.lost_admission_fixture();job['original_tick_arguments']=dict(job['original_tick_arguments'],version='OTHER')
        with self.assertRaisesRegex(qa.GateClosed,'ORIGINAL_ADMISSION_REQUEST_BINDING'):
            qa.recover_recorded_attempt(None,job)

    def test_recovery_cannot_use_original_reservation_twice(self):
        with self.assertRaisesRegex(qa.GateClosed,'SEPARATELY_RESERVED'):
            qa.recover_recorded_attempt(None,dict(attempt_id=str(uuid.uuid4()),reserved_cpu_seconds=30))

    def test_remote_pending_requires_actual_local_sealed_bytes(self):
        job=dict(release_artifact_sha256='a'*64,qa_state={'pending_snapshot':{'intent_sha256':'b'*64}})
        with self.assertRaisesRegex(qa.GateClosed,'SEALED_LOCAL_BYTES'):qa._pending(job)

    def test_lost_intent_ack_adopts_exact_remote_intent_without_new_work(self):
        snapshot=manifest();pending=dict(prepared_sha256='a'*64,host_instance=socket.gethostname(),
            expected_revision=2,from_phase='SYNTHETIC_BASELINE',snapshot=snapshot)
        job=dict(release_artifact_sha256='a'*64,qa_state=dict(revision=2,phase='SYNTHETIC_BASELINE',
            pending_snapshot=dict(snapshot=snapshot,intent_sha256='c'*64)))
        transfer=qa._transfer_root(job);qa.atomic_file(transfer/'pending.json',qa.canonical_bytes(pending))
        recovered=qa._pending(job)
        self.assertEqual(recovered['intent_sha256'],'c'*64)
        self.assertEqual(json.loads((transfer/'pending.json').read_bytes()),recovered)

    def test_different_remote_intent_does_not_replace_pending_work(self):
        pending=dict(prepared_sha256='a'*64,host_instance=socket.gethostname(),expected_revision=2,
            from_phase='SYNTHETIC_BASELINE',snapshot=manifest())
        job=dict(release_artifact_sha256='a'*64,qa_state=dict(revision=2,phase='SYNTHETIC_BASELINE',
            pending_snapshot=dict(snapshot={'payload_sha256':'f'*64},intent_sha256='c'*64)))
        qa.atomic_file(qa._transfer_root(job)/'pending.json',qa.canonical_bytes(pending))
        with self.assertRaisesRegex(qa.GateClosed,'REMOTE_PENDING'):qa._pending(job)

    def recovery_replay_fixture(self):
        attempt=str(uuid.uuid4());recovery_id=str(uuid.uuid4());owner='render_eq20_mission_test'
        args=dict(attempt_id=attempt,recovery_id=recovery_id,receipt={'test_fixture':True})
        saved=dict(operation='terminal_recover',owner=owner,args=args)
        boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        proof=dict(operation='terminal_recover',attempt_id=attempt,
            request_sha256=qa.object_hash(dict(op='terminal_recover',owner=owner,args=args)),
            host_instance=socket.gethostname(),process_finished=True,process_termination_proof='SPECIFIC_CHILD_WAIT4',
            process_identity=dict(pid=12345,process_group=12345,start_ticks=999,boot_id=boot))
        qa.atomic_file(self.directory/('recovery_terminal_'+attempt+'.json'),qa.canonical_bytes(saved))
        qa.atomic_file(self.directory/('recovery_rpc_proof_'+attempt+'.json'),qa.canonical_bytes(proof))
        job=dict(attempt_id=attempt,recovery_id=recovery_id,replay_id=str(uuid.uuid4()),reserved_cpu_seconds=30,
            original_owner=owner,host_instance=socket.gethostname(),request_timeout_verification=request_bound())
        return job,saved,proof

    def test_second_reserved_recovery_replays_exact_request_without_child(self):
        job,saved,proof=self.recovery_replay_fixture();calls=[]
        def call(op,owner,args):calls.append((op,owner,args));return {'settled':True}
        with patch.object(qa.time,'sleep') as sleeper:
            result=qa.replay_recorded_recovery_terminal(SimpleNamespace(call=call),job)
        self.assertEqual(result,{'settled':True});sleeper.assert_called_once_with(qa.SQL_TAIL_SECONDS)
        op,owner,args=calls[0]
        self.assertEqual(op,saved['operation']);self.assertEqual(owner,saved['owner'])
        self.assertEqual({k:v for k,v in args.items() if k not in ('replay_id','replay_receipt')},saved['args'])
        self.assertIs(args['replay_receipt']['research_child_reexecuted'],False)
        self.assertIs(args['replay_receipt']['research_child_signalled'],False)
        self.assertEqual(qa.digest(args['replay_receipt']['rpc_request_canonical_utf8'].encode()),proof['request_sha256'])

    def test_saved_recovery_request_cannot_be_rebound_after_helper_proof(self):
        job,saved,proof=self.recovery_replay_fixture();saved['args']['receipt']['test_fixture']=False
        (self.directory/('recovery_terminal_'+job['attempt_id']+'.json')).write_bytes(qa.canonical_bytes(saved))
        with self.assertRaisesRegex(qa.GateClosed,'HTTP_HELPER_PROOF'):
            qa.replay_recorded_recovery_terminal(None,job)

    def test_second_recovery_also_requires_its_own_reservation(self):
        job,saved,proof=self.recovery_replay_fixture();job.pop('replay_id')
        with self.assertRaisesRegex(qa.GateClosed,'SEPARATELY_RESERVED_RECOVERY_REPLAY'):
            qa.replay_recorded_recovery_terminal(None,job)


if __name__=='__main__':unittest.main()
