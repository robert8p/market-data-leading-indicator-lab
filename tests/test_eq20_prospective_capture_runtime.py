import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock,patch

path=Path(__file__).resolve().parents[1]/'app'/'eq20_prospective_capture_runtime.py'
spec=importlib.util.spec_from_file_location('capture_runtime_test',path)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class CacheTests(unittest.TestCase):
    def value(self,sequence=1):
        raw='{"actual":true,"first_received_at":"2026-10-05T13:30:01Z"}'
        return {'source_rows':[{'family':'MARKET','row':json.loads(raw),'row_evidence_text':raw,
                               'row_sha256':hashlib.sha256(raw.encode()).hexdigest()}],
                'source_sequence':sequence,'decision_index':0,
                'source_rows_sha256':'a'*64,'raw_prefix_sha256':'b'*64}

    def test_cache_retains_exact_native_rows_and_rejects_corruption(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            cache=m.SourceRevisionCache(Path(d),scratch_safe=lambda n:True)
            value=self.value();self.assertTrue(cache.store('activation','2026-10-05','security',value))
            self.assertEqual(cache.load('activation','2026-10-05','security'),value)
            file=Path(d)/(cache.key('activation','2026-10-05','security')+'.z');file.write_bytes(b'corrupt')
            self.assertIsNone(cache.load('activation','2026-10-05','security'));cache.close()

    def test_mutated_native_row_hash_never_enters_cache(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            cache=m.SourceRevisionCache(Path(d),scratch_safe=lambda n:True)
            value=self.value();value['source_rows'][0]['row_evidence_text']='{"later":true}'
            with self.assertRaisesRegex(m.Closed,'EXACT_RAW_REVISION_HASH'):
                cache.store('activation','2026-10-05','security',value)
            self.assertIsNone(cache.load('activation','2026-10-05','security'));cache.close()

    def test_shared_scratch_refusal_is_cache_miss_not_scientific_admission(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            cache=m.SourceRevisionCache(Path(d),scratch_safe=lambda n:False)
            self.assertFalse(cache.store('activation','2026-10-05','security',self.value()))
            self.assertIsNone(cache.load('activation','2026-10-05','security'));cache.close()

    def test_pending_provider_page_retains_original_bytes_until_commit_ack(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            cache=m.PendingPageCache(Path(d));request_sha='a'*64
            raw=b'{"bars":[{"t":"2026-10-05T13:30:00Z","c":2}]}'
            receipt={'request_sha256':request_sha,'raw_payload_base64':base64.b64encode(raw).decode(),
                     'raw_sha256':m.sha(raw),'raw_bytes':len(raw),'first_received_at':'2026-10-05T13:31:02Z'}
            self.assertTrue(cache.store(request_sha,receipt))
            self.assertEqual(cache.load(request_sha),(receipt,json.loads(raw)))
            revised=b'{"bars":[{"t":"2026-10-05T13:30:00Z","c":3}]}'
            revised_receipt=dict(receipt,raw_payload_base64=base64.b64encode(revised).decode(),
                                 raw_sha256=m.sha(revised),raw_bytes=len(revised))
            with self.assertRaisesRegex(m.Closed,'IMMUTABLE'):
                cache.store(request_sha,revised_receipt)
            self.assertEqual(cache.load(request_sha)[0],receipt)
            pending=cache.pending()
            self.assertEqual(pending,[{'request_sha256':request_sha,'raw_sha256':m.sha(raw)}])
            with self.assertRaisesRegex(m.Closed,'COMMIT_READBACK_SCOPE_MISMATCH'):
                cache.acknowledge(pending,[{'request_sha256':request_sha,'raw_sha256':m.sha(revised)}])
            self.assertIsNotNone(cache.load(request_sha))
            cache.acknowledge(pending,pending);self.assertIsNone(cache.load(request_sha))

    def test_pending_provider_page_capacity_cannot_evict_uncommitted_evidence(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            cache=m.PendingPageCache(Path(d));cache.MAX_PENDING=1;raw=b'{}'
            receipt={'request_sha256':'a'*64,'raw_payload_base64':base64.b64encode(raw).decode(),
                     'raw_sha256':m.sha(raw),'raw_bytes':len(raw)}
            cache.store('a'*64,receipt)
            with self.assertRaisesRegex(m.Closed,'FINITE_PENDING_PROVIDER_PAGE_CACHE_EXHAUSTED'):
                cache.store('b'*64,dict(receipt,request_sha256='b'*64))
            self.assertEqual(cache.load('a'*64)[0],receipt)

    def test_decision_ack_observation_requires_exact_native_ack_before_removal(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            queue=m.PendingDecisionObservations(Path(d))
            value={'version':'EQ20_CAPTURE_DECISION_COMMIT_OBSERVATION_V1','activation_key':'a',
                   'decision_batch_readback':{'receipt_sha256':'b'*64}}
            value['observation_sha256']=m.object_hash(value);queue.store(value)
            pending=queue.pending('a');self.assertEqual(pending,[value])
            self.assertEqual(queue.pending('other'),[])
            with self.assertRaisesRegex(m.Closed,'NATIVE_ACK_SCOPE_MISMATCH'):
                queue.acknowledge(pending,[{'observation_sha256':'c'*64}])
            self.assertEqual(queue.pending('a'),pending)
            queue.acknowledge(pending,[{'observation_sha256':value['observation_sha256']}])
            self.assertEqual(queue.pending('a'),[])

class LifecycleTests(unittest.TestCase):
    def cycle(self):
        return {'attempt_id':'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa','activation_key':'activation',
          'invocation_id':'b'*32,'owner':'owner','host_instance':m.socket.gethostname(),'boot_id':m.boot_id(),
          'module_sha256':'c'*64,'parent_pid':os.getpid(),'parent_cpu_start':time.process_time(),
          'meter_id':'d'*32}

    def journal(self):
        value=Mock();value.data={'calls':[],'pending':None}
        value.export.return_value={'calls':[],'calls_sha256':m.object_hash([]),'unknown_call_count':0,
                                  'all_started_calls_accounted':True,'all_sql_tails_closed':True}
        return value

    def test_known_claim_is_preserved_before_local_gate_failure(self):
        cycle=self.cycle();claim={'state':'RUNNING','attempt_id':cycle['attempt_id'],
                'activation_key':'activation','job_sha256':'f'*64,'job':{}}
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)),patch.object(m,'guards') as g, \
             patch.object(m,'journal_for',return_value=self.journal()), \
             patch.object(m,'validate_job',side_effect=m.Closed('MISSING_ACTUAL_CERTIFICATE')), \
             patch.object(m,'complete_terminal',return_value={'committed':True}) as terminal,patch.object(m,'launch') as launch:
            g.return_value.scratch_safe.return_value=True
            self.assertTrue(m.execute_claim(Mock(),cycle,claim)['committed'])
            launch.assert_not_called()
            self.assertTrue((Path(d)/('claim_received_'+cycle['invocation_id']+'.json')).exists())
            receipt=terminal.call_args.args[2]['receipt']
            self.assertEqual(receipt['physical_quiescence']['termination_proof'],'EXACT_OWNED_NO_RESEARCH_LAUNCH')
            self.assertEqual(receipt['result']['reason'],'MISSING_ACTUAL_CERTIFICATE')

    def test_unknown_child_result_uses_exact_identity_and_funded_terminal(self):
        cycle=self.cycle();identity={'pid':1234,'process_group':1234,'start_ticks':9,'boot_id':cycle['boot_id']}
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)),patch.object(m,'guards') as g, \
             patch.object(m,'journal_for',return_value=self.journal()),patch.object(m.time,'sleep'), \
             patch.object(m,'complete_terminal',return_value={'state':'QUIESCENT'}) as terminal,patch.object(m,'launch') as launch:
            g.return_value.scratch_safe.return_value=True
            g.return_value.quiesce_recorded_child.return_value={'process_finished':True,'proof':'RECORDED_PID_ABSENT'}
            for prefix,value in [('claim_received_',{}),('launch_',{}),('process_',identity)]:
                key=cycle['invocation_id'] if prefix=='claim_received_' else cycle['attempt_id']
                (Path(d)/(prefix+key+'.json')).write_text(json.dumps(value))
            self.assertEqual(m.reconcile_active(Mock(),cycle)['state'],'QUIESCENT')
            launch.assert_not_called();g.return_value.quiesce_recorded_child.assert_called_once()
            receipt=terminal.call_args.args[2]['receipt']
            self.assertIsNone(receipt['child_cpu_seconds']);self.assertTrue(receipt['unknown_work_retains_full54'])
            self.assertEqual(terminal.call_args.kwargs['recovery_slot'],0)

    def test_finite_closure_slots_have_stable_invocation_and_no_third_retry(self):
        cycle=self.cycle();carry=dict(cycle,terminal_receipt_sha256='e'*64,physical_quiescence_sha256='f'*64)
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)),patch.object(m,'guards') as g:
            g.return_value.scratch_safe.return_value=True;rpc=Mock()
            rpc.call.side_effect=[TimeoutError('first'),TimeoutError('second')]
            for _ in range(2):
                with self.assertRaises(TimeoutError):m.conservative_close(rpc,'owner',carry)
            calls=rpc.call.call_args_list
            self.assertEqual(calls[0].args[2]['closure_invocation'],calls[1].args[2]['closure_invocation'])
            self.assertEqual([call.args[2]['closure_slot'] for call in calls],[0,1])
            with self.assertRaisesRegex(m.Closed,'FINITE_CAPTURE_CLOSURE_POOL_EXHAUSTED'):
                m.conservative_close(rpc,'owner',carry)
            self.assertEqual(rpc.call.call_count,2)

    def test_ambiguous_journal_never_labels_all_calls_complete(self):
        journal=self.journal();journal.export.side_effect=ValueError('pending')
        journal.data={'calls':[],'pending':{'operation':'claim'}}
        physical={'process_finished':True,'termination_proof':'RECORDED_PID_ABSENT','cpu_seconds':None,'wall_seconds':None}
        receipt=m.terminal_args(self.cycle(),physical,{'state':'BLOCKED'},journal)['receipt']
        self.assertFalse(receipt['all_started_calls_accounted']);self.assertTrue(receipt['unknown_work_retains_full54'])

    def test_compact_job_and_context_have_distinct_native_readbacks(self):
        context={'contract_binding':{'private':'registered'}};text=json.dumps(context)
        compact={'activation_key':'a','session_date':'2026-10-05','execution_context_sha256':m.sha(text.encode())}
        jobtext=json.dumps(compact)
        claim={'job':compact,'job_evidence_text':jobtext,'job_sha256':m.sha(jobtext.encode()),
            'execution_context':{'evidence_text':text,'sha256':m.sha(text.encode())},
            'runtime_binding':{'attempt_id':'a'*36,'host_instance':'actual-host'}}
        hydrated=m.hydrate_job(claim)
        self.assertEqual(hydrated['contract_binding'],context['contract_binding'])
        self.assertNotIn('contract_binding',claim['job'])
        claim['job_evidence_text']=jobtext+' '
        with self.assertRaisesRegex(m.Closed,'COMPACT_JOB_NATIVE_READBACK'):
            m.hydrate_job(claim)

    def test_context_cannot_replace_fixed_session_or_runtime_owner(self):
        context={'session_date':'2099-01-01'};text=json.dumps(context)
        compact={'activation_key':'a','session_date':'2026-10-05','execution_context_sha256':m.sha(text.encode())}
        jobtext=json.dumps(compact)
        claim={'job':compact,'job_evidence_text':jobtext,'job_sha256':m.sha(jobtext.encode()),
            'execution_context':{'evidence_text':text,'sha256':m.sha(text.encode())},'runtime_binding':{}}
        with self.assertRaisesRegex(m.Closed,'CONTEXT_CANNOT_OVERRIDE'):
            m.hydrate_job(claim)

    def test_unknown_initial_probe_uses_two_funded_reconciliations_only(self):
        cycle=dict(self.cycle(),phase='PROBE_PENDING')
        original={'p_op':'funded_probe','p_owner':'owner','p_args':{'activation_key':'activation',
                   'invocation_id':cycle['invocation_id'],'attempt_id':cycle['attempt_id']}}
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)),patch.object(m,'guards') as g,patch.object(m,'launch') as launch:
            g.return_value.scratch_safe.return_value=True
            (Path(d)/('probe_request_'+cycle['invocation_id']+'.json')).write_text(json.dumps(original))
            (Path(d)/('probe_helper_'+cycle['invocation_id']+'.json')).write_text(json.dumps({
                'process_finished':True,'process_termination_proof':'SPECIFIC_CHILD_WAIT4',
                'request_sha256':m.object_hash(original),'sql_tail_waited_seconds':3}))
            rpc=Mock();rpc.call.side_effect=TimeoutError('lost funded metadata response')
            for _ in range(2):
                with self.assertRaises(TimeoutError):m.reconcile_active(rpc,cycle)
            self.assertTrue((Path(d)/('probe_request_'+cycle['invocation_id']+'.json')).exists())
            with self.assertRaisesRegex(m.Closed,'FINITE_CAPTURE_METADATA_RECONCILIATION_EXHAUSTED'):
                m.reconcile_active(rpc,cycle)
            self.assertEqual(rpc.call.call_count,2);launch.assert_not_called()
            self.assertNotEqual(rpc.call.call_args_list[0].args[2]['invocation_id'],rpc.call.call_args_list[1].args[2]['invocation_id'])

    def test_funded_metadata_probe_uses_server_permit(self):
        helper=Mock();helper.send.return_value={'state':'BLOCKED_BY_IDENTIFIED_DEPENDENCY'}
        with patch.object(m,'load',return_value=helper):
            rpc=m.RPC();rpc.direct({'p_op':'funded_probe','p_owner':'owner','p_args':{}})
            self.assertIs(helper.send.call_args.kwargs['read_only'],False)
            rpc.direct({'p_op':'status','p_owner':'owner','p_args':{}})
            self.assertIs(helper.send.call_args.kwargs['read_only'],True)

    def test_wait_action_is_not_reported_as_committed_capture_progress(self):
        cycle=dict(self.cycle(),child_governed_seconds=10,child_wall_seconds=20)
        job={'attempt_id':cycle['attempt_id'],'work_key':'fixed-window',
             'activation_key':'activation','incremental_module_sha256':'a'*64,
             'source_producer_bindings':{},'incremental_release_binding':{}}
        pipeline=Mock();pipeline.perform_action.return_value={
            'state':'AWAITING_ELIGIBLE_EVIDENCE','committed_progress':False}
        core=Mock();core.SliceComplete=type('SliceComplete',(Exception,),{})
        core.GovernedBudget.return_value.receipt.return_value={'actual':0.01}
        core.GovernedBudget.return_value.io.side_effect=lambda name,limit,function:function()
        rpc=Mock();rpc.direct.return_value={'state':'RUNNING','work':{'action':'WAIT'}}
        journal=self.journal()
        with patch.object(m,'validate_job',return_value=(job,{}, {},pipeline,core)), \
             patch.object(m,'load',return_value=Mock()),patch.object(m,'journal_for',return_value=journal), \
             patch.object(m,'SourceRevisionCache'),patch.object(m,'PendingPageCache') as pages, \
             patch.object(m,'PendingDecisionObservations') as observations:
            pages.return_value.pending.return_value=[]
            observations.return_value.pending.return_value=[]
            result=m.run_slice({},cycle,rpc=rpc)
        self.assertEqual(result['completed_actions'],0)
        self.assertEqual(result['attempted_actions'],1)
        self.assertIsNone(result['last_committed_transition'])
        self.assertFalse(m.result_committed(result))
        self.assertEqual(rpc.direct.call_count,1)

    def test_post_launch_identity_publication_failure_preserves_wait4_receipt(self):
        cycle=self.cycle();claim={'state':'RUNNING','attempt_id':cycle['attempt_id'],
            'activation_key':'activation','job_sha256':'f'*64,'job':{}}
        job={'maximum_wall_seconds':7,'deadline_at':'2099-01-01T00:00:00Z'}
        process=Mock();process.original_identity={'pid':123,'process_group':123,'start_ticks':9,'boot_id':cycle['boot_id']}
        process.termination_proof='SPECIFIC_CHILD_WAIT4';process.cpu=.1;process.exit_code=-15
        journal=self.journal();original_atomic=m.atomic
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)),patch.object(m,'guards') as guards, \
             patch.object(m,'journal_for',return_value=journal), \
             patch.object(m,'validate_job',return_value=(job,None,None,None,None)), \
             patch.object(m,'launch',return_value=process),patch.object(m.time,'sleep'), \
             patch.object(m,'complete_terminal',return_value={'committed':False}) as terminal:
            guards.return_value.scratch_safe.return_value=True
            def fail_identity(path,value,**kwargs):
                if path.name.startswith('process_'):raise OSError('synthetic identity publication failure')
                return original_atomic(path,value,**kwargs)
            with patch.object(m,'atomic',side_effect=fail_identity):
                m.execute_claim(Mock(),cycle,claim)
            process.stop.assert_called_once()
            receipt=json.loads((Path(d)/('physical_'+cycle['attempt_id']+'.json')).read_text())
            self.assertEqual(receipt['termination_proof'],'SPECIFIC_CHILD_WAIT4')
            self.assertEqual(receipt['cpu_seconds'],.1)
            self.assertEqual(terminal.call_args.args[2]['receipt']['result']['reason'],'CAPTURE_POST_LAUNCH_CONTROL_FAILURE')

    def test_lost_claim_recovery_has_two_existing_slots_and_observed_ack_replay(self):
        cycle=dict(self.cycle(),phase='CLAIM_REQUESTED')
        request={'p_op':'claim','p_owner':'owner','p_args':{'attempt_id':cycle['attempt_id']}}
        proof={'request_sha256':m.object_hash(request),'process_finished':True,
            'process_termination_proof':'SPECIFIC_CHILD_WAIT4','response_readback_verified':False,
            'sql_tail_waited_seconds':3}
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)),patch.object(m,'guards') as guards, \
             patch.object(m,'launch') as launch:
            guards.return_value.scratch_safe.return_value=True
            for prefix,value in [('claim_request_',request),('claim_helper_',proof)]:
                (Path(d)/(prefix+cycle['invocation_id']+'.json')).write_text(json.dumps(value))
            rpc=Mock();rpc.call.side_effect=TimeoutError('synthetic lost reconciliation ACK')
            for _ in range(2):
                with self.assertRaises(TimeoutError):m.reconcile_active(rpc,cycle)
            self.assertEqual([c.args[2]['recovery_slot'] for c in rpc.call.call_args_list],[0,1])
            self.assertNotEqual(*(c.args[2]['recovery_invocation'] for c in rpc.call.call_args_list))
            with self.assertRaisesRegex(m.Closed,'FINITE_CAPTURE_CLAIM_RECONCILIATIONS_EXHAUSTED'):
                m.reconcile_active(rpc,cycle)
            ack={'claim_closed':True,'reservation_created':False,'attempt_id':cycle['attempt_id']}
            (Path(d)/('claim_reconcile_ack_'+cycle['attempt_id']+'.json')).write_text(json.dumps(ack))
            self.assertEqual(m.reconcile_active(rpc,cycle),ack)
            self.assertEqual(rpc.call.call_count,2);launch.assert_not_called()

    def test_carry_two_helpers_share_one_finite_twelve_second_tail(self):
        cycle=self.cycle();journal=self.journal()
        with patch.object(m.time,'process_time',return_value=cycle['parent_cpu_start']+.1), \
             patch.object(m,'cycle_call_totals',return_value=4.8):
            self.assertTrue(m.carry_control_fits(cycle,journal))
        with patch.object(m.time,'process_time',return_value=cycle['parent_cpu_start']+.2), \
             patch.object(m,'cycle_call_totals',return_value=4.9):
            self.assertFalse(m.carry_control_fits(cycle,journal))
        journal.data['pending']={'operation':'funded_probe'}
        self.assertFalse(m.carry_control_fits(cycle,journal))

    def test_metadata_exhaustion_is_shared_and_never_reset_by_stale_ack(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'SHARED_METADATA_ROOT',Path(d)), \
             patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            ack={'funded':True,'activation_key':'actual','remaining_normal_probe_starts':0,'metadata_pool_exhausted':True}
            m.record_metadata_ack('actual',ack)
            self.assertTrue(m.metadata_pool_stopped('actual'))
            m.record_metadata_ack('actual',dict(ack,remaining_normal_probe_starts=317,metadata_pool_exhausted=False))
            self.assertTrue(m.metadata_pool_stopped('actual'))
            self.assertFalse(m.metadata_pool_stopped('different-activation'))

    def test_first_claim_needs_actual_separate_control_predebit(self):
        cycle=self.cycle()
        ack={key:cycle[key] for key in ('attempt_id','activation_key','invocation_id','owner','host_instance','boot_id')}
        ack.update(funded=True,prepaid_seconds=36,slots=3,seconds_per_slot=12,
            scope='INITIAL_CLAIM_AND_TWO_EXACT_NO_RESERVATION_RECONCILIATIONS',
            conservative_no_refund=True,within_actual_activation_predebit=True)
        m.require_initial_claim_funding(cycle,ack)
        with self.assertRaisesRegex(m.Closed,'ACTUAL_INITIAL_CLAIM'):
            m.require_initial_claim_funding(cycle,dict(ack,prepaid_seconds=0))

class PersistedScheduleTests(unittest.TestCase):
    def official(self):
        return {'state':'AWAITING_ELIGIBLE_EVIDENCE','schedule_basis':'REGISTERED_OFFICIAL_SESSION',
            'server_time':'2026-10-02T20:00:00Z','next_due_at':'2026-10-05T13:20:00Z',
            'metadata_accounting_ack':{'funded':True,'activation_key':'actual'}}

    def test_weekend_schedule_survives_process_memory_reset_without_another_probe(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)/'capture'), \
             patch.object(m,'guards') as guards,patch.object(m,'boot_id',return_value='same-boot'), \
             patch.object(m.time,'monotonic',return_value=1000):
            guards.return_value.scratch_safe.return_value=True
            m.record_probe_schedule('actual',self.official())
            with patch.object(m,'_last_poll',0),patch.object(m,'_last_delay',0):
                rpc=Mock()
                result=m.supervise_once(rpc,'owner',scheduled_at=1,trigger='PERSISTENT_WORKER_TIMER',activation_key='actual')
            self.assertGreater(result['next_poll_seconds'],2*86400)
            self.assertFalse(result['new_rpc_or_reservation_started']);rpc.call.assert_not_called()
            self.assertFalse((m.ROOT/'active_cycle.json').exists())

    def test_same_boot_schedule_uses_monotonic_due_and_not_wallclock_jump(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)), \
             patch.object(m,'guards') as guards,patch.object(m,'boot_id',return_value='same-boot'):
            guards.return_value.scratch_safe.return_value=True
            with patch.object(m.time,'monotonic',return_value=100):m.record_probe_schedule('actual',self.official())
            with patch.object(m.time,'monotonic',return_value=101),patch.object(m,'datetime') as wall:
                result=m.probe_schedule_wait('actual');wall.now.assert_not_called()
            self.assertGreater(result['next_poll_seconds'],2*86400)

    def test_two_unchanged_dependency_retries_then_persistent_local_stop(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)),patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            probe=dict(self.official(),schedule_basis='FINITE_DEPENDENCY_RETRY',
                next_due_at='2026-10-02T20:05:00Z',dependency_fingerprint='a'*64,
                maximum_dependency_probe_retries=2)
            self.assertFalse(m.record_probe_schedule('actual',probe)['stopped'])
            self.assertFalse(m.record_probe_schedule('actual',probe)['stopped'])
            self.assertTrue(m.record_probe_schedule('actual',probe)['stopped'])
            self.assertEqual(m.probe_schedule_wait('actual')['reason'],'FINITE_UNCHANGED_DEPENDENCY_PROBE_RETRIES_EXHAUSTED')
            self.assertIsNone(m.probe_schedule_wait('another-activation'))

    def test_zero_retry_stop_or_missing_native_schedule_never_repolls(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)),patch.object(m,'guards') as guards:
            guards.return_value.scratch_safe.return_value=True
            probe=dict(self.official(),schedule_basis='FINITE_DEPENDENCY_RETRY',
                dependency_fingerprint='a'*64,maximum_dependency_probe_retries=0)
            self.assertTrue(m.record_probe_schedule('actual',probe)['stopped'])
            missing={'metadata_accounting_ack':{'funded':True,'activation_key':'other'}}
            self.assertEqual(m.record_probe_schedule('other',missing)['stop_reason'],'ACTUAL_NATIVE_PROBE_SCHEDULE_INVALID')

    def test_timer_honors_long_due_without_clamping_to_five_minutes(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)), \
             patch.object(m,'_last_poll',0),patch.object(m,'_last_delay',0), \
             patch.object(m,'supervise_once',return_value={'state':'AWAITING_ELIGIBLE_EVIDENCE','next_poll_seconds':2*86400}) as call:
            m.timer_tick('owner',activation_key='actual')
            self.assertEqual(m._last_delay,2*86400)
            result=m.timer_tick('owner',activation_key='actual')
            self.assertFalse(result['new_rpc_or_reservation_started']);self.assertEqual(call.call_count,1)

    def test_initial_peer_probe_cannot_close_healthy_other_lane_carry(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)/'prospective_capture'), \
             patch.object(m,'metadata_pool_stopped',return_value=False):
            peer=Path(d)/'prospective_consumer_v3';peer.mkdir();(peer/'carry.json').write_text('{"native":"pending"}')
            rpc=Mock();result=m.supervise_once(rpc,'owner',scheduled_at=1,trigger='PERSISTENT_WORKER_TIMER',activation_key='actual')
            self.assertEqual(result['reason'],'OWNED_PEER_CYCLE_MUST_FINISH_FIRST');rpc.call.assert_not_called()
            self.assertTrue((peer/'carry.json').exists())

    def test_new_actual_activation_does_not_inherit_old_activation_wake_delay(self):
        with tempfile.TemporaryDirectory() as d,patch.object(m,'ROOT',Path(d)), \
             patch.object(m,'_last_poll',time.monotonic()),patch.object(m,'_last_delay',2*86400), \
             patch.object(m,'_last_activation_key','old-activation'), \
             patch.object(m,'supervise_once',return_value={'state':'AWAITING_ELIGIBLE_EVIDENCE','next_poll_seconds':300}) as call:
            m.timer_tick('owner',activation_key='new-activation')
            self.assertEqual(call.call_count,1)

if __name__=='__main__':unittest.main()
