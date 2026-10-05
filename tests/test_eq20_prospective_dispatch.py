"""Synthetic durable-dispatch tests; no Supabase credentials or market IO."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zlib

ROOT = Path(__file__).resolve().parents[1]

def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    obj = importlib.util.module_from_spec(spec); spec.loader.exec_module(obj); return obj

rt = module('prospective_dispatch_tests', ROOT/'app/eq20_prospective_dispatch.py')
fixtures = module('prospective_fixtures', ROOT/'tests/test_eq20_prospective_kernel.py')
k = fixtures.k
bf = module('prospective_batch_fixtures', ROOT/'tests/test_eq20_prospective_batch.py')
pf = module('prospective_producer_fixtures', ROOT/'tests/test_eq20_prospective_features.py')


def artifact(kind, status, key, evidence, created='2026-12-01T00:00:00Z'):
    raw = rt.canonical(evidence).decode()
    ref = {'kind': kind, 'status': status, 'artifact_key': key, 'implementation_sha256': rt.sha(raw.encode())}
    return ref, dict(ref, evidence_text=raw, created_at=created)


def job_fixture():
    c,rows,work,j,db=bf.fixture(1,1);c['reference_outcome_mode']='EXACT_SUBSEQUENT_TRADE_V1';payload=rows[0];compressed=next(iter(db.payloads.values()))
    pop=work['population'];segments=work['input_stream']['segments']
    stream={'format':'EQ20_PROSPECTIVE_RAW_SECURITY_JSONL_V2','population_day_sha256':pop['population_day_sha256'],
            'security_session_count':1,'segments':segments,'manifest_sha256':rt.sha(rt.canonical(segments))}
    contract_ref,contract_row=artifact('SUCCESSOR_PROSPECTIVE_SESSION_CONTRACT','REGISTERED_OUTCOME_BLIND','C',c)
    plan={name:c[name] for name in ('candidate_family_sha256','execution_policy_sha256','official_calendar_sha256',
           'population_manifest_sha256','official_session_dates_sha256','official_session_dates')}
    plan.update(method_contract_sha256=rt.METHOD_CONTRACT_SHA256,scope_sha256='f'*64,
        bindings={'population':{'implementation_sha256':c['population_policy_artifact_sha256']}})
    cal_ref,cal_row=artifact('SUCCESSOR_OFFICIAL_CALENDAR_CERTIFICATE','VERIFIED_OFFICIAL_CALENDAR','CAL',
        {'sessions':c['official_sessions'],'official_calendar_sha256':c['official_calendar_sha256']})
    plan['bindings']['calendar']=cal_ref
    binding=pf.binding(c)
    text="VERSION='EQ20_PROSPECTIVE_PRIVATE_ARITHMETIC_V1'\nclass Producer:\n def __init__(self,c,d,f):\n  import json,hashlib\n  self.session_contract_sha256=hashlib.sha256(json.dumps(c,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()\n"
    binding['source_files'][pf.p.ENTRYPOINT]=text
    source=json.loads(binding['artifact']['evidence_text']);source.update(scope_sha256=plan['scope_sha256'],real_implementation_verified=True)
    for item in source['code_files']:item['sha256']=rt.sha(binding['source_files'][item['name']].encode())
    sr,sa=artifact('SUCCESSOR_PROSPECTIVE_SOURCE_PRODUCER_RELEASE','VERIFIED_EXECUTABLE_RELEASE','SOURCE',source)
    binding.update(reference=sr,artifact=sa)
    er,ea=artifact('SUCCESSOR_PROSPECTIVE_EXECUTION_PRODUCER_RELEASE','VERIFIED_EXECUTABLE_RELEASE','EXECUTION',
        {'session_contract_sha256':k.digest(c),'scope_sha256':plan['scope_sha256'],'real_implementation_verified':True,
         'additional_paid_cost':0,'module_sha256':rt.sha((ROOT/'app/eq20_execution_replay.py').read_bytes())})
    proof={'consumer_module_sha256':rt.sha(Path(rt.__file__).read_bytes()),'batch_module_sha256':rt.sha((ROOT/'app/eq20_prospective_batch.py').read_bytes()),
        'session_contract_sha256':k.digest(c),'allocation_key':'FUNDED','official_session_dates_sha256':c['official_session_dates_sha256'],
        'source_producer_sha256':sr['implementation_sha256'],'execution_producer_sha256':er['implementation_sha256'],
        'additional_paid_cost':0,'additional_paid_capacity':False,'maximum_security_sessions_per_date':1,'maximum_input_bytes_per_date':100000,
        'projected_total_slices':2200,'projected_governed_seconds':118900,'maximum_cache_reconstructions':2,'maximum_security_wall_seconds':.1,'maximum_aggregate_wall_seconds':.1,
        'maximum_segment_decode_wall_seconds':.1,'projected_peak_rss_bytes':1000000,'projected_peak_shared_scratch_bytes':1000000,
        'projected_maximum_file_bytes':100000,'projected_remote_storage_charge_bytes':100000000,
        'scope_exceedance_action':'STOP_WITH_FULL_DENOMINATOR_PRESERVED','minimum_security_records_per_slice':10000,
        'minimum_input_bytes_per_slice':1000000,'minimum_committed_output_bytes_per_slice':1000000,
        'maximum_committed_output_bytes_per_date':10000,'producer_collection_governed_seconds':100,'workload_cardinality_manifest_sha256':'1'*64}
    for key in ('actual_workload_benchmarked','complete_denominator_costed','cold_hydration_costed','committed_output_transport_costed',
        'two_terminal_replays_per_slice_costed','aggregate_and_fixed_horizon_costed','independently_reviewed','existing_entitlements_verified'):proof[key]=True
    refs={};benchmarks={}
    for kind in ('COLLECT_AND_SELECT','REPLAY_AND_COMMIT','COLD_HYDRATION','AGGREGATE_AND_EVALUATE'):
        fact={name:proof[name] for name in ('consumer_module_sha256','batch_module_sha256','source_producer_sha256','execution_producer_sha256','workload_cardinality_manifest_sha256')}
        fact.update(benchmark_kind=kind,actual_execution_measured=True,full_count_projection_independently_reviewed=True,
            governed_includes_rpc_and_terminal=True,maximum_child_wall_seconds=1,maximum_child_cpu_seconds=1)
        refs[kind],benchmarks[kind]=artifact('SUCCESSOR_BATCH_RESOURCE_BENCHMARK','VERIFIED_MEASURED_BOUNDED_EXECUTION',kind,fact)
    proof['benchmark_references']=refs
    fr,fa=artifact('SUCCESSOR_BATCH_RESOURCE_FEASIBILITY','VERIFIED_FULL_DENOMINATOR_FINITE_COST','RESOURCE',proof)
    cr,ca=artifact('SUCCESSOR_COLLECTOR_RELEASE','VERIFIED_EXECUTABLE_RELEASE','COLLECTOR',
        {'prospective_session_contract_sha256':k.digest(c),'consumer_module_sha256':proof['consumer_module_sha256'],
         'consumer_kernel_sha256':rt.KERNEL_SHA256,'batch_module_sha256':proof['batch_module_sha256'],
         'source_producer_reference':sr,'execution_producer_reference':er,'resource_feasibility_reference':fr})
    plan['bindings']['collector']=cr
    ar,aa=artifact('SUCCESSOR_EVIDENCE_ACTIVATION','AWAITING_ELIGIBLE_EVIDENCE','ACTIVATION',
        {'activation_key':'ACTIVATION','frozen_plan':plan,'registered_at':'2026-12-02T00:00:00Z',
         'first_official_session_open_at':c['official_sessions'][0]['open_at'],
         'resolved_bindings':{'collector':ca,'calendar':cal_row,'candidate':{'created_at':'2026-12-01T00:00:00Z'}},'epoch':1,'alpha_fraction':'1/480'})
    ir,ia=artifact('SUCCESSOR_INPUT_CAPSULE_CERTIFICATE','VERIFIED_CAUSAL_COMPLETE_SCOPE','INPUT',
        {'activation_key':'ACTIVATION','action':'PROCESS_SECURITY_BATCH','work_artifact_key':'WORK',**k.scope(c),
         'session_date':payload['session_date'],'input_stream':stream,'population':pop,'outcome_based_input_filtering':False,
         'complete_scheduled_input_slots_retained':True,'source_and_execution_producer_review_complete':True})
    wr,wa=artifact('SUCCESSOR_PROSPECTIVE_WORK_ITEM','READY_CERTIFIED_INPUTS','WORK',
        {'version':'EQ20_PROSPECTIVE_WORK_ITEM_V2','action':'PROCESS_SECURITY_BATCH','activation_key':'ACTIVATION',
         'session_date':payload['session_date'],'population':pop,'input_stream':stream,'contract_reference':contract_ref,'input_certificate_reference':ir})
    j.update(reserved_governed_seconds=30,resource_predebit_verified=True,terminal_replay_prepaid_seconds=24,total_funded_seconds=54,terminal_storage_prepaid_bytes=786432,
        activation_reserved_seconds=130000,reserved_remote_storage_charge_bytes=200000000,activation_commit_acknowledged=True,
        activation_key='ACTIVATION',allocation_key='FUNDED',commit_acknowledgement_at='2026-12-02T00:00:01Z',
        work_artifact=wa,work_reference=wr,contract_artifact=contract_row,activation_artifact=aa,activation_reference=ar,
        source_producer_artifact=sa,execution_producer_artifact=ea,source_producer_bindings=binding,input_certificate_artifact=ia,
        resource_feasibility_artifact=fa,resource_benchmark_artifacts=benchmarks,dated_population_binding=fixtures.population_binding(c,pop))
    return j,compressed


class DispatchTest(unittest.TestCase):
    @staticmethod
    def recovery_guards(child_cpu=.1, exit_code=0):
        class Child:
            original_identity = {'pid': 2345, 'process_group': 2345, 'start_ticks': 123,
                'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
            termination_proof = 'SPECIFIC_CHILD_WAIT4'
            cpu = child_cpu
            def __init__(self, command): self.exit_code = exit_code
            def reap(self): return True
            def stop(self): pass
        class Guard:
            ReapedChild = Child
            @staticmethod
            def scratch_safe(size): return True
            @staticmethod
            def control_identity_equal(left, right): return isinstance(left, dict) and left == right
        return Guard()

    def test_lost_claim_retains_exact_helper_and_reconciles_without_new_child(self):
        class RecoveryRPC:
            calls = []
            def call(self, op, owner, args):
                self.calls.append((op, owner, args))
                return {'state': 'RUNNING', 'reconciled': True, 'committed': True}
        with tempfile.TemporaryDirectory() as temp, patch.object(rt, 'ROOT', Path(temp)), patch.object(rt, 'guards', return_value=self.recovery_guards(exit_code=1)), patch.object(rt.time, 'sleep'), patch.object(rt, 'execute_reserved') as launch:
            with self.assertRaisesRegex(rt.Closed, 'HTTP_HELPER_FAILURE'):
                rt.supervise_once(rt.RPC(), 'original_owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=1)
            envelopes = [p for p in Path(temp).glob('claim_*.json') if len(p.stem) == len('claim_')+32]
            self.assertEqual(len(envelopes), 1)
            invocation = json.loads(envelopes[0].read_bytes())['args']['invocation_id']
            proof = json.loads((Path(temp)/('claim_helper_'+invocation+'.json')).read_bytes())
            self.assertEqual(proof['process_termination_proof'], 'SPECIFIC_CHILD_WAIT4')
            self.assertFalse(proof['response_readback_verified'])
            self.assertEqual(proof['request_sha256'], rt.sha(envelopes[0].read_bytes()))
            self.assertTrue((Path(temp)/('rpc_'+invocation+'.json')).exists())
            rpc = RecoveryRPC()
            with patch.object(rt.time, 'monotonic', return_value=proof['finished_monotonic']+4):
                result = rt.supervise_once(rpc, 'new_owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=2)
            self.assertEqual([x[:2] for x in rpc.calls], [('claim_reconcile', 'original_owner')])
            self.assertEqual(rpc.calls[0][2]['replay_slot'], 0)
            self.assertTrue(result['reconciled']); self.assertEqual(list(Path(temp).iterdir()), [])
            launch.assert_not_called()

    def test_no_launch_reconciliation_is_finite_and_never_ignores_a_launch_marker(self):
        class FailingRPC:
            calls = []
            def call(self, op, owner, args):
                self.calls.append(op); raise rt.Closed('CONSUMER_RPC_TRANSPORT_FAILURE')
        with tempfile.TemporaryDirectory() as temp, patch.object(rt, 'ROOT', Path(temp)), patch.object(rt, 'guards', return_value=self.recovery_guards(exit_code=1)), patch.object(rt.time, 'sleep'), patch.object(rt, 'execute_reserved') as launch:
            with self.assertRaises(rt.Closed):
                rt.supervise_once(rt.RPC(), 'owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=1)
            envelope = next(p for p in Path(temp).glob('claim_*.json') if len(p.stem) == len('claim_')+32)
            invocation = json.loads(envelope.read_bytes())['args']['invocation_id']
            proof = json.loads((Path(temp)/('claim_helper_'+invocation+'.json')).read_bytes())
            marker = Path(temp)/('claim_launch_'+invocation+'.json'); marker.write_text('{}')
            rpc = FailingRPC()
            with patch.object(rt.time, 'monotonic', return_value=proof['finished_monotonic']+4):
                blocked = rt.supervise_once(rpc, 'owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=2)
                self.assertIn('LAUNCH_BOUNDARY', blocked['dependencies'][0]); self.assertEqual(rpc.calls, [])
                marker.unlink()
                for _ in range(2):
                    with self.assertRaises(rt.Closed):
                        rt.supervise_once(rpc, 'owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=3)
                blocked = rt.supervise_once(rpc, 'owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=4)
            self.assertEqual(rpc.calls, ['claim_reconcile', 'claim_reconcile'])
            self.assertIn('FINITE_CLAIM', blocked['dependencies'][0]); launch.assert_not_called()

    def test_known_measured_overrun_is_persisted_and_submitted_as_blocked(self):
        attempt = '00000000-0000-0000-0000-000000000001'
        class TerminalRPC:
            calls = []
            def call(self, op, owner, args):
                self.calls.append((op, args))
                self.persisted = (rt.ROOT/('terminal_'+attempt+'.json')).exists()
                return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'committed': False}
        rpc = TerminalRPC()
        with tempfile.TemporaryDirectory() as temp, patch.object(rt, 'ROOT', Path(temp)), patch.object(rt, 'guards', return_value=self.recovery_guards()), patch.object(rt.time, 'monotonic', side_effect=[0, 7.001]), patch.object(rt.time, 'process_time', return_value=0):
            result = rt.execute_reserved(rpc, 'owner', {'attempt_id': attempt, 'reserved_governed_seconds': 30})
            receipt = json.loads((Path(temp)/('terminal_'+attempt+'.json')).read_bytes())['args']['receipt']
        self.assertTrue(rpc.persisted); self.assertEqual(rpc.calls[0][0], 'terminal')
        self.assertEqual(receipt['child_wall_seconds'], 7.001)
        self.assertEqual(receipt['process_termination_proof'], 'SPECIFIC_CHILD_WAIT4')
        self.assertFalse(receipt['resource_envelope_verified'])
        self.assertEqual(receipt['error'], 'CONSUMER_MEASURED_ENVELOPE_EXCEEDED')
        self.assertEqual(result['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_missing_measurement_retains_receipt_without_claiming_settlement(self):
        attempt = '00000000-0000-0000-0000-000000000001'
        with tempfile.TemporaryDirectory() as temp, patch.object(rt, 'ROOT', Path(temp)), patch.object(rt, 'guards', return_value=self.recovery_guards(child_cpu=None)), patch.object(rt.time, 'monotonic', side_effect=[0, 1]), patch.object(rt.time, 'process_time', return_value=0):
            class RPC:
                def call(self, *args): raise AssertionError('unknown measurement cannot settle')
            with self.assertRaisesRegex(rt.Closed, 'TERMINATION_OR_MEASUREMENT_UNKNOWN'):
                rt.execute_reserved(RPC(), 'owner', {'attempt_id': attempt, 'reserved_governed_seconds': 30})
            receipt = json.loads((Path(temp)/('terminal_'+attempt+'.json')).read_bytes())['args']['receipt']
        self.assertIsNone(receipt['child_cpu_seconds'])
        self.assertFalse(receipt['resource_envelope_verified'])

    def test_acknowledged_settlement_cleanup_resumes_locally_after_crash(self):
        attempt = '00000000-0000-0000-0000-000000000001'; invocation = '1'*32
        with tempfile.TemporaryDirectory() as temp, patch.object(rt, 'ROOT', Path(temp)), patch.object(rt, 'guards', return_value=self.recovery_guards()):
            for name in ('terminal_'+attempt, 'replay_count_'+attempt, 'claim_launch_'+invocation, 'claim_'+invocation):
                (Path(temp)/(name+'.json')).write_text('{}')
            original = rt.cleanup_claim
            def interrupted(value):
                (Path(temp)/('claim_'+value+'.json')).unlink()
                raise OSError('simulated crash during deletion')
            with patch.object(rt, 'cleanup_claim', side_effect=interrupted):
                with self.assertRaises(OSError):
                    rt.finish_local_settlement(attempt, invocation, 'owner',
                        {'committed': True, 'attempt_id': attempt, 'state': 'VERIFIED'})
            self.assertTrue((Path(temp)/('settled_'+attempt+'.json')).exists())
            class RPC:
                def call(self, *args): raise AssertionError('observed ACK cleanup requires no extra transport')
            result = rt.supervise_once(RPC(), 'owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=2)
            self.assertEqual(result['verification_scope'], 'LOCAL_CLEANUP_OF_OBSERVED_SERVER_SETTLEMENT_ONLY')
            self.assertEqual(list(Path(temp).iterdir()), [])

    def test_real_fixed_rule_batch_produces_immutable_scope_bound_output(self):
        job,blob=job_fixture();batch=rt.load_local('eq20_prospective_batch');work=json.loads(job['work_artifact']['evidence_text'])
        db=bf.Database(job,work['input_stream']['segments'],{work['input_stream']['segments'][0]['compressed_sha256']:blob})
        with tempfile.TemporaryDirectory() as temp,patch.object(rt,'ROOT',Path(temp)),patch.object(rt,'RPC',return_value=db),patch.object(rt,'guards',return_value=bf.Host(Path(temp),db)),patch.object(batch,'produce',side_effect=bf.producer):
            result=rt.run_job(job,lambda meta: self.fail('legacy capsule fetch'))
        self.assertEqual(result['state'],'VERIFIED');self.assertEqual(len(db.outputs),1)
        receipt=batch.decode_receipt(db.outputs[0]);self.assertEqual(receipt['claim_slots'][0]['n_nonqualify'],1)
        self.assertEqual(receipt['claim_slots'][2]['n_success'],1);self.assertFalse(result['research_objective_achieved'])

    def test_actual_missing_or_tampered_producer_and_contract_gate_precedes_read(self):
        for mutation in ('producer', 'contract', 'hash', 'preopen', 'resource'):
            job, blob = job_fixture(); reads = []
            if mutation == 'producer': job.pop('execution_producer_artifact')
            elif mutation == 'contract': job['contract_artifact']['evidence_text'] += ' '
            elif mutation == 'hash': job['work_reference']['implementation_sha256'] = '0'*64
            elif mutation == 'preopen': job['commit_acknowledgement_at'] = '2027-01-04T15:00:00Z'
            else: job['resource_predebit_verified'] = False
            with self.assertRaises((rt.Closed, ValueError)):
                rt.run_job(job, lambda meta: reads.append(meta) or blob)
            self.assertEqual(reads, [])

    def test_raw_corruption_and_trailing_compressed_data_rejected(self):
        raw=b'{"source":1}';blob=zlib.compress(raw)
        meta={'compressed_bytes':len(blob),'compressed_sha256':rt.sha(blob),'raw_bytes':len(raw),'raw_sha256':rt.sha(raw)}
        for bad in (blob[:-1]+bytes([blob[-1]^1]),blob+b'x'):
            with self.assertRaises((rt.Closed,zlib.error)):list(rt.raw_chunks(bad,meta))

    def test_stream_decoder_is_bounded_and_checks_entire_content(self):
        raw = b'{"a":1}\n'*10000; compressed = zlib.compress(raw)
        meta = {'compressed_bytes': len(compressed), 'compressed_sha256': rt.sha(compressed),
                'raw_bytes': len(raw), 'raw_sha256': rt.sha(raw)}
        self.assertEqual(sum(1 for _ in rt.json_lines(rt.raw_chunks(compressed, meta))), 10000)
        meta['raw_bytes'] -= 1
        with self.assertRaisesRegex(rt.Closed, 'DECOMPRESSION_BOUND'):
            list(rt.raw_chunks(compressed, meta))

    def test_final_evaluation_never_uses_activation_as_final_receipts(self):
        job, blob = job_fixture(); contract, work, act = rt.verify_job(job)
        with self.assertRaisesRegex(rt.Closed, 'ELEVEN_FINAL'):
            rt.verified_final_gates(job, contract, act)

    def test_blocked_timer_makes_one_read_only_claim_and_never_launches(self):
        class FakeRPC:
            calls = []
            def call(self, op, owner, args):
                self.calls.append(op)
                return {'state': 'BLOCKED_BY_IDENTIFIED_DEPENDENCY', 'dependencies': ['ACKNOWLEDGED_PROSPECTIVE_ACTIVATION_PENDING']}
        rpc = FakeRPC()
        with tempfile.TemporaryDirectory() as temp, patch.object(rt, 'ROOT', Path(temp)), patch.object(rt, 'guards', return_value=type('Guard', (), {'scratch_safe': staticmethod(lambda size: True)})()), patch.object(rt, 'execute_reserved') as launch:
            result = rt.supervise_once(rpc, 'currentowner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=1)
            self.assertEqual(rpc.calls, ['claim']); launch.assert_not_called()
            self.assertEqual(result['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_persisted_terminal_is_replayed_before_any_new_compute(self):
        class FakeRPC:
            calls = []
            def call(self, op, owner, args):
                self.calls.append((op, owner, args))
                return {'state': 'VERIFIED', 'committed': True, 'attempt_id': args['attempt_id']}
        class FakeGuards:
            @staticmethod
            def scratch_safe(size): return True
        rpc = FakeRPC(); attempt = '00000000-0000-0000-0000-000000000001'
        with tempfile.TemporaryDirectory() as temp, patch.object(rt, 'ROOT', Path(temp)), patch.object(rt, 'guards', return_value=FakeGuards()), patch.object(rt, 'execute_reserved') as launch:
            terminal = {'owner': 'original_owner', 'args': {'attempt_id': attempt,
                'receipt': {'process_finished': True, 'process_termination_proof': 'SPECIFIC_CHILD_WAIT4'}}}
            (Path(temp)/('terminal_'+attempt+'.json')).write_bytes(rt.canonical(terminal))
            rt.supervise_once(rpc, 'new_controller_owner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=1)
            self.assertEqual(rpc.calls[0][0:2], ('terminal_replay', 'original_owner'))
            self.assertIn('replay_id', rpc.calls[0][2]); launch.assert_not_called()
            self.assertEqual(list(Path(temp).glob('terminal_*.json')), [])

    def test_lost_terminal_replay_ack_stops_after_finite_two_transports(self):
        class FakeRPC:
            calls = []
            def call(self, op, owner, args):
                self.calls.append(op); raise rt.Closed('CONSUMER_RPC_TRANSPORT_FAILURE')
        class FakeGuards:
            @staticmethod
            def scratch_safe(size): return True
        rpc = FakeRPC(); attempt = '00000000-0000-0000-0000-000000000001'
        with tempfile.TemporaryDirectory() as temp, patch.object(rt, 'ROOT', Path(temp)), patch.object(rt, 'guards', return_value=FakeGuards()):
            (Path(temp)/('terminal_'+attempt+'.json')).write_bytes(rt.canonical({'owner': 'original_owner', 'args': {'attempt_id': attempt, 'receipt': {}}}))
            for _ in range(2):
                with self.assertRaises(rt.Closed): rt.supervise_once(rpc, 'newowner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=1)
            final = rt.supervise_once(rpc, 'newowner', trigger='PERSISTENT_WORKER_TIMER', scheduled_at=1)
            self.assertEqual(rpc.calls, ['terminal_replay', 'terminal_replay'])
            self.assertEqual(final['state'], 'BLOCKED_BY_IDENTIFIED_DEPENDENCY')

    def test_full_cost_gate_rejects_date_pooling_missing_benchmarks_and_nonfinite_values(self):
        for mutation in ('date_pooling','missing_benchmark','nonfinite','free_replay','no_storage'):
            job,_=job_fixture();contract,work,act=rt.verify_job(job)
            collector=json.loads(act['resolved_bindings']['collector']['evidence_text'])
            cert=json.loads(job['input_certificate_artifact']['evidence_text'])
            proof=json.loads(job['resource_feasibility_artifact']['evidence_text'])
            if mutation=='date_pooling':
                proof['maximum_security_sessions_per_date']=101;proof['minimum_security_records_per_slice']=100
            elif mutation=='missing_benchmark':job['resource_benchmark_artifacts'].pop('COLD_HYDRATION')
            elif mutation=='nonfinite':proof['projected_governed_seconds']='NaN'
            elif mutation=='free_replay':job['terminal_replay_prepaid_seconds']=0
            else:job['terminal_storage_prepaid_bytes']=0
            ref,row=artifact('SUCCESSOR_BATCH_RESOURCE_FEASIBILITY','VERIFIED_FULL_DENOMINATOR_FINITE_COST','RESOURCE',proof)
            job['resource_feasibility_artifact']=row;collector['resource_feasibility_reference']=ref
            with self.assertRaises(ValueError):rt.load_local('eq20_prospective_batch').verify_plan(rt._self_module(),job,contract,work,collector,cert)

    def test_actual_calendar_receipt_cannot_substitute_different_official_clocks(self):
        job,_=job_fixture();act=json.loads(job['activation_artifact']['evidence_text'])
        calendar=json.loads(act['resolved_bindings']['calendar']['evidence_text'])
        calendar['sessions'][0]['open_at']='2027-01-04T14:31:00Z'
        ref,row=artifact('SUCCESSOR_OFFICIAL_CALENDAR_CERTIFICATE','VERIFIED_OFFICIAL_CALENDAR','CAL',calendar)
        act['resolved_bindings']['calendar']=row;act['frozen_plan']['bindings']['calendar']=ref
        ref,row=artifact('SUCCESSOR_EVIDENCE_ACTIVATION','AWAITING_ELIGIBLE_EVIDENCE','ACTIVATION',act)
        job['activation_reference']=ref;job['activation_artifact']=row
        with self.assertRaisesRegex(rt.Closed,'ACTUAL_OFFICIAL_CALENDAR_ROWS'):rt.verify_job(job)

    def test_aggregation_population_is_authorized_before_any_output_read(self):
        job,_=job_fixture();contract,work,act=rt.verify_job(job)
        collector=json.loads(act['resolved_bindings']['collector']['evidence_text'])
        certificate=json.loads(job['input_certificate_artifact']['evidence_text'])
        job['action']='AGGREGATE_DATE';job['dated_population_binding']=None
        with self.assertRaisesRegex(ValueError,'DATED_POPULATION_REGISTRY_BINDING_REQUIRED'):
            rt.load_local('eq20_prospective_batch').verify_plan(rt._self_module(),job,contract,work,collector,certificate)

    def test_batch_content_pin_precedes_import(self):
        job,_=job_fixture();act=json.loads(job['activation_artifact']['evidence_text'])
        collector=json.loads(act['resolved_bindings']['collector']['evidence_text']);collector['batch_module_sha256']='0'*64
        cr,ca=artifact('SUCCESSOR_COLLECTOR_RELEASE','VERIFIED_EXECUTABLE_RELEASE','COLLECTOR',collector)
        act['resolved_bindings']['collector']=ca;act['frozen_plan']['bindings']['collector']=cr
        ar,aa=artifact('SUCCESSOR_EVIDENCE_ACTIVATION','AWAITING_ELIGIBLE_EVIDENCE','ACTIVATION',act)
        job['activation_reference']=ar;job['activation_artifact']=aa
        with self.assertRaisesRegex(rt.Closed,'INSTALLED_COMPONENT_PIN_MISMATCH'):rt.verify_job(job)

    def test_source_executable_is_initialized_and_hash_checked_before_protected_input(self):
        job,_=job_fixture();job['source_producer_bindings']['source_files'][pf.p.ENTRYPOINT]+='\nchanged'
        with self.assertRaisesRegex(rt.Closed,'PRIVATE_SOURCE_CODE_CONTENT_PIN'):rt.verify_job(job)

    def test_kernel_source_pin_cannot_be_substituted(self):
        with patch.object(rt, 'KERNEL_SHA256', '0'*64):
            with self.assertRaisesRegex(rt.Closed, 'COMPONENT_PIN'):
                rt.verify_job(job_fixture()[0])


if __name__ == '__main__': unittest.main()
