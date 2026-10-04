"""Independent failure-path tests for the prospective closed-cycle proof codec.

These tests certify the validator's explicit gates. Actual meter-start ordering,
native atomic transfer and production resource capacity require separate proof.
"""
import copy
from decimal import Decimal
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

PATH=Path(__file__).resolve().parents[1]/'app'/'eq20_prospective_accounting_v3.py'
SPEC=importlib.util.spec_from_file_location('tested_eq20_prospective_accounting_v3',PATH)
a=importlib.util.module_from_spec(SPEC);sys.modules[SPEC.name]=a;SPEC.loader.exec_module(a)
SCOPE=dict(attempt_id='11111111-1111-4111-8111-111111111111',activation_key='SYNTHETIC_ACTIVATION',
 owner='SYNTHETIC_OWNER',host='SYNTHETIC_HOST',module_sha256='a'*64,terminal_sha256='b'*64,physical_sha256='c'*64)
BOOT='33333333-3333-4333-8333-333333333333'


def process():
    return dict(pid=12345,process_group=12345,start_ticks=123,boot_id=BOOT)


def transport_receipt(current):
    target='public.eq20_prospective_consumer_v3(text,text,jsonb)'
    stages=['READ_ONLY_METADATA'] if current['operation'] in ('status','probe') else ['PERMIT_MINT','PERMITTED_MUTATION']
    result=dict(version='EQ20_RPC_SERVER_ADMISSION_PERMIT_V3',target_rpc=target,
        logical_request_sha256=current['request_sha256'],all_started_calls_accounted=True,
        client_clock_used_for_admission=False,calls=[dict(stage=stage,
            rpc_name='eq20_request_permit_v3' if stage=='PERMIT_MINT' else 'eq20_prospective_consumer_v3',
            state='RESPONSE_READ_BACK',request_sha256='1'*64,response_sha256=current['response_sha256'],
            elapsed_seconds='0.04') for stage in stages])
    if len(stages)==2:
        result['permit']=dict(permit_id='55555555-5555-4555-8555-555555555555',
            host_instance=current['host_instance'],boot_id=BOOT,target_rpc=target,
            request_sha256=current['request_sha256'],request_jsonb_sha256='2'*64,
            issued_at='2026-10-04T16:00:00Z',expires_at='2026-10-04T16:00:00.500000Z')
    return result


def call(number=0,operation='terminal',slot=None):
    result=dict(sequence=number,call_id=format(number+1,'032x'),completed=True,response_readback_verified=True,
        sql_tail_closed=True,attempt_id=SCOPE['attempt_id'],owner=SCOPE['owner'],host_instance=SCOPE['host'],
        request_sha256='d'*64,response_sha256='e'*64,rpc_elapsed_seconds='0.1',helper_cpu_seconds='0.02',
        sql_tail_seconds='0',transport='REAPED_HTTP_HELPER',termination_proof='SPECIFIC_CHILD_WAIT4',
        process_identity=process(),recovery_slot=slot,operation=operation)
    if not operation.startswith('PROVIDER_GET_'):result['transport_receipt']=transport_receipt(result)
    return result


def fixture():
    calls=[call()]
    cut=dict(meter_id='1'*32,attempt_id=SCOPE['attempt_id'],host_instance=SCOPE['host'],boot_id=BOOT,
        terminal_ack_sha256='e'*64,all_old_calls_closed=True,all_children_reaped=True,
        parent_cpu_counter_at_cut='100.5',monotonic_at_cut='123456.5')
    return dict(version=a.VERSION,attempt_id=SCOPE['attempt_id'],activation_key=SCOPE['activation_key'],
        owner=SCOPE['owner'],host_instance=SCOPE['host'],module_sha256=SCOPE['module_sha256'],
        terminal_receipt_sha256=SCOPE['terminal_sha256'],physical_quiescence_sha256=SCOPE['physical_sha256'],
        all_started_calls_accounted=True,all_sql_tails_closed=True,terminal_acknowledgement_observed=True,
        prior_v2_debits_changed=False,measurement_boundary='AFTER_EXACT_TERMINAL_ACK_AND_HELPER_WAIT4',
        meter_cut=cut,meter_cut_sha256=a.digest(cut),terminal_ack_sha256='e'*64,
        unknown_call_count=0,calls=calls,calls_sha256=a.digest(calls),boot_id=BOOT,
        research_child=dict(process_finished=True,termination_proof='SPECIFIC_CHILD_WAIT4',
            process_identity=process(),cpu_seconds='1',wall_seconds='2'),
        parent_control_cpu_seconds='0.1',recovery_parent_cpu_seconds='0',governed_seconds='1.220000')


def run(proof):
    return a.validate_closed_cycle(proof,**SCOPE)


class ClosedCycleTests(unittest.TestCase):
    def test_complete_bound_receipt_computes_exact_conservative_sum(self):
        result=run(fixture())
        self.assertEqual(result['governed_seconds'],'1.220000')
        self.assertEqual(result['reservation_seconds'],54)
        self.assertFalse(result['research_objective_achieved'])

    def test_missing_terminal_ack_never_earns_credit(self):
        proof=fixture();proof.pop('terminal_acknowledgement_observed')
        with self.assertRaises(a.Closed):run(proof)

    def test_unknown_call_keeps_the_whole_reservation(self):
        proof=fixture();proof['unknown_call_count']=1
        with self.assertRaisesRegex(a.Closed,'UNKNOWN_CYCLE'):run(proof)

    def test_unknown_tail_cannot_be_relabeled_closed(self):
        proof=fixture();proof['calls'][0]['sql_tail_closed']=False;proof['calls_sha256']=a.digest(proof['calls'])
        with self.assertRaises(a.Closed):run(proof)

    def test_missing_or_incomplete_two_http_census_retains_full_reservation(self):
        for delta in ('missing','not_all_accounted','unknown_mint','unknown_mutation','missing_mutation'):
            with self.subTest(delta=delta):
                proof=fixture();current=proof['calls'][0];receipt=current['transport_receipt']
                if delta=='missing':current.pop('transport_receipt')
                elif delta=='not_all_accounted':receipt['all_started_calls_accounted']=False
                elif delta=='unknown_mint':receipt['calls'][0]['state']='STARTED_RESPONSE_UNKNOWN'
                elif delta=='unknown_mutation':receipt['calls'][1]['state']='STARTED_RESPONSE_UNKNOWN'
                else:receipt['calls'].pop()
                proof['calls_sha256']=a.digest(proof['calls'])
                with self.assertRaises(a.Closed):run(proof)

    def test_inner_elapsed_cannot_borrow_unrecorded_outer_time(self):
        proof=fixture();proof['calls'][0]['transport_receipt']['calls'][1]['elapsed_seconds']='0.060001'
        proof['calls_sha256']=a.digest(proof['calls'])
        with self.assertRaisesRegex(a.Closed,'NOT_COVERED_BY_ACTUAL_OUTER_CALL'):run(proof)

    def test_two_inner_http_times_are_verified_without_double_charge(self):
        proof=fixture();proof['calls'][0]['transport_receipt']['calls'][1]['elapsed_seconds']='0.06'
        proof['calls_sha256']=a.digest(proof['calls'])
        self.assertEqual(run(proof)['governed_seconds'],'1.220000')

    def test_permit_cannot_rebind_another_host_boot_target_or_request(self):
        for key in ('host_instance','boot_id','target_rpc','request_sha256'):
            with self.subTest(key=key):
                proof=fixture();proof['calls'][0]['transport_receipt']['permit'][key]='OTHER'
                proof['calls_sha256']=a.digest(proof['calls'])
                with self.assertRaisesRegex(a.Closed,'PERMIT_AND_PROCESS_SCOPE'):run(proof)

    def test_inner_mutation_ack_must_equal_outer_final_ack(self):
        proof=fixture();proof['calls'][0]['transport_receipt']['calls'][-1]['response_sha256']='f'*64
        proof['calls_sha256']=a.digest(proof['calls'])
        with self.assertRaisesRegex(a.Closed,'NOT_COVERED_BY_ACTUAL_OUTER_CALL'):run(proof)

    def test_nonfinite_inner_measurement_is_rejected(self):
        for value in ('NaN','Infinity',True,-1):
            with self.subTest(value=value):
                current=call();current['transport_receipt']['calls'][0]['elapsed_seconds']=value
                with self.assertRaises(a.Closed):a.validate_transport_receipt(current,Decimal('.1'),boot=BOOT)

    def test_read_only_metadata_and_provider_have_distinct_actual_transports(self):
        current=call(operation='status');a.validate_transport_receipt(current,Decimal('.1'),boot=BOOT)
        self.assertNotIn('permit',current['transport_receipt'])
        provider=call(operation='PROVIDER_GET_BARS');a.validate_transport_receipt(provider,Decimal('.1'),boot=BOOT)
        provider['transport_receipt']=current['transport_receipt']
        with self.assertRaisesRegex(a.Closed,'CANNOT_IMPERSONATE_DB_PERMIT'):
            a.validate_transport_receipt(provider,Decimal('.1'),boot=BOOT)

    def test_nonfinite_or_boolean_measurements_are_rejected(self):
        for value in ['NaN','Infinity','-Infinity',True,-1]:
            with self.subTest(value=value):
                proof=fixture();proof['research_child']['cpu_seconds']=value
                with self.assertRaises(a.Closed):run(proof)

    def test_duplicate_call_identity_is_not_additional_evidence(self):
        proof=fixture();proof['calls'].append(copy.deepcopy(proof['calls'][0]));proof['calls'][1]['sequence']=1
        proof['calls_sha256']=a.digest(proof['calls'])
        with self.assertRaisesRegex(a.Closed,'CALL_ID_UNIQUE'):run(proof)

    def test_sequence_gap_rejected_even_with_new_manifest_hash(self):
        proof=fixture();proof['calls'][0]['sequence']=1;proof['calls_sha256']=a.digest(proof['calls'])
        with self.assertRaisesRegex(a.Closed,'CALL_RECEIPT'):run(proof)

    def test_cross_owner_call_is_rejected(self):
        proof=fixture();proof['calls'][0]['owner']='OTHER_OWNER';proof['calls_sha256']=a.digest(proof['calls'])
        with self.assertRaisesRegex(a.Closed,'CALL_RECEIPT'):run(proof)

    def test_whole_proof_cannot_rebind_another_activation(self):
        proof=fixture();proof['activation_key']='OTHER_ACTIVATION'
        with self.assertRaisesRegex(a.Closed,'SCOPE_OR_ACK'):run(proof)

    def test_manifest_byte_changes_require_actual_new_hash(self):
        proof=fixture();proof['calls'][0]['rpc_elapsed_seconds']='0.2'
        with self.assertRaisesRegex(a.Closed,'CALL_MANIFEST'):run(proof)

    def test_terminal_ack_must_be_the_last_actual_call(self):
        proof=fixture();proof['calls'].append(call(1,'source_capture'));proof['calls_sha256']=a.digest(proof['calls'])
        with self.assertRaisesRegex(a.Closed,'END_WITH_ACKNOWLEDGED_TERMINAL'):run(proof)

    def test_terminal_ack_cannot_reference_a_different_response_hash(self):
        proof=fixture();proof['terminal_ack_sha256']='f'*64
        with self.assertRaisesRegex(a.Closed,'TERMINAL_ACK_IN_CALL'):run(proof)

    def test_a_pid_from_another_boot_is_not_quiescence(self):
        proof=fixture();proof['research_child']['process_identity']['boot_id']='44444444-4444-4444-8444-444444444444'
        with self.assertRaisesRegex(a.Closed,'PROCESS_IDENTITY'):run(proof)

    def test_overrun_cannot_be_credited_below_the_reservation(self):
        proof=fixture();proof['research_child']['cpu_seconds']='30'
        with self.assertRaisesRegex(a.Closed,'ENVELOPE_EXCEEDED'):run(proof)

    def test_only_two_registered_recovery_slots_exist(self):
        proof=fixture();proof['calls'][0]['recovery_slot']=2;proof['calls_sha256']=a.digest(proof['calls'])
        with self.assertRaisesRegex(a.Closed,'FINITE_RECOVERY_SLOT'):run(proof)

    def test_one_recovery_slot_cannot_borrow_another_slots_unused_limit(self):
        proof=fixture();proof['calls']=[call(0,'capture',0),call(1,'terminal',1)]
        proof['calls'][0].update(rpc_elapsed_seconds='7',helper_cpu_seconds='6')
        proof['calls'][1].update(rpc_elapsed_seconds='1',helper_cpu_seconds='0')
        proof['calls_sha256']=a.digest(proof['calls']);proof['governed_seconds']='15.350000'
        with self.assertRaisesRegex(a.Closed,'INDIVIDUAL_RECOVERY_SLOT'):run(proof)

    def test_shared_recovery_parent_cpu_is_conservatively_charged_to_each_slot(self):
        proof=fixture();proof['calls']=[call(0,'capture',0),call(1,'terminal',1)]
        proof['calls'][0].update(rpc_elapsed_seconds='9',helper_cpu_seconds='2.9')
        proof['calls'][1].update(rpc_elapsed_seconds='0.1',helper_cpu_seconds='0')
        proof['recovery_parent_cpu_seconds']='0.2';proof['calls_sha256']=a.digest(proof['calls'])
        proof['governed_seconds']='13.550000'
        with self.assertRaisesRegex(a.Closed,'INDIVIDUAL_RECOVERY_SLOT'):run(proof)

    def test_unattributed_recovery_parent_work_requires_a_slot(self):
        proof=fixture();proof['recovery_parent_cpu_seconds']='0.1'
        with self.assertRaisesRegex(a.Closed,'ENVELOPE_EXCEEDED'):run(proof)

    def test_exact_sum_cannot_round_down_arbitrarily(self):
        proof=fixture();proof['governed_seconds']='1.219999'
        with self.assertRaisesRegex(a.Closed,'EXACT_MEASURED_GOVERNED_SUM'):run(proof)

    def test_final_closure_is_full54_and_never_claims_a_refund(self):
        receipt=a.conservative_charge('FINAL_COHORT_CLOSURE',attempt_id=SCOPE['attempt_id'],activation_key=SCOPE['activation_key'])
        self.assertEqual(receipt['governed_seconds'],'54.000000');self.assertEqual(receipt['credit_seconds'],'0.000000')

    def test_old_arbitrary_closure_credit_cannot_replace_a_measured_cut(self):
        proof=fixture();proof.pop('meter_cut');proof['measurement_closure_allowance_seconds']='0.25'
        with self.assertRaisesRegex(a.Closed,'METER_CUT'):run(proof)

    def test_meter_cut_matches_exact_ack_host_boot_attempt(self):
        for key,value in [('attempt_id','DIFFERENT'),('host_instance','DIFFERENT'),
                ('boot_id','44444444-4444-4444-8444-444444444444'),('terminal_ack_sha256','f'*64),
                ('all_old_calls_closed',False),('all_children_reaped',False)]:
            with self.subTest(key=key):
                proof=fixture();proof['meter_cut'][key]=value;proof['meter_cut_sha256']=a.digest(proof['meter_cut'])
                with self.assertRaisesRegex(a.Closed,'METER_CUT'):run(proof)

    def test_cut_bytes_cannot_change_without_their_recorded_hash(self):
        proof=fixture();proof['meter_cut']['parent_cpu_counter_at_cut']='100.6'
        with self.assertRaisesRegex(a.Closed,'METER_CUT'):run(proof)

    def test_actual_nonnegative_finite_cut_counters_required(self):
        for key in ['parent_cpu_counter_at_cut','monotonic_at_cut']:
            for value in ['NaN','Infinity',-1,True,None]:
                with self.subTest(key=key,value=value):
                    proof=fixture();proof['meter_cut'][key]=value;proof['meter_cut_sha256']=a.digest(proof['meter_cut'])
                    with self.assertRaises(a.Closed):run(proof)


class RolloverTests(unittest.TestCase):
    def rollover(self,proof,**overrides):
        kwargs=dict(next_meter_id='2'*32,next_attempt_id='22222222-2222-4222-8222-222222222222',
            next_owner=SCOPE['owner'],next_host=SCOPE['host'],next_boot_id=BOOT,meter_started_before_closure=True)
        kwargs.update(overrides)
        return a.prepare_rollover(proof,**kwargs)

    def full_prefix(self,governed_seconds):
        proof=fixture();proof['research_child']['cpu_seconds']='30';proof['parent_control_cpu_seconds']='0'
        remainder=Decimal(governed_seconds)-Decimal('30')
        proof['calls']=[call(0,'capture',0),call(1,'terminal',1)]
        for current,amount in zip(proof['calls'],[min(remainder,Decimal('12')),max(Decimal(0),remainder-12)]):
            current.update(rpc_elapsed_seconds=str(min(amount,Decimal('9'))),
                helper_cpu_seconds=str(max(Decimal(0),amount-9)),sql_tail_seconds='0')
            if amount<Decimal('.08'):
                for inner in current['transport_receipt']['calls']:inner['elapsed_seconds']='0'
        proof['calls_sha256']=a.digest(proof['calls']);proof['governed_seconds']=format(Decimal(governed_seconds),'.6f')
        return proof

    def test_valid_rollover_binds_canonical_prior_cut_and_new_meter(self):
        proof=fixture();result=self.rollover(proof)
        self.assertEqual(result['closed_cycle_canonical_utf8'],a.canonical(proof).decode())
        self.assertEqual(result['next_meter_sha256'],a.digest(result['next_meter']))
        self.assertEqual(result['next_meter']['predecessor_meter_cut_sha256'],proof['meter_cut_sha256'])
        self.assertEqual(result['next_meter']['bounded_rollover_allowance_seconds'],'12')
        self.assertTrue(result['next_meter']['denied_or_unknown_admission_keeps_old_full54'])

    def test_exact_twelve_seconds_of_held_headroom_is_sufficient(self):
        result=self.rollover(self.full_prefix('42'))
        self.assertEqual(result['next_meter']['old_proven_prefix_seconds'],'42.000000')

    def test_prefix_without_twelve_seconds_cannot_start_unfunded_rollover(self):
        for amount in ['42.000001','54']:
            with self.subTest(amount=amount):
                proof=self.full_prefix(amount)
                self.assertEqual(run(proof)['governed_seconds'],format(Decimal(amount),'.6f'))
                with self.assertRaisesRegex(a.Closed,'NO_FUNDED_ROLLOVER_HEADROOM'):self.rollover(proof)

    def test_a_new_durable_meter_must_precede_any_closure_work(self):
        for changes in [dict(meter_started_before_closure=False),dict(next_meter_id='1'*32),
                dict(next_meter_id='INVALID'),dict(next_attempt_id='INVALID'),dict(next_owner=''),
                dict(next_host=''),dict(next_boot_id='INVALID'),
                dict(next_attempt_id=SCOPE['attempt_id']),dict(next_owner='DIFFERENT'),
                dict(next_host='DIFFERENT'),dict(next_boot_id='44444444-4444-4444-8444-444444444444')]:
            with self.subTest(changes=changes):
                with self.assertRaisesRegex(a.Closed,'DURABLE_SUCCESSOR_METER'):self.rollover(fixture(),**changes)

    def test_builder_produces_exact_proof_without_fixed_closure_credit(self):
        proof=fixture();scope={k:proof[k] for k in ['attempt_id','activation_key','owner','host_instance','module_sha256','boot_id']}
        journal={k:proof[k] for k in ['all_started_calls_accounted','all_sql_tails_closed','unknown_call_count','calls','calls_sha256']}
        result=a.build_closed_cycle(journal=journal,scope=scope,research_child=proof['research_child'],
            terminal_ack_sha256=proof['terminal_ack_sha256'],terminal_sha256=proof['terminal_receipt_sha256'],
            physical_sha256=proof['physical_quiescence_sha256'],parent_cpu_seconds='0.1',
            recovery_parent_cpu_seconds='0',meter_cut=proof['meter_cut'])
        self.assertEqual(run(result)['governed_seconds'],'1.220000')
        self.assertNotIn('measurement_closure_allowance_seconds',result)


class CallJournalTests(unittest.TestCase):
    def test_missing_finish_survives_cache_readback_as_unknown(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'journal.json';scope=dict(attempt_id=SCOPE['attempt_id'],owner=SCOPE['owner'])
            journal=a.CallJournal(path,scope,scratch_safe=lambda size:True)
            journal.begin('capture','d'*64,transport='REAPED_HTTP_HELPER')
            restored=a.CallJournal(path,scope,scratch_safe=lambda size:True)
            with self.assertRaisesRegex(a.Closed,'UNKNOWN_CALL'):restored.export()
            with self.assertRaisesRegex(a.Closed,'UNFINISHED_CALL'):restored.begin('terminal','d'*64,transport='REAPED_HTTP_HELPER')

    def test_journal_scope_cannot_switch_activation(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'journal.json';journal=a.CallJournal(path,{'activation':'FIRST'},scratch_safe=lambda size:True)
            journal.begin('capture','d'*64,transport='REAPED_HTTP_HELPER')
            with self.assertRaisesRegex(a.Closed,'JOURNAL_SCOPE'):
                a.CallJournal(path,{'activation':'SECOND'},scratch_safe=lambda size:True)

    def test_missing_transport_finish_cannot_clear_pending_call(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'journal.json';current=call();scope={k:current[k] for k in ('attempt_id','owner','host_instance')}
            scope['boot_id']=BOOT;journal=a.CallJournal(path,scope,scratch_safe=lambda size:True)
            call_id=journal.begin('terminal','d'*64,transport='REAPED_HTTP_HELPER')
            with self.assertRaisesRegex(a.Closed,'COMPLETE_SERVER_PERMIT_TRANSPORT'):
                journal.finish(call_id,response_sha256='e'*64,rpc_elapsed_seconds='.1',
                    helper_cpu_seconds='.02',process_identity=process(),termination_proof='SPECIFIC_CHILD_WAIT4')
            self.assertEqual(journal.data['pending']['call_id'],call_id)
            with self.assertRaisesRegex(a.Closed,'UNKNOWN_CALL'):journal.export()

    def test_failed_finish_publication_preserves_memory_and_durable_pending(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'journal.json';current=call();scope={k:current[k] for k in ('attempt_id','owner','host_instance')}
            scope['boot_id']=BOOT;journal=a.CallJournal(path,scope,scratch_safe=lambda size:True)
            call_id=journal.begin('terminal','d'*64,transport='REAPED_HTTP_HELPER')
            with patch.object(a,'checked_atomic',side_effect=OSError('synthetic disk failure')):
                with self.assertRaises(OSError):journal.finish(call_id,response_sha256='e'*64,rpc_elapsed_seconds='.1',
                    helper_cpu_seconds='.02',process_identity=process(),termination_proof='SPECIFIC_CHILD_WAIT4',
                    transport_receipt=current['transport_receipt'])
            self.assertEqual(journal.data['pending']['call_id'],call_id)
            restored=a.CallJournal(path,scope,scratch_safe=lambda size:True)
            with self.assertRaisesRegex(a.Closed,'UNKNOWN_CALL'):restored.export()


if __name__=='__main__':unittest.main()
