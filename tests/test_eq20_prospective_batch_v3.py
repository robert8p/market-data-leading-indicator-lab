"""Synthetic transport, finite funding, and causal emission regression tests."""
from copy import deepcopy
from datetime import date, timedelta
import importlib.util
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path);obj=importlib.util.module_from_spec(spec);spec.loader.exec_module(obj);return obj
b=module('batch_v3_test',ROOT/'app/eq20_prospective_batch_v3.py')
old=module('batch_v3_legacy_fixture',ROOT/'tests/test_eq20_prospective_batch.py')
k=old.k
original_fixture=old.fixture


def transport_fixture(*args,**kwargs):
    result=original_fixture(*args,**kwargs);_,rows,work,job,_=result
    for segment in work['input_stream']['segments']:
        segment['member_source_bindings']=[{'ordinal':i,'security_id':rows[i]['security_id'],
            'raw_source_sha256':b.sha(b.canonical(rows[i])),'source_receipt_sha256':'a'*64}
            for i in range(segment['first_ordinal'],segment['last_ordinal']+1)]
    return result
old.fixture=transport_fixture
old.b=b


def projection():
    names=('actual_benchmark_readback_verified','complete_252_session_grid_costed',
        'consumer_recomputation_included','targeted_execution_acquisition_included',
        'server_permit_mint_and_mutation_costed','finite_chain_closure_costed',
        'all320_metadata_probes_costed','unknown_cycle_allowance_finite',
        'initial_admission_and_denied_claim_controls_costed','dated_workload_ceilings_reconciled',
        'producer_and_consumer_costs_in_same_projection','no_predicted_credit_for_unobserved_ambiguous_calls',
        'no_extra_paid_capacity')
    return dict({name:True for name in names},projected_total_cycles=2,projected_measured_cycles=1,
        projected_conservative_cycles=1,projected_chain_count=1,projected_initial_admission_controls=1,
        projected_measured_cycle_governed_seconds=1,projected_total_governed_seconds=2355,
        projected_initial_metadata_probes=1,projected_metadata_reconciliation_probes=0,
        projected_total_issued_metadata_probes=1,outside_session_probe_census={'initial':1,'reconciliation':0},
        combined_capture_consumer_dated_probe_census=[{
            'session_date':(date(2030,1,1)+timedelta(days=i)).isoformat(),
            'capture_initial':0,'consumer_initial':0,'reconciliation':0} for i in range(252)])


def producer_fixture():
    contract,raw=old.f.fixture();contract['reference_outcome_mode']='ORIGINAL_CERTIFIED_MINUTE_BAR_COVERAGE_V1'
    calls=[];selection=k.select_first_signals(contract,raw['features'])
    expected=dict(selection['first_indices'],__UNION__=selection['union_first_index'])
    entries=[]
    for claim,index in sorted(expected.items()):
        decision=k.stamp(raw['features']['decisions'][index]['decision_ts'])
        record={'version':'EQ20_COMMITTED_FIRST_ALERT_V1','activation_key':'SYNTHETIC',
            'session_date':raw['session_date'],'security_id':raw['security_id'],'claim_id':claim,
            'first_decision_index':index,'first_decision_ts':decision.isoformat(),
            'recorded_before_commit_at':(decision+timedelta(milliseconds=100)).isoformat()}
        text=b.canonical(record).decode();record_sha=b.sha(text.encode())
        ack={'version':'EQ20_FIRST_ALERT_ACK_OBSERVATION_V1','activation_key':'SYNTHETIC',
            'session_date':raw['session_date'],'security_id':raw['security_id'],'claim_id':claim,
            'first_alert_receipt_sha256':record_sha,
            'upper_bound_basis':'SERVER_NEXT_REQUEST_ENTRY_AFTER_ACTUAL_PRIOR_ACK',
            'host_clock_extrapolation_used':False,'decision_batch_receipt_sha256':'e'*64,
            'decision_batch_action_key':'SYNTHETIC_ACTION','first_member_ordinal':0,
            'ack_latest_utc':(decision+timedelta(seconds=1)).isoformat()}
        ack_text=b.canonical(ack).decode()
        entries.append({'claim_id':claim,'receipt_evidence_text':text,'receipt_sha256':record_sha,
            'acknowledgement_readback':{'receipt_evidence_text':ack_text,'receipt_sha256':b.sha(ack_text.encode())}})
    raw['capture_provenance']={'version':'EQ20_CAPTURE_FINAL_SOURCE_BINDING_V1','activation_key':'SYNTHETIC',
        'session_contract_sha256':k.digest(contract),'session_date':raw['session_date'],'security_id':raw['security_id'],
        'population_day_sha256':raw['population_day_sha256'],'source_manifest_sha256':'c'*64,
        'online_first_signal_state_sha256':'d'*64,'pipeline_module_sha256':'b'*64,
        'full_scheduled_decision_grid_committed':True,'source_truth_certified_by_assembly':False,
        'online_selection_sha256':k.digest(selection),'first_alert_receipts':entries}
    raw['execution_market']={'synthetic':True}
    job={'activation_key':'SYNTHETIC','pipeline_module_sha256':'b'*64,'capture_core_sha256':'c'*64,
        '_source_module_sha256':'d'*64,'_execution_module_sha256':'e'*64,'source_producer_bindings':{},
        'source_policy_binding':{'synthetic':True},'_current_member_source_binding':None}
    class Source:
        def produce_features(self,c,r,binding):calls.append('features');return {'features':r['features'],'provenance':{'synthetic':True}}
        def build_reference_labels(self,c,f,r,binding):calls.append('reference');return {'reference_labels':r['reference_labels']}
    class Execution:
        def build_execution_labels(self,c,f,market,indices):
            calls.append(('execution',indices));return {'execution_labels':[raw['execution_labels'][i] if i in indices else None for i in range(3)],'receipt_sha256':'f'*64}
    class Pipeline:
        def resolve(self,*args):return {'synthetic':True}
        def verify_native_capture_record(self,c,r,member,policy,**kwargs):
            calls.append('native_source_readback')
            if not isinstance(member,dict) or member.get('raw_source_sha256')!=k.digest(r):
                raise ValueError('NATIVE_SOURCE_BINDING_MISMATCH')
    source=Source();execution=Execution();pipeline=Pipeline()
    class Host:
        def load_local(self,name,expected=None):
            return {'eq20_prospective_features':source,'eq20_execution_replay':execution,
                'eq20_prospective_kernel':k,'eq20_prospective_capture_pipeline':pipeline,
                'eq20_prospective_capture':object()}[name]
    return contract,raw,job,Host(),calls


def bind(job,raw):
    job['_current_member_source_binding']={'raw_source_sha256':k.digest(raw),
        'source_receipt_sha256':'a'*64,'ordinal':0,'security_id':raw['security_id']}


class TransportRegression(old.BatchTest):
    def test_original_bar_mode_precedes_execution_and_uses_frozen_family(self):
        c,raw,job,host,calls=producer_fixture();bind(job,raw)
        result,proof=b.produce(host,job,c,raw)
        self.assertEqual(calls,['native_source_readback','features','reference',('execution',[0,1])])
        self.assertEqual(result['first_signal_selection'],k.select_first_signals(c,raw['features']))
        self.assertEqual(proof['raw_source_sha256'],k.digest(raw))


class FundingAndEmissionTest(unittest.TestCase):
    def test_both_daily_lanes_cannot_borrow_or_omit_finite_metadata_slots(self):
        proof=projection();proof['outside_session_probe_census']['initial']=0
        for row in proof['combined_capture_consumer_dated_probe_census']:
            row['capture_initial']=1;row['consumer_initial']=1
        proof.update(projected_initial_metadata_probes=504,projected_total_issued_metadata_probes=504)
        with self.assertRaises(ValueError):b.validate_carry_projection(proof,172800)
        proof.update(projected_initial_metadata_probes=252,projected_total_issued_metadata_probes=252)
        with self.assertRaises(ValueError):b.validate_carry_projection(proof,172800)

    def test_probe_census_exact_dates_recoveries_and_initial_control_relationship(self):
        good=projection();rows=good['combined_capture_consumer_dated_probe_census']
        official=[{'session_date':r['session_date']} for r in rows]
        self.assertEqual(b.validate_probe_projection(good,official)['total_issued_metadata_probes'],1)
        for mutation in ('missing_date','duplicate_date','wrong_official','missing_consumer','bool_count',
                         'unreported_recovery','extra_initial_control'):
            proof=deepcopy(good);expected=deepcopy(official)
            if mutation=='missing_date':proof['combined_capture_consumer_dated_probe_census'].pop()
            if mutation=='duplicate_date':proof['combined_capture_consumer_dated_probe_census'][1]['session_date']=rows[0]['session_date']
            if mutation=='wrong_official':expected[-1]['session_date']='2031-01-01'
            if mutation=='missing_consumer':del proof['combined_capture_consumer_dated_probe_census'][0]['consumer_initial']
            if mutation=='bool_count':proof['outside_session_probe_census']['initial']=True
            if mutation=='unreported_recovery':proof['outside_session_probe_census']['reconciliation']=1
            if mutation=='extra_initial_control':proof['projected_initial_admission_controls']=2
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):b.validate_probe_projection(proof,expected)

    def test_exact_projection_includes_all_controls_without_using_predicted_credit(self):
        result=b.validate_carry_projection(projection(),2355)
        self.assertEqual(result['remaining_after_projection'],'0')
        for omitted in (36,24,2240):
            bad=projection();bad['projected_total_governed_seconds']-=omitted
            with self.assertRaises(ValueError):b.validate_carry_projection(bad,172800)
        bad=projection();bad['no_predicted_credit_for_unobserved_ambiguous_calls']=False
        with self.assertRaises(ValueError):b.validate_carry_projection(bad,172800)

    def test_missing_nonfinite_overrun_or_inconsistent_finite_counts_rejected(self):
        cases=[('projected_total_cycles',3),('projected_chain_count',2),('projected_initial_admission_controls',0),
            ('projected_measured_cycles',0),('projected_measured_cycle_governed_seconds','NaN'),
            ('projected_measured_cycle_governed_seconds',True),('projected_measured_cycle_governed_seconds',43),
            ('actual_benchmark_readback_verified',None)]
        for key,value in cases:
            with self.subTest(key=key,value=value):
                proof=projection();proof[key]=value
                with self.assertRaises(ValueError):b.validate_carry_projection(proof,172800)
        with self.assertRaises(ValueError):b.validate_carry_projection(projection(),2354)

    def test_native_source_proof_and_online_selection_precede_labels(self):
        for mutation in ('native_hash','online_selection','cross_security'):
            c,raw,job,host,calls=producer_fixture()
            if mutation=='online_selection':raw['capture_provenance']['online_selection_sha256']='0'*64
            if mutation=='cross_security':raw['capture_provenance']['security_id']='OTHER'
            bind(job,raw)
            if mutation=='native_hash':job['_current_member_source_binding']['raw_source_sha256']='0'*64
            with self.assertRaises(ValueError):b.produce(host,job,c,raw)
            self.assertNotIn('reference',calls)
            self.assertFalse(any(isinstance(x,tuple) for x in calls))

    def test_missing_ack_retains_signal_and_conservative_unknown(self):
        c,raw,job,host,calls=producer_fixture()
        raw['capture_provenance']['first_alert_receipts'][0]['acknowledgement_readback']=None
        bind(job,raw);result,_=b.produce(host,job,c,raw)
        slot=result['claim_slots'][1]
        self.assertEqual([slot[k] for k in ('n_signals','n_success','n_nonqualify','n_unresolved')],[1,0,0,1])
        self.assertEqual(result['claim_slots'][0]['n_signals'],1)
        self.assertEqual(result['scheduled_decisions'],3)

    def test_late_union_cannot_borrow_a_timely_rule_execution_at_same_index(self):
        c,raw,job,host,calls=producer_fixture()
        item=next(x for x in raw['capture_provenance']['first_alert_receipts'] if x['claim_id']=='__UNION__')
        ack=__import__('json').loads(item['acknowledgement_readback']['receipt_evidence_text'])
        ack['ack_latest_utc']=(k.stamp(ack['ack_latest_utc'])+timedelta(seconds=2)).isoformat()
        text=b.canonical(ack).decode();item['acknowledgement_readback']={'receipt_evidence_text':text,'receipt_sha256':b.sha(text.encode())}
        bind(job,raw);result,_=b.produce(host,job,c,raw)
        self.assertEqual(result['claim_slots'][3]['n_nonqualify'],1)
        self.assertEqual(result['claim_slots'][11]['n_unresolved'],1)
        self.assertEqual(result['claim_slots'][11]['n_nonqualify'],0)
        self.assertEqual(result['claim_slots'][10]['n_success'],1)
        self.assertEqual(result['receipt_sha256'],k.digest({key:value for key,value in result.items() if key!='receipt_sha256'}))

    def test_insertion_clock_never_substitutes_for_actual_ack_or_cross_claim_proof(self):
        c,raw,job,host,calls=producer_fixture()
        entries=raw['capture_provenance']['first_alert_receipts']
        entries[0]['acknowledgement_readback']=deepcopy(entries[1]['acknowledgement_readback'])
        bind(job,raw)
        with self.assertRaisesRegex(ValueError,'EXACT_NATIVE_FIRST_ALERT_ACK'):b.produce(host,job,c,raw)
        self.assertNotIn('reference',calls)


if __name__=='__main__':unittest.main()
