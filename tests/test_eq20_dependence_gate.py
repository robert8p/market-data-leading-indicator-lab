"""Synthetic native-record gate checks; no market-independence claim is tested."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

PATH=Path(__file__).resolve().parents[1]/'app'/'eq20_dependence_gate.py'
SPEC=importlib.util.spec_from_file_location('tested_eq20_dependence_gate',PATH)
g=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(g)


def record(kind,status,evidence,hour=10):
    raw=json.dumps(evidence,sort_keys=True)
    h=hashlib.sha256(raw.encode()).hexdigest()
    ref=dict(kind=kind,status=status,artifact_key='SYNTHETIC_'+kind,implementation_sha256=h)
    return dict(ref,evidence_text=raw,evidence=evidence,created_at=f'2026-10-04T{hour:02d}:00:00Z'),ref


def fixture():
    scope={name:hashlib.sha256(name.encode()).hexdigest() for name in g.SCOPE_KEYS}
    scope['method_contract_sha256']=g.METHOD
    sh=g.digest(scope);lineage='a'*64
    policy=dict(gate_version=g.VERSION,method_contract_sha256=g.METHOD,
        gate_source_sha256=hashlib.sha256(PATH.read_bytes()).hexdigest(),
        market_independence_currently_established=False,diagnostic_nonrejection_grants_independence=False,
        protected_outcomes_accessed=False)
    pr,pref=record('SUCCESSOR_DEPENDENCE_REVIEW_POLICY','REGISTERED_OUTCOME_BLIND',policy,8)
    rows={};refs={}
    support={
        'structural_analysis':dict(actual_source_and_rule_lineage_inspected=True,
            candidate_family_sha256=scope['candidate_family_sha256'],
            population_manifest_sha256=scope['population_manifest_sha256'],source_lineage_sha256=lineage,
            cross_date_dependency_graph_sha256='b'*64,omitted_known_shared_drivers=[],
            driver_assessments={x:dict(analysis='Synthetic structural-model review only.',
                incidence_conditioning_addressed=True,disposition='EXPLICIT_UNTESTABLE_MODEL_ASSUMPTION_REVIEWED')
                for x in g.DRIVER_FAMILIES}),
        'mathematical_analysis':dict(assumption=g.ASSUMPTION,estimand=g.ESTIMAND,
            factorization_statement='Synthetic conditional product-law fixture.',
            conditional_incidence_weighting_proved=True,unconditional_independence_substituted_for_conditional=False),
        'diagnostic_protocol':dict(evidence_class='DEVELOPMENT_ONLY',registered_before_diagnostic_analysis=True,
            historical_development_exposure_disclosed=True,diagnostic_results_can_select_method_or_calendar=False,
            nonrejection_can_grant_independence=False,prespecified_checks={x:'Synthetic predeclared diagnostic.' for x in g.DIAGNOSTIC_CHECKS},
            adverse_result_action='BLOCK_AND_PRESERVE_OR_REGISTER_SEPARATE_OUTCOME_BLIND_SUCCESSOR'),
    }
    for role,data in support.items():
        data.update(scope_sha256=sh,protected_outcomes_accessed=False)
        rows[role],refs[role]=record(*g.SUPPORT_ROLES[role],data,9)
    assessment=dict(scope_sha256=sh,protected_outcomes_accessed=False,
        protocol_artifact_sha256=refs['diagnostic_protocol']['implementation_sha256'],
        analysis_code_sha256='c'*64,development_input_manifest_sha256='d'*64,actual_execution_receipt_sha256='e'*64,
        evidence_class='DEVELOPMENT_ONLY',independence_inferred_from_nonrejection=False,reserved_outcomes_read=False,
        check_results={x:dict(state='INSUFFICIENT_DIAGNOSTIC_INFORMATION',evidence_sha256='f'*64,
            limitations='Synthetic fixture cannot establish market independence.') for x in g.DIAGNOSTIC_CHECKS})
    rows['diagnostic_assessment'],refs['diagnostic_assessment']=record(*g.SUPPORT_ROLES['diagnostic_assessment'],assessment,10)
    common=dict(scope_sha256=sh,method_contract_sha256=g.METHOD,
        dependence_review_policy_sha256=pref['implementation_sha256'],protected_outcomes_accessed=False)
    argument=dict(common,estimand=g.ESTIMAND,weights='n_d/sum_d(n_d)',within_date_dependence='UNRESTRICTED',
        statistical_unit='COMPLETE_OFFICIAL_SESSION_DATE_CLUSTER',
        conditioning_information='ACTUAL_COMPLETE_FIXED_CANDIDATE_ALERT_INCIDENCE',assumption=g.ASSUMPTION,
        sections={x:'Synthetic explicit conditional-model argument.' for x in g.REQUIRED_SECTIONS},
        justification_bases=['EXPLICIT_STRUCTURAL_PROBABILITY_MODEL_AND_INDEPENDENT_REVIEW'],
        incidence_conditioning_can_induce_dependence_addressed=True,future_transportability_is_additional_assumption=True,
        finite_diagnostics_prove_independence=False,remaining_model_assumptions=['Synthetic untestable product-law assumption.'],
        unresolved_material_objections=[],supporting_references=refs,author_id='SYNTHETIC_AUTHOR')
    ar,aref=record('SUCCESSOR_DEPENDENCE_ARGUMENT','REGISTERED_OUTCOME_BLIND_ASSUMPTION_JUSTIFICATION',argument,11)
    review=dict(common,argument_artifact_sha256=aref['implementation_sha256'],reviewed_supporting_references=refs,
        independent_reviewer_id='SYNTHETIC_DISTINCT_REVIEWER',author_and_reviewer_distinct=True,
        section_decisions={x:'SUPPORTED_AS_EXPLICIT_CONDITIONAL_MODEL_ASSUMPTION' for x in g.REQUIRED_SECTIONS},
        unresolved_objections=[],conditional_date_independence_supported=True,
        reviewed_model_assumptions=argument['remaining_model_assumptions'],nonrejection_only_justification=False,
        independence_empirically_proved=False)
    rr,rref=record('SUCCESSOR_DEPENDENCE_REVIEW','VERIFIED_CONDITIONAL_DATE_INDEPENDENCE',review,12)
    return dict(argument_row=ar,argument_ref=aref,review_row=rr,review_ref=rref,supporting_rows=rows,
        policy_row=pr,policy_ref=pref,scope=scope,source_lineage_sha256=lineage,first_reserved_outcome_at='2026-10-05T13:30:00Z')


def rewrite(data,role,change):
    """Create a new, self-consistent synthetic record, including review bindings."""
    if role=='argument':
        evidence=copy.deepcopy(data['argument_row']['evidence']);change(evidence)
        row,ref=record(data['argument_ref']['kind'],data['argument_ref']['status'],evidence,11)
        data['argument_row']=row;data['argument_ref']=ref
        rewrite(data,'review',lambda x:x.update(argument_artifact_sha256=ref['implementation_sha256']))
    elif role=='review':
        evidence=copy.deepcopy(data['review_row']['evidence']);change(evidence)
        data['review_row'],data['review_ref']=record(data['review_ref']['kind'],data['review_ref']['status'],evidence,12)
    else:
        evidence=copy.deepcopy(data['supporting_rows'][role]['evidence']);change(evidence)
        row,ref=record(*g.SUPPORT_ROLES[role],evidence,10 if role=='diagnostic_assessment' else 9)
        data['supporting_rows'][role]=row
        rewrite(data,'argument',lambda x:x['supporting_references'].update({role:ref}))
        rewrite(data,'review',lambda x:x['reviewed_supporting_references'].update({role:ref}))


class DependenceGateTests(unittest.TestCase):
    def check_blocked(self,data):
        result=g.adjudicate(**data)
        self.assertEqual(result['state'],'BLOCKED_BY_IDENTIFIED_DEPENDENCY')
        self.assertFalse(result['grants_evidence_access']);self.assertFalse(result['claims_certified'])
        return result

    def test_full_synthetic_record_set_verifies_only_provenance_and_review_content(self):
        result=g.adjudicate(**fixture())
        self.assertEqual(result['state'],'VERIFIED',result)
        for name in ['independence_empirically_proved','future_stationarity_proved','claims_certified',
                'grants_evidence_access','research_objective_achieved']:
            self.assertFalse(result[name])

    def test_boolean_only_independence_receipt_cannot_pass(self):
        data=fixture();rewrite(data,'argument',lambda x:x.clear())
        self.check_blocked(data)

    def test_registered_native_bytes_and_hash_are_required(self):
        data=fixture();data['review_row']['evidence_text']+=' '
        self.check_blocked(data)

    def test_no_late_or_timezone_ambiguous_registration(self):
        for stamp in ['2026-10-05T13:30:00Z','2026-10-04T12:00:00']:
            data=fixture();data['review_row']['created_at']=stamp
            self.check_blocked(data)

    def test_no_diagnostic_shortcut_can_justify_independence(self):
        for basis in g.PROHIBITED_BASES:
            with self.subTest(basis=basis):
                data=fixture();rewrite(data,'argument',lambda x:x.update(justification_bases=[basis]))
                self.check_blocked(data)

    def test_different_estimand_or_weights_do_not_reuse_v2(self):
        for key,value in [('weights','1/D'),('estimand','FINITE_POPULATION_LABEL_AUDIT'),
            ('assumption','UNCONDITIONAL_DATE_INDEPENDENCE'),('conditioning_information','PAST_ONLY')]:
            data=fixture();rewrite(data,'argument',lambda x:x.update({key:value}));self.check_blocked(data)

    def test_actual_candidate_population_and_source_must_match_structural_analysis(self):
        for key in ['candidate_family_sha256','population_manifest_sha256','source_lineage_sha256']:
            data=fixture();rewrite(data,'structural_analysis',lambda x:x.update({key:'0'*64}));self.check_blocked(data)

    def test_all_known_shared_driver_families_require_analysis(self):
        data=fixture();rewrite(data,'structural_analysis',lambda x:x['driver_assessments'].pop(next(iter(g.DRIVER_FAMILIES))))
        self.check_blocked(data)

    def test_material_structural_objection_cannot_be_hidden_by_passing_review(self):
        data=fixture();rewrite(data,'argument',lambda x:x.update(unresolved_material_objections=['Shared event residuals unexplained.']))
        self.check_blocked(data)

    def test_explicit_model_risk_is_retained_not_asserted_away(self):
        data=fixture();rewrite(data,'argument',lambda x:x.update(remaining_model_assumptions=[]))
        self.check_blocked(data)

    def test_actual_diagnostic_assessment_is_required(self):
        data=fixture();data['supporting_rows'].pop('diagnostic_assessment')
        self.check_blocked(data)

    def test_diagnostic_protocol_must_precede_its_analysis_receipt(self):
        data=fixture();data['supporting_rows']['diagnostic_protocol']['created_at']='2026-10-04T10:30:00Z'
        self.check_blocked(data)

    def test_development_exposure_is_explicit_and_diagnostics_never_grant_pass(self):
        for role,key,value in [('diagnostic_protocol','historical_development_exposure_disclosed',False),
                ('diagnostic_assessment','reserved_outcomes_read',True),
                ('diagnostic_assessment','independence_inferred_from_nonrejection',True)]:
            data=fixture();rewrite(data,role,lambda x:x.update({key:value}));self.check_blocked(data)

    def test_same_author_cannot_supply_independent_review(self):
        data=fixture();rewrite(data,'review',lambda x:x.update(independent_reviewer_id='SYNTHETIC_AUTHOR'))
        self.check_blocked(data)

    def test_author_identity_cannot_be_missing_or_disguised_with_whitespace(self):
        for value in [None,'','   ',' SYNTHETIC_DISTINCT_REVIEWER ']:
            data=fixture();rewrite(data,'argument',lambda x:x.update(author_id=value));self.check_blocked(data)

    def test_review_cannot_omit_remaining_model_assumptions(self):
        data=fixture();rewrite(data,'review',lambda x:x.update(reviewed_model_assumptions=[]))
        self.check_blocked(data)

    def test_actual_policy_readback_and_current_code_pin_are_required(self):
        data=fixture();data['policy_row']['evidence_text']=data['policy_row']['evidence_text'].replace('REGISTERED','CHANGED')+' '
        self.check_blocked(data)

    def test_unknown_or_malformed_shapes_fail_closed(self):
        for key in ['argument_row','policy_row','scope','review_ref','supporting_rows']:
            data=fixture();data[key]=None;self.check_blocked(data)


if __name__=='__main__':unittest.main()
