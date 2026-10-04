"""Substantive, outcome-blind evidence gate for the conditional EQ20 theorem.

This validates the provenance and required content of an independent assumption
review. It neither infers independence from data nor turns a diagnostic into a
proof. An unsupported assumption remains an explicit scientific dependency.
"""
from datetime import datetime
import hashlib
import json
import re
from pathlib import Path

VERSION='EQ20_DEPENDENCE_ADJUDICATION_GATE_V1'
METHOD='9143ac194c39051d5224890e19e9b3b33dc5264e3085d2d4bff4126a7aea6d21'
HASH=re.compile(r'[0-9a-f]{64}')
ESTIMAND='SIGNAL_WEIGHTED_EXPECTED_CONSERVATIVE_FAILURE_CONDITIONAL_ON_FULL_ALERT_INCIDENCE'
ASSUMPTION='MUTUAL_DATE_CLUSTER_INDEPENDENCE_CONDITIONAL_ON_FULL_ALERT_INCIDENCE'
SCOPE_KEYS=('candidate_family_sha256','population_manifest_sha256','official_calendar_sha256',
 'official_session_dates_sha256','execution_policy_sha256','method_contract_sha256')
REQUIRED_SECTIONS=('estimand_and_incidence_conditioning','causal_information_flow',
 'cross_date_shared_drivers','mathematical_factorization','diagnostic_design_and_limitations',
 'population_and_transportability','remaining_model_risk')
SUPPORT_ROLES={
 'structural_analysis':('SUCCESSOR_DEPENDENCE_STRUCTURAL_ANALYSIS','VERIFIED_OUTCOME_BLIND_STRUCTURE'),
 'mathematical_analysis':('SUCCESSOR_DEPENDENCE_MATHEMATICAL_ANALYSIS','VERIFIED_CONDITIONAL_DERIVATION'),
 'diagnostic_protocol':('SUCCESSOR_DEPENDENCE_DIAGNOSTIC_PROTOCOL','REGISTERED_OUTCOME_BLIND'),
 'diagnostic_assessment':('SUCCESSOR_DEPENDENCE_DIAGNOSTIC_ASSESSMENT','VERIFIED_DEVELOPMENT_LIMITATIONS_RECORDED'),
}
DRIVER_FAMILIES={'PERSISTENT_MARKET_AND_SECTOR_REGIMES','SHARED_SECURITY_AND_ISSUER',
 'REUSED_EVENTS_AND_SOURCE_RECORDS','OVERLAPPING_FEATURE_INFORMATION',
 'CAUSAL_ALERT_INCIDENCE_AND_SELECTION','CALENDAR_AND_PUBLICATION_CLOCK'}
DIAGNOSTIC_CHECKS={'DATE_LAGGED_CLUSTER_DEPENDENCE','MULTIDAY_BLOCK_SENSITIVITY',
 'SECURITY_AND_EVENT_OVERLAP','REGIME_AND_CALENDAR_STRATIFICATION',
 'ALERT_INCIDENCE_CONDITIONING_SENSITIVITY','INFORMATION_AVAILABILITY_AND_SOURCE_REUSE'}
PROHIBITED_BASES={'AUTOCORRELATION_NONREJECTION_ONLY','BOOTSTRAP_REPLICATION_COUNT',
 'DATES_ARE_DIFFERENT','TRADES_TREATED_AS_INDEPENDENT','PROSPECTIVE_STATUS_ALONE',
 'NO_SIGNIFICANT_DEPENDENCE_FOUND','FINITE_POPULATION_LABEL_AUDIT_SUBSTITUTION'}


class Closed(ValueError):pass


def need(test,reason):
    if not test:raise Closed(reason)


def utc(value):
    need(isinstance(value,str),'DEPENDENCE_ACTUAL_REGISTRATION_TIME_REQUIRED')
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    need(stamp.tzinfo is not None,'DEPENDENCE_REGISTRATION_TIMEZONE_REQUIRED')
    return stamp


def resolve(row,ref,kind,status):
    need(isinstance(row,dict) and isinstance(ref,dict),'DEPENDENCE_NATIVE_RECORD_REQUIRED')
    need(row.get('kind')==ref.get('kind')==kind and row.get('status')==ref.get('status')==status,
         'DEPENDENCE_REGISTERED_KIND_AND_STATUS_REQUIRED')
    need(isinstance(ref.get('artifact_key'),str) and bool(ref['artifact_key'])
         and row.get('artifact_key')==ref['artifact_key'],'DEPENDENCE_EXACT_RECORD_KEY_REQUIRED')
    raw=row.get('evidence_text');h=ref.get('implementation_sha256')
    need(isinstance(raw,str) and 0<len(raw.encode())<=1024*1024 and isinstance(h,str)
         and HASH.fullmatch(h) and row.get('implementation_sha256')==h
         and hashlib.sha256(raw.encode()).hexdigest()==h,'DEPENDENCE_NATIVE_READBACK_HASH_REQUIRED')
    data=json.loads(raw);need(isinstance(data,dict),'DEPENDENCE_OBJECT_EVIDENCE_REQUIRED')
    if 'evidence'in row:need(data==row['evidence'],'DEPENDENCE_READBACK_CONTENT_MISMATCH')
    return data,utc(row.get('created_at'))


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),
        ensure_ascii=False,allow_nan=False).encode()).hexdigest()


def adjudicate(argument_row,argument_ref,review_row,review_ref,supporting_rows,
               policy_row,policy_ref,*,scope,source_lineage_sha256,first_reserved_outcome_at):
    """Read actual immutable records; a boolean-only independence claim fails.

    This method preserves the registered V2 theorem exactly. A martingale,
    mixing, alternate clustering or future-population theorem needs a separate
    registered statistical design and cannot be approved through this gate.
    """
    reasons=[]
    try:
        need(isinstance(scope,dict) and set(scope)==set(SCOPE_KEYS)
             and all(isinstance(h,str) and HASH.fullmatch(h) for h in scope.values())
             and scope['method_contract_sha256']==METHOD
             and isinstance(source_lineage_sha256,str) and HASH.fullmatch(source_lineage_sha256),
             'DEPENDENCE_SCOPE_AND_POLICY_PIN_REQUIRED')
        scope_sha256=digest(scope)
        first=utc(first_reserved_outcome_at)
        policy,policy_registered=resolve(policy_row,policy_ref,'SUCCESSOR_DEPENDENCE_REVIEW_POLICY',
            'REGISTERED_OUTCOME_BLIND')
        policy_sha256=policy_ref['implementation_sha256']
        need(policy.get('gate_version')==VERSION and policy.get('method_contract_sha256')==METHOD
             and policy.get('gate_source_sha256')==hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
             and policy.get('market_independence_currently_established') is False
             and policy.get('diagnostic_nonrejection_grants_independence') is False
             and policy.get('protected_outcomes_accessed') is False,
             'DEPENDENCE_EXACT_REGISTERED_REVIEW_POLICY_REQUIRED')
        argument,registered=resolve(argument_row,argument_ref,'SUCCESSOR_DEPENDENCE_ARGUMENT',
            'REGISTERED_OUTCOME_BLIND_ASSUMPTION_JUSTIFICATION')
        review,reviewed=resolve(review_row,review_ref,'SUCCESSOR_DEPENDENCE_REVIEW',
            'VERIFIED_CONDITIONAL_DATE_INDEPENDENCE')
        need(policy_registered<=registered<=reviewed<first,
             'DEPENDENCE_PREREGISTRATION_BEFORE_RESERVED_OUTCOMES_REQUIRED')
        for item in (argument,review):
            need(item.get('scope_sha256')==scope_sha256 and item.get('method_contract_sha256')==METHOD
                 and item.get('dependence_review_policy_sha256')==policy_sha256
                 and item.get('protected_outcomes_accessed') is False,'DEPENDENCE_EXACT_SCOPE_METHOD_POLICY_REQUIRED')
        need(argument.get('estimand')==ESTIMAND and argument.get('weights')=='n_d/sum_d(n_d)'
             and argument.get('within_date_dependence')=='UNRESTRICTED'
             and argument.get('statistical_unit')=='COMPLETE_OFFICIAL_SESSION_DATE_CLUSTER'
             and argument.get('conditioning_information')=='ACTUAL_COMPLETE_FIXED_CANDIDATE_ALERT_INCIDENCE'
             and argument.get('assumption')==ASSUMPTION,'DEPENDENCE_ORIGINAL_SIGNAL_ESTIMAND_AND_CONDITIONING_REQUIRED')
        sections=argument.get('sections')
        need(isinstance(sections,dict) and set(sections)==set(REQUIRED_SECTIONS)
             and all(isinstance(v,str) and v.strip() for v in sections.values()),
             'DEPENDENCE_SUBSTANTIVE_STRUCTURAL_AND_MATHEMATICAL_ARGUMENT_REQUIRED')
        bases=argument.get('justification_bases')
        need(isinstance(bases,list) and bases and all(isinstance(v,str) for v in bases)
             and not(PROHIBITED_BASES & set(bases)),'DEPENDENCE_PROHIBITED_SUBSTITUTE_FOR_JUSTIFICATION')
        need(argument.get('incidence_conditioning_can_induce_dependence_addressed') is True
             and argument.get('future_transportability_is_additional_assumption') is True
             and argument.get('finite_diagnostics_prove_independence') is False
             and isinstance(argument.get('remaining_model_assumptions'),list)
             and argument['remaining_model_assumptions']
             and all(isinstance(v,str) and v.strip() for v in argument['remaining_model_assumptions'])
             and argument.get('unresolved_material_objections')==[],
             'DEPENDENCE_UNRESOLVED_STRUCTURAL_OR_INCIDENCE_CONDITIONING_PATH')
        refs=argument.get('supporting_references')
        need(isinstance(refs,dict) and set(refs)==set(SUPPORT_ROLES)
             and isinstance(supporting_rows,dict) and set(supporting_rows)==set(SUPPORT_ROLES),
             'DEPENDENCE_ACTUAL_SUPPORTING_REGISTRY_REQUIRED')
        actual={}
        timestamps={}
        for role,(kind,status) in SUPPORT_ROLES.items():
            data,created=resolve(supporting_rows[role],refs[role],kind,status)
            need(data.get('scope_sha256')==scope_sha256 and data.get('protected_outcomes_accessed') is False
                 and created<=registered,'DEPENDENCE_SUPPORT_SCOPE_OR_PREREGISTRATION_REQUIRED')
            actual[role]=data
            timestamps[role]=created
        structural=actual['structural_analysis']
        need(structural.get('actual_source_and_rule_lineage_inspected') is True
             and all(isinstance(structural.get(k),str) and HASH.fullmatch(structural[k]) for k in
               ('candidate_family_sha256','source_lineage_sha256','population_manifest_sha256','cross_date_dependency_graph_sha256'))
             and structural.get('candidate_family_sha256')==scope['candidate_family_sha256']
             and structural.get('population_manifest_sha256')==scope['population_manifest_sha256']
             and structural.get('source_lineage_sha256')==source_lineage_sha256
             and structural.get('omitted_known_shared_drivers')==[],
             'DEPENDENCE_ACTUAL_STRUCTURAL_LINEAGE_REQUIRED')
        drivers=structural.get('driver_assessments')
        need(isinstance(drivers,dict) and set(drivers)==DRIVER_FAMILIES
             and all(isinstance(v,dict) and isinstance(v.get('analysis'),str) and v['analysis'].strip()
              and v.get('incidence_conditioning_addressed') is True
              and v.get('disposition') in ('FACTORISATION_PROVED_UNDER_STATED_MODEL',
                  'EXPLICIT_UNTESTABLE_MODEL_ASSUMPTION_REVIEWED') for v in drivers.values()),
             'DEPENDENCE_SHARED_DRIVERS_REQUIRE_SUBSTANTIVE_DISPOSITIONS')
        math=actual['mathematical_analysis']
        need(math.get('assumption')==ASSUMPTION and math.get('estimand')==ESTIMAND
             and isinstance(math.get('factorization_statement'),str) and math['factorization_statement'].strip()
             and math.get('conditional_incidence_weighting_proved') is True
             and math.get('unconditional_independence_substituted_for_conditional') is False,
             'DEPENDENCE_THEOREM_CONDITIONING_PROOF_REQUIRED')
        diagnostic=actual['diagnostic_protocol']
        need(diagnostic.get('evidence_class')=='DEVELOPMENT_ONLY'
             and diagnostic.get('registered_before_diagnostic_analysis') is True
             and diagnostic.get('historical_development_exposure_disclosed') is True
             and diagnostic.get('diagnostic_results_can_select_method_or_calendar') is False
             and diagnostic.get('nonrejection_can_grant_independence') is False
             and isinstance(diagnostic.get('prespecified_checks'),dict)
             and set(diagnostic['prespecified_checks'])==DIAGNOSTIC_CHECKS
             and all(isinstance(v,str) and v.strip() for v in diagnostic['prespecified_checks'].values())
             and diagnostic.get('adverse_result_action')=='BLOCK_AND_PRESERVE_OR_REGISTER_SEPARATE_OUTCOME_BLIND_SUCCESSOR',
             'DEPENDENCE_PREREGISTERED_DIAGNOSTIC_SCOPE_REQUIRED')
        assessment=actual['diagnostic_assessment']
        checks=assessment.get('check_results')
        need(timestamps['diagnostic_protocol']<timestamps['diagnostic_assessment']
             and assessment.get('protocol_artifact_sha256')==refs['diagnostic_protocol']['implementation_sha256']
             and all(isinstance(assessment.get(k),str) and HASH.fullmatch(assessment[k]) for k in
                 ('analysis_code_sha256','development_input_manifest_sha256','actual_execution_receipt_sha256'))
             and assessment.get('evidence_class')=='DEVELOPMENT_ONLY'
             and assessment.get('independence_inferred_from_nonrejection') is False
             and assessment.get('reserved_outcomes_read') is False
             and isinstance(checks,dict) and set(checks)==DIAGNOSTIC_CHECKS
             and all(isinstance(v,dict) and v.get('state') in ('OBSERVED','INSUFFICIENT_DIAGNOSTIC_INFORMATION')
                 and isinstance(v.get('evidence_sha256'),str) and HASH.fullmatch(v['evidence_sha256'])
                 and isinstance(v.get('limitations'),str) and v['limitations'].strip()
                 for v in checks.values()),'DEPENDENCE_ACTUAL_DEVELOPMENT_DIAGNOSTICS_AND_LIMITATIONS_REQUIRED')
        need(review.get('argument_artifact_sha256')==argument_ref['implementation_sha256']
             and review.get('reviewed_supporting_references')==refs
             and isinstance(argument.get('author_id'),str) and argument['author_id'].strip()
             and isinstance(review.get('independent_reviewer_id'),str) and review['independent_reviewer_id'].strip()
             and review['independent_reviewer_id'].strip()!=argument['author_id'].strip()
             and review.get('author_and_reviewer_distinct') is True,
             'DEPENDENCE_ACTUAL_INDEPENDENT_REVIEW_BINDING_REQUIRED')
        decisions=review.get('section_decisions')
        need(isinstance(decisions,dict) and set(decisions)==set(REQUIRED_SECTIONS)
             and all(value=='SUPPORTED_AS_EXPLICIT_CONDITIONAL_MODEL_ASSUMPTION' for value in decisions.values())
             and review.get('unresolved_objections')==[]
             and review.get('conditional_date_independence_supported') is True
             and review.get('reviewed_model_assumptions')==argument['remaining_model_assumptions']
             and review.get('nonrejection_only_justification') is False
             and review.get('independence_empirically_proved') is False,
             'DEPENDENCE_UNRESOLVED_INDEPENDENT_SCIENTIFIC_REVIEW')
    except (Closed,ValueError,TypeError,KeyError,AttributeError) as error:
        reasons.append(str(error) if isinstance(error,Closed) else 'INVALID_DEPENDENCE_REVIEW_DOCUMENT')
    return dict(version=VERSION,state='BLOCKED_BY_IDENTIFIED_DEPENDENCY' if reasons else 'VERIFIED',
        dependencies=reasons,verified_scope='CONDITIONAL_ASSUMPTION_REVIEW_PROVENANCE_AND_CONTENT_ONLY',
        independence_empirically_proved=False,future_stationarity_proved=False,
        claims_certified=False,grants_evidence_access=False,protected_outcomes_accessed=False,
        research_objective_achieved=False)
