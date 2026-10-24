-- Assignment23928: bounded retained F05 candidate processor. No family acceptance or source downloads.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.f05_retained_candidate_v1(
 cohort_sha256 text NOT NULL, acceptance_sha256 text NOT NULL, fact_key text NOT NULL,
 source_row_sha256 text NOT NULL, record jsonb NOT NULL, record_sha256 text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(cohort_sha256,acceptance_sha256,fact_key));
ALTER TABLE population_top3000_202608_202609_v1.f05_retained_candidate_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.f05_retained_candidate_v1 FROM PUBLIC,anon,authenticated;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.f05_retained_source_set_v1() RETURNS jsonb LANGUAGE sql STABLE SET search_path TO 'pg_catalog' SET "TimeZone" TO 'UTC' AS $fn$
 SELECT jsonb_agg(to_jsonb(f) ORDER BY fact_key) FROM population_top3000_202608_202609_v1.sec_financial_fact_v1 f
 WHERE metric IN('eps_basic','eps_diluted','revenue') AND accepted_at>='2026-08-01T00:00Z' AND accepted_at<'2026-10-01T00:00Z'
 AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 s,jsonb_array_elements(s.content->'companies')c
 WHERE s.snapshot_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' AND c->>'company_id'='CIK:'||f.company_cik)
$fn$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.f05_retained_candidate_worker_v1(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET "TimeZone" TO 'UTC' AS $fn$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;src jsonb;f jsonb;r jsonb;h text;total integer;next_assignment bigint;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id IS DISTINCT FROM 'F05' OR t.stage NOT IN('DERIVE','VALIDATE') OR t.payload->>'kind' IS DISTINCT FROM 'F05_RETAINED_REPORTED_CANDIDATES_V1'
 OR t.partition_key IS DISTINCT FROM 'f05-retained-reported-candidates-v1'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR t.input_versions->>'F05_contract_sha256' IS DISTINCT FROM 'c5284ff9a46765d008337e4b9b8d84a44342da93465e4c957c642475f15b2a7a'
 OR t.input_versions->>'policy_sha256' IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
 OR t.input_versions->>'cohort_sha256' IS DISTINCT FROM '2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'
 OR t.input_versions->>'membership_acceptance_sha256' IS DISTINCT FROM '20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9'
 OR t.input_versions->>'source_set_sha256' IS DISTINCT FROM '7adaa1eadfd956686ae2a02fa346d6a3ad5a27c51a5180a6a7894bd46de973f9'
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_practical_membership_v1 m WHERE m.snapshot_sha256=t.input_versions->>'cohort_sha256' AND m.acceptance_sha256=t.input_versions->>'membership_acceptance_sha256')
 OR (SELECT budget_evidence#>>'{cohort_selection_policy,policy_sha256}' FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton) IS DISTINCT FROM t.input_versions->>'policy_sha256'
 THEN RAISE EXCEPTION 'F05_POLICY_COHORT_SOURCE_SCOPE_CHANGED';END IF;
 src:=population_top3000_202608_202609_v1.f05_retained_source_set_v1();
 IF jsonb_array_length(src) IS DISTINCT FROM 12 OR encode(sha256(convert_to(src::text,'UTF8')),'hex') IS DISTINCT FROM t.input_versions->>'source_set_sha256' THEN RAISE EXCEPTION 'F05_EXACT_RETAINED_SOURCE_CHANGED';END IF;
 SELECT event_id INTO next_assignment FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5' AND details->>'assignment_id'='f05-exact-retained-earnings-event-consensus-verification-v1-20261024' AND details->>'owner_id'='top3000-completion-20261024-root-5db48b32d103' AND details->>'owner_fence'='3' AND details->>'stop_generation'='1' AND details->>'authority_event_id'='9528' AND details->>'assigned_executor'='automation:6adc2c96db348191bca7cf4eacfdac32' AND details->>'scope_sha256'=t.input_versions->>'scope_sha256' AND details#>>'{input_versions,policy_sha256}'=t.input_versions->>'policy_sha256' AND details#>>'{input_versions,membership_acceptance_sha256}'=t.input_versions->>'membership_acceptance_sha256' AND details#>>'{input_versions,cohort_sha256}'=t.input_versions->>'cohort_sha256' AND details#>>'{input_versions,source_set_sha256}'=t.input_versions->>'source_set_sha256' AND (details->>'not_after')::timestamptz>clock_timestamp() ORDER BY event_id DESC LIMIT 1;
 IF next_assignment IS NULL THEN RAISE EXCEPTION 'F05_CONCRETE_VERIFICATION_SUCCESSOR_REQUIRED';END IF;
 IF t.stage='VALIDATE' THEN
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','F05_EARNINGS_EVENT_BASIS_CONSENSUS_VERIFICATION_REQUIRED','resumption_condition',jsonb_build_object('engineering_assignment_event_id',next_assignment,'cohort_sha256',t.input_versions->>'cohort_sha256','source_set_sha256',t.input_versions->>'source_set_sha256','candidate_count',12,'requirements',jsonb_build_array('Independent source body numerical/context check','Earnings announcement time separate from filing acceptance','Quarter versus year-to-date and comparative period reconciliation','Genuine pre-announcement matched consensus vintage and basis','Guidance source evidence; unsupported guidance is not N/A'),'family_accepted',false,'source_accuracy_accepted',false,'automatic_retry',false));END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 FOR f IN SELECT value FROM jsonb_array_elements(src) LOOP
 r:=jsonb_build_object('company_id','CIK:'||(f->>'company_cik'),'fact_key',f->>'fact_key','source_id',f->>'source_id','source_kind',f->>'source_kind','accession_number',f->>'accession_number','metric',f->>'metric','reported_candidate_value',f->'value','period_start',f->'period_start','period_end',f->'period_end','unit',f->'unit','concept',f->'concept','context_id',f->'context_id','source_acceptance_representation',f->'accepted_at','source_classification',f->'classification','source_extraction_state',f->'extraction_state','source_payload',f->'payload','source_lineage',f->'lineage','earnings_event_at',NULL,'actual_earnings_value',NULL,'reported_guidance',NULL,'preannouncement_consensus',NULL,'consensus_vintage',NULL,'surprise',NULL,'field_state',jsonb_build_object('reported_candidate_value','CONDITIONAL_RETAINED_SOURCE_VALUE','earnings_event_at','UNVERIFIED','period_basis','RETAINED_NOT_REINTERPRETED_AS_QUARTER','guidance','UNASSESSED','consensus','MISSING_REQUIRED_EVIDENCE','surprise','UNAVAILABLE_WITHOUT_MATCHED_ACTUAL_AND_CONSENSUS'),'membership_acceptance_sha256',t.input_versions->>'membership_acceptance_sha256','policy_sha256',t.input_versions->>'policy_sha256','cohort_sha256',t.input_versions->>'cohort_sha256','historical_receipt_proven',false,'family_accepted',false);
 h:=encode(sha256(convert_to(r::text,'UTF8')),'hex');
 INSERT INTO population_top3000_202608_202609_v1.f05_retained_candidate_v1(cohort_sha256,acceptance_sha256,fact_key,source_row_sha256,record,record_sha256) VALUES(t.input_versions->>'cohort_sha256',t.input_versions->>'membership_acceptance_sha256',f->>'fact_key',encode(sha256(convert_to(f::text,'UTF8')),'hex'),r,h) ON CONFLICT DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.f05_retained_candidate_v1 c WHERE c.cohort_sha256=t.input_versions->>'cohort_sha256' AND c.acceptance_sha256=t.input_versions->>'membership_acceptance_sha256' AND c.fact_key=f->>'fact_key' AND c.record=r AND c.record_sha256=h AND c.source_row_sha256=encode(sha256(convert_to(f::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'F05_CANDIDATE_REPLAY_CONFLICT';END IF;
 END LOOP;
 SELECT count(*),encode(sha256(convert_to(jsonb_agg(jsonb_build_array(c.fact_key,c.record_sha256) ORDER BY c.fact_key)::text,'UTF8')),'hex') INTO total,h FROM population_top3000_202608_202609_v1.f05_retained_candidate_v1 c WHERE c.cohort_sha256=t.input_versions->>'cohort_sha256' AND c.acceptance_sha256=t.input_versions->>'membership_acceptance_sha256';
 IF total<>12 THEN RAISE EXCEPTION 'F05_EXACT_OUTPUT_COUNT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','F05_RETAINED_CANDIDATES_V1:'||(t.input_versions->>'cohort_sha256')||':'||(t.input_versions->>'membership_acceptance_sha256')||':'||(t.input_versions->>'source_set_sha256'),'output_sha256',h,'input_versions',t.input_versions,'immutable_output_ids',(SELECT jsonb_agg(c.fact_key ORDER BY c.fact_key) FROM population_top3000_202608_202609_v1.f05_retained_candidate_v1 c WHERE c.cohort_sha256=t.input_versions->>'cohort_sha256' AND c.acceptance_sha256=t.input_versions->>'membership_acceptance_sha256'),'new_committed_units',12,'candidate_only',true,'verified_earnings_values',0,'family_accepted',false,'verification_assignment_event_id',next_assignment));
END;$fn$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.f05_retained_source_set_v1(),population_top3000_202608_202609_v1.f05_retained_candidate_worker_v1(uuid,text) FROM PUBLIC,anon,authenticated;
