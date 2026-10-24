-- Practical candidate route. No legacy approval is reinterpreted; no serving promotion.
CREATE TABLE population_top3000_202608_202609_v1.ref01_practical_snapshot_v1(
 snapshot_sha256 text PRIMARY KEY CHECK(snapshot_sha256~'^[0-9a-f]{64}$'),
 policy_sha256 text NOT NULL,source_sha256 text NOT NULL,company_sha256 text NOT NULL,instrument_sha256 text NOT NULL,
 content jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 membership_accepted boolean NOT NULL DEFAULT false CHECK(NOT membership_accepted));
CREATE TABLE population_top3000_202608_202609_v1.ref01_practical_population_v1(
 snapshot_sha256 text NOT NULL REFERENCES population_top3000_202608_202609_v1.ref01_practical_snapshot_v1,
 family_id text NOT NULL,policy_sha256 text NOT NULL,company_sha256 text NOT NULL,company_count integer NOT NULL,
 content jsonb NOT NULL,record_sha256 text NOT NULL,acceptance_sha256 text NOT NULL DEFAULT '',PRIMARY KEY(snapshot_sha256,family_id,acceptance_sha256));
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_practical_snapshot_v1,population_top3000_202608_202609_v1.ref01_practical_population_v1 FROM PUBLIC,anon,authenticated;
ALTER TABLE population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ref01_practical_population_v1 ENABLE ROW LEVEL SECURITY;
CREATE FUNCTION population_top3000_202608_202609_v1.ref01_practical_material_v1() RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog AS $f$
WITH src AS MATERIALIZED (
 SELECT s.*,c.decision,c.record_sha256 case_sha256,c.content->'unresolved_codes' unresolved,
 CASE WHEN s.eligibility_state IS DISTINCT FROM 'SOURCE_SUPPORTED_COMMON' OR s.projection->>'native_type' IS NULL OR s.projection->>'native_type' NOT IN('CS','ADRC') THEN 'NON_COMMON_OR_CLASS_AMBIGUOUS'
 WHEN s.company_identity_state IS DISTINCT FROM 'SOURCE_SUPPORTED_ISSUER' OR s.security_identity_state IS DISTINCT FROM 'DATED_NATIVE_IDENTIFIERS'
 OR s.case_key IS DISTINCT FROM 'CIK:'||(s.projection->>'native_cik') OR s.projection->>'native_cik' !~ '^[0-9]{10}$' THEN 'IDENTITY_AMBIGUITY'
 WHEN s.projection->>'rank_date' IS DISTINCT FROM '2026-08-03' OR s.projection->'historical_active' IS DISTINCT FROM 'true'::jsonb
 OR s.projection->>'primary_exchange' IS NULL OR s.projection->>'primary_exchange' NOT IN('XNYS','XNAS','XASE','ARCX','BATS','IEXG') THEN 'HISTORICAL_LISTING_AMBIGUITY'
 WHEN c.decision IS NULL OR c.decision NOT IN('ELIGIBLE','REVIEW_REQUIRED') OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(c.content->'unresolved_codes') u WHERE u NOT IN('ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION','RANK_CLOSE_UNAVAILABLE_OR_AMBIGUOUS','MISSING_DATED_COMPANY_CAP','MISSING_COMPANY_EQUIVALENT_SHARES')) THEN 'ADJUDICATED_IDENTITY_OR_CLASS_HOLD'
 WHEN s.projection->>'vendor_company_cap_usd' IS NULL OR (s.projection->>'vendor_company_cap_usd')::numeric<=0 THEN 'SELECTION_PROXY_UNAVAILABLE'
 ELSE NULL END exclusion
 FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s
 LEFT JOIN population_top3000_202608_202609_v1.ref01_adjudication_head_v3 h ON h.run_key='ref01-company-adjudication-v3-20261023' AND h.case_key=s.case_key
 LEFT JOIN population_top3000_202608_202609_v1.ref01_adjudication_case_v3 c ON c.run_key=h.run_key AND c.case_key=h.case_key AND c.revision_key=h.revision_key
 WHERE s.run_key='ref01-company-reconciliation-v2-20261023'
), eligible AS MATERIALIZED(SELECT * FROM src WHERE exclusion IS NULL),
 companies AS MATERIALIZED(SELECT case_key company_id,max((projection->>'vendor_company_cap_usd')::numeric) selection_proxy_usd,
 jsonb_agg(jsonb_build_object('ticker',ticker,'security_projection_sha256',output_sha256,'case_sha256',case_sha256) ORDER BY ticker COLLATE "C") sources FROM eligible GROUP BY case_key),
 ranked AS MATERIALIZED(SELECT *,row_number() OVER(ORDER BY selection_proxy_usd DESC,company_id COLLATE "C") selection_rank FROM companies),
 chosen AS MATERIALIZED(SELECT * FROM ranked WHERE selection_rank<=3000),
 instruments AS MATERIALIZED(SELECT e.case_key company_id,e.ticker,e.projection->>'composite_figi' composite_figi,e.projection->>'share_class_figi' share_class_figi,
 e.projection->>'native_type' security_type,e.projection->>'primary_exchange' exchange,'2026-08-03'::text observed_date,
 e.projection->>'list_date_source_text' provider_list_date_text,e.output_sha256 projection_sha256,e.projection->>'census_record_sha256' census_record_sha256,e.projection->>'overview_record_sha256' overview_record_sha256,
 false full_period_interval_proven FROM eligible e JOIN chosen c ON c.company_id=e.case_key),
 material AS(SELECT jsonb_build_object('policy_sha256','3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb',
 'source_sha256',(SELECT encode(sha256(convert_to(string_agg(ticker||':'||output_sha256||':'||coalesce(case_sha256,'MISSING'),E'\n' ORDER BY ticker COLLATE "C"),'UTF8')),'hex') FROM src),
 'selection_semantics',jsonb_build_object('anchor_date','2026-08-03','proxy','MAX_AVAILABLE_VENDOR_COMPANY_MARKET_CAP_OVER_ELIGIBLE_LINES_PER_CANONICAL_CIK','tie_break','CANONICAL_COMPANY_ID_C_COLLATION_ASC','source_run','ref01-company-reconciliation-v2-20261023','not_certified_exact_largest_3000',true,'proxy_is_verified_financial_value',false,'original_historical_receipt_proven',false,'publication_time_verified',false),
 'eligible_company_count',(SELECT count(*) FROM companies),'selected_company_count',(SELECT count(*) FROM chosen),'shortfall',greatest(0,3000-(SELECT count(*) FROM chosen)),
 'companies',(SELECT jsonb_agg(to_jsonb(c) ORDER BY selection_rank) FROM chosen c),
 'instruments',(SELECT jsonb_agg(to_jsonb(i) ORDER BY company_id COLLATE "C",ticker COLLATE "C") FROM instruments i),
 'exclusions',(SELECT jsonb_agg(jsonb_build_object('ticker',ticker,'case_key',case_key,'reason',exclusion,'projection_sha256',output_sha256,'case_sha256',case_sha256) ORDER BY ticker COLLATE "C") FROM src WHERE exclusion IS NOT NULL),
 'membership_status','CANDIDATE_PENDING_INDEPENDENT_REVIEW_AND_ROOT_ACCEPTANCE','full_period_instrument_completeness',false,'financial_facts_accepted',false,'families_accepted',false) v)
SELECT v||jsonb_build_object('company_sha256',encode(sha256(convert_to((v->'companies')::text,'UTF8')),'hex'),'instrument_sha256',encode(sha256(convert_to((v->'instruments')::text,'UTF8')),'hex')) FROM material
$f$;
CREATE FUNCTION population_top3000_202608_202609_v1.ref01_practical_guard_v1() RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence<>3 OR a.stop_generation<>1 OR NOT p.enabled
 OR p.budget_evidence#>>'{cohort_selection_policy,policy_sha256}' IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
 OR p.budget_evidence#>>'{cohort_selection_policy,authority_event_id}' IS DISTINCT FROM '23086'
 THEN RAISE EXCEPTION 'PRACTICAL_CURRENT_POLICY_OWNER_REQUIRED';END IF;
END $f$;
CREATE FUNCTION population_top3000_202608_202609_v1.ref01_practical_worker_v1(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;m jsonb;h text;n integer:=0;z record;rowdata jsonb;nextid uuid;acc text:='';
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 PERFORM population_top3000_202608_202609_v1.ref01_practical_guard_v1();
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.family_id IS DISTINCT FROM 'REF01' OR t.input_versions->>'policy_sha256' IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
 OR t.payload->>'snapshot_sha256' IS DISTINCT FROM t.input_versions->>'cohort_sha256' THEN RAISE EXCEPTION 'PRACTICAL_TASK_POLICY_HASH_REQUIRED';END IF;
 IF t.stage='VALIDATE' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','PRACTICAL_COHORT_INDEPENDENT_MEMBERSHIP_ROOT_ACCEPTANCE_AND_INTERVAL_RECONCILIATION_REQUIRED','resumption_condition',jsonb_build_object('policy_sha256',t.input_versions->>'policy_sha256','cohort_sha256',t.input_versions->>'cohort_sha256','selected_identity_review_required',true,'full_period_instrument_review_required',true,'exact_global_cutoff_required',false,'root_acceptance_required',NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_practical_membership_v1 WHERE snapshot_sha256=t.input_versions->>'cohort_sha256'),'membership_and_interval_are_separate',true));END IF;
 IF t.stage<>'DERIVE' THEN RAISE EXCEPTION 'PRACTICAL_STAGE_INVALID';END IF;
 IF t.payload->>'kind'='PRACTICAL_COHORT_CANDIDATE_V1' THEN
 m:=population_top3000_202608_202609_v1.ref01_practical_material_v1();h:=encode(sha256(convert_to(m::text,'UTF8')),'hex');
 IF h IS DISTINCT FROM t.input_versions->>'cohort_sha256' OR m->>'source_sha256' IS DISTINCT FROM t.input_versions->>'source_sha256' THEN RAISE EXCEPTION 'PRACTICAL_SOURCE_OR_COHORT_CHANGED';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ref01_practical_snapshot_v1(snapshot_sha256,policy_sha256,source_sha256,company_sha256,instrument_sha256,content)
 VALUES(h,m->>'policy_sha256',m->>'source_sha256',m->>'company_sha256',m->>'instrument_sha256',m) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS n=ROW_COUNT;
 IF (SELECT content FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 WHERE snapshot_sha256=h) IS DISTINCT FROM m THEN RAISE EXCEPTION 'PRACTICAL_IMMUTABLE_REPLAY_CONFLICT';END IF;
 nextid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','DERIVE','practical-population-v1:'||h,jsonb_build_object('kind','PRACTICAL_POPULATION_CONSUME_V1','snapshot_sha256',h),t.input_versions,tid);
 ELSE
 IF t.payload->>'kind' IS NULL OR t.payload->>'kind' NOT IN('PRACTICAL_POPULATION_CONSUME_V1','PRACTICAL_ACCEPTED_POPULATION_V1') THEN RAISE EXCEPTION 'PRACTICAL_KIND_INVALID';END IF;
 h:=t.input_versions->>'cohort_sha256';
 SELECT content INTO STRICT m FROM population_top3000_202608_202609_v1.ref01_practical_snapshot_v1 WHERE snapshot_sha256=h;
 IF encode(sha256(convert_to(m::text,'UTF8')),'hex') IS DISTINCT FROM h OR m->>'source_sha256' IS DISTINCT FROM t.input_versions->>'source_sha256' THEN RAISE EXCEPTION 'PRACTICAL_FROZEN_CONTENT_CHANGED';END IF;
 IF t.payload->>'kind'='PRACTICAL_ACCEPTED_POPULATION_V1' THEN
 SELECT acceptance_sha256 INTO STRICT acc FROM population_top3000_202608_202609_v1.ref01_practical_membership_v1 WHERE snapshot_sha256=h AND acceptance_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex');
 IF acc IS DISTINCT FROM t.input_versions->>'membership_acceptance_sha256' THEN RAISE EXCEPTION 'PRACTICAL_ACCEPTANCE_INPUT_CHANGED';END IF;
 END IF;
 FOR z IN SELECT family_id FROM population_top3000_202608_202609_v1.catalog_family_contract ORDER BY family_id LOOP
 rowdata:=jsonb_build_object('policy_sha256',m->>'policy_sha256','cohort_sha256',h,'company_sha256',m->>'company_sha256','instrument_sha256',m->>'instrument_sha256','selected_company_ids',(SELECT jsonb_agg(v->>'company_id' ORDER BY v->>'company_id') FROM jsonb_array_elements(m->'companies') v),'family_id',z.family_id,'state',CASE WHEN acc='' THEN 'FROZEN_CANDIDATE_CONSUMED_PENDING_MEMBERSHIP_ACCEPTANCE_AND_FAMILY_APPLICABILITY' ELSE 'ACCEPTED_MEMBERSHIP_CONSUMED_FAMILY_APPLICABILITY_PENDING' END,'membership_accepted',acc<>'','membership_acceptance_sha256',acc,'expected_observation_count',NULL,'missing_is_not_absent',true,'full_period_instruments_complete',false,'family_accepted',false,'D06_preservation_only',z.family_id='D06');
 INSERT INTO population_top3000_202608_202609_v1.ref01_practical_population_v1 VALUES(h,z.family_id,m->>'policy_sha256',m->>'company_sha256',(m->>'selected_company_count')::integer,rowdata,encode(sha256(convert_to(rowdata::text,'UTF8')),'hex'),acc) ON CONFLICT DO NOTHING;
 IF FOUND THEN n:=n+1;END IF;
 IF (SELECT content FROM population_top3000_202608_202609_v1.ref01_practical_population_v1 WHERE snapshot_sha256=h AND family_id=z.family_id AND acceptance_sha256=acc) IS DISTINCT FROM rowdata THEN RAISE EXCEPTION 'PRACTICAL_POPULATION_REPLAY_CONFLICT';END IF;
 END LOOP;
 END IF;
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key',(t.payload->>'kind')||':'||h||':'||acc,'output_sha256',h,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(h),'input_versions',t.input_versions,'selected_company_count',m->>'selected_company_count','company_sha256',m->>'company_sha256','instrument_sha256',m->>'instrument_sha256','population_successor',nextid,'membership_accepted',acc<>'','financial_facts_accepted',false,'full_period_instruments_complete',false));
END $f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_practical_material_v1(),population_top3000_202608_202609_v1.ref01_practical_guard_v1(),population_top3000_202608_202609_v1.ref01_practical_worker_v1(uuid,text) FROM PUBLIC,anon,authenticated;
