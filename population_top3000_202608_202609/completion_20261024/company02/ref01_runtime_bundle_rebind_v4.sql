-- ROOT ONLY, after independent acceptance07 review of the authority patch + this rebind.
-- Retains original task versions, historical outputs and all 5,910 cases.
DO $rebind$
DECLARE old_run jsonb;old_adapters jsonb;new_bundle text;ev bigint;function_manifest jsonb;
 old_bundle constant text:='48060ba2851ae1ad70a51290cc25b41ff9a9124eaf70fc2ed361271a2a081f8a';
 expected_new constant text:='0f6db1deaa585076b00edde23e9c1c175b1e2f9188694d37fb1f1bb84115b680';
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-completion-20261024-root-5db48b32d103' AND owner_fence=3 AND stop_generation=1)
 THEN RAISE EXCEPTION 'REF01_REBIND_EXPECTED_CURRENT_OWNER_STOP_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l
  JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id)
  JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id) WHERE t.family_id='REF01' AND l.released_at IS NULL)
 THEN RAISE EXCEPTION 'REF01_REBIND_ACTIVE_LANE_FORBIDDEN';END IF;
 new_bundle:=population_top3000_202608_202609_v1.ref01_adjudication_bundle_v3();
 IF new_bundle IS DISTINCT FROM expected_new THEN RAISE EXCEPTION 'REF01_REBIND_UNREVIEWED_CODE_CHANGED';END IF;
 SELECT to_jsonb(r) INTO STRICT old_run FROM population_top3000_202608_202609_v1.ref01_adjudication_run_v3 r
 WHERE r.run_key='ref01-company-adjudication-v3-20261023' FOR UPDATE;
 IF old_run#>>'{input_versions,adjudication_bundle_sha256}'=new_bundle
  AND old_run#>>'{input_versions,runtime_authority_rebind_event_id}' IS NOT NULL THEN RETURN;END IF;
 IF old_run#>>'{input_versions,adjudication_bundle_sha256}' IS DISTINCT FROM old_bundle OR (old_run->>'expected_cases')::integer IS DISTINCT FROM 5910
  OR old_run->>'source_stage_sha256' IS DISTINCT FROM 'acf1a0c4b34521301c14c678256ad1e81a762d35b300e05a100b694d16f76d79'
 THEN RAISE EXCEPTION 'REF01_REBIND_SOURCE_OR_DENOMINATOR_CHANGED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01'
  AND payload->>'run_key'=old_run->>'run_key' AND status NOT IN('COMPLETE','BLOCKED','CANCELLED'))
 THEN RAISE EXCEPTION 'REF01_REBIND_PENDING_TASK_VERSION_REQUIRES_EXPLICIT_RECOVERY';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id='REF01' AND producer_revision='REF01_ADJUDICATION_V3') IS DISTINCT FROM 4
 OR EXISTS(SELECT 1 FROM (VALUES
  ('DERIVE','population_top3000_202608_202609_v1.ref01_adjudication_route_derive_v3(uuid,text)','2fb4fe2a2e06400abfd8ef2fd4b0d1cee71c7e77ac88aa8f3c2231a8f7f0e097'),
  ('VALIDATE','population_top3000_202608_202609_v1.ref01_adjudication_route_validate_v3(uuid,text)','d9b97e6e5a2dc7f74593213cb32b546a2f0c44895b4d91c7f7a5bf6b4ae39663'),
  ('PUBLISH','population_top3000_202608_202609_v1.ref01_adjudication_route_publish_v3(uuid,text)','5ba8b10c0526f8fc079efe8328099545e9d9260fcfee443ab86632de8a2fe150'),
  ('DELIVER','population_top3000_202608_202609_v1.ref01_adjudication_route_deliver_v3(uuid,text)','4437ecb194f664c0e1642e5c49495c9e7e87d88adff72d4e189ed73dd53069b6')
 ) expected(stage,signature,sha)
 LEFT JOIN population_top3000_202608_202609_v1.pipeline_adapter a ON a.family_id='REF01' AND a.stage=expected.stage
 WHERE a.producer_revision IS DISTINCT FROM 'REF01_ADJUDICATION_V3' OR a.function_oid::oid IS DISTINCT FROM to_regprocedure(expected.signature)::oid
 OR a.function_sha256 IS DISTINCT FROM expected.sha
 OR encode(sha256(convert_to(pg_get_functiondef(a.function_oid::oid),'UTF8')),'hex') IS DISTINCT FROM expected.sha)
 THEN RAISE EXCEPTION 'REF01_REBIND_UNRELATED_ADAPTER_CHANGE_FORBIDDEN';END IF;
 SELECT jsonb_agg(to_jsonb(a) ORDER BY stage) INTO old_adapters FROM population_top3000_202608_202609_v1.pipeline_adapter a
 WHERE family_id='REF01' AND producer_revision='REF01_ADJUDICATION_V3';
 SELECT jsonb_object_agg(p.oid::regprocedure::text,encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex')) INTO function_manifest
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1'
 AND (p.proname LIKE 'ref01_adjudication_%_v3' OR p.proname LIKE 'ref01_cohort_%_v3') AND p.proname<>'ref01_adjudication_bundle_v3';
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 VALUES('REF01_RUNTIME_AUTHORITY_BUNDLE_REBOUND_20261024',jsonb_build_object('owner_id','top3000-completion-20261024-root-5db48b32d103',
  'owner_fence',3,'stop_generation',1,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd',
  'reviewed_by','top3000-completion-20261024-acceptance07','produced_by','top3000-completion-20261024-company02',
  'purpose','Apply independently reviewed current owner/reviewer authorization without replaying or rewriting historical data outputs',
  'old_bundle_sha256',old_bundle,'new_bundle_sha256',new_bundle,'prior_run',old_run,'prior_adapters',old_adapters,
  'live_function_sha256',function_manifest,'expected_cases',5910,'all_historical_task_input_versions_preserved',true,
  'all_prior_case_and_snapshot_content_preserved',true,'source_derivation_unchanged',true,'cohort_not_certified',true)) RETURNING event_id INTO ev;
 UPDATE population_top3000_202608_202609_v1.ref01_adjudication_run_v3
 SET input_versions=jsonb_set(input_versions,'{adjudication_bundle_sha256}',to_jsonb(new_bundle))||jsonb_build_object('runtime_authority_rebind_event_id',ev)
 WHERE run_key=old_run->>'run_key';
 UPDATE population_top3000_202608_202609_v1.pipeline_adapter a
 SET reviewed_test_evidence=a.reviewed_test_evidence||jsonb_build_object('adjudication_bundle_sha256',new_bundle,
  'runtime_authority_rebind',jsonb_build_object('event_id',ev,'old_bundle_sha256',old_bundle,'new_bundle_sha256',new_bundle,
   'reviewed_by','top3000-completion-20261024-acceptance07','owner_fence',3,'stop_generation',1,'prior_test_evidence_retained',true))
 WHERE a.family_id='REF01' AND a.producer_revision='REF01_ADJUDICATION_V3';
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
END $rebind$;
SELECT run_key,expected_cases,input_versions FROM population_top3000_202608_202609_v1.ref01_adjudication_run_v3
WHERE run_key='ref01-company-adjudication-v3-20261023';
