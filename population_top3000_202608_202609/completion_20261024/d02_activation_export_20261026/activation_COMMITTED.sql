BEGIN;SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='30s';SET LOCAL work_mem='32MB';SET LOCAL jit=off;DO $natural$ DECLARE v_until timestamptz:=clock_timestamp()+interval '8 seconds';v_locked boolean;BEGIN LOOP
BEGIN v_locked:=pg_try_advisory_xact_lock(2026083000261022);IF NOT v_locked THEN RAISE EXCEPTION 'D02_V3_BUSY_SENTINEL';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING'OR ended_at IS NULL)OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='RUNNING')THEN RAISE EXCEPTION 'D02_V3_BUSY_SENTINEL';END IF;EXIT;
EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM IS DISTINCT FROM 'D02_V3_BUSY_SENTINEL' THEN RAISE;END IF;END;
IF clock_timestamp()>=v_until THEN RAISE EXCEPTION 'D02_V3_BOUNDED_NATURAL_GAP_UNAVAILABLE';END IF;PERFORM pg_sleep(0.15);END LOOP;END $natural$;
DO $guard$
DECLARE ac population_top3000_202608_202609_v1.acquisition_control%rowtype; pc population_top3000_202608_202609_v1.pipeline_control%rowtype; a jsonb;c jsonb;
BEGIN
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING' OR ended_at IS NULL)
OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)
OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='RUNNING')
THEN RAISE EXCEPTION 'D02_POINT_NATURAL_QUIET_REQUIRED'; END IF;
SELECT * INTO STRICT ac FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
SELECT * INTO STRICT pc FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
SELECT details INTO STRICT a FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=83495 AND event='ENGINEERING_IMPLEMENTATION_ASSIGNMENT_V5';
SELECT details INTO STRICT c FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=83514 AND event='ENGINEERING_ASSIGNMENT_CLAIMED_V5';
IF (ac.owner_id='top3000-completion-20261024-root-5db48b32d103' AND ac.owner_fence=3 AND ac.stop_generation=1 AND ac.enabled AND NOT ac.stopped AND ac.hold_reason IS NULL
AND ac.admitted_until>clock_timestamp() AND pc.enabled AND pc.deadline>clock_timestamp()
AND ac.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
AND pc.expected_owner_id=ac.owner_id AND pc.expected_owner_fence=ac.owner_fence AND pc.expected_stop_generation=ac.stop_generation AND pc.scope_sha256=ac.scope_sha256
AND c->>'assignment_event_id'='83495' AND c->>'assignment_details_sha256'='170424fd8ea1d5e3fcce163d3e44731bc5c402bcf2ee1bb21d77bcdefb723dfd'
AND encode(sha256(convert_to(a::text,'UTF8')),'hex')='170424fd8ea1d5e3fcce163d3e44731bc5c402bcf2ee1bb21d77bcdefb723dfd' AND c->>'executor'='automation:6adc2c96db348191bca7cf4eacfdac32'
AND (c->>'expires_at')::timestamptz>clock_timestamp() AND (a->>'not_after')::timestamptz>clock_timestamp()
AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=81090 AND event='USER_POST_DEADLINE_CONTINUATION_AUTHORITY_V1' AND details->>'owner_id'=ac.owner_id AND details->>'scope_sha256'=ac.scope_sha256 AND details->>'stop_generation'='1')
AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND details->>'claim_event_id'='83514')) IS DISTINCT FROM TRUE
THEN RAISE EXCEPTION 'D02_POINT_CURRENT_CLAIM'; END IF;
IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.ctx06_aug03_check_v5(boolean,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '182c4ece2975b98fc114fda6f904aae832d05a58221f19271d55e50f1b314afd' THEN RAISE EXCEPTION 'D02_POINT_ASSERTION_HELPER_PIN';END IF;
END $guard$;
DO $complete$
DECLARE v_review jsonb; v_manifest text; v_rows bigint; v_dates bigint; v_bad bigint;v_original jsonb;
BEGIN
SELECT details INTO STRICT v_review FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=83517 AND event='D02_CURRENT_BINDING_REUSE_PRODUCTION_REVIEW_V1';
IF (v_review->>'approved'='true' AND v_review->>'assignment_event_id'='83495' AND v_review->>'claim_event_id'='83514' AND v_review->>'independent_numerical_review'='true' AND v_review->>'reviewer' IS DISTINCT FROM '/root/retained_qualification')IS DISTINCT FROM TRUE THEN RAISE EXCEPTION 'D02_ACTUAL_INDEPENDENT_FULL_REVIEW_REQUIRED';END IF;
SELECT count(*),count(DISTINCT session_date) INTO v_rows,v_dates FROM population_top3000_202608_202609_v1.d02_anchor_source_point_v2;
IF v_rows<>126000 OR v_dates<>42 OR(SELECT count(*)FROM population_top3000_202608_202609_v1.d02_anchor_source_checkpoint_v2)<>42 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_anchor_source_checkpoint_v2 WHERE rows<>3000)THEN RAISE EXCEPTION 'D02_FULL42_126000_REQUIRED';END IF;
SELECT count(*)INTO v_bad FROM(SELECT p.session_date,encode(sha256(convert_to(string_agg(to_jsonb(p)::text,E'\n' ORDER BY company_ordinal),'UTF8')),'hex')h,count(*)n FROM population_top3000_202608_202609_v1.d02_anchor_source_point_v2 p GROUP BY session_date)x JOIN population_top3000_202608_202609_v1.d02_anchor_source_checkpoint_v2 c USING(session_date)WHERE x.h IS DISTINCT FROM c.value_manifest_sha256 OR x.n<>3000;
IF v_bad<>0 THEN RAISE EXCEPTION 'D02_ALL42_VALUE_SHA_REQUIRED';END IF;
SELECT details->'preserve_completed_checkpoint_preimage' INTO STRICT v_original FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=83382;
IF v_original IS DISTINCT FROM(SELECT jsonb_agg(to_jsonb(c)ORDER BY session_date)FROM population_top3000_202608_202609_v1.d02_anchor_source_checkpoint_v2 c WHERE source_day_id IN(4,5,6))THEN RAISE EXCEPTION 'D02_FIRST3_IMMUTABLE_REQUIRED';END IF;
SELECT encode(sha256(convert_to(string_agg(jsonb_build_object('session_date',session_date,'value_manifest_sha256',value_manifest_sha256,'checkpoint_sha256',checkpoint_sha256)::text,E'\n'ORDER BY session_date),'UTF8')),'hex')INTO v_manifest FROM population_top3000_202608_202609_v1.d02_anchor_source_checkpoint_v2;
IF v_manifest IS DISTINCT FROM v_review->>'verified_grid_manifest_sha256' THEN RAISE EXCEPTION 'D02_INDEPENDENT_GRID_MANIFEST_MISMATCH';END IF;
UPDATE population_top3000_202608_202609_v1.d02_anchor_source_completion_v2 SET status='COMPLETE_INDEPENDENTLY_VERIFIED',verified_grid_manifest_sha256=v_manifest,independent_review_event_id=83517,assignment_event_id=83495,claim_event_id=83514 WHERE singleton AND status='STAGED';
IF NOT FOUND THEN RAISE EXCEPTION 'D02_EXACT_STAGED_PREIMAGE_REQUIRED';END IF;
IF(SELECT count(*)FROM population_top3000_202608_202609_v1.working_d02_dated_source_points_v2)<>126000 THEN RAISE EXCEPTION 'D02_ACTUAL_COMPLETE_CONSUMER_126000_REQUIRED';END IF;
IF(SELECT sum(pg_total_relation_size(oid))FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relname IN('d02_anchor_source_point_v2','d02_anchor_source_checkpoint_v2','d02_anchor_source_completion_v2')AND relkind='r')>67108864 THEN RAISE EXCEPTION 'D02_WHOLE64MIB';END IF;
END $complete$;
SELECT jsonb_build_object('completion',(SELECT to_jsonb(c)FROM population_top3000_202608_202609_v1.d02_anchor_source_completion_v2 c WHERE singleton),'consumer_rows',(SELECT count(*)FROM population_top3000_202608_202609_v1.working_d02_dated_source_points_v2))proof;
COMMIT;


