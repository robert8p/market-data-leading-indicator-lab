BEGIN; SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='15s';
DO $admit$ DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;n bigint;
BEGIN
IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN RAISE EXCEPTION 'BUSY'; END IF;
SELECT * INTO a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
SELECT * INTO p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence<>3 OR a.stop_generation<>1 OR a.stopped OR p.expected_stop_generation<>1 OR NOT population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) THEN RAISE EXCEPTION 'Authority mismatch'; END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE ended_at IS NULL) THEN RAISE EXCEPTION 'In flight';END IF;
SELECT coalesce(sum(pg_total_relation_size(c.oid)),0) INTO n FROM pg_class c JOIN pg_namespace s ON s.oid=c.relnamespace WHERE s.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
IF n>3450000000 THEN RAISE EXCEPTION 'Measured normalization headroom changed %',n;END IF;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET payload=jsonb_set(payload,'{max_series}','13044'),status='READY',blocker_code=null,resumption_condition=null,not_before=clock_timestamp(),owner_fence=3,stop_generation=1 WHERE task_id='1be894c2-f9c2-43c7-b7a6-d3c048ea388f' AND status='BLOCKED' AND blocker_code='DAILY_SHARED_FIRST_BATCH_RESOURCE_REVIEW' AND checkpoint='{"last_series":10000}'::jsonb;
IF NOT FOUND THEN RAISE EXCEPTION 'Daily checkpoint changed';END IF;
UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET enabled=false WHERE family_id='D03' AND stage='DERIVE';
UPDATE population_top3000_202608_202609_v1.acquisition_control SET enabled=true,hold_reason=null,next_stage='PIPELINE',next_action='Complete retained13044 daily inputs; bounded derive measurement follows automatically through controller assignment',updated_at=clock_timestamp() WHERE singleton;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ENGINEERING_INTEGRATION_VERIFIED',jsonb_build_object('assignment_id','completion-20261024-prompt01','authority_event_id',9528,'owner_fence',3,'stop_generation',1,'patch_sha256','0352ddbc41cb5921ff9be9de7962a95ca1ab4a28c3162690775a528c19a1b388','independent_reviewer','top3000-completion-20261024-acceptance07','policy_fixture_passes',15,'type_infinity_rejected',true,'counters_preserved',true));
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('DAILY_RETAINED_COMPLETION_ADMITTED_20261024',jsonb_build_object('assignment_id','completion-20261024-prompt03','owner_fence',3,'stop_generation',1,'prior_normalized_series',10000,'full_series_denominator',13044,'remaining_input_cells',313532,'projected_additional_input_bytes',145000000,'measured_relation_bytes',n,'relation_guard_unchanged',4000000000,'normalization_finish_estimate_minutes',jsonb_build_array(4,6),'full_derivation_estimate_minutes',25,'decision','Complete tested13044 run; new cohort successor needs unaccepted company manifest and implementation, so cannot shorten immediate completion','derive_gate','D03 DERIVE adapter temporarily disabled until first25 measurement admitted; root/runtime assignment responsible','counters_and_original_denominator_preserved',true));
END $admit$;
SELECT population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-completion-20261024-root-5db48b32d103',3,'Resume admitted retained daily input completion under authority9528; independent policy review passed');
COMMIT;
