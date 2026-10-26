BEGIN;SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='10s';SET LOCAL work_mem='32MB';SET LOCAL jit=off;DO $natural$ DECLARE v_until timestamptz:=clock_timestamp()+interval '8 seconds';v_locked boolean;BEGIN LOOP
BEGIN v_locked:=pg_try_advisory_xact_lock(2026083000261022);IF NOT v_locked THEN RAISE EXCEPTION 'D02_V3_BUSY_SENTINEL';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE outcome='RUNNING'OR ended_at IS NULL)OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.connector_probe_lease_v1 WHERE released_at IS NULL)OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_job WHERE status='RUNNING')THEN RAISE EXCEPTION 'D02_V3_BUSY_SENTINEL';END IF;EXIT;
EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM IS DISTINCT FROM 'D02_V3_BUSY_SENTINEL' THEN RAISE;END IF;END;
IF clock_timestamp()>=v_until THEN RAISE EXCEPTION 'D02_V3_BOUNDED_NATURAL_GAP_UNAVAILABLE';END IF;PERFORM pg_sleep(0.15);END LOOP;END $natural$;
SELECT set_config('d02guard83514.before_ac',(SELECT to_jsonb(x)::text FROM population_top3000_202608_202609_v1.acquisition_control x WHERE singleton),true),set_config('d02guard83514.before_pc',(SELECT to_jsonb(x)::text FROM population_top3000_202608_202609_v1.pipeline_control x WHERE singleton),true);
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
DO $negative$ BEGIN BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET owner_id='D02_TEST_STALE' WHERE singleton;
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
END;
RAISE EXCEPTION 'D02_TEST_GUARD_FAILED_OPEN';EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM IS DISTINCT FROM 'D02_POINT_CURRENT_CLAIM' THEN RAISE;END IF;END;END $negative$;
DO $negative$ BEGIN BEGIN UPDATE population_top3000_202608_202609_v1.acquisition_control SET stop_generation=stop_generation+1 WHERE singleton;
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
END;
RAISE EXCEPTION 'D02_TEST_GUARD_FAILED_OPEN';EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM IS DISTINCT FROM 'D02_POINT_CURRENT_CLAIM' THEN RAISE;END IF;END;END $negative$;
DO $negative$ BEGIN BEGIN DECLARE ac population_top3000_202608_202609_v1.acquisition_control%rowtype; pc population_top3000_202608_202609_v1.pipeline_control%rowtype; a jsonb;c jsonb;
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
c:=jsonb_set(c,'{expires_at}',to_jsonb('2000-01-01T00:00:00Z'::text));
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
END;
RAISE EXCEPTION 'D02_TEST_GUARD_FAILED_OPEN';EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM IS DISTINCT FROM 'D02_POINT_CURRENT_CLAIM' THEN RAISE;END IF;END;END $negative$;
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
SELECT jsonb_build_object('current_guard_positive',true,'full_ac_control_restored',(SELECT to_jsonb(x)=current_setting('d02guard83514.before_ac')::jsonb FROM population_top3000_202608_202609_v1.acquisition_control x WHERE singleton),'full_pc_control_restored',(SELECT to_jsonb(x)=current_setting('d02guard83514.before_pc')::jsonb FROM population_top3000_202608_202609_v1.pipeline_control x WHERE singleton),'stale_owner_rejected',true,'later_stop_rejected',true,'expired_claim_rejected',true,'staged_rows',(SELECT count(*)FROM population_top3000_202608_202609_v1.d02_anchor_source_point_v2),'consumer_rows',(SELECT count(*)FROM population_top3000_202608_202609_v1.working_d02_dated_source_points_v2),'first3_checkpoint_hashes',(SELECT jsonb_agg(checkpoint_sha256 ORDER BY session_date)FROM population_top3000_202608_202609_v1.d02_anchor_source_checkpoint_v2))proof;
ROLLBACK;

