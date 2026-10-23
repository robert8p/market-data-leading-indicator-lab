BEGIN;SET LOCAL lock_timeout='15s';SET LOCAL statement_timeout='25s';SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE h text;n integer;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_ADMISSION_CHANGED';END IF;
h:=encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.finra_independent_validate_v1(uuid,text)'::regprocedure),'UTF8')),'hex');
IF h<>'bdc992cc72a89a39d0942e2f56b46c9f0f0451a8cc95de2166090aa3dae6b15c' THEN RAISE EXCEPTION 'TESTED_HASH_CHANGED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='S01' AND stage='VALIDATE' AND status='LEASED') THEN RAISE EXCEPTION 'VALIDATOR_LEASE_ACTIVE';END IF;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('FINRA_VERIFICATION_BOUNDED_RUNTIME_REVIEW',jsonb_build_object('result','PASS','owner_fence',2,'stop_generation',0,'tested_function_sha256',h,'test_result','PASS_FINRA_ACTUAL_ALTERNATE_PARSER_ALL_ROWS_VALUE_MISSINGNESS_AND_HOLD','actual_source_date','2026-09-30','actual_rows_compared',12372,'negative_value_and_missing_row_cases_passed',true,'reason','12 files exceeded the prior 4500ms full-readback time guard; measured scheduler dispatches approximately 5 to 6.4 seconds. Retain exact checks and admit a 9500ms helper ceiling / 10 second batch.','algorithm_unchanged',true,'producer_bundle_unchanged',true,'no_source_requests',true,'counters_preserved',true));
UPDATE population_top3000_202608_202609_v1.pipeline_adapter SET function_sha256=h,expected_batch_seconds=10,producer_revision='finra-regex-independent-readback-v2-bounded10s',
reviewed_test_evidence=reviewed_test_evidence||jsonb_build_object('bounded_runtime_test',jsonb_build_object('result','PASS','tested_function_sha256',h,'source_date','2026-09-30','rows',12372,'all_fixture_changes_rolled_back',true,'helper_max_ms',9500))
WHERE family_id='S01' AND stage='VALIDATE' AND function_sha256='c7eaa7eff4645d75ba79321e9eeff3b63ecf441d79e08d0943c364b1d42adf59';
GET DIAGNOSTICS n=ROW_COUNT;IF n<>1 THEN RAISE EXCEPTION 'EXACT_PRIOR_VALIDATOR_BINDING_REQUIRED';END IF;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp()
WHERE family_id='S01' AND stage='VALIDATE' AND status='BLOCKED' AND blocker_code='PARSER' AND owner_fence=2 AND stop_generation=0
AND resumption_condition#>>'{details,message}'='FINRA_READBACK_TIME_BOUND';
GET DIAGNOSTICS n=ROW_COUNT;IF n<>12 THEN RAISE EXCEPTION 'EXACT_12_MEASURED_TIME_BOUND_FAILURES_REQUIRED n=%',n;END IF;
PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'TESTED_BOUNDED_FINRA_READBACK_RUNTIME_RECOVERY');
END $admit$;COMMIT;
