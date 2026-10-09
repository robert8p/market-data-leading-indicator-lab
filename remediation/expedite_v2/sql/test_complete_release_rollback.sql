BEGIN;
SET LOCAL lock_timeout='20s';
SET LOCAL ROLE service_role;
DO $$ BEGIN
 IF public.market_data_remediation_validation_step_v2('{"run_id":"market_data_remediation_20261008_v1","version":"expedite_20261009_v2","worker_id":"validation-11111111-1111-1111-1111-111111111111","action":"next","kind":"FEATURE"}')->>'status' IS DISTINCT FROM 'PAUSED' THEN RAISE EXCEPTION 'disabled_gate_failed';END IF;
END $$;
RESET ROLE;
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{expedite_v2,validation_enabled}','true') WHERE run_id='market_data_remediation_20261008_v1';
SET LOCAL ROLE service_role;
DO $$ DECLARE q jsonb;BEGIN
 q=public.market_data_remediation_validation_step_v2('{"run_id":"market_data_remediation_20261008_v1","version":"expedite_20261009_v2","worker_id":"validation-11111111-1111-1111-1111-111111111111","action":"next","kind":"FEATURE"}');
 IF q->>'status' IS DISTINCT FROM 'VALIDATED' THEN RAISE EXCEPTION 'service_role_validation_failed:%',q;END IF;
 q=public.market_data_remediation_validation_step_v2('{"run_id":"market_data_remediation_20261008_v1","version":"expedite_20261009_v2","worker_id":"validation-11111111-1111-1111-1111-111111111111","action":"finalize"}');
 IF q->>'status' IS DISTINCT FROM 'WAITING_VALIDATION' THEN RAISE EXCEPTION 'premature_release:%',q;END IF;
 IF EXISTS(SELECT 1 FROM public.market_data_shared_features_canonical_v2(12520,'2026-03-02','2026-03-02T20:00Z',true)) THEN RAISE EXCEPTION 'unreleased_features_visible';END IF;
 BEGIN
 PERFORM public.market_data_remediation_validation_step_v2('{"run_id":"other","version":"expedite_20261009_v2","worker_id":"validation-11111111-1111-1111-1111-111111111111","action":"next","kind":"SOURCE"}');
 RAISE EXCEPTION 'invalid_scope_accepted';EXCEPTION WHEN OTHERS THEN IF SQLERRM='invalid_scope_accepted' THEN RAISE;END IF;END;
END $$;
RESET ROLE;
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{user_stop,active}','true') WHERE run_id='market_data_remediation_20261008_v1';
SET LOCAL ROLE service_role;
DO $$ BEGIN
 IF public.market_data_remediation_validation_step_v2('{"run_id":"market_data_remediation_20261008_v1","version":"expedite_20261009_v2","worker_id":"validation-11111111-1111-1111-1111-111111111111","action":"next","kind":"SOURCE"}')->>'status' IS DISTINCT FROM 'PAUSED' THEN RAISE EXCEPTION 'stop_gate_failed';END IF;
END $$;
RESET ROLE;
DO $$ BEGIN
 IF has_function_privilege('anon','public.market_data_remediation_validation_step_v2(jsonb)','EXECUTE') OR has_function_privilege('authenticated','public.market_data_remediation_validation_step_v2(jsonb)','EXECUTE') OR has_table_privilege('service_role','market_governance.remediation_listed_price_release_v2','SELECT') THEN RAISE EXCEPTION 'privilege_leak';END IF;
END $$;
RESET ROLE;
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{user_stop,active}','false') WHERE run_id='market_data_remediation_20261008_v1';
-- Hypothetical complete feature generation: positive release branch only, rolled back.
UPDATE market_governance.remediation_validation_job_v2 SET status='VALIDATED',evidence=jsonb_build_object('sessions',CASE WHEN ordinal=11289 THEN 3053710 ELSE 0 END,'retained_minutes',CASE WHEN ordinal=11289 THEN 371008247 ELSE 0 END,'scheduled_minutes',CASE WHEN ordinal=11289 THEN 1186167180 ELSE 0 END) WHERE kind='FEATURE';
UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(protections,'{shared_feature_runner,status}','"COMPLETE_PENDING_RELEASE_VALIDATION"') WHERE run_id='market_data_remediation_20261008_v1';
SET LOCAL ROLE service_role;
DO $$ DECLARE q jsonb;BEGIN
 q=public.market_data_remediation_validation_step_v2('{"run_id":"market_data_remediation_20261008_v1","version":"expedite_20261009_v2","worker_id":"validation-11111111-1111-1111-1111-111111111111","action":"finalize"}');
 IF q->>'status' IS DISTINCT FROM 'FEATURE_RELEASE_PROMOTED' THEN RAISE EXCEPTION 'positive_release_branch:%',q;END IF;
 IF NOT EXISTS(SELECT 1 FROM public.market_data_shared_features_canonical_v2(12520,'2026-03-02','2026-03-02T20:00Z',true)) THEN RAISE EXCEPTION 'released_archive_features_missing';END IF;
 IF EXISTS(SELECT 1 FROM public.market_data_shared_features_canonical_v2(12520,'2026-03-02','2026-03-02T20:00Z',false)) THEN RAISE EXCEPTION 'released_strict_features_leak';END IF;
END $$;
ROLLBACK;
SELECT 'service_role_scope_stop_and_premature_release_tests_passed' result;
