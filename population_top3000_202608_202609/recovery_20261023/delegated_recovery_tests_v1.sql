-- These tests do not mutate production ownership, stops, budgets, or admissions.
-- Run after installing the reviewed decision function. Any failed case raises.
BEGIN;
SET LOCAL statement_timeout='8s';
DO $tests$
DECLARE base jsonb:='{"qualified":true,"owner_stop_matches":true,"scope_matches":true,"stopped":false,"controls_enabled":true,"admission_valid":true,"ticks_available":true,"budget_scheduler_valid":true,"scheduler_binding_valid":true,"scheduler_active":false,"maintenance_wait":false,"engineering_active":false,"active_operations":false,"competing_session":false,"code_matches":true,"resources_valid":true,"runnable_count":3,"database_only_reviewed":true,"restart_allowance":true,"cooldown_and_progress":true}';t record;actual text;n integer:=0;
BEGIN
 FOR t IN SELECT * FROM (VALUES
 ('qualified_restore','{}'::jsonb,'ELIGIBLE_FOR_GUARDED_WAKE'),
 ('no_delegation','{"qualified":false}','NO_QUALIFIED_DELEGATION'),
 ('stale_owner_or_stop_generation','{"owner_stop_matches":false}','OWNER_OR_STOP_CHANGED'),
 ('scope_changed','{"scope_matches":false}','SCOPE_CHANGED'),
 ('user_stop','{"stopped":true}','USER_STOP'),
 ('explicit_hold','{"controls_enabled":false}','EXPLICIT_HOLD'),
 ('expired_authority','{"admission_valid":false}','AUTHORISATION_EXPIRED'),
 ('tick_limit','{"ticks_available":false}','TICK_LIMIT'),
 ('budget_hold','{"budget_scheduler_valid":false}','BUDGET_OR_SCHEDULER_HOLD'),
 ('scheduler_binding_changed','{"scheduler_binding_valid":false}','SCHEDULER_BINDING_CHANGED'),
 ('no_duplicate_worker','{"scheduler_active":true}','ALREADY_ACTIVE'),
 ('maintenance_wait','{"maintenance_wait":true}','BOUNDED_MAINTENANCE_WAIT'),
 ('active_engineering','{"engineering_active":true}','ACTIVE_ENGINEERING_CHECKPOINT'),
 ('active_or_uncertain_lease','{"active_operations":true}','ACTIVE_OR_UNCERTAIN_OPERATION'),
 ('competing_implementation','{"competing_session":true}','COMPETING_DATABASE_OPERATION'),
 ('untested_code_change','{"code_matches":false}','CODE_OR_REGISTRATION_CHANGED_REQUALIFY'),
 ('resource_ceiling','{"resources_valid":false}','RESOURCE_LIMIT'),
 ('no_ready_work','{"runnable_count":0}','NO_RUNNABLE_WORK'),
 ('unreviewed_provider_work','{"database_only_reviewed":false}','UNREVIEWED_OR_PROVIDER_WORK_REQUIRES_CONTROLLER'),
 ('finite_restart_budget','{"restart_allowance":false}','BOUNDED_RECOVERY_LIMIT'),
 ('no_progress_or_cooldown','{"cooldown_and_progress":false}','COOLDOWN_OR_NO_USEFUL_PROGRESS'),
 ('unknown_ownership_fails_closed','{"owner_stop_matches":null}','OWNER_OR_STOP_CHANGED'),
 ('unknown_budget_fails_closed','{"budget_scheduler_valid":null}','BUDGET_OR_SCHEDULER_HOLD'),
 ('unknown_code_fails_closed','{"code_matches":null}','CODE_OR_REGISTRATION_CHANGED_REQUALIFY'),
 ('unknown_operation_fails_closed','{"active_operations":null}','ACTIVE_OR_UNCERTAIN_OPERATION')
 ) AS c(name,patch,expected) LOOP
  actual:=population_top3000_202608_202609_v1.population_recovery_decision_v1(base||t.patch);
  IF actual IS DISTINCT FROM t.expected THEN RAISE EXCEPTION 'Recovery test % expected %, got %',t.name,t.expected,actual;END IF;
  n:=n+1;
 END LOOP;
 IF n<>25 THEN RAISE EXCEPTION 'Unexpected test count %',n;END IF;
 IF has_function_privilege('anon','population_top3000_202608_202609_v1.population_recovery_request_v1(text,text)','EXECUTE') OR has_function_privilege('authenticated','population_top3000_202608_202609_v1.population_recovery_request_v1(text,text)','EXECUTE') THEN RAISE EXCEPTION 'Recovery endpoint must not be publicly executable';END IF;
 PERFORM set_config('population.recovery_test_summary',jsonb_build_object('passed',n,'failed',0,'production_controls_mutated',false,'code_manifest',population_top3000_202608_202609_v1.population_recovery_manifest_v1())::text,true);
END $tests$;
SELECT current_setting('population.recovery_test_summary')::jsonb;
ROLLBACK;
