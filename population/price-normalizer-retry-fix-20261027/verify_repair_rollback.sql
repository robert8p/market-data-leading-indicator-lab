-- Reproduction harness for the actual 27 October 2026 test.
-- Candidate and test bodies are pinned immutable mission artifacts, not provider data.
-- Requires a separately valid, current test execution authorization.
-- Never change ROLLBACK to COMMIT: this is not a production migration or a state handoff.
BEGIN;
SET LOCAL statement_timeout='35s';
SET LOCAL lock_timeout='750ms';
DO $run$
DECLARE code text; tests text;
BEGIN
 IF NOT pg_try_advisory_xact_lock(2026083000261022) THEN
  RAISE EXCEPTION 'BUSY_NO_CHANGE';
 END IF;
 IF to_regprocedure('population_top3000_202608_202609_v1.retained_price_normalize_v4(uuid,text)') IS NOT NULL THEN
  RAISE EXCEPTION 'UNEXPECTED_DEPLOYED_CANDIDATE';
 END IF;
 SELECT details->>'candidate_sql' INTO STRICT code
 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=94953;
 SELECT details->>'test_sql' INTO STRICT tests
 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event_id=95014;
 IF encode(sha256(convert_to(code,'UTF8')),'hex')<>'2ea06221f85c42d06640fe948321525f742becba1060e214a1fec8eb0bab1a9f'
 OR encode(sha256(convert_to(tests,'UTF8')),'hex')<>'930ecebbd103bcf839aa4166d959c9275ac5d1853c45108e60cdd7d1016572c9' THEN
  RAISE EXCEPTION 'PINNED_TEST_CHANGED';
 END IF;
 EXECUTE code;
 EXECUTE tests;
END $run$;
SELECT proof||jsonb_build_object(
 'candidate_event_id',94953,
 'regression_suite_event_id',95014,
 'actual_database_test',true,
 'rollback_only',true,
 'recorded_at',clock_timestamp()) AS proof
FROM norm4_proof;
ROLLBACK;

-- Separately read original immutable artifacts without executing them:
-- SELECT event_id, details->>'candidate_sql', details->>'test_sql'
-- FROM population_top3000_202608_202609_v1.pipeline_event
-- WHERE event_id IN (94953,95014);
