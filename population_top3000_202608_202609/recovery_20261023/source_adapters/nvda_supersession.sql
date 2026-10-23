BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $supersede$
DECLARE oldrow jsonb;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND hold_reason IS NULL) THEN RAISE EXCEPTION 'OWNER_CHANGED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id='433682bd-ab82-4cba-ad5f-07818422748f' AND status='COMPLETE')
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id='aaf0d303-2337-4ec1-8b8c-c32d3e15f355' AND status='COMPLETE')
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_cover_fact_v1 WHERE source_id='company:sec-document:0001045810-26-000052' AND extraction_state='PARSED_COVER_INSTANT_CANDIDATE' AND value=24200000000 AND measurement_date='2026-05-15')
 THEN RAISE EXCEPTION 'VERIFIED_SUCCESSOR_SOURCE_REQUIRED';END IF;
 SELECT to_jsonb(t) INTO oldrow FROM population_top3000_202608_202609_v1.pipeline_task t WHERE task_id='a6f7b8b5-2c54-46fa-a98f-9eda6277c953' AND status='BLOCKED' FOR UPDATE;
 IF NOT FOUND THEN RETURN;END IF;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='CANCELLED',completed_at=clock_timestamp(),
 checkpoint=checkpoint||jsonb_build_object('superseded_by','433682bd-ab82-4cba-ad5f-07818422748f','reason','Exact retained provider document acquired and normalized through a new immutable source-hash-bound task; original task and failure history preserved')
 WHERE task_id='a6f7b8b5-2c54-46fa-a98f-9eda6277c953';
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,task_id,details) VALUES('BLOCKED_SOURCE_TASK_SUPERSEDED',
 'a6f7b8b5-2c54-46fa-a98f-9eda6277c953',jsonb_build_object('prior_task',oldrow,'successor_task','433682bd-ab82-4cba-ad5f-07818422748f','successor_normalize','aaf0d303-2337-4ec1-8b8c-c32d3e15f355','quality_certified',false,'original_input_versions_preserved',true));
END $supersede$;
COMMIT;
