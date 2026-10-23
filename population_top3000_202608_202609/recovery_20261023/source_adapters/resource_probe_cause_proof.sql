BEGIN;
SET LOCAL lock_timeout='3s';
DO $proof$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0) THEN RAISE EXCEPTION 'OWNER_FENCE_CHANGED'; END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
 SELECT 'RESOURCE_PROBE_JSON_SERIALIZATION_CAUSE_VERIFIED',jsonb_build_object(
 'result','CONFIRMED','owner_fence',2,'probe_attempt',2,
 'validation_error','INVALID_SANITIZED_PROBE_RESULT','root_cause','POSTGRES345_JSONB_DOUBLE_SERIALIZATION',
 'local_test_sha256','e228ca07add51f48a3f1e04cd3e80b0bbf1652c57d2c3a08c89919aea84c2f21',
 'handler_test_sha256','7381bf94c2c5eabff319f134f4fb6bf56c55ea01defdf274f1a19597070728a5',
 'root_reexecuted_actual_serializer_tests',true,'server_error_window_start','2026-10-23T11:54:20Z','server_error_window_end','2026-10-23T11:54:24Z',
 'driver_source','https://github.com/porsager/postgres/tree/v3.4.5/src',
 'request_replayed',false)
 WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='RESOURCE_PROBE_JSON_SERIALIZATION_CAUSE_VERIFIED');
END $proof$;
COMMIT;
