-- Root ONLY after actual first file header/footer/decimal/source-row reconciliation.
BEGIN;
SET LOCAL lock_timeout='3s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE c record;bundle text;tid uuid;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton
  AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0
  AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
 bundle:=population_top3000_202608_202609_v1.finra_code_hash_v1();
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event e JOIN population_top3000_202608_202609_v1.source_document d
  ON d.source_id='S01:FINRA_CNMS:2026-08-03' AND d.source_sha256=e.details->>'source_sha256'
  WHERE e.event='FINRA_FIRST_FILE_RECONCILED' AND e.details->>'result'='PASS' AND e.details->>'finra_bundle_sha256'=bundle)
 THEN RAISE EXCEPTION 'ACTUAL_FIRST_FILE_RECONCILIATION_REQUIRED';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date BETWEEN '2026-08-03' AND '2026-09-30')<>42
 THEN RAISE EXCEPTION 'FROZEN_42_SESSION_CALENDAR_REQUIRED';END IF;
 FOR c IN SELECT * FROM population_top3000_202608_202609_v1.catalog_calendar WHERE session_date BETWEEN '2026-08-03' AND '2026-09-30' ORDER BY session_date LOOP
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('S01','ACQUIRE','CNMS:'||c.session_date::text,
   jsonb_build_object('kind','FINRA_CNMS_DAILY','session_date',c.session_date,'provisional',true),
   jsonb_build_object('calendar_record_sha256',c.source_record_sha256,'finra_bundle_sha256',bundle,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'));
 END LOOP;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('FINRA_CALENDAR_WAVE_ADMITTED',
  jsonb_build_object('calendar_sessions',42,'finra_bundle_sha256',bundle,'company_universe_frozen',false,'publication_eligible',false));
END $admit$;
COMMIT;
