DO $test$
DECLARE a uuid;n uuid;r jsonb;p bigint;h bigint;
BEGIN
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL)
  THEN RAISE EXCEPTION 'LIVE_LEASES_MUST_BE_RECONCILED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE enabled AND resource_class<>'DATABASE')
  THEN RAISE EXCEPTION 'DATABASE_ONLY_ADAPTERS_REQUIRED';END IF;
 SELECT provider_requests,http_invocations INTO p,h FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity',blocker_until=NULL WHERE status IN('READY','BLOCKED');
 UPDATE population_top3000_202608_202609_v1.pipeline_control SET enabled=true,next_wakeup_at=clock_timestamp() WHERE singleton;
 a:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE','rollback-retained-nvda-document-'||gen_random_uuid(),
  '{"kind":"SEC_DOCUMENT","url":"https://api.massive.com/v1/reference/sec/filings/0001045810-26-000052/files/nvda-20260426.htm","source_id":"company:sec-document:0001045810-26-000052","group_key":"CIK:0001045810","canonical_sec_url":"https://www.sec.gov/Archives/edgar/data/1045810/000104581026000052/nvda-20260426.htm","source_provider":"Massive","retained_only":true,"provisional":true}',
  '{"source_sha256":"1b5de37b973da4a3f1cd31a09aa455c01c519ea7cc409c73de2250ad156f99e4","accession":"0001045810-26-000052","source_provider":"Massive","admission_version":"retained-document-v1"}');
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF r#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR (r->>'task_id')::uuid IS DISTINCT FROM a THEN RAISE EXCEPTION 'RETAINED_DOCUMENT_ACQUIRE: %',r;END IF;
 n:=(r#>>'{result,successor_task_id}')::uuid;
 r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
 IF r#>>'{result,state}' IS DISTINCT FROM 'COMPLETE' OR (r->>'task_id')::uuid IS DISTINCT FROM n THEN RAISE EXCEPTION 'REAL_SEC_DOCUMENT_NORMALIZE: %',r;END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.company_cover_fact_v1
  WHERE source_id='company:sec-document:0001045810-26-000052' AND source_sha256='1b5de37b973da4a3f1cd31a09aa455c01c519ea7cc409c73de2250ad156f99e4'
   AND entity_cik='0001045810' AND extraction_state='PARSED_COVER_INSTANT_CANDIDATE' AND value>0 AND measurement_date<='2026-08-03')
  THEN RAISE EXCEPTION 'SOURCE_BOUND_COVER_FACT_NOT_EXTRACTED';END IF;
 IF (SELECT provider_requests FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>p
  OR (SELECT http_invocations FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton)<>h
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_universe WHERE status='FROZEN')
  OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document WHERE source_id='company:sec-document:0001045810-26-000052'
   AND (source_provider<>'Massive' OR historical_received_at IS NOT NULL)) THEN RAISE EXCEPTION 'SOURCE_SEMANTICS_OR_NO_NETWORK_ASSERTION';END IF;
END $test$;
SELECT 'PASS_RETAINED_MASSIVE_FILING_ACQUIRE_NORMALIZE_COVER_EXTRACTION' result,
 jsonb_agg(jsonb_build_object('source_id',source_id,'concept',concept,'value',value,'measurement_date',measurement_date,'entity_cik',entity_cik,'state',extraction_state)) facts
FROM population_top3000_202608_202609_v1.company_cover_fact_v1 WHERE source_id='company:sec-document:0001045810-26-000052';
ROLLBACK;
