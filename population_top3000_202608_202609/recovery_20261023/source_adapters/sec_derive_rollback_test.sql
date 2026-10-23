-- Root-only; all work rolls back. Requires fully parsed retained index and zero current candidate links.
BEGIN;
SET LOCAL lock_timeout='3s';
SET LOCAL statement_timeout='50s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $test$
DECLARE tid uuid;v jsonb;r jsonb;i integer;n bigint;expected_n bigint;last1 bigint;last2 bigint;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND NOT stopped AND enabled AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'OWNER_STOP_OR_ADMISSION_CHANGED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED' AND lease_until>clock_timestamp()) THEN RAISE EXCEPTION 'LIVE_TASK_MUST_FINISH_BEFORE_TEST';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.sec_source_chunk_v1 WHERE source_id='SEC_Q3_2026_MASTER_INDEX' AND parse_state<>'PARSED') OR (SELECT count(*) FROM population_top3000_202608_202609_v1.sec_filing_index_v1 WHERE source_id='SEC_Q3_2026_MASTER_INDEX')<>207593 THEN RAISE EXCEPTION 'SEC_COMPLETE_PARSED_SOURCE_REQUIRED';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.sec_candidate_filing_link_v1 WHERE source_id='SEC_Q3_2026_MASTER_INDEX') THEN RAISE EXCEPTION 'CANDIDATE_LINKS_ALREADY_EXIST_RECONCILE_TEST';END IF;
SELECT task_id INTO tid FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='F01' AND stage='DERIVE' AND partition_key='sec-master-2026q3' AND input_versions->>'source_sha256'='24f0ee423c27ffe0d2e04ad989ba45e4a0613bad88f805f145b554a5b3e81a62';
IF tid IS NULL THEN RAISE EXCEPTION 'EXPECTED_DERIVE_SUCCESSOR_MISSING';END IF;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET not_before='infinity'::timestamptz WHERE status='READY';
INSERT INTO population_top3000_202608_202609_v1.pipeline_adapter(family_id,stage,function_oid,function_sha256,resource_class,enabled,reviewed_test_evidence,producer_revision,expected_batch_seconds)
VALUES('F01','DERIVE','population_top3000_202608_202609_v1.sec_candidate_link_v1(uuid,text)'::regprocedure::oid,
encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.sec_candidate_link_v1(uuid,text)'::regprocedure),'UTF8')),'hex'),
'DATABASE',true,'{"test":"sec_candidate_two_batch_rollback","status":"IN_PROGRESS_ROLLBACK_ONLY"}','sec-candidate-link-recovery-20261023',30)
ON CONFLICT(family_id,stage) DO NOTHING;
UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',not_before=clock_timestamp(),blocker_code=NULL,resumption_condition=NULL,blocker_until=NULL
WHERE task_id=tid AND (status='READY' OR status='BLOCKED' AND blocker_code='ADAPTER_NOT_INSTALLED');
UPDATE population_top3000_202608_202609_v1.pipeline_control SET next_wakeup_at=clock_timestamp() WHERE singleton;
FOR i IN 1..2 LOOP
r:=population_top3000_202608_202609_v1.pipeline_tick_v1();
IF r->>'state'<>'DISPATCHED' OR r->>'task_id'<>tid::text OR r#>>'{result,state}' NOT IN('CONTINUE','COMPLETE') THEN RAISE EXCEPTION 'SEC_CANDIDATE_BATCH_FAILED tick=% receipt=%',i,r;END IF;
EXIT WHEN r#>>'{result,state}'='COMPLETE';
END LOOP;
SELECT count(*) INTO n FROM population_top3000_202608_202609_v1.sec_candidate_filing_link_v1 WHERE source_id='SEC_Q3_2026_MASTER_INDEX';
SELECT least(4000,count(*)) INTO expected_n FROM population_top3000_202608_202609_v1.sec_filing_index_v1 i
JOIN (SELECT DISTINCT lpad(raw_record->>'cik',10,'0') cik FROM population_top3000_202608_202609_v1.ticker_overview WHERE nullif(raw_record->>'cik','') IS NOT NULL) o ON o.cik=i.filing_cik
WHERE i.source_id='SEC_Q3_2026_MASTER_INDEX';
IF n<>expected_n OR n=0 THEN RAISE EXCEPTION 'SEC_CANDIDATE_BATCH_COUNT actual=% expected=%',n,expected_n;END IF;
IF EXISTS(WITH expected AS(
 SELECT i.source_id,i.source_line_number,i.filing_cik company_cik
 FROM population_top3000_202608_202609_v1.sec_filing_index_v1 i
 JOIN (SELECT DISTINCT lpad(raw_record->>'cik',10,'0') cik FROM population_top3000_202608_202609_v1.ticker_overview WHERE nullif(raw_record->>'cik','') IS NOT NULL)o ON o.cik=i.filing_cik
 WHERE i.source_id='SEC_Q3_2026_MASTER_INDEX' ORDER BY i.source_line_number LIMIT 4000
),observed AS(SELECT source_id,source_line_number,company_cik FROM population_top3000_202608_202609_v1.sec_candidate_filing_link_v1 WHERE source_id='SEC_Q3_2026_MASTER_INDEX')
SELECT 1 FROM ((SELECT * FROM expected EXCEPT SELECT * FROM observed) UNION ALL (SELECT * FROM observed EXCEPT SELECT * FROM expected))delta
) THEN RAISE EXCEPTION 'SEC_CANDIDATE_MEMBERSHIP_MISMATCH';END IF;
IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_universe WHERE status='FROZEN') THEN RAISE EXCEPTION 'CANDIDATE_LINKING_MUST_NOT_FREEZE_UNIVERSE';END IF;
END $test$;
SELECT 'PASS_ROLLBACK_ONLY_SEC_CANDIDATE_BATCHES' result,clock_timestamp() observed_at,
(SELECT count(*) FROM population_top3000_202608_202609_v1.sec_candidate_filing_link_v1 WHERE source_id='SEC_Q3_2026_MASTER_INDEX') candidate_links,
(SELECT jsonb_build_object('status',status,'checkpoint',checkpoint,'executions',executions) FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='F01' AND stage='DERIVE' AND partition_key='sec-master-2026q3') continuation;
ROLLBACK;
