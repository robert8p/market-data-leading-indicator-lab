BEGIN;
SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='10s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $admit$
DECLARE n integer;
BEGIN
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND enabled AND NOT stopped AND hold_reason IS NULL AND admitted_until>clock_timestamp()) THEN RAISE EXCEPTION 'CURRENT_OWNER_ADMISSION_REQUIRED'; END IF;
IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND enabled AND shared_scheduler_verified AND budget_evidence->>'provider_admissions_allowed'='true' AND budget_evidence->'allowed_providers' ? 'Massive') THEN RAISE EXCEPTION 'CURRENT_BUDGET_ADMISSION_REQUIRED';END IF;
IF (SELECT count(*) FROM population_top3000_202608_202609_v1.source_document WHERE (source_id,source_sha256) IN (
('massive-archive:N02:cfbde693cd6a9d4798eaaaac6be0eed981e8b81a6467cfca005f15f1b89a76e0:page:1','5e168bd1aca8562e172563aaba851ed1c647df5d50c151a660e4345a3950b867'),
('massive-archive:N02:bc6a3d57a3cc00f149c50c31ae0533affc4adf636a23d0816e960e0a8ccb69c6:page:1','5054cc5676b63c21d4581dcba75979ccb496a174dba83f35ae6885a582c25857'))
AND source_sha256=encode(sha256(convert_to(content_text,'UTF8')),'hex') AND content_bytes=octet_length(content_text))<>2 THEN RAISE EXCEPTION 'REVIEWED_FIRST_PAGE_HASHES_REQUIRED';END IF;
UPDATE population_top3000_202608_202609_v1.pipeline_task
SET payload=payload||'{"max_pages":32,"first_page_review_required":false,"first_pages_verified_at":"2026-10-23T12:40:17Z"}'::jsonb,
 status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL,not_before=clock_timestamp()
WHERE task_id IN('1091be86-f141-44ce-9bd1-3d1ec613b6ca','4a16aee2-7115-4a9a-a0ed-a90a041a4f10')
 AND family_id='N02' AND stage='ACQUIRE' AND status='BLOCKED' AND blocker_code='MASSIVE_DIVIDEND_PAGE_CAP_REVIEW'
 AND owner_fence=2 AND stop_generation=0 AND executions=2 AND checkpoint->>'next_page'='2' AND payload->>'max_pages'='1'
 AND input_versions->>'dividends_bundle_sha256'='acfb161bf0b796401364536ef53ae172b05f56f39860959a04edfb6901af862f';
GET DIAGNOSTICS n=ROW_COUNT;
IF n<>2 THEN RAISE EXCEPTION 'EXACT_TWO_PAUSED_DIVIDEND_STREAMS_REQUIRED n=%',n;END IF;
INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('N02_REVIEWED_PAGINATION_CONTINUATION_ADMITTED',
jsonb_build_object('owner_fence',2,'stop_generation',0,'reviewed_first_pages',2,'records_per_first_page',1000,'first_page_hashes',jsonb_build_array('5e168bd1aca8562e172563aaba851ed1c647df5d50c151a660e4345a3950b867','5054cc5676b63c21d4581dcba75979ccb496a174dba83f35ae6885a582c25857'),'filtered_date_window_verified',true,'max_pages_per_stream',32,'maximum_additional_provider_requests',62,'max_response_bytes',8388608,'current_global_limits_preserved',true,'checkpoints_and_counters_preserved',true,'company_coverage_verified',false,'independent_provider_accuracy_verified',false,'new_subscription',false));
PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-recovery-20261023-root-4b1314bcb5c2',2,'REVIEWED_N02_PAGINATION_CONTINUATION');
END $admit$;
COMMIT;
