WITH pages AS MATERIALIZED (
 SELECT d.as_of,d.request_key,t.input_versions,p.*,s.content_text,s.content_bytes,s.source_sha256 doc_sha
 FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9 d
 JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.payload->>'request_key'=d.request_key AND t.stage='ACQUIRE'
 JOIN population_top3000_202608_202609_v1.ref01_date_page_v4 p ON p.acquire_task_id=t.task_id
 JOIN population_top3000_202608_202609_v1.source_document s ON s.source_id=p.source_id
 WHERE d.plan_id='ref01-date-fullperiod-v9' AND d.as_of<='2026-09-06'
), anchor AS MATERIALIZED (
 SELECT ticker,content,record_sha256 FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3
 WHERE cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'
 AND eligible AND point_date='2026-08-03'
), raw AS MATERIALIZED (
 SELECT p.source_id,p.source_sha256,p.doc_sha,p.as_of,p.input_versions,x.rec,x.ord::integer ord,a.content anchor,
 (a.content->>'company_id' IS NOT NULL AND nullif(x.rec->>'composite_figi','') IS NOT NULL AND nullif(x.rec->>'share_class_figi','') IS NOT NULL
 AND x.rec->>'composite_figi'=a.content#>>'{source_record,composite_figi}' AND x.rec->>'share_class_figi'=a.content#>>'{source_record,share_class_figi}'
 AND x.rec->>'cik' IS NOT DISTINCT FROM a.content#>>'{source_record,cik}' AND x.rec->>'type'=a.content#>>'{source_record,type}'
 AND x.rec->>'primary_exchange'=a.content#>>'{source_record,primary_exchange}' AND x.rec->>'active'='true' AND x.rec->>'market'='stocks' AND x.rec->>'locale'='us') good
 FROM pages p CROSS JOIN LATERAL jsonb_array_elements(p.content_text::jsonb->'results')WITH ORDINALITY x(rec,ord)
 LEFT JOIN anchor a ON a.ticker=x.rec->>'ticker'
), expected AS MATERIALIZED (
 SELECT r.*,jsonb_build_object('source_record',rec,'source_sha256',doc_sha,'source_id',source_id,'source_ordinal',ord,'point_date',as_of::text,
 'cohort_sha256',input_versions->>'cohort_sha256','policy_sha256',input_versions->>'policy_sha256','membership_acceptance_sha256',input_versions->>'membership_acceptance_sha256',
 'company_id',CASE WHEN good THEN anchor->>'company_id' END,'identity_state',CASE WHEN good THEN 'FROZEN_ANCHOR_TICKER_AND_TWO_FIGI_MATCH' ELSE 'UNSELECTED_OR_IDENTITY_LIFECYCLE_REVIEW_REQUIRED' END,
 'anchor_record_sha256',CASE WHEN anchor IS NOT NULL THEN encode(sha256(convert_to(anchor::text,'UTF8')),'hex') END,
 'original_historical_receipt',false,'full_window_complete',false,'family_accepted',false) expect
 FROM raw r
), compared AS MATERIALIZED (
 SELECT e.*,n.record_sha256,n.content stored,n.company_id stored_company,n.identity_state stored_state,n.point_date stored_date
 FROM expected e LEFT JOIN population_top3000_202608_202609_v1.ref01_date_record_v4 n ON n.source_id=e.source_id AND n.ordinal=e.ord
), derived AS MATERIALIZED (
 SELECT c.*,d.content derived_content,d.record_sha256 derived_sha,
 (expect-'source_record'||jsonb_build_object('source_record_sha256',encode(sha256(convert_to(expect::text,'UTF8')),'hex'),'ticker',rec->>'ticker','evidence_semantics','STRONG_IDENTIFIER_MATCHED_VENDOR_DATE_POINT_NOT_INTERVAL')) dexpect
 FROM compared c LEFT JOIN population_top3000_202608_202609_v1.ref01_practical_dated_instrument_v3 d ON d.cohort_sha256=c.input_versions->>'cohort_sha256'
 AND d.source_set_sha256=c.doc_sha AND d.company_id=c.anchor->>'company_id' AND d.ticker=c.rec->>'ticker' AND d.point_date=c.as_of
 WHERE c.good IS TRUE
)
SELECT clock_timestamp() observed_at,
 (SELECT count(*) FROM pages) pages,(SELECT count(DISTINCT as_of) FROM pages) dates,
 (SELECT sum(result_count) FROM pages) source_declared_rows,
 (SELECT count(*) FROM pages WHERE source_sha256 IS DISTINCT FROM doc_sha OR doc_sha IS DISTINCT FROM encode(sha256(convert_to(content_text,'UTF8')),'hex') OR content_bytes IS DISTINCT FROM octet_length(content_text)
 OR result_count IS DISTINCT FROM jsonb_array_length(content_text::jsonb->'results') OR normalized_source_records IS DISTINCT FROM result_count OR normalized_at IS NULL) source_page_mismatches,
 (SELECT count(*) FROM anchor WHERE record_sha256 IS DISTINCT FROM encode(sha256(convert_to(content::text,'UTF8')),'hex')) anchor_hash_mismatches,
 (SELECT count(*) FROM compared) raw_records,
 (SELECT count(*) FROM compared WHERE good IS TRUE) expected_selected_points,
 (SELECT count(*) FROM compared WHERE good IS NOT TRUE) expected_review_or_unselected,
 (SELECT count(*) FROM compared WHERE stored IS DISTINCT FROM expect OR record_sha256 IS DISTINCT FROM encode(sha256(convert_to(expect::text,'UTF8')),'hex')
 OR stored_company IS DISTINCT FROM CASE WHEN good THEN anchor->>'company_id' END
 OR stored_state IS DISTINCT FROM expect->>'identity_state' OR stored_date IS DISTINCT FROM as_of) normalization_mismatches,
 (SELECT count(*) FROM derived WHERE derived_content IS DISTINCT FROM dexpect OR derived_sha IS DISTINCT FROM encode(sha256(convert_to(dexpect::text,'UTF8')),'hex')) derived_mismatches,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_date_record_v4 n JOIN pages p USING(source_id)) actual_normalized_rows,
 (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_practical_dated_instrument_v3 d JOIN pages p ON d.source_set_sha256=p.doc_sha AND d.point_date=p.as_of AND d.cohort_sha256=p.input_versions->>'cohort_sha256') actual_derived_rows,
 (SELECT jsonb_agg(jsonb_build_object('date',as_of,'points',n,'companies',companies) ORDER BY as_of) FROM(SELECT as_of,count(*)n,count(DISTINCT anchor->>'company_id') companies FROM derived GROUP BY as_of)x) date_points,
 false full_period_qualified,false lifecycle_intervals_qualified,false original_historical_receipt_qualified,false family_accepted;
