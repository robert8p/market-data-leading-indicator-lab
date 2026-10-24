WITH dates AS MATERIALIZED (
SELECT * FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9 WHERE plan_id='ref01-date-fullperiod-v9' AND as_of<='2026-09-06'
), pages AS MATERIALIZED (
SELECT d.as_of,p.*,doc.source_sha256 doc_sha,
(doc.source_sha256=encode(sha256(convert_to(doc.content_text,'UTF8')),'hex') AND doc.content_bytes=octet_length(doc.content_text)) body_ok,
jsonb_array_length(doc.content_text::jsonb->'results') json_result_count,
lead(p.request_url) OVER(PARTITION BY d.as_of ORDER BY p.page_number) following_request,
max(p.page_number) OVER(PARTITION BY d.as_of) final_page
FROM dates d JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.payload->>'request_key'=d.request_key AND t.stage='ACQUIRE'
JOIN population_top3000_202608_202609_v1.ref01_date_page_v4 p ON p.acquire_task_id=t.task_id
JOIN population_top3000_202608_202609_v1.source_document doc USING(source_id)
), norm AS MATERIALIZED (
SELECT p.source_id,count(*) rows,count(*) FILTER(WHERE r.record_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.content::text,'UTF8')),'hex')) bad_hash,
count(*) FILTER(WHERE r.content->>'source_sha256' IS DISTINCT FROM p.doc_sha OR r.content->>'source_id' IS DISTINCT FROM r.source_id OR r.content->>'source_ordinal' IS DISTINCT FROM r.ordinal::text OR r.content->>'point_date' IS DISTINCT FROM p.as_of::text OR r.point_date IS DISTINCT FROM p.as_of) bad_link,
encode(sha256(convert_to(string_agg(r.record_sha256,E'\n' ORDER BY ordinal),'UTF8')),'hex') manifest_sha256
FROM pages p JOIN population_top3000_202608_202609_v1.ref01_date_record_v4 r USING(source_id) GROUP BY p.source_id
), der AS MATERIALIZED (
SELECT p.as_of,p.source_id,d.company_id,d.ticker,d.record_sha256,
(d.record_sha256=encode(sha256(convert_to(d.content::text,'UTF8')),'hex')) hash_ok,
(d.content->>'evidence_semantics'='STRONG_IDENTIFIER_MATCHED_VENDOR_DATE_POINT_NOT_INTERVAL' AND d.content->>'full_window_complete'='false' AND d.content->>'family_accepted'='false' AND d.content->>'original_historical_receipt'='false') semantics_ok
FROM pages p JOIN population_top3000_202608_202609_v1.ref01_practical_dated_instrument_v3 d ON d.source_set_sha256=p.source_sha256 AND d.cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'
), ddates AS (
SELECT as_of,count(*) dated_rows,count(DISTINCT company_id) selected_companies,count(*) FILTER(WHERE hash_ok IS DISTINCT FROM true) hash_mismatches,count(*) FILTER(WHERE semantics_ok IS DISTINCT FROM true) semantics_mismatches,
encode(sha256(convert_to(string_agg(record_sha256,E'\n' ORDER BY source_id,company_id,ticker),'UTF8')),'hex') manifest_sha256 FROM der GROUP BY as_of
), pdates AS (
SELECT p.as_of,count(*) pages,min(page_number) first_page,max(page_number) last_page,sum(result_count) raw_records,sum(n.rows) normalized_records,
count(*) FILTER(WHERE body_ok IS DISTINCT FROM true OR doc_sha IS DISTINCT FROM source_sha256) source_body_mismatches,
count(*) FILTER(WHERE json_result_count IS DISTINCT FROM result_count OR normalized_at IS NULL OR normalized_source_records IS DISTINCT FROM result_count OR coalesce(n.rows,0)<>result_count) count_mismatches,
count(*) FILTER(WHERE page_number=final_page AND next_url IS NOT NULL OR page_number<final_page AND next_url IS NULL) terminal_chain_mismatches,
count(*) FILTER(WHERE page_number<final_page AND next_url IS DISTINCT FROM following_request) exact_url_string_mismatches,
sum(n.bad_hash) normalized_hash_mismatches,sum(n.bad_link) normalized_source_link_mismatches,
encode(sha256(convert_to(string_agg(source_sha256||':'||n.manifest_sha256,E'\n' ORDER BY page_number),'UTF8')),'hex') page_record_manifest_sha256
FROM pages p LEFT JOIN norm n USING(source_id) GROUP BY p.as_of
)
SELECT jsonb_build_object('cutoff','2026-09-06','date_count',(SELECT count(*) FROM dates),'page_set_sha256',(SELECT encode(sha256(convert_to(string_agg(source_id||':'||source_sha256||':'||page_number,E'\n' ORDER BY as_of,page_number),'UTF8')),'hex') FROM pages),
'dates',(SELECT jsonb_agg(to_jsonb(p)||to_jsonb(d)||jsonb_build_object('missing_selected_point_companies',3000-selected_companies) ORDER BY p.as_of) FROM pdates p JOIN ddates d USING(as_of)),
'wire_archive_bytes_rehashed',false,'full_period_qualified',false,'intervals_qualified',false,'family_accepted',false) audit;

WITH dates AS MATERIALIZED(SELECT as_of FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9 WHERE plan_id='ref01-date-fullperiod-v9' AND as_of<='2026-09-06'),
anchors AS MATERIALIZED(SELECT ticker,content,content->>'company_id' company_id FROM population_top3000_202608_202609_v1.ref01_practical_instrument_point_v3 WHERE cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' AND eligible AND point_date='2026-08-03'),
have AS MATERIALIZED(SELECT DISTINCT d.point_date,d.company_id FROM population_top3000_202608_202609_v1.ref01_practical_dated_instrument_v3 d JOIN dates ON dates.as_of=d.point_date WHERE d.cohort_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'),
missing AS MATERIALIZED(SELECT dates.as_of,a.* FROM dates CROSS JOIN anchors a LEFT JOIN have h ON h.point_date=dates.as_of AND h.company_id=a.company_id WHERE h.company_id IS NULL),
raw AS MATERIALIZED(SELECT r.point_date,r.content->'source_record' rec FROM population_top3000_202608_202609_v1.ref01_date_record_v4 r JOIN dates ON dates.as_of=r.point_date WHERE r.content#>>'{source_record,ticker}' IN(SELECT ticker FROM missing)),
cases AS (SELECT m.as_of,m.company_id,m.ticker,count(r.rec) candidates,
bool_or(r.rec->>'composite_figi' IS DISTINCT FROM m.content#>>'{source_record,composite_figi}') composite_mismatch,
bool_or(r.rec->>'share_class_figi' IS DISTINCT FROM m.content#>>'{source_record,share_class_figi}') share_class_mismatch,
bool_or(r.rec->>'cik' IS DISTINCT FROM m.content#>>'{source_record,cik}') cik_mismatch,
bool_or(r.rec->>'type' IS DISTINCT FROM m.content#>>'{source_record,type}') type_mismatch,
bool_or(r.rec->>'primary_exchange' IS DISTINCT FROM m.content#>>'{source_record,primary_exchange}') exchange_mismatch
FROM missing m LEFT JOIN raw r ON r.point_date=m.as_of AND r.rec->>'ticker'=m.ticker GROUP BY m.as_of,m.company_id,m.ticker)
SELECT jsonb_build_object('missing_company_dates',count(*),'distinct_missing_companies',count(DISTINCT company_id),'no_retained_anchor_ticker',count(*) FILTER(WHERE candidates=0),'one_retained_ticker_unqualified',count(*) FILTER(WHERE candidates=1),'multiple_retained_ticker_rows',count(*) FILTER(WHERE candidates>1),'present_composite_figi_mismatch',count(*) FILTER(WHERE candidates>0 AND composite_mismatch),'present_share_class_figi_mismatch',count(*) FILTER(WHERE candidates>0 AND share_class_mismatch),'present_cik_mismatch',count(*) FILTER(WHERE candidates>0 AND cik_mismatch),'present_type_mismatch',count(*) FILTER(WHERE candidates>0 AND type_mismatch),'present_exchange_mismatch',count(*) FILTER(WHERE candidates>0 AND exchange_mismatch),'case_set_sha256',encode(sha256(convert_to(string_agg(company_id||':'||as_of||':'||candidates,E'\n' ORDER BY company_id,as_of),'UTF8')),'hex'),'absence_does_not_mean_inactive',true,'no_reclassification',true) gap_audit FROM cases;