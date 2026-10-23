-- Read-only, bounded review after root's first real page. Does not declare accuracy PASS.
BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';
SET LOCAL lock_timeout='2s';
WITH pages AS MATERIALIZED (
 SELECT p.*,d.content_text,d.content_bytes,d.source_sha256 document_sha256,d.source_uri,d.evidence
 FROM population_top3000_202608_202609_v1.massive_archive_page_v1 p
 JOIN population_top3000_202608_202609_v1.source_document d USING(source_id)
 WHERE p.family_id='N03' AND p.partition_key='splits-execution-202608-202609'
 ORDER BY p.page_number LIMIT 1
), rows AS MATERIALIZED (
 SELECT p.source_id,x.ordinality source_row,x.value event
 FROM pages p CROSS JOIN LATERAL jsonb_array_elements(p.content_text::jsonb->'results') WITH ORDINALITY x(value,ordinality)
), shape AS (
 SELECT count(*) actual_rows,
 count(*) FILTER(WHERE nullif(event->>'id','') IS NULL) missing_ids,
 count(*) FILTER(WHERE nullif(event->>'ticker','') IS NULL) missing_tickers,
 count(*) FILTER(WHERE population_top3000_202608_202609_v1.massive_actions_date_v1(event->>'execution_date') IS NULL) missing_or_invalid_dates,
 count(*) FILTER(WHERE population_top3000_202608_202609_v1.massive_actions_date_v1(event->>'execution_date') NOT BETWEEN date '2026-08-01' AND date '2026-09-30') out_of_window_dates,
 count(*) FILTER(WHERE event->>'split_from' IS NULL OR event->>'split_to' IS NULL) missing_ratios,
 count(*) FILTER(WHERE event->>'adjustment_type' IS NULL OR event->>'adjustment_type' NOT IN('forward_split','reverse_split','stock_dividend')) missing_or_unknown_type,
 count(*)-count(DISTINCT encode(sha256(convert_to(event::text,'UTF8')),'hex')) identical_duplicate_records,
 count(event->>'id')-count(DISTINCT event->>'id') repeated_ids
 FROM rows
)
SELECT p.source_id,p.page_number,p.result_count,p.request_url,p.next_url IS NOT NULL has_next_page,
p.source_sha256,p.wire_sha256,p.wire_bytes,p.content_bytes,
p.document_sha256=p.source_sha256 AND p.source_sha256=encode(sha256(convert_to(p.content_text,'UTF8')),'hex') stored_hash_matches,
p.content_bytes=octet_length(p.content_text) stored_bytes_match,
p.source_uri=p.request_url request_lineage_matches,
p.evidence->>'wire_sha256'=p.wire_sha256 original_body_digest_matches_manifest,
p.content_text !~* '[?&](apikey|api_key|access_token)=' credential_query_fields_absent,
s.*,false independent_provider_accuracy_verified,false top3000_company_membership_verified
FROM pages p CROSS JOIN shape s;
SELECT task_id,stage,status,blocker_code,checkpoint,resumption_condition
FROM population_top3000_202608_202609_v1.pipeline_task
WHERE family_id='N03' ORDER BY created_at LIMIT 10;
ROLLBACK;
