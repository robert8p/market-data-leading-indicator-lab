BEGIN READ ONLY;
SET LOCAL statement_timeout='10s';SET LOCAL lock_timeout='2s';
WITH pages AS MATERIALIZED (
 SELECT p.*,d.content_text,d.content_bytes,d.source_sha256 document_sha256,d.source_uri
 FROM population_top3000_202608_202609_v1.massive_dividend_page_v1 p
 JOIN population_top3000_202608_202609_v1.source_document d USING(source_id)
 WHERE p.page_number=1 ORDER BY p.partition_key LIMIT 2
), rows AS MATERIALIZED (
 SELECT p.partition_key,x.value event FROM pages p CROSS JOIN LATERAL jsonb_array_elements(p.content_text::jsonb->'results') x(value)
), shape AS (
 SELECT partition_key,count(*) actual_rows,
 count(*) FILTER(WHERE nullif(event->>'id','') IS NULL) missing_ids,
 count(*) FILTER(WHERE event->>'cash_amount' IS NULL) missing_cash_amount,
 count(*) FILTER(WHERE event->>'currency' IS NULL) missing_currency,
 count(*) FILTER(WHERE event->>'declaration_date' IS NULL) missing_declaration_date,
 count(*) FILTER(WHERE event->>'ex_dividend_date' IS NULL) missing_ex_date,
 count(*) FILTER(WHERE CASE WHEN partition_key='dividends-exdate-202608-202609' THEN population_top3000_202608_202609_v1.massive_actions_date_v1(event->>'ex_dividend_date') ELSE population_top3000_202608_202609_v1.massive_actions_date_v1(event->>'declaration_date') END NOT BETWEEN date '2026-08-01' AND date '2026-09-30') out_of_query_dates,
 count(*)-count(DISTINCT encode(sha256(convert_to(event::text,'UTF8')),'hex')) repeated_identical_records
 FROM rows GROUP BY partition_key
)
SELECT p.partition_key,p.source_id,p.request_url,p.next_url IS NOT NULL has_next_page,p.result_count,
p.source_sha256,p.wire_sha256,p.content_bytes,p.wire_bytes,
p.document_sha256=p.source_sha256 AND p.source_sha256=encode(sha256(convert_to(p.content_text,'UTF8')),'hex') stored_hash_matches,
p.content_bytes=octet_length(p.content_text) stored_bytes_match,p.source_uri=p.request_url request_lineage_matches,
p.content_text !~* '[?&](apikey|api_key|access_token)=' credential_query_fields_absent,
s.*,false independent_provider_accuracy_verified,false top3000_company_membership_verified
FROM pages p LEFT JOIN shape s USING(partition_key);
SELECT partition_key,count(*) observation_versions,
count(*) FILTER(WHERE field_state->>'provider_event_id'='CONFLICTING') conflicting_id_versions,
count(*) FILTER(WHERE available_at IS NOT NULL OR source_published_at IS NOT NULL OR historical_received_at IS NOT NULL) unexpected_availability_claims
FROM population_top3000_202608_202609_v1.source_observation WHERE family_id='N02' GROUP BY partition_key;
SELECT task_id,stage,partition_key,status,blocker_code,checkpoint,resumption_condition
FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='N02' ORDER BY created_at LIMIT 20;
ROLLBACK;
