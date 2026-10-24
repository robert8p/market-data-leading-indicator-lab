-- Private source-occurrence view. Calendar classifications do not establish expected prints or company membership.
CREATE OR REPLACE VIEW population_top3000_202608_202609_v1.massive_minute_session_candidate_v2
WITH (security_invoker=true) AS
SELECT 'Massive'::text source_provider,t.payload->>'source_symbol' source_symbol,(t.payload->>'instrument_key')::integer candidate_instrument_key,
 (t.payload->>'trading_date')::date source_date,p.archive_key,p.page_number,b.*,
 CASE WHEN b.provider_ny_date IS DISTINCT FROM (t.payload->>'trading_date')::date THEN 'outside_regular_extended_or_missing_timestamp'
 WHEN b.bar_start_at>=cal.regular_open_utc AND b.bar_start_at<cal.regular_close_utc THEN 'regular'
 WHEN (b.bar_start_at AT TIME ZONE 'America/New_York')::time>=time '04:00' AND b.bar_start_at<cal.regular_open_utc THEN 'premarket'
 WHEN b.bar_start_at>=cal.regular_close_utc AND (b.bar_start_at AT TIME ZONE 'America/New_York')::time<time '20:00' THEN 'postmarket'
 ELSE 'outside_regular_extended_or_missing_timestamp' END session_label,
 p.source_id,p.source_sha256,p.wire_sha256,p.retrieved_at,p.verified_at source_reconciled_at,
 cal.source_record_sha256 calendar_record_sha256,cal.source_receipt_sha256 calendar_receipt_sha256,
 NULL::timestamptz source_published_at,NULL::timestamptz historical_received_at,
 'AGGREGATE_WINDOW_START_NOT_PUBLICATION_TIME'::text timestamp_role,
 false company_population_verified,false independent_price_accuracy_verified,false publication_eligible,
 false missing_minutes_filled,'SOURCE_OCCURRENCES_NOT_EXPECTED_PRINT_DENOMINATOR'::text applicability
FROM population_top3000_202608_202609_v1.massive_minute_bar_v2 b
JOIN population_top3000_202608_202609_v1.massive_minute_page_v2 p USING(page_id)
JOIN population_top3000_202608_202609_v1.pipeline_task t ON t.task_id=p.acquire_task_id
JOIN population_top3000_202608_202609_v1.source_document sd ON sd.source_id=p.source_id
JOIN (VALUES('AAPL',41),('GOOG',5083),('GOOGL',5084),('NVDA',8372),('WEAV',12595)) known(symbol,instrument_key)
 ON t.payload->>'source_symbol'=known.symbol AND t.payload->>'instrument_key'=known.instrument_key::text
JOIN population_top3000_202608_202609_v1.catalog_calendar cal ON cal.scope_id='corrected_company_scope_8e6ca1178f03822c' AND cal.session_date=(t.payload->>'trading_date')::date
WHERE t.family_id='P01' AND t.stage='ACQUIRE' AND t.payload->>'kind'='MASSIVE_MINUTE_KNOWN5_V2'
AND t.payload->>'trading_date'='2026-09-09' AND t.partition_key='massive-minute-'||known.symbol||'-20260909-v2'
AND p.input_versions=t.input_versions AND sd.source_provider='Massive' AND sd.source_sha256=p.source_sha256
AND sd.source_uri=p.request_url AND sd.evidence->>'source_symbol'=known.symbol AND sd.evidence->>'trading_date'='2026-09-09'
AND sd.evidence->>'wire_sha256'=p.wire_sha256 AND sd.content_bytes=p.response_bytes
AND sd.source_published_at IS NULL AND sd.historical_received_at IS NULL
AND cal.qualification='PROVIDER_AND_NYSE_RULES_RECONCILED'
AND cal.source_record_sha256='5b2c188c23926391b2a9ed897bd796fb531f52ae13b98bb6735016fae8cb9dc7'
AND cal.source_receipt_sha256='f1d4b274a5ed1fe8507cbfe8fa08987fed4ac44e8a5602c82693067de01454c2'
AND cal.source_record_sha256=encode(sha256(convert_to(cal.source_record_text,'UTF8')),'hex')
AND cal.source_receipt_sha256=encode(sha256(convert_to(cal.source_receipt_text,'UTF8')),'hex');
REVOKE ALL ON population_top3000_202608_202609_v1.massive_minute_session_candidate_v2 FROM PUBLIC,anon,authenticated;
