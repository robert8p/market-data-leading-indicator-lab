-- Tested fragment in candidate event 94953; not a standalone migration.
-- Complete candidate SQL is immutably retained in pipeline_event.details.candidate_sql.
-- Candidate source SHA256: 2ea06221f85c42d06640fe948321525f742becba1060e214a1fec8eb0bab1a9f
-- pg_get_functiondef SHA256: 223bdc2dbfbce5ef188cc4b04edb9b485e726d10a2ac513566cd145380abd73b
-- The manifest lookup additionally uses FOR UPDATE to serialize the same manifest.
-- Insert these checks after output/expectation insertion, before COMPLETE is returned.
-- No production activation or independent approval is represented by this file.

-- Retry correctness: compare complete stored values and provenance, not just hashes.
IF EXISTS(
 WITH expected(manifest_id,bar_ts,company_id,source_symbol,instrument_key,session_date,session_label,open,high,low,close,volume,trade_count,vwap,source_row_sha256,source_manifest_sha256,identity_record_sha256,normalization_sha256,lineage) AS MATERIALIZED (
SELECT m.manifest_id,b.bar_ts,d.company_id,k.symbol,b.instrument_key,m.session_date,m.session_label,b.open,b.high,b.low,b.close,b.volume,b.trade_count,b.vwap,
encode(sha256(convert_to(jsonb_build_array(b.instrument_key,extract(epoch FROM b.bar_ts),b.session_label::text,encode(float8send(b.open),'hex'),encode(float8send(b.high),'hex'),encode(float8send(b.low),'hex'),encode(float8send(b.close),'hex'),b.volume,b.trade_count,encode(float8send(b.vwap),'hex'),b.loaded_by_job_id,extract(epoch FROM b.source_loaded_at))::text,'UTF8')),'hex'),
m.manifest_sha256,encode(d.identity_record_sha256,'hex'),nh,jsonb_build_object('loader_sha256',m.lineage->>'loader_definition_sha256','source_symbol_basis','PRESERVED_LOADER_JOIN_ON_SYMBOL','identity_source_sha256',encode(d.identity_source_sha256,'hex'),'identity_source_ordinal',d.identity_source_ordinal,'source_loaded_at',b.source_loaded_at,'source_job_id',b.loaded_by_job_id,'source_content_sha256',h,'temporal_level','RETAINED_ARCHIVE_DATED_IDENTITY','original_historical_receipt_verified',false,'independent_economic_accuracy_verified',false,'family_accepted',false)
FROM market_fixed_20250901_20260831.alpaca_sip_bars_1m_202608 b WHERE b.instrument_key=m.instrument_key AND b.bar_ts>=lo AND b.bar_ts<hi AND b.session_label::text=m.session_label
 ), stored AS (SELECT * FROM population_top3000_202608_202609_v1.retained_price_observation_v2 WHERE manifest_id=m.manifest_id)
 SELECT 1 FROM expected e FULL JOIN stored o USING(manifest_id,bar_ts)
 WHERE to_jsonb(o) IS DISTINCT FROM to_jsonb(e)
 OR float8send(o.open) IS DISTINCT FROM float8send(e.open)
 OR float8send(o.high) IS DISTINCT FROM float8send(e.high)
 OR float8send(o.low) IS DISTINCT FROM float8send(e.low)
 OR float8send(o.close) IS DISTINCT FROM float8send(e.close)
 OR float8send(o.vwap) IS DISTINCT FROM float8send(e.vwap)
) THEN RAISE EXCEPTION 'NORMALIZED_OUTPUT_CONFLICT'; END IF;

-- Compare the whole expectation record, including an explicitly unknown minute count.
IF NOT EXISTS(
 SELECT 1 FROM population_top3000_202608_202609_v1.retained_price_expectation_v2 actual
 CROSS JOIN (VALUES(m.manifest_id,d.company_id,m.session_date,m.session_label,1,NULL,n,'["open","high","low","close","volume","trade_count","vwap"]'::jsonb,jsonb_build_object('expected_unit','SELECTED_COMPANY_DATED_SESSION_PARTITION','observed_bar_count_is_not_expected_count',true,'source_minute_completeness','UNRESOLVED_PROVIDER_EVENT_DENOMINATOR','dense_clock_denominator_used',false,'missing_minutes_not_zero_or_no_trade',true,'calendar_record_sha256',cal.source_record_sha256,'dated_identity_record_sha256',encode(d.identity_record_sha256,'hex'),'family_accepted',false),nh)) expected(manifest_id,company_id,session_date,session_label,expected_partition_units,expected_minute_bars,observed_bars,required_fields,qualification,normalization_sha256)
 WHERE actual.manifest_id=m.manifest_id AND to_jsonb(actual) IS NOT DISTINCT FROM to_jsonb(expected)
) THEN RAISE EXCEPTION 'NORMALIZATION_EXPECTATION_CONFLICT'; END IF;
