BEGIN READ ONLY;
SET LOCAL statement_timeout='8s';
WITH wanted(name) AS (VALUES
 ('reference.alpaca_fixed_window_instruments_v1'),
 ('reference.alpaca_fixed_window_instruments_remediated_v1'),
 ('reference.equity_security_state_v1'),
 ('reference.equity_identity_day_overrides_remediation_v1'),
 ('market_factors_20250901_20260731_v1.precursor_identity_day_reconciliation_v8'),
 ('market_factors_20250901_20260731_v1.security_daily_full_v2'),
 ('market_factors_20250901_20260731_v1.security_daily_calendar_patch_v8')),
 metadata AS (SELECT w.name,c.oid,c.relkind,
 (SELECT jsonb_agg(jsonb_build_object('column',a.attname,'type',format_type(a.atttypid,a.atttypmod),'not_null',a.attnotnull)ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped)columns,
 (SELECT jsonb_agg(jsonb_build_object('name',i.indexrelid::regclass::text,'definition',pg_get_indexdef(i.indexrelid),'ready',i.indisready,'valid',i.indisvalid)) FROM pg_index i WHERE i.indrelid=c.oid)indexes
 FROM wanted w LEFT JOIN pg_class c ON c.oid=to_regclass(w.name)),
 sample_keys AS MATERIALIZED (SELECT instrument_key FROM reference.alpaca_fixed_window_instruments_v1 WHERE symbol IN('AAPL','MSFT','NVDA') ORDER BY instrument_key LIMIT 6)
SELECT jsonb_build_object('revision','DAILY_SHARED_IDENTITY_SOURCE_READ_V1','observed_at',clock_timestamp(),
 'metadata',(SELECT jsonb_agg(to_jsonb(m)) FROM metadata m),
 'fixed_instruments',(SELECT jsonb_agg(to_jsonb(q)) FROM(SELECT * FROM reference.alpaca_fixed_window_instruments_v1 WHERE instrument_key IN(SELECT instrument_key FROM sample_keys) LIMIT 6)q),
 'security_states',(SELECT jsonb_agg(to_jsonb(q)) FROM(SELECT * FROM reference.equity_security_state_v1 WHERE instrument_key IN(SELECT instrument_key FROM sample_keys) AND valid_from<=date '2026-09-30' AND valid_to>date '2026-05-05' ORDER BY instrument_key,valid_from LIMIT 30)q),
 'identity_overrides',(SELECT jsonb_agg(to_jsonb(q)) FROM(SELECT * FROM reference.equity_identity_day_overrides_remediation_v1 WHERE instrument_key IN(SELECT instrument_key FROM sample_keys) AND session_date IN(date '2026-07-31',date '2026-08-03',date '2026-09-30') ORDER BY instrument_key,session_date LIMIT 9)q),
 'identity_reconciliation',(SELECT jsonb_agg(to_jsonb(q)) FROM(SELECT * FROM market_factors_20250901_20260731_v1.precursor_identity_day_reconciliation_v8 WHERE instrument_key IN(SELECT instrument_key FROM sample_keys) AND session_date IN(date '2026-07-31',date '2026-08-03',date '2026-09-30') ORDER BY instrument_key,session_date LIMIT 9)q),
 'retained_support',(SELECT jsonb_agg(to_jsonb(q)) FROM(SELECT * FROM market_factors_20250901_20260731_v1.security_daily_features_remediated_v1 WHERE instrument_key IN(SELECT instrument_key FROM sample_keys) AND session_date IN(date '2026-05-05',date '2026-07-31',date '2026-08-03',date '2026-09-30') ORDER BY instrument_key,session_date LIMIT 12)q),
 'provider_called',false,'source_rows_scanned','BOUNDED_THREE_SYMBOL_SAMPLES_ONLY','population_completeness_claimed',false)daily_shared_identity_source;
COMMIT;
