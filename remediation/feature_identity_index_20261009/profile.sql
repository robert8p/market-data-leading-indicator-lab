BEGIN READ ONLY; SET LOCAL work_mem='32MB'; SET LOCAL statement_timeout='45s'; SET LOCAL TimeZone='UTC'; EXPLAIN(ANALYZE,BUFFERS,VERBOSE,FORMAT JSON) SELECT f.*,i.identity_contract_sha256,
 astra_shared_equity_features_20260928_v1.compact_output_hash_remediated_v1(f.source_rows_sha256,i.identity_contract_sha256,f.minute_indices,f.validity_codes,f.dependency_source_observed_at,f.coefficient_values,f.retained_source_rows,f.unaligned_source_rows,f.invalid_ohlc_rows,f.invalid_volume_rows),statement_timestamp()
FROM(WITH scope AS(SELECT u.instrument_key,c.session_date,c.regular_open,c.regular_close FROM reference.alpaca_fixed_window_instruments_v1 u CROSS JOIN market_governance.us_equity_fixed_session_calendar c WHERE u.instrument_key BETWEEN 6039 AND 6050 AND c.session_date BETWEEN DATE '2026-05-01' AND DATE '2026-05-29' AND 6039>=1 AND 6050<=13277 AND 6050>=6039 AND 6050-6039<=99 AND DATE '2026-05-01'>='2025-09-01' AND DATE '2026-05-29'<'2026-08-01' AND DATE '2026-05-29'>=DATE '2026-05-01' AND DATE '2026-05-29'-DATE '2026-05-01'<=31 AND (NOT false OR NOT EXISTS(SELECT 1 FROM astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1 z WHERE z.instrument_key=u.instrument_key AND z.session_date=c.session_date))), source_all AS MATERIALIZED(
 SELECT s.instrument_key,s.session_date,s.regular_open,s.regular_close,
 b.bar_ts,b.open,b.high,b.low,b.close,b.volume,b.trade_count,b.vwap,b.source_loaded_at,b.loaded_by_job_id,
 (extract(epoch FROM b.bar_ts-s.regular_open)/60)::smallint minute_index,
 b.bar_ts=date_trunc('minute',b.bar_ts)clock_aligned,
 sha256(convert_to(jsonb_build_array(b.instrument_key,b.bar_ts,b.open,b.high,b.low,b.close,b.volume,b.trade_count,b.vwap,b.source_loaded_at,b.loaded_by_job_id)::text,'UTF8'))row_sha256
 FROM scope s JOIN market_fixed_20250901_20260831.alpaca_sip_bars_1m b
 ON b.instrument_key=s.instrument_key AND b.bar_ts>=s.regular_open AND b.bar_ts<s.regular_close
), validated AS MATERIALIZED(
 SELECT *,coalesce(open>0 AND high>0 AND low>0 AND close>0
 AND market_factors_20250901_20260731_v1.finite_float_remediated_v1(open)
 AND market_factors_20250901_20260731_v1.finite_float_remediated_v1(high)
 AND market_factors_20250901_20260731_v1.finite_float_remediated_v1(low)
 AND market_factors_20250901_20260731_v1.finite_float_remediated_v1(close)
 AND high>=greatest(open,close,low)AND low<=least(open,close,high),false)valid_bar,
 coalesce(volume>=0,false)valid_volume_flag,
 lag(minute_index)OVER(PARTITION BY instrument_key,session_date ORDER BY minute_index)prior_minute
 FROM source_all WHERE clock_aligned
), base AS MATERIALIZED(
 SELECT *,CASE WHEN valid_bar THEN close END valid_close,
 CASE WHEN valid_volume_flag THEN volume::float8 END valid_volume,
 CASE WHEN valid_bar AND vwap>0 AND market_factors_20250901_20260731_v1.finite_float_remediated_v1(vwap)THEN vwap END valid_vwap,
 greatest(coalesce(max(minute_index)FILTER(WHERE NOT valid_bar)OVER wu,-1),coalesce(max(minute_index-1)FILTER(WHERE prior_minute IS NULL OR minute_index<>prior_minute+1)OVER wu,-1))last_bad_price,
 greatest(coalesce(max(minute_index)FILTER(WHERE NOT valid_volume_flag)OVER wu,-1),coalesce(max(minute_index-1)FILTER(WHERE prior_minute IS NULL OR minute_index<>prior_minute+1)OVER wu,-1))last_bad_volume
 FROM validated WINDOW wu AS(PARTITION BY instrument_key,session_date ORDER BY minute_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), lagged AS MATERIALIZED(
 SELECT *,minute_index-last_bad_price consecutive_valid_prices,minute_index-last_bad_volume consecutive_valid_volumes,
 lag(valid_close,1)OVER wi lag1,lag(valid_close,5)OVER wi lag5,lag(valid_close,10)OVER wi lag10,
 lag(valid_close,20)OVER wi lag20,lag(valid_close,60)OVER wi lag60,lag(valid_close,120)OVER wi lag120,
 avg(valid_close)OVER w4 sma5,avg(valid_close)OVER w19 sma20,avg(valid_close)OVER w59 sma60,
 avg(valid_volume)OVER w19 vmean20,stddev_samp(valid_volume)OVER w19 vsd20,
 avg(valid_close*valid_volume)OVER w19 dvmean20,
 max(high)FILTER(WHERE valid_bar)OVER w19 hi20,min(low)FILTER(WHERE valid_bar)OVER w19 lo20,
 sum(coalesce(valid_vwap,valid_close)*valid_volume)FILTER(WHERE valid_volume>0 AND coalesce(valid_vwap,valid_close)>0)OVER wu cum_pv,
 sum(valid_volume)FILTER(WHERE valid_volume>0 AND coalesce(valid_vwap,valid_close)>0)OVER wu cum_volume,
 bool_or(valid_vwap IS NULL AND valid_close IS NOT NULL AND valid_volume>0)OVER wu session_vwap_uses_close_proxy,
 max(source_loaded_at)OVER wu dependency_source_observed_at,
 count(source_loaded_at)OVER wu=count(*)OVER wu dependency_receipts_complete,
 count(*)FILTER(WHERE valid_bar)OVER wu=minute_index+1 session_clock_coverage_complete
 FROM base
 WINDOW wi AS(PARTITION BY instrument_key,session_date ORDER BY minute_index),
 w4 AS(PARTITION BY instrument_key,session_date ORDER BY minute_index ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
 w19 AS(PARTITION BY instrument_key,session_date ORDER BY minute_index ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
 w59 AS(PARTITION BY instrument_key,session_date ORDER BY minute_index ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
 wu AS(PARTITION BY instrument_key,session_date ORDER BY minute_index ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), returns AS(
 SELECT *,CASE WHEN consecutive_valid_prices>=2 AND lag1>0 THEN ln(valid_close/lag1)END logret1 FROM lagged
), rolled AS(
 SELECT *,stddev_samp(logret1)OVER w4 rv5,stddev_samp(logret1)OVER w19 rv20,stddev_samp(logret1)OVER w59 rv60
 FROM returns WINDOW
 w4 AS(PARTITION BY instrument_key,session_date ORDER BY minute_index ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
 w19 AS(PARTITION BY instrument_key,session_date ORDER BY minute_index ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
 w59 AS(PARTITION BY instrument_key,session_date ORDER BY minute_index ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)
), coefficients AS(
 SELECT instrument_key,session_date,minute_index,dependency_source_observed_at,
 ((CASE WHEN valid_bar THEN 1 ELSE 0 END)+(CASE WHEN valid_volume IS NOT NULL THEN 2 ELSE 0 END)+
 (CASE WHEN valid_vwap IS NOT NULL THEN 4 ELSE 0 END)+(CASE WHEN session_vwap_uses_close_proxy THEN 8 ELSE 0 END)+
 (CASE WHEN dependency_receipts_complete THEN 16 ELSE 0 END)+(CASE WHEN session_clock_coverage_complete THEN 32 ELSE 0 END))::smallint validity_code,
 ARRAY[
 CASE WHEN consecutive_valid_prices>=2 AND lag1>0 THEN valid_close/lag1-1 END,
 CASE WHEN consecutive_valid_prices>=6 AND lag5>0 THEN valid_close/lag5-1 END,
 CASE WHEN consecutive_valid_prices>=11 AND lag10>0 THEN valid_close/lag10-1 END,
 CASE WHEN consecutive_valid_prices>=21 AND lag20>0 THEN valid_close/lag20-1 END,
 CASE WHEN consecutive_valid_prices>=61 AND lag60>0 THEN valid_close/lag60-1 END,
 CASE WHEN consecutive_valid_prices>=121 AND lag120>0 THEN valid_close/lag120-1 END,
 CASE WHEN consecutive_valid_prices>=6 THEN rv5 END,CASE WHEN consecutive_valid_prices>=21 THEN rv20 END,CASE WHEN consecutive_valid_prices>=61 THEN rv60 END,
 CASE WHEN consecutive_valid_prices>=20 AND lo20>0 THEN hi20/lo20-1 END,
 CASE WHEN consecutive_valid_volumes>=20 THEN vmean20 END,CASE WHEN consecutive_valid_volumes>=20 THEN vsd20 END,
 CASE WHEN least(consecutive_valid_prices,consecutive_valid_volumes)>=20 THEN dvmean20 END,
 CASE WHEN consecutive_valid_prices>=5 THEN sma5 END,CASE WHEN consecutive_valid_prices>=20 THEN sma20 END,CASE WHEN consecutive_valid_prices>=60 THEN sma60 END,
 CASE WHEN cum_volume>0 THEN cum_pv/cum_volume END]::float8[] coefficient_values
 FROM rolled
), packed AS(
 SELECT instrument_key,session_date,count(*)::integer aligned_source_rows,
 array_agg(minute_index ORDER BY minute_index)minute_indices,
 array_agg(validity_code ORDER BY minute_index)validity_codes,
 array_agg(dependency_source_observed_at ORDER BY minute_index)dependency_source_observed_at,
 array_agg(coefficient_values ORDER BY minute_index)coefficient_values,
 count(*)FILTER(WHERE(validity_code&1)=0)::integer invalid_ohlc_rows,
 count(*)FILTER(WHERE(validity_code&2)=0)::integer invalid_volume_rows
 FROM coefficients GROUP BY instrument_key,session_date
), source_summary AS(
 SELECT instrument_key,session_date,count(*)::integer retained_source_rows,
 count(*)FILTER(WHERE NOT clock_aligned)::integer unaligned_source_rows,
 min(source_loaded_at)first_source_acquisition,max(source_loaded_at)latest_source_acquisition,
 sha256(string_agg(row_sha256,''::bytea ORDER BY bar_ts))source_rows_sha256
 FROM source_all GROUP BY instrument_key,session_date
)
SELECT s.instrument_key,s.session_date,s.regular_open,s.regular_close,
 (extract(epoch FROM s.regular_close-s.regular_open)/60)::smallint expected_grid_rows,
 coalesce(t.retained_source_rows,0)retained_source_rows,coalesce(t.unaligned_source_rows,0)unaligned_source_rows,
 coalesce(p.aligned_source_rows,0)aligned_source_rows,
 coalesce(p.minute_indices,'{}'::smallint[])minute_indices,coalesce(p.validity_codes,'{}'::smallint[])validity_codes,
 coalesce(p.dependency_source_observed_at,'{}'::timestamptz[])dependency_source_observed_at,
 coalesce(p.coefficient_values,'{}'::float8[])coefficient_values,
 coalesce(p.invalid_ohlc_rows,0)invalid_ohlc_rows,coalesce(p.invalid_volume_rows,0)invalid_volume_rows,
 t.first_source_acquisition,t.latest_source_acquisition,
 coalesce(t.source_rows_sha256,sha256(''::bytea))source_rows_sha256
FROM scope s LEFT JOIN packed p USING(instrument_key,session_date)LEFT JOIN source_summary t USING(instrument_key,session_date)
)f JOIN astra_shared_equity_features_20260928_v1.shared_identity_contract_remediated_v1 i USING(instrument_key,session_date); ROLLBACK;
