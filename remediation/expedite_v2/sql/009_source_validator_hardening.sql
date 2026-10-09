CREATE OR REPLACE FUNCTION market_governance.remediation_validate_source_v2(p_job_key text,p_raw text)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' SET "TimeZone"='UTC' AS $$
DECLARE j market_governance.remediation_validation_job_v2%rowtype;b market_governance.remediation_source_batch_v1%rowtype;s market_governance.remediation_source_response_v1%rowtype;
 raw jsonb;n bigint;bad bigint;v jsonb;d record;o reference.massive_security_master_observation_v1%rowtype;c market_governance.us_equity_fixed_session_calendar%rowtype;
 eligible boolean;reason text;promoted bigint=0;unresolved int=0;final_status text;rec_count bigint;
BEGIN
 SELECT * INTO STRICT j FROM market_governance.remediation_validation_job_v2 WHERE job_key=p_job_key;
 SELECT * INTO STRICT b FROM market_governance.remediation_source_batch_v1 WHERE batch_id=j.batch_id;
 SELECT * INTO STRICT s FROM market_governance.remediation_source_response_v1 WHERE source_id=j.source_id;
 IF b.status IS DISTINCT FROM 'COMPLETE' OR b.current_source_id IS DISTINCT FROM j.source_id OR NOT market_governance.remediation_noncrypto_task_v1(to_jsonb(b)) OR s.batch_id IS DISTINCT FROM b.batch_id OR s.http_status IS DISTINCT FROM 200 OR s.credential_redactions IS DISTINCT FROM 0 OR s.source_sha256 IS DISTINCT FROM s.stored_body_sha256
 OR encode(s.source_sha256,'hex') IS DISTINCT FROM j.source_sha256 OR octet_length(p_raw) IS DISTINCT FROM s.original_bytes OR sha256(convert_to(p_raw,'UTF8')) IS DISTINCT FROM s.source_sha256 THEN RAISE EXCEPTION 'validation_source_receipt_mismatch';END IF;
 IF s.parser_version IS DISTINCT FROM b.request_json->>'required_parser_version' THEN RAISE EXCEPTION 'validation_parser_contract';END IF;
 raw=p_raw::jsonb;
 IF jsonb_typeof(raw) IS DISTINCT FROM 'object' OR coalesce(raw->>'status','') NOT IN('OK','DELAYED') OR jsonb_typeof(coalesce(raw->'results','[]'))<>'array' THEN RAISE EXCEPTION 'validation_native_shape';END IF;
 IF b.source_type='massive_reference_tickers' THEN
  n=jsonb_array_length(raw->'results');
  IF n IS DISTINCT FROM (b.validation->>'source_native_reference_rows')::bigint OR n IS DISTINCT FROM (raw->>'count')::bigint OR n>1000
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(raw->'results')x WHERE x->>'market' IS DISTINCT FROM 'stocks' OR x->>'active' IS DISTINCT FROM 'true' OR nullif(x->>'ticker','') IS NULL)
  OR (SELECT count(DISTINCT x->>'ticker') FROM jsonb_array_elements(raw->'results')x)<>n THEN RAISE EXCEPTION 'validation_reference_population_mismatch';END IF;
  IF raw->>'next_url' IS DISTINCT FROM b.validation->>'next_url' OR b.validation->>'source_reference_date' IS DISTINCT FROM b.request_json->>'reference_date' OR b.validation->>'normalization_passed' IS DISTINCT FROM 'true' THEN RAISE EXCEPTION 'reference_chain_evidence_mismatch';END IF;
  INSERT INTO reference.massive_warmup_listing_remediated_v2
  SELECT s.source_id,(b.request_json->>'reference_date')::date,x->>'ticker',x->>'composite_figi',x->>'share_class_figi',x->>'cik',x->>'type',x->>'primary_exchange',x,sha256(convert_to(x::text,'UTF8')),j.source_sha256,s.received_at FROM jsonb_array_elements(raw->'results')x;
  RETURN jsonb_build_object('status','VALIDATED','native_rows',n,'source_sha256',j.source_sha256,'dated_capture_release_pending',true,'page_number',b.request_json->'page_number','terminal_page',b.validation->'source_terminal_page','next_cursor_sha256',b.validation->'next_cursor_sha256','identity_mapping_complete',false,'historical_first_receipt_recovered',false);
 END IF;
 IF raw->>'ticker' IS DISTINCT FROM b.symbol OR raw->>'adjusted' IS DISTINCT FROM 'false' OR raw->>'next_url' IS NOT NULL THEN RAISE EXCEPTION 'validation_price_native_contract';END IF;
 n=jsonb_array_length(coalesce(raw->'results','[]'));
 IF n IS DISTINCT FROM b.valid_count OR coalesce(raw->>'resultsCount','-1')::bigint IS DISTINCT FROM n OR n>=50000 OR b.validation->>'normalization_passed' IS DISTINCT FROM 'true' OR (b.validation->>'invalid_count')::bigint IS DISTINCT FROM 0 THEN RAISE EXCEPTION 'validation_price_population';END IF;
 SELECT count(*) INTO rec_count FROM market_governance.remediation_observation_v1 WHERE source_id=s.source_id;
 IF rec_count<>n THEN RAISE EXCEPTION 'validation_typed_count';END IF;
 IF n>0 THEN
  WITH native AS(SELECT to_timestamp((x->>'t')::numeric/1000) ts,(x->>'o')::float8 o,(x->>'h')::float8 h,(x->>'l')::float8 l,(x->>'c')::float8 c,(x->>'v')::float8 v,(x->>'n')::bigint trades,(x->>'vw')::float8 vw FROM jsonb_array_elements(raw->'results')x)
  SELECT count(*) INTO bad FROM native x FULL JOIN (SELECT * FROM market_governance.remediation_observation_v1 WHERE source_id=s.source_id) y ON y.observed_at=x.ts
  WHERE x.ts IS NULL OR y.source_id IS NULL OR x.ts<b.start_ts OR x.ts>=b.end_ts OR x.ts<>date_trunc('minute',x.ts) OR y.bar_end<>x.ts+interval '1 minute'
  OR NOT market_factors_20250901_20260731_v1.finite_float_remediated_v1(x.o) OR NOT market_factors_20250901_20260731_v1.finite_float_remediated_v1(x.h) OR NOT market_factors_20250901_20260731_v1.finite_float_remediated_v1(x.l) OR NOT market_factors_20250901_20260731_v1.finite_float_remediated_v1(x.c) OR NOT market_factors_20250901_20260731_v1.finite_float_remediated_v1(x.v) OR x.o<=0 OR x.l<=0 OR x.c<=0 OR x.h<greatest(x.o,x.l,x.c) OR x.l>least(x.o,x.h,x.c) OR x.v<0
  OR x.o IS DISTINCT FROM y.open OR x.h IS DISTINCT FROM y.high OR x.l IS DISTINCT FROM y.low OR x.c IS DISTINCT FROM y.close OR x.v IS DISTINCT FROM y.volume OR x.trades IS DISTINCT FROM y.trade_count OR x.vw IS DISTINCT FROM y.vwap;
  IF bad<>0 OR (SELECT count(DISTINCT x->>'t') FROM jsonb_array_elements(raw->'results')x)<>n THEN RAISE EXCEPTION 'validation_native_typed_mismatch';END IF;
 END IF;
 IF jsonb_typeof(b.request_json->'affected_listing_days') IS DISTINCT FROM 'array' OR jsonb_array_length(b.request_json->'affected_listing_days')=0 THEN RAISE EXCEPTION 'listing_day_evidence_missing';END IF;
 FOR d IN SELECT value a FROM jsonb_array_elements(b.request_json->'affected_listing_days') LOOP
  SELECT * INTO STRICT o FROM reference.massive_security_master_observation_v1 WHERE observation_id=(d.a->>'observation_id')::bigint;
  SELECT * INTO STRICT c FROM market_governance.us_equity_fixed_session_calendar WHERE session_date=(d.a->>'date')::date;
  IF o.ticker IS DISTINCT FROM b.symbol OR o.requested_as_of IS DISTINCT FROM c.session_date OR o.row_hash IS DISTINCT FROM d.a->>'native_row_sha256' OR o.raw_payload#>>'{_remediation_acquisition,source_sha256}' IS DISTINCT FROM d.a->>'native_source_sha256' THEN RAISE EXCEPTION 'validation_listing_evidence_changed';END IF;
  SELECT count(*) INTO rec_count FROM market_governance.remediation_observation_v1 WHERE source_id=s.source_id AND observed_at>=c.regular_open AND observed_at<c.regular_close;
  eligible=o.instrument_key IS NOT NULL AND o.instrument_key=(b.request_json->>'candidate_instrument_key')::int AND o.raw_payload#>>'{_remediation_mapping,state}'='UNIQUE_DATED_COMPOSITE_FIGI_SHARE_COMPATIBLE' AND NOT EXISTS(SELECT 1 FROM market_governance.remediation_listed_identity_exception_v1 e WHERE e.observation_id=o.observation_id AND e.active);
  reason=CASE WHEN NOT coalesce(eligible,false) THEN 'DATED_SECURITY_IDENTITY_UNRESOLVED' WHEN rec_count=0 THEN 'EMPTY_RTH_RESPONSE_REQUIRES_INDEPENDENT_ABSENCE_EVIDENCE' ELSE 'NATIVE_PRICE_AND_DATED_SECURITY_VALIDATED' END;
  IF eligible AND rec_count>0 THEN
   IF EXISTS(SELECT 1 FROM market_governance.remediation_observation_v1 x JOIN market_governance.remediation_listed_price_release_v2 z ON z.instrument_key=o.instrument_key AND z.bar_ts=x.observed_at WHERE x.source_id=s.source_id AND x.observed_at>=c.regular_open AND x.observed_at<c.regular_close AND (z.open,z.high,z.low,z.close,z.volume,z.trade_count,z.vwap) IS DISTINCT FROM (x.open,x.high,x.low,x.close,x.volume,x.trade_count,x.vwap)) THEN RAISE EXCEPTION 'validation_existing_release_conflict';END IF;
   INSERT INTO market_governance.remediation_listed_price_release_v2(instrument_key,session_date,bar_ts,bar_end,open,high,low,close,volume,trade_count,vwap,native_ticker,listing_observation_id,source_id,source_sha256,record_sha256,acquired_at,identity_known_at,available_at)
   SELECT o.instrument_key,c.session_date,x.observed_at,x.bar_end,x.open,x.high,x.low,x.close,x.volume,x.trade_count,x.vwap,b.symbol,o.observation_id,s.source_id,j.source_sha256,x.record_sha256,s.received_at,o.research_available_at,greatest(s.received_at,o.research_available_at,s.created_at,x.bar_end)
   FROM market_governance.remediation_observation_v1 x WHERE x.source_id=s.source_id AND x.observed_at>=c.regular_open AND x.observed_at<c.regular_close ON CONFLICT(instrument_key,bar_ts) DO NOTHING;
   GET DIAGNOSTICS bad=ROW_COUNT;promoted=promoted+bad;
  ELSE unresolved=unresolved+1;
  END IF;
  INSERT INTO market_governance.remediation_listed_day_validation_v2 VALUES(s.source_id,o.observation_id,c.session_date,b.symbol,o.instrument_key,CASE WHEN eligible AND rec_count>0 THEN 'PROMOTED' ELSE 'NEEDS_EVIDENCE' END,rec_count,reason,jsonb_build_object('source_sha256',j.source_sha256,'native_row_sha256',o.row_hash,'full_minute_coverage_certified',false,'historical_first_receipt_recovered',false),clock_timestamp());
 END LOOP;
 final_status=CASE WHEN unresolved>0 THEN 'NEEDS_EVIDENCE' ELSE 'PROMOTED' END;
 v=jsonb_build_object('status',final_status,'raw_price_rows',n,'promoted_rth_rows',promoted,'unresolved_listing_days',unresolved,'source_sha256',j.source_sha256,'native_and_typed_values_match',true,'historical_first_receipt_recovered',false);
 IF promoted>0 THEN
  INSERT INTO market_governance.remediation_release_v2 VALUES('LISTED_SOURCE:'||b.batch_id,'LISTED_PRICE',jsonb_build_object('batch_id',b.batch_id,'source_id',s.source_id,'native_ticker',b.symbol),'PROMOTED',v,'["Late acquired unadjusted native prices; original historical receipt and full-minute coverage are not certified.","Only independently matched dated securities are served; identity and empty-response exceptions remain unresolved."]','public.market_data_listed_prices_as_of_remediated_v2',clock_timestamp());
 END IF;
 RETURN v;
END; $$;
REVOKE ALL ON FUNCTION market_governance.remediation_validate_source_v2(text,text) FROM PUBLIC,anon,authenticated,service_role;

