CREATE OR REPLACE FUNCTION market_governance.remediation_finalize_warmup_v2()
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
DECLARE d record;v jsonb;n bigint;distinct_n bigint;valid boolean;new_releases int=0;
BEGIN
 FOR d IN SELECT c.* FROM market_governance.remediation_warmup_reference_date_coverage_v1 c
 WHERE c.dated_endpoint_capture_complete AND NOT EXISTS(SELECT 1 FROM market_governance.remediation_release_v2 r WHERE r.release_key='WARMUP_NATIVE:'||c.reference_date::text AND r.status='PROMOTED') LOOP
  SELECT count(*),bool_and(j.status='VALIDATED' AND j.source_id=b.current_source_id AND j.source_sha256=encode(s.source_sha256,'hex') AND (j.evidence->>'native_rows')::bigint=(b.validation->>'source_native_reference_rows')::bigint),jsonb_build_object('source_manifest_sha256',encode(sha256(convert_to(string_agg(j.source_sha256,'' ORDER BY (b.request_json->>'page_number')::int),'UTF8')),'hex'),'pages',count(*),'native_rows',sum((j.evidence->>'native_rows')::bigint)) INTO n,valid,v
  FROM market_governance.remediation_source_batch_v1 b JOIN market_governance.remediation_source_response_v1 s ON s.source_id=b.current_source_id
  LEFT JOIN market_governance.remediation_validation_job_v2 j ON j.job_key='SOURCE:'||b.batch_id
  WHERE b.run_id='market_data_remediation_20261008_v1' AND b.provider='massive' AND b.status='COMPLETE'
  AND (b.request_json->>'noncrypto_scope'='LISTED_EQUITY_GAPS_20261009_V1' OR b.request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1')
  AND b.source_type='massive_reference_tickers' AND b.request_json->>'reference_date'=d.reference_date::text;
  IF n IS DISTINCT FROM d.page_tasks OR valid IS DISTINCT FROM true OR (v->>'native_rows')::bigint IS DISTINCT FROM d.native_rows_acquired THEN CONTINUE;END IF;
  SELECT count(*),count(DISTINCT ticker) INTO n,distinct_n FROM reference.massive_warmup_listing_remediated_v2 WHERE reference_date=d.reference_date;
  IF n<>distinct_n OR n IS DISTINCT FROM d.native_rows_acquired THEN RAISE EXCEPTION 'warmup_native_population_conflict';END IF;
  INSERT INTO market_governance.remediation_release_v2 VALUES('WARMUP_NATIVE:'||d.reference_date::text,'WARMUP_NATIVE',jsonb_build_object('reference_date',d.reference_date),'PROMOTED',v||jsonb_build_object('dated_endpoint_capture_complete',true,'all_native_rows_preserved',true), '["Provider-dated active-stock snapshot; full historical market universe is not certified.","Late receipt is retained. Security identity resolution and prewindow price/feature rebuild remain separate dependencies."]','public.market_data_warmup_native_listings_as_of_v2',clock_timestamp()) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS n=ROW_COUNT;new_releases=new_releases+n;
 END LOOP;
 RETURN jsonb_build_object('warmup_dates_newly_released',new_releases,'full_remediation_complete',false);
END $$;
REVOKE ALL ON FUNCTION market_governance.remediation_finalize_warmup_v2() FROM PUBLIC,anon,authenticated,service_role;

CREATE OR REPLACE FUNCTION market_governance.remediation_warmup_native_listings_as_of_v2(p_date date,p_decision timestamptz,p_archive boolean,p_after_ticker text,p_limit int)
RETURNS SETOF reference.massive_warmup_listing_remediated_v2 LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path='pg_catalog' AS $$
BEGIN
 IF current_setting('role',true) IS DISTINCT FROM 'service_role' AND session_user NOT IN('postgres','supabase_admin') THEN RAISE EXCEPTION 'service_role_required';END IF;
 IF p_date NOT BETWEEN '2025-06-04' AND '2025-08-29' OR p_date IS NULL OR p_decision IS NULL OR p_archive IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 OR p_after_ticker IS NULL THEN RAISE EXCEPTION 'warmup_consumer_scope';END IF;
 RETURN QUERY SELECT w.* FROM reference.massive_warmup_listing_remediated_v2 w
 JOIN market_governance.remediation_release_v2 r ON r.release_key='WARMUP_NATIVE:'||w.reference_date::text AND r.status='PROMOTED'
 WHERE w.reference_date=p_date AND w.ticker>p_after_ticker AND p_date<=p_decision::date AND (p_archive OR greatest(w.acquired_at,r.promoted_at)<=p_decision)
 AND EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 b WHERE b.current_source_id=w.source_id AND b.status='COMPLETE')
 ORDER BY w.ticker LIMIT p_limit;
END $$;
REVOKE ALL ON FUNCTION market_governance.remediation_warmup_native_listings_as_of_v2(date,timestamptz,boolean,text,int) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_warmup_native_listings_as_of_v2(date,timestamptz,boolean,text,int) TO service_role;
CREATE OR REPLACE FUNCTION public.market_data_warmup_native_listings_as_of_v2(p_date date,p_decision_ts timestamptz,p_allow_archived_proxy boolean DEFAULT false,p_after_ticker text DEFAULT '',p_limit int DEFAULT 500)
RETURNS SETOF reference.massive_warmup_listing_remediated_v2 LANGUAGE sql STABLE SECURITY INVOKER SET search_path='pg_catalog' AS $$
 SELECT * FROM market_governance.remediation_warmup_native_listings_as_of_v2(p_date,p_decision_ts,p_allow_archived_proxy,p_after_ticker,p_limit)
$$;
REVOKE ALL ON FUNCTION public.market_data_warmup_native_listings_as_of_v2(date,timestamptz,boolean,text,int) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_warmup_native_listings_as_of_v2(date,timestamptz,boolean,text,int) TO service_role;

-- Include new releases in the ordinary remediation ledger exactly once.
CREATE OR REPLACE FUNCTION market_governance.remediation_record_release_v2()
RETURNS trigger LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
BEGIN
 INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,operation,evidence,validation)
 VALUES('RELEASE:EXPEDITE_V2:'||NEW.release_key,'market_data_remediation_20261008_v1',CASE WHEN NEW.kind='FEATURE' THEN ARRAY['RT-07','IP-06'] ELSE ARRAY['RT-01','IP-04'] END,'market_governance.remediation_release_v2',jsonb_build_object('release_key',NEW.release_key),null,to_jsonb(NEW),'SCOPED_VALIDATED_CANONICAL_RELEASE_V2',NEW.evidence,jsonb_build_object('full_remediation_complete',false,'consumer_route',NEW.consumer_route,'limitations',NEW.limitations));
 RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION market_governance.remediation_record_release_v2() FROM PUBLIC,anon,authenticated,service_role;
CREATE TRIGGER remediation_record_release_v2 AFTER INSERT ON market_governance.remediation_release_v2 FOR EACH ROW EXECUTE FUNCTION market_governance.remediation_record_release_v2();
NOTIFY pgrst,'reload schema';
