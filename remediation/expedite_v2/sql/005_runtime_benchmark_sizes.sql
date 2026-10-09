ALTER TABLE market_governance.remediation_performance_probe_v2 DROP CONSTRAINT remediation_performance_probe_v2_probe_key_check;
ALTER TABLE market_governance.remediation_performance_probe_v2 ADD CHECK(probe_key IN('serial_a','serial_b','parallel_a','parallel_b','combined','small_a','small_b'));
CREATE OR REPLACE FUNCTION market_governance.remediation_performance_probe_v2(p_request jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path='pg_catalog' SET statement_timeout='30s' SET work_mem='32MB' AS $$
DECLARE k text=p_request->>'probe_key';b jsonb;t timestamptz;v jsonb;r jsonb;lo int;hi int;expected_hash text;
BEGIN
 IF current_setting('role',true) IS DISTINCT FROM 'service_role' AND session_user NOT IN('postgres','supabase_admin') THEN RAISE EXCEPTION 'service_role_required';END IF;
 IF p_request->>'run_id' IS DISTINCT FROM 'market_data_remediation_20261008_v1' OR p_request->>'version' IS DISTINCT FROM 'expedite_20261009_v2' OR k NOT IN('serial_a','serial_b','parallel_a','parallel_b','combined','small_a','small_b') OR k IS NULL THEN RAISE EXCEPTION 'benchmark_scope';END IF;
 SELECT result INTO v FROM market_governance.remediation_performance_probe_v2 WHERE probe_key=k;
 IF FOUND THEN RETURN v||jsonb_build_object('replayed',true);END IF;
 SELECT protections INTO r FROM market_governance.remediation_run_v1 WHERE run_id=p_request->>'run_id' AND project_ref='oxzabweahkoimtevbbny' AND phase IN(3,4) AND status!~*'(stop|cancel|complete|closed)';
 IF r IS NULL OR r#>>'{user_stop,active}'='true' OR r#>>'{expedite_v2,benchmark_enabled}' IS DISTINCT FROM 'true' THEN RETURN jsonb_build_object('status','PAUSED');END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended('remediation_probe:'||k,0)) THEN RETURN jsonb_build_object('status','BUSY');END IF;
 SELECT after_row INTO STRICT b FROM market_governance.remediation_change_v1 WHERE change_id='RT07:COMPACT_FEATURES:MANIFEST_V1:B'||CASE WHEN right(k,1)='a' THEN '07001' ELSE '07002' END;
 lo=(b->>'lo_key')::int;hi=(b->>'hi_key')::int;
 IF k='combined' THEN lo=12520;hi=12538; ELSIF k='small_a' THEN lo=12526;hi=12531; ELSIF k='small_b' THEN lo=12532;hi=12538;END IF;
 SELECT encode(sha256(string_agg(output_sha256,''::bytea ORDER BY instrument_key,session_date)),'hex') INTO STRICT expected_hash FROM astra_shared_equity_features_20260928_v1.feature_session_cache_remediated_v1 WHERE instrument_key BETWEEN lo AND hi AND session_date BETWEEN '2026-03-02' AND '2026-03-31';
 t=clock_timestamp();
 SELECT jsonb_build_object('sessions',count(*),'retained_rows',sum(retained_source_rows),'output_sha256',encode(sha256(string_agg(output_sha256,''::bytea ORDER BY instrument_key,session_date)),'hex')) INTO v FROM astra_shared_equity_features_20260928_v1.compute_feature_sessions_remediated_v1(lo,hi,(b->>'start_date')::date,(b->>'end_date')::date,false);
 IF v->>'output_sha256' IS DISTINCT FROM expected_hash THEN RAISE EXCEPTION 'benchmark_output_changed';END IF;
 v=v||jsonb_build_object('status','MEASURED','probe_key',k,'started_at',t,'finished_at',clock_timestamp(),'elapsed_seconds',extract(epoch FROM clock_timestamp()-t),'work_mem_mb',32,'read_only_feature_computation',true);
 INSERT INTO market_governance.remediation_performance_probe_v2 VALUES(k,t,(v->>'finished_at')::timestamptz,v);
 RETURN v;
END; $$;
REVOKE ALL ON FUNCTION market_governance.remediation_performance_probe_v2(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_performance_probe_v2(jsonb) TO service_role;
CREATE OR REPLACE FUNCTION public.market_data_remediation_performance_probe_v2(p_request jsonb)
RETURNS jsonb LANGUAGE sql SECURITY INVOKER SET search_path='pg_catalog' AS $$ SELECT market_governance.remediation_performance_probe_v2(p_request) $$;
REVOKE ALL ON FUNCTION public.market_data_remediation_performance_probe_v2(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_remediation_performance_probe_v2(jsonb) TO service_role;
NOTIFY pgrst,'reload schema';
