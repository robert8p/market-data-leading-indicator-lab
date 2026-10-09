CREATE TABLE market_governance.remediation_performance_probe_v2(
 probe_key text PRIMARY KEY CHECK(probe_key IN('serial_a','serial_b','parallel_a','parallel_b')),
 started_at timestamptz NOT NULL,finished_at timestamptz NOT NULL,
 result jsonb NOT NULL
);
ALTER TABLE market_governance.remediation_performance_probe_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON market_governance.remediation_performance_probe_v2 FROM PUBLIC,anon,authenticated;
CREATE OR REPLACE FUNCTION market_governance.remediation_performance_probe_v2(p_request jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path='pg_catalog' SET statement_timeout='30s' SET work_mem='32MB' AS $$
DECLARE k text=p_request->>'probe_key';b jsonb;t timestamptz;v jsonb;r jsonb;
BEGIN
 IF current_setting('role',true) IS DISTINCT FROM 'service_role' AND session_user NOT IN('postgres','supabase_admin') THEN RAISE EXCEPTION 'service_role_required';END IF;
 IF p_request->>'run_id' IS DISTINCT FROM 'market_data_remediation_20261008_v1' OR p_request->>'version' IS DISTINCT FROM 'expedite_20261009_v2' OR k NOT IN('serial_a','serial_b','parallel_a','parallel_b') OR k IS NULL THEN RAISE EXCEPTION 'benchmark_scope';END IF;
 SELECT result INTO v FROM market_governance.remediation_performance_probe_v2 WHERE probe_key=k;
 IF FOUND THEN RETURN v||jsonb_build_object('replayed',true);END IF;
 SELECT protections INTO r FROM market_governance.remediation_run_v1 WHERE run_id=p_request->>'run_id' AND project_ref='oxzabweahkoimtevbbny' AND phase IN(3,4) AND status!~*'(stop|cancel|complete|closed)';
 IF r IS NULL OR r#>>'{user_stop,active}'='true' OR r#>>'{expedite_v2,benchmark_enabled}' IS DISTINCT FROM 'true' THEN RETURN jsonb_build_object('status','PAUSED');END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended('remediation_probe:'||k,0)) THEN RETURN jsonb_build_object('status','BUSY');END IF;
 SELECT after_row INTO STRICT b FROM market_governance.remediation_change_v1 WHERE change_id='RT07:COMPACT_FEATURES:MANIFEST_V1:B'||CASE WHEN right(k,1)='a' THEN '07001' ELSE '07002' END;
 t=clock_timestamp();
 SELECT jsonb_build_object('sessions',count(*),'retained_rows',sum(retained_source_rows),'output_sha256',encode(sha256(string_agg(output_sha256,''::bytea ORDER BY instrument_key,session_date)),'hex')) INTO v FROM astra_shared_equity_features_20260928_v1.compute_feature_sessions_remediated_v1((b->>'lo_key')::int,(b->>'hi_key')::int,(b->>'start_date')::date,(b->>'end_date')::date,false);
 IF v->>'output_sha256' IS DISTINCT FROM (CASE WHEN right(k,1)='a' THEN '1fe0af89888d587e67bcf7ed5a213fb93b9ce9367092d888cbf991254216056d' ELSE 'd285c5a355a0981b7cf0130fc98fa8f9d6c90bfb6cbcdcaa83ebd92ecb041916' END) THEN RAISE EXCEPTION 'benchmark_output_changed';END IF;
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
