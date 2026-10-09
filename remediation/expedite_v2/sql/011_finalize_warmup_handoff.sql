CREATE OR REPLACE FUNCTION market_governance.remediation_validation_step_v2(p_request jsonb)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path='pg_catalog' SET "TimeZone"='UTC' SET statement_timeout='30s' SET lock_timeout='3s' SET work_mem='32MB' AS $$
DECLARE r market_governance.remediation_run_v1%rowtype;j market_governance.remediation_validation_job_v2%rowtype;s market_governance.remediation_source_response_v1%rowtype;
 action text=p_request->>'action';v_kind text=p_request->>'kind';wid text=p_request->>'worker_id';v jsonb;b jsonb;cp jsonb;valid boolean;n bigint;err text;errstate text;
BEGIN
 IF current_setting('role',true) IS DISTINCT FROM 'service_role' AND session_user NOT IN('postgres','supabase_admin') THEN RAISE EXCEPTION 'service_role_required';END IF;
 IF p_request->>'run_id' IS DISTINCT FROM 'market_data_remediation_20261008_v1' OR p_request->>'version' IS DISTINCT FROM 'expedite_20261009_v2' OR coalesce(wid,'')!~'^validation-[a-f0-9-]{36}$' OR action NOT IN('next','commit','fail','finalize') OR action IS NULL OR octet_length(p_request::text)>16777216 THEN RAISE EXCEPTION 'validation_request_scope';END IF;
 SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id=p_request->>'run_id' AND project_ref='oxzabweahkoimtevbbny';
 IF r.phase NOT IN(3,4) OR r.status~*'(cancel|stop|complete|closed)' OR r.protections#>>'{user_stop,active}'='true' OR r.protections#>>'{expedite_v2,validation_enabled}' IS DISTINCT FROM 'true' THEN RETURN jsonb_build_object('status','PAUSED');END IF;
 IF action='finalize' THEN
  PERFORM market_governance.remediation_finalize_warmup_v2();
  -- Release only after all exact finite checkpoints and independently verified hashes.
  SELECT count(*),coalesce(bool_and(status='VALIDATED'),false) INTO n,valid FROM market_governance.remediation_validation_job_v2 WHERE kind='FEATURE';
  IF n=11289 AND valid AND r.protections#>>'{shared_feature_runner,status}'='COMPLETE_PENDING_RELEASE_VALIDATION' THEN
   SELECT jsonb_build_object('batches',count(*),'sessions',sum((evidence->>'sessions')::bigint),'retained_minutes',sum((evidence->>'retained_minutes')::bigint),'grid_minutes',sum((evidence->>'scheduled_minutes')::bigint),'validation_manifest_sha256',encode(sha256(convert_to(string_agg(evidence::text,'' ORDER BY ordinal),'UTF8')),'hex')) INTO v FROM market_governance.remediation_validation_job_v2 WHERE kind='FEATURE' AND status='VALIDATED';
   IF v->>'sessions'<>'3053710' OR v->>'retained_minutes'<>'371008247' OR v->>'grid_minutes'<>'1186167180' THEN RAISE EXCEPTION 'full_feature_release_population_mismatch';END IF;
   INSERT INTO market_governance.remediation_release_v2 VALUES('COMPACT_FEATURES:compact_full_20261008_v1','FEATURE',jsonb_build_object('manifest_sha256','db596062a61de56b36863ab2415772863e1764885b32356f4de89611703eccad','start','2025-09-01','end','2026-07-31'),'PROMOTED',v,'["Retained-source population only; unresolved historical universe is not certified.","Archival proxy must be explicitly requested; original historical first receipt is not recovered.","New supplemental prices require a separately reconciled derived-data generation; the frozen source generation is preserved."]','public.market_data_shared_features_canonical_v2',clock_timestamp()) ON CONFLICT DO NOTHING;
   RETURN jsonb_build_object('status','FEATURE_RELEASE_PROMOTED','evidence',v,'full_remediation_complete',false);
  END IF;
  RETURN jsonb_build_object('status','WAITING_VALIDATION','feature_jobs',n,'full_remediation_complete',false);
 END IF;
 IF action='next' THEN
  IF v_kind NOT IN('SOURCE','FEATURE') OR v_kind IS NULL THEN RAISE EXCEPTION 'validation_kind';END IF;
  UPDATE market_governance.remediation_validation_job_v2 SET status=CASE WHEN attempts<3 THEN 'READY' ELSE 'BLOCKED' END,worker_id=null,lease_token=null,lease_expires_at=null,updated_at=clock_timestamp(),evidence=evidence||'{"last_error":"LEASE_EXPIRED"}' WHERE status='RUNNING' AND lease_expires_at<clock_timestamp();
  SELECT z.* INTO j FROM market_governance.remediation_validation_job_v2 z
  WHERE z.kind=v_kind AND z.status='READY' AND (z.kind='SOURCE' OR EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 c WHERE c.change_id='RT07:COMPACT_FEATURES:CHECKPOINT_V1:B'||lpad(z.ordinal::text,5,'0')))
  ORDER BY z.ordinal FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN RETURN jsonb_build_object('status','WAITING_DEPENDENCY','kind',v_kind,'full_remediation_complete',false);END IF;
  UPDATE market_governance.remediation_validation_job_v2 SET status='RUNNING',worker_id=wid,lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',attempts=attempts+1,updated_at=clock_timestamp() WHERE job_key=j.job_key RETURNING * INTO j;
  IF j.kind='SOURCE' THEN
   SELECT * INTO STRICT s FROM market_governance.remediation_source_response_v1 WHERE source_id=j.source_id;
   IF s.original_bytes>8388608 OR octet_length(s.compressed_body)>1048576 THEN
    UPDATE market_governance.remediation_validation_job_v2 SET status='NEEDS_EVIDENCE',evidence='{"reason":"LARGE_RETAINED_SOURCE_REQUIRES_STREAMING_VALIDATOR","recoverable":true}',updated_at=clock_timestamp() WHERE job_key=j.job_key;
    RETURN jsonb_build_object('status','NEEDS_EVIDENCE','job_key',j.job_key);
   END IF;
   RETURN jsonb_build_object('status','SOURCE_READY','job_key',j.job_key,'lease_token',j.lease_token,'source_id',j.source_id,'source_sha256',j.source_sha256,'original_bytes',s.original_bytes,'compressed_bytes',octet_length(s.compressed_body),'compressed_sha256',encode(s.compressed_sha256,'hex'),'compressed_body_base64',encode(s.compressed_body,'base64'));
  END IF;
  BEGIN
   SELECT after_row INTO STRICT b FROM market_governance.remediation_change_v1 WHERE change_id='RT07:COMPACT_FEATURES:MANIFEST_V1:B'||lpad(j.ordinal::text,5,'0');
   SELECT after_row INTO STRICT cp FROM market_governance.remediation_change_v1 WHERE change_id='RT07:COMPACT_FEATURES:CHECKPOINT_V1:B'||lpad(j.ordinal::text,5,'0') AND after_sha256=encode(sha256(convert_to(after_row::text,'UTF8')),'hex');
   v=market_governance.remediation_compact_chunk_metrics_v1((b->>'lo_key')::int,(b->>'hi_key')::int,(b->>'start_date')::date,(b->>'end_date')::date);
   IF cp->>'manifest_sha256' IS DISTINCT FROM b->>'manifest_sha256' OR v->>'source_manifest_sha256' IS DISTINCT FROM cp->>'source_manifest_sha256' OR v->>'output_manifest_sha256' IS DISTINCT FROM cp->>'output_manifest_sha256'
   OR v->>'sessions' IS DISTINCT FROM b->>'expected_sessions' OR v->>'retained_minutes' IS DISTINCT FROM b->>'expected_retained_minutes' OR v->>'scheduled_minutes' IS DISTINCT FROM b->>'expected_grid_minutes'
   OR v->>'identity_failures' IS DISTINCT FROM '0' OR v->>'output_failures' IS DISTINCT FROM '0' OR v->>'nonfinite_coefficients' IS DISTINCT FROM '0' THEN RAISE EXCEPTION 'independent_feature_reconciliation_failed';END IF;
   UPDATE market_governance.remediation_validation_job_v2 SET status='VALIDATED',evidence=v||jsonb_build_object('manifest_sha256',b->>'manifest_sha256','checkpoint_sha256_verified',true),updated_at=clock_timestamp(),lease_expires_at=null WHERE job_key=j.job_key;
   RETURN jsonb_build_object('status','VALIDATED','job_key',j.job_key,'kind','FEATURE','sessions',v->'sessions');
  EXCEPTION WHEN query_canceled OR OTHERS THEN
   GET STACKED DIAGNOSTICS err=MESSAGE_TEXT,errstate=RETURNED_SQLSTATE;
   UPDATE market_governance.remediation_validation_job_v2 SET status=CASE WHEN errstate IN('57014','55P03','40001','40P01') AND attempts<3 THEN 'READY' ELSE 'BLOCKED' END,evidence=jsonb_build_object('sqlstate',errstate,'message_sha256',encode(sha256(convert_to(err,'UTF8')),'hex'),'recoverable',true),updated_at=clock_timestamp(),lease_expires_at=null WHERE job_key=j.job_key;
   RETURN jsonb_build_object('status','BLOCKED','job_key',j.job_key,'sqlstate',errstate);
  END;
 END IF;
 SELECT * INTO STRICT j FROM market_governance.remediation_validation_job_v2 WHERE job_key=p_request->>'job_key' FOR UPDATE;
 IF j.kind<>'SOURCE' OR j.worker_id IS DISTINCT FROM wid OR j.lease_token IS DISTINCT FROM (p_request->>'lease_token')::uuid THEN RETURN jsonb_build_object('status','LEASE_LOST','job_key',j.job_key);END IF;
 IF j.status IN('VALIDATED','PROMOTED','NEEDS_EVIDENCE') AND action='commit' THEN RETURN j.evidence||jsonb_build_object('status',j.status,'replayed',true,'job_key',j.job_key);END IF;
 IF j.status<>'RUNNING' OR j.lease_expires_at<clock_timestamp() THEN RETURN jsonb_build_object('status','LEASE_LOST','job_key',j.job_key);END IF;
 IF action='fail' THEN
  UPDATE market_governance.remediation_validation_job_v2 SET status='BLOCKED',evidence=jsonb_build_object('reason','CLIENT_SOURCE_INTEGRITY_FAILURE','recoverable',true),updated_at=clock_timestamp(),lease_expires_at=null WHERE job_key=j.job_key;
  RETURN jsonb_build_object('status','BLOCKED','job_key',j.job_key);
 END IF;
 BEGIN
  v=market_governance.remediation_validate_source_v2(j.job_key,p_request->>'raw_text');
  UPDATE market_governance.remediation_validation_job_v2 SET status=v->>'status',evidence=v,updated_at=clock_timestamp(),lease_expires_at=null WHERE job_key=j.job_key;
  RETURN v||jsonb_build_object('job_key',j.job_key);
 EXCEPTION WHEN query_canceled OR OTHERS THEN
  GET STACKED DIAGNOSTICS err=MESSAGE_TEXT,errstate=RETURNED_SQLSTATE;
  UPDATE market_governance.remediation_validation_job_v2 SET status=CASE WHEN errstate IN('57014','55P03','40001','40P01') AND attempts<3 THEN 'READY' ELSE 'BLOCKED' END,evidence=jsonb_build_object('sqlstate',errstate,'message_sha256',encode(sha256(convert_to(err,'UTF8')),'hex'),'recoverable',true),updated_at=clock_timestamp(),lease_expires_at=null WHERE job_key=j.job_key;
  RETURN jsonb_build_object('status','BLOCKED','job_key',j.job_key,'sqlstate',errstate);
 END;
END; $$;
REVOKE ALL ON FUNCTION market_governance.remediation_validation_step_v2(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_validation_step_v2(jsonb) TO service_role;
CREATE OR REPLACE FUNCTION public.market_data_remediation_validation_step_v2(p_request jsonb)
RETURNS jsonb LANGUAGE sql SECURITY INVOKER SET search_path='pg_catalog' SET statement_timeout='30s' AS $$ SELECT market_governance.remediation_validation_step_v2(p_request) $$;
REVOKE ALL ON FUNCTION public.market_data_remediation_validation_step_v2(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_remediation_validation_step_v2(jsonb) TO service_role;

