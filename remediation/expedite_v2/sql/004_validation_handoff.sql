CREATE TABLE market_governance.remediation_validation_job_v2(
 job_key text PRIMARY KEY,run_id text NOT NULL DEFAULT 'market_data_remediation_20261008_v1' CHECK(run_id='market_data_remediation_20261008_v1'),
 kind text NOT NULL CHECK(kind IN('SOURCE','FEATURE')),ordinal bigint NOT NULL,
 batch_id bigint,source_id uuid,source_sha256 text,
 status text NOT NULL DEFAULT 'READY' CHECK(status IN('READY','RUNNING','VALIDATED','PROMOTED','NEEDS_EVIDENCE','BLOCKED')),
 worker_id text,lease_token uuid,lease_expires_at timestamptz,attempts int NOT NULL DEFAULT 0,
 evidence jsonb NOT NULL DEFAULT '{}',created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX remediation_validation_ready_v2 ON market_governance.remediation_validation_job_v2(kind,status,ordinal);
ALTER TABLE market_governance.remediation_validation_job_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON market_governance.remediation_validation_job_v2 FROM PUBLIC,anon,authenticated;
GRANT INSERT,SELECT(job_key) ON market_governance.remediation_validation_job_v2 TO service_role;

CREATE TABLE market_governance.remediation_release_v2(
 release_key text PRIMARY KEY,kind text NOT NULL,scope jsonb NOT NULL,status text NOT NULL CHECK(status IN('PROMOTED','BLOCKED')),
 evidence jsonb NOT NULL,limitations jsonb NOT NULL,consumer_route text NOT NULL,promoted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE market_governance.remediation_release_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON market_governance.remediation_release_v2 FROM PUBLIC,anon,authenticated;

CREATE TABLE market_governance.remediation_listed_price_release_v2(
 instrument_key integer NOT NULL,session_date date NOT NULL CHECK(session_date>='2025-09-01' AND session_date<'2026-08-01'),
 bar_ts timestamptz NOT NULL,bar_end timestamptz NOT NULL,open float8 NOT NULL,high float8 NOT NULL,low float8 NOT NULL,close float8 NOT NULL,volume float8 NOT NULL,trade_count bigint,vwap float8,
 native_ticker text NOT NULL,listing_observation_id bigint NOT NULL,source_id uuid NOT NULL REFERENCES market_governance.remediation_source_response_v1(source_id),
 source_sha256 text NOT NULL,record_sha256 bytea NOT NULL,acquired_at timestamptz NOT NULL,identity_known_at timestamptz NOT NULL,available_at timestamptz NOT NULL,
 promoted_at timestamptz NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(instrument_key,bar_ts),UNIQUE(source_id,bar_ts)
);
CREATE INDEX remediation_listed_release_source_v2 ON market_governance.remediation_listed_price_release_v2(source_id);
ALTER TABLE market_governance.remediation_listed_price_release_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON market_governance.remediation_listed_price_release_v2 FROM PUBLIC,anon,authenticated;

CREATE TABLE market_governance.remediation_listed_day_validation_v2(
 source_id uuid NOT NULL REFERENCES market_governance.remediation_source_response_v1(source_id),observation_id bigint NOT NULL,
 session_date date NOT NULL,native_ticker text NOT NULL,instrument_key integer,
 status text NOT NULL,price_rows bigint NOT NULL,reason text NOT NULL,evidence jsonb NOT NULL,checked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(source_id,observation_id)
);
ALTER TABLE market_governance.remediation_listed_day_validation_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON market_governance.remediation_listed_day_validation_v2 FROM PUBLIC,anon,authenticated;

CREATE TABLE reference.massive_warmup_listing_remediated_v2(
 source_id uuid NOT NULL REFERENCES market_governance.remediation_source_response_v1(source_id),reference_date date NOT NULL CHECK(reference_date BETWEEN '2025-06-04' AND '2025-08-29'),
 ticker text NOT NULL,composite_figi text,share_class_figi text,cik text,security_type text,primary_exchange text,
 raw_payload jsonb NOT NULL,row_sha256 bytea NOT NULL,source_sha256 text NOT NULL,acquired_at timestamptz NOT NULL,
 PRIMARY KEY(source_id,ticker)
);
CREATE INDEX massive_warmup_identity_v2 ON reference.massive_warmup_listing_remediated_v2(composite_figi,reference_date,ticker);
ALTER TABLE reference.massive_warmup_listing_remediated_v2 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON reference.massive_warmup_listing_remediated_v2 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION market_governance.remediation_enqueue_validation_v2() RETURNS trigger LANGUAGE plpgsql SET search_path='pg_catalog' AS $$
BEGIN
 IF NEW.status='COMPLETE' AND market_governance.remediation_noncrypto_task_v1(to_jsonb(NEW)) THEN
  INSERT INTO market_governance.remediation_validation_job_v2(job_key,kind,ordinal,batch_id,source_id,source_sha256)
  SELECT 'SOURCE:'||NEW.batch_id,'SOURCE',NEW.batch_id,NEW.batch_id,NEW.current_source_id,encode(s.source_sha256,'hex') FROM market_governance.remediation_source_response_v1 s WHERE s.source_id=NEW.current_source_id
  ON CONFLICT(job_key) DO NOTHING;
 END IF;RETURN NEW;
END; $$;
REVOKE ALL ON FUNCTION market_governance.remediation_enqueue_validation_v2() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_enqueue_validation_v2() TO service_role;

INSERT INTO market_governance.remediation_validation_job_v2(job_key,kind,ordinal,batch_id,source_id,source_sha256)
SELECT 'SOURCE:'||b.batch_id,'SOURCE',b.batch_id,b.batch_id,b.current_source_id,encode(s.source_sha256,'hex') FROM market_governance.remediation_source_batch_v1 b JOIN market_governance.remediation_source_response_v1 s ON s.source_id=b.current_source_id
WHERE b.run_id='market_data_remediation_20261008_v1' AND b.provider='massive' AND b.status='COMPLETE' AND (b.request_json->>'noncrypto_scope'='LISTED_EQUITY_GAPS_20261009_V1' OR b.request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1') ON CONFLICT DO NOTHING;
INSERT INTO market_governance.remediation_validation_job_v2(job_key,kind,ordinal)
SELECT 'FEATURE:'||i,'FEATURE',i FROM generate_series(1,11289)i ON CONFLICT DO NOTHING;

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
 raw=p_raw::jsonb;
 IF jsonb_typeof(raw) IS DISTINCT FROM 'object' OR coalesce(raw->>'status','') NOT IN('OK','DELAYED') OR jsonb_typeof(coalesce(raw->'results','[]'))<>'array' THEN RAISE EXCEPTION 'validation_native_shape';END IF;
 IF b.source_type='massive_reference_tickers' THEN
  n=jsonb_array_length(raw->'results');
  IF n IS DISTINCT FROM (b.validation->>'source_native_reference_rows')::bigint OR n IS DISTINCT FROM (raw->>'count')::bigint OR n>1000
  OR EXISTS(SELECT 1 FROM jsonb_array_elements(raw->'results')x WHERE x->>'market' IS DISTINCT FROM 'stocks' OR x->>'active' IS DISTINCT FROM 'true' OR nullif(x->>'ticker','') IS NULL)
  OR (SELECT count(DISTINCT x->>'ticker') FROM jsonb_array_elements(raw->'results')x)<>n THEN RAISE EXCEPTION 'validation_reference_population_mismatch';END IF;
  INSERT INTO reference.massive_warmup_listing_remediated_v2
  SELECT s.source_id,(b.request_json->>'reference_date')::date,x->>'ticker',x->>'composite_figi',x->>'share_class_figi',x->>'cik',x->>'type',x->>'primary_exchange',x,sha256(convert_to(x::text,'UTF8')),j.source_sha256,s.received_at FROM jsonb_array_elements(raw->'results')x;
  RETURN jsonb_build_object('status','VALIDATED','native_rows',n,'source_sha256',j.source_sha256,'dated_capture_release_pending',true,'identity_mapping_complete',false,'historical_first_receipt_recovered',false);
 END IF;
 IF raw->>'ticker' IS DISTINCT FROM b.symbol OR raw->>'adjusted' IS DISTINCT FROM 'false' OR raw->>'next_url' IS NOT NULL THEN RAISE EXCEPTION 'validation_price_native_contract';END IF;
 n=jsonb_array_length(coalesce(raw->'results','[]'));
 IF n<>b.valid_count OR n>=50000 OR b.validation->>'normalization_passed' IS DISTINCT FROM 'true' OR (b.validation->>'invalid_count')::bigint IS DISTINCT FROM 0 THEN RAISE EXCEPTION 'validation_price_population';END IF;
 SELECT count(*) INTO rec_count FROM market_governance.remediation_observation_v1 WHERE source_id=s.source_id;
 IF rec_count<>n THEN RAISE EXCEPTION 'validation_typed_count';END IF;
 IF n>0 THEN
  WITH native AS(SELECT to_timestamp((x->>'t')::numeric/1000) ts,(x->>'o')::float8 o,(x->>'h')::float8 h,(x->>'l')::float8 l,(x->>'c')::float8 c,(x->>'v')::float8 v,(x->>'n')::bigint trades,(x->>'vw')::float8 vw FROM jsonb_array_elements(raw->'results')x)
  SELECT count(*) INTO bad FROM native x FULL JOIN (SELECT * FROM market_governance.remediation_observation_v1 WHERE source_id=s.source_id) y ON y.observed_at=x.ts
  WHERE x.ts IS NULL OR y.source_id IS NULL OR x.ts<b.start_ts OR x.ts>=b.end_ts OR x.ts<>date_trunc('minute',x.ts) OR y.bar_end<>x.ts+interval '1 minute'
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
RETURNS jsonb LANGUAGE sql SECURITY INVOKER SET search_path='pg_catalog' AS $$ SELECT market_governance.remediation_validation_step_v2(p_request) $$;
REVOKE ALL ON FUNCTION public.market_data_remediation_validation_step_v2(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_remediation_validation_step_v2(jsonb) TO service_role;

-- Newly acquired native supplemental bars retain their provider and late knowledge.
-- No replacement of the original Alpaca tape, frozen features, or protected consumers.
CREATE OR REPLACE FUNCTION market_governance.remediation_listed_prices_as_of_v2(p_key int,p_date date,p_decision timestamptz,p_archive boolean)
RETURNS SETOF market_governance.remediation_listed_price_release_v2 LANGUAGE sql STABLE SECURITY DEFINER SET search_path='pg_catalog' AS $$
 SELECT p FROM market_governance.remediation_listed_price_release_v2 p
 WHERE p.instrument_key=p_key AND p.session_date=p_date AND p_date BETWEEN '2025-09-01' AND '2026-07-31' AND p.bar_end<=p_decision
 AND (p_archive IS TRUE OR greatest(p.available_at,p.promoted_at)<=p_decision)
 AND NOT EXISTS(SELECT 1 FROM market_governance.remediation_listed_identity_exception_v1 e WHERE e.observation_id=p.listing_observation_id AND e.active)
$$;
REVOKE ALL ON FUNCTION market_governance.remediation_listed_prices_as_of_v2(int,date,timestamptz,boolean) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_listed_prices_as_of_v2(int,date,timestamptz,boolean) TO service_role;
CREATE OR REPLACE FUNCTION public.market_data_listed_prices_as_of_remediated_v2(p_instrument_key int,p_session_date date,p_decision_ts timestamptz,p_allow_archived_proxy boolean DEFAULT false)
RETURNS SETOF market_governance.remediation_listed_price_release_v2 LANGUAGE sql STABLE SECURITY INVOKER SET search_path='pg_catalog' AS $$
 SELECT * FROM market_governance.remediation_listed_prices_as_of_v2(p_instrument_key,p_session_date,p_decision_ts,p_allow_archived_proxy)
$$;
REVOKE ALL ON FUNCTION public.market_data_listed_prices_as_of_remediated_v2(int,date,timestamptz,boolean) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_listed_prices_as_of_remediated_v2(int,date,timestamptz,boolean) TO service_role;

CREATE OR REPLACE FUNCTION market_governance.remediation_shared_features_canonical_v2(p_instrument_key int,p_session_date date,p_decision_ts timestamptz,p_allow_archived_proxy boolean DEFAULT false)
RETURNS SETOF astra_shared_equity_features_20260928_v1.feature_minute_remediated_v1 LANGUAGE sql STABLE SECURITY DEFINER SET search_path='pg_catalog' SET "TimeZone"='UTC' AS $$
 SELECT f.* FROM public.market_data_shared_features_as_of_remediated_v1(p_instrument_key,p_session_date,p_decision_ts,p_allow_archived_proxy) f
 WHERE EXISTS(SELECT 1 FROM market_governance.remediation_release_v2 WHERE release_key='COMPACT_FEATURES:compact_full_20261008_v1' AND status='PROMOTED')
$$;
REVOKE ALL ON FUNCTION market_governance.remediation_shared_features_canonical_v2(int,date,timestamptz,boolean) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_shared_features_canonical_v2(int,date,timestamptz,boolean) TO service_role;
CREATE OR REPLACE FUNCTION public.market_data_shared_features_canonical_v2(p_instrument_key int,p_session_date date,p_decision_ts timestamptz,p_allow_archived_proxy boolean DEFAULT false)
RETURNS SETOF astra_shared_equity_features_20260928_v1.feature_minute_remediated_v1 LANGUAGE sql STABLE SECURITY INVOKER SET search_path='pg_catalog' AS $$
 SELECT * FROM market_governance.remediation_shared_features_canonical_v2(p_instrument_key,p_session_date,p_decision_ts,p_allow_archived_proxy)
$$;
REVOKE ALL ON FUNCTION public.market_data_shared_features_canonical_v2(int,date,timestamptz,boolean) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_shared_features_canonical_v2(int,date,timestamptz,boolean) TO service_role;
NOTIFY pgrst,'reload schema';
