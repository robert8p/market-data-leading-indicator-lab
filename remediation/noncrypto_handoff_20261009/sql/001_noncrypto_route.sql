CREATE OR REPLACE FUNCTION market_governance.remediation_noncrypto_task_v1(q jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE SET search_path='pg_catalog' AS $$
 SELECT coalesce(q->>'run_id'='market_data_remediation_20261008_v1'
 AND q->>'provider'='massive' AND (
  (q->>'source_type'='massive_reference_tickers' AND q->>'symbol'='US_STOCKS'
   AND q#>>'{request_json,required_parser_version}'='massive_native_warmup_reference_page_20261008_v1'
   AND q#>>'{request_json,warmup_contract}'='RTH_DAILY_LOOKBACK61_PREWINDOW_2025-06-04_2025-08-29_V1')
  OR (q->>'source_type'='massive_candles' AND q->>'interval_seconds'='60'
   AND q#>>'{request_json,noncrypto_scope}'='LISTED_EQUITY_GAPS_20261009_V1'
   AND q#>>'{request_json,asset_class}'='stocks'
   AND q#>>'{request_json,required_parser_version}'='ohlc_source_rules_20261008_v3_native_symbol_quote_contract'
   AND q#>>'{request_json,coverage_evidence_sha256}' ~ '^[a-f0-9]{64}$'
   AND q->>'batch_key' LIKE 'listed:gaps:20261009:%'
   AND q->>'symbol' ~ '^[A-Za-z0-9][A-Za-z0-9._/-]{0,63}$'
   AND q->>'symbol' NOT LIKE '%..%'
   AND (q->>'start_ts')::timestamptz>='2025-09-01T00:00:00Z'::timestamptz
   AND (q->>'end_ts')::timestamptz<='2026-08-01T00:00:00Z'::timestamptz
   AND jsonb_array_length(q#>'{request_json,affected_listing_days}') BETWEEN 1 AND 23)
 ),false)
$$;
REVOKE ALL ON FUNCTION market_governance.remediation_noncrypto_task_v1(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_noncrypto_task_v1(jsonb) TO service_role;

CREATE OR REPLACE FUNCTION public.market_data_remediation_noncrypto_claim_v1(p_request jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' SET statement_timeout='30s' SET lock_timeout='5s' AS $$
DECLARE rid text=p_request->>'run_id'; wid text=p_request->>'worker_id';
 r market_governance.remediation_run_v1%rowtype;
 q market_governance.remediation_source_batch_v1%rowtype; g jsonb; cfg jsonb; n integer; last_type text;
 base jsonb=jsonb_build_object('version','noncrypto_equity_handoff_20261009_v1','run_id','market_data_remediation_20261008_v1');
BEGIN
 IF rid IS DISTINCT FROM 'market_data_remediation_20261008_v1'
 OR p_request->>'version' IS DISTINCT FROM 'noncrypto_equity_handoff_20261009_v1'
 OR coalesce(wid,'') NOT LIKE 'remediation:%' OR length(wid)>160 THEN RAISE EXCEPTION 'noncrypto_request_invalid';END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended(rid||':source_claim',0)) THEN RETURN base||jsonb_build_object('status','idle','reason','source_claim_busy');END IF;
 SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id=rid;
 cfg=r.protections->'noncrypto_runner_v1';
 IF cfg->>'enabled' IS DISTINCT FROM 'true' OR cfg->>'version' IS DISTINCT FROM 'noncrypto_equity_handoff_20261009_v1'
 OR r.protections#>>'{noncrypto_priority_v1,active}' IS DISTINCT FROM 'true' THEN
  RETURN base||jsonb_build_object('status','idle','reason','noncrypto_route_disabled');END IF;
 g=market_governance.remediation_worker_gate_v1(rid);
 IF g->>'ready' IS DISTINCT FROM 'true' THEN RETURN base||g||jsonb_build_object('status','idle');END IF;
 UPDATE market_governance.remediation_source_batch_v1 b SET status=CASE WHEN attempts>=3 THEN 'FAILED' ELSE 'RETRY' END,
 attempt_history=attempt_history||jsonb_build_array(jsonb_build_object('attempt',attempts,'status','LEASE_EXPIRED','source_id',current_source_id,'validation',validation,'completed_at',clock_timestamp())),
 last_error=jsonb_build_object('code','expired_noncrypto_worker_lease'),worker_id=null,lease_token=null,lease_expires_at=null,updated_at=clock_timestamp()
 WHERE run_id=rid AND status='RUNNING' AND lease_expires_at<clock_timestamp() AND market_governance.remediation_noncrypto_task_v1(to_jsonb(b));
 IF EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=rid AND status='RUNNING' AND lease_expires_at>=clock_timestamp()) THEN
  RETURN base||jsonb_build_object('status','idle','reason','existing_source_lease');END IF;
 SELECT b.source_type INTO last_type FROM market_governance.remediation_source_batch_v1 b
 WHERE b.run_id=rid AND b.provider='massive' AND b.status='COMPLETE' AND b.completed_at>='2026-10-09T00:00:00Z'::timestamptz
 AND market_governance.remediation_noncrypto_task_v1(to_jsonb(b)) ORDER BY b.completed_at DESC LIMIT 1;
 SELECT b.* INTO q FROM market_governance.remediation_source_batch_v1 b
 WHERE b.run_id=rid AND b.provider='massive' AND b.status IN('QUEUED','RETRY') AND b.attempts<3 AND b.not_before<=clock_timestamp()
 AND market_governance.remediation_noncrypto_task_v1(to_jsonb(b))
 AND CASE WHEN b.source_type='massive_reference_tickers' THEN p_request#>>'{source_capabilities,massive_warmup_reference}'=b.request_json->>'required_parser_version'
 ELSE p_request#>>'{source_capabilities,massive_candles}'=b.request_json->>'required_parser_version' END
 ORDER BY (b.source_type IS DISTINCT FROM last_type) DESC,b.priority DESC,b.batch_id
 FOR UPDATE OF b SKIP LOCKED LIMIT 1;
 IF NOT FOUND THEN RETURN base||jsonb_build_object('status','idle','reason','no_eligible_noncrypto_tasks_pending_or_dependency_blocked','full_remediation_complete',false);END IF;
 UPDATE market_governance.remediation_source_batch_v1 SET status='RUNNING',attempts=attempts+1,worker_id=wid,
 lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
 current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
 validation='{}',completed_at=null,updated_at=clock_timestamp(),last_error=null WHERE batch_id=q.batch_id RETURNING * INTO q;
 RETURN base||g||jsonb_build_object('status','claimed','batch',to_jsonb(q));
END $$;
REVOKE ALL ON FUNCTION public.market_data_remediation_noncrypto_claim_v1(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_remediation_noncrypto_claim_v1(jsonb) TO service_role;

CREATE OR REPLACE FUNCTION market_governance.remediation_worker_gate_v1(p_run_id text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare r market_governance.remediation_run_v1%rowtype; used_bytes bigint; limit_bytes bigint;
 wal_now bigint; wal_reset timestamptz; wal_start bigint; wal_limit bigint; wal_growth bigint;
 unbilled_headroom bigint; physical_gate jsonb;
begin
 if p_run_id is distinct from 'market_data_remediation_20261008_v1' then raise exception 'remediation_run_not_allowed'; end if;
 select * into strict r from market_governance.remediation_run_v1 where run_id=p_run_id;
 if r.project_ref<>'oxzabweahkoimtevbbny' then raise exception 'remediation_project_mismatch'; end if;
 if r.protections#>>'{user_stop,active}'='true' or (r.protections#>>'{sequential_pipeline_v1,enabled}'='false' AND r.protections#>>'{noncrypto_runner_v1,enabled}' IS DISTINCT FROM 'true') or r.phase<>3 or r.status ~* '(cancel|stop|complete|closed)' then
  return jsonb_build_object('ready',false,'reason','phase_not_ready','phase',r.phase,'run_status',r.status);
 end if;
 limit_bytes=coalesce((r.protections->'source_acquisition'->>'max_storage_bytes')::bigint,0);
 used_bytes=pg_total_relation_size('market_governance.remediation_source_batch_v1')+
            pg_total_relation_size('market_governance.remediation_source_response_v1')+
            pg_total_relation_size('market_governance.remediation_observation_v1')+
            pg_total_relation_size('market_governance.remediation_option_bar_v1')+
            pg_total_relation_size('market_governance.remediation_equity_event_v1')+
            pg_total_relation_size('market_governance.remediation_coinbase_manifest_checkpoint_v1');
 if market_governance.remediation_budgets_removed_v1() then
  physical_gate=market_governance.remediation_physical_capacity_gate_v2(67108864);
  return physical_gate||jsonb_build_object('budgets_removed',true,'used_bytes',used_bytes,'max_bytes',NULL,'max_wal_growth_bytes',NULL,'phase',r.phase);
 end if;
 if limit_bytes<=0 or nullif(r.protections->'source_acquisition'->>'price_evidence','') is null then
  return jsonb_build_object('ready',false,'reason','priced_storage_budget_required','used_bytes',used_bytes);
 end if;
 select wal_bytes::bigint,stats_reset into wal_now,wal_reset from pg_stat_wal;
 wal_start=(r.protections->'source_acquisition'->>'wal_start_bytes')::bigint;
 wal_limit=(r.protections->'source_acquisition'->>'max_wal_growth_bytes')::bigint;
 if wal_start is null or wal_limit is null or wal_limit<=0 or wal_reset is distinct from
 nullif(r.protections->'source_acquisition'->>'wal_stats_reset','')::timestamptz or wal_now<wal_start then
  return jsonb_build_object('ready',false,'reason','wal_baseline_required_or_reset','used_bytes',used_bytes);
 end if;
 wal_growth=wal_now-wal_start;
 if wal_growth+33554432>=wal_limit then
  return jsonb_build_object('ready',false,'reason','global_wal_growth_ceiling_reached','used_bytes',used_bytes,'wal_growth_bytes',wal_growth,'max_wal_growth_bytes',wal_limit);
 end if;
 -- Public pricing is not authorization for extra charges. Bulk acquisition must
 -- fit measured space before the current allocated disk's autoscale threshold.
 unbilled_headroom=(r.protections->'source_acquisition'->>'verified_bytes_before_storage_autoscale')::bigint;
 if unbilled_headroom is null or unbilled_headroom<=0 or
 nullif(r.protections->'source_acquisition'->>'existing_storage_headroom_evidence','') is null then
  return jsonb_build_object('ready',false,'reason','verified_existing_unbilled_storage_headroom_required','used_bytes',used_bytes,'wal_growth_bytes',wal_growth);
 end if;
 if r.protections#>>'{resource_accounting_v2,enabled}'='true' then
  physical_gate=market_governance.remediation_physical_capacity_gate_v2(67108864);
  if physical_gate->>'ready' is distinct from 'true' then return physical_gate||jsonb_build_object('used_bytes',used_bytes,'max_bytes',limit_bytes,'wal_growth_bytes',wal_growth);end if;
 elsif used_bytes+wal_growth+33554432>=unbilled_headroom then
  return jsonb_build_object('ready',false,'reason','existing_unbilled_storage_headroom_ceiling_reached','used_bytes',used_bytes,'wal_growth_bytes',wal_growth,'verified_bytes_before_storage_autoscale',unbilled_headroom);
 end if;
 if used_bytes+16777216>=limit_bytes then
  return jsonb_build_object('ready',false,'reason','storage_budget_reached','used_bytes',used_bytes,'max_bytes',limit_bytes);
 end if;
 return jsonb_build_object('ready',true,'used_bytes',used_bytes,'max_bytes',limit_bytes,'wal_growth_bytes',wal_growth,'max_wal_growth_bytes',wal_limit,'verified_bytes_before_storage_autoscale',unbilled_headroom,'phase',r.phase);
end $function$

;

CREATE OR REPLACE FUNCTION public.market_data_remediation_pipeline_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
 SET "TimeZone" TO 'UTC'
 SET statement_timeout TO '30s'
 SET lock_timeout TO '5s'
 SET work_mem TO '32MB'
AS $function$
DECLARE
 r market_governance.remediation_run_v1%rowtype;
 cfg jsonb; f jsonb; n jsonb; src jsonb; gate jsonb; counts jsonb;
 base jsonb := jsonb_build_object('run_id','market_data_remediation_20261008_v1','version','remediation_sequential_handoff_20261008_v1');
 stage text; next_stage text; change_key text; before_control jsonb;
 batches bigint; sessions bigint; retained bigint; valid boolean;
 cp market_governance.remediation_coinbase_manifest_checkpoint_v1%rowtype;
BEGIN
 IF p_request IS NULL OR jsonb_typeof(p_request)<>'object' OR octet_length(p_request::text)>2048
 OR p_request->>'run_id' IS DISTINCT FROM 'market_data_remediation_20261008_v1'
 OR p_request->>'version' IS DISTINCT FROM 'remediation_sequential_handoff_20261008_v1'
 OR coalesce(p_request->>'worker_id','') !~ '^pipeline-[a-f0-9-]{36}$'
 OR EXISTS(SELECT 1 FROM jsonb_object_keys(p_request) k WHERE k NOT IN('run_id','version','worker_id')) THEN
  RAISE EXCEPTION 'remediation_pipeline_request_invalid';
 END IF;
 IF NOT pg_try_advisory_xact_lock(hashtextextended('market_data_remediation_20261008_v1:source_claim',0)) THEN
  RETURN base||jsonb_build_object('status','BUSY');
 END IF;
 SELECT * INTO STRICT r FROM market_governance.remediation_run_v1
 WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny' FOR NO KEY UPDATE;
 IF r.phase<>3 OR r.status~*'(cancel|stop|complete|closed)' OR r.protections#>>'{user_stop,active}'='true' THEN
  RETURN base||jsonb_build_object('status','PAUSED','reason','run_stopped_or_phase_changed');
 END IF;
 base:=base||jsonb_build_object('noncrypto_enabled',coalesce(r.protections#>>'{noncrypto_runner_v1,enabled}'='true',false));
 cfg:=r.protections->'sequential_pipeline_v1'; stage:=cfg->>'stage';
 IF cfg->>'version' IS DISTINCT FROM 'remediation_sequential_handoff_20261008_v1'
 OR cfg->>'authorization_change_id' IS DISTINCT FROM 'OPS:SPEED_DEPLOYMENT:20261008_V1' THEN
  RETURN base||jsonb_build_object('status','PAUSED','reason','pipeline_not_installed');
 END IF;
 f:=r.protections->'shared_feature_runner'; n:=r.protections->'native_listing_runner'; src:=r.protections->'source_acquisition';
 IF stage='FEATURES' AND (f->>'status' IS DISTINCT FROM 'COMPLETE_PENDING_RELEASE_VALIDATION'
   OR n->>'status' IS DISTINCT FROM 'COMPLETE_PENDING_RELEASE_VALIDATION') THEN
  RETURN base||jsonb_build_object('status','FEATURES','reason','materialization_not_reconciled');
 END IF;
 IF r.protections#>>'{crypto_pause,active}'='true' THEN
  RETURN base||jsonb_build_object('status','PAUSED','reason','crypto_work_paused_by_user','noncrypto_priority','US_EQUITIES_AND_LISTED_SECURITIES');
 END IF;
 IF cfg->>'enabled' IS DISTINCT FROM 'true' THEN
  RETURN base||jsonb_build_object('status','PAUSED','reason','pipeline_handoff_disabled');
 END IF;
 IF NOT market_governance.remediation_budgets_removed_v1() THEN
  RETURN base||jsonb_build_object('status','PAUSED','reason','explicit_unlimited_policy_required');
 END IF;
 IF stage='FEATURES' THEN
  IF f->>'manifest_sha256' IS DISTINCT FROM 'db596062a61de56b36863ab2415772863e1764885b32356f4de89611703eccad'
   OR f->>'generation_id' IS DISTINCT FROM 'compact_full_20261008_v1' OR f->>'enabled' IS DISTINCT FROM 'false'
   OR f->>'completed_batches' IS DISTINCT FROM '11289' OR f->>'sessions' IS DISTINCT FROM '3053710'
   OR f->>'retained_minutes' IS DISTINCT FROM '371008247' OR f->>'scheduled_minutes' IS DISTINCT FROM '1186167180'
   OR n->>'generation_id' IS DISTINCT FROM 'native_listing_full_20261008_v1'
   OR n->>'imported_pages' IS DISTINCT FROM '2948' OR n->>'imported_rows' IS DISTINCT FROM '2830917'
   OR n->>'imported_dates' IS DISTINCT FROM '230' OR n->>'excluded_native_rows' IS DISTINCT FROM '0'
   OR n->>'source_revisions_require_reconciliation' IS DISTINCT FROM 'false'
   OR n->>'pending_batch_key' IS NOT NULL OR src->>'claims_paused' IS DISTINCT FROM 'true' THEN
   RETURN base||jsonb_build_object('status','PAUSED','reason','exact_materialization_reconciliation_required');
  END IF;
  SELECT count(*),sum((after_row->>'sessions')::bigint),sum((after_row->>'retained_minutes')::bigint),
   bool_and(coalesce(validation->>'all_batch_rows_checked'='true' AND validation->>'identity_and_output_hashes_verified'='true',false))
   INTO batches,sessions,retained,valid FROM market_governance.remediation_change_v1
   WHERE run_id=r.run_id AND operation='COMPACT_FEATURE_FINITE_CHECKPOINT_V1'
    AND after_row->>'manifest_sha256'=f->>'manifest_sha256';
  IF batches<>11289 OR sessions<>3053710 OR retained<>371008247 OR valid IS DISTINCT FROM true THEN
   RETURN base||jsonb_build_object('status','PAUSED','reason','feature_checkpoint_evidence_required');
  END IF;
  IF EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=r.run_id AND status='RUNNING')
   OR src#>>'{coinbase_cohort,enabled}'='true' OR src#>>'{fomc_two_windows,enabled}'='true' THEN
   RETURN base||jsonb_build_object('status','PAUSED','reason','conflicting_source_lane');
  END IF;
  IF src#>>'{binance_daily_finite,manifest_sha256}' IS DISTINCT FROM '2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c'
   OR src#>>'{binance_daily_finite,generation_id}' IS DISTINCT FROM 'binance_um_daily_remaining_17533_20261008_v1'
   OR NOT EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 WHERE change_id='OPS:SPEED_PREPARED_SOURCES:20261008_V1' AND validation->>'source_preparation_passed'='true') THEN
   RETURN base||jsonb_build_object('status','PAUSED','reason','prepared_binance_evidence_required');
  END IF;
  gate:=market_governance.remediation_physical_capacity_gate_v2(134217728);
  IF gate->>'ready' IS DISTINCT FROM 'true' THEN RETURN base||jsonb_build_object('status','PAUSED','reason',gate->>'reason');END IF;
  before_control:=jsonb_build_object('source',src,'pipeline',cfg,'native_enabled',n->'enabled');
  src:=jsonb_set(jsonb_set(jsonb_set(src,'{binance_daily_finite,enabled}','true'),'{binance_daily_finite,execution_stage}','"FULL"'),'{claims_paused}','false');
  n:=n||jsonb_build_object('enabled',false);
  next_stage:='BINANCE';change_key:='OPS:SPEED_HANDOFF:FEATURES_TO_BINANCE_V1';
 ELSIF stage='BINANCE' THEN
  IF src#>>'{binance_daily_finite,enabled}' IS DISTINCT FROM 'true' OR src->>'claims_paused' IS DISTINCT FROM 'false'
   OR f->>'enabled'='true' OR n->>'enabled'='true' THEN
   RETURN base||jsonb_build_object('status','PAUSED','reason','source_control_changed');
  END IF;
  SELECT count(*),count(*) FILTER(WHERE status IN('QUEUED','RETRY','RUNNING')) INTO batches,sessions
   FROM market_governance.remediation_source_batch_v1 WHERE run_id=r.run_id
   AND request_json->>'source_generation_id'='binance_um_daily_remaining_17533_20261008_v1';
  IF batches<>17533 THEN RETURN base||jsonb_build_object('status','PAUSED','reason','binance_population_changed');END IF;
  IF sessions>0 THEN RETURN base||jsonb_build_object('status','BINANCE','generation_id','binance_um_daily_remaining_17533_20261008_v1','manifest_sha256','2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c');END IF;
  SELECT jsonb_object_agg(status,n) INTO counts FROM(SELECT status,count(*) n FROM market_governance.remediation_source_batch_v1 WHERE run_id=r.run_id AND request_json->>'source_generation_id'='binance_um_daily_remaining_17533_20261008_v1' GROUP BY status)x;
  IF EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=r.run_id AND status='RUNNING') THEN
   RETURN base||jsonb_build_object('status','PAUSED','reason','existing_source_lease');
  END IF;
  SELECT * INTO cp FROM market_governance.remediation_coinbase_manifest_checkpoint_v1 WHERE run_id=r.run_id AND generation_id='coinbase_native_minute_full_remaining_20261008_v1';
  IF NOT FOUND OR cp.expected_tasks<>826993 OR cp.manifest_sha256<>'3ebe9402349311dd2be04951768830badb9a7abe2da4cc8fb6dceb709b65e651'
   OR cp.task_identity_sha256<>'201869ed32f1a86ad22f82909cb8d38bf6b3113663782a45eba685e5a2695296' THEN
   RETURN base||jsonb_build_object('status','PAUSED','reason','sealed_coinbase_generation_required');
  END IF;
  gate:=market_governance.remediation_physical_capacity_gate_v2(134217728);
  IF gate->>'ready' IS DISTINCT FROM 'true' THEN RETURN base||jsonb_build_object('status','PAUSED','reason',gate->>'reason');END IF;
  before_control:=jsonb_build_object('source',src,'pipeline',cfg);
  src:=jsonb_set(src,'{binance_daily_finite,enabled}','false');
  src:=jsonb_set(src,'{coinbase_cohort}',jsonb_build_object('enabled',true,'mode','SEALED_NATIVE_COINBASE_FULL','budget_version','coinbase_sealed_manifest_budget_20261008_v2',
   'generation_id',cp.generation_id,'manifest_sha256',cp.manifest_sha256,'max_tasks',cp.expected_tasks,'max_claims',cp.max_claims,'expires_at',cp.expires_at,
   'max_concurrency',8,'rate_limit_rps',2.5,'source_price_usd',0,'pilot_verified',true,'pilot_evidence_sha256','1e1c67a3e6a203220385441e487779c645f9fad41c3d65880a762ce8f7ce4c8e','budget_limits_enforced',false));
  next_stage:='COINBASE';change_key:='OPS:SPEED_HANDOFF:BINANCE_TO_COINBASE_V1';
 ELSIF stage='COINBASE' THEN
  IF src#>>'{coinbase_cohort,enabled}' IS DISTINCT FROM 'true' OR src->>'claims_paused' IS DISTINCT FROM 'false'
   OR f->>'enabled'='true' OR n->>'enabled'='true' OR src#>>'{binance_daily_finite,enabled}'='true'
   OR src#>>'{coinbase_cohort,generation_id}' IS DISTINCT FROM 'coinbase_native_minute_full_remaining_20261008_v1'
   OR src#>>'{coinbase_cohort,manifest_sha256}' IS DISTINCT FROM '3ebe9402349311dd2be04951768830badb9a7abe2da4cc8fb6dceb709b65e651' THEN
   RETURN base||jsonb_build_object('status','PAUSED','reason','source_control_changed');
  END IF;
  RETURN base||jsonb_build_object('status','COINBASE','generation_id','coinbase_native_minute_full_remaining_20261008_v1','manifest_sha256','3ebe9402349311dd2be04951768830badb9a7abe2da4cc8fb6dceb709b65e651');
 ELSE
  RETURN base||jsonb_build_object('status','PAUSED','reason','unknown_pipeline_stage');
 END IF;
 cfg:=cfg||jsonb_build_object('stage',next_stage,'last_transition_at',clock_timestamp(),'last_transition_change_id',change_key);
 INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,before_row,after_row,operation,evidence,validation)
 VALUES(change_key,r.run_id,ARRAY['IP-06','CD-F01','CD-F06','CD-F07'],'market_governance.remediation_run_v1',jsonb_build_object('run_id',r.run_id),before_control,
  jsonb_build_object('pipeline',cfg,'source',src,'prior_generation_status_counts',counts),'VERIFIED_SEQUENTIAL_STAGE_HANDOFF',
  jsonb_build_object('authorization_change_id','OPS:SPEED_DEPLOYMENT:20261008_V1','capacity',gate,'worker_id',p_request->>'worker_id'),
  jsonb_build_object('full_remediation_complete',false,'canonical_promotion_complete',false,'prior_acquisition_failures_preserved',true,'completed_checkpoints_preserved',true));
 UPDATE market_governance.remediation_run_v1 SET protections=jsonb_set(jsonb_set(jsonb_set(protections,'{source_acquisition}',src),'{native_listing_runner}',n),'{sequential_pipeline_v1}',cfg),updated_at=clock_timestamp() WHERE run_id=r.run_id;
 RETURN base||jsonb_build_object('status',next_stage,'generation_id',CASE WHEN next_stage='BINANCE' THEN 'binance_um_daily_remaining_17533_20261008_v1' ELSE 'coinbase_native_minute_full_remaining_20261008_v1' END,
 'manifest_sha256',CASE WHEN next_stage='BINANCE' THEN '2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c' ELSE '3ebe9402349311dd2be04951768830badb9a7abe2da4cc8fb6dceb709b65e651' END);
END $function$

;

CREATE OR REPLACE FUNCTION public.market_data_remediation_commit_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare q market_governance.remediation_source_batch_v1%rowtype; g jsonb; sid uuid; a jsonb; n bigint; v jsonb;
begin
 g=market_governance.remediation_worker_gate_v1(p_request->>'run_id');
 if not (g->>'ready')::boolean then raise exception 'remediation_commit_gate_%',g->>'reason'; end if;
 select * into strict q from market_governance.remediation_source_batch_v1 where batch_id=(p_request->>'batch_id')::bigint and run_id=p_request->>'run_id' for update;
 if exists(select 1 from market_governance.remediation_run_v1 where run_id=q.run_id and protections#>>'{crypto_pause,active}'='true') and not market_governance.remediation_noncrypto_task_v1(to_jsonb(q)) then raise exception 'crypto_pause_noncrypto_scope_required';end if;
 if q.status in ('COMPLETE','SOURCE_INVALID') and p_request->>'final'='true' and
 q.lease_token=(p_request->>'lease_token')::uuid and q.worker_id=p_request->>'worker_id' and
 q.current_source_id=(p_request->>'source_id')::uuid and q.valid_count=(p_request->'validation'->>'valid_count')::bigint then
 return jsonb_build_object('stored',true,'source_id',q.current_source_id,'valid_count',q.valid_count,'final',true,'replayed',true); end if;
 if q.status<>'RUNNING' or q.lease_token is distinct from (p_request->>'lease_token')::uuid or q.worker_id is distinct from p_request->>'worker_id' or q.lease_expires_at<clock_timestamp() then raise exception 'source_lease_mismatch'; end if;


 -- Exact finite native options pilot; raw artifacts are retained before parsing.
 if q.source_type in ('massive_option_reference','massive_option_minutes') then
  if q.attempts<>1 or not market_governance.remediation_options_task_valid_v1(to_jsonb(q))
   or (q.source_type='massive_option_minutes' and not market_governance.remediation_options_minute_proof_v1(to_jsonb(q)))
   then raise exception 'options_exact_task_and_reference_proof_required';end if;
  if p_request ? 'source' then
   if p_request->>'artifact_only' is distinct from 'true'
    or p_request->'source'->>'source_url' is distinct from market_governance.remediation_options_source_url_v1(q.source_type,q.symbol,q.request_json)
    or p_request->'source'->>'parser_version' is distinct from 'massive_native_options_pilot_20261008_v1'
    or p_request->'source'->>'revision_at' is not null
    or p_request->'source'->>'role' is distinct from (case when p_request->'source'->>'http_status'='200' then 'primary' else 'error' end)
    or (p_request->'source'->>'original_bytes')::bigint not between 0 and 1048577
    or jsonb_typeof(p_request->'source'->'provenance'->'response_body_complete') is distinct from 'boolean'
    or jsonb_typeof(p_request->'source'->'provenance'->'source_payload_intact') is distinct from 'boolean'
    or p_request->'source'->'provenance'->>'native_pilot_id' is distinct from 'intended_cohort_options_native_pilot_20261008_v1'
    or p_request->'source'->'provenance'->>'source_hash_basis' is distinct from 'RETAINED_RESPONSE_BYTES_WITH_EXPLICIT_COMPLETENESS_FLAG'
    or (p_request->'source'->'provenance'->>'response_body_complete')::boolean is distinct from ((p_request->'source'->>'original_bytes')::bigint<=1048576)
    or (p_request->'source'->'provenance'->>'source_payload_intact')::boolean is distinct from (
       (p_request->'source'->'provenance'->>'response_body_complete')::boolean and (p_request->'source'->>'credential_redactions')::integer=0)
    then raise exception 'options_exact_raw_artifact_contract';end if;
   if exists(select 1 from market_governance.remediation_source_response_v1 z where z.batch_id=q.batch_id
    and z.source_id<>(p_request->'source'->>'source_id')::uuid) then raise exception 'options_second_physical_response_disallowed';end if;
  else
   if not exists(select 1 from market_governance.remediation_source_response_v1 z
     where z.source_id=(p_request->>'source_id')::uuid and z.batch_id=q.batch_id and z.role='primary' and z.http_status=200
      and z.parser_version='massive_native_options_pilot_20261008_v1' and z.credential_redactions=0 and z.revision_at is null
      and z.source_url=market_governance.remediation_options_source_url_v1(q.source_type,q.symbol,q.request_json)
      and z.original_bytes between 1 and 1048576 and z.provenance->>'response_body_complete'='true'
      and z.provenance->>'source_payload_intact'='true')
     then raise exception 'options_complete_native_source_required_before_normalization';end if;
  end if;
 end if;

 -- One exact raw earnings entitlement probe. Existing source contracts below are unchanged.
 if q.source_type='twelvedata_earnings_probe' then
  if q.attempts<>1 or coalesce(p_request->'records','[]'::jsonb) is distinct from '[]'::jsonb then
   raise exception 'earnings_single_attempt_raw_only_required';end if;
  if p_request ? 'source' then
   if p_request->>'artifact_only' is distinct from 'true'
    or p_request->'source'->>'parser_version' is distinct from 'twelvedata_earnings_included_quota_probe_20261008_v1'
    or p_request->'source'->>'revision_at' is not null
    or coalesce(p_request->'source'->>'source_url','') not in ('https://api.twelvedata.com/api_usage?format=JSON','https://api.twelvedata.com/earnings?symbol=AAPL&start_date=2025-09-01&end_date=2026-07-31&outputsize=1000&format=JSON')
    or p_request->'source'->>'role' is distinct from (case when p_request->'source'->>'http_status'='200' then 'primary' else 'error' end)
    or p_request->'source'->'provenance'->>'strict_historical_replay_eligible' is distinct from 'false'
    then raise exception 'earnings_exact_artifact_contract';end if;
   if exists(select 1 from market_governance.remediation_source_response_v1 r where r.batch_id=q.batch_id
      and r.source_url=p_request->'source'->>'source_url' and r.source_id<>(p_request->'source'->>'source_id')::uuid)
      then raise exception 'earnings_second_physical_response_not_allowed';end if;
   if p_request->'source'->>'source_url'='https://api.twelvedata.com/earnings?symbol=AAPL&start_date=2025-09-01&end_date=2026-07-31&outputsize=1000&format=JSON' and (
    not market_governance.remediation_td_earnings_quota_valid_v1(q.validation->'quota_preflight')
    or not exists(select 1 from market_governance.remediation_source_response_v1 r
       where r.batch_id=q.batch_id and r.source_id=(q.validation->>'quota_source_id')::uuid
        and r.source_url='https://api.twelvedata.com/api_usage?format=JSON' and r.http_status=200 and r.role='primary'
        and r.parser_version='twelvedata_earnings_included_quota_probe_20261008_v1'
        and encode(r.source_sha256,'hex')=q.validation->>'quota_source_sha256'
        and r.received_at=(q.validation->>'quota_received_at')::timestamptz
        and (p_request->'source'->>'received_at')::timestamptz>=r.received_at
        and (p_request->'source'->>'received_at')::timestamptz<=r.received_at+interval '2 minutes'))
       then raise exception 'retained_included_quota_required_before_earnings';end if;
  else
   v=p_request->'validation';
   if jsonb_typeof(v) is distinct from 'object'
    or v->>'parser_version' is distinct from 'twelvedata_earnings_included_quota_probe_20261008_v1'
    or v->>'normalized_rows_created' is distinct from '0'
    or v->>'max_attempts' is distinct from '1' or v->>'max_physical_requests' is distinct from '2'
    or v->>'max_weighted_credits_per_attempt' is distinct from '21'
    or v->>'incremental_price_usd' is distinct from '0'
    or exists(select 1 from unnest(ARRAY['raw_count','valid_count','invalid_count','duplicate_count','outside_count','duplicate_conflict_count']) k where v->>k is distinct from '0')
    or exists(select 1 from unnest(ARRAY['historical_forecast_vintage_recovered','historical_announcement_timestamp_recovered','historical_first_receipt_recovered','strict_historical_surprise_eligible','source_coverage_complete']) k where v->>k is distinct from 'false')
    or not exists(select 1 from market_governance.remediation_source_response_v1 r where r.batch_id=q.batch_id
       and r.source_id=(v->>'quota_source_id')::uuid and r.source_url='https://api.twelvedata.com/api_usage?format=JSON' and r.role='primary' and r.http_status=200
       and encode(r.source_sha256,'hex')=v->>'quota_source_sha256' and r.received_at=(v->>'quota_received_at')::timestamptz)
    then raise exception 'earnings_raw_validation_and_quota_identity_required';end if;
   if v->'quota_preflight'->>'allowed'='true' and not market_governance.remediation_td_earnings_quota_valid_v1(v->'quota_preflight') then
    raise exception 'earnings_quota_arithmetic_invalid';end if;
   if coalesce(p_request->>'final','false')='false' then
    if p_request->>'source_id' is distinct from v->>'quota_source_id'
     or v->>'source_record_grain' is distinct from 'RAW_ONLY_QUOTA_PREFLIGHT'
     or v->>'physical_requests_attempted' is distinct from '1'
     or v->>'weighted_credits_upper_bound' is distinct from '1'
     or v->>'earnings_request_sent' is distinct from 'false'
     then raise exception 'earnings_quota_only_checkpoint_required';end if;
   else
    if not market_governance.remediation_td_earnings_quota_valid_v1(v->'quota_preflight')
     or v->'quota_preflight' is distinct from q.validation->'quota_preflight'
     or v->>'quota_source_id' is distinct from q.validation->>'quota_source_id'
     or v->>'source_record_grain' is distinct from 'RAW_ONLY_NATIVE_EARNINGS_ENTITLEMENT_PROBE'
     or v->>'physical_requests_attempted' is distinct from '2'
     or v->>'weighted_credits_upper_bound' is distinct from '21'
     or v->>'earnings_request_sent' is distinct from 'true' or v->>'raw_native_earnings_retained' is distinct from 'true'
     or jsonb_typeof(v->'source_native_earnings_rows') is distinct from 'number'
     or not coalesce(v->>'source_native_earnings_rows' ~ '^[0-9]{1,4}$',false)
     or (v->>'source_native_earnings_rows')::integer not between 0 and 1000
     or v->>'historical_publication_recovered' is distinct from 'false'
     or v->>'retained_original_revision_history_complete' is distinct from 'false'
     or not exists(select 1 from market_governance.remediation_source_response_v1 r where r.batch_id=q.batch_id
       and r.source_id=(p_request->>'source_id')::uuid and r.source_url='https://api.twelvedata.com/earnings?symbol=AAPL&start_date=2025-09-01&end_date=2026-07-31&outputsize=1000&format=JSON'
       and r.http_status=200 and r.role='primary' and r.parser_version='twelvedata_earnings_included_quota_probe_20261008_v1')
     then raise exception 'earnings_raw_final_contract';end if;
   end if;
  end if;
 end if;
 if p_request ? 'source' then
  if q.source_type='tardis_liquidations' and (
   p_request->'source'->>'role' is distinct from 'primary' or
   p_request->'source'->>'source_url' is distinct from 'https://datasets.tardis.dev/v1/binance-futures/liquidations/'||to_char(q.start_ts at time zone 'UTC','YYYY/MM/DD/')||q.symbol||'.csv.gz' or
   p_request->'source'->>'parser_version' is distinct from 'tardis_free_liquidation_source_rules_20261008_v1' or
   p_request->'source'->>'revision_at' is not null) then raise exception 'source_liquidation_artifact_contract'; end if;
  sid=market_governance.remediation_store_source_v1(q.batch_id,p_request->'source');
  if coalesce((p_request->>'artifact_only')::boolean,false) then
   update market_governance.remediation_source_batch_v1 set heartbeat_at=clock_timestamp(),lease_expires_at=clock_timestamp()+interval '3 minutes',
   current_source_id=case when p_request->'source'->>'role'='primary' and p_request->'source'->>'http_status'='200' then sid else current_source_id end where batch_id=q.batch_id;
   return jsonb_build_object('stored',true,'source_id',sid,'artifact_only',true);
  end if;
 else sid=(p_request->>'source_id')::uuid;
 end if;
 if not exists(select 1 from market_governance.remediation_source_response_v1 where source_id=sid and batch_id=q.batch_id and role='primary' and http_status=200) then raise exception 'valid_primary_source_required'; end if;
 a=coalesce(p_request->'records','[]'::jsonb);
 if jsonb_typeof(a)<>'array' or jsonb_array_length(a)>1000 then raise exception 'record_batch_size_invalid'; end if;
 if exists(select 1 from jsonb_array_elements(a) x where x->>'observed_at' is null or jsonb_typeof(x->'values') is distinct from 'object') then raise exception 'record_required_fields_missing'; end if;
 if q.source_type in ('coinbase_candles','twelvedata_candles','massive_candles','binance_klines','binance_mark','binance_index','alpaca_option_bars') and
 exists(select 1 from jsonb_array_elements(a) x where x->>'bar_end' is null or
 x->'values'->>'open' is null or x->'values'->>'high' is null or x->'values'->>'low' is null or x->'values'->>'close' is null) then raise exception 'candle_required_fields_missing'; end if;
 if q.source_type in ('binance_mark','binance_index') and exists(select 1 from jsonb_array_elements(a) x where
 x->'values'->>'volume' is not null or x->'values'->>'quote_volume' is not null or x->'values'->>'trade_count' is not null or
 x->'values'->>'taker_buy_base_volume' is not null or x->'values'->>'taker_buy_quote_volume' is not null) then raise exception 'mark_index_trade_activity_not_supported'; end if;
 if q.source_type='binance_book_depth' and exists(select 1 from jsonb_array_elements(a) x where
 x->>'bar_end' is not null or jsonb_typeof(x->'values'->'depth_bands') is distinct from 'array' or x->'values'->>'open' is not null or
 x->'values'->>'close' is not null or x->'values'->>'volume' is not null or x->'values'->>'trade_count' is not null) then raise exception 'source_depth_profile_contract_mismatch'; end if;
 if q.source_type<>'binance_book_depth' and exists(select 1 from jsonb_array_elements(a) x where x->'values'->>'depth_bands' is not null) then raise exception 'source_depth_field_not_allowed'; end if;
 if q.source_type='coinmetrics_supply' and exists(select 1 from jsonb_array_elements(a) x cross join lateral jsonb_object_keys(x->'values') k
 where k not in('current_supply_native','market_cap_current_supply_usd','reference_price_usd','availability_not_before','availability_basis')) then
 raise exception 'source_supply_unsupported_field'; end if;
 if q.source_type='coinmetrics_supply' and exists(select 1 from jsonb_array_elements(a) x where
  x->>'bar_end' is null or (x->>'bar_end')::timestamptz is distinct from (x->>'observed_at')::timestamptz+interval '1 day' or
  (x->>'observed_at')::timestamptz is distinct from date_trunc('day',(x->>'observed_at')::timestamptz at time zone 'UTC') at time zone 'UTC' or
  (x->'values'->>'availability_not_before')::timestamptz is distinct from (x->>'bar_end')::timestamptz or
  x->'values'->>'availability_basis' is distinct from 'DOCUMENTED_DAILY_PERIOD_COMPLETION_LOWER_BOUND_NOT_PUBLICATION' or
  x->'values'->>'open' is not null or x->'values'->>'volume' is not null or x->'values'->>'trade_count' is not null or
  x->'values'->>'open_interest_quantity' is not null or x->'values'->>'funding_rate' is not null) then
  raise exception 'source_supply_contract_mismatch'; end if;
 if q.source_type<>'coinmetrics_supply' and exists(select 1 from jsonb_array_elements(a) x where
  x->'values'->>'current_supply_native' is not null or x->'values'->>'market_cap_current_supply_usd' is not null or x->'values'->>'reference_price_usd' is not null) then
  raise exception 'source_supply_field_not_allowed'; end if;
 if q.source_type='tardis_liquidations' then
  if exists(select 1 from jsonb_array_elements(a)x cross join lateral jsonb_object_keys(x->'values')k where k not in
   ('liquidation_price','liquidation_quantity_native','liquidation_side','source_collector_received_at','source_event_id','source_row_number')) then raise exception 'source_liquidation_unsupported_field'; end if;
  if exists(select 1 from jsonb_array_elements(a)x where x->>'bar_end' is not null or
   x->'values'->>'liquidation_price' is null or (x->'values'->>'liquidation_price')::numeric<=0 or
   x->'values'->>'liquidation_quantity_native' is null or (x->'values'->>'liquidation_quantity_native')::numeric<0 or
   x->'values'->>'liquidation_side' is null or x->'values'->>'liquidation_side' not in('buy','sell','unknown') or
   x->'values'->>'source_collector_received_at' is null or
   (x->'values'->>'source_collector_received_at')::timestamptz<(x->>'observed_at')::timestamptz or
   (x->'values'->>'source_collector_received_at')::timestamptz<q.start_ts or (x->'values'->>'source_collector_received_at')::timestamptz>=q.end_ts or
   x->'values'->>'source_row_number' is null or (x->'values'->>'source_row_number')::bigint<0) then raise exception 'source_liquidation_typed_contract'; end if;
 elsif exists(select 1 from jsonb_array_elements(a)x where x->'values' ?| array['liquidation_price','liquidation_quantity_native','liquidation_side','source_collector_received_at','source_event_id','source_row_number']) then
  raise exception 'source_liquidation_field_not_allowed';
 end if;
 if exists(select 1 from jsonb_array_elements(a) x where (x->>'observed_at')::timestamptz<q.start_ts or (x->>'observed_at')::timestamptz>=q.end_ts or
 (nullif(x->>'bar_end','')::timestamptz is not null and (x->>'bar_end')::timestamptz>q.end_ts)) then raise exception 'record_outside_authorized_batch'; end if;
 if exists(select 1 from jsonb_array_elements(a) x cross join lateral jsonb_each_text(x->'values') m
 where m.value ~* '^[-+]?(nan|infinity|inf)$') then raise exception 'nonfinite_source_value'; end if;
 -- Multi-instrument products retain native identity; the legacy observation key
 -- stays untouched. This branch runs under the same gate, lease and source proof.
 if q.source_type='massive_option_reference' then
  if a<>'[]'::jsonb then raise exception 'options_reference_raw_only';end if;n=0;
 elsif q.source_type='massive_option_minutes' then
  if exists(select 1 from jsonb_array_elements(a)x cross join lateral jsonb_object_keys(x->'values') k
    where k not in('open','high','low','close','volume_contracts','trade_count','vwap','availability_not_before','availability_basis'))
   then raise exception 'options_minute_unsupported_value';end if;
  if exists(select 1 from jsonb_array_elements(a)x where
   x->>'native_symbol' is distinct from q.symbol
   or x->>'session_date' is distinct from market_governance.remediation_options_candidate_v1(q.request_json->>'candidate_id')->>'session_date'
   or x->>'underlying_instrument_key' is distinct from market_governance.remediation_options_candidate_v1(q.request_json->>'candidate_id')->>'instrument_key'
   or x->>'underlying_native_symbol' is distinct from q.request_json->'selected_contract'->>'underlying_native_symbol'
   or x->>'expiry' is distinct from q.request_json->'selected_contract'->>'expiry'
   or x->>'option_right' is distinct from q.request_json->'selected_contract'->>'option_right'
   or (x->>'strike')::numeric is distinct from (q.request_json->'selected_contract'->>'strike')::numeric
   or x->>'shares_per_contract' is distinct from '100'
   or x->>'reference_evidence_id' is distinct from (q.request_json->>'reference_source_id')||'#native_row:'||(q.request_json->'selected_contract'->>'source_row_ordinal')
   or x->>'available_at' is not null or x->>'publication_at' is not null or x->>'open_interest' is not null
   or x->>'implied_volatility' is not null or x->>'greeks' is not null
   or (x->>'observed_at')::timestamptz is distinct from date_trunc('minute',(x->>'observed_at')::timestamptz)
   or (x->>'bar_end')::timestamptz is distinct from (x->>'observed_at')::timestamptz+interval '1 minute'
   or (x->'values'->>'availability_not_before')::timestamptz is distinct from (x->>'bar_end')::timestamptz
   or x->'values'->>'availability_basis' is distinct from 'MINUTE_COMPLETION_LOWER_BOUND_NOT_PUBLICATION'
   or x->'values'->>'volume_contracts' is null
   or (x->'values'->>'volume_contracts')::numeric<>trunc((x->'values'->>'volume_contracts')::numeric))
   then raise exception 'options_minute_contract_or_timing_mismatch';end if;
  if exists(select 1 from jsonb_array_elements(a)x join market_governance.remediation_option_bar_v1 o
   on o.source_id=sid and o.native_symbol=x->>'native_symbol' and o.observed_at=(x->>'observed_at')::timestamptz
   where o.record_sha256<>sha256(convert_to(x::text,'UTF8')))
   or exists(select 1 from jsonb_array_elements(a)x group by x->>'native_symbol',(x->>'observed_at')::timestamptz having count(distinct x::text)>1)
   then raise exception 'options_native_minute_version_conflict';end if;
  insert into market_governance.remediation_option_bar_v1(source_id,native_symbol,observed_at,session_date,bar_end,underlying_instrument_key,underlying_native_symbol,expiry,option_right,strike,shares_per_contract,reference_evidence_id,open,high,low,close,volume_contracts,trade_count,vwap,availability_not_before,availability_basis,record_sha256)
  select sid,x->>'native_symbol',(x->>'observed_at')::timestamptz,(x->>'session_date')::date,(x->>'bar_end')::timestamptz,
   x->>'underlying_instrument_key',x->>'underlying_native_symbol',(x->>'expiry')::date,x->>'option_right',(x->>'strike')::numeric,
   (x->>'shares_per_contract')::integer,x->>'reference_evidence_id',
   (x->'values'->>'open')::numeric,(x->'values'->>'high')::numeric,(x->'values'->>'low')::numeric,(x->'values'->>'close')::numeric,
   (x->'values'->>'volume_contracts')::numeric,(x->'values'->>'trade_count')::bigint,(x->'values'->>'vwap')::numeric,
   (x->'values'->>'availability_not_before')::timestamptz,x->'values'->>'availability_basis',sha256(convert_to(x::text,'UTF8'))
   from jsonb_array_elements(a)x on conflict(source_id,native_symbol,observed_at) do nothing;
  select count(*) into n from market_governance.remediation_option_bar_v1 where source_id=sid;
 elsif q.source_type in ('alpaca_option_daily_panel','alpaca_equity_quotes','alpaca_equity_trades') then
  if q.provider is distinct from 'alpaca' or q.request_json->>'required_parser_version' is distinct from 'alpaca_multi_instrument_source_contract_20261008_v2'
  or not exists(select 1 from market_governance.remediation_source_response_v1 where source_id=sid and parser_version='alpaca_multi_instrument_source_contract_20261008_v2') then raise exception 'source_panel_capability_required'; end if;
  if exists(select 1 from jsonb_array_elements(a) x where nullif(x->>'native_symbol','') is null or x->>'available_at' is not null or x->>'publication_at' is not null) then raise exception 'source_panel_native_identity_or_availability'; end if;
  if q.source_type='alpaca_option_daily_panel' then
   if exists(select 1 from jsonb_array_elements(a) x where not exists(
     select 1 from jsonb_array_elements(q.request_json->'contracts') ref where ref->>'native_symbol'=x->>'native_symbol'
      and ref->>'expiry'=x->>'expiry' and ref->>'right'=x->>'option_right' and (ref->>'strike')::numeric=(x->>'strike')::numeric
      and ref->>'shares_per_contract'='100' and ref->>'adjusted_contract'='false'
      and ref->>'underlying_instrument_key'=x->>'underlying_instrument_key' and ref->>'underlying_native_symbol'=x->>'underlying_native_symbol'
      and ref->>'reference_evidence_id'=x->>'reference_evidence_id' and ref->'eligible_session_dates' ? (x->>'session_date'))
     or q.request_json->'daily_sessions'->(x->>'session_date')->>'close' is null
     or (q.request_json->'daily_sessions'->(x->>'session_date')->>'close')::timestamptz is distinct from (x->>'bar_end')::timestamptz
     or x->>'open_interest' is not null or x->>'implied_volatility' is not null or x->>'greeks' is not null
     or (x->>'observed_at')::timestamptz is distinct from ((x->>'session_date')::date::timestamp at time zone 'America/New_York'))
    then raise exception 'source_option_historical_contract_mismatch'; end if;
   if exists(select 1 from jsonb_array_elements(a) x join market_governance.remediation_option_bar_v1 o on o.source_id=sid and o.native_symbol=x->>'native_symbol' and o.observed_at=(x->>'observed_at')::timestamptz where o.record_sha256<>sha256(convert_to(x::text,'UTF8')))
    or exists(select 1 from jsonb_array_elements(a) x group by x->>'native_symbol',(x->>'observed_at')::timestamptz having count(distinct x::text)>1) then raise exception 'source_option_version_conflict'; end if;
   insert into market_governance.remediation_option_bar_v1(source_id,native_symbol,observed_at,session_date,bar_end,underlying_instrument_key,underlying_native_symbol,expiry,option_right,strike,shares_per_contract,reference_evidence_id,open,high,low,close,volume_contracts,trade_count,vwap,availability_not_before,availability_basis,record_sha256)
   select sid,x->>'native_symbol',(x->>'observed_at')::timestamptz,(x->>'session_date')::date,(x->>'bar_end')::timestamptz,x->>'underlying_instrument_key',x->>'underlying_native_symbol',(x->>'expiry')::date,x->>'option_right',(x->>'strike')::numeric,(x->>'shares_per_contract')::integer,x->>'reference_evidence_id',
    (x->'values'->>'open')::numeric,(x->'values'->>'high')::numeric,(x->'values'->>'low')::numeric,(x->'values'->>'close')::numeric,(x->'values'->>'volume_contracts')::numeric,(x->'values'->>'trade_count')::bigint,(x->'values'->>'vwap')::numeric,
    (x->'values'->>'availability_not_before')::timestamptz,x->'values'->>'availability_basis',sha256(convert_to(x::text,'UTF8')) from jsonb_array_elements(a)x on conflict(source_id,native_symbol,observed_at) do nothing;
   select count(*) into n from market_governance.remediation_option_bar_v1 where source_id=sid;
  else
   if exists(select 1 from jsonb_array_elements(a) x where not(q.request_json->'symbols' ? (x->>'native_symbol'))
    or x->>'source_asof' is distinct from q.request_json->>'source_asof' or x->>'feed' is distinct from 'sip'
    or x->>'event_kind' is distinct from case when q.source_type='alpaca_equity_quotes' then 'quote' else 'trade' end
    or (x->>'event_ns')::numeric <extract(epoch from q.start_ts)*1000000000 or (x->>'event_ns')::numeric >=extract(epoch from q.end_ts)*1000000000
    or x->>'bar_end' is not null or x->'values' is distinct from '{}'::jsonb
    or (x->>'event_time_display_utc')::timestamptz is distinct from (x->>'observed_at')::timestamptz)
    then raise exception 'source_equity_native_contract_mismatch'; end if;
   if exists(select 1 from jsonb_array_elements(a)x join market_governance.remediation_equity_event_v1 o on o.source_id=sid and o.native_symbol=x->>'native_symbol' and o.source_row_ordinal=(x->>'source_row_ordinal')::integer where o.record_sha256<>sha256(convert_to(x::text,'UTF8')))
    or exists(select 1 from jsonb_array_elements(a)x group by x->>'native_symbol',(x->>'source_row_ordinal')::integer having count(distinct x::text)>1) then raise exception 'source_equity_version_conflict'; end if;
   insert into market_governance.remediation_equity_event_v1(source_id,native_symbol,source_row_ordinal,event_kind,event_ns,event_time_native,event_time_display_utc,source_asof,feed,source_event_id,conditions,tape,price,trade_size,trade_exchange,bid_price,ask_price,bid_size_native,ask_size_native,bid_exchange,ask_exchange,quote_size_basis,bid_size_shares,ask_size_shares,record_sha256)
   select sid,x->>'native_symbol',(x->>'source_row_ordinal')::integer,x->>'event_kind',(x->>'event_ns')::bigint,x->>'event_time_native',(x->>'event_time_display_utc')::timestamptz,(x->>'source_asof')::date,x->>'feed',x->>'source_event_id',array(select jsonb_array_elements_text(x->'conditions')),x->>'tape',
    (x->>'price')::numeric,(x->>'trade_size')::numeric,x->>'trade_exchange',(x->>'bid_price')::numeric,(x->>'ask_price')::numeric,(x->>'bid_size_native')::numeric,(x->>'ask_size_native')::numeric,x->>'bid_exchange',x->>'ask_exchange',x->>'quote_size_basis',(x->>'bid_size_shares')::numeric,(x->>'ask_size_shares')::numeric,sha256(convert_to(x::text,'UTF8')) from jsonb_array_elements(a)x on conflict(source_id,native_symbol,source_row_ordinal) do nothing;
   select count(*) into n from market_governance.remediation_equity_event_v1 where source_id=sid;
  end if;
 else
 if exists(select 1 from jsonb_array_elements(a) x join market_governance.remediation_observation_v1 o
 on o.source_id=sid and o.observed_at=(x->>'observed_at')::timestamptz where o.record_sha256<>sha256(convert_to(x::text,'UTF8'))) then raise exception 'normalized_source_version_conflict'; end if;
 if exists(select 1 from jsonb_array_elements(a) x group by (x->>'observed_at')::timestamptz having count(distinct x::text)>1) then raise exception 'within_commit_source_version_conflict'; end if;
 insert into market_governance.remediation_observation_v1(
 source_id,observed_at,bar_end,open,high,low,close,volume,quote_volume,trade_count,vwap,taker_buy_base_volume,taker_buy_quote_volume,
 open_interest_quantity,open_interest_quote,global_account_long_short_ratio,top_account_long_short_ratio,top_position_long_short_ratio,taker_long_short_ratio,
 funding_rate,funding_interval_hours,depth_bands,current_supply_native,market_cap_current_supply_usd,reference_price_usd,liquidation_price,liquidation_quantity_native,liquidation_side,source_collector_received_at,source_event_id,source_row_number,availability_not_before,availability_basis,record_sha256)
 select sid,(x->>'observed_at')::timestamptz,nullif(x->>'bar_end','')::timestamptz,
 nullif(x->'values'->>'open','')::float8,nullif(x->'values'->>'high','')::float8,nullif(x->'values'->>'low','')::float8,nullif(x->'values'->>'close','')::float8,
 nullif(x->'values'->>'volume','')::float8,nullif(x->'values'->>'quote_volume','')::float8,nullif(x->'values'->>'trade_count','')::bigint,
 nullif(x->'values'->>'vwap','')::float8,nullif(x->'values'->>'taker_buy_base_volume','')::float8,nullif(x->'values'->>'taker_buy_quote_volume','')::float8,
 nullif(x->'values'->>'open_interest_quantity','')::float8,nullif(x->'values'->>'open_interest_quote','')::float8,
 nullif(x->'values'->>'global_account_long_short_ratio','')::float8,nullif(x->'values'->>'top_account_long_short_ratio','')::float8,
 nullif(x->'values'->>'top_position_long_short_ratio','')::float8,nullif(x->'values'->>'taker_long_short_ratio','')::float8,
 nullif(x->'values'->>'funding_rate','')::float8,nullif(x->'values'->>'funding_interval_hours','')::float8,
 x->'values'->'depth_bands',
 nullif(x->'values'->>'current_supply_native','')::numeric,nullif(x->'values'->>'market_cap_current_supply_usd','')::numeric,nullif(x->'values'->>'reference_price_usd','')::numeric,
 nullif(x->'values'->>'liquidation_price','')::numeric,nullif(x->'values'->>'liquidation_quantity_native','')::numeric,
 nullif(x->'values'->>'liquidation_side',''),nullif(x->'values'->>'source_collector_received_at','')::timestamptz,
 nullif(x->'values'->>'source_event_id',''),nullif(x->'values'->>'source_row_number','')::bigint,
 nullif(x->'values'->>'availability_not_before','')::timestamptz,nullif(x->'values'->>'availability_basis',''),sha256(convert_to(x::text,'UTF8'))
 from jsonb_array_elements(a) x on conflict(source_id,observed_at) do nothing;
 select count(*) into n from market_governance.remediation_observation_v1 where source_id=sid;
 end if;
 v=coalesce(p_request->'validation','{}'::jsonb);
 if coalesce((p_request->>'final')::boolean,false) then
  if exists(select 1 from (values ('raw_count'),('valid_count'),('invalid_count'),('duplicate_count'),('outside_count')) k(name)
  where nullif(v->>k.name,'') is null or (v->>k.name)::bigint<0) then raise exception 'final_validation_counts_required'; end if;
  if jsonb_typeof(v->'normalization_passed') is distinct from 'boolean' then raise exception 'final_validation_status_required'; end if;
  if n<>coalesce((v->>'valid_count')::bigint,-1) then raise exception 'final_valid_count_mismatch'; end if;

  if q.source_type in ('massive_option_reference','massive_option_minutes') then
   if v->>'parser_version' is distinct from 'massive_native_options_pilot_20261008_v1'
    or v->>'underlying_identity_basis' is distinct from q.request_json->>'underlying_identity_basis'
    or exists(select 1 from unnest(array['historical_publication_recovered','historical_first_receipt_recovered','strict_historical_replay_eligible','source_coverage_complete','retained_original_revision_history_complete','native_equity_identity_release_verified','quotes_obtained','open_interest_obtained','implied_volatility_obtained','greeks_obtained']) k where v->>k is distinct from 'false')
    then raise exception 'options_final_limits_required';end if;
   if q.source_type='massive_option_minutes' and (
    v->>'source_record_grain' is distinct from 'NATIVE_OPTION_CONTRACT_MINUTE'
    or v->>'source_native_symbol' is distinct from q.symbol or v->>'source_adjusted' is distinct from 'false'
    or v->>'source_request_complete' is distinct from 'true'
    or v->>'source_volume_unit' is distinct from 'NATIVE_OPTION_CONTRACTS'
    or v->>'source_price_unit' is distinct from 'NATIVE_UNADJUSTED_OPTION_TRADE_PRICE'
    or v->>'missing_minutes_are_not_imputed' is distinct from 'true'
    or (v->>'raw_count')::integer>1440 or n>1440) then raise exception 'options_minute_population_required';end if;
  end if;

  if q.source_type='tardis_liquidations' then
   if v->>'source_contract' is distinct from 'BINANCE_FORCE_ORDER_LATEST_PER_SYMBOL_PER1000MS_NOT_ALL_LIQUIDATIONS' or
    v->>'source_unit_basis' is distinct from 'EXCHANGE_NATIVE_AMOUNT_NO_IMPLICIT_CONTRACT_MULTIPLIER' or
    v->>'count_basis' is distinct from 'RETAINED_PROVIDER_NATIVE_LIQUIDATION_UPDATES' or
    v->>'provider_collector_arrival_retained' is distinct from 'true' or
    v->>'user_historical_first_receipt_recovered' is distinct from 'false' or v->>'strict_replay_certified' is distinct from 'false' or
    (v->>'same_event_timestamp_count')::bigint is distinct from 0 or
    (v->>'exact_repeated_source_rows_retained')::bigint is distinct from 0 or
    (v->>'collector_before_exchange_count')::bigint is distinct from 0 then raise exception 'source_liquidation_final_contract'; end if;
  end if;
  if q.source_type='coinmetrics_supply' then
   if coalesce((v->>'source_valuation_arithmetic_passed')::boolean,false) is not true then raise exception 'source_supply_valuation_arithmetic_unverified'; end if;
   if exists(select 1 from(select count(current_supply_native)sp,count(market_cap_current_supply_usd)cap,count(reference_price_usd)px,
    count(*)filter(where current_supply_native is null)missing_supply from market_governance.remediation_observation_v1 where source_id=sid)c where
    c.sp is distinct from(v->'field_nonnull_counts'->>'SplyCur')::bigint or c.cap is distinct from(v->'field_nonnull_counts'->>'CapMrktCurUSD')::bigint or
    c.px is distinct from(v->'field_nonnull_counts'->>'PriceUSD')::bigint or c.missing_supply is distinct from(v->>'required_supply_missing_rows')::bigint)then raise exception 'source_supply_field_population_mismatch'; end if;
   if exists(select 1 from market_governance.remediation_observation_v1 where source_id=sid and current_supply_native is not null and reference_price_usd is not null and market_cap_current_supply_usd is not null and
   abs(market_cap_current_supply_usd-current_supply_native*reference_price_usd)/greatest(abs(market_cap_current_supply_usd),1)>0.00000001)then raise exception 'source_supply_valuation_arithmetic_failure'; end if;
  end if;
  if q.source_type in ('alpaca_option_daily_panel','alpaca_equity_quotes','alpaca_equity_trades') and
   (v->>'identity_ineligible_count' is null or (v->>'identity_ineligible_count')::bigint<0 or v->>'parser_version' is distinct from 'alpaca_multi_instrument_source_contract_20261008_v2') then raise exception 'final_panel_population_required'; end if;
  if q.source_type not in ('alpaca_option_daily_panel','alpaca_equity_quotes','alpaca_equity_trades') and coalesce((v->>'identity_ineligible_count')::bigint,0)<>0 then raise exception 'final_unsupported_identity_exclusion'; end if;
  if (v->>'raw_count')::bigint<>n+coalesce((v->>'invalid_count')::bigint,0)+coalesce((v->>'duplicate_count')::bigint,0)+coalesce((v->>'outside_count')::bigint,0)+coalesce((v->>'identity_ineligible_count')::bigint,0) then raise exception 'final_source_population_mismatch'; end if;
 end if;
 update market_governance.remediation_source_batch_v1 set current_source_id=sid,valid_count=n,heartbeat_at=clock_timestamp(),
 lease_expires_at=clock_timestamp()+interval '3 minutes',updated_at=clock_timestamp(),
 acquired_count=case when p_request ? 'validation' then (v->>'raw_count')::bigint else acquired_count end,
 invalid_count=case when p_request ? 'validation' then coalesce((v->>'invalid_count')::bigint,0) else invalid_count end,
 duplicate_count=case when p_request ? 'validation' then coalesce((v->>'duplicate_count')::bigint,0) else duplicate_count end,
 outside_count=case when p_request ? 'validation' then coalesce((v->>'outside_count')::bigint,0) else outside_count end,
 validation=case when p_request ? 'validation' then v else validation end,
 attempt_history=case when p_request->>'final'='true' then attempt_history||jsonb_build_array(jsonb_build_object('attempt',attempts,
 'status',case when coalesce((v->>'invalid_count')::bigint,0)>0 or coalesce((v->>'duplicate_conflict_count')::bigint,0)>0 or not coalesce((v->>'normalization_passed')::boolean,false) then 'SOURCE_INVALID' else 'COMPLETE' end,
 'source_id',sid,'acquired_count',(v->>'raw_count')::bigint,'valid_count',n,'invalid_count',coalesce((v->>'invalid_count')::bigint,0),
 'duplicate_count',coalesce((v->>'duplicate_count')::bigint,0),'outside_count',coalesce((v->>'outside_count')::bigint,0),'validation',v,'completed_at',clock_timestamp())) else attempt_history end,
 status=case when p_request->>'final'='true' then case when coalesce((v->>'invalid_count')::bigint,0)>0 or coalesce((v->>'duplicate_conflict_count')::bigint,0)>0 or not coalesce((v->>'normalization_passed')::boolean,false) then 'SOURCE_INVALID' else 'COMPLETE' end else status end,
 completed_at=case when p_request->>'final'='true' then clock_timestamp() else completed_at end
 where batch_id=q.batch_id;
 return jsonb_build_object('stored',true,'source_id',sid,'valid_count',n,'final',coalesce((p_request->>'final')::boolean,false));
end $function$

;

CREATE OR REPLACE FUNCTION public.market_data_remediation_heartbeat_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare g jsonb; n integer;
begin
 g=market_governance.remediation_worker_gate_v1(p_request->>'run_id');
 if not (g->>'ready')::boolean then return g||jsonb_build_object('renewed',false); end if;
 update market_governance.remediation_source_batch_v1 set heartbeat_at=clock_timestamp(),lease_expires_at=clock_timestamp()+interval '3 minutes',updated_at=clock_timestamp()
 where batch_id=(p_request->>'batch_id')::bigint and run_id=p_request->>'run_id' and status='RUNNING'
 and (not exists(select 1 from market_governance.remediation_run_v1 r where r.run_id=p_request->>'run_id' and r.protections#>>'{crypto_pause,active}'='true') or market_governance.remediation_noncrypto_task_v1(to_jsonb(remediation_source_batch_v1)))
 and lease_token=(p_request->>'lease_token')::uuid and worker_id=p_request->>'worker_id' and lease_expires_at>=clock_timestamp();
 get diagnostics n=row_count;
 return g||jsonb_build_object('renewed',n=1);
end $function$

;

CREATE OR REPLACE FUNCTION public.market_data_remediation_claim_v1(p_request jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare rid text=p_request->>'run_id'; wid text=p_request->>'worker_id'; g jsonb; q market_governance.remediation_source_batch_v1%rowtype;
 import_mode boolean=coalesce((p_request->>'import_only')::boolean,false); requested_key text=p_request->>'batch_key'; manifest_sha text=p_request->>'manifest_sha256';
 cohort_mode boolean=coalesce((p_request->>'coinbase_cohort')::boolean,false); control jsonb; generation_tasks bigint; generation_claims bigint; generation_valid boolean; slots integer; batches jsonb; resume_source jsonb; sealed_mode boolean=false; checkpoint market_governance.remediation_coinbase_manifest_checkpoint_v1%rowtype;
begin
 if exists(select 1 from market_governance.remediation_run_v1 where run_id=rid and protections#>>'{crypto_pause,active}'='true') then return jsonb_build_object('status','idle','reason','legacy_source_route_paused_use_noncrypto_route');end if;
 g=market_governance.remediation_worker_gate_v1(rid);
 if not (g->>'ready')::boolean then return g||jsonb_build_object('status','idle'); end if;
 if not import_mode and exists(select 1 from market_governance.remediation_run_v1 where run_id=rid and protections->'source_acquisition'->>'claims_paused'='true') then return g||jsonb_build_object('status','idle','reason','claims_paused_for_drain');end if;
 if wid is null or length(wid)>160 then raise exception 'worker_identity_required'; end if;

 -- Exact finite FOMC acquisition; original options/cohort/import branches stay below unchanged.
 if p_request ? 'fomc_probe_version' then
  if p_request->>'fomc_probe_version' is distinct from 'massive_fomc_two_windows_20261008_v1' or import_mode or cohort_mode or requested_key is not null or manifest_sha is not null or wid like 'saved-payload-import:%' then raise exception 'exact_native_fomc_mode_required';end if;
  select protections#>'{source_acquisition,fomc_two_windows}' into control from market_governance.remediation_run_v1 where run_id=rid;
  if control->>'enabled' is distinct from 'true' or control->>'version' is distinct from 'massive_fomc_two_windows_20261008_v1' or (NOT market_governance.remediation_budgets_removed_v1() AND ((control->>'expires_at')::timestamptz<=clock_timestamp() or (control->>'expires_at')::timestamptz>'2026-10-10T04:15:49.938363Z'::timestamptz)) then return g||jsonb_build_object('status','idle','reason','finite_fomc_probe_disabled_or_expired');end if;
  if exists(select 1 from market_governance.remediation_run_v1 where run_id=rid and (protections#>>'{shared_feature_runner,enabled}'='true' or protections#>>'{native_listing_runner,enabled}'='true' or protections#>>'{source_acquisition,coinbase_cohort,enabled}'='true'))then return g||jsonb_build_object('status','idle','reason','finite_fomc_conflicting_lane');end if;
  if p_request#>>'{source_capabilities,massive_fomc_futures}' is distinct from 'massive_fomc_two_windows_20261008_v1' then return g||jsonb_build_object('status','idle','reason','finite_fomc_capability_required');end if;
  perform pg_advisory_xact_lock(hashtextextended(rid||':source_claim',0));
  update market_governance.remediation_source_batch_v1 set status='FAILED',
   attempt_history=attempt_history||jsonb_build_array(jsonb_build_object('attempt',attempts,'status','FAILED',
    'source_id',current_source_id,'acquired_count',acquired_count,'valid_count',valid_count,'invalid_count',invalid_count,
    'duplicate_count',duplicate_count,'outside_count',outside_count,'validation',validation,
    'error',jsonb_build_object('code','finite_fomc_single_attempt_lease_expired'),'completed_at',clock_timestamp())),
   worker_id=null,last_error=jsonb_build_object('code','finite_fomc_single_attempt_lease_expired'),lease_token=null,lease_expires_at=null,updated_at=clock_timestamp()
   where run_id=rid and batch_key in('massive:fomc:ZQF6:2026-01-28:1845-1900','massive:fomc:ZQJ6:2026-04-29:1745-1800') and status='RUNNING' and attempts>=1 and lease_expires_at<clock_timestamp();
  if exists(select 1 from market_governance.remediation_source_batch_v1 where run_id=rid and status='RUNNING' and lease_expires_at>=clock_timestamp())then return g||jsonb_build_object('status','idle','reason','existing_source_lease');end if;
  select b.* into q from market_governance.remediation_source_batch_v1 b
   join (values
    ('massive:fomc:ZQF6:2026-01-28:1845-1900','ZQF6','2026-01-28T18:45:00Z'::timestamptz,'2026-01-28T19:00:00Z'::timestamptz),
    ('massive:fomc:ZQJ6:2026-04-29:1745-1800','ZQJ6','2026-04-29T17:45:00Z'::timestamptz,'2026-04-29T18:00:00Z'::timestamptz)
   )s(k,t,lo,hi) on b.batch_key=s.k and b.symbol=s.t and b.start_ts=s.lo and b.end_ts=s.hi
   where b.run_id=rid and b.provider='massive' and b.source_type='massive_candles' and b.interval_seconds=60
    and b.status='QUEUED' and b.attempts=0 and b.not_before<=clock_timestamp()
    and b.request_json->>'probe_id'='fm03_native_two_windows_20261008_v1'
    and b.request_json->>'required_parser_version'='massive_fomc_two_windows_20261008_v1'
    and b.request_json->>'required_futures_version'='massive_fomc_two_windows_20261008_v1'
    and b.request_json->>'provider_calls_cap'='1' and b.request_json->>'max_attempts'='1'
    and b.request_json->>'maximum_incremental_cost_usd'='0'
    and b.request_json->>'price_status'='INCLUDED_NO_INCREMENTAL_CHARGE'
    and b.request_json->>'price_evidence'='https://massive.com/docs/rest/futures/aggregates'
   order by b.batch_key for update of b skip locked limit 1;
  if not found then return g||jsonb_build_object('status','idle','reason','finite_fomc_no_ready_batches');end if;
  update market_governance.remediation_source_batch_v1 set status='RUNNING',attempts=1,worker_id=wid,
    lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
    current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
    validation='{}'::jsonb,completed_at=null,updated_at=clock_timestamp(),last_error=null where batch_id=q.batch_id returning * into q;
  return g||jsonb_build_object('status','claimed','batch',to_jsonb(q));
 end if;
 if import_mode then
  if wid not like 'saved-payload-import:%' or requested_key is null or length(requested_key)>512 or manifest_sha !~ '^[0-9a-f]{64}$' or manifest_sha is null then
   raise exception 'exact_saved_payload_import_identity_required';
  end if;
 elsif requested_key is not null or manifest_sha is not null or wid like 'saved-payload-import:%' then
  raise exception 'saved_payload_import_mode_required';
 end if;
 perform pg_advisory_xact_lock(hashtextextended(rid||':source_claim',0));
 update market_governance.remediation_source_batch_v1 set status=case when attempts>=3 or (source_type in ('twelvedata_earnings_probe','massive_option_reference','massive_option_minutes') and attempts>=1) then 'FAILED' else 'RETRY' end,
 attempt_history=attempt_history||jsonb_build_array(jsonb_build_object('attempt',attempts,'status',case when attempts>=3 or (source_type in ('twelvedata_earnings_probe','massive_option_reference','massive_option_minutes') and attempts>=1) then 'FAILED' else 'RETRY' end,
 'source_id',current_source_id,'acquired_count',acquired_count,'valid_count',valid_count,'invalid_count',invalid_count,
 'duplicate_count',duplicate_count,'outside_count',outside_count,'validation',validation,'error',jsonb_build_object('code','expired_worker_lease'),'completed_at',clock_timestamp())),
 last_error=jsonb_build_object('code','expired_worker_lease'),worker_id=null,lease_token=null,lease_expires_at=null,updated_at=clock_timestamp()
 where run_id=rid and status='RUNNING' and lease_expires_at<clock_timestamp();
 if import_mode then
  select * into q from market_governance.remediation_source_batch_v1
  where run_id=rid and batch_key=requested_key and request_json->>'acquisition_mode'='IMPORT_SAVED_PAYLOAD'
    and request_json->>'import_manifest_sha256'=manifest_sha
    and ((status='RUNNING' and worker_id=wid and lease_expires_at>=clock_timestamp()) or status='COMPLETE');
  if found then
   return g||jsonb_build_object('status',case when q.status='COMPLETE' then 'complete' else 'claimed' end,'replayed',true,'batch',to_jsonb(q));
  end if;
 end if;
 if exists(select 1 from market_governance.remediation_source_batch_v1 where run_id=rid and status='RUNNING' and lease_expires_at>=clock_timestamp()) then
  return g||jsonb_build_object('status','idle','reason','existing_source_lease');
 end if;
 -- Exact finite Coinbase generation only. Normal/import claims retain the global
 -- single-lease rule and cannot take any task carrying a cohort generation key.
 if cohort_mode then
  select protections->'source_acquisition'->'coinbase_cohort' into control
  from market_governance.remediation_run_v1 where run_id=rid;
  sealed_mode=control->>'budget_version'='coinbase_sealed_manifest_budget_20261008_v2';
  if import_mode or wid not like 'remediation:%' or p_request->'source_capabilities'->>'coinbase_fetch_cohort' is distinct from 'coinbase_finite_fetch_cohort_20261008_v1'
  or control->>'enabled' is distinct from 'true'
  or control->>'generation_id' is distinct from p_request->>'cohort_generation_id'
  or control->>'manifest_sha256' is distinct from p_request->>'cohort_manifest_sha256'
  or coalesce((p_request->>'max_cohort_tasks')::integer,0) not between 1 and 8
  or coalesce(control->>'generation_id','') !~ '^[A-Za-z0-9_-]{1,80}$'
  or coalesce(control->>'manifest_sha256','') !~ '^[a-f0-9]{64}$'
  or (NOT market_governance.remediation_budgets_removed_v1() AND coalesce((control->>'expires_at')::timestamptz,'-infinity'::timestamptz)<=clock_timestamp())
  or (not coalesce(sealed_mode,false) and ((control->>'max_tasks')::integer not between 1 and 5000
   or (control->>'max_claims')::integer not between (control->>'max_tasks')::integer and 15000))
  or (coalesce(sealed_mode,false) and (control->>'generation_id'<>'coinbase_native_minute_full_remaining_20261008_v1'
   or control->>'manifest_sha256'<>'3ebe9402349311dd2be04951768830badb9a7abe2da4cc8fb6dceb709b65e651'
   or (control->>'max_tasks')::integer<>826993 or (control->>'max_claims')::integer<>2480979
   or p_request->'source_capabilities'->>'coinbase_manifest_budget' is distinct from 'coinbase_sealed_manifest_budget_20261008_v2'))
  or (control->>'max_concurrency')::integer not between 1 and 8
  or (control->>'rate_limit_rps')::numeric not between 0.01 and 2.5
  or ((control->>'max_tasks')::integer>24 and (control->>'pilot_verified' is distinct from 'true' or coalesce(control->>'pilot_evidence_sha256','') !~ '^[a-f0-9]{64}$'))
  then raise exception 'source_cohort_generation_not_authorized'; end if;
  if (control->>'max_tasks') is null or (control->>'max_claims') is null or (control->>'max_concurrency') is null or (control->>'rate_limit_rps') is null then raise exception 'source_cohort_generation_limits_required';end if;
  if coalesce(sealed_mode,false) then
   select * into checkpoint from market_governance.remediation_coinbase_manifest_checkpoint_v1
   where run_id=rid and generation_id=control->>'generation_id' for update;
   if not found or checkpoint.manifest_sha256<>control->>'manifest_sha256'
    or checkpoint.expected_tasks<>(control->>'max_tasks')::integer
    or checkpoint.max_claims<>(control->>'max_claims')::integer
    or (NOT market_governance.remediation_budgets_removed_v1() AND (checkpoint.expires_at<=clock_timestamp()
    or (control->>'expires_at')::timestamptz>checkpoint.expires_at)) then
    raise exception 'source_cohort_immutable_manifest_checkpoint_required';end if;
   generation_tasks=checkpoint.expected_tasks;generation_claims=checkpoint.claims_issued;generation_valid=true;
  else
  select count(*),coalesce(sum(attempts),0),coalesce(bool_and(coalesce(provider='coinbase' and source_type='coinbase_candles'
   and interval_seconds=60 and end_ts-start_ts<=interval '5 hours'
   and request_json->>'cohort_manifest_sha256'=control->>'manifest_sha256'
   and request_json->>'required_cohort_version'='coinbase_finite_fetch_cohort_20261008_v1'
   and request_json->>'required_parser_version'=p_request->'source_capabilities'->>source_type
   and coalesce(request_json->>'acquisition_mode','FETCH')='FETCH',false)),false)
  into generation_tasks,generation_claims,generation_valid
  from market_governance.remediation_source_batch_v1 where run_id=rid and request_json->>'cohort_generation_id'=control->>'generation_id';
  end if;
  if generation_tasks<>(control->>'max_tasks')::integer or not generation_valid then raise exception 'source_cohort_exact_manifest_population_required';end if;
  slots=least((control->>'max_concurrency')::integer,greatest(0,(p_request->>'max_cohort_tasks')::integer),
   CASE WHEN market_governance.remediation_budgets_removed_v1() THEN (control->>'max_concurrency')::integer ELSE greatest(0,(control->>'max_claims')::integer-generation_claims) END);
  if slots is null or slots not between 1 and 8 then return g||jsonb_build_object('status','idle','reason','cohort_finite_generation_claim_limit','generation_claims',generation_claims);end if;
  batches='[]'::jsonb;
  for q in select * from market_governance.remediation_source_batch_v1
   where run_id=rid and request_json->>'cohort_generation_id'=control->>'generation_id'
   and status in('QUEUED','RETRY') and attempts<3 and not_before<=clock_timestamp()
   and request_json->>'required_parser_version'=p_request->'source_capabilities'->>source_type
   order by priority desc,batch_id for update skip locked limit slots
  loop
   resume_source=null;
   if q.current_source_id is not null and (q.last_error->>'code' in('expired_worker_lease','worker_lease_or_stop','worker_stopping') or q.last_error->>'code' like 'rpc_%') then
    select jsonb_build_object('source_id',r.source_id,'role',r.role,'source_url',r.source_url,'http_status',r.http_status,
     'headers',r.response_headers,'received_at',r.received_at,'revision_at',r.revision_at,
     'source_sha256',encode(r.source_sha256,'hex'),'stored_body_sha256',encode(r.stored_body_sha256,'hex'),
     'compressed_sha256',encode(r.compressed_sha256,'hex'),'original_bytes',r.original_bytes,
     'compressed_base64',encode(r.compressed_body,'base64'),'parser_version',r.parser_version,
     'credential_redactions',r.credential_redactions,'provenance',r.provenance)
    into resume_source from market_governance.remediation_source_response_v1 r
    where r.source_id=q.current_source_id and r.batch_id=q.batch_id and r.role='primary' and r.http_status=200
     and r.parser_version=q.request_json->>'required_parser_version' and r.credential_redactions=0
     and r.storage_encoding='gzip' and r.original_bytes between 0 and 1048576 and octet_length(r.compressed_body)<=65536
     and coalesce(r.provenance->>'response_body_complete','true')='true';
   end if;
   update market_governance.remediation_source_batch_v1 set status='RUNNING',attempts=attempts+1,worker_id=wid,
    lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
    current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
    validation='{}'::jsonb,completed_at=null,updated_at=clock_timestamp(),last_error=null
    where batch_id=q.batch_id returning * into q;
   batches=batches||jsonb_build_array(to_jsonb(q)||jsonb_build_object('resume_source',resume_source));
  end loop;
  return g||jsonb_build_object('status',case when jsonb_array_length(batches)>0 then 'cohort_claimed' else 'idle' end,
   'reason',case when jsonb_array_length(batches)=0 then 'cohort_no_ready_tasks' else null end,
   'batches',batches,'generation_id',control->>'generation_id','manifest_sha256',control->>'manifest_sha256',
   'generation_claims_before',generation_claims,'max_generation_claims',CASE WHEN market_governance.remediation_budgets_removed_v1() THEN NULL ELSE (control->>'max_claims')::integer END,'budgets_removed',market_governance.remediation_budgets_removed_v1(),'rate_limit_rps',(control->>'rate_limit_rps')::numeric);
 end if;

 -- Exact finite Binance daily archive generation. Earlier FOMC/import/Coinbase branches and the later ordinary options selector are unchanged.
 if not import_mode then
  select protections#>'{source_acquisition,binance_daily_finite}' into control
  from market_governance.remediation_run_v1 where run_id=rid;
  if control->>'enabled'='true' then
   if wid not like 'remediation:%'
    or control->>'version' is distinct from 'binance_um_daily_finite_manifest_20261008_v1'
    or control->>'generation_id' is distinct from 'binance_um_daily_remaining_17533_20261008_v1'
    or control->>'manifest_sha256' is distinct from '2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c'
    or control->>'seed_complete' is distinct from 'true'
    or control->'max_distinct_tasks' is distinct from '17533'::jsonb
    or control->'max_claims' is distinct from '52599'::jsonb
    or control->'max_source_requests' is distinct from '105198'::jsonb
    or control->'max_attempts_per_task' is distinct from '3'::jsonb
    or control->'minimum_source_request_spacing_seconds' is distinct from '1'::jsonb
    or control->'source_price_usd' is distinct from '0'::jsonb
    or control->'maximum_incremental_cost_usd' is distinct from '0'::jsonb
    or (NOT market_governance.remediation_budgets_removed_v1() AND (control->'source_max_storage_bytes' is distinct from '65000000000'::jsonb
    or control->>'no_new_paid_service_costs' is distinct from 'true'))
    or coalesce(control->>'execution_stage','') not in('MEASUREMENT52','FULL')
    or control->'measurement_distinct_tasks' is distinct from '52'::jsonb
    then return g||jsonb_build_object('status','idle','reason','binance_finite_exact_manifest_control_required');end if;
   if NOT market_governance.remediation_budgets_removed_v1() AND (coalesce((control->>'expires_at')::timestamptz,'-infinity'::timestamptz)<=clock_timestamp()
    or (control->>'expires_at')::timestamptz>'2026-10-10T04:15:49.938363Z'::timestamptz)
    then return g||jsonb_build_object('status','idle','reason','binance_finite_original_expiry_required');end if;
   if exists(select 1 from market_governance.remediation_run_v1 where run_id=rid and
    (protections#>>'{shared_feature_runner,enabled}'='true' or protections#>>'{native_listing_runner,enabled}'='true'
     or protections#>>'{source_acquisition,coinbase_cohort,enabled}'='true'
     or protections#>>'{source_acquisition,fomc_two_windows,enabled}'='true'))then
    return g||jsonb_build_object('status','idle','reason','binance_finite_conflicting_execution_lane');end if;
   if p_request#>>'{source_capabilities,binance_book_depth}' is distinct from 'binance_depth_profile_remediation_v2'
    or p_request#>>'{source_capabilities,binance_metrics}' is distinct from 'binance_archive_source_rules_20261008_v1'
    then return g||jsonb_build_object('status','idle','reason','binance_finite_parser_capabilities_required');end if;
   if NOT market_governance.remediation_budgets_removed_v1() AND ((g->>'max_bytes')::bigint<>65000000000 or (g->>'used_bytes')::bigint+67108864>=(g->>'max_bytes')::bigint) then
    return g||jsonb_build_object('status','idle','reason','binance_finite_source_allocation_required');end if;
   select b.* into q from market_governance.remediation_source_batch_v1 b
    where b.run_id=rid and b.request_json->>'source_generation_id'='binance_um_daily_remaining_17533_20261008_v1'
     and b.status in('QUEUED','RETRY') and b.attempts<3 and b.not_before<=clock_timestamp()
     and (control->>'execution_stage'='FULL' or (b.start_ts='2025-09-01T00:00:00Z'::timestamptz and b.attempts=0))
    order by b.priority desc,b.batch_id for update of b skip locked limit 1;
   if not found then return g||jsonb_build_object('status','idle','reason','binance_finite_no_ready_files','generation_id','binance_um_daily_remaining_17533_20261008_v1');end if;
   if market_governance.remediation_binance_daily_identity_v1(q) is distinct from true then raise exception 'binance_finite_claimed_identity_mismatch';end if;
   update market_governance.remediation_source_batch_v1 set status='RUNNING',attempts=attempts+1,worker_id=wid,
    lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
    current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
    validation='{}'::jsonb,completed_at=null,updated_at=clock_timestamp(),last_error=null
    where batch_id=q.batch_id returning * into q;
   return g||jsonb_build_object('status','claimed','batch',to_jsonb(q),'generation_id','binance_um_daily_remaining_17533_20261008_v1','manifest_sha256','2825d3786055e65d9aa27e4947b836ce5a9b43cfcbb3ab35f20a23b874b5196c');
  end if;
 end if;
 select b.* into q
 from unnest(case when import_mode then ARRAY[requested_key] else ARRAY['massive:options:pilot-v1:1d009a6da92d33f39a6a8723b2621461514da0e0deb83b45d717c28683376e7c:minutes:recovery:429-v1','massive:options:pilot-v1:6820e2108a87f03e78e2860c89be43acbcaa87638d2c75c2ca58f881f59ba5cf:minutes:recovery:429-v1','massive:options:pilot-v1:cdcd01811ce03ccbf260a87192b1a6b3be3cd6516b3f910cbd084dce74b1e910:minutes:recovery:429-v1'] end) as allowed(eligible_batch_key)
 join market_governance.remediation_source_batch_v1 b
  on b.run_id=rid and b.batch_key=allowed.eligible_batch_key
 where status in ('QUEUED','RETRY') and attempts<3 and not_before<=clock_timestamp()
 -- Finite ordinary fetch window: exact options pilot only. Earlier Coinbase branch and saved-payload import remain intact.
 and (import_mode or (
      source_type='massive_option_minutes'
      and request_json->'recovery'->>'recovery_id'='massive_options_429_recovery_20261008_v1'
      and p_request->'source_capabilities'->>'massive_option_minutes_429_recovery'='massive_options_429_recovery_20261008_v1'
      and request_json->>'pilot_id'='intended_cohort_options_native_pilot_20261008_v1'
      and not exists (
       select 1 from unnest(ARRAY['massive:options:pilot-v1:1d009a6da92d33f39a6a8723b2621461514da0e0deb83b45d717c28683376e7c:minutes:recovery:429-v1','massive:options:pilot-v1:6820e2108a87f03e78e2860c89be43acbcaa87638d2c75c2ca58f881f59ba5cf:minutes:recovery:429-v1','massive:options:pilot-v1:cdcd01811ce03ccbf260a87192b1a6b3be3cd6516b3f910cbd084dce74b1e910:minutes:recovery:429-v1']) recent_key(batch_key)
       join market_governance.remediation_source_batch_v1 recent
        on recent.run_id=rid and recent.batch_key=recent_key.batch_key
       where recent.attempts>0 and recent.updated_at>clock_timestamp()-interval '20 seconds')
      and request_json->>'required_parser_version'='massive_native_options_pilot_20261008_v1'
      and market_governance.remediation_options_candidate_v1(request_json->>'candidate_id') is not null
      and attempts=0 and p_request->'source_capabilities'->>source_type='massive_native_options_pilot_20261008_v1'))
 and (source_type not in ('massive_option_reference','massive_option_minutes') or
      (attempts=0 and p_request->'source_capabilities'->>source_type='massive_native_options_pilot_20261008_v1'))
 and (source_type<>'massive_option_minutes' or market_governance.remediation_options_minute_proof_v1(to_jsonb(b)))
 and (source_type<>'twelvedata_earnings_probe' or
      (attempts=0 and p_request->'source_capabilities'->>'twelvedata_earnings_probe'='twelvedata_earnings_included_quota_probe_20261008_v1'))
 and case when import_mode then
  batch_key=requested_key and request_json->>'acquisition_mode'='IMPORT_SAVED_PAYLOAD' and request_json->>'import_manifest_sha256'=manifest_sha
 else coalesce(request_json->>'acquisition_mode','FETCH')<>'IMPORT_SAVED_PAYLOAD'
  and request_json->>'cohort_generation_id' is null
  and (
    (provider in ('coinbase','binance_archive','twelvedata','massive') and source_type<>'massive_reference_tickers'
     and (p_request->'source_capabilities' is null
          or coalesce(request_json->>'required_parser_version','')=''
          or p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version'))
    or (provider='alpaca' and source_type in ('alpaca_option_bars','alpaca_equity_quotes_probe','alpaca_equity_trades_probe')
        and request_json->>'required_parser_version'='alpaca_historical_source_contract_20261008_v1'
        and p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version')
    or (provider='massive' and source_type='massive_reference_tickers'
        and request_json->>'required_parser_version'='massive_native_reference_page_20261008_v1'
        and p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version')
    or (provider='massive' and source_type='massive_reference_tickers'
        and request_json->>'required_parser_version'='massive_native_warmup_reference_page_20261008_v1'
        and request_json->>'warmup_contract'='RTH_DAILY_LOOKBACK61_PREWINDOW_2025-06-04_2025-08-29_V1'
        and p_request->'source_capabilities'->>'massive_warmup_reference'=request_json->>'required_parser_version')
    or (provider='alpaca' and source_type in ('alpaca_option_daily_panel','alpaca_equity_quotes','alpaca_equity_trades')
        and request_json->>'required_parser_version'='alpaca_multi_instrument_source_contract_20261008_v2'
        and p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version'
        and (request_json->>'required_continuation_version' IS NULL
             or (request_json->>'required_continuation_version'='alpaca_native_tick_cursor_chain_20261008_v1'
                 and p_request->'source_capabilities'->>'alpaca_tick_continuation'=request_json->>'required_continuation_version')))
    or (provider='alpaca' and source_type='alpaca_assets_snapshot'
        and request_json->>'required_parser_version'='alpaca_current_asset_metadata_20261008_v1'
        and p_request->'source_capabilities'->>source_type=request_json->>'required_parser_version')
  ) end
 order by priority desc,batch_id for update of b skip locked limit 1;
 if not found then return g||jsonb_build_object('status','idle','reason','no_ready_batches'); end if;
 update market_governance.remediation_source_batch_v1 set status='RUNNING',attempts=attempts+1,worker_id=wid,
 lease_token=gen_random_uuid(),lease_expires_at=clock_timestamp()+interval '3 minutes',heartbeat_at=clock_timestamp(),
 current_source_id=null,acquired_count=0,valid_count=0,invalid_count=0,duplicate_count=0,outside_count=0,
 validation='{}'::jsonb,completed_at=null,updated_at=clock_timestamp(),last_error=null where batch_id=q.batch_id returning * into q;
 return g||jsonb_build_object('status','claimed','batch',to_jsonb(q));
end $function$

;
