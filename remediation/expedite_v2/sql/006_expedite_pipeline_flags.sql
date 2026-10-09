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
 base:=base||jsonb_build_object('noncrypto_enabled',coalesce(r.protections#>>'{noncrypto_runner_v1,enabled}'='true',false),'benchmark_enabled',coalesce(r.protections#>>'{expedite_v2,benchmark_enabled}'='true',false),'validation_enabled',coalesce(r.protections#>>'{expedite_v2,validation_enabled}'='true',false),'noncrypto_overlap_enabled',coalesce(r.protections#>>'{expedite_v2,acquisition_overlap_enabled}'='true',false));
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
END $function$;

NOTIFY pgrst, 'reload schema';
