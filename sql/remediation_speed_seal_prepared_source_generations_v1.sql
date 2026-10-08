SET LOCAL statement_timeout='90s';
SET LOCAL lock_timeout='15s';
SET LOCAL work_mem='32MB';
-- Prevent task identity/population changes between verification and sealing.
LOCK TABLE market_governance.remediation_source_batch_v1 IN SHARE ROW EXCLUSIVE MODE;
DO $seal$
DECLARE r market_governance.remediation_run_v1%rowtype; total bigint; bad bigint; touched bigint; digest text;
 pilot_n bigint; pilot_valid bigint; pilot_digest text; binance_n bigint; binance_bad bigint; binance_done bigint;
 evidence jsonb; sealed timestamptz:=clock_timestamp();
BEGIN
 SELECT count(*),count(*) FILTER(WHERE status<>'QUEUED' OR attempts<>0 OR worker_id IS NOT NULL OR lease_token IS NOT NULL),
 count(*) FILTER(WHERE provider<>'coinbase' OR source_type<>'coinbase_candles' OR interval_seconds<>60
  OR start_ts<'2025-09-01Z'::timestamptz OR end_ts>'2026-08-01Z'::timestamptz OR end_ts<=start_ts OR end_ts-start_ts>interval '5 hours'
  OR request_json->>'cohort_manifest_sha256' IS DISTINCT FROM '3ebe9402349311dd2be04951768830badb9a7abe2da4cc8fb6dceb709b65e651'
  OR request_json->>'required_parser_version' IS DISTINCT FROM 'coinbase_candle_source_rules_20261008_v1'
  OR request_json->>'required_cohort_version' IS DISTINCT FROM 'coinbase_finite_fetch_cohort_20261008_v1'),
 encode(sha256(convert_to(string_agg(batch_key||chr(10),'' ORDER BY batch_key COLLATE "C"),'UTF8')),'hex')
 INTO total,touched,bad,digest FROM market_governance.remediation_source_batch_v1
 WHERE run_id='market_data_remediation_20261008_v1' AND request_json->>'cohort_generation_id'='coinbase_native_minute_full_remaining_20261008_v1';
 IF total<>826993 OR bad<>0 OR touched<>0 OR digest<>'201869ed32f1a86ad22f82909cb8d38bf6b3113663782a45eba685e5a2695296' THEN RAISE EXCEPTION 'speed_coinbase_prepared_population_mismatch';END IF;
 SELECT count(*),count(*) FILTER(WHERE status='COMPLETE' AND validation->>'normalization_passed'='true' AND invalid_count=0 AND current_source_id IS NOT NULL),
 encode(sha256(convert_to(string_agg(batch_key||':'||current_source_id::text||':'||validation::text,chr(10) ORDER BY batch_key COLLATE "C"),'UTF8')),'hex')
 INTO pilot_n,pilot_valid,pilot_digest FROM market_governance.remediation_source_batch_v1
 WHERE run_id='market_data_remediation_20261008_v1' AND request_json->>'cohort_generation_id'='coinbase_native_minute_pilot24_20261008_v1';
 IF pilot_n<>24 OR pilot_valid<>24 OR pilot_digest<>'1e1c67a3e6a203220385441e487779c645f9fad41c3d65880a762ce8f7ce4c8e' THEN RAISE EXCEPTION 'speed_coinbase_pilot_changed';END IF;
 SELECT count(*),count(*) FILTER(WHERE NOT market_governance.remediation_binance_daily_identity_v1(b)
  OR (status='COMPLETE' AND (validation->>'normalization_passed' IS DISTINCT FROM 'true' OR validation->>'published_archive_checksum_verified' IS DISTINCT FROM 'true' OR invalid_count<>0 OR current_source_id IS NULL))),
  count(*) FILTER(WHERE status='COMPLETE') INTO binance_n,binance_bad,binance_done
 FROM market_governance.remediation_source_batch_v1 b WHERE run_id='market_data_remediation_20261008_v1' AND request_json->>'source_generation_id'='binance_um_daily_remaining_17533_20261008_v1';
 IF binance_n<>17533 OR binance_bad<>0 OR binance_done<>52 THEN RAISE EXCEPTION 'speed_binance_preparation_changed';END IF;
 SELECT * INTO STRICT r FROM market_governance.remediation_run_v1 WHERE run_id='market_data_remediation_20261008_v1' AND project_ref='oxzabweahkoimtevbbny' FOR NO KEY UPDATE;
 IF r.phase<>3 OR r.status<>'IN_PROGRESS' OR r.protections#>>'{user_stop,active}'='true'
  OR r.protections#>>'{source_acquisition,claims_paused}' IS DISTINCT FROM 'true'
  OR EXISTS(SELECT 1 FROM market_governance.remediation_source_batch_v1 WHERE run_id=r.run_id AND status='RUNNING')
  OR NOT market_governance.remediation_budgets_removed_v1() THEN RAISE EXCEPTION 'speed_seal_active_paused_run_required';END IF;
 evidence:=jsonb_build_object('seed_completion_change_id','CD-F06:COINBASE_FULL_REMAINING_SEED_V1:COMPLETE',
  'seed_completion_after_sha256','8906c2513d2b39a44238b9f25f04554258aee3123ecaeb56fe887fdcf2156fdd',
  'live_tasks',total,'live_invalid',bad,'live_touched',touched,'key_digest_collation','C; LF terminated',
  'key_sha256',digest,'pilot_tasks',pilot_n,'pilot_valid',pilot_valid,'pilot_evidence_sha256',pilot_digest,
  'binance_tasks',binance_n,'binance_invalid',binance_bad,'binance_measurement_complete',binance_done,
  'source_claims_remain_paused',true,'budget_expiry_enforced',false,'unlimited_policy_change_id','OPS:BUDGET_REMOVAL:20261008_130925');
 IF NOT EXISTS(SELECT 1 FROM market_governance.remediation_change_v1 WHERE change_id='CD-F06:COINBASE_FULL_REMAINING_SEED_V1:COMPLETE' AND after_sha256='8906c2513d2b39a44238b9f25f04554258aee3123ecaeb56fe887fdcf2156fdd') THEN RAISE EXCEPTION 'speed_original_seed_receipt_changed';END IF;
 INSERT INTO market_governance.remediation_coinbase_manifest_checkpoint_v1(run_id,generation_id,manifest_sha256,expected_tasks,max_claims,claims_issued,sealed_at,expires_at,seed_evidence,task_identity_sha256,updated_at)
 VALUES(r.run_id,'coinbase_native_minute_full_remaining_20261008_v1','3ebe9402349311dd2be04951768830badb9a7abe2da4cc8fb6dceb709b65e651',826993,2480979,0,sealed,sealed+interval '7 days',evidence,digest,clock_timestamp());
 INSERT INTO market_governance.remediation_change_v1(change_id,run_id,finding_ids,relation_name,row_identity,after_row,after_sha256,operation,evidence,validation)
 VALUES('OPS:SPEED_PREPARED_SOURCES:20261008_V1',r.run_id,ARRAY['CD-F01','CD-F06','CD-F07'],'market_governance.remediation_coinbase_manifest_checkpoint_v1',jsonb_build_object('generation_id','coinbase_native_minute_full_remaining_20261008_v1'),evidence,
 encode(sha256(convert_to(evidence::text,'UTF8')),'hex'),'VERIFIED_SOURCE_MANIFEST_SEALED_WITHOUT_ACTIVATION',
 jsonb_build_object('authorization_change_id','OPS:SPEED_DEPLOYMENT:20261008_V1'),jsonb_build_object('source_preparation_passed',true,'source_http_calls',0,'new_paid_service_activated',false));
END $seal$;
