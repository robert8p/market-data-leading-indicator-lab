-- ROOT ONLY guarded performance correction to an installed but never-admitted repair helper.
-- No original producer/helper/input versions are changed. No binding or completed work may exist.
BEGIN;
SET LOCAL lock_timeout='3s';SET LOCAL statement_timeout='15s';
SELECT pg_advisory_xact_lock(2026083000261022);
DO $guard$
BEGIN
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(uuid,text)'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '2fffcec72a958d565b2d87e764ef2ec30d0fe9b611c769ffcca1239c8fc285e0' THEN RAISE EXCEPTION 'D02_REPAIR_UNTESTED_HELPER_CHANGED_RECONCILE_FIRST';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2) THEN RAISE EXCEPTION 'D02_REPAIR_ALREADY_ADMITTED_DO_NOT_REWRITE';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') THEN RAISE EXCEPTION 'D02_REPAIR_PAUSED_DRAINED_WINDOW_REQUIRED';END IF;
 IF population_top3000_202608_202609_v1.d02_grouped_bundle_sha256_v1() IS DISTINCT FROM '3e267e75a2c471ca6975c9ffe3dc9baabf93640500398992d9f32bf0482085da' THEN RAISE EXCEPTION 'D02_REPAIR_ORIGINAL_SOURCE_BUNDLE_CHANGED';END IF;
END $guard$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' SET timezone TO 'UTC' SET lock_timeout TO '500ms' AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;b population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2%rowtype;
 ctx jsonb;last_key text;end_key text;batch_keys text[];batch_rows integer;fresh integer;updated integer;completed_count integer;old_count integer;
 h text;baseline_hash text;relations bigint;began timestamptz:=clock_timestamp();elapsed_ms numeric;is_terminal boolean;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 ctx:=population_top3000_202608_202609_v1.d02_grouped_repair_context_v2(tid);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 SELECT * INTO b FROM population_top3000_202608_202609_v1.d02_grouped_repair_binding_v2 WHERE task_id=tid FOR UPDATE;
 PERFORM population_top3000_202608_202609_v1.d02_grouped_repair_resource_v2();
 last_key:=coalesce(t.checkpoint->>'last_derived_unit_key','');old_count:=coalesce((t.checkpoint->>'derived_normalized_rows')::integer,0);
 IF t.checkpoint<>'{}'::jsonb AND (t.checkpoint->>'repair_binding_sha256' IS DISTINCT FROM b.binding_sha256 OR t.checkpoint->>'repair_bundle_sha256' IS DISTINCT FROM b.repair_bundle_sha256)
 OR old_count<0 OR old_count>b.normalized_rows THEN RAISE EXCEPTION 'D02_REPAIR_CHECKPOINT_VERSION_MISMATCH';END IF;
 IF old_count<>(SELECT count(*) FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key<=last_key AND q.post_record_sha256 IS NOT NULL) THEN RAISE EXCEPTION 'D02_REPAIR_CHECKPOINT_PREFIX_MISMATCH';END IF;
 SELECT array_agg(x.unit_key ORDER BY x.unit_key),count(*),max(x.unit_key) INTO batch_keys,batch_rows,end_key
 FROM (SELECT o.unit_key FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.vintage_key=b.archive_key AND o.unit_key>last_key ORDER BY o.unit_key LIMIT 500) x;
 IF batch_rows=0 AND b.normalized_rows>0 THEN RAISE EXCEPTION 'D02_REPAIR_EMPTY_BATCH_BEFORE_COMPLETION';END IF;
 -- Reject unrecorded prior derivation rather than overwriting it. Preserve every original normalized JSON record once.
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.unit_key=ANY(batch_keys)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=o.release_id AND q.family_id=o.family_id AND q.partition_key=o.partition_key AND q.unit_key=o.unit_key)
 AND (o.lineage ? 'derivation_function_sha256' OR o.values_json ? 'dollar_volume_vwap_approximation')) THEN RAISE EXCEPTION 'D02_REPAIR_UNRECORDED_DERIVED_STATE';END IF;
 INSERT INTO population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2(task_id,release_id,family_id,partition_key,unit_key,source_id,source_record_key,normalization_sha256,pre_values_json,pre_field_state,pre_lineage,pre_record_sha256,first_attempt_id)
 SELECT tid,o.release_id,o.family_id,o.partition_key,o.unit_key,o.source_id,o.source_record_key,o.normalization_sha256,o.values_json,o.field_state,o.lineage,o.record_sha256,p_attempt FROM population_top3000_202608_202609_v1.source_observation o
 WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.unit_key=ANY(batch_keys)
 AND o.vintage_key=b.archive_key AND o.source_id=b.source_id AND o.normalization_sha256=b.original_normalizer_sha256
 ON CONFLICT(task_id,release_id,family_id,partition_key,unit_key) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys))<>batch_rows THEN RAISE EXCEPTION 'D02_REPAIR_BASELINE_PRESERVATION_MISMATCH';END IF;
 -- Every CTE is materialized once. Conflict enumeration scans only this 12,402-row retained partition.
 -- The update joins the complete existing primary key, including release_id.
 WITH conflicts AS MATERIALIZED (
  SELECT o.source_record_key FROM population_top3000_202608_202609_v1.source_observation o WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.vintage_key=b.archive_key GROUP BY o.source_record_key HAVING count(*)>1
 ), batch AS MATERIALIZED (
  SELECT q.* FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys)
 ), transformed AS MATERIALIZED (
  SELECT q.*,
   q.pre_values_json||jsonb_build_object('dollar_volume_vwap_approximation',CASE WHEN q.pre_field_state->>'share_volume'='VALUE' AND q.pre_field_state->>'provider_vwap'='VALUE' THEN (q.pre_values_json->>'share_volume')::numeric*(q.pre_values_json->>'provider_vwap')::numeric ELSE NULL END) out_values,
   q.pre_field_state||jsonb_build_object('dollar_volume_vwap_approximation',CASE WHEN q.pre_field_state->>'share_volume'='VALUE' AND q.pre_field_state->>'provider_vwap'='VALUE' THEN 'VALUE' ELSE 'UNAVAILABLE' END,'ticker',CASE WHEN cf.source_record_key IS NOT NULL THEN 'CONFLICTING' ELSE q.pre_field_state->>'ticker' END) out_states,
   q.pre_lineage||jsonb_build_object('derivation_function_sha256',b.repair_deriver_sha256,'derivation_revision','massive-grouped-vwap-approximation-20261023-v2-bounded-repair','dollar_volume_formula','provider_vwap * share_volume; rounded-source approximation, not independently summed trade dollars','raw_price_adjustment_applied',false,'repair_binding_sha256',b.binding_sha256,'pre_derivation_record_sha256',q.pre_record_sha256,'original_source_bundle_sha256',b.original_bundle_sha256) out_lineage
  FROM batch q LEFT JOIN conflicts cf ON cf.source_record_key=q.source_record_key
 ), expected AS MATERIALIZED (
  SELECT x.*,encode(sha256(convert_to(jsonb_build_object('values',x.out_values,'states',x.out_states,'lineage',x.out_lineage)::text,'UTF8')),'hex') out_hash FROM transformed x
 )
 UPDATE population_top3000_202608_202609_v1.source_observation o SET values_json=x.out_values,field_state=x.out_states,lineage=x.out_lineage,record_sha256=x.out_hash
 FROM expected x WHERE o.release_id=x.release_id AND o.family_id=x.family_id AND o.partition_key=x.partition_key AND o.unit_key=x.unit_key
 AND o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.vintage_key=b.archive_key
 AND o.source_id=x.source_id AND o.source_record_key=x.source_record_key AND o.normalization_sha256=x.normalization_sha256
 AND ((o.record_sha256=x.pre_record_sha256 AND o.values_json=x.pre_values_json AND o.field_state=x.pre_field_state AND o.lineage=x.pre_lineage) OR (o.record_sha256=x.out_hash AND o.values_json=x.out_values AND o.field_state=x.out_states AND o.lineage=x.out_lineage));
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>batch_rows THEN RAISE EXCEPTION 'D02_REPAIR_EXISTING_ROW_REPLAY_CONFLICT';END IF;
 -- Materialize only this batch's post-update keys and hashes. Both sides are explicitly bounded to the same <=500 keys.
 -- The original unrestricted source join chose a repeated 12,402-row scan despite the full-key equality.
 WITH observed_batch AS MATERIALIZED (
  SELECT o.release_id,o.family_id,o.partition_key,o.unit_key,o.record_sha256
  FROM population_top3000_202608_202609_v1.source_observation o
  WHERE o.release_id=b.release_id AND o.family_id='D02' AND o.partition_key=b.partition_key AND o.unit_key=ANY(batch_keys)
 )
 UPDATE population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q SET post_record_sha256=o.record_sha256
 FROM observed_batch o WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys)
 AND o.release_id=q.release_id AND o.family_id=q.family_id AND o.partition_key=q.partition_key AND o.unit_key=q.unit_key
 AND (q.post_record_sha256 IS NULL OR q.post_record_sha256=o.record_sha256);
 GET DIAGNOSTICS updated=ROW_COUNT;
 IF updated<>batch_rows THEN RAISE EXCEPTION 'D02_REPAIR_IMMUTABLE_OUTPUT_HASH_CONFLICT';END IF;
 completed_count:=old_count+batch_rows;is_terminal:=completed_count=b.normalized_rows;
 IF completed_count>b.normalized_rows THEN RAISE EXCEPTION 'D02_REPAIR_COUNT_EXCEEDED';END IF;
 SELECT encode(sha256(convert_to(coalesce(string_agg(q.unit_key||':'||q.post_record_sha256,E'\n' ORDER BY q.unit_key),''),'UTF8')),'hex') INTO h
 FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key AND q.unit_key=ANY(batch_keys);
 IF is_terminal THEN
  SELECT encode(sha256(convert_to(coalesce(string_agg(q.unit_key||':'||q.pre_record_sha256,E'\n' ORDER BY q.unit_key),''),'UTF8')),'hex') INTO baseline_hash
  FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.release_id=b.release_id AND q.family_id='D02' AND q.partition_key=b.partition_key;
  IF baseline_hash IS DISTINCT FROM b.baseline_normalized_sha256 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.d02_grouped_repair_baseline_v2 q WHERE q.task_id=tid AND q.post_record_sha256 IS NOT NULL)<>b.normalized_rows THEN RAISE EXCEPTION 'D02_REPAIR_FULL_BASELINE_RECONCILIATION_FAILED';END IF;
 END IF;
 relations:=population_top3000_202608_202609_v1.d02_grouped_repair_resource_v2();elapsed_ms:=round(extract(epoch FROM clock_timestamp()-began)*1000,3);
 RETURN jsonb_build_object('state',CASE WHEN is_terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('last_derived_unit_key',coalesce(end_key,last_key),'derived_normalized_rows',completed_count,'repair_binding_sha256',b.binding_sha256,'repair_bundle_sha256',b.repair_bundle_sha256),
 'commit_receipt',jsonb_build_object('commit_key','d02-bounded-repair:'||tid::text||':'||coalesce(end_key,'EMPTY')||':'||b.repair_bundle_sha256,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(b.archive_key),'input_versions',t.input_versions,'repair_binding_sha256',b.binding_sha256,'repair_bundle_sha256',b.repair_bundle_sha256,'source_bundle_sha256',b.original_bundle_sha256,'batch_rows',batch_rows,'derived_normalized_rows',completed_count,'total_normalized_rows',b.normalized_rows,'pre_derivation_evidence_preserved',true,'mission_relation_bytes',relations,'elapsed_ms',elapsed_ms,'cohort_certified',false,'company_cap_promoted',false,'regular_close_independently_verified',false));
END $f$;
SELECT jsonb_build_object('repair_deriver_sha256',encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_grouped_derive_repair_v2(uuid,text)'::regprocedure),'UTF8')),'hex'),'repair_bundle_sha256',population_top3000_202608_202609_v1.d02_grouped_repair_bundle_sha256_v2(),'original_bundle_sha256',population_top3000_202608_202609_v1.d02_grouped_bundle_sha256_v1(),'registered',false,'cron_still_paused',NOT EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active));
COMMIT;
