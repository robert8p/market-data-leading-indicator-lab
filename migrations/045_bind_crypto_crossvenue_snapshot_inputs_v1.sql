-- Bind the eventual cross-venue experiment to immutable manifest hashes of the
-- exact materialised feature and outcome partitions. Same snapshot key + changed
-- manifest is a hard error rather than silent research drift.

insert into research_hub.datasets(dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,status,metadata)
values
('derived.crypto_crossvenue_features_v1','market_data_primary','research_hub','feature_rows','crypto','research_hub','1m','canonical-symbol-minute','decision_ts','instrument_key','observable_at',false,true,'materializing',jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v1','definition_version','crypto-crossvenue-sync-v1.1','derived_only',true)),
('derived.crypto_crossvenue_outcomes_v1','market_data_primary','research_hub','outcome_rows','crypto','research_hub','mixed','target-symbol-decision-horizon','decision_ts','instrument_key',null,false,false,'materializing',jsonb_build_object('outcome_set_key','crypto.crossvenue.nextopen.v1','future_label_only',true,'never_predictor',true))
on conflict(dataset_key) do update set status=excluded.status,metadata=excluded.metadata,updated_at=now();

create or replace function research_hub.bind_crypto_crossvenue_experiment_inputs_v1(p_run_id uuid)
returns jsonb language plpgsql security invoker set search_path=research_hub,pg_temp as $$
declare
 v_total bigint; v_completed bigint; v_feature_rows bigint; v_outcome_rows bigint; v_watermark timestamptz;
 v_manifest_text text; v_feature_hash text; v_outcome_hash text; v_feature_snapshot uuid; v_outcome_snapshot uuid;
 v_existing_hash text; v_definition_version text; v_run_key text;
begin
 select run_key into v_run_key from research_hub.experiment_runs where run_id=p_run_id;
 if v_run_key is null then raise exception 'Unknown experiment run %',p_run_id; end if;
 select count(*),count(*) filter(where status='completed'),coalesce(sum(row_count),0),coalesce(sum((metadata->>'outcome_rows')::bigint) filter(where metadata ? 'outcome_rows'),0),max(last_source_ts)
 into v_total,v_completed,v_feature_rows,v_outcome_rows,v_watermark
 from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v1';
 if v_total=0 or v_completed<>v_total then raise exception 'Cross-venue materialisation incomplete: %/% completed',v_completed,v_total; end if;
 select metadata->>'definition_version' into v_definition_version from research_hub.feature_sets where feature_set_key='crypto.crossvenue.sync.v1';
 if v_definition_version is null then raise exception 'Cross-venue feature definition version is not frozen'; end if;
 select string_agg(partition_key||'|'||coalesce(row_count,0)||'|'||coalesce(last_source_ts::text,'')||'|'||coalesce(metadata->>'outcome_rows','0')||'|'||coalesce(metadata->>'definition_version',v_definition_version),E'\n' order by partition_key)
 into v_manifest_text from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v1';
 v_feature_hash:=encode(digest('features|'||v_definition_version||E'\n'||v_manifest_text,'sha256'),'hex');
 v_outcome_hash:=encode(digest('outcomes|crypto.crossvenue.nextopen.v1'||E'\n'||v_manifest_text,'sha256'),'hex');

 select content_hash into v_existing_hash from research_hub.dataset_snapshots where snapshot_key='cv-sync-v1.1-features-20260628-20260728';
 if v_existing_hash is not null and v_existing_hash<>v_feature_hash then raise exception 'Immutable cross-venue feature snapshot drift: existing % new %',v_existing_hash,v_feature_hash; end if;
 insert into research_hub.dataset_snapshots(snapshot_key,dataset_key,start_ts,end_ts,row_count,content_hash,source_watermark,manifest,immutable,hash_type)
 values('cv-sync-v1.1-features-20260628-20260728','derived.crypto_crossvenue_features_v1',timestamptz '2026-06-28 00:01:00+00',timestamptz '2026-07-28 16:53:00+00',v_feature_rows,v_feature_hash,v_watermark::text,jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v1','definition_version',v_definition_version,'symbols',v_total,'manifest_basis','ordered symbol partition row counts, watermarks and outcome counts','adaptive_reuse',true,'promotion_requires_future_replication',true),true,'manifest_sha256')
 on conflict(snapshot_key) do nothing;
 select snapshot_id into v_feature_snapshot from research_hub.dataset_snapshots where snapshot_key='cv-sync-v1.1-features-20260628-20260728';

 select content_hash into v_existing_hash from research_hub.dataset_snapshots where snapshot_key='cv-nextopen-v1-outcomes-20260628-20260728';
 if v_existing_hash is not null and v_existing_hash<>v_outcome_hash then raise exception 'Immutable cross-venue outcome snapshot drift: existing % new %',v_existing_hash,v_outcome_hash; end if;
 insert into research_hub.dataset_snapshots(snapshot_key,dataset_key,start_ts,end_ts,row_count,content_hash,source_watermark,manifest,immutable,hash_type)
 values('cv-nextopen-v1-outcomes-20260628-20260728','derived.crypto_crossvenue_outcomes_v1',timestamptz '2026-06-28 00:01:00+00',timestamptz '2026-07-28 16:53:00+00',v_outcome_rows,v_outcome_hash,v_watermark::text,jsonb_build_object('outcome_set_key','crypto.crossvenue.nextopen.v1','symbols',v_total,'future_label_only',true,'costs_embedded',false,'adaptive_reuse',true),true,'manifest_sha256')
 on conflict(snapshot_key) do nothing;
 select snapshot_id into v_outcome_snapshot from research_hub.dataset_snapshots where snapshot_key='cv-nextopen-v1-outcomes-20260628-20260728';

 insert into research_hub.experiment_inputs(run_id,input_role,dataset_key,snapshot_id,point_in_time_verified,metadata)
 values
 (p_run_id,'features','derived.crypto_crossvenue_features_v1',v_feature_snapshot,true,jsonb_build_object('feature_set_key','crypto.crossvenue.sync.v1','definition_version',v_definition_version)),
 (p_run_id,'future_outcomes','derived.crypto_crossvenue_outcomes_v1',v_outcome_snapshot,false,jsonb_build_object('outcome_set_key','crypto.crossvenue.nextopen.v1','future_label_only',true,'never_predictor',true))
 on conflict(run_id,input_role,dataset_key) do update set snapshot_id=excluded.snapshot_id,point_in_time_verified=excluded.point_in_time_verified,metadata=excluded.metadata;

 update research_hub.experiment_runs set definition_hash=encode(digest(v_run_key||'|'||v_feature_hash||'|'||v_outcome_hash||'|'||coalesce(config::text,'{}'),'sha256'),'hex'),provenance=coalesce(provenance,'{}'::jsonb)||jsonb_build_object('feature_snapshot_id',v_feature_snapshot,'feature_snapshot_hash',v_feature_hash,'outcome_snapshot_id',v_outcome_snapshot,'outcome_snapshot_hash',v_outcome_hash,'input_manifest_bound',true),updated_at=now() where run_id=p_run_id;
 update research_hub.datasets set status='active',row_estimate=v_feature_rows,coverage_start=timestamptz '2026-06-28 00:01:00+00',coverage_end=timestamptz '2026-07-28 16:53:00+00',updated_at=now() where dataset_key='derived.crypto_crossvenue_features_v1';
 update research_hub.datasets set status='active',row_estimate=v_outcome_rows,coverage_start=timestamptz '2026-06-28 00:01:00+00',coverage_end=timestamptz '2026-07-28 16:53:00+00',updated_at=now() where dataset_key='derived.crypto_crossvenue_outcomes_v1';
 return jsonb_build_object('run_id',p_run_id,'feature_snapshot_id',v_feature_snapshot,'feature_hash',v_feature_hash,'feature_rows',v_feature_rows,'outcome_snapshot_id',v_outcome_snapshot,'outcome_hash',v_outcome_hash,'outcome_rows',v_outcome_rows,'holdout_accessed',false);
end $$;

create or replace function research_hub.bind_crypto_crossvenue_inputs_trigger_v1()
returns trigger language plpgsql security invoker set search_path=research_hub,pg_temp as $$
begin
 if new.run_key='RH-CV-SYNC-V1-20260812' then perform research_hub.bind_crypto_crossvenue_experiment_inputs_v1(new.run_id); end if;
 return new;
end $$;

drop trigger if exists trg_bind_crypto_crossvenue_inputs_v1 on research_hub.experiment_runs;
create trigger trg_bind_crypto_crossvenue_inputs_v1 after insert or update of feature_set_key,outcome_set_key,discovery_start,discovery_end,validation_start,validation_end on research_hub.experiment_runs for each row execute function research_hub.bind_crypto_crossvenue_inputs_trigger_v1();