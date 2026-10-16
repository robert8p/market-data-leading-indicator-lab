-- Ensure every symbol checkpoint explicitly records the frozen feature-definition
-- version, independently of the row-level quality metadata.
create or replace function research_hub.materialize_next_crypto_crossvenue_symbol_v1()
returns jsonb language plpgsql security invoker set search_path=research_hub,public,pg_temp as $$
declare v_symbol text; v_result jsonb; v_attempts integer; v_definition_version text;
begin
 if not pg_try_advisory_xact_lock(hashtext('research_hub_crossvenue_materialize_v1')::bigint) then return jsonb_build_object('status','busy','holdout_accessed',false); end if;
 select partition_key,coalesce((metadata->>'attempts')::integer,0) into v_symbol,v_attempts from research_hub.feature_materialization_checkpoints where feature_set_key='crypto.crossvenue.sync.v1' and status in ('queued','failed') and coalesce((metadata->>'attempts')::integer,0)<4 order by case when status='failed' then 1 else 0 end,partition_key for update skip locked limit 1;
 if v_symbol is null then return jsonb_build_object('status','all_available_tasks_resolved','holdout_accessed',false); end if;
 update research_hub.feature_materialization_checkpoints set status='running',last_error=null,metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('attempts',v_attempts+1),updated_at=now() where feature_set_key='crypto.crossvenue.sync.v1' and partition_key=v_symbol;
 begin
  v_result:=research_hub.materialize_crypto_crossvenue_symbol_v1(v_symbol);
  v_definition_version:=coalesce(v_result->>'feature_definition_version',(select metadata->>'definition_version' from research_hub.feature_sets where feature_set_key='crypto.crossvenue.sync.v1'));
  update research_hub.feature_materialization_checkpoints set status='completed',row_count=(v_result->>'feature_rows')::bigint,last_source_ts=(select max(source_ts) from research_hub.crypto_crossvenue_observations_v1 where symbol=v_symbol),code_version='crypto_crossvenue_sync_v1',last_error=null,metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object('outcome_rows',(v_result->>'outcome_rows')::bigint,'holdout_accessed',false,'definition_version',v_definition_version),updated_at=now() where feature_set_key='crypto.crossvenue.sync.v1' and partition_key=v_symbol;
  return v_result||jsonb_build_object('status','completed');
 exception when others then
  update research_hub.feature_materialization_checkpoints set status='failed',last_error=left(sqlerrm,4000),updated_at=now() where feature_set_key='crypto.crossvenue.sync.v1' and partition_key=v_symbol;
  return jsonb_build_object('symbol',v_symbol,'status','failed','error',sqlerrm,'holdout_accessed',false);
 end;
end $$;