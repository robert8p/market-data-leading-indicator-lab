do $$
begin
    if to_regclass('research.xal_state_feature_panel') is null
       or to_regclass('research.xal_target_outcomes') is null then
        raise notice 'XAL research tables are not present; Research Hub XAL adapter skipped.';
        return;
    end if;

    insert into research_hub.datasets
    (dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,status,metadata)
    values
    ('primary.xal_state_feature_panel','market_data_primary','research','xal_state_feature_panel','multi','multi','15m','research-panel','signal_ts',null,'signal_ts',false,true,'available','{"role":"existing_point_in_time_research_panel"}'),
    ('primary.xal_target_outcomes','market_data_primary','research','xal_target_outcomes','multi','multi','15m','research-outcome','signal_ts',null,null,false,false,'available','{"role":"existing_outcomes"}')
    on conflict (dataset_key) do update set status=excluded.status,metadata=excluded.metadata,updated_at=now();

    insert into research_hub.feature_sets
    (feature_set_key,description,decision_grain,source_dataset_keys,materialization_schema,materialization_relation,point_in_time_verified,verification_notes,metadata)
    values
    ('xal.market_state.v1','Adapter over the existing XAL point-in-time state feature panel','15m',array['primary.xal_state_feature_panel'],'research_hub','feature_rows',true,'Existing XAL signal_ts is the completed and observable signal timestamp.','{"adapter":"research.xal_state_feature_panel","legacy_preserved":true}')
    on conflict (feature_set_key) do update set point_in_time_verified=true,verification_notes=excluded.verification_notes,metadata=excluded.metadata,updated_at=now();

    insert into research_hub.outcome_sets
    (outcome_set_key,description,outcome_keys,materialization_schema,materialization_relation,metadata)
    values
    ('xal.targets.v1','Adapter over existing XAL target outcomes',array['fwd_return_15m','fwd_return_30m','fwd_return_60m','fwd_return_120m'],'research_hub','outcome_rows','{"adapter":"research.xal_target_outcomes","legacy_preserved":true}')
    on conflict (outcome_set_key) do update set outcome_keys=excluded.outcome_keys,metadata=excluded.metadata,updated_at=now();

    insert into research_hub.feature_definitions
    (feature_key,dataset_key,feature_name,feature_family,value_type,source_expression,decision_time_rule,observable_at_rule,lookback_seconds,metadata)
    select distinct 'xal.'||feature,'primary.xal_state_feature_panel',feature,family,'double precision','research.xal_state_feature_panel.value','decision_ts = signal_ts','observable_at = signal_ts',null::integer,jsonb_build_object('legacy_family',family)
    from research.xal_state_feature_panel where feature is not null
    on conflict (feature_key) do nothing;

    insert into research_hub.feature_rows
    (feature_set_key,instrument_key,decision_ts,observable_at,features,source_dataset_key,quality)
    select 'xal.market_state.v1',run_id::text||':'||family,signal_ts,signal_ts,
           jsonb_object_agg(feature,value order by feature) filter(where feature is not null),
           'primary.xal_state_feature_panel',jsonb_build_object('legacy_run_id',run_id,'family',family)
    from research.xal_state_feature_panel
    group by run_id,family,signal_ts
    having count(*) filter(where feature is not null)>0
    on conflict (feature_set_key,instrument_key,decision_ts) do update set observable_at=excluded.observable_at,features=excluded.features,quality=excluded.quality;

    insert into research_hub.outcome_rows
    (outcome_set_key,instrument_key,decision_ts,horizon_seconds,entry_ts,exit_ts,gross_return,metadata)
    select 'xal.targets.v1',run_id::text||':'||target_venue||':'||target_symbol,signal_ts,horizon_minutes*60,entry_ts,exit_ts,gross_return,
           jsonb_build_object('legacy_run_id',run_id,'target_venue',target_venue,'target_symbol',target_symbol)
    from research.xal_target_outcomes
    where horizon_minutes>0 and (exit_ts is null or exit_ts>signal_ts)
    on conflict (outcome_set_key,instrument_key,decision_ts,horizon_seconds) do update set entry_ts=excluded.entry_ts,exit_ts=excluded.exit_ts,gross_return=excluded.gross_return,metadata=excluded.metadata;
end $$;
