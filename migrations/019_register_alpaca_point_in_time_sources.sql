insert into research_hub.datasets
(dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,status,metadata)
values
('alpaca_rapid.pti_universe_snapshots','alpaca_rapid_discovery','public','ra_point_in_time_universe_snapshots','equity','alpaca','snapshot','universe-snapshot','snapshot_date',null,'snapshot_date',false,true,'external_registered','{"purpose":"survivorship_control","join_key":"snapshot_universe_run_id"}'),
('alpaca_rapid.analysis_universe','alpaca_rapid_discovery','public','ra_analysis_universe','equity','alpaca','snapshot','universe-member',null,'symbol',null,false,true,'external_registered','{"purpose":"historical_universe_membership","join_key":"universe_run_id"}'),
('alpaca_rapid.full_history_backfills','alpaca_rapid_discovery','public','ra_full_history_backfills','equity','alpaca','run','feature-backfill-run',null,null,null,false,true,'external_registered','{"purpose":"point_in_time_feature_lineage"}')
on conflict (dataset_key) do update set
    status=excluded.status,
    metadata=excluded.metadata,
    point_in_time_safe=excluded.point_in_time_safe,
    updated_at=now();
