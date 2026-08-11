-- Research-hub inventory hardening, 2026-08-11.
-- Idempotent catalogue/control changes only. No raw market data is copied.

update research_hub.datasets
set ts_column='bar_ts', instrument_column='symbol', observable_at_column='bar_ts',
    point_in_time_safe=false,
    coverage_start='2026-06-04 13:30:00+00'::timestamptz,
    coverage_end='2026-08-03 19:50:00+00'::timestamptz,
    row_estimate=7486756,
    metadata=metadata || '{"preferred_for_federated_research":true,"mixed_predictor_outcome_columns":true,"predictor_use_requires_column_allowlist":true,"future_columns":["fwd_return_5m_pct","fwd_return_15m_pct","fwd_return_30m_pct","fwd_return_60m_pct"]}'::jsonb,
    updated_at=now()
where dataset_key='alpaca_rapid.discovery_samples';

update research_hub.datasets
set coverage_start='2017-01-03 14:30:00+00'::timestamptz,
    coverage_end='2026-08-10 23:59:00+00'::timestamptz,
    row_estimate=560572497,
    metadata=metadata || '{"coverage_regime_note":"Historical bars before May 2025 are a narrow benchmark/subset; broad full-universe density begins May 2025.","full_universe_effective_start":"2025-05-05","pre_full_universe_research_use":"benchmark_or_targeted_only"}'::jsonb,
    updated_at=now()
where dataset_key='alpaca_rapid.bars_monthly';

update research_hub.datasets
set coverage_start='2025-05-05 00:00:00+00'::timestamptz,
    coverage_end='2026-08-11 00:00:00+00'::timestamptz,
    metadata=metadata || '{"coverage_kind":"broad_feature_materialization","full_universe_effective_start":"2025-05-05"}'::jsonb,
    updated_at=now()
where dataset_key='alpaca_rapid.intraday_features_monthly';

update research_hub.datasets
set ts_column='decision_ts', instrument_column='symbol', observable_at_column='latest_bar_ts',
    point_in_time_safe=true,
    coverage_start='2024-01-02 17:00:00+00'::timestamptz,
    coverage_end='2024-03-28 17:00:00+00'::timestamptz,
    row_estimate=929152,
    metadata=metadata || '{"specialist_programme":"13.8","point_in_time_basis":"latest_bar_ts <= decision_ts"}'::jsonb,
    updated_at=now()
where dataset_key='alpaca_138.decision_snapshots';

insert into research_hub.datasets
(dataset_key,store_key,schema_name,relation_name,asset_class,provider,frequency,grain,ts_column,instrument_column,observable_at_column,is_raw,point_in_time_safe,coverage_start,coverage_end,row_estimate,status,metadata)
values
('alpaca_rapid.daily_features','alpaca_rapid_discovery','public','rd_daily_features','equity','alpaca','1d','instrument-session','last_bar_ts','symbol','last_bar_ts',false,true,'2025-05-05 00:00:00+00','2026-08-10 23:59:59+00',3430267,'external_registered','{"role":"research_ready_daily_features","availability_rule":"usable only at or after last_bar_ts"}'),
('alpaca_rapid.research_ledger','alpaca_rapid_discovery','public','ra_research_ledger','equity','alpaca','experiment','candidate-research-record',null,null,null,false,false,'2026-06-04 00:00:00+00','2026-08-03 23:59:59+00',143,'external_registered_control','{"role":"experiment_memory","not_a_predictor":true}'),
('alpaca_rapid.candidate_rules','alpaca_rapid_discovery','public','ra_candidate_rules','equity','alpaca','candidate','candidate-rule',null,null,null,false,false,'2026-08-07 10:35:18.063853+00','2026-08-08 02:20:10.569921+00',143,'external_registered_control','{"role":"candidate_memory","contains_validation_and_sealed_results":true,"not_a_predictor":true}'),
('alpaca_rapid.robustness_results','alpaca_rapid_discovery','public','ra_robustness_results','equity','alpaca','experiment','robustness-result',null,null,null,false,false,null,null,2202,'external_registered_control','{"role":"robustness_evidence","not_a_predictor":true}'),
('alpaca_rapid.quality_reports','alpaca_rapid_discovery','public','ra_quality_reports','equity','alpaca','report','quality-report','created_at',null,'created_at',false,null,'2026-08-05 02:24:17.828672+00','2026-08-05 02:24:17.828672+00',1,'external_registered_control','{"role":"source_quality_evidence","not_a_market_predictor":true}'),
('alpaca_rapid.research_periods','alpaca_rapid_discovery','public','ra_research_periods','equity','alpaca','control','research-stage',null,null,null,false,true,'2025-05-04 00:00:00+00','2026-08-03 23:59:59+00',4,'external_registered_control','{"role":"discovery_validation_holdout_boundaries","sealed_holdout_start":"2026-08-04"}'),
('alpaca_138.daily_bars','alpaca_138_research','public','daily_bars','equity','alpaca','1d','instrument-session','ts','symbol',null,true,false,'2023-12-22 05:00:00+00','2024-04-01 04:00:00+00',597129,'external_registered','{"specialist_programme":"13.8","availability_rule":"full daily bar must be lagged to a later decision; ts alone is not proof of observability"}'),
('alpaca_138.instruments','alpaca_138_research','public','instruments','equity','alpaca','snapshot','instrument-record',null,'symbol',null,false,false,'2026-07-29 21:45:44.845772+00','2026-08-06 09:41:02.741084+00',65438,'external_registered_reference','{"specialist_programme":"13.8","current_state_fields_present":true,"not_point_in_time_for_historical_decisions":true}'),
('alpaca_138.signal_triggers','alpaca_138_research','public','signal_triggers','equity','alpaca','event','signal-trigger','decision_ts','symbol','exact_signal_trade_ts',false,true,'2024-01-02 17:00:00+00','2024-03-28 17:00:00+00',2789,'external_registered','{"specialist_programme":"13.8","temporal_audit":{"exact_signal_after_decision_rows":0,"missing_exact_signal_ts_rows":1063}}'),
('alpaca_138.execution_targets','alpaca_138_research','public','execution_targets','equity','alpaca','event','execution-target','decision_ts','symbol','decision_ts',false,true,'2024-01-02 17:00:00+00','2024-03-28 17:00:00+00',1525,'external_registered_control','{"specialist_programme":"13.8","role":"execution_and_matched_control_target_set"}'),
('alpaca_138.corporate_actions','alpaca_138_research','public','corporate_actions','equity','alpaca','event','corporate-action','execution_date','symbol',null,false,false,'2024-01-02 00:00:00+00','2024-03-28 23:59:59+00',292,'external_registered_reference','{"specialist_programme":"13.8","timing_warning":"announcement/knowledge timestamp unavailable; do not use as same-time predictor"}'),
('alpaca_138.market_sessions','alpaca_138_research','public','market_sessions','equity','exchange_calendar','session','market-session','session_open',null,'session_open',false,true,'2024-01-02 14:30:00+00','2024-03-28 20:00:00+00',70,'external_registered_reference','{"specialist_programme":"13.8","role":"calendar_and_decision_timing"}'),
('alpaca_138.research_tranches','alpaca_138_research','public','research_tranches','equity','internal','control','research-tranche',null,null,null,false,false,'2024-01-01 00:00:00+00','2026-04-19 23:59:59+00',9,'external_registered_control','{"specialist_programme":"13.8","role":"research_protocol_and_results","not_a_predictor":true}')
on conflict (dataset_key) do update set
 store_key=excluded.store_key,schema_name=excluded.schema_name,relation_name=excluded.relation_name,asset_class=excluded.asset_class,provider=excluded.provider,frequency=excluded.frequency,grain=excluded.grain,ts_column=excluded.ts_column,instrument_column=excluded.instrument_column,observable_at_column=excluded.observable_at_column,is_raw=excluded.is_raw,point_in_time_safe=excluded.point_in_time_safe,coverage_start=excluded.coverage_start,coverage_end=excluded.coverage_end,row_estimate=excluded.row_estimate,status=excluded.status,metadata=research_hub.datasets.metadata || excluded.metadata,updated_at=now();

update research_hub.datasets set coverage_start='2025-05-27 00:00:00+00',coverage_end='2025-06-02 23:59:59+00',row_estimate=3,metadata=metadata||'{"coverage_warning":"Only three historical point-in-time universe snapshots are currently present."}'::jsonb,updated_at=now() where dataset_key='alpaca_rapid.pti_universe_snapshots';
update research_hub.datasets set row_estimate=43732,metadata=metadata||'{"membership_rows":43732}'::jsonb,updated_at=now() where dataset_key='alpaca_rapid.analysis_universe';
update research_hub.datasets set row_estimate=3,updated_at=now() where dataset_key='alpaca_rapid.full_history_backfills';

update research_hub.datasets d
set row_estimate=s.n_live_tup::bigint, updated_at=now()
from pg_stat_user_tables s
where d.store_key='market_data_primary' and d.schema_name=s.schemaname and d.relation_name=s.relname and d.relation_name not like '%*%';

insert into research_hub.sync_checkpoints(dataset_key,status,last_row_count,metadata)
select d.dataset_key,'adapter_required',d.row_estimate,jsonb_build_object('store_key',d.store_key,'read_mode',(select read_mode from research_hub.data_stores s where s.store_key=d.store_key),'reason','External store is catalogued; research_hub materialization/federated adapter must preserve point-in-time and predictor/outcome boundaries.')
from research_hub.datasets d
where d.store_key in ('alpaca_rapid_discovery','alpaca_138_research')
on conflict(dataset_key) do update set last_row_count=excluded.last_row_count,status=case when research_hub.sync_checkpoints.status in ('ready','synced','active') then research_hub.sync_checkpoints.status else excluded.status end,metadata=research_hub.sync_checkpoints.metadata||excluded.metadata,updated_at=now();

insert into research_hub.data_quality_issues(dataset_key,severity,issue_type,range_start,range_end,details)
select 'alpaca_rapid.bars_monthly','warning','coverage_regime_change','2017-01-03 14:30:00+00','2025-05-05 00:00:00+00','{"finding":"Pre-May-2025 history is a narrow symbol subset, not broad full-universe coverage.","evidence":{"rd_bars_202504_estimated_distinct_symbols":7,"rd_bars_202505_estimated_distinct_symbols":6209,"rd_bars_202607_estimated_distinct_symbols":6782},"research_rule":"Do not pool pre-May-2025 rows with the broad-universe period without an explicitly matched historical universe."}'::jsonb
where not exists(select 1 from research_hub.data_quality_issues where dataset_key='alpaca_rapid.bars_monthly' and issue_type='coverage_regime_change' and resolved_at is null);

insert into research_hub.data_quality_issues(dataset_key,severity,issue_type,range_start,range_end,details)
select 'alpaca_rapid.pti_universe_snapshots','warning','point_in_time_universe_incomplete','2025-05-05 00:00:00+00','2026-08-10 23:59:59+00','{"finding":"Only three PIT universe snapshots are registered, spanning 2025-05-27 to 2025-06-02, while broad data extends far beyond that interval.","risk":"Survivorship bias if current or later universe membership is projected backward.","required_control":"Use snapshot-linked analysis_universe where available; generate additional effective-dated snapshots before broad historical universe comparisons."}'::jsonb
where not exists(select 1 from research_hub.data_quality_issues where dataset_key='alpaca_rapid.pti_universe_snapshots' and issue_type='point_in_time_universe_incomplete' and resolved_at is null);

insert into research_hub.data_quality_issues(dataset_key,severity,issue_type,details)
select 'primary.equity_microstructure_1m','info','research_ready_materialization_pending','{"finding":"The research-ready equity microstructure table is currently empty while raw selected quotes/trades are present.","risk":"No microstructure feature search should assume this derived panel is available yet.","required_control":"Materialize only from neutral anomaly/baseline capture windows with point-in-time lineage."}'::jsonb
where not exists(select 1 from research_hub.data_quality_issues where dataset_key='primary.equity_microstructure_1m' and issue_type='research_ready_materialization_pending' and resolved_at is null);
