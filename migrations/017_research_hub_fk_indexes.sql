create index if not exists idx_rh_candidate_run on research_hub.candidate_ledger(run_id);
create index if not exists idx_rh_dq_dataset on research_hub.data_quality_issues(dataset_key);
create index if not exists idx_rh_runs_feature_set on research_hub.experiment_runs(feature_set_key);
create index if not exists idx_rh_runs_outcome_set on research_hub.experiment_runs(outcome_set_key);
create index if not exists idx_rh_feature_defs_dataset on research_hub.feature_definitions(dataset_key);
create index if not exists idx_rh_feature_rows_source_dataset on research_hub.feature_rows(source_dataset_key);
create index if not exists idx_rh_pit_universe_source_dataset on research_hub.point_in_time_universe(source_dataset_key);
