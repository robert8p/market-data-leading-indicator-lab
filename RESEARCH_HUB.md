# Research Hub

The `research_hub` schema is the private semantic and experiment-control layer between raw market facts and ChatGPT-directed discovery.

## Design rules

1. Raw collectors continue to record facts only.
2. Predictor features must be observable at or before `decision_ts`; `feature_rows` enforces `observable_at <= decision_ts`.
3. Future outcomes are stored separately in `outcome_rows`.
4. Discovery thresholds and trade direction are learned only from the discovery period.
5. Validation applies the frozen discovery rule unchanged.
6. Holdout data is inaccessible to `run_univariate_tail_screen`; it requires the separate `evaluate_frozen_holdout` call after candidates are frozen.
7. Benjamini-Hochberg FDR is applied across the discovery search space using a one-sided alternative for positive after-cost edge.
8. Raw cross-database data should not be duplicated. Register external stores and federate or copy only research-ready derived datasets.
9. Point-in-time universe membership belongs in `point_in_time_universe` to control survivorship bias.
10. Candidate definitions are immutable once frozen; changed thresholds, horizons, direction, costs or feature definitions become new candidates.

## Core objects

- `data_stores`, `datasets`: catalogue physical stores and logical datasets.
- `feature_definitions`, `feature_sets`, `feature_rows`: point-in-time predictor layer.
- `outcome_definitions`, `outcome_sets`, `outcome_rows`: future-target layer.
- `point_in_time_universe`: effective-dated universe membership/tradability.
- `experiment_runs`, `experiment_tests`: experiment ledger and multiple-testing evidence.
- `candidate_ledger`: frozen candidate definitions and validation/holdout evidence.
- `sync_checkpoints`, `data_quality_issues`: federation and data-quality control.

## Current adapter

`scripts/seed_research_hub_xal.sql` adapts the existing XAL market-state feature panel and target outcomes without modifying the legacy XAL tables.

The first production smoke run screened 540 discovery hypotheses over the XAL adapter, retained zero candidates after positive-edge FDR and validation, and did not access holdout. This is expected and demonstrates that the engine rejects an unproductive family rather than promoting statistical significance with negative economics.

## Federation

`postgres_fdw` is the preferred architecture for curated read-only access to the Alpaca Rapid Discovery and specialist research databases. Do not federate the monthly raw-bar partitions by default. Prefer research-ready tables such as daily/feature panels and execution panels. Credentials must be held outside source control and mapped through dedicated read-only database roles.
