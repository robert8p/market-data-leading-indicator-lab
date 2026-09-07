# Canonical pattern research: standalone reference mathematics

This folder contains no credentials, raw market observations, private performance results, or ingestion jobs. It is isolated from application entry points and existing deployed branches.

## Executed verification

The reference engine and synthetic fixtures were executed with Python 3.13.5 and the pinned dependencies. All 34 tests passed. The canonical `block_ratio_stats` function also independently reproduced the private PostgreSQL implementation's mean, confidence intervals and centered-bootstrap p-value within 1e-9 numerical tolerance. A separate PostgreSQL calculation reconstructed one complete evaluation directly from raw source bars, without reading the feature, label or trial views.

This is reference mathematics, not a public database adapter or a newly deployed trading service. The production-side extraction, live permission guards, frozen protocol, source manifests, exact SQL definitions, failure logs and append-only results are retained in the private `astra_pattern_v1` schema and the existing `research_hub` experiment/candidate ledgers. No private evidence is published here.

## Contracts

`features` requires exactly 60 ordered, complete one-minute bars and checks an explicit post-close availability assumption. `label` uses post-decision entry and one-hour exit opens, rejects incomplete windows, and excludes the exit bar's subsequent high/low. Raw intraday representations must not be silently mixed with adjusted or total-return data.

The six `signal` branches implement fixed mechanism checks. Market-relative, futures and cross-sectional inputs must already have passed the private feature-only timing and eligibility checks. The standalone E1 interface calls the cross-sectional percentile `rank`; its private feature-panel column is `strength_rank`. Unsupported inputs must be excluded before signal evaluation. A zero signal is cash, not a missing outcome.

`block_ratio_stats` is the canonical programme uncertainty calculation: simultaneous assets are collapsed into chronological daily aggregates before deterministic circular five-session resampling. Its rows are `[date, complete_cases, incremental_sum_bps, fixed_slot_portfolio_net_bps, trades]`. It withholds intervals when there are fewer than 20 dates or eight weekly clusters. Weekly clusters are not asserted to be statistically independent. `block_stats` is a separate synthetic-fixture helper and is not the programme's ratio statistic. Holm correction is applied over the entire declared configuration family, not just favourable candidates.

The local `guard` enforces the bounded reference windows. It is not a replacement for the live private database guard, which checks current sealed split and experiment metadata, including open-ended ranges. Neither code publication nor a new programme name authorises access to protected outcomes.

## Reproduction by an authorised maintainer

From this folder, install `requirements.txt` and run `python -m pytest -q`. This runs synthetic tests only: no network requests or database credentials are required. The application owner does not need to run these commands; the checks have already been executed. Reproducing private numerical results additionally requires authorised access to the stored private protocol and snapshots, and must never read sealed periods.

A development or reused-evaluation result is not an independent success. Scenario transaction costs are not historical executable fills, and a conditional average is not a calibrated probability.
