# Architecture — v3.3.0

## Boundary

```text
Public/paid market-data sources
              ↓
Market Data Miner
              ↓
Supabase PostgreSQL + Supabase Storage
              ↓
Separate integration/export layer
              ↓
Backtesting, analysis and ChatGPT research
```

The miner records facts. It does not create extracts, matched controls, predictive labels, models or trading recommendations.

## Permanent broad layer

### All selected instruments

`market_bars_1m` is the common historical fact table. Alpaca, Coinbase and Binance catalogues are refreshed before planning. With the v3.3.0 defaults:

- Alpaca: every active tradable US equity is catalogued; exchange-listed assets are collected through SIP, while OTC assets are transparently excluded unless the separate OTC entitlement is enabled.
- Coinbase: every online spot product.
- Binance: every spot symbol with trading enabled.
- Twelve Data: the explicit configured indicator/validation set, capped by API quota.

`market_universe_snapshots` records catalogue counts at refresh time.

### Prospective crypto broad layer

`crypto_market_observations_1m` stores one-minute pair-level observations for all mapped live markets. It preserves venue, market type, venue symbol, canonical base asset, quote asset, price, rolling volume and available top-of-book fields.

A local SQLite ring buffer stores 15-second broad observations for the preceding 120 minutes. It is bounded and pruned. When a neutral multi-venue condition activates deeper collection, the relevant asset's buffered observations are written as compressed `broad_pretrigger` objects in Supabase Storage and indexed through `crypto_raw_objects`.

The local buffer is best-effort across instance replacement; permanent one-minute facts remain in Supabase and stream gaps are separately observable.

## Equity microstructure layer

Every Alpaca stock receives full SIP one-minute bars. Raw SIP trades and quotes are expensive, so v3.3.0 collects them for two neutral sample classes:

1. Anomaly windows based on price, volume and trade participation.
2. Deterministic baseline windows sampled independently of future returns.

`capture_windows` records candidates. `capture_decisions` records whether each window was admitted or excluded and why. The defaults reserve up to 1,000 baseline windows within an overall 5,000-window run cap.

After tick partitions complete, each trade is classified against the most recent SIP quote within five seconds. `equity_microstructure_1m` then stores reusable minute-level trade imbalance, notional, VWAP and spread/quote statistics.

If run-level storage caps are reached, admitted windows are selected by a deterministic hash rather than symbol order or future outcomes; every exclusion remains in `capture_decisions`.

## Deep crypto layer

`crypto_microstructure_1s` stores one-second facts for core and dynamically admitted assets. Deep feeds include supported trades, order-book depth, derivatives and liquidations.

`crypto_dynamic_detections` retains every qualifying decision, including assets excluded by the dynamic capacity cap. The cap controls expensive deep subscriptions only; it does not limit full-pair broad observations.

## Storage strategy

- PostgreSQL: structured one-minute and selected one-second facts, catalogues, provenance, coverage and decisions.
- Supabase Storage: compressed high-frequency raw segments and preserved pre-trigger buffers.
- Local ephemeral SQLite: bounded rolling crypto broad buffer only.

## Restartability

- Historical work is partitioned and checkpointed in `collection_partitions`.
- Deterministic keys make retries idempotent.
- Stale partitions are reclaimed automatically.
- Failed live database flushes are re-buffered.
- Failed object uploads, including preserved pre-trigger buffers, remain pending and are retried.
