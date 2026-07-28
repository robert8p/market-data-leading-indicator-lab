# Changelog

## 3.3.0

### Miner-first full-universe correction

- Coinbase now catalogues every online spot pair by default.
- Binance now catalogues every tradeable spot pair by default.
- Preferred quote lists now influence priority only; they do not exclude pairs when `CRYPTO_FULL_PAIR_UNIVERSE=true`.
- Broad crypto mapping no longer collapses each asset to one preferred pair.
- Multiple pairs on the same venue remain distinct detector evidence.

### Scalable broad crypto retention

- Added permanent all-pair one-minute observations in `crypto_market_observations_1m`.
- Added a 15-second local SQLite rolling buffer with a default 120-minute retention period.
- Added compressed pre-trigger buffer preservation to Supabase Storage.
- Kept the 75-asset cap only for expensive deep trades/order-book subscriptions.

### Equity research-integrity improvements

- Catalogues the full active/tradable Alpaca universe and explicitly records OTC feed eligibility rather than silently failing SIP requests.
- Increased the default pre-trigger window from 60 to 120 minutes.
- Added deterministic baseline equity windows independent of future outcomes.
- Added explicit capture admission/exclusion records.
- Added quote-based Alpaca trade classification and permanent `equity_microstructure_1m` aggregates after SIP trade/quote backfill.
- Added a distinct aggregation stage to the UI and worker pipeline.

### Coverage and operations

- Added `market_universe_snapshots`.
- Added migration `005_miner_first_coverage.sql`.
- Updated the dashboard for capture decisions, equity microstructure and all-pair crypto observations.
- Added retry tests for broad observations and pre-trigger object uploads.
- Added deterministic hash admission under storage caps, avoiding alphabetical truncation.
- Added full-pair catalogue and deterministic-baseline tests.
- Catalogue refreshes now mark stale crypto venue mappings non-tradable.

## 3.2.0

- Added ranked multi-venue dynamic crypto detection across Coinbase, Binance spot/futures, Kraken and Bybit.
- Added explicit capacity decisions and multi-window trigger logic.

## 3.1.0

- Added the polished responsive UI, progress bars, stage rail and automatic active-run refresh.

## 3.0.x

- Added collection-only equity/crypto enrichment, resumable partitions and prospective crypto microstructure.
