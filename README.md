# Market Data Leading Indicator Miner v3.0.1

A private, collection-only application that populates Supabase with restartable market data for later use by a separate integration/export layer.

This release is rebuilt directly from the original v1.0.2 package. It does not depend on, or assume deployment of, the previously proposed v2 package.

## Responsibility boundary

The miner records market facts, provider provenance, collection health and neutral acquisition windows. It does **not**:

- create ZIP/CSV/Parquet extracts;
- label winners or failed moves;
- create matched controls;
- train or score predictive models;
- decide whether a structure is tradeable.

Those functions belong to the separate integration, export and backtesting applications.

## Data collected

### Existing broad 30-day layer

- Alpaca US equities one-minute bars, now configured for the SIP feed.
- Coinbase spot one-minute bars.
- Binance spot one-minute bars.
- Twelve Data cross-asset indicators and validation instruments.

### US-equity enrichment

- Alpaca SIP trades and best-bid/best-ask quotes around neutral unusual-activity windows.
- Massive company reference, float and settlement-dated short-interest snapshots.
- SEC filing metadata plus bounded document scanning for offering, ATM, shelf, warrant and convertible language.
- FINRA daily short-volume files.
- Alpaca market news around captured equity symbols and periods.

### Crypto historical/context enrichment

- Coinbase, Binance, Kraken, Binance Futures and Bybit venue-symbol catalogues.
- CoinGecko supply and market snapshots.
- Binance Futures funding, open interest, long/short ratios and taker buy/sell ratios where contracts exist.
- Binance historical aggregate trades around neutral capture windows.

### Prospective crypto microstructure

A dedicated Render worker records:

- spot and perpetual trades;
- multi-level order-book state;
- aggressive buy/sell volume;
- spreads, microprice, weighted book prices and depth imbalance;
- funding, mark/index price and open interest;
- liquidations;
- cross-venue observations from Coinbase, Binance, Kraken and Bybit.

One-second derived facts are stored in Postgres. Optional raw messages are written as compressed, time-bounded objects in private Supabase Storage for dynamically triggered assets. Core assets are aggregated continuously; raw core capture is off by default to control cost.

## Reliability design

- Every historical task is a durable Supabase partition.
- Cursors are committed after each provider page.
- Stored rows use deterministic keys and safe upserts.
- Stale running partitions are reclaimed automatically.
- Failed partitions retry with bounded attempts and backoff.
- Existing completed v1.0.2 bars can be reused through **Mine/enrich stored bars**.
- Crypto stream sessions, heartbeats, reconnects and detected gaps are recorded.
- Raw object paths are deterministic and checksummed.

## Neutral capture windows

Capture triggers only decide where additional facts should be collected. They do not label outcomes or assert predictive value.

Default equity triggers include a material move from the regular-session open, a rapid five-minute rise or abnormal relative volume with positive price movement. Default crypto triggers include rapid five-/fifteen-minute movement or abnormal relative volume. Thresholds are configurable through environment variables.

## Supabase table groups

- Broad bars: `market_bars_1m`
- Acquisition control: `collection_runs`, `collection_partitions`, `capture_windows`
- Equity microstructure/context: `market_trades`, `market_quotes_l1`, `equity_context_snapshots`, `sec_filings`, `finra_short_volume`, `market_news`
- Crypto mapping/context: `crypto_venue_symbols`, `crypto_supply_snapshots`, `crypto_derivatives_metrics`
- Crypto live microstructure: `crypto_microstructure_1s`, `crypto_liquidations`, `crypto_capture_targets`
- Raw storage catalogue: `crypto_raw_objects`
- Operations: `provider_health`, `crypto_stream_sessions`, `crypto_stream_gaps`

The legacy v1 export tables remain in the original migration for database compatibility, but v3 does not read or write them.

## Deployment summary

1. Replace the GitHub repository contents with this package.
2. Preserve the existing Supabase project and database.
3. Configure the secrets in `DEPLOYMENT.md`.
4. Deploy the web service first so migrations 002 and 003 run.
5. Verify `/health`.
6. Deploy the collection worker.
7. Deploy the crypto-stream worker.
8. Open the original completed run and select **Mine/enrich stored bars**, or begin a new 30-day run.

Full instructions and operational checks are in [DEPLOYMENT.md](DEPLOYMENT.md).
