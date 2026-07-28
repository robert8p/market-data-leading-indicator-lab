# Market Data Leading Indicator Miner v3.3.0

A collection-only market-data application that populates Supabase with reusable, point-in-time facts. A separate integration layer is responsible for extracts, matched controls, feature construction, backtesting and ChatGPT-ready research packages.

## Coverage model

### Alpaca US equities

- Catalogues every active asset marked tradable by Alpaca.
- Permanently stores 30-day SIP one-minute bars for every exchange-listed asset eligible under the configured feed.
- Catalogues OTC assets too, but marks them uncollected unless the separate OTC entitlement is enabled with `ALPACA_OTC_ENABLED=true`.
- Scans every stored equity series for neutral unusual-activity windows.
- Adds deterministic outcome-independent baseline windows, preventing the tick dataset from containing only obvious movers.
- Backfills SIP trades and SIP best-bid/best-offer quotes for admitted windows, including 120 minutes before and 120 minutes after the observation by default.
- Records every capture admission or exclusion and its reason.
- Classifies captured trades against the prevailing SIP quote and permanently aggregates trades/quotes into `equity_microstructure_1m`.
- Collects Massive reference/float/short-interest context, SEC filings, FINRA short volume and Alpaca news.

### Coinbase and Binance spot

- Catalogues every online/trading spot pair by default, including pairs outside the preferred quote-currency lists.
- Permanently stores one-minute historical bars for every selected pair.
- Keeps each venue pair separate while retaining a canonical base-asset mapping.
- Permanently stores one-minute live broad-market observations for every mapped pair.
- Maintains a local 15-second rolling buffer for the preceding 120 minutes.
- When neutral activity is detected, preserves the affected asset's pre-trigger multi-pair buffer to compressed Supabase Storage.
- Applies the 75-asset dynamic cap only to expensive deep trades/order-book subscriptions, not to broad discovery coverage.

### Crypto derivatives and other venues

- Broad detection also uses Binance perpetual futures, Kraken spot and Bybit perpetual futures.
- Deep capture can collect trades, order-book depth, funding, open interest and liquidations from supported venues.
- Historical crypto context includes supply snapshots and derivatives metrics where available.

### Twelve Data cross-assets

Twelve Data remains a configured, quota-controlled indicator set rather than an exhaustive global instrument universe. The exact selected catalogue and provider counts are stored so downstream analysis can distinguish full-universe sources from curated sources.

## Research-integrity protections

- Collection facts are separate from model labels and trading decisions.
- Baseline equity windows are selected deterministically without using future returns.
- Capture thresholds control storage only; they do not label a pattern as predictive.
- All admitted and excluded capture decisions are retained.
- Effective dates and provider metadata are preserved.
- Every partition is idempotent, checkpointed and restartable.
- Failed high-frequency writes, raw uploads and pre-trigger-buffer uploads are retained for retry.
- Universe snapshots document what was actually tradable and collected.

## Services

- `market-data-lab-web`: UI, migrations and control plane.
- `market-data-lab-worker`: catalogues, historical bars, enrichment, tick backfill and aggregation.
- `market-data-crypto-stream`: prospective all-pair observations, rolling buffer and selected deep microstructure.

## Upgrade path

This package upgrades directly from the original v1.0.2 database. Migrations 002–005 are additive. Existing one-minute bars are preserved and can be reused through **Mine/enrich stored bars**.

See `DEPLOYMENT.md` for the deployment sequence and `ARCHITECTURE.md` for table-level responsibilities.
