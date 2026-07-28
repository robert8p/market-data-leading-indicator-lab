# Architecture — v3.0.1

## System boundary

```text
Public/paid market-data providers
              ↓
   Market Data Miner v3
              ↓
 Supabase raw + normalised facts
              ↓
  Separate integration layer
              ↓
Exports / backtests / ChatGPT analysis
```

The miner is intentionally unaware of research labels, future outcomes and model definitions.

## Services

### 1. Web service

- Applies idempotent migrations before startup.
- Provides authenticated run control and operational visibility.
- Supports new runs, reuse of stored v1 bars, pause, resume, retry and cancellation.
- Does not generate exports.

### 2. Historical collection worker

Processes durable `collection_partitions` for:

- catalogues;
- one-minute bars;
- neutral capture scanning;
- targeted historical trades and quotes;
- Massive, SEC, FINRA and news enrichment;
- crypto catalogues, supply snapshots and historical derivatives.

Each provider page is committed with a cursor before the next page is requested.

### 3. Prospective crypto-stream worker

Maintains public WebSocket connections for configured venues. It:

- tracks visible order-book state;
- classifies aggressive trade side from provider semantics;
- aggregates facts into one-second rows;
- records derivatives and liquidation observations;
- writes raw triggered messages into compressed Storage segments;
- refreshes dynamic targets without restarting the whole deployment;
- records stream sessions, provider health and reconnect gaps.

## Collection funnel

### Broad collection

One-minute bars are collected across the selected universes. Core crypto assets also receive continuous one-second microstructure aggregation.

### Neutral acquisition scan

The miner locates unusual activity using configurable contemporaneous rules. This creates `capture_windows`, not outcome labels.

### Targeted enrichment

Only bounded windows receive expensive historical tick collection. Equity context can be collected for all Alpaca symbols or only captured symbols. Raw prospective crypto capture is limited to active dynamic targets unless explicitly enabled for core assets.

## Point-in-time discipline

- Massive short interest is stored using settlement dates.
- Float/reference snapshots preserve provider effective dates and point-in-time warnings.
- CoinGecko snapshots retain collection timestamps.
- SEC filing dates and acceptance timestamps are retained.
- The future integration layer must use causal as-of joins and must not treat current reference snapshots as historical facts.

## Storage strategy

### Postgres

Stores queryable, low-level facts and one-second/one-minute aggregates.

### Supabase Storage

Stores compressed raw crypto message segments. Postgres contains the object path, checksum, time range, provider, market type and message count.

This avoids turning every high-frequency order-book update into a heavily indexed Postgres row while preserving raw evidence for later targeted extraction.

## Restart and failure behaviour

Historical jobs use database locks, heartbeats, attempt counters and deterministic upserts. A worker crash leaves completed pages intact. Stale tasks are reclaimed after the configured threshold.

The live stream reconnects after provider or network failures and records session/gap metadata. A disconnected live feed cannot reconstruct missed full-depth order-book changes; the gap is disclosed rather than silently filled with invented data.

## Cost guardrails

- Raw core crypto capture disabled by default.
- Maximum dynamic crypto targets defaults to 30.
- Dynamic raw capture expires automatically.
- Raw files rotate every 15 minutes by default.
- Historical tick collection is restricted to neutral capture windows.
- Derivatives backfill has a configurable symbol cap.
- Massive and CoinGecko clients are rate-limited.
