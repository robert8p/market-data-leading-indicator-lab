# Deployment — Market Data Miner v3.3.0

This is an in-place upgrade. Keep the existing GitHub repository and existing Supabase project.

## 1. Before deployment

1. Confirm a recent Supabase database backup exists.
2. Suspend `market-data-lab-worker` while replacing the code.
3. Suspend `market-data-crypto-stream` if it already exists.
4. Keep the existing repository; replace its application files rather than deleting the repository.
5. Copy the contents of this package into the repository root, commit and push.

## 2. Web service first

Open `market-data-lab-web` in Render and confirm:

```text
DATABASE_URL
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
RAW_BUCKET=market-data-raw
APP_USERNAME=rob
APP_PASSWORD=<your UI password>
SESSION_SECRET=<long random value>
```

Deploy the latest commit. The pre-deploy command runs `python -m app.migrate` and applies additive migrations through:

```text
005_miner_first_coverage.sql
```

Confirm `/health` returns:

```json
{"status":"ok","version":"3.3.0","role":"collection_only"}
```

Do not continue until the health check succeeds.

## 3. Historical worker

Keep the existing Supabase, Alpaca, Massive, SEC and Twelve Data values. Add or update:

```text
ALPACA_FEED=sip
ALPACA_OTC_ENABLED=false
ALPACA_REQUESTS_PER_MINUTE=9000

BINANCE_PAIR_MODE=all
CRYPTO_FULL_PAIR_UNIVERSE=true

CAPTURE_SCAN_ENABLED=true
CAPTURE_WINDOW_BEFORE_MINUTES=120
CAPTURE_WINDOW_AFTER_MINUTES=120
MAX_CAPTURE_WINDOWS_PER_RUN=5000
MAX_CAPTURE_WINDOWS_PER_INSTRUMENT=1000

EQUITY_BASELINE_SAMPLE_ENABLED=true
EQUITY_BASELINE_SAMPLE_RATE=0.005
EQUITY_BASELINE_MAX_WINDOWS_PER_RUN=1000
EQUITY_BASELINE_SEED=miner-baseline-v1

EQUITY_ENRICHMENT_ENABLED=true
EQUITY_CONTEXT_SCOPE=all
CRYPTO_ENRICHMENT_ENABLED=true
```

Deploy or resume `market-data-lab-worker`.

### Important scale consequence

`CRYPTO_FULL_PAIR_UNIVERSE=true` and `BINANCE_PAIR_MODE=all` cause historical bars to be planned for every tradeable Binance and Coinbase spot pair, not one preferred pair per coin. This materially increases:

- collection partitions;
- API calls;
- Supabase rows and disk usage;
- total run duration.

The worker remains resumable. Do not restart a progressing run merely because it is large.

## 4. Crypto stream worker

Create or update `market-data-crypto-stream` with:

```text
DATABASE_URL
SUPABASE_URL
SUPABASE_SERVICE_ROLE_KEY
RAW_BUCKET=market-data-raw

CRYPTO_STREAM_ENABLED=true
CRYPTO_STREAM_VENUES=coinbase,binance_spot,binance_futures,kraken,bybit
CRYPTO_STREAM_CORE_SYMBOLS=BTC,ETH,SOL,XRP,BNB,DOGE,ADA,AVAX,LINK,LTC,BCH,DOT,UNI,AAVE,ATOM
CRYPTO_STREAM_MAX_DYNAMIC_TARGETS=75

CRYPTO_FULL_PAIR_UNIVERSE=true
CRYPTO_BROAD_OBSERVATION_ENABLED=true
CRYPTO_BROAD_OBSERVATION_SECONDS=60
CRYPTO_BROAD_BUFFER_SECONDS=15
CRYPTO_BROAD_BUFFER_MINUTES=120
CRYPTO_PRESERVE_PRETRIGGER_BUFFER=true

CRYPTO_AGGREGATION_SECONDS=1
CRYPTO_ORDER_BOOK_DEPTH=20
CRYPTO_RAW_CAPTURE_ENABLED=true
CRYPTO_RAW_CORE_ENABLED=false
CRYPTO_RAW_SEGMENT_MINUTES=15
```

Keep the v3.2 multi-venue trigger settings unless intentionally changing them.

Deploy the stream worker after migration 005 is applied and `crypto_venue_symbols` has been populated by the historical worker's enrichment stage. It can run earlier, but it will wait for mappings.

## 5. Reuse the existing run

If your original 30-day run is complete:

1. Open the run in the web UI.
2. Select **Mine/enrich stored bars** once.
3. Existing stored bars are reused.
4. New capture scanning, context enrichment, SIP tick backfill and aggregation are checkpointed.

If you want all Coinbase/Binance pairs historically, the old run may not contain bars for pairs excluded by the original preferred-pair catalogue. Start a new 30-day run to collect those newly included pairs. The enhancement action cannot create historical bars for instruments that were never in the original catalogue.

## 6. Expected stages

```text
Plan → Collect → Scan → Enrich → Aggregate → Ready
```

## 7. Supabase checks

```sql
select provider, asset_class, tradable_count, preferred_count, snapshot_ts
from market_universe_snapshots
order by snapshot_ts desc, provider;
```

```sql
select
  (select count(*) from market_bars_1m) as bars_1m,
  (select count(*) from capture_decisions) as capture_decisions,
  (select count(*) from equity_microstructure_1m) as equity_microstructure_1m,
  (select count(*) from crypto_market_observations_1m) as crypto_pair_observations_1m,
  (select count(*) from crypto_microstructure_1s) as crypto_deep_1s,
  (select count(*) from crypto_raw_objects where channel='broad_pretrigger') as preserved_pretrigger_objects;
```

## 8. Scope clarification

- Alpaca: all active/tradable assets are catalogued; SIP-eligible exchange-listed assets are collected by default. OTC assets require a separate entitlement and `ALPACA_OTC_ENABLED=true`.
- Coinbase/Binance: exhaustive online/trading spot pairs by default.
- Kraken/Bybit/crypto derivatives: mapped broad/deep context, not primary full historical bar universes.
- Twelve Data: explicit curated and quota-limited indicator set.
