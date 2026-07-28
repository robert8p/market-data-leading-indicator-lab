# Validation report

**Package:** Market Data Leading Indicator Miner v3.3.0  
**Boundary:** collection-only  
**Upgrade:** additive from v1.0.2 / compatible with v3.x migrations

## Automated validation

- Python package compilation: passed.
- Package structure check: passed.
- Collection/export boundary check: passed.
- Automated test suite: 35 tests passed.

The tests cover:

- collection-window partitioning;
- deterministic equity baseline sampling;
- regular-session equity anomaly detection;
- crypto anomaly detection without future labels;
- full-pair Coinbase and Binance catalogue inclusion;
- multi-pair detector evidence;
- multi-venue confirmation and derivatives-led triggers;
- cooldown and capacity-related detector behavior;
- order-book metric calculations;
- raw-upload and pre-trigger-buffer retry retention;
- one-second database re-buffering;
- all-pair broad-observation re-buffering;
- UI login, dashboard and progress rendering;
- PostgreSQL planning query construction.

## Static integrity checks

- Migrations 001–005 are present and sorted additively.
- `render.yaml` retains compatible service names.
- Required commands are present:
  - `python -m app.migrate`
  - `python -m app.worker`
  - `python -m app.crypto_stream`
- No active exporter, feature-builder or model-training module is included.
- No credentials are embedded.
- Source files use LF line endings through `.gitattributes`.

## Material design checks

- Every active/tradable Alpaca asset is retained by the catalogue; OTC collection eligibility is explicitly recorded and tested.
- Coinbase and Binance quote filters do not exclude pairs when full-universe mode is enabled.
- Full-pair broad mappings are not reduced to one pair per canonical asset.
- Deep crypto capacity limits do not restrict permanent broad observations.
- Equity baseline windows are deterministic and independent of future returns.
- Excluded capture windows remain auditable through `capture_decisions`.
- Equity SIP tick aggregates are planned only after trade/quote partitions are terminal.
- Alpaca trades are classified against a recent SIP quote before buy/sell imbalance is aggregated.
- Run-level storage caps use deterministic hash ordering rather than alphabetical truncation.
- Stale crypto venue mappings are marked non-tradable on each catalogue refresh.
- Permanent broad crypto storage is one-minute rather than five-second, avoiding uncontrolled row growth.
- The higher-frequency crypto buffer is bounded, locally pruned and preserved only when needed.

## Production validation still required

The following require live credentials and production infrastructure and therefore were not executed here:

1. Applying migration 005 against the production Supabase schema.
2. Full Alpaca SIP and Massive API calls.
3. Live Coinbase/Binance/Kraken/Bybit WebSocket throughput.
4. Supabase write capacity under the complete Coinbase/Binance pair universe.
5. Supabase Storage upload of a real preserved pre-trigger object.
6. Render restart behavior and local rolling-buffer loss/recovery characteristics.
7. Actual 30-day storage footprint and run duration.

Monitor database CPU, disk growth, partition throughput and crypto-stream reconnects closely during the first production day.
