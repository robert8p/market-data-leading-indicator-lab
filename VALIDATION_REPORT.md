# Validation report

**Package:** Market Data Leading Indicator Miner v3.0.1  
**Upgrade basis:** original v1.0.2 package  
**Architecture:** collection-only; separate integration/export layer

## Completed checks

- Python compilation of the full `app` package.
- Sixteen automated tests passed.
- Neutral equity and crypto capture-window logic tested.
- Crypto order-book aggregation calculations tested.
- Configuration validation tested.
- Run-planning SQL construction tested.
- Web templates and health routes tested.
- Additive migration filenames preserve the existing v1.0.2 migration history.
- Render Blueprint service names preserve the existing v1.0.2 web and worker resources for an in-place upgrade.
- Historical pagination paths retain durable cursors and deterministic keys.
- Postgres insert column/placeholder counts checked for one-second crypto aggregation.
- No active exporter, matched-control generator or model feature module remains.
- No credentials are embedded in the package.

## Not possible in the build environment

The following require your production credentials and connected infrastructure and were not claimed as completed:

- executing migrations against your Supabase project;
- a full 30-day live Alpaca SIP backfill;
- live Massive entitlement validation;
- SEC/FINRA production-volume collection;
- long-running multi-venue WebSocket observation;
- Supabase Storage upload under your service-role policy;
- Render memory and throughput measurement under production load.

## Mandatory production smoke test

1. Back up Supabase.
2. Deploy the web service and confirm migrations 002/003 complete.
3. Confirm `/health` returns version 3.0.1.
4. Deploy the historical worker and enhance a narrow existing run first if available.
5. Confirm at least one `capture_scan` partition completes.
6. Confirm Alpaca SIP trade and quote rows are inserted for one captured equity.
7. Confirm Massive, SEC and FINRA tables receive rows.
8. Confirm crypto venue mappings populate.
9. Deploy the crypto stream and confirm session heartbeat and one-second rows.
10. Trigger or temporarily configure raw core capture for one symbol and verify a compressed object plus `crypto_raw_objects` record, then switch raw core capture off again.
11. Restart both workers and confirm progress resumes without row duplication.
12. Inspect any `crypto_stream_gaps` and failed partitions before scaling the universe.

## Residual risks

- Public provider schemas and rate limits can change.
- Current float/reference fields may not be historically point-in-time; metadata explicitly warns the integration layer.
- Full-depth public crypto streams begin prospectively and cannot fill outage gaps retrospectively.
- Cross-exchange symbol mapping can be imperfect for renamed or bridged assets and needs later data-quality review.
- High-frequency storage growth depends on market activity, dynamic triggers and selected venues.
- A successful collection system does not prove that predictive patterns exist.
