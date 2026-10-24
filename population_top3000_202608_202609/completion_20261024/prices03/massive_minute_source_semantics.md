# Massive minute source contract, verified 24 October 2026

This source acquisition does not establish company membership, full expected-print coverage, independent accuracy, or original historical receipt. Existing verified AAPL 8 September archive (888 source records, 90,747 stored bytes) remains unchanged. The separately versioned pilot uses the same native secret aliases and shared provider reservation mechanism for five retained symbol candidates on 9 September.

## Primary provider documentation

- [Custom bars specification](https://massive.com/docs/rest/stocks/aggregates/custom-bars): one-minute bars use qualifying trades and cover all trading sessions. `adjusted=false` requests prices unadjusted for splits. `t` is the start of the aggregate window in Unix milliseconds; it is not publication or receipt time. `n` is the optional transaction count, `v` volume and `vw` optional provider VWAP. `otc` may be absent when false; raw absence is preserved. Requests use ascending order, 50,000 base-aggregate limit, exact response counts and an explicit pagination chain. Documentation describes absent bars when no eligible trades occur; this implementation preserves missing observations and does not promote a missing interval to a verified no-trade state.
- [Stocks overview](https://massive.com/docs/rest/stocks/overview): the ordinary ET session boundaries are 04:00–09:30, 09:30–16:00 and 16:00–20:00. The private view also requires the mission's pinned session calendar. Millisecond timestamps follow the endpoint-specific contract rather than the overview's general timestamp description.
- [Provider VWAP explanation](https://massive.com/knowledge-base/article/how-is-a-stock-bars-vwap-calculated-and-why-can-it-fall-outside-the-high-and-low): volume-eligible trades can differ from trades eligible to set OHLC. A provider VWAP outside a bar's range is consequently not sufficient evidence of an error. Minute VWAP aggregation has provider rounding limits. Daily bars are constructed separately and can include trades absent from every minute bar, so daily volume or VWAP need not equal a minute-derived value.

## Representation and temporal qualification

The worker retains exact UTF-8 response text except any credential-bearing `next_url` is sanitized. It stores distinct wire and retained-content hashes and byte counts. SQL parses numeric source lexemes directly to `numeric`; missing fields and invalid values remain distinguishable. A 39-digit adversarial decimal is included only in the all-rollback synthetic fixture, never in acquired production data.

The current retrieval clock is stored as `retrieved_at`. Both historical receipt and source publication remain NULL. No WebSocket latency promise, plan recency label, aggregate timestamp, or flat-file availability schedule is substituted for an original receipt.

The view classifies acquired source occurrences by pinned calendar and ET boundaries. It never fills 390 regular slots with invented prices or zero volume. Both P01 and P02 can reuse one retained archive. Required family denominators, every eligible share class, identity effective intervals, corporate actions, prior support and independent source accuracy remain acceptance requirements.

## Bounded native pilot

Candidate pairs are AAPL/41, GOOG/5083, GOOGL/5084, NVDA/8372 and WEAV/12595. The request window is 2026-09-09 00:00 ET through the end of that NY day: `[1788926400000,1789012800000)` milliseconds. Each source permits at most two pages, 8 MiB per response and 50,000 total records. The worker has a 45-second bound, no automatic source retry, exact symbol/date/cursor admission and shared Massive spacing. This is a source feasibility subset, not a replacement for the complete population.

Scale-up requires actual provider receipts, terminal pagination, measured runtime/bytes, full candidate inventory, deduplication against retained archives, separate finite admission and root integration. No broader request queue is admitted by this document.
