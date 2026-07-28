# Changelog

## 3.0.1

- Rebuilt directly from v1.0.2 as a collection-only miner.
- Added additive equity and crypto migrations.
- Switched default Alpaca equity feed to SIP.
- Added targeted Alpaca trade/quote collection with durable pagination.
- Added Massive reference, float and short-interest snapshots.
- Added SEC dilution-signal scanning, FINRA daily short volume and Alpaca news.
- Added neutral capture windows without future-outcome labels.
- Added crypto venue catalogues, CoinGecko supply snapshots and Binance derivatives history.
- Added prospective Coinbase, Binance, Kraken and Bybit microstructure collection.
- Added one-second cross-venue facts, liquidations, raw compressed segments and stream health/gap records.
- Added dynamic-trigger cost controls and maximum active-target limits.
- Removed active export and feature-generation code from the application.
- Added direct reuse of completed v1.0.2 bar runs through **Mine/enrich stored bars**.
