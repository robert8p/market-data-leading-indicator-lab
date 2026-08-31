# Kalshi Perps Probability Lab

Production-oriented, read-only decision-support application for Kalshi cryptocurrency perpetual futures. It discovers active margin markets dynamically, stores public Kalshi market/candle/funding data, issues immutable 1-hour, 2-hour and 4-hour directional probabilities, scores outcomes, and exposes calibration and operational evidence.

## Evidence boundary

- **Kalshi-native models** may be promoted only after chronological, purged, untouched-holdout probability metrics pass the frozen gates.
- **Cross-venue transfer models** are always labelled experimental, even when their external holdout metrics pass.
- Until a validated model exists, the system serves a 50/50 no-edge baseline and continues collecting immutable evidence.
- The application does not place orders.

## Architecture

- FastAPI/Jinja responsive web application.
- One bounded asynchronous scheduler in a single Render web process.
- Public unauthenticated Kalshi Margin REST market, candle and funding endpoints.
- Private `kalshi_perps` schema in Supabase.
- Narrow `SECURITY DEFINER` RPCs protected by a dedicated capability token; no database service-role key is used by the app.
- Append-only predictions and outcomes, with model, calibration, feature and source timestamps retained.

## Main routes

- `/` — active markets and current horizon forecasts.
- `/market/{ticker}` — price path, forecasts and resolved outcomes.
- `/predictions` — immutable prediction ledger and CSV export.
- `/evidence` — model versions, holdout metrics and limitations.
- `/system` — ingestion checkpoints, freshness and errors.
- `/healthz` — lightweight deployment health check.

## Research design

For each horizon, the research runner uses:

1. chronological discovery data for model fitting;
2. three chronological validation segments for base calibration, non-negative stacking and ensemble calibration;
3. an untouched final holdout;
4. a 24-hour maximum-lookback purge plus the forecast horizon;
5. proper probability scores, calibration regression, expected calibration error and block-bootstrap Brier-skill intervals;
6. shuffled-label negative control;
7. confidence-band abstention and regime failure analysis.

The ensemble compares regularised logistic regression, histogram gradient boosting, a recent-return model and a market-context model. Weights sum to one and are learned from validation log loss with concentration and redundancy penalties.

## Runtime variables

Required, server-side only:

- `SUPABASE_URL`
- `SUPABASE_PUBLISHABLE_KEY`
- `KALSHI_APP_TOKEN`

Optional scheduling and model limits are declared in `kalshi_perps_app/config.py`.

## Deployment

The dedicated Dockerfile is `Dockerfile.kalshi`. The app is intended to run with exactly one web worker because its scheduler is process-local. Separate worker infrastructure should be introduced only if ingestion volume later justifies it.

## Source bundle integrity

The deployable source, tests and model-governance documentation are committed as deterministic Base64 chunks under `kalshi_bundle/` because the connected GitHub write interface accepts UTF-8 text only. Docker and CI reconstruct `kalshi-source.tar.gz` and verify SHA-256 `2e3f9d7c2e0563557dfa23d73a018c554d9ab844d6b266f396183e6cacc510bd` before extraction. No credential is present in the bundle.
