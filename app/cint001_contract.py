from __future__ import annotations

from datetime import datetime, timedelta, timezone

RULE_VERSION = "C-INT-001-frozen-2026-08-10"
VALIDATION_START = datetime(2025, 10, 1, tzinfo=timezone.utc)
VALIDATION_END = datetime(2026, 3, 1, tzinfo=timezone.utc)
FINAL_HOLDOUT_START = datetime(2026, 3, 1, tzinfo=timezone.utc)
FINAL_HOLDOUT_END = datetime(2026, 6, 28, tzinfo=timezone.utc)

# Master universe frozen after blank-canvas discovery/replication. These names are
# outputs of the current research cycle, not inputs imported from any earlier
# strategy programme.
MASTER_UNIVERSE = (
    "1000SATSUSDT", "ADAUSDT", "ARBUSDT", "AVAXUSDT", "BETAUSDT",
    "BNXUSDT", "BOMEUSDT", "BONKUSDT", "CRVUSDT", "DOGEUSDT",
    "ENSUSDT", "ETHFIUSDT", "FETUSDT", "FLOKIUSDT", "FTMUSDT",
    "IOUSDT", "LDOUSDT", "LINKUSDT", "LISTAUSDT", "NEARUSDT",
    "NOTUSDT", "ORDIUSDT", "PENDLEUSDT", "PEOPLEUSDT", "RNDRUSDT",
    "RUNEUSDT", "SHIBUSDT", "TRXUSDT", "WLDUSDT", "ZKUSDT",
)

# Raw-data coverage audit performed before derivatives outcomes were loaded:
# BETAUSDT, BNXUSDT, FTMUSDT and RNDRUSDT have zero canonical Binance spot bars
# throughout the 2025-10-01..2026-02-28 validation window. The original validated
# computation therefore actually ranked these 26 continuously observable names.
# Freeze that realised validation universe explicitly rather than pretending the
# missing four were present or adding successor/replacement symbols post hoc.
VALIDATION_ACTIVE_UNIVERSE = tuple(
    symbol
    for symbol in MASTER_UNIVERSE
    if symbol not in {"BETAUSDT", "BNXUSDT", "FTMUSDT", "RNDRUSDT"}
)
EXPECTED_VALIDATION_MEMBERS = 26
UNIVERSE = MASTER_UNIVERSE  # backwards-compatible audit alias

RETURN_LOOKBACK_MINUTES = 60
RETURN_LOOKBACK_BARS = 4  # compatibility only; v2 validation uses exact clock-time joins
CROSS_SECTIONAL_BUCKETS = 5
SELECT_BUCKET = 5
SIGNAL_COMPLETION_MINUTES = 15
EXECUTION_DELAY_MINUTES = 60
ENTRY_OFFSET_MINUTES = SIGNAL_COMPLETION_MINUTES + EXECUTION_DELAY_MINUTES
HOLD_MINUTES = 24 * 60
VALIDATION_SIGNAL_END = VALIDATION_END - timedelta(minutes=ENTRY_OFFSET_MINUTES + HOLD_MINUTES)

EXECUTION_SPEC = {
    "master_universe": "30 names frozen after discovery",
    "validation_active_universe": "26 names with continuous raw Binance spot availability throughout validation; BETA/BNX/FTM/RNDR absent and not replaced",
    "signal_completeness": "all 26 validation-active names must have a complete 15-minute signal bar and an exact complete bar 60 clock-minutes earlier before a timestamp can be ranked",
    "predictor": "top cross-sectional quintile of both exact trailing 1h return and completed 15m high/low range",
    "signal_knowledge_time": "15m bar completion",
    "entry": "Binance USD-M perpetual 15m bar open 60 minutes after signal becomes knowable",
    "exit": "same perpetual 15m bar open 24 hours after entry",
    "relative_expression": "short equal-weight qualifying perpetuals plus long equal-weight validation-active spot panel",
    "funding": "actual archived USD-M funding events during holding period; short receives positive funding",
    "phase_validation": "96 non-overlapping UTC 15-minute clock-phase cohorts",
    "holdout_policy": "2026-03-01 through 2026-06-28 remains sealed; validation signals are purged if their delayed 24h exit would cross into 2026-03-01",
}
