from datetime import datetime, timedelta, timezone

from app.dynamic_detection import DetectorConfig, MarketObservation, MultiVenueDetector


def observation(provider, market_type, symbol, canonical, ts, price, volume=1_000_000):
    return MarketObservation(
        provider=provider,
        market_type=market_type,
        venue_symbol=symbol,
        canonical_symbol=canonical,
        ts=ts,
        price=price,
        quote_volume_24h=volume,
    )


def config(**overrides):
    values = {
        "confirmation_seconds": 90,
        "cooldown_seconds": 300,
    }
    values.update(overrides)
    return DetectorConfig(**values)


def test_fast_single_venue_move_activates_candidate():
    detector = MultiVenueDetector(config())
    start = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    assert detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start, 100)) is None
    decision = detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start + timedelta(seconds=60), 100.8, 1_010_000))
    assert decision is not None
    assert "fast_single_venue" in decision.trigger_type
    assert decision.provider_count == 1


def test_lower_move_is_admitted_when_two_venues_confirm():
    detector = MultiVenueDetector(config())
    start = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start, 100))
    detector.ingest(observation("kraken", "spot", "ABC/USD", "ABC", start, 100))
    first = detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start + timedelta(minutes=5), 100.8, 1_020_000))
    assert first is None
    decision = detector.ingest(observation("kraken", "spot", "ABC/USD", "ABC", start + timedelta(minutes=5), 100.8, 1_020_000))
    assert decision is not None
    assert "cross_venue_confirmation" in decision.trigger_type
    assert decision.provider_count == 2


def test_derivatives_lead_requires_spot_confirmation():
    detector = MultiVenueDetector(config(confirmed_move_pct=5.0))
    start = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start, 100))
    detector.ingest(observation("binance_futures", "perpetual", "ABCUSDT", "ABC", start, 100))
    detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start + timedelta(minutes=5), 100.4, 1_010_000))
    decision = detector.ingest(observation("binance_futures", "perpetual", "ABCUSDT", "ABC", start + timedelta(minutes=5), 101.1, 1_030_000))
    assert decision is not None
    assert "derivatives_led_with_spot_confirmation" in decision.trigger_type
    assert decision.spot_provider_count == 1
    assert decision.derivatives_provider_count == 1


def test_slow_gradual_move_is_detected_without_false_fast_window():
    detector = MultiVenueDetector(config(fast_move_pct=5.0, single_venue_move_pct=5.0))
    start = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    detector.ingest(observation("kraken", "spot", "ABC/USD", "ABC", start, 100))
    # Frequent observations ensure each window uses an appropriately aged reference.
    for minute in range(1, 15):
        detector.ingest(observation("kraken", "spot", "ABC/USD", "ABC", start + timedelta(minutes=minute), 100 + minute * 0.15, 1_000_000 + minute * 1_000))
    decision = detector.ingest(observation("kraken", "spot", "ABC/USD", "ABC", start + timedelta(minutes=15), 102.6, 1_030_000))
    assert decision is not None
    assert "slow_single_venue" in decision.trigger_type


def test_cooldown_suppresses_repeat_detection():
    detector = MultiVenueDetector(config())
    start = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start, 100))
    first = detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start + timedelta(seconds=60), 101, 1_010_000))
    assert first is not None
    repeat = detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start + timedelta(seconds=90), 102, 1_020_000))
    assert repeat is None


def test_multiple_pairs_on_same_provider_remain_distinct_evidence():
    detector = MultiVenueDetector(config())
    start = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start, 100))
    detector.ingest(observation("coinbase", "spot", "ABC-EUR", "ABC", start, 90))
    detector.ingest(observation("coinbase", "spot", "ABC-USD", "ABC", start + timedelta(seconds=60), 100.6))
    decision = detector.ingest(observation("coinbase", "spot", "ABC-EUR", "ABC", start + timedelta(seconds=60), 90.8))
    assert decision is not None
    assert {item.venue_symbol for item in decision.evidence} == {"ABC-USD", "ABC-EUR"}
    assert decision.provider_count == 1
