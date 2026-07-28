from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Deque


def _utc(ts: datetime) -> datetime:
    return ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class DetectorConfig:
    fast_window_seconds: int = 60
    fast_move_pct: float = 0.75
    base_window_seconds: int = 300
    single_venue_move_pct: float = 1.50
    slow_window_seconds: int = 900
    slow_move_pct: float = 2.50
    confirmation_count: int = 2
    confirmed_move_pct: float = 0.75
    derivative_move_pct: float = 1.00
    spot_confirmation_pct: float = 0.35
    volume_move_pct: float = 0.50
    volume_acceleration: float = 3.0
    confirmation_seconds: int = 45
    cooldown_seconds: int = 300


@dataclass(frozen=True)
class MarketObservation:
    provider: str
    market_type: str
    venue_symbol: str
    canonical_symbol: str
    ts: datetime
    price: float
    quote_volume_24h: float | None = None


@dataclass(frozen=True)
class VenueSignal:
    provider: str
    market_type: str
    venue_symbol: str
    canonical_symbol: str
    observed_at: datetime
    price: float
    fast_move_pct: float | None
    base_move_pct: float | None
    slow_move_pct: float | None
    volume_acceleration: float | None
    recent_quote_volume: float | None

    @property
    def best_move_pct(self) -> float:
        return max(
            self.fast_move_pct if self.fast_move_pct is not None else float("-inf"),
            self.base_move_pct if self.base_move_pct is not None else float("-inf"),
            self.slow_move_pct if self.slow_move_pct is not None else float("-inf"),
        )

    def as_dict(self) -> dict:
        payload = asdict(self)
        payload["observed_at"] = self.observed_at.isoformat()
        payload["best_move_pct"] = self.best_move_pct
        return payload


@dataclass(frozen=True)
class DetectionDecision:
    canonical_symbol: str
    detected_at: datetime
    score: float
    trigger_type: str
    provider_count: int
    spot_provider_count: int
    derivatives_provider_count: int
    evidence: tuple[VenueSignal, ...]

    def as_reason(self) -> dict:
        return {
            "score": self.score,
            "trigger_type": self.trigger_type,
            "provider_count": self.provider_count,
            "spot_provider_count": self.spot_provider_count,
            "derivatives_provider_count": self.derivatives_provider_count,
            "evidence": [item.as_dict() for item in self.evidence],
        }


class MultiVenueDetector:
    """Stateful, provider-neutral detector for broad crypto candidate discovery.

    It only decides which symbols merit deeper collection. It does not make a
    trading prediction or label an event as a winner.
    """

    def __init__(self, config: DetectorConfig):
        self.config = config
        self.history: dict[tuple[str, str, str], Deque[MarketObservation]] = defaultdict(deque)
        self.latest: dict[tuple[str, str, str, str], VenueSignal] = {}
        self.last_trigger: dict[str, datetime] = {}

    def ingest(self, observation: MarketObservation) -> DetectionDecision | None:
        obs = MarketObservation(
            provider=observation.provider.lower(),
            market_type=observation.market_type.lower(),
            venue_symbol=observation.venue_symbol,
            canonical_symbol=observation.canonical_symbol.upper(),
            ts=_utc(observation.ts),
            price=float(observation.price),
            quote_volume_24h=(
                float(observation.quote_volume_24h)
                if observation.quote_volume_24h is not None
                else None
            ),
        )
        if obs.price <= 0:
            return None

        key = (obs.provider, obs.market_type, obs.venue_symbol)
        points = self.history[key]
        if points and obs.ts < points[-1].ts:
            return None
        points.append(obs)

        retention = max(
            self.config.slow_window_seconds,
            self.config.base_window_seconds * 2,
        ) + self.config.confirmation_seconds
        cutoff = obs.ts - timedelta(seconds=retention)
        while points and points[0].ts < cutoff:
            points.popleft()

        signal = self._signal(points)
        if signal is None:
            return None
        latest_key = (signal.provider, signal.market_type, signal.venue_symbol, signal.canonical_symbol)
        existing = self.latest.get(latest_key)
        if existing is None or signal.observed_at >= existing.observed_at:
            self.latest[latest_key] = signal

        stale_cutoff = obs.ts - timedelta(seconds=self.config.confirmation_seconds)
        evidence = [
            item
            for item in self.latest.values()
            if item.canonical_symbol == obs.canonical_symbol and item.observed_at >= stale_cutoff
        ]
        if not evidence:
            return None

        decision = self._evaluate(obs.canonical_symbol, obs.ts, evidence)
        if decision is None:
            return None
        last = self.last_trigger.get(obs.canonical_symbol)
        if last and obs.ts - last < timedelta(seconds=self.config.cooldown_seconds):
            return None
        self.last_trigger[obs.canonical_symbol] = obs.ts
        return decision

    def _signal(self, points: Deque[MarketObservation]) -> VenueSignal | None:
        if len(points) < 2:
            return None
        current = points[-1]
        fast = self._move(points, current, self.config.fast_window_seconds)
        base = self._move(points, current, self.config.base_window_seconds)
        slow = self._move(points, current, self.config.slow_window_seconds)
        volume_acceleration, recent_volume = self._volume_acceleration(points, current)
        if fast is None and base is None and slow is None:
            return None
        return VenueSignal(
            provider=current.provider,
            market_type=current.market_type,
            venue_symbol=current.venue_symbol,
            canonical_symbol=current.canonical_symbol,
            observed_at=current.ts,
            price=current.price,
            fast_move_pct=fast,
            base_move_pct=base,
            slow_move_pct=slow,
            volume_acceleration=volume_acceleration,
            recent_quote_volume=recent_volume,
        )

    @staticmethod
    def _at_or_before(points: Deque[MarketObservation], target: datetime) -> MarketObservation | None:
        candidate = None
        for point in points:
            if point.ts <= target:
                candidate = point
            else:
                break
        return candidate

    def _move(
        self,
        points: Deque[MarketObservation],
        current: MarketObservation,
        seconds: int,
    ) -> float | None:
        target = current.ts - timedelta(seconds=seconds)
        base = self._at_or_before(points, target)
        if base is None or base.price <= 0:
            return None
        observed_span = (current.ts - base.ts).total_seconds()
        if observed_span > seconds * 1.25:
            return None
        return (current.price / base.price - 1.0) * 100.0

    def _volume_acceleration(
        self,
        points: Deque[MarketObservation],
        current: MarketObservation,
    ) -> tuple[float | None, float | None]:
        if current.quote_volume_24h is None:
            return None, None
        first_cutoff = current.ts - timedelta(seconds=self.config.base_window_seconds)
        second_cutoff = current.ts - timedelta(seconds=self.config.base_window_seconds * 2)
        previous = self._at_or_before(points, first_cutoff)
        older = self._at_or_before(points, second_cutoff)
        if previous is None or older is None:
            return None, None
        if previous.quote_volume_24h is None or older.quote_volume_24h is None:
            return None, None
        recent_delta = max(current.quote_volume_24h - previous.quote_volume_24h, 0.0)
        prior_delta = max(previous.quote_volume_24h - older.quote_volume_24h, 0.0)
        if recent_delta <= 0:
            return 0.0, 0.0
        if prior_delta <= 0:
            return 10.0, recent_delta
        return min(recent_delta / prior_delta, 10.0), recent_delta

    def _evaluate(
        self,
        canonical: str,
        now: datetime,
        evidence: list[VenueSignal],
    ) -> DetectionDecision | None:
        config = self.config
        trigger_types: list[str] = []

        fast_hits = [s for s in evidence if (s.fast_move_pct or float("-inf")) >= config.fast_move_pct]
        base_hits = [s for s in evidence if (s.base_move_pct or float("-inf")) >= config.single_venue_move_pct]
        slow_hits = [s for s in evidence if (s.slow_move_pct or float("-inf")) >= config.slow_move_pct]
        if fast_hits:
            trigger_types.append("fast_single_venue")
        if base_hits:
            trigger_types.append("base_single_venue")
        if slow_hits:
            trigger_types.append("slow_single_venue")

        confirmation_hits = [
            s
            for s in evidence
            if (
                (s.base_move_pct is not None and s.base_move_pct >= config.confirmed_move_pct)
                or (s.fast_move_pct is not None and s.fast_move_pct >= config.confirmed_move_pct * 0.60)
                or (s.slow_move_pct is not None and s.slow_move_pct >= config.confirmed_move_pct * 1.75)
            )
        ]
        confirmation_providers = {s.provider for s in confirmation_hits}
        if len(confirmation_providers) >= config.confirmation_count:
            trigger_types.append("cross_venue_confirmation")

        spot = [s for s in evidence if s.market_type == "spot"]
        derivatives = [s for s in evidence if s.market_type != "spot"]
        derivative_move = max((s.base_move_pct or s.fast_move_pct or float("-inf") for s in derivatives), default=float("-inf"))
        spot_move = max((s.base_move_pct or s.fast_move_pct or float("-inf") for s in spot), default=float("-inf"))
        if derivative_move >= config.derivative_move_pct and spot_move >= config.spot_confirmation_pct:
            trigger_types.append("derivatives_led_with_spot_confirmation")

        volume_hits = [
            s
            for s in evidence
            if (s.base_move_pct or s.fast_move_pct or float("-inf")) >= config.volume_move_pct
            and (s.volume_acceleration or 0.0) >= config.volume_acceleration
        ]
        if volume_hits:
            trigger_types.append("price_volume_acceleration")

        if not trigger_types:
            return None

        provider_count = len({s.provider for s in evidence})
        spot_count = len({s.provider for s in spot})
        derivatives_count = len({s.provider for s in derivatives})
        fast_ratio = max(((s.fast_move_pct or 0.0) / config.fast_move_pct for s in evidence), default=0.0)
        base_ratio = max(((s.base_move_pct or 0.0) / config.single_venue_move_pct for s in evidence), default=0.0)
        slow_ratio = max(((s.slow_move_pct or 0.0) / config.slow_move_pct for s in evidence), default=0.0)
        max_price_ratio = max(fast_ratio, base_ratio, slow_ratio)
        positive_moves = [s.best_move_pct for s in evidence if s.best_move_pct > 0]
        median_move = median(positive_moves) if positive_moves else 0.0
        max_volume_accel = max((s.volume_acceleration or 0.0 for s in evidence), default=0.0)

        score = 0.0
        score += min(max_price_ratio, 2.0) * 30.0
        score += min(provider_count, 4) * 8.0
        score += min(max(median_move, 0.0) / max(config.confirmed_move_pct, 0.01), 2.0) * 8.0
        score += min(max_volume_accel / max(config.volume_acceleration, 0.01), 2.0) * 7.0
        if "cross_venue_confirmation" in trigger_types:
            score += 10.0
        if "derivatives_led_with_spot_confirmation" in trigger_types:
            score += 8.0
        score = round(min(score, 100.0), 3)

        ordered = tuple(sorted(evidence, key=lambda item: (item.best_move_pct, item.provider), reverse=True))
        return DetectionDecision(
            canonical_symbol=canonical,
            detected_at=now,
            score=score,
            trigger_type="+".join(trigger_types),
            provider_count=provider_count,
            spot_provider_count=spot_count,
            derivatives_provider_count=derivatives_count,
            evidence=ordered,
        )
