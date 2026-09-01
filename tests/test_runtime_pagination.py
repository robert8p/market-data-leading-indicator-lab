from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from kalshi_perps_app.supabase import SupabaseRPC


class PagedFakeRPC(SupabaseRPC):
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.settings = None
        self.client = None
        self.pages = list(pages)
        self.payloads: list[dict[str, Any]] = []

    async def call(
        self,
        function_name: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
        retries: int = 3,
    ) -> Any:
        self.payloads.append(dict(payload or {}))
        rows = self.pages.pop(0) if self.pages else []
        return [{"row_data": row} for row in rows]


def test_hosted_row_cap_does_not_truncate_training_or_live_candles() -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    training = [
        {
            "decision_ts": (start + timedelta(hours=i)).isoformat(),
            "symbol": "BTC",
        }
        for i in range(2_500)
    ]
    training_db = PagedFakeRPC(
        [training[:1_000], training[1_000:2_000], training[2_000:]]
    )

    async def collect_training() -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        async for page in training_db.paged_training_rows("discovery", 7_500):
            output.extend(page)
        return output

    assert len(asyncio.run(collect_training())) == 2_500
    assert all(payload["p_limit"] == 1_000 for payload in training_db.payloads)

    candles = [
        {
            "end_period_ts": (start + timedelta(minutes=i)).isoformat(),
            "price_close": 100 + i,
        }
        for i in range(1_562)
    ]
    candle_db = PagedFakeRPC([candles[:1_000], candles[1_000:]])

    loaded = asyncio.run(
        candle_db.candle_rows("KXBTCPERP", page_size=5_000)
    )

    assert len(loaded) == 1_562
    assert all(payload["p_limit"] == 1_000 for payload in candle_db.payloads)
