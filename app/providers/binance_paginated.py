from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from app.exceptions import EmptyData
from app.providers.base import Page, as_float, as_int
from app.providers.binance import BinanceProvider as BaseBinanceProvider


class BinanceProvider(BaseBinanceProvider):
    """Binance provider with durable pagination for 1-minute bar backfills.

    Binance /api/v3/klines returns at most 1,000 candles per request.  The
    original bar implementation treated the first response as the complete
    partition, silently truncating any interval longer than ~16h40m.  This
    implementation advances by the final candle open time and persists the
    next start timestamp in the partition cursor so long backfills are complete
    and resumable.
    """

    def iter_bar_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        symbol = partition["provider_symbol"]
        cursor = dict(partition.get("cursor") or {})
        if cursor.get("finished"):
            yield Page(rows=[], cursor=cursor, done=True)
            return

        partition_start_ms = int(partition["start_ts"].timestamp() * 1000)
        end_ms = int((partition["end_ts"] - timedelta(milliseconds=1)).timestamp() * 1000)
        start_ms = int(cursor.get("next_start_ms") or partition_start_ms)
        yielded = False

        while start_ms <= end_ms:
            payload = self.http.get(
                "https://api.binance.com/api/v3/klines",
                params={
                    "symbol": symbol,
                    "interval": "1m",
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": 1000,
                },
            )

            rows: list[dict[str, Any]] = []
            valid_candles = [c for c in payload if len(c) >= 11 and int(c[0]) <= end_ms]
            for candle in valid_candles:
                rows.append(
                    {
                        "provider": self.name,
                        "instrument_id": partition["instrument_id"],
                        "ts": datetime.fromtimestamp(int(candle[0]) / 1000, tz=timezone.utc),
                        "open": as_float(candle[1]),
                        "high": as_float(candle[2]),
                        "low": as_float(candle[3]),
                        "close": as_float(candle[4]),
                        "volume": as_float(candle[5]),
                        "quote_volume": as_float(candle[7]),
                        "trade_count": as_int(candle[8]),
                        "vwap": None,
                        "taker_buy_base_volume": as_float(candle[9]),
                        "taker_buy_quote_volume": as_float(candle[10]),
                        "source_feed": "binance_spot",
                    }
                )

            if not valid_candles:
                if not yielded:
                    raise EmptyData(f"No Binance klines for {symbol} in this partition")
                yield Page(rows=[], cursor={"next_start_ms": None, "finished": True}, done=True)
                return

            yielded = True
            last_open_ms = int(valid_candles[-1][0])
            next_start_ms = last_open_ms + 60_000
            done = len(valid_candles) < 1000 or next_start_ms > end_ms

            yield Page(
                rows=rows,
                cursor={"next_start_ms": None if done else next_start_ms, "finished": done},
                done=done,
            )
            if done:
                return

            if next_start_ms <= start_ms:
                raise RuntimeError(
                    f"Binance kline pagination did not advance for {symbol}: "
                    f"start={start_ms} next={next_start_ms}"
                )
            start_ms = next_start_ms
