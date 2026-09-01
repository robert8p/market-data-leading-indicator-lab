from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, AsyncIterator

import httpx

from .config import Settings

LOGGER = logging.getLogger(__name__)


class SupabaseRPCError(RuntimeError):
    pass


@dataclass(slots=True)
class SupabaseRPC:
    settings: Settings
    client: httpx.AsyncClient

    @classmethod
    def create(cls, settings: Settings) -> "SupabaseRPC":
        headers = {
            "apikey": settings.supabase_publishable_key,
            "Authorization": f"Bearer {settings.supabase_publishable_key}",
            "Content-Type": "application/json",
            "Accept-Profile": "public",
            "Content-Profile": "public",
            "User-Agent": "kalshi-perps-probability-lab/0.1",
        }
        return cls(
            settings=settings,
            client=httpx.AsyncClient(headers=headers, timeout=httpx.Timeout(90.0, connect=20.0)),
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def call(
        self,
        function_name: str,
        payload: dict[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
        retries: int = 3,
    ) -> Any:
        body = dict(payload or {})
        body.setdefault("p_token", self.settings.kalshi_app_token)
        url = f"{self.settings.supabase_url}/rest/v1/rpc/{function_name}"

        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = await self.client.post(
                    url,
                    content=json.dumps(body, default=str, separators=(",", ":")),
                    timeout=timeout_seconds or 90.0,
                )
                if response.status_code >= 400:
                    detail = response.text[:2_000]
                    raise SupabaseRPCError(
                        f"RPC {function_name} failed with HTTP {response.status_code}: {detail}"
                    )
                if not response.content:
                    return None
                return response.json()
            except (httpx.HTTPError, ValueError, SupabaseRPCError) as exc:
                last_error = exc
                retryable = not isinstance(exc, SupabaseRPCError) or "HTTP 5" in str(exc) or "429" in str(exc)
                if attempt + 1 >= retries or not retryable:
                    break
                await asyncio.sleep(min(2**attempt, 8))

        raise SupabaseRPCError(f"RPC {function_name} failed: {last_error}") from last_error

    @staticmethod
    def unwrap_scalar(value: Any) -> Any:
        """Normalise PostgREST scalar/table-function response shapes."""
        if isinstance(value, list) and len(value) == 1:
            item = value[0]
            if isinstance(item, dict):
                if len(item) == 1:
                    return next(iter(item.values()))
            return item
        return value

    @staticmethod
    def unwrap_rows(value: Any) -> list[dict[str, Any]]:
        if value is None:
            return []
        if isinstance(value, dict):
            value = [value]
        if not isinstance(value, list):
            raise SupabaseRPCError(f"Expected row list, received {type(value).__name__}")
        rows: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, dict):
                continue
            row = item.get("row_data", item)
            if isinstance(row, dict):
                rows.append(row)
        return rows

    async def paged_training_rows(
        self,
        split: str,
        page_size: int,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        # Hosted PostgREST projects commonly cap table-function responses at 1,000 rows
        # even when the SQL function accepts a larger limit. Page at or below that cap
        # so a short HTTP response cannot silently truncate the research dataset.
        request_limit = min(max(int(page_size), 1), 1_000)
        after_ts: str | None = None
        after_symbol: str | None = None
        while True:
            response = await self.call(
                "kalshi_app_crossvenue_training_page",
                {
                    "p_split": split,
                    "p_after_ts": after_ts,
                    "p_after_symbol": after_symbol,
                    "p_limit": request_limit,
                },
                timeout_seconds=120,
            )
            rows = self.unwrap_rows(response)
            if not rows:
                break
            yield rows
            last = rows[-1]
            next_ts = str(last["decision_ts"])
            next_symbol = str(last["symbol"])
            if next_ts == after_ts and next_symbol == after_symbol:
                raise SupabaseRPCError(
                    f"Training pagination cursor did not advance for {split}: "
                    f"{after_ts}/{after_symbol}"
                )
            after_ts, after_symbol = next_ts, next_symbol
            if len(rows) < request_limit:
                break

    async def candle_rows(
        self,
        ticker: str,
        *,
        after_ts: str | None = None,
        page_size: int = 7_500,
        all_pages: bool = False,
    ) -> list[dict[str, Any]]:
        request_limit = min(max(int(page_size), 1), 1_000)
        should_page = bool(all_pages or int(page_size) > request_limit)
        rows_out: list[dict[str, Any]] = []
        cursor = after_ts
        while True:
            response = await self.call(
                "kalshi_app_candle_page",
                {"p_ticker": ticker, "p_after_ts": cursor, "p_limit": request_limit},
                timeout_seconds=120,
            )
            rows = self.unwrap_rows(response)
            rows_out.extend(rows)
            if not should_page or len(rows) < request_limit:
                break
            next_cursor = str(rows[-1]["end_period_ts"])
            if next_cursor == cursor:
                raise SupabaseRPCError(
                    f"Candle pagination cursor did not advance for {ticker}: {cursor}"
                )
            cursor = next_cursor
        return rows_out
