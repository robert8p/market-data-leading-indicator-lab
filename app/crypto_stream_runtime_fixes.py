from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

from app.config import get_settings
from app.db import db_connection, fetch_all


class _CanonicalSymbolLookup:
    """Provider-aware compatibility lookup for legacy stream variable names.

    Kraken only calls .get(); Bybit both iterates the mapping and calls .get().
    Iteration therefore exposes Bybit symbols only, while .get() resolves either
    Kraken or Bybit symbols. This preserves the intended behaviour of both handlers.
    """

    def __init__(self, lookup: dict[str, str], bybit_symbols: list[str]):
        self._lookup = lookup
        self._bybit_symbols = tuple(bybit_symbols)

    def __iter__(self) -> Iterator[str]:
        return iter(self._bybit_symbols)

    def __len__(self) -> int:
        return len(self._bybit_symbols)

    def get(self, key: str, default: Any = None) -> Any:
        return self._lookup.get(key, default)


def _health(provider: str, service: str, *, status: str, error: str | None = None, messages: int = 0) -> None:
    """Health upsert with fully typed values."""
    now = datetime.now(timezone.utc)
    last_message_at = now if messages > 0 else None
    last_success_at = now if status == "connected" else None
    last_error_at = now if error is not None else None
    reconnect_delta = 1 if status == "reconnecting" else 0

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into provider_health(
                provider,service,status,last_message_at,last_success_at,last_error_at,
                message_count,reconnect_count,last_error,updated_at
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
            on conflict(provider,service) do update set
                status=excluded.status,
                last_message_at=coalesce(excluded.last_message_at,provider_health.last_message_at),
                last_success_at=coalesce(excluded.last_success_at,provider_health.last_success_at),
                last_error_at=coalesce(excluded.last_error_at,provider_health.last_error_at),
                message_count=provider_health.message_count+excluded.message_count,
                reconnect_count=provider_health.reconnect_count+excluded.reconnect_count,
                last_error=coalesce(excluded.last_error,provider_health.last_error),
                updated_at=now()
            """,
            (
                provider,
                service,
                status,
                last_message_at,
                last_success_at,
                last_error_at,
                int(messages),
                reconnect_delta,
                error,
            ),
        )
        conn.commit()


def _active_targets() -> set[str]:
    settings = get_settings()
    targets = set(settings.crypto_stream_core_symbols)
    rows = fetch_all(
        """
        select canonical_symbol
          from crypto_capture_targets
         where expires_at > now()
         order by priority_score desc nulls last, last_observed_at desc nulls last, updated_at desc
         limit %s
        """,
        (settings.crypto_stream_max_dynamic_targets,),
    )
    targets.update(str(row["canonical_symbol"]).upper() for row in rows)
    return targets


def install_crypto_stream_runtime_fixes(stream_module: Any) -> None:
    """Install narrow fixes before the stream event loop creates any tasks."""
    stream_module._health = _health

    targets = sorted(_active_targets())
    rows = fetch_all(
        """
        select provider,venue_symbol,canonical_symbol
          from crypto_venue_symbols
         where provider in ('coinbase','kraken','bybit')
           and tradable=true
           and canonical_symbol = any(%s)
        """,
        (targets,),
    )
    coinbase: dict[str, str] = {}
    kraken_and_bybit: dict[str, str] = {}
    bybit_symbols: list[str] = []
    for row in rows:
        provider = row["provider"]
        venue_symbol = row["venue_symbol"]
        canonical_symbol = row["canonical_symbol"]
        if provider == "coinbase":
            coinbase[venue_symbol] = canonical_symbol
        elif provider in {"kraken", "bybit"}:
            kraken_and_bybit[venue_symbol] = canonical_symbol
            if provider == "bybit":
                bybit_symbols.append(venue_symbol)

    # Compatibility aliases for three handlers whose local variable is named
    # mapping_by_* but whose body references canonical_by_*.
    stream_module.canonical_by_product = coinbase
    stream_module.canonical_by_symbol = _CanonicalSymbolLookup(
        kraken_and_bybit,
        sorted(bybit_symbols),
    )
