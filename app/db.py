from __future__ import annotations

import contextlib
import logging
import os
import re
from typing import Any, Iterator, Sequence

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from app.config import get_settings


logger = logging.getLogger(__name__)
_pool: ConnectionPool | None = None


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _connection_info(base: str) -> str:
    """Optionally override only the Supabase pooler port without exposing secrets.

    Render deploys workers with zero-downtime overlap.  Supabase session mode has
    a small per-role client cap, so an incoming worker can temporarily use the
    transaction-pool port during handoff.  The database host, user, password,
    database name and query parameters remain unchanged.
    """
    override = os.getenv("DB_POOLER_PORT_OVERRIDE", "").strip()
    if not override:
        return base
    try:
        port = int(override)
    except ValueError:
        raise ValueError("DB_POOLER_PORT_OVERRIDE must be an integer")
    if not (1 <= port <= 65535):
        raise ValueError("DB_POOLER_PORT_OVERRIDE is outside the valid TCP port range")
    updated, count = re.subn(
        r"(?<=\.pooler\.supabase\.com):\d+(?=/)",
        f":{port}",
        base,
        count=1,
    )
    if count != 1:
        raise ValueError("DB_POOLER_PORT_OVERRIDE was requested but DATABASE_URL is not a Supabase pooler URL")
    logger.warning("Using Supabase pooler port override=%s for this worker process", port)
    return updated


def _reconnect_failed(pool: ConnectionPool) -> None:
    logger.error(
        "Postgres pool could not reconnect within the configured reconnect window; "
        "the pool remains open and later acquisitions will continue retrying"
    )


def get_pool() -> ConnectionPool:
    """Return the process-wide Postgres pool with stale-connection protection.

    The worker normally uses Supabase's session pooler, so it deliberately keeps
    a small client pool. During a Render handoff the port can be overridden to
    the transaction pooler without changing credentials. Each checkout is
    health-checked, old/idle connections are recycled, and psycopg's reconnect
    worker uses exponential backoff before the reconnect timeout is reached.
    """
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = ConnectionPool(
            conninfo=_connection_info(settings.database_url),
            min_size=1,
            max_size=settings.db_pool_size,
            timeout=_env_float("DB_POOL_ACQUIRE_TIMEOUT_SECONDS", 60.0),
            max_lifetime=_env_float("DB_POOL_MAX_LIFETIME_SECONDS", 900.0),
            max_idle=_env_float("DB_POOL_MAX_IDLE_SECONDS", 120.0),
            reconnect_timeout=_env_float("DB_POOL_RECONNECT_TIMEOUT_SECONDS", 300.0),
            reconnect_failed=_reconnect_failed,
            check=ConnectionPool.check_connection,
            kwargs={
                "row_factory": dict_row,
                "prepare_threshold": None,
                "autocommit": False,
                "connect_timeout": _env_int("DB_CONNECT_TIMEOUT_SECONDS", 15),
                "application_name": os.getenv("DB_APPLICATION_NAME", "market-data-lab"),
            },
            open=True,
        )
    return _pool


def check_pool() -> None:
    """Check idle pooled connections and replace any that are broken."""
    get_pool().check()


@contextlib.contextmanager
def db_connection() -> Iterator[Connection]:
    with get_pool().connection() as conn:
        try:
            yield conn
        except Exception:
            # Never return an aborted transaction to the pool. psycopg's pool
            # also resets returned connections; the explicit rollback keeps the
            # contract obvious for long-running workers.
            try:
                conn.rollback()
            except Exception:
                pass
            raise


def fetch_one(sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        row = cur.fetchone()
        conn.commit()
        return row


def fetch_all(sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = list(cur.fetchall())
        conn.commit()
        return rows


def execute(sql: str, params: Sequence[Any] | None = None) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        conn.commit()
