from __future__ import annotations

import contextlib
from typing import Any, Iterator, Sequence

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from app.config import get_settings


_pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = ConnectionPool(
            conninfo=settings.database_url,
            min_size=1,
            max_size=settings.db_pool_size,
            kwargs={"row_factory": dict_row, "prepare_threshold": None, "autocommit": False},
            open=True,
        )
    return _pool


@contextlib.contextmanager
def db_connection() -> Iterator[Connection]:
    with get_pool().connection() as conn:
        yield conn


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
