from __future__ import annotations

import csv
import gzip
import hashlib
import logging
import os
import socket
import tempfile
import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from psycopg.types.json import Jsonb

from app.db import db_connection, fetch_one

logger = logging.getLogger(__name__)
DATA_BASE = "https://datasets.tardis.dev/v1/binance-futures/book_snapshot_25"
_STARTED = False
_START_LOCK = threading.Lock()


def _truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _enabled() -> bool:
    return _truthy("CINT001_TARDIS_DEPTH_ENABLED")


def _run_id() -> UUID:
    value = os.getenv("CINT001_BOOKTICKER_RUN_ID", "").strip()
    if not value:
        raise RuntimeError("CINT001_BOOKTICKER_RUN_ID is required")
    return UUID(value)


def _concurrency() -> int:
    return max(1, min(2, int(os.getenv("CINT001_TARDIS_DEPTH_CONCURRENCY", "1"))))


def _max_priority() -> int:
    return int(os.getenv("CINT001_TARDIS_DEPTH_MAX_PRIORITY", "0"))


def start_background() -> None:
    global _STARTED
    if not _enabled():
        return
    with _START_LOCK:
        if _STARTED:
            return
        _STARTED = True
    threading.Thread(
        target=_supervisor,
        name="cint001-tardis-depth-supervisor",
        daemon=True,
    ).start()


def _wait_for_schema() -> bool:
    for _ in range(180):
        try:
            row = fetch_one(
                """
                select
                    to_regclass('public.cint001_depth_days') d,
                    to_regclass('public.cint001_depth_snapshots') s,
                    to_regclass('public.cint001_signal_selection') sel
                """
            )
            if row and row.get("d") and row.get("s") and row.get("sel"):
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def _supervisor() -> None:
    try:
        run_id = _run_id()
    except Exception:
        logger.exception("C-INT-001 Tardis depth supervisor disabled: invalid run id")
        return
    if not _wait_for_schema():
        logger.error("C-INT-001 Tardis depth schema was not ready")
        return

    workers: list[threading.Thread] = []
    for slot in range(_concurrency()):
        thread = threading.Thread(
            target=_worker_loop,
            args=(run_id, slot),
            name=f"cint001-tardis-depth-{slot}",
            daemon=True,
        )
        thread.start()
        workers.append(thread)

    logger.info(
        "C-INT-001 Tardis depth samples started run=%s concurrency=%s max_priority=%s",
        run_id,
        len(workers),
        _max_priority(),
    )
    for thread in workers:
        thread.join()
    logger.info("C-INT-001 Tardis depth eligible queue drained run=%s", run_id)


def _claim(run_id: UUID, worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select id
                from cint001_depth_days
                where run_id=%s
                  and status in ('queued','retry_wait')
                  and not_before<=now()
                  and priority<=%s
                order by priority,trade_date,id
                for update skip locked
                limit 1
            )
            update cint001_depth_days d
               set status='running',attempts=attempts+1,locked_by=%s,
                   locked_at=now(),updated_at=now()
              from candidate c
             where d.id=c.id
            returning d.*
            """,
            (run_id, _max_priority(), worker_id),
        )
        row = cur.fetchone()
        conn.commit()
        return row


def _pending(run_id: UUID) -> int:
    row = fetch_one(
        """
        select count(*) n from cint001_depth_days
        where run_id=%s and priority<=%s
          and status in ('queued','retry_wait','running')
        """,
        (run_id, _max_priority()),
    ) or {}
    return int(row.get("n") or 0)


def _worker_loop(run_id: UUID, slot: int) -> None:
    worker_id = f"{socket.gethostname()}:{os.getpid()}:tardis-depth:{slot}"
    while True:
        item = _claim(run_id, worker_id)
        if item:
            _process_item(item)
            continue
        if _pending(run_id) == 0:
            return
        time.sleep(2)


def _targets(run_id: UUID, symbol: str, trade_date: date) -> list[datetime]:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select distinct target_ts from (
                select entry_ts target_ts from cint001_signal_selection
                where run_id=%s and futures_symbol=%s and entry_ts::date=%s
                union
                select exit_ts target_ts from cint001_signal_selection
                where run_id=%s and futures_symbol=%s and exit_ts::date=%s
            ) q order by target_ts
            """,
            (run_id, symbol, trade_date, run_id, symbol, trade_date),
        )
        return [row["target_ts"] for row in cur.fetchall()]


def _url(symbol: str, trade_date: date) -> str:
    return (
        f"{DATA_BASE}/{trade_date:%Y}/{trade_date:%m}/{trade_date:%d}/"
        f"{symbol}.csv.gz"
    )


def _download(client: httpx.Client, url: str, path: Path) -> tuple[int, str, int]:
    digest = hashlib.sha256()
    size = 0
    with client.stream("GET", url) as response:
        status = response.status_code
        if status != 200:
            return status, "", 0
        with path.open("wb") as handle:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                digest.update(chunk)
                size += len(chunk)
                handle.write(chunk)
    return 200, digest.hexdigest(), size


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromtimestamp(int(value) / 1_000_000, tz=timezone.utc)


def _levels(row: dict[str, str], side: str) -> list[dict[str, float]]:
    result: list[dict[str, float]] = []
    for i in range(25):
        price_raw = row.get(f"{side}s[{i}].price", "")
        amount_raw = row.get(f"{side}s[{i}].amount", "")
        if not price_raw or not amount_raw:
            continue
        try:
            price = float(price_raw)
            amount = float(amount_raw)
        except ValueError:
            continue
        if price <= 0 or amount <= 0:
            continue
        result.append({"price": price, "amount": amount})
    return result


def _extract(path: Path, targets: list[datetime]) -> tuple[list[tuple], dict[str, Any], int]:
    if not targets:
        return [], {"target_count": 0}, 0

    snapshots: list[tuple] = []
    target_index = 0
    row_count = 0
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
            "timestamp",
            "local_timestamp",
            "asks[0].price",
            "asks[0].amount",
            "bids[0].price",
            "bids[0].amount",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"Unsupported Tardis book_snapshot_25 schema; missing {missing}")

        for row in reader:
            row_count += 1
            try:
                snapshot_ts = _parse_timestamp(row["timestamp"])
                local_ts = _parse_timestamp(row["local_timestamp"])
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            bids = _levels(row, "bid")
            asks = _levels(row, "ask")
            if not bids or not asks or bids[0]["price"] >= asks[0]["price"]:
                continue

            while target_index < len(targets) and snapshot_ts >= targets[target_index]:
                target = targets[target_index]
                age_ms = max(0.0, (snapshot_ts - target).total_seconds() * 1000.0)
                snapshots.append((target, snapshot_ts, local_ts, age_ms, bids, asks))
                target_index += 1
            if target_index >= len(targets):
                break

    schema = {
        "provider": "tardis.dev",
        "dataset": "book_snapshot_25",
        "quote_construction": "top25_reconstructed_from_exchange_l2",
        "timing_policy": "first_valid_exchange_snapshot_at_or_after_target",
        "target_count": len(targets),
        "matched": len(snapshots),
        "raw_rows_scanned": row_count,
        "max_age_ms": max((snap[3] for snap in snapshots), default=None),
    }
    return snapshots, schema, row_count


def _finish(item_id: int, status: str, rows: int, checksum: str | None,
            schema: dict[str, Any], error: str | None = None) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_depth_days
               set status=%s,row_count=%s,checksum=%s,schema_info=%s,last_error=%s,
                   locked_by=null,locked_at=null,updated_at=now()
             where id=%s
            """,
            (status, rows, checksum, Jsonb(schema), error, item_id),
        )
        conn.commit()


def _fail(item: dict[str, Any], exc: BaseException) -> None:
    attempts = int(item.get("attempts") or 1)
    max_attempts = int(item.get("max_attempts") or 4)
    status = "failed" if attempts >= max_attempts else "retry_wait"
    delay = min(900, 30 * (2 ** min(attempts, 5)))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_depth_days
               set status=%s,last_error=%s,locked_by=null,locked_at=null,
                   not_before=now()+(%s*interval '1 second'),updated_at=now()
             where id=%s
            """,
            (status, f"{type(exc).__name__}: {exc}", delay, item["id"]),
        )
        conn.commit()


def _process_item(item: dict[str, Any]) -> None:
    run_id = UUID(str(item["run_id"]))
    symbol = str(item["futures_symbol"])
    trade_date = item["trade_date"]
    if isinstance(trade_date, str):
        trade_date = date.fromisoformat(trade_date)
    targets = _targets(run_id, symbol, trade_date)
    url = _url(symbol, trade_date)
    try:
        with tempfile.TemporaryDirectory(prefix="cint001-depth-") as temp_dir:
            path = Path(temp_dir) / f"{symbol}-{trade_date}.csv.gz"
            with httpx.Client(timeout=600, follow_redirects=True) as client:
                status_code, checksum, size = _download(client, url, path)
            if status_code != 200:
                if status_code == 404:
                    _finish(item["id"], "missing", 0, None,
                            {"provider": "tardis.dev", "url": url, "target_count": len(targets)},
                            "Tardis depth sample 404")
                    return
                raise RuntimeError(f"Tardis depth HTTP {status_code}")

            snapshots, schema, row_count = _extract(path, targets)
            schema["url"] = url
            schema["gzip_bytes"] = size
            if len(snapshots) != len(targets):
                raise ValueError(f"Matched {len(snapshots)} of {len(targets)} target timestamps")

            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into cint001_depth_snapshots(
                        run_id,futures_symbol,target_ts,snapshot_ts,local_ts,age_ms,
                        bids,asks,source_date,source
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict(run_id,futures_symbol,target_ts) do update set
                        snapshot_ts=excluded.snapshot_ts,local_ts=excluded.local_ts,
                        age_ms=excluded.age_ms,bids=excluded.bids,asks=excluded.asks,
                        source_date=excluded.source_date,source=excluded.source,
                        inserted_at=now()
                    """,
                    [
                        (run_id, symbol, snap[0], snap[1], snap[2], snap[3],
                         Jsonb(snap[4]), Jsonb(snap[5]), trade_date,
                         "tardis_book_snapshot_25_free_sample")
                        for snap in snapshots
                    ],
                )
                conn.commit()
            _finish(item["id"], "completed", row_count, checksum, schema)
            logger.info(
                "Tardis depth complete symbol=%s date=%s targets=%s rows_scanned=%s gzip_bytes=%s",
                symbol, trade_date, len(targets), row_count, size,
            )
    except Exception as exc:
        logger.exception("Tardis depth failed symbol=%s date=%s", symbol, trade_date)
        _fail(item, exc)
