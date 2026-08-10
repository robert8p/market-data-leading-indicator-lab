from __future__ import annotations

import csv
import gzip
import hashlib
import io
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
DATA_BASE = "https://datasets.tardis.dev/v1/binance-futures/quotes"
_STARTED = False
_START_LOCK = threading.Lock()


def _enabled() -> bool:
    return os.getenv("CINT001_TARDIS_QUOTES_ENABLED", "").strip().lower() in {
        "1", "true", "yes", "on"
    }


def _run_id() -> UUID:
    value = os.getenv("CINT001_BOOKTICKER_RUN_ID", "").strip()
    if not value:
        raise RuntimeError("CINT001_BOOKTICKER_RUN_ID is required")
    return UUID(value)


def _concurrency() -> int:
    return max(1, min(4, int(os.getenv("CINT001_TARDIS_QUOTES_CONCURRENCY", "2"))))


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
        name="cint001-tardis-quotes-supervisor",
        daemon=True,
    ).start()


def _wait_for_schema() -> bool:
    for _ in range(180):
        try:
            row = fetch_one(
                """
                select
                    to_regclass('public.cint001_bookticker_days') d,
                    to_regclass('public.cint001_bookticker_snapshots') s,
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
        logger.exception("C-INT-001 Tardis quote supervisor disabled: invalid run id")
        return
    if not _wait_for_schema():
        logger.error("C-INT-001 Tardis quote schema was not ready")
        return

    workers: list[threading.Thread] = []
    for slot in range(_concurrency()):
        thread = threading.Thread(
            target=_worker_loop,
            args=(run_id, slot),
            name=f"cint001-tardis-quotes-{slot}",
            daemon=True,
        )
        thread.start()
        workers.append(thread)

    logger.info(
        "C-INT-001 Tardis free quote samples started run=%s concurrency=%s",
        run_id,
        len(workers),
    )
    for thread in workers:
        thread.join()
    logger.info("C-INT-001 Tardis free quote sample queue drained run=%s", run_id)


def _claim(run_id: UUID, worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select id
                from cint001_bookticker_days
                where run_id=%s
                  and extract(day from trade_date)=1
                  and status in ('queued','retry_wait')
                  and not_before<=now()
                order by trade_date,id
                for update skip locked
                limit 1
            )
            update cint001_bookticker_days d
               set status='running',
                   attempts=attempts+1,
                   locked_by=%s,
                   locked_at=now(),
                   updated_at=now()
              from candidate c
             where d.id=c.id
            returning d.*
            """,
            (run_id, worker_id),
        )
        row = cur.fetchone()
        conn.commit()
        return row


def _pending(run_id: UUID) -> int:
    row = fetch_one(
        """
        select count(*) n
        from cint001_bookticker_days
        where run_id=%s
          and extract(day from trade_date)=1
          and status in ('queued','retry_wait','running')
        """,
        (run_id,),
    ) or {}
    return int(row.get("n") or 0)


def _worker_loop(run_id: UUID, slot: int) -> None:
    worker_id = f"{socket.gethostname()}:{os.getpid()}:tardis-quotes:{slot}"
    while True:
        item = _claim(run_id, worker_id)
        if item:
            _process_item(item)
            continue
        if _pending(run_id) == 0:
            return
        time.sleep(2)


def _targets(run_id: UUID, futures_symbol: str, trade_date: date) -> list[datetime]:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            select distinct target_ts
            from (
                select entry_ts target_ts
                from cint001_signal_selection
                where run_id=%s and futures_symbol=%s and entry_ts::date=%s
                union
                select exit_ts target_ts
                from cint001_signal_selection
                where run_id=%s and futures_symbol=%s and exit_ts::date=%s
            ) q
            order by target_ts
            """,
            (run_id, futures_symbol, trade_date, run_id, futures_symbol, trade_date),
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
    raw = int(value)
    return datetime.fromtimestamp(raw / 1_000_000, tz=timezone.utc)


def _extract(path: Path, targets: list[datetime]) -> tuple[list[tuple], dict[str, Any], int]:
    if not targets:
        return [], {"target_count": 0}, 0

    snapshots: list[tuple] = []
    row_count = 0
    target_index = 0
    required = {
        "timestamp",
        "local_timestamp",
        "ask_amount",
        "ask_price",
        "bid_price",
        "bid_amount",
    }

    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or [])
        missing = sorted(required - fields)
        if missing:
            raise ValueError(f"Unsupported Tardis quotes schema; missing {missing}")

        for row in reader:
            row_count += 1
            try:
                quote_ts = _parse_timestamp(row["timestamp"])
                local_ts = _parse_timestamp(row["local_timestamp"])
                ask = float(row["ask_price"])
                bid = float(row["bid_price"])
                ask_qty = float(row["ask_amount"]) if row["ask_amount"] else None
                bid_qty = float(row["bid_amount"]) if row["bid_amount"] else None
            except (TypeError, ValueError, OverflowError, OSError):
                continue

            if bid <= 0 or ask < bid:
                continue

            while target_index < len(targets) and quote_ts >= targets[target_index]:
                target = targets[target_index]
                age_ms = max(0.0, (quote_ts - target).total_seconds() * 1000.0)
                local_age_ms = (local_ts - target).total_seconds() * 1000.0
                snapshots.append(
                    (
                        target,
                        quote_ts,
                        bid,
                        bid_qty,
                        ask,
                        ask_qty,
                        age_ms,
                        "at_or_after",
                        local_age_ms,
                    )
                )
                target_index += 1

            if target_index >= len(targets):
                break

    schema = {
        "provider": "tardis.dev",
        "dataset": "quotes",
        "quote_construction": "reconstructed_from_exchange_l2",
        "timing_policy": "first_valid_exchange_timestamp_at_or_after_target",
        "target_count": len(targets),
        "matched": len(snapshots),
        "raw_rows_scanned": row_count,
        "max_exchange_age_ms": max((row[6] for row in snapshots), default=None),
        "max_local_age_ms": max((row[8] for row in snapshots), default=None),
    }
    return snapshots, schema, row_count


def _finish(
    item_id: int,
    status: str,
    rows: int,
    checksum: str | None,
    schema: dict[str, Any],
    error: str | None = None,
) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_bookticker_days
               set status=%s,
                   row_count=%s,
                   checksum=%s,
                   schema_info=%s,
                   last_error=%s,
                   locked_by=null,
                   locked_at=null,
                   updated_at=now()
             where id=%s
            """,
            (status, rows, checksum, Jsonb(schema), error, item_id),
        )
        conn.commit()


def _fail(item: dict[str, Any], exc: BaseException) -> None:
    attempts = int(item.get("attempts") or 1)
    max_attempts = int(item.get("max_attempts") or 5)
    status = "failed" if attempts >= max_attempts else "retry_wait"
    delay = min(900, 15 * (2 ** min(attempts, 6)))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_bookticker_days
               set status=%s,
                   last_error=%s,
                   locked_by=null,
                   locked_at=null,
                   not_before=now()+(%s*interval '1 second'),
                   updated_at=now()
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
    if not targets:
        _finish(item["id"], "completed", 0, None, {"target_count": 0})
        return

    url = _url(symbol, trade_date)
    try:
        with tempfile.TemporaryDirectory(prefix="cint001-tardis-") as temp_dir:
            path = Path(temp_dir) / f"{symbol}-{trade_date}.csv.gz"
            with httpx.Client(timeout=300, follow_redirects=True) as client:
                status_code, checksum, size = _download(client, url, path)

            if status_code != 200:
                if status_code in {401, 403}:
                    raise RuntimeError(
                        f"Tardis free-sample access denied HTTP {status_code} for {trade_date}"
                    )
                if status_code == 404:
                    _finish(
                        item["id"],
                        "missing",
                        0,
                        None,
                        {"provider": "tardis.dev", "url": url, "target_count": len(targets)},
                        "Tardis sample 404",
                    )
                    return
                raise RuntimeError(f"Tardis HTTP {status_code}")

            snapshots, schema, row_count = _extract(path, targets)
            schema["url"] = url
            schema["gzip_bytes"] = size

            if len(snapshots) != len(targets):
                raise ValueError(
                    f"Matched {len(snapshots)} of {len(targets)} target timestamps"
                )

            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into cint001_bookticker_snapshots(
                        run_id,futures_symbol,target_ts,quote_ts,bid_price,bid_qty,
                        ask_price,ask_qty,age_ms,timing_relation,source_date,source
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict(run_id,futures_symbol,target_ts)
                    do update set
                        quote_ts=excluded.quote_ts,
                        bid_price=excluded.bid_price,
                        bid_qty=excluded.bid_qty,
                        ask_price=excluded.ask_price,
                        ask_qty=excluded.ask_qty,
                        age_ms=excluded.age_ms,
                        timing_relation=excluded.timing_relation,
                        source_date=excluded.source_date,
                        source=excluded.source,
                        inserted_at=now()
                    """,
                    [
                        (
                            run_id,
                            symbol,
                            snap[0],
                            snap[1],
                            snap[2],
                            snap[3],
                            snap[4],
                            snap[5],
                            snap[6],
                            snap[7],
                            trade_date,
                            "tardis_reconstructed_l2_quotes_free_sample",
                        )
                        for snap in snapshots
                    ],
                )
                conn.commit()

            _finish(item["id"], "completed", row_count, checksum, schema)
            logger.info(
                "Tardis quote sample complete symbol=%s date=%s targets=%s rows_scanned=%s gzip_bytes=%s",
                symbol,
                trade_date,
                len(targets),
                row_count,
                size,
            )
    except Exception as exc:
        logger.exception("Tardis quote sample failed symbol=%s date=%s", symbol, trade_date)
        _fail(item, exc)
