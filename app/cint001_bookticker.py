from __future__ import annotations

import csv
import hashlib
import io
import itertools
import logging
import os
import socket
import tempfile
import threading
import time
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from psycopg.types.json import Jsonb

from app.db import db_connection, fetch_one

logger = logging.getLogger(__name__)
DATA_BASE = "https://data.binance.vision"
_STARTED = False
_START_LOCK = threading.Lock()


def _enabled() -> bool:
    return os.getenv("CINT001_BOOKTICKER_ENABLED", "").strip().lower() in {
        "1", "true", "yes", "on"
    }


def _run_id() -> UUID:
    value = os.getenv("CINT001_BOOKTICKER_RUN_ID", "").strip()
    if not value:
        raise RuntimeError(
            "CINT001_BOOKTICKER_RUN_ID is required when bookTicker backfill is enabled"
        )
    return UUID(value)


def _concurrency() -> int:
    return max(1, min(8, int(os.getenv("CINT001_BOOKTICKER_CONCURRENCY", "2"))))


def _max_priority() -> int:
    return int(os.getenv("CINT001_BOOKTICKER_MAX_PRIORITY", "0"))


def start_background() -> None:
    global _STARTED
    if not _enabled():
        return
    with _START_LOCK:
        if _STARTED:
            return
        _STARTED = True
    thread = threading.Thread(
        target=_supervisor,
        name="cint001-bookticker-supervisor",
        daemon=True,
    )
    thread.start()


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
        logger.exception("C-INT-001 bookTicker supervisor disabled: invalid run id")
        return
    if not _wait_for_schema():
        logger.error("C-INT-001 bookTicker schema was not ready")
        return

    workers: list[threading.Thread] = []
    for slot in range(_concurrency()):
        thread = threading.Thread(
            target=_worker_loop,
            args=(run_id, slot),
            name=f"cint001-bookticker-{slot}",
            daemon=True,
        )
        thread.start()
        workers.append(thread)

    logger.info(
        "C-INT-001 bookTicker backfill started run=%s concurrency=%s max_priority=%s",
        run_id,
        len(workers),
        _max_priority(),
    )
    for thread in workers:
        thread.join()
    logger.info(
        "C-INT-001 bookTicker backfill drained eligible queue run=%s max_priority=%s",
        run_id,
        _max_priority(),
    )


def _claim(run_id: UUID, worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select id
                from cint001_bookticker_days
                where run_id=%s
                  and status in ('queued','retry_wait')
                  and not_before<=now()
                  and priority<=%s
                order by priority,id
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
            (run_id, _max_priority(), worker_id),
        )
        row = cur.fetchone()
        conn.commit()
        return row


def _reclaim(run_id: UUID) -> int:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_bookticker_days
               set status='retry_wait',
                   locked_by=null,
                   locked_at=null,
                   not_before=now(),
                   updated_at=now(),
                   last_error=coalesce(last_error,'stale bookTicker worker lock reclaimed')
             where run_id=%s
               and status='running'
               and priority<=%s
               and locked_at<now()-interval '30 minutes'
            """,
            (run_id, _max_priority()),
        )
        count = cur.rowcount
        conn.commit()
        return count


def _pending(run_id: UUID) -> int:
    row = fetch_one(
        """
        select count(*) n
        from cint001_bookticker_days
        where run_id=%s
          and priority<=%s
          and status in ('queued','retry_wait','running')
        """,
        (run_id, _max_priority()),
    ) or {}
    return int(row.get("n") or 0)


def _worker_loop(run_id: UUID, slot: int) -> None:
    worker_id = f"{socket.gethostname()}:{os.getpid()}:bookticker:{slot}"
    last_reclaim = 0.0
    while True:
        now = time.monotonic()
        if slot == 0 and now - last_reclaim > 60:
            reclaimed = _reclaim(run_id)
            if reclaimed:
                logger.warning("Reclaimed %s stale bookTicker days", reclaimed)
            last_reclaim = now

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
        f"{DATA_BASE}/data/futures/um/daily/bookTicker/{symbol}/"
        f"{symbol}-bookTicker-{trade_date:%Y-%m-%d}.zip"
    )


def _download(
    client: httpx.Client,
    url: str,
    path: Path,
) -> tuple[bool, str | None, int]:
    digest = hashlib.sha256()
    size = 0
    with client.stream("GET", url) as response:
        if response.status_code == 404:
            return False, None, 0
        response.raise_for_status()
        with path.open("wb") as handle:
            for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                digest.update(chunk)
                size += len(chunk)
                handle.write(chunk)

    checksum_response = client.get(url + ".CHECKSUM")
    source_checksum = (
        checksum_response.text.strip().split()[0].lower()
        if checksum_response.status_code == 200 and checksum_response.text.strip()
        else None
    )
    computed = digest.hexdigest()
    if source_checksum and len(source_checksum) == 64 and source_checksum != computed:
        raise ValueError(f"Checksum mismatch for {url}")
    return True, computed, size


def _normalise_header(value: str) -> str:
    return value.strip().lower().replace(" ", "_").replace("-", "_")


def _header_indices(header: list[str]) -> dict[str, int]:
    raw = {value.strip(): i for i, value in enumerate(header)}
    normalised = {_normalise_header(value): i for i, value in enumerate(header)}

    def find(*names: str) -> int | None:
        for name in names:
            if name in raw:
                return raw[name]
            key = _normalise_header(name)
            if key in normalised:
                return normalised[key]
        return None

    indices = {
        "bid_price": find("best_bid_price", "bid_price", "bestBidPrice", "b"),
        "bid_qty": find("best_bid_qty", "bid_qty", "bid_quantity", "bestBidQty", "B"),
        "ask_price": find("best_ask_price", "ask_price", "bestAskPrice", "a"),
        "ask_qty": find("best_ask_qty", "ask_qty", "ask_quantity", "bestAskQty", "A"),
        "time": find(
            "transaction_time",
            "transactionTime",
            "transact_time",
            "T",
            "event_time",
            "eventTime",
            "E",
            "timestamp",
            "time",
        ),
    }
    missing = [key for key in ("bid_price", "ask_price", "time") if indices[key] is None]
    if missing:
        raise ValueError(
            f"Unsupported bookTicker header; missing {missing}; header={header}"
        )
    return {key: int(value) if value is not None else -1 for key, value in indices.items()}


def _row_parser(first_row: list[str]) -> tuple[dict[str, int], bool, dict[str, Any]]:
    def numeric(value: str) -> bool:
        try:
            float(value)
            return True
        except (TypeError, ValueError):
            return False

    has_header = not all(numeric(value) for value in first_row)
    if has_header:
        return (
            _header_indices(first_row),
            True,
            {"mode": "header", "header": first_row},
        )

    if len(first_row) == 7:
        return (
            {
                "bid_price": 1,
                "bid_qty": 2,
                "ask_price": 3,
                "ask_qty": 4,
                "time": 5,
            },
            False,
            {"mode": "validated_inferred_7_column", "columns": 7},
        )

    raise ValueError(
        f"Unsupported headerless bookTicker schema with {len(first_row)} columns: "
        f"{first_row[:10]}"
    )


def _parse_timestamp(value: str) -> datetime:
    raw = int(float(value))
    magnitude = abs(raw)
    if magnitude >= 100_000_000_000_000_000:
        divisor = 1_000_000_000
    elif magnitude >= 100_000_000_000_000:
        divisor = 1_000_000
    elif magnitude >= 100_000_000_000:
        divisor = 1_000
    else:
        divisor = 1
    return datetime.fromtimestamp(raw / divisor, tz=timezone.utc)


def _parse_float(row: list[str], index: int) -> float | None:
    if index < 0 or index >= len(row) or row[index] == "":
        return None
    return float(row[index])


def _extract(
    path: Path,
    targets: list[datetime],
    expected_date: date,
) -> tuple[list[tuple], dict[str, Any], int]:
    """Use the first valid top-of-book update at or after each target."""
    if not targets:
        return [], {"target_count": 0}, 0

    snapshots: list[tuple] = []
    row_count = 0
    target_index = 0

    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not names:
            raise ValueError(f"No CSV in {path.name}")

        with archive.open(names[0]) as raw, io.TextIOWrapper(
            raw, encoding="utf-8", newline=""
        ) as text:
            reader = csv.reader(text)
            first = next(reader, None)
            if first is None:
                return [], {"target_count": len(targets), "empty": True}, 0

            indices, has_header, schema = _row_parser(first)
            rows = reader if has_header else itertools.chain([first], reader)

            for row in rows:
                row_count += 1
                try:
                    quote_ts = _parse_timestamp(row[indices["time"]])
                    bid = float(row[indices["bid_price"]])
                    ask = float(row[indices["ask_price"]])
                    bid_qty = _parse_float(row, indices["bid_qty"])
                    ask_qty = _parse_float(row, indices["ask_qty"])
                except (ValueError, OverflowError, IndexError, OSError):
                    continue

                if bid <= 0 or ask < bid:
                    continue

                while target_index < len(targets) and quote_ts >= targets[target_index]:
                    target = targets[target_index]
                    age_ms = max(0.0, (quote_ts - target).total_seconds() * 1000.0)
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
                        )
                    )
                    target_index += 1

                if target_index >= len(targets):
                    break

    schema.update(
        {
            "target_count": len(targets),
            "matched": len(snapshots),
            "raw_rows_scanned": row_count,
            "expected_date": expected_date.isoformat(),
            "timing_policy": "first_valid_quote_at_or_after_target",
        }
    )
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
        _finish(
            item["id"],
            "completed",
            0,
            None,
            {"target_count": 0, "reason": "no_targets"},
        )
        return

    url = _url(symbol, trade_date)
    try:
        with tempfile.TemporaryDirectory(prefix="cint001-bookticker-") as temp_dir:
            path = Path(temp_dir) / f"{symbol}-{trade_date}.zip"
            with httpx.Client(timeout=300, follow_redirects=True) as client:
                exists, checksum, size = _download(client, url, path)

            if not exists:
                _finish(
                    item["id"],
                    "missing",
                    0,
                    None,
                    {"url": url, "target_count": len(targets)},
                    "archive 404",
                )
                return

            snapshots, schema, row_count = _extract(path, targets, trade_date)
            schema["url"] = url
            schema["zip_bytes"] = size

            if len(snapshots) != len(targets):
                raise ValueError(
                    f"Matched {len(snapshots)} of {len(targets)} target timestamps"
                )

            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into cint001_bookticker_snapshots(
                        run_id,
                        futures_symbol,
                        target_ts,
                        quote_ts,
                        bid_price,
                        bid_qty,
                        ask_price,
                        ask_qty,
                        age_ms,
                        timing_relation,
                        source_date
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                        inserted_at=now()
                    """,
                    [(run_id, symbol, *snapshot, trade_date) for snapshot in snapshots],
                )
                conn.commit()

            _finish(item["id"], "completed", row_count, checksum, schema)
            logger.info(
                "bookTicker complete symbol=%s date=%s targets=%s rows_scanned=%s zip_bytes=%s",
                symbol,
                trade_date,
                len(targets),
                row_count,
                size,
            )
    except Exception as exc:
        logger.exception("bookTicker failed symbol=%s date=%s", symbol, trade_date)
        _fail(item, exc)


if __name__ == "__main__":
    if not _enabled():
        raise SystemExit("CINT001_BOOKTICKER_ENABLED is not true")
    _supervisor()
