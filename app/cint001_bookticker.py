from __future__ import annotations

import csv
import hashlib
import io
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

from app.cint001_execution import _looks_numeric, _timestamp
from app.db import db_connection, fetch_one

logger = logging.getLogger(__name__)
DATA_BASE = "https://data.binance.vision"
_STARTED = False
_START_LOCK = threading.Lock()


def _enabled() -> bool:
    return os.getenv("CINT001_BOOKTICKER_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}


def _run_id() -> UUID:
    value = os.getenv("CINT001_BOOKTICKER_RUN_ID", "").strip()
    if not value:
        raise RuntimeError("CINT001_BOOKTICKER_RUN_ID is required when bookTicker backfill is enabled")
    return UUID(value)


def _concurrency() -> int:
    return max(1, min(8, int(os.getenv("CINT001_BOOKTICKER_CONCURRENCY", "4"))))


def start_background() -> None:
    global _STARTED
    if not _enabled():
        return
    with _START_LOCK:
        if _STARTED:
            return
        _STARTED = True
    thread = threading.Thread(target=_supervisor, name="cint001-bookticker-supervisor", daemon=True)
    thread.start()


def _wait_for_schema() -> bool:
    for _ in range(180):
        try:
            row = fetch_one("select to_regclass('public.cint001_bookticker_days') d, to_regclass('public.cint001_bookticker_snapshots') s")
            if row and row.get("d") and row.get("s"):
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
    workers = []
    for slot in range(_concurrency()):
        thread = threading.Thread(target=_worker_loop, args=(run_id, slot), name=f"cint001-bookticker-{slot}", daemon=True)
        thread.start()
        workers.append(thread)
    logger.info("C-INT-001 bookTicker backfill started run=%s concurrency=%s", run_id, len(workers))
    for thread in workers:
        thread.join()
    logger.info("C-INT-001 bookTicker backfill finished run=%s", run_id)


def _claim(run_id: UUID, worker_id: str) -> dict[str, Any] | None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select id from cint001_bookticker_days
                where run_id=%s and status in ('queued','retry_wait') and not_before<=now()
                order by priority,id
                for update skip locked limit 1
            )
            update cint001_bookticker_days d
               set status='running',attempts=attempts+1,locked_by=%s,locked_at=now(),updated_at=now()
              from candidate c where d.id=c.id
            returning d.*
            """,
            (run_id, worker_id),
        )
        row = cur.fetchone()
        conn.commit()
        return row


def _reclaim(run_id: UUID) -> int:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_bookticker_days
               set status='retry_wait',locked_by=null,locked_at=null,not_before=now(),updated_at=now(),
                   last_error=coalesce(last_error,'stale bookTicker worker lock reclaimed')
             where run_id=%s and status='running' and locked_at<now()-interval '30 minutes'
            """,
            (run_id,),
        )
        count = cur.rowcount
        conn.commit()
        return count


def _pending(run_id: UUID) -> int:
    row = fetch_one(
        "select count(*) n from cint001_bookticker_days where run_id=%s and status in ('queued','retry_wait','running')",
        (run_id,),
    ) or {}
    return int(row.get("n") or 0)


def _worker_loop(run_id: UUID, slot: int) -> None:
    worker_id = f"{socket.gethostname()}:{os.getpid()}:bookticker:{slot}"
    last_reclaim = 0.0
    while True:
        now = time.monotonic()
        if slot == 0 and now-last_reclaim>60:
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
            select distinct target_ts from (
                select entry_ts target_ts from cint001_signal_selection
                 where run_id=%s and futures_symbol=%s and entry_ts::date=%s
                union
                select exit_ts target_ts from cint001_signal_selection
                 where run_id=%s and futures_symbol=%s and exit_ts::date=%s
            ) q order by target_ts
            """,
            (run_id, futures_symbol, trade_date, run_id, futures_symbol, trade_date),
        )
        return [row["target_ts"] for row in cur.fetchall()]


def _url(symbol: str, d: date) -> str:
    return f"{DATA_BASE}/data/futures/um/daily/bookTicker/{symbol}/{symbol}-bookTicker-{d:%Y-%m-%d}.zip"


def _download(client: httpx.Client, url: str, path: Path) -> tuple[bool, str | None, int]:
    digest = hashlib.sha256()
    size = 0
    with client.stream("GET", url) as response:
        if response.status_code == 404:
            return False, None, 0
        response.raise_for_status()
        with path.open("wb") as handle:
            for chunk in response.iter_bytes(chunk_size=1024*1024):
                if chunk:
                    digest.update(chunk)
                    size += len(chunk)
                    handle.write(chunk)
    checksum_url = url+".CHECKSUM"
    checksum_response = client.get(checksum_url)
    source_checksum = checksum_response.text.strip().split()[0].lower() if checksum_response.status_code==200 and checksum_response.text.strip() else None
    computed = digest.hexdigest()
    if source_checksum and len(source_checksum)==64 and source_checksum!=computed:
        raise ValueError(f"Checksum mismatch for {url}")
    return True, computed, size


def _norm(value: str) -> str:
    return value.strip().lower().replace(" ", "_").replace("-", "_")


def _header_indices(header: list[str]) -> dict[str, int]:
    raw = {v.strip(): i for i,v in enumerate(header)}
    norm = {_norm(v): i for i,v in enumerate(header)}
    def find(*names: str) -> int | None:
        for name in names:
            if name in raw:
                return raw[name]
            key = _norm(name)
            if key in norm:
                return norm[key]
        return None
    indices = {
        "bid_price": find("best_bid_price","bid_price","bestBidPrice","b"),
        "bid_qty": find("best_bid_qty","bid_qty","bid_quantity","bestBidQty","B"),
        "ask_price": find("best_ask_price","ask_price","bestAskPrice","a"),
        "ask_qty": find("best_ask_qty","ask_qty","ask_quantity","bestAskQty","A"),
        "time": find("transaction_time","transactionTime","transact_time","T","event_time","eventTime","E","timestamp","time"),
    }
    missing = [key for key in ("bid_price","ask_price","time") if indices[key] is None]
    if missing:
        raise ValueError(f"Unsupported bookTicker header; missing {missing}; header={header}")
    return {k:int(v) if v is not None else -1 for k,v in indices.items()}


def _row_parser(first_row: list[str]) -> tuple[dict[str,int], bool, dict[str,Any]]:
    has_header = not all(_looks_numeric(value) for value in first_row)
    if has_header:
        indices = _header_indices(first_row)
        return indices, True, {"mode":"header","header":first_row}
    if len(first_row)==7:
        # Binance Data Vision's compact bookTicker export is:
        # update_id,best_bid_price,best_bid_qty,best_ask_price,best_ask_qty,transaction_time,event_time.
        # Validate every parsed row economically before accepting this inferred layout.
        indices = {"bid_price":1,"bid_qty":2,"ask_price":3,"ask_qty":4,"time":5}
        return indices, False, {"mode":"validated_inferred_7_column","columns":7}
    raise ValueError(f"Unsupported headerless bookTicker schema with {len(first_row)} columns: {first_row[:10]}")


def _parse_float(row: list[str], index: int) -> float | None:
    if index < 0 or index>=len(row) or row[index]=="":
        return None
    return float(row[index])


def _extract(path: Path, targets: list[datetime], expected_date: date) -> tuple[list[tuple],dict[str,Any],int]:
    if not targets:
        return [], {"target_count":0}, 0
    snapshots: list[tuple] = []
    row_count = 0
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith('.csv')]
        if not names:
            raise ValueError(f"No CSV in {path.name}")
        with archive.open(names[0]) as raw, io.TextIOWrapper(raw,encoding='utf-8',newline='') as text:
            reader = csv.reader(text)
            first = next(reader,None)
            if first is None:
                return [], {"target_count":len(targets),"empty":True}, 0
            indices, has_header, schema = _row_parser(first)
            stream = reader if has_header else iter([first,*reader])
            target_index = 0
            last_quote: tuple | None = None
            after_fallback = 0
            at_or_before = 0
            for row in stream:
                row_count += 1
                try:
                    ts = _timestamp(row[indices['time']])
                    bid = float(row[indices['bid_price']])
                    ask = float(row[indices['ask_price']])
                    bid_qty = _parse_float(row,indices['bid_qty'])
                    ask_qty = _parse_float(row,indices['ask_qty'])
                except (ValueError,OverflowError,IndexError):
                    continue
                if bid<=0 or ask<bid:
                    continue
                current = (ts,bid,bid_qty,ask,ask_qty)
                while target_index<len(targets) and targets[target_index] < ts:
                    target = targets[target_index]
                    if last_quote is not None:
                        qts,qbid,qbqty,qask,qaqty = last_quote
                        age_ms = max(0.0,(target-qts).total_seconds()*1000.0)
                        snapshots.append((target,qts,qbid,qbqty,qask,qaqty,age_ms,'at_or_before'))
                        at_or_before += 1
                    else:
                        age_ms = max(0.0,(ts-target).total_seconds()*1000.0)
                        snapshots.append((target,ts,bid,bid_qty,ask,ask_qty,age_ms,'after_fallback'))
                        after_fallback += 1
                    target_index += 1
                last_quote = current
                while target_index<len(targets) and targets[target_index] == ts:
                    target = targets[target_index]
                    snapshots.append((target,ts,bid,bid_qty,ask,ask_qty,0.0,'at_or_before'))
                    at_or_before += 1
                    target_index += 1
                if target_index>=len(targets):
                    break
            while target_index<len(targets) and last_quote is not None:
                target = targets[target_index]
                qts,qbid,qbqty,qask,qaqty = last_quote
                age_ms = max(0.0,(target-qts).total_seconds()*1000.0)
                snapshots.append((target,qts,qbid,qbqty,qask,qaqty,age_ms,'at_or_before'))
                at_or_before += 1
                target_index += 1
    schema.update({"target_count":len(targets),"matched":len(snapshots),"at_or_before":at_or_before,"after_fallback":after_fallback,"raw_rows_scanned":row_count,"expected_date":expected_date.isoformat()})
    return snapshots,schema,row_count


def _finish(item_id: int, status: str, rows: int, checksum: str | None, schema: dict[str,Any], error: str | None=None) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_bookticker_days set status=%s,row_count=%s,checksum=%s,schema_info=%s,last_error=%s,
                   locked_by=null,locked_at=null,updated_at=now() where id=%s
            """,
            (status,rows,checksum,Jsonb(schema),error,item_id),
        )
        conn.commit()


def _fail(item: dict[str,Any], exc: BaseException) -> None:
    attempts = int(item.get('attempts') or 1)
    max_attempts = int(item.get('max_attempts') or 5)
    status = 'failed' if attempts>=max_attempts else 'retry_wait'
    delay = min(900,15*(2**min(attempts,6)))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_bookticker_days set status=%s,last_error=%s,locked_by=null,locked_at=null,
                   not_before=now()+(%s*interval '1 second'),updated_at=now() where id=%s
            """,
            (status,f"{type(exc).__name__}: {exc}",delay,item['id']),
        )
        conn.commit()


def _process_item(item: dict[str,Any]) -> None:
    run_id = UUID(str(item['run_id']))
    symbol = str(item['futures_symbol'])
    d = item['trade_date']
    if isinstance(d,str):
        d = date.fromisoformat(d)
    targets = _targets(run_id,symbol,d)
    url = _url(symbol,d)
    try:
        with tempfile.TemporaryDirectory(prefix='cint001-bookticker-') as temp_dir, httpx.Client(timeout=180,follow_redirects=True) as client:
            path = Path(temp_dir)/f"{symbol}-{d}.zip"
            exists,checksum,size = _download(client,url,path)
            if not exists:
                _finish(item['id'],'missing',0,None,{"url":url,"target_count":len(targets)},'archive 404')
                return
            snapshots,schema,row_count = _extract(path,targets,d)
            schema['url'] = url
            schema['zip_bytes'] = size
            with db_connection() as conn, conn.cursor() as cur:
                cur.executemany(
                    """
                    insert into cint001_bookticker_snapshots(
                        run_id,futures_symbol,target_ts,quote_ts,bid_price,bid_qty,ask_price,ask_qty,age_ms,timing_relation,source_date
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    on conflict(run_id,futures_symbol,target_ts) do update set
                        quote_ts=excluded.quote_ts,bid_price=excluded.bid_price,bid_qty=excluded.bid_qty,
                        ask_price=excluded.ask_price,ask_qty=excluded.ask_qty,age_ms=excluded.age_ms,
                        timing_relation=excluded.timing_relation,source_date=excluded.source_date,inserted_at=now()
                    """,
                    [(run_id,symbol,*snapshot,d) for snapshot in snapshots],
                )
                conn.commit()
            if len(snapshots)!=len(targets):
                raise ValueError(f"Matched {len(snapshots)} of {len(targets)} target timestamps")
            _finish(item['id'],'completed',row_count,checksum,schema)
            logger.info("bookTicker complete symbol=%s date=%s targets=%s rows=%s bytes=%s",symbol,d,len(targets),row_count,size)
    except Exception as exc:
        logger.exception("bookTicker failed symbol=%s date=%s",symbol,d)
        _fail(item,exc)
