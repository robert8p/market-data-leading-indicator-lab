from __future__ import annotations

import logging
import os
import socket
import threading
from datetime import datetime, timezone
from typing import Any

from app.db import db_connection
from app.exceptions import EmptyData, ProviderError
from app.jobs import (
    checksum_rows,
    complete_partition,
    retry_or_fail_partition,
    save_bar_page,
    skip_partition,
)
from app.providers import PROVIDER_CLASSES


logger = logging.getLogger(__name__)
MIN_PRIORITY = int(os.getenv("URGENT_COLLECTION_MIN_PRIORITY", "6000000"))
POLL_SECONDS = max(2, int(os.getenv("URGENT_COLLECTION_POLL_SECONDS", "5")))
ENABLED = os.getenv("URGENT_COLLECTION_ENABLED", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _worker_id() -> str:
    return f"urgent:{socket.gethostname()}:{os.getpid()}"


def claim_urgent_partition(worker_id: str) -> dict[str, Any] | None:
    """Claim only urgent bar partitions without disturbing the normal queue.

    This lane exists so a live-signal catch-up cannot sit behind long-running
    research work. Ordinary collection still follows the main worker's normal
    scheduling rules.
    """
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with candidate as (
                select cp.id
                  from collection_partitions cp
                  join collection_runs cr on cr.id=cp.run_id
                 where cp.status in ('queued','retry_wait')
                   and (cp.not_before is null or cp.not_before<=now())
                   and cr.status in ('queued','running')
                   and cp.data_type='bars_1m'
                   and cp.priority >= %s
                 order by cp.priority desc,cp.created_at,cp.id
                 for update of cp skip locked
                 limit 1
            )
            update collection_partitions cp
               set status='running',locked_by=%s,locked_at=now(),heartbeat_at=now(),
                   attempts=attempts+1,updated_at=now()
              from candidate
             where cp.id=candidate.id
            returning cp.*
            """,
            (MIN_PRIORITY, worker_id),
        )
        row = cur.fetchone()
        conn.commit()
        return dict(row) if row else None


def process_urgent_partition(partition: dict[str, Any], providers: dict[str, Any]) -> None:
    provider_name = partition["provider"]
    provider = providers.get(provider_name)
    if provider is None:
        retry_or_fail_partition(
            partition,
            f"No provider implementation for urgent {provider_name}/bars_1m",
            "urgent_provider_missing",
            None,
            False,
        )
        return

    logger.warning(
        "Urgent collection processing symbol=%s range=%s..%s attempt=%s",
        partition.get("provider_symbol"),
        partition.get("start_ts"),
        partition.get("end_ts"),
        partition.get("attempts"),
    )
    rows_seen = int(partition.get("row_count") or 0)
    existing_cursor = dict(partition.get("cursor") or {})
    first_ts = datetime.fromisoformat(existing_cursor["first_ts"]) if existing_cursor.get("first_ts") else None
    last_ts = datetime.fromisoformat(existing_cursor["last_ts"]) if existing_cursor.get("last_ts") else None
    yielded_page = False

    try:
        for page in provider.iter_bar_pages(partition):
            yielded_page = True
            if page.rows:
                page.rows = list({row["ts"]: row for row in page.rows}.values())
                page_first = min(row["ts"] for row in page.rows)
                page_last = max(row["ts"] for row in page.rows)
                first_ts = min(first_ts, page_first) if first_ts else page_first
                last_ts = max(last_ts, page_last) if last_ts else page_last
            checkpoint = dict(page.cursor)
            if first_ts:
                checkpoint["first_ts"] = first_ts.isoformat()
            if last_ts:
                checkpoint["last_ts"] = last_ts.isoformat()
            rows_seen += save_bar_page(partition["id"], page.rows, checkpoint)
            partition["cursor"] = checkpoint
            if page.done:
                break

        if not yielded_page or rows_seen == 0:
            complete_partition(partition["id"], empty=True)
        else:
            complete_partition(
                partition["id"],
                checksum=checksum_rows(rows_seen, first_ts, last_ts),
            )
        logger.warning(
            "Urgent collection completed symbol=%s rows=%s first=%s last=%s",
            partition.get("provider_symbol"),
            rows_seen,
            first_ts,
            last_ts,
        )
    except EmptyData:
        complete_partition(partition["id"], empty=True)
        logger.warning("Urgent collection empty symbol=%s", partition.get("provider_symbol"))
    except ProviderError as exc:
        if not exc.retryable and provider_name == "twelvedata":
            skip_partition(partition["id"], str(exc), exc.code)
        elif not exc.retryable and exc.code == "http_404":
            skip_partition(partition["id"], str(exc), exc.code)
        else:
            retry_or_fail_partition(partition, str(exc), exc.code, exc.retry_at, exc.retryable)
        logger.warning(
            "Urgent provider error symbol=%s code=%s retryable=%s message=%s",
            partition.get("provider_symbol"),
            exc.code,
            exc.retryable,
            exc,
        )
    except Exception as exc:
        retry_or_fail_partition(
            partition,
            f"{type(exc).__name__}: {exc}",
            "urgent_worker_exception",
            None,
            True,
        )
        logger.exception("Urgent collection failed symbol=%s", partition.get("provider_symbol"))


def run_urgent_collection_loop(stop_event: threading.Event) -> None:
    if not ENABLED:
        return
    worker_id = _worker_id()
    providers = {name: cls() for name, cls in PROVIDER_CLASSES.items()}
    logger.warning(
        "Urgent collection lane started worker=%s min_priority=%s",
        worker_id,
        MIN_PRIORITY,
    )
    while not stop_event.is_set():
        try:
            partition = claim_urgent_partition(worker_id)
            if partition:
                process_urgent_partition(partition, providers)
                continue
        except Exception:
            logger.exception("Urgent collection iteration failed; it will retry")
        stop_event.wait(POLL_SECONDS)
    logger.warning("Urgent collection lane stopping worker=%s", worker_id)
