from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
from datetime import datetime
from typing import Any

from app.aggregation import aggregate_equity_microstructure
from app.capture import advance_mining_runs, scan_capture_partition
from app.config import get_settings
from app.db import fetch_one, get_pool
from app.enrichment import process_enrichment_partition
from app.exceptions import CancelRequested, EmptyData, PauseRequested, ProviderError
from app.jobs import (
    assert_collection_active,
    cancel_running_partition,
    checksum_rows,
    claim_collection_partition,
    complete_partition,
    find_runs_ready_for_planning,
    plan_data_partitions,
    reclaim_stale_work,
    release_partition_for_pause,
    retry_or_fail_partition,
    save_bar_page,
    save_quote_page,
    save_trade_page,
    skip_partition,
    upsert_instruments,
)
from app.providers import PROVIDER_CLASSES


settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
shutdown_event = threading.Event()

ENRICHMENT_TYPES = {
    "massive_context",
    "sec_filings",
    "finra_short_volume",
    "news",
    "crypto_catalogues",
    "coingecko_supply",
    "crypto_derivatives",
}


def _worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _handle_signal(signum, _frame) -> None:
    logger.warning("Received signal %s; finishing the current atomic step", signum)
    shutdown_event.set()


def wait_for_schema() -> None:
    for _attempt in range(120):
        try:
            row = fetch_one("select to_regclass('public.capture_decisions') as table_name")
            if row and row["table_name"]:
                return
        except Exception:
            pass
        if shutdown_event.wait(2):
            return
    raise RuntimeError("Database schema through migration 005 was not ready after four minutes")


def process_collection_partition(partition: dict[str, Any], providers: dict[str, Any]) -> None:
    provider_name = partition["provider"]
    provider = providers.get(provider_name)
    logger.info(
        "Processing partition id=%s run=%s provider=%s type=%s symbol=%s range=%s..%s attempt=%s",
        partition["id"], partition["run_id"], provider_name, partition["data_type"],
        partition.get("provider_symbol"), partition.get("start_ts"), partition.get("end_ts"),
        partition.get("attempts"),
    )
    try:
        assert_collection_active(partition["run_id"])

        if partition["data_type"] == "capture_scan":
            count = scan_capture_partition(partition)
            complete_partition(partition["id"], empty=count == 0)
            return

        if partition["data_type"] in ENRICHMENT_TYPES:
            count = process_enrichment_partition(partition)
            complete_partition(partition["id"], empty=count == 0)
            return

        if partition["data_type"] == "equity_microstructure_aggregate":
            count = aggregate_equity_microstructure(partition)
            complete_partition(partition["id"], empty=count == 0)
            return

        if partition["data_type"] == "catalogue":
            if provider is None:
                raise ValueError(f"No catalogue provider implementation for {provider_name}")
            items = provider.catalogue()
            assert_collection_active(partition["run_id"])
            if shutdown_event.is_set():
                raise PauseRequested()
            upsert_instruments(items, replace_provider=provider_name)
            complete_partition(partition["id"], empty=not bool(items))
            plan_data_partitions(partition["run_id"])
            logger.info("Catalogue completed provider=%s instruments=%s", provider_name, len(items))
            return

        if provider is None:
            raise ValueError(f"No provider implementation for {provider_name}/{partition['data_type']}")

        rows_seen = int(partition.get("row_count") or 0)
        existing_cursor = dict(partition.get("cursor") or {})
        first_ts = datetime.fromisoformat(existing_cursor["first_ts"]) if existing_cursor.get("first_ts") else None
        last_ts = datetime.fromisoformat(existing_cursor["last_ts"]) if existing_cursor.get("last_ts") else None
        yielded_page = False

        if partition["data_type"] == "trades":
            page_iterator = provider.iter_trade_pages(partition)
            save_page = save_trade_page
        elif partition["data_type"] == "quotes":
            page_iterator = provider.iter_quote_pages(partition)
            save_page = save_quote_page
        elif partition["data_type"] == "bars_1m":
            page_iterator = provider.iter_bar_pages(partition)
            save_page = save_bar_page
        else:
            raise ValueError(f"Unsupported provider partition type: {partition['data_type']}")

        for page in page_iterator:
            yielded_page = True
            assert_collection_active(partition["run_id"])
            if shutdown_event.is_set():
                raise PauseRequested()
            if page.rows:
                if partition["data_type"] in {"trades", "quotes"}:
                    page.rows = list({row["message_key"]: row for row in page.rows}.values())
                else:
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
            rows_seen += save_page(partition["id"], page.rows, checkpoint)
            partition["cursor"] = checkpoint
            if page.done:
                break

        if not yielded_page or rows_seen == 0:
            complete_partition(partition["id"], empty=True)
        else:
            complete_partition(partition["id"], checksum=checksum_rows(rows_seen, first_ts, last_ts))

    except EmptyData as exc:
        complete_partition(partition["id"], empty=True)
        logger.debug("Empty partition %s: %s", partition["id"], exc)
    except PauseRequested:
        release_partition_for_pause(partition["id"])
        logger.info("Released partition %s because the run is paused or worker is stopping", partition["id"])
    except CancelRequested:
        cancel_running_partition(partition["id"])
        logger.info("Cancelled partition %s", partition["id"])
    except ProviderError as exc:
        if not exc.retryable and provider_name == "twelvedata":
            skip_partition(partition["id"], str(exc), exc.code)
        elif not exc.retryable and exc.code == "http_404":
            skip_partition(partition["id"], str(exc), exc.code)
        else:
            retry_or_fail_partition(partition, str(exc), exc.code, exc.retry_at, exc.retryable)
        logger.warning(
            "Provider error partition=%s code=%s retryable=%s message=%s",
            partition["id"], exc.code, exc.retryable, exc,
        )
    except Exception as exc:
        retry_or_fail_partition(
            partition, f"{type(exc).__name__}: {exc}", "worker_exception", None, True
        )
        logger.exception("Unexpected partition failure id=%s", partition["id"])


def main() -> None:
    settings.validate_worker()
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    wait_for_schema()
    worker_id = _worker_id()
    providers = {name: cls() for name, cls in PROVIDER_CLASSES.items()}
    logger.info("Collection worker started id=%s", worker_id)
    last_reclaim = 0.0

    while not shutdown_event.is_set():
        did_work = False
        now_monotonic = time.monotonic()
        if now_monotonic - last_reclaim >= 60:
            recovered = reclaim_stale_work()
            if any(recovered.values()):
                logger.warning("Recovered stale work: %s", recovered)
            last_reclaim = now_monotonic

        # Existing queued collection work is the critical path. Claim it before
        # advancing mining/capture stages so a slow or timing-out mining planner
        # cannot add minutes of latency between historical partitions.
        partition = claim_collection_partition(worker_id)
        if partition:
            process_collection_partition(partition, providers)
            continue

        try:
            if advance_mining_runs():
                did_work = True
        except Exception:
            logger.exception("Failed to advance a mining stage; it will be retried")

        for run_id in find_runs_ready_for_planning():
            if shutdown_event.is_set():
                break
            try:
                inserted = plan_data_partitions(run_id)
                if inserted:
                    logger.info("Planned %s bar partitions for run %s", inserted, run_id)
                    did_work = True
            except Exception:
                logger.exception("Failed to plan collection run %s; it will be retried", run_id)

        if shutdown_event.is_set():
            break

        # Planning or mining advancement may have created new collection work.
        # Give it one immediate claim before sleeping.
        partition = claim_collection_partition(worker_id)
        if partition:
            process_collection_partition(partition, providers)
            continue

        if not did_work:
            shutdown_event.wait(settings.worker_poll_seconds)

    logger.info("Collection worker stopping id=%s", worker_id)
    try:
        get_pool().close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
