from __future__ import annotations

import logging
import os
import signal
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, TypeVar

from app.aggregation import aggregate_equity_microstructure
from app.b001_operational_hardening import (
    claim_b001_work,
    is_transient_db_error,
    process_b001_work,
    reclaim_stale_b001_work,
)
from app.capture import advance_mining_runs, scan_capture_partition
from app.cint001_execution_v2 import (
    claim_execution_work,
    process_execution_work,
    reclaim_stale_execution_work,
)
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
from app.option_vol_research import (
    claim_option_vol_event,
    process_option_vol_event,
    reclaim_stale_option_vol_events,
)
from app.providers import PROVIDER_CLASSES
from app.quality import run_ready_quality_checks


settings = get_settings()
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
shutdown_event = threading.Event()
B001_REQUESTED_PARALLELISM = max(1, int(os.getenv("B001_PARALLELISM", "8")))
# B-001 monthly loads hold a DB connection through COPY + canonical insert + 15m
# derivation. Reserve one connection for claiming/checkpoint/reclaim work so a
# worker cannot deadlock its own control plane by running more DB-heavy tasks
# than the shared pool can support.
B001_PARALLELISM = min(
    B001_REQUESTED_PARALLELISM,
    max(1, settings.db_pool_size - 1),
)
B001_EXCLUSIVE = os.getenv("B001_EXCLUSIVE", "false").strip().lower() in {"1", "true", "yes", "on"}

T = TypeVar("T")

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


def _db_call(label: str, call: Callable[[], T], default: T) -> T:
    """Keep the long-running worker alive across transient DB connection faults."""
    try:
        return call()
    except Exception as exc:
        if not is_transient_db_error(exc):
            raise
        logger.warning("Transient database failure during %s; worker will retry: %s", label, exc)
        return default


def wait_for_schema() -> None:
    for _attempt in range(120):
        try:
            row = fetch_one(
                """
                select
                    to_regclass('public.capture_decisions') as capture_table,
                    to_regclass('public.crypto_b001_replication_runs') as b001_table,
                    to_regclass('public.cint001_execution_runs') as cint001_table,
                    to_regclass('public.cint001_spot_15m') as cint001_spot_table,
                    to_regclass('public.option_vol_research_events') as option_vol_table
                """
            )
            if (
                row
                and row["capture_table"]
                and row["b001_table"]
                and row["cint001_table"]
                and row["cint001_spot_table"]
                and row["option_vol_table"]
            ):
                return
        except Exception:
            pass
        if shutdown_event.wait(2):
            return
    raise RuntimeError("Database schema through migration 013 was not ready after four minutes")


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


def _process_b001_batch(worker_id: str) -> int:
    items: list[dict[str, Any]] = []
    for slot in range(B001_PARALLELISM):
        item = _db_call(
            "B-001 work claim",
            lambda slot=slot: claim_b001_work(f"{worker_id}:b001:{slot}"),
            None,
        )
        if not item:
            break
        items.append(item)
    if not items:
        return 0

    logger.info("Processing %s B-001 replication work items", len(items))
    with ThreadPoolExecutor(max_workers=len(items), thread_name_prefix="b001") as executor:
        futures = [(item, executor.submit(process_b001_work, item)) for item in items]
        for item, future in futures:
            try:
                future.result()
            except Exception as exc:
                if is_transient_db_error(exc):
                    logger.warning(
                        "B-001 work escaped on transient DB failure; leaving for durable reclaim "
                        "stage=%s key=%s error=%s",
                        item.get("stage"),
                        item.get("partition_key"),
                        exc,
                    )
                else:
                    logger.exception(
                        "B-001 work escaped its durable failure handler stage=%s key=%s",
                        item.get("stage"),
                        item.get("partition_key"),
                    )
    return len(items)


def _process_cint001_once(worker_id: str) -> bool:
    item = _db_call(
        "C-INT-001 execution claim",
        lambda: claim_execution_work(f"{worker_id}:cint001"),
        None,
    )
    if not item:
        return False
    try:
        process_execution_work(item)
    except Exception as exc:
        if is_transient_db_error(exc):
            logger.warning(
                "C-INT-001 work escaped on transient DB failure; durable reclaim will recover key=%s error=%s",
                item.get("partition_key"),
                exc,
            )
        else:
            logger.exception("C-INT-001 work escaped its durable failure handler key=%s", item.get("partition_key"))
    return True


def _process_option_vol_once(worker_id: str) -> bool:
    item = _db_call(
        "option-vol research claim",
        lambda: claim_option_vol_event(f"{worker_id}:option-vol"),
        None,
    )
    if not item:
        return False
    try:
        process_option_vol_event(item)
    except Exception:
        # process_option_vol_event has its own durable retry/failure handler.
        logger.exception("Option-vol work escaped its durable handler id=%s", item.get("id"))
    return True


def main() -> None:
    settings.validate_worker()
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    wait_for_schema()
    worker_id = _worker_id()
    providers = {name: cls() for name, cls in PROVIDER_CLASSES.items()}
    logger.info(
        "Collection worker started id=%s b001_parallelism=%s requested_b001_parallelism=%s "
        "db_pool_size=%s b001_exclusive=%s",
        worker_id,
        B001_PARALLELISM,
        B001_REQUESTED_PARALLELISM,
        settings.db_pool_size,
        B001_EXCLUSIVE,
    )
    last_reclaim = 0.0

    while not shutdown_event.is_set():
        did_work = False
        now_monotonic = time.monotonic()
        if now_monotonic - last_reclaim >= 60:
            recovered = _db_call("stale collection reclaim", reclaim_stale_work, {})
            if any(recovered.values()):
                logger.warning("Recovered stale collection work: %s", recovered)
            recovered_b001 = _db_call("stale B-001 reclaim", reclaim_stale_b001_work, 0)
            if recovered_b001:
                logger.warning("Recovered %s stale B-001 work items", recovered_b001)
            recovered_cint001 = _db_call("stale C-INT-001 reclaim", reclaim_stale_execution_work, 0)
            if recovered_cint001:
                logger.warning("Recovered %s stale C-INT-001 execution items", recovered_cint001)
            recovered_option_vol = _db_call("stale option-vol reclaim", reclaim_stale_option_vol_events, 0)
            if recovered_option_vol:
                logger.warning("Recovered %s stale option-vol research events", recovered_option_vol)
            last_reclaim = now_monotonic

        b001_count = _process_b001_batch(worker_id)
        if b001_count:
            did_work = True

        cint001_did_work = _process_cint001_once(worker_id)
        if cint001_did_work:
            did_work = True

        if b001_count and B001_EXCLUSIVE:
            continue

        partition = _db_call(
            "collection partition claim",
            lambda: claim_collection_partition(worker_id),
            None,
        )
        if partition:
            process_collection_partition(partition, providers)
            continue

        try:
            if advance_mining_runs():
                did_work = True
        except Exception:
            logger.exception("Failed to advance a mining stage; it will be retried")

        try:
            quality_checked = run_ready_quality_checks()
            if quality_checked:
                logger.info("Completed readiness quality checks for %s collection run(s)", quality_checked)
                did_work = True
        except Exception:
            logger.exception("Failed to run collection readiness quality checks; it will be retried")

        ready_runs = _db_call("find runs ready for planning", find_runs_ready_for_planning, [])
        for run_id in ready_runs:
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

        partition = _db_call(
            "collection partition claim",
            lambda: claim_collection_partition(worker_id),
            None,
        )
        if partition:
            process_collection_partition(partition, providers)
            continue

        # Option repricing research is deliberately lowest priority. It cannot
        # pre-empt a B-001 batch, C-INT work item, or collection partition.
        if not b001_count and not cint001_did_work:
            if _process_option_vol_once(worker_id):
                did_work = True
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
