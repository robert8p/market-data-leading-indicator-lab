from __future__ import annotations

import logging
import threading
import time

from app import worker as collection_worker
from app.cint001_execution import (
    claim_execution_work,
    process_execution_work,
    reclaim_stale_execution_work,
)
from app.db import fetch_one

logger = logging.getLogger(__name__)


def _wait_for_cint_schema() -> bool:
    while not collection_worker.shutdown_event.is_set():
        try:
            row = fetch_one("select to_regclass('public.cint001_execution_runs') as table_name")
            if row and row.get("table_name"):
                return True
        except Exception:
            pass
        collection_worker.shutdown_event.wait(2)
    return False


def _cint_loop() -> None:
    if not _wait_for_cint_schema():
        return
    worker_id = f"{collection_worker._worker_id()}:cint001"
    last_reclaim = 0.0
    logger.info("C-INT-001 execution worker started id=%s", worker_id)
    while not collection_worker.shutdown_event.is_set():
        try:
            now = time.monotonic()
            if now - last_reclaim >= 60:
                reclaimed = reclaim_stale_execution_work()
                if reclaimed:
                    logger.warning("Reclaimed %s stale C-INT-001 execution items", reclaimed)
                last_reclaim = now
            item = claim_execution_work(worker_id)
            if item:
                process_execution_work(item)
                continue
        except Exception as exc:
            if collection_worker.is_transient_db_error(exc):
                logger.warning("Transient C-INT-001 database error; retrying: %s", exc)
            else:
                logger.exception("C-INT-001 execution loop error; retrying")
        collection_worker.shutdown_event.wait(2)
    logger.info("C-INT-001 execution worker stopping id=%s", worker_id)


def main() -> None:
    thread = threading.Thread(target=_cint_loop, name="cint001-execution", daemon=True)
    thread.start()
    collection_worker.main()
    thread.join(timeout=10)


if __name__ == "__main__":
    main()
