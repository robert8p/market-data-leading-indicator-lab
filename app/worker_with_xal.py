from __future__ import annotations

import logging
import os
import socket
import threading
import time

# Compatibility entry point for the canonical Render worker. XAL-006 remains
# owned by app.worker. The equity-reference backfill is deliberately isolated in
# a daemon lane: it shares the worker's authorised provider credentials and DB
# pool, but it does not alter collection-partition semantics or candidate work.
from app.equity_reference_backfill import process_equity_reference_backfill_once
from app.worker import main

logger = logging.getLogger(__name__)


def _equity_reference_lane() -> None:
    worker_id = f"equity-reference:{socket.gethostname()}:{os.getpid()}"
    while True:
        try:
            did_work = process_equity_reference_backfill_once(worker_id)
            if not did_work:
                time.sleep(5)
        except Exception:
            logger.exception("Governed equity-reference backfill lane escaped one iteration; retrying")
            time.sleep(10)


if __name__ == "__main__":
    threading.Thread(
        target=_equity_reference_lane,
        name="equity-reference-backfill",
        daemon=True,
    ).start()
    main()
