from __future__ import annotations

import logging
import os
import socket
import threading
import time

from app.worker import main as collection_worker_main
from app.xal006_live import XAL006LiveMonitor


logger = logging.getLogger(__name__)


def _run_xal006_monitor() -> None:
    worker_id = f"xal006:{socket.gethostname()}:{os.getpid()}"
    monitor = XAL006LiveMonitor(worker_id)
    logger.info("Starting XAL-006 evidence monitor worker_id=%s enabled=%s", worker_id, monitor.enabled)
    while True:
        try:
            monitor.tick()
        except Exception:
            logger.exception("XAL-006 monitor escaped tick protection; retrying")
        time.sleep(1)


def main() -> None:
    monitor_thread = threading.Thread(
        target=_run_xal006_monitor,
        name="xal006-live-monitor",
        daemon=True,
    )
    monitor_thread.start()
    collection_worker_main()


if __name__ == "__main__":
    main()
