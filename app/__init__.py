from __future__ import annotations

import logging
import os

__version__ = "3.4.0"

# Opt-in research backfills must be explicitly enabled on the intended service.
# `python -m app.worker` always imports this package before the worker module,
# making this a reliable bootstrap point without changing the production worker
# command. All other services remain inert because the flag is absent.
if os.getenv("CINT001_BOOKTICKER_ENABLED", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}:
    try:
        from app.cint001_bookticker import start_background

        start_background()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start opt-in C-INT-001 bookTicker background backfill"
        )
