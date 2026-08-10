from __future__ import annotations

import logging
import os

__version__ = "3.4.0"

_TRUTHY = {"1", "true", "yes", "on"}

# Opt-in research backfills must be explicitly enabled on the intended service.
# `python -m app.worker` always imports this package before the worker module,
# making this a reliable bootstrap point without changing the production worker
# command. All other services remain inert because the flags are absent.
if os.getenv("CINT001_BOOKTICKER_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_bookticker import start_background as start_bookticker_background

        start_bookticker_background()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start opt-in C-INT-001 Binance bookTicker backfill"
        )

if os.getenv("CINT001_TARDIS_QUOTES_ENABLED", "").strip().lower() in _TRUTHY:
    try:
        from app.cint001_tardis_quotes import start_background as start_tardis_quotes_background

        start_tardis_quotes_background()
    except Exception:
        logging.getLogger(__name__).exception(
            "Failed to start opt-in C-INT-001 Tardis quote sample backfill"
        )
