"""Process-level opt-in hooks for isolated research backfills.

Python imports ``sitecustomize`` automatically when it is present on sys.path.
The hook is inert unless a service-specific environment flag is explicitly set.
This lets the existing Render worker run a temporary research backfill without
changing its primary worker command or creating another paid service.
"""

from __future__ import annotations

import logging
import os

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
