"""Deterministic bootstrap for the fixed index-futures evidence window.

This module deliberately patches the legacy ingestion module before its opt-in
starter is called.  The database RPCs independently enforce the same window,
so the application and database fail closed together.
"""

from __future__ import annotations

from datetime import date, timedelta

from app import index_futures_ingest_http as _base

FIXED_WINDOW_START = date(2025, 9, 1)
FIXED_WINDOW_END_EXCLUSIVE = date(2026, 9, 1)

# Patch the authoritative HTTP/RPC implementation before importing v2, because
# v2 derives its historical point-in-time catalogue dates at import time.
_base._START = FIXED_WINDOW_START
_base._END_EXCLUSIVE = FIXED_WINDOW_END_EXCLUSIVE

from app import index_futures_ingest_http_v2 as _v2  # noqa: E402

_v2._SNAPSHOT_DATES = (
    FIXED_WINDOW_START,
    FIXED_WINDOW_START + timedelta(days=182),
    FIXED_WINDOW_END_EXCLUSIVE - timedelta(days=1),
)


def start_fixed_window_index_futures_ingestion() -> None:
    """Start one resumable, opt-in fixed-window ingestion thread."""

    _v2.start_index_futures_ingestion_if_enabled()
