from __future__ import annotations

import logging
from uuid import UUID
from typing import Any

import app.b001_methodology_hardening  # noqa: F401  # applies all frozen-runtime patches
import app.b001_chronology_hardening  # noqa: F401  # enforces post-signal execution delay and revised gates
import app.b001_replication as replication
from app.b001_analysis import run_full_analysis

logger = logging.getLogger(__name__)

claim_b001_work = replication.claim_b001_work
reclaim_stale_b001_work = replication.reclaim_stale_b001_work
advance_b001_run = replication.advance_b001_run


def process_b001_work(item: dict[str, Any]) -> None:
    """Process one durable item without redundantly advancing the run.

    The shared worker calls advance_b001_run once after the whole claimed batch
    completes. This preserves identical stage semantics while avoiding N
    concurrent progress scans for an N-file archive batch.
    """
    try:
        stage = item["stage"]
        if stage == "discover_archives":
            replication._discover_archives(item)
        elif stage == "discover_symbol":
            replication._discover_symbol(item)
        elif stage == "spot_month":
            replication._process_spot_month(item)
        elif stage == "derive_features":
            replication._derive_features(item)
        elif stage == "market_state":
            replication._materialize_market_state(item)
        elif stage == "signals":
            replication._generate_signals(item)
        elif stage == "shortability":
            replication._check_shortability(item)
        elif stage == "analysis":
            run_full_analysis(UUID(str(item["run_id"])))
            replication._complete(item["id"], 1)
        else:
            raise ValueError(f"Unknown B-001 work stage {stage}")
    except Exception as exc:
        logger.exception(
            "B-001 work failed stage=%s key=%s",
            item.get("stage"),
            item.get("partition_key"),
        )
        replication._fail(item, exc)
