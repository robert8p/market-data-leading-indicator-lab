from __future__ import annotations

import asyncio
import gc
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

from kalshi_perps_app.config import get_settings
from kalshi_perps_app.features import TRANSFER_FEATURES, transfer_frame_from_rows
from kalshi_perps_app.modeling import train_probability_model
from kalshi_perps_app.supabase import SupabaseRPC

UTC = timezone.utc
LOGGER = logging.getLogger("kalshi-transfer-research")
TARGET_COLUMNS = ["target_return_1h", "target_return_2h", "target_return_4h"]
REQUIRED_COLUMNS = ["decision_ts", "symbol", "split", *TRANSFER_FEATURES, *TARGET_COLUMNS]


def _iso(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def _coverage(frame: pd.DataFrame, split: str) -> tuple[str | None, str | None]:
    rows = frame[frame["split"].eq(split)]
    if rows.empty:
        return None, None
    return _iso(rows["decision_ts"].min()), _iso(rows["decision_ts"].max())


def _compact_chunk(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in REQUIRED_COLUMNS if column in frame.columns]
    compact = frame.loc[:, columns].copy()
    for column in [*TRANSFER_FEATURES, *TARGET_COLUMNS]:
        if column not in compact:
            compact[column] = np.nan
        compact[column] = pd.to_numeric(compact[column], errors="coerce").astype("float32")
    return compact


def _cap_discovery(frame: pd.DataFrame, maximum: int) -> pd.DataFrame:
    discovery = frame[frame["split"].eq("discovery")]
    remainder = frame[~frame["split"].eq("discovery")]
    if maximum > 0 and len(discovery) > maximum:
        indices = np.linspace(0, len(discovery) - 1, maximum, dtype=int)
        discovery = discovery.iloc[indices]
    reduced = pd.concat([discovery, remainder], ignore_index=True, copy=False)
    del discovery, remainder
    reduced = reduced.sort_values(["decision_ts", "symbol"], kind="stable").reset_index(drop=True)
    reduced["symbol"] = reduced["symbol"].astype("category")
    reduced["split"] = reduced["split"].astype("category")
    return reduced


async def _load_transfer_frame(
    db: SupabaseRPC,
    page_size: int,
    max_training_rows: int,
) -> tuple[pd.DataFrame, int, dict[str, int]]:
    chunks: list[pd.DataFrame] = []
    total = 0
    split_counts: dict[str, int] = {}

    for split in ("discovery", "validation", "holdout"):
        split_total = 0
        async for page in db.paged_training_rows(split, page_size):
            chunk = transfer_frame_from_rows(page)
            if chunk.empty:
                continue
            chunk = _compact_chunk(chunk)
            chunks.append(chunk)
            split_total += len(chunk)
            total += len(chunk)
            if total % 10_000 < len(chunk):
                LOGGER.info("Loaded %d transfer rows", total)
        split_counts[split] = split_total
        LOGGER.info("Loaded %d rows for %s", split_total, split)

    if not chunks:
        raise RuntimeError("Cross-venue transfer dataset is empty")

    frame = pd.concat(chunks, ignore_index=True, copy=False)
    del chunks
    gc.collect()
    frame = frame.sort_values(["decision_ts", "symbol"], kind="stable").reset_index(drop=True)

    observed = {key: int(value) for key, value in frame.groupby("split", observed=True).size().to_dict().items()}
    LOGGER.info("Full transfer frame: %d rows, split counts=%s", len(frame), observed)
    for split in ("discovery", "validation", "holdout"):
        if int(observed.get(split, 0)) < 500:
            raise RuntimeError(
                f"Incomplete transfer dataset after pagination: {split}={int(observed.get(split, 0))}"
            )

    reduced = _cap_discovery(frame, max_training_rows)
    del frame
    gc.collect()
    LOGGER.info(
        "Research frame after discovery cap: %d rows, split counts=%s",
        len(reduced),
        {key: int(value) for key, value in reduced.groupby("split", observed=True).size().to_dict().items()},
    )
    return reduced, total, observed


async def _handle_active_runs(db: SupabaseRPC, evidence: dict[str, Any]) -> dict[str, Any] | None:
    now = pd.Timestamp.now(tz="UTC")
    active = [
        row
        for row in (evidence.get("research_runs") or [])
        if row.get("source_type") == "crossvenue_transfer" and row.get("status") == "running"
    ]
    for row in active:
        started = pd.to_datetime(row.get("started_at"), utc=True, errors="coerce")
        age_minutes = float((now - started).total_seconds() / 60) if not pd.isna(started) else 9999.0
        if age_minutes < 10:
            return {
                "status": "skipped_active_research_run",
                "research_run_id": row.get("research_run_id"),
                "age_minutes": age_minutes,
            }
        await db.call(
            "kalshi_app_finish_research_run",
            {
                "p_research_run_id": row.get("research_run_id"),
                "p_status": "failed",
                "p_summary": {"error": "Recovered stale active research run after worker termination"},
                "p_limitations": [
                    "The preceding research process terminated without closing its run record."
                ],
            },
            timeout_seconds=120,
        )
        LOGGER.warning("Closed stale research run %s after %.1f minutes", row.get("research_run_id"), age_minutes)
    return None


async def run() -> dict[str, Any]:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = get_settings()
    db = SupabaseRPC.create(settings)
    run_id: str | None = None
    limitations = [
        "Training data are Binance futures/spot observations rather than Kalshi-native history.",
        "A passed model is displayed as experimental transfer evidence, not Kalshi-native validation.",
        "A model that fails the frozen holdout promotion gate is stored for audit but is not served.",
    ]

    try:
        evidence = db.unwrap_scalar(await db.call("kalshi_app_model_evidence")) or {}
        active_result = await _handle_active_runs(db, evidence)
        if active_result is not None:
            LOGGER.info("%s", json.dumps(active_result, default=str, sort_keys=True))
            return active_result

        inventory = ((evidence.get("inventory") or {}).get("crossvenue_transfer") or {})
        expected_rows = int(inventory.get("rows") or 0)
        force = os.getenv("FORCE_RETRAIN", "false").strip().lower() in {"1", "true", "yes"}

        completed_full_runs = [
            row
            for row in (evidence.get("research_runs") or [])
            if row.get("source_type") == "crossvenue_transfer"
            and row.get("status") in {"completed", "completed_with_warnings"}
            and int(((row.get("configuration") or {}).get("source_rows") or (row.get("configuration") or {}).get("rows") or 0))
            >= expected_rows
        ]
        if completed_full_runs and not force:
            result = {
                "status": "skipped_existing_full_run",
                "expected_rows": expected_rows,
                "latest_run": completed_full_runs[0].get("research_run_id"),
            }
            LOGGER.info("%s", json.dumps(result, default=str, sort_keys=True))
            return result

        frame, source_rows, source_split_counts = await _load_transfer_frame(
            db,
            settings.research_page_size,
            settings.max_training_rows,
        )
        if expected_rows and source_rows != expected_rows:
            raise RuntimeError(
                f"Transfer row-count mismatch: loaded={source_rows:,}, inventory={expected_rows:,}"
            )

        selected_split_counts = {
            key: int(value) for key, value in frame.groupby("split", observed=True).size().to_dict().items()
        }
        run_id = str(
            db.unwrap_scalar(
                await db.call(
                    "kalshi_app_begin_research_run",
                    {
                        "p_run_name": "crossvenue-transfer-full-v3",
                        "p_source_type": "crossvenue_transfer",
                        "p_feature_set_version": "transfer-v1",
                        "p_configuration": {
                            "features": TRANSFER_FEATURES,
                            "source_rows": source_rows,
                            "source_split_counts": source_split_counts,
                            "selected_rows": len(frame),
                            "selected_split_counts": selected_split_counts,
                            "max_training_rows": settings.max_training_rows,
                            "bootstrap_iterations": settings.bootstrap_iterations,
                            "page_size": min(settings.research_page_size, 1_000),
                            "warning": "External venue history; not Kalshi-native validation.",
                            "promotion_policy": "serve only if frozen holdout gate passes",
                        },
                        "p_code_commit": settings.git_commit_sha,
                    },
                    timeout_seconds=120,
                )
            )
        )

        completed: dict[str, Any] = {}
        for horizon, target in (
            (60, "target_return_1h"),
            (120, "target_return_2h"),
            (240, "target_return_4h"),
        ):
            version = f"transfer-v3-h{horizon}-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
            LOGGER.info("Training %s", version)
            trained = await asyncio.to_thread(
                train_probability_model,
                frame,
                horizon_minutes=horizon,
                feature_names=TRANSFER_FEATURES,
                source_type="crossvenue_transfer",
                feature_set_version="transfer-v1",
                model_version=version,
                random_seed=settings.random_seed + horizon,
                bootstrap_iterations=settings.bootstrap_iterations,
                max_training_rows=settings.max_training_rows,
                asset_column="symbol",
                target_return_column=target,
            )
            passed = bool(trained.bundle.metrics.get("passed"))
            trained.model_payload["research_run_id"] = run_id
            trained.model_payload["code_commit"] = settings.git_commit_sha
            trained.model_payload["serving_enabled"] = passed
            trained.model_payload["status"] = "candidate" if passed else "rejected"
            trained.model_payload["promoted_at"] = datetime.now(UTC).isoformat() if passed else None

            store = db.unwrap_scalar(
                await db.call(
                    "kalshi_app_store_model",
                    {"p_model": trained.model_payload, "p_metrics": trained.metric_payloads},
                    timeout_seconds=240,
                )
            )
            holdout = trained.bundle.metrics["holdout"]
            completed[str(horizon)] = {
                "model_version": version,
                "stored": store,
                "passed": passed,
                "holdout": holdout,
                "abstention_threshold": trained.bundle.abstention_threshold,
                "ensemble_weights": trained.bundle.ensemble_weights,
            }
            LOGGER.info(
                "Completed %s: passed=%s brier_skill=%.6f log_loss=%.6f ece=%.6f",
                version,
                passed,
                float(holdout.get("brier_skill") or 0.0),
                float(holdout.get("log_loss") or 0.0),
                float(holdout.get("expected_calibration_error") or 0.0),
            )
            gc.collect()

        discovery_start, discovery_end = _coverage(frame, "discovery")
        validation_start, validation_end = _coverage(frame, "validation")
        holdout_start, holdout_end = _coverage(frame, "holdout")
        await db.call(
            "kalshi_app_finish_research_run",
            {
                "p_research_run_id": run_id,
                "p_status": "completed_with_warnings",
                "p_discovery_start": discovery_start,
                "p_discovery_end": discovery_end,
                "p_validation_start": validation_start,
                "p_validation_end": validation_end,
                "p_holdout_start": holdout_start,
                "p_holdout_end": holdout_end,
                "p_summary": completed,
                "p_limitations": limitations,
            },
            timeout_seconds=120,
        )
        result = {
            "status": "completed_with_warnings",
            "source_rows": source_rows,
            "selected_rows": len(frame),
            "models": completed,
        }
        LOGGER.info("Research result: %s", json.dumps(result, default=str, sort_keys=True))
        return result
    except Exception as exc:
        LOGGER.exception("Isolated transfer research failed")
        if run_id:
            try:
                await db.call(
                    "kalshi_app_finish_research_run",
                    {
                        "p_research_run_id": run_id,
                        "p_status": "failed",
                        "p_summary": {"error": str(exc)},
                        "p_limitations": limitations,
                    },
                    timeout_seconds=120,
                )
            except Exception:
                LOGGER.exception("Unable to mark failed research run")
        raise
    finally:
        await db.close()


if __name__ == "__main__":
    print(json.dumps(asyncio.run(run()), default=str, sort_keys=True))
