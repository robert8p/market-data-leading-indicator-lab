from __future__ import annotations

import hmac
from pathlib import PurePosixPath
from urllib.parse import quote
from uuid import UUID

import httpx
from fastapi import Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from app import __version__
from app.b001_contract import (
    CLOSE_VS_VWAP_MAX,
    DISPERSION_MAX,
    EXACT_THRESHOLDS,
    EXECUTION_SPEC,
    FINAL_5M_MAX,
    HIGH_TO_CLOSE_MIN,
    PRIMARY_COMBINED_COST_BP,
    RULE_VERSION,
)
from app.b001_replication import create_b001_run
from app.db import db_connection, fetch_all, fetch_one
from app.main import app, render_template, require_user, settings


def _run_context(run_id: UUID | None = None) -> dict:
    runs = fetch_all("select * from crypto_b001_replication_runs order by created_at desc limit 20")
    run = None
    if run_id is not None:
        run = fetch_one("select * from crypto_b001_replication_runs where id=%s", (run_id,))
        if not run:
            raise HTTPException(status_code=404, detail="B-001 replication run not found")
    elif runs:
        run = runs[0]

    work = []
    qa = []
    metrics = []
    placebos = []
    robustness = []
    export = None
    primary = None
    blocks: list[dict] = []
    comparisons: list[dict] = []
    executable = None
    scorecard: dict = {}

    if run:
        work = fetch_all(
            """
            select stage,status,count(*) as items,coalesce(sum(row_count),0) as rows
            from crypto_b001_replication_work_items where run_id=%s
            group by stage,status order by stage,status
            """,
            (run["id"],),
        )
        qa = fetch_all(
            "select * from crypto_b001_replication_qa where run_id=%s order by check_number",
            (run["id"],),
        )
        metrics = fetch_all(
            """
            select * from crypto_b001_replication_metrics where run_id=%s
            order by structure,position_mode,execution_subset,cost_bp,block
            """,
            (run["id"],),
        )
        placebos = fetch_all(
            "select * from crypto_b001_replication_placebos where run_id=%s order by placebo_type,variant",
            (run["id"],),
        )
        robustness = fetch_all(
            "select * from crypto_b001_replication_robustness where run_id=%s order by robustness_type,variant",
            (run["id"],),
        )
        export = fetch_one(
            "select * from crypto_b001_replication_exports where run_id=%s order by created_at desc limit 1",
            (run["id"],),
        )
        primary = next(
            (
                row for row in metrics
                if row["structure"] == "B-001a"
                and row["position_mode"] == "portfolio"
                and row["execution_subset"] == "research"
                and float(row["cost_bp"]) == PRIMARY_COMBINED_COST_BP
                and row["block"] == "aggregate"
            ),
            None,
        )
        for block in ("1", "2", "3"):
            row = next(
                (
                    metric for metric in metrics
                    if metric["structure"] == "B-001a"
                    and metric["position_mode"] == "portfolio"
                    and metric["execution_subset"] == "research"
                    and float(metric["cost_bp"]) == PRIMARY_COMBINED_COST_BP
                    and metric["block"] == block
                ),
                None,
            )
            if row:
                blocks.append(row)
        for structure in ("B-001a", "B-001b", "B-001c"):
            row = next(
                (
                    metric for metric in metrics
                    if metric["structure"] == structure
                    and metric["position_mode"] == "portfolio"
                    and metric["execution_subset"] == "research"
                    and metric["block"] == "aggregate"
                ),
                None,
            )
            if row:
                comparisons.append(row)
        executable = next(
            (
                row for row in metrics
                if row["structure"] == "B-001a"
                and row["position_mode"] == "portfolio"
                and row["execution_subset"] == "historically_executable"
                and float(row["cost_bp"]) == PRIMARY_COMBINED_COST_BP
                and row["block"] == "aggregate"
            ),
            None,
        )
        execution = run.get("execution_spec") or {}
        scorecard = execution.get("hard_rule_scorecard") or {}

    return {
        "runs": runs,
        "run": run,
        "work": work,
        "qa": qa,
        "metrics": metrics,
        "primary": primary,
        "blocks": blocks,
        "comparisons": comparisons,
        "executable": executable,
        "placebo_count": len(placebos),
        "robustness_count": len(robustness),
        "export": export,
        "scorecard": scorecard,
        "rule_version": RULE_VERSION,
        "thresholds": EXACT_THRESHOLDS,
        "execution": EXECUTION_SPEC,
        "dispersion_max": DISPERSION_MAX,
        "final_5m_max": FINAL_5M_MAX,
        "high_to_close_min": HIGH_TO_CLOSE_MIN,
        "close_vs_vwap_max": CLOSE_VS_VWAP_MAX,
    }


@app.get("/b001", response_class=HTMLResponse)
def b001_page(request: Request, _user: str = Depends(require_user)):
    return render_template(request, "b001_replication.html", _run_context())


@app.get("/b001/runs/{run_id}", response_class=HTMLResponse)
def b001_run_page(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    return render_template(request, "b001_replication.html", _run_context(run_id))


@app.post("/b001/runs")
def start_b001_run(
    request: Request,
    target_months: int = Form(default=24),
    _user: str = Depends(require_user),
):
    if target_months not in {12, 24}:
        raise HTTPException(status_code=400, detail="Locked replication UI only permits the predeclared 12- or 24-month windows")
    run_id = create_b001_run(target_months=target_months)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "update crypto_b001_replication_runs set code_version=%s where id=%s",
            (__version__, run_id),
        )
        conn.commit()
    return RedirectResponse(f"/b001/runs/{run_id}", status_code=303)


@app.post("/b001/runs/{run_id}/pause")
def pause_b001_run(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "update crypto_b001_replication_runs set status='paused',updated_at=now() where id=%s and status in ('queued','running')",
            (run_id,),
        )
        conn.commit()
    return RedirectResponse(f"/b001/runs/{run_id}", status_code=303)


@app.post("/b001/runs/{run_id}/resume")
def resume_b001_run(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "update crypto_b001_replication_runs set status='running',error=null,completed_at=null,updated_at=now() where id=%s and status in ('paused','completed_with_errors')",
            (run_id,),
        )
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set status='retry_wait',not_before=now(),locked_by=null,locked_at=null,updated_at=now()
             where run_id=%s and status='failed' and attempts<max_attempts
            """,
            (run_id,),
        )
        conn.commit()
    return RedirectResponse(f"/b001/runs/{run_id}", status_code=303)


@app.post("/b001/runs/{run_id}/cancel")
def cancel_b001_run(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "update crypto_b001_replication_runs set status='cancelled',completed_at=now(),updated_at=now() where id=%s",
            (run_id,),
        )
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set status='cancelled',locked_by=null,locked_at=null,updated_at=now()
             where run_id=%s and status in ('queued','retry_wait','running')
            """,
            (run_id,),
        )
        conn.commit()
    return RedirectResponse(f"/b001/runs/{run_id}", status_code=303)


@app.get("/b001/runs/{run_id}/export")
def download_b001_export(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    row = fetch_one(
        "select * from crypto_b001_replication_exports where run_id=%s and export_type='full_zip' order by created_at desc limit 1",
        (run_id,),
    )
    if not row:
        raise HTTPException(status_code=404, detail="Replication export is not available yet")
    object_path = str(row["storage_object_path"])
    encoded = quote(object_path, safe="/")
    headers = {
        "Authorization": f"Bearer {settings.supabase_service_role_key}",
        "apikey": settings.supabase_service_role_key,
    }
    response = httpx.get(
        f"{settings.supabase_url.rstrip('/')}/storage/v1/object/{settings.raw_bucket}/{encoded}",
        headers=headers,
        timeout=120,
        follow_redirects=True,
    )
    response.raise_for_status()
    filename = PurePosixPath(object_path).name
    return Response(
        content=response.content,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
