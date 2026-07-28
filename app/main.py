from __future__ import annotations

import hmac
import logging
from datetime import datetime, timedelta, timezone
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from psycopg.types.json import Jsonb
from starlette.middleware.sessions import SessionMiddleware

from app.capture import request_enhancement
from app.config import get_settings
from app.db import db_connection, fetch_all, fetch_one
from app.jobs import ALL_PROVIDERS, create_collection_run


settings = get_settings()
settings.validate_web()
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Market Data Leading Indicator Miner", version="3.0.1")
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.session_secret,
    same_site="lax",
    https_only=True,
    max_age=60 * 60 * 12,
)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


def format_bytes(value: int | None) -> str:
    if not value:
        return "0 B"
    amount = float(value)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if amount < 1024 or unit == "TB":
            return f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{amount:.1f} TB"


templates.env.filters["format_bytes"] = format_bytes


def render_template(request: Request, name: str, context: dict | None = None, *, status_code: int = 200):
    return templates.TemplateResponse(
        request=request,
        name=name,
        context=dict(context or {}),
        status_code=status_code,
    )


def require_user(request: Request) -> str:
    if request.session.get("authenticated") is not True:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return str(request.session.get("username") or settings.app_username)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if exc.status_code == 303 and exc.headers and exc.headers.get("Location"):
        return RedirectResponse(exc.headers["Location"], status_code=303)
    return render_template(request, "error.html", {"message": exc.detail}, status_code=exc.status_code)


@app.get("/health")
def health() -> dict:
    row = fetch_one("select now() as database_time")
    stream = fetch_one(
        """
        select status,last_heartbeat_at,message_count,flush_count
          from crypto_stream_sessions order by started_at desc limit 1
        """
    )
    return {
        "status": "ok",
        "version": "3.0.1",
        "role": "collection_only",
        "database_time": row["database_time"].isoformat() if row else None,
        "crypto_stream": {
            "status": stream["status"],
            "last_heartbeat_at": stream["last_heartbeat_at"].isoformat() if stream and stream.get("last_heartbeat_at") else None,
            "message_count": stream["message_count"],
            "flush_count": stream["flush_count"],
        } if stream else None,
    }


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return render_template(request, "login.html", {"error": None})


@app.post("/login", response_class=HTMLResponse)
def login(request: Request, username: Annotated[str, Form()], password: Annotated[str, Form()]):
    username_ok = hmac.compare_digest(username, settings.app_username)
    password_ok = hmac.compare_digest(password, settings.app_password)
    if not username_ok or not password_ok:
        return render_template(request, "login.html", {"error": "Incorrect username or password"}, status_code=401)
    request.session.clear()
    request.session["authenticated"] = True
    request.session["username"] = username
    return RedirectResponse("/", status_code=303)


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, _user: str = Depends(require_user)):
    runs = fetch_all("select * from collection_runs order by created_at desc limit 25")
    database_size = fetch_one("select pg_database_size(current_database()) as bytes")
    provider_counts = fetch_all(
        """
        select provider,count(*) as instruments,
               count(*) filter (where preferred) as preferred_instruments
          from instruments group by provider order by provider
        """
    )
    table_counts = fetch_one(
        """
        select
          (select count(*) from capture_windows) as capture_windows,
          (select count(*) from market_trades) as market_trades,
          (select count(*) from market_quotes_l1) as market_quotes,
          (select count(*) from crypto_microstructure_1s) as crypto_seconds,
          (select count(*) from crypto_derivatives_metrics) as derivative_rows,
          (select count(*) from crypto_supply_snapshots) as supply_rows,
          (select count(*) from crypto_raw_objects) as raw_objects
        """
    )
    stream = fetch_one(
        """
        select * from crypto_stream_sessions
         order by started_at desc limit 1
        """
    )
    health = fetch_all(
        "select * from provider_health order by provider,service"
    )
    return render_template(
        request,
        "dashboard.html",
        {
            "runs": runs,
            "providers": ALL_PROVIDERS,
            "database_bytes": int(database_size["bytes"]) if database_size else 0,
            "provider_counts": provider_counts,
            "table_counts": table_counts or {},
            "stream": stream,
            "provider_health": health,
            "settings": settings,
        },
    )


@app.post("/runs")
def create_run(
    request: Request,
    name: Annotated[str, Form()],
    days: Annotated[int, Form()] = 30,
    providers: Annotated[list[str] | None, Form()] = None,
    _user: str = Depends(require_user),
):
    if days < 1 or days > 365:
        raise HTTPException(status_code=400, detail="Days must be between 1 and 365")
    run_id = create_collection_run(name.strip() or f"Mining run {datetime.now():%Y-%m-%d}", providers or [], days)
    return RedirectResponse(f"/runs/{run_id}", status_code=303)


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    run = fetch_one("select * from collection_runs where id=%s", (run_id,))
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    partition_counts = fetch_all(
        """
        select provider,data_type,status,count(*) as partitions,coalesce(sum(row_count),0) as rows
          from collection_partitions
         where run_id=%s
         group by provider,data_type,status
         order by data_type,provider,status
        """,
        (run_id,),
    )
    failed = fetch_all(
        """
        select id,provider,provider_symbol,data_type,start_ts,end_ts,attempts,last_error,error_code
          from collection_partitions
         where run_id=%s and status in ('failed','retry_wait')
         order by updated_at desc limit 100
        """,
        (run_id,),
    )
    capture_summary = fetch_all(
        """
        select provider,asset_class,trigger_kind,count(*) as windows,
               min(trigger_ts) as first_trigger,max(trigger_ts) as last_trigger
          from capture_windows where run_id=%s
         group by provider,asset_class,trigger_kind
         order by provider,trigger_kind
        """,
        (run_id,),
    )
    return render_template(
        request,
        "run_detail.html",
        {
            "run": run,
            "partition_counts": partition_counts,
            "failed": failed,
            "capture_summary": capture_summary,
        },
    )


@app.post("/runs/{run_id}/enhance")
def enhance_run(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    request_enhancement(run_id)
    return RedirectResponse(f"/runs/{run_id}", status_code=303)


@app.post("/runs/{run_id}/pause")
def pause_run(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "update collection_runs set status='paused',updated_at=now() where id=%s and status in ('queued','running')",
            (run_id,),
        )
        conn.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=303)


@app.post("/runs/{run_id}/resume")
def resume_run(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_runs
               set status='running',completed_at=null,error=null,updated_at=now()
             where id=%s and status in ('paused','completed_with_errors')
            """,
            (run_id,),
        )
        cur.execute(
            """
            update collection_partitions
               set status='retry_wait',not_before=now(),locked_by=null,locked_at=null,updated_at=now()
             where run_id=%s and status in ('failed','cancelled')
               and attempts < max_attempts
            """,
            (run_id,),
        )
        cur.execute("select refresh_collection_run_counts(%s)", (run_id,))
        conn.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=303)


@app.post("/runs/{run_id}/cancel")
def cancel_run(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "update collection_runs set status='cancelled',completed_at=now(),updated_at=now() where id=%s",
            (run_id,),
        )
        cur.execute(
            """
            update collection_partitions
               set status='cancelled',locked_by=null,locked_at=null,updated_at=now()
             where run_id=%s and status in ('queued','retry_wait','running')
            """,
            (run_id,),
        )
        conn.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=303)


@app.post("/runs/cancel-all")
def cancel_all_runs(request: Request, _user: str = Depends(require_user)):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_runs
               set status='cancelled',completed_at=now(),updated_at=now()
             where status in ('queued','running','paused')
            """
        )
        cur.execute(
            """
            update collection_partitions
               set status='cancelled',locked_by=null,locked_at=null,updated_at=now()
             where status in ('queued','retry_wait','running')
            """
        )
        conn.commit()
    return RedirectResponse("/", status_code=303)


@app.post("/runs/{run_id}/retry-failed")
def retry_failed(request: Request, run_id: UUID, _user: str = Depends(require_user)):
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update collection_partitions
               set status='retry_wait',not_before=now(),locked_by=null,locked_at=null,
                   last_error=null,error_code=null,updated_at=now()
             where run_id=%s and status in ('failed','cancelled')
            """,
            (run_id,),
        )
        cur.execute(
            """
            update collection_runs
               set status='running',completed_at=null,error=null,updated_at=now()
             where id=%s and status not in ('cancelled','failed')
            """,
            (run_id,),
        )
        cur.execute("select refresh_collection_run_counts(%s)", (run_id,))
        conn.commit()
    return RedirectResponse(f"/runs/{run_id}", status_code=303)


@app.get("/instruments", response_class=HTMLResponse)
def instruments_page(
    request: Request,
    provider: str | None = None,
    asset_class: str | None = None,
    _user: str = Depends(require_user),
):
    clauses = ["1=1"]
    params: list[object] = []
    if provider:
        clauses.append("provider=%s")
        params.append(provider)
    if asset_class:
        clauses.append("asset_class=%s")
        params.append(asset_class)
    rows = fetch_all(
        f"""
        select * from instruments
         where {' and '.join(clauses)}
         order by provider,priority desc,provider_symbol
         limit 5000
        """,
        params,
    )
    classes = fetch_all("select distinct asset_class from instruments order by asset_class")
    return render_template(
        request,
        "instruments.html",
        {
            "instruments": rows,
            "providers": ALL_PROVIDERS,
            "asset_classes": [row["asset_class"] for row in classes],
            "selected_provider": provider,
            "selected_asset_class": asset_class,
        },
    )
