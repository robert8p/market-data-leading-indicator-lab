from __future__ import annotations

"""Release-time hardening facade for the locked B-001 replication runtime.

The large replication and analysis modules hold the research implementation. This
facade applies narrowly scoped correctness overrides before the worker begins:
- T-75 non-membership is treated as extreme_state FALSE, without weakening the
  mandatory T/T-15/T-30 membership and extreme requirements.
- ablations remove only the named component instead of retaining hidden joins.
- B-001c never selects basket constituents based on future exit availability.
- coverage timestamps come from complete bars inside the actual test window.
- a zero-signal run advances to analysis/QA instead of stalling forever.

No frozen B-001 threshold, execution parameter, cost or primary structure is
changed here.
"""

import csv
import json
import tempfile
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

import app.b001_analysis as analysis
import app.b001_replication as replication
from app.b001_contract import (
    CLOSE_VS_VWAP_MAX,
    DISPERSION_MAX,
    EXTREME_PERCENTILE,
    FINAL_5M_MAX,
    HIGH_TO_CLOSE_MIN,
    PRIMARY_COMBINED_COST_BP,
)
from app.db import db_connection, fetch_all, fetch_one
from app.storage import SupabaseStorage


_ORIGINAL_ADVANCE = replication.advance_b001_run


def _refresh_run_stats(run_id: UUID) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with archives as (
                select count(*) planned,
                       count(*) filter(where source_status='loaded') completed,
                       count(*) filter(where source_status='missing') missing,
                       coalesce(sum(rows_in_replication_window) filter(where source_status='loaded'),0) minute_rows,
                       coalesce(sum(incomplete_15m_count) filter(where source_status='loaded'),0) incomplete15
                from crypto_b001_replication_archive_files where run_id=%s
            ), bars as (
                select count(*) complete15,count(distinct symbol) symbols,
                       min(bucket_start) first_bucket,max(bucket_start)+interval '15 minutes' last_bucket_end
                from crypto_b001_replication_15m b
                join crypto_b001_replication_runs r on r.id=b.run_id
                where b.run_id=%s and b.bucket_start>=r.requested_start and b.bucket_start<r.requested_end
            )
            update crypto_b001_replication_runs r set
                archive_files_planned=a.planned,
                archive_files_completed=a.completed,
                archive_files_missing=a.missing,
                one_minute_rows=a.minute_rows,
                complete_15m_rows=b.complete15,
                incomplete_15m_buckets=a.incomplete15,
                symbols_loaded=b.symbols,
                effective_start=b.first_bucket,
                effective_end=least(r.requested_end,b.last_bucket_end),
                completeness_pct=case when a.planned>0 then 100.0*a.completed/a.planned else 0 end,
                updated_at=now()
            from archives a,bars b where r.id=%s
            """,
            (run_id, run_id, run_id),
        )
        conn.commit()


def _generate_signals(item: dict[str, Any]) -> None:
    start = datetime.fromisoformat(item["payload"]["start"])
    end = datetime.fromisoformat(item["payload"]["end"])
    run = fetch_one(
        "select requested_start,requested_end from crypto_b001_replication_runs where id=%s",
        (item["run_id"],),
    )
    if not run:
        raise RuntimeError("Replication run disappeared")
    span = run["requested_end"] - run["requested_start"]
    b1 = run["requested_start"] + span / 3
    b2 = run["requested_start"] + span * 2 / 3
    rank_start = start - timedelta(minutes=75)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "delete from crypto_b001_replication_signals where run_id=%s and bucket_start >= %s and bucket_start < %s",
            (item["run_id"], start, end),
        )
        cur.execute(
            """
            insert into crypto_b001_replication_signals(
                run_id,symbol,bucket_start,signal_ts,chronological_block,range15,pos_vs_low4h,qv_ratio16,
                range15_pct,pos_vs_low4h_pct,qv_ratio16_pct,extreme_t,extreme_t15,extreme_t30,extreme_t75,
                ret15,previous_range15,final_5m_return,high_to_close_rejection,close_vs_vwap,
                minute_rejection_a,minute_rejection_b,minute_rejection_c,minute_rejection_count,dispersion15,
                trailing_liquidity_avg_qv,liquidity_pct
            )
            with ranked as (
                select f.*,
                    percent_rank() over(partition by bucket_start order by range15) range_pct,
                    percent_rank() over(partition by bucket_start order by pos_vs_low4h) low_pct,
                    percent_rank() over(partition by bucket_start order by qv_ratio16) qv_pct
                from crypto_b001_replication_features f
                where f.run_id=%s and f.bucket_start >= %s and f.bucket_start < %s
                  and f.liquidity_eligible and f.range15 is not null
                  and f.pos_vs_low4h is not null and f.qv_ratio16 is not null
            ), state as (
                select ranked.*,(range_pct >= %s and low_pct >= %s and qv_pct >= %s) extreme
                from ranked
            ), candidates as (
                select c.*,p.range15 previous_range,
                    p.extreme extreme_15,p2.extreme extreme_30,coalesce(p5.extreme,false) extreme_75,
                    m.dispersion15
                from state c
                join state p on p.symbol=c.symbol and p.bucket_start=c.bucket_start-interval '15 minutes'
                join state p2 on p2.symbol=c.symbol and p2.bucket_start=c.bucket_start-interval '30 minutes'
                left join state p5 on p5.symbol=c.symbol and p5.bucket_start=c.bucket_start-interval '75 minutes'
                join crypto_b001_replication_market_state m on m.run_id=c.run_id and m.bucket_start=c.bucket_start
                where c.bucket_start >= %s and c.bucket_start < %s
            )
            select run_id,symbol,bucket_start,signal_ts,
                case when bucket_start < %s then 1 when bucket_start < %s then 2 else 3 end,
                range15,pos_vs_low4h,qv_ratio16,range_pct,low_pct,qv_pct,true,true,true,false,
                ret15,previous_range,final_5m_return,high_to_close_rejection,close_vs_vwap,
                final_5m_return <= %s,
                high_to_close_rejection >= %s,
                close_vs_vwap <= %s,
                (final_5m_return <= %s)::int+(high_to_close_rejection >= %s)::int+(close_vs_vwap <= %s)::int,
                dispersion15,trailing_liquidity_avg_qv,liquidity_pct
            from candidates
            where extreme and extreme_15 and extreme_30 and not extreme_75
              and ret15 <= 0 and range15 < previous_range
              and ((final_5m_return <= %s)::int+(high_to_close_rejection >= %s)::int+(close_vs_vwap <= %s)::int) >= 2
              and dispersion15 <= %s
            """,
            (
                item["run_id"],rank_start,end,
                EXTREME_PERCENTILE,EXTREME_PERCENTILE,EXTREME_PERCENTILE,
                max(start,run["requested_start"]),min(end,run["requested_end"]),b1,b2,
                FINAL_5M_MAX,HIGH_TO_CLOSE_MIN,CLOSE_VS_VWAP_MAX,
                FINAL_5M_MAX,HIGH_TO_CLOSE_MIN,CLOSE_VS_VWAP_MAX,
                FINAL_5M_MAX,HIGH_TO_CLOSE_MIN,CLOSE_VS_VWAP_MAX,DISPERSION_MAX,
            ),
        )
        count = cur.rowcount
        conn.commit()
    replication._complete(item["id"], count)


def _candidate_rows_for_variant(
    run_id: UUID,
    removed: str | None = None,
    dispersion_max: float = DISPERSION_MAX,
    final_5m_max: float = FINAL_5M_MAX,
    high_to_close_min: float = HIGH_TO_CLOSE_MIN,
    close_vs_vwap_max: float = CLOSE_VS_VWAP_MAX,
) -> list[tuple[str, datetime]]:
    run = fetch_one(
        "select requested_start,requested_end from crypto_b001_replication_runs where id=%s",
        (run_id,),
    )
    output: list[tuple[str, datetime]] = []
    cursor = run["requested_start"].replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    while cursor < run["requested_end"]:
        month_end = (cursor + timedelta(days=32)).replace(day=1)
        start = max(cursor,run["requested_start"])
        end = min(month_end,run["requested_end"])
        rank_start = start - timedelta(minutes=75)
        conditions: list[str] = []
        if removed != "extreme_state":
            conditions.append("c.extreme")
        if removed != "persistence_recency":
            conditions.append("coalesce(p.extreme,false) and coalesce(p2.extreme,false) and not coalesce(p5.extreme,false)")
        if removed != "exhaustion_confirmation":
            conditions.append("c.ret15 <= 0 and p.range15 is not null and c.range15 < p.range15")
        if removed != "minute_rejection":
            conditions.append("((c.final_5m_return <= %(f5)s)::int+(c.high_to_close_rejection >= %(hc)s)::int+(c.close_vs_vwap <= %(cv)s)::int)>=2")
        if removed != "dispersion_filter":
            conditions.append("m.dispersion15 <= %(disp)s")
        where = " and ".join(conditions) if conditions else "true"
        sql = f"""
        with ranked as (
            select f.*,
                percent_rank() over(partition by bucket_start order by range15) rp,
                percent_rank() over(partition by bucket_start order by pos_vs_low4h) lp,
                percent_rank() over(partition by bucket_start order by qv_ratio16) qp
            from crypto_b001_replication_features f
            where f.run_id=%(run)s and f.bucket_start >= %(rank_start)s and f.bucket_start < %(end)s
              and f.liquidity_eligible and f.range15 is not null
              and f.pos_vs_low4h is not null and f.qv_ratio16 is not null
        ), state as (
            select ranked.*,(rp>=0.90 and lp>=0.90 and qp>=0.90) extreme from ranked
        )
        select c.symbol,c.bucket_start from state c
        left join state p on p.symbol=c.symbol and p.bucket_start=c.bucket_start-interval '15 minutes'
        left join state p2 on p2.symbol=c.symbol and p2.bucket_start=c.bucket_start-interval '30 minutes'
        left join state p5 on p5.symbol=c.symbol and p5.bucket_start=c.bucket_start-interval '75 minutes'
        join crypto_b001_replication_market_state m on m.run_id=c.run_id and m.bucket_start=c.bucket_start
        where c.bucket_start >= %(start)s and c.bucket_start < %(end)s and {where}
        """
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "run":run_id,"rank_start":rank_start,"start":start,"end":end,
                    "f5":final_5m_max,"hc":high_to_close_min,"cv":close_vs_vwap_max,"disp":dispersion_max,
                },
            )
            output.extend((row["symbol"],row["bucket_start"]) for row in cur.fetchall())
            conn.commit()
        cursor = month_end
    return output


def _signal_price_outcome(run_id: UUID, signal: dict[str, Any], hold_hours: int = 8) -> dict[str, Any] | None:
    entry_ts = signal["bucket_start"] + timedelta(minutes=15)
    exit_ts = entry_ts + timedelta(hours=hold_hours)
    row = fetch_one(
        """
        select te.open token_entry,tx.open token_exit,be.open btc_entry,bx.open btc_exit
        from crypto_b001_replication_15m te
        join crypto_b001_replication_15m tx on tx.run_id=te.run_id and tx.symbol=te.symbol and tx.bucket_start=%s
        join crypto_b001_replication_15m be on be.run_id=te.run_id and be.symbol='BTCUSDT' and be.bucket_start=%s
        join crypto_b001_replication_15m bx on bx.run_id=te.run_id and bx.symbol='BTCUSDT' and bx.bucket_start=%s
        where te.run_id=%s and te.symbol=%s and te.bucket_start=%s
        """,
        (exit_ts,entry_ts,exit_ts,run_id,signal["symbol"],entry_ts),
    )
    if not row:
        return None
    token_gross = 1.0 - float(row["token_exit"]) / float(row["token_entry"])
    btc_return = float(row["btc_exit"]) / float(row["btc_entry"]) - 1.0
    basket = fetch_one(
        """
        with members as (
            select symbol
            from crypto_b001_replication_features
            where run_id=%s and bucket_start=%s and liquidity_eligible and symbol<>%s
        ), entry_members as (
            select m.symbol,e.open entry_open
            from members m
            join crypto_b001_replication_15m e on e.run_id=%s and e.symbol=m.symbol and e.bucket_start=%s
        ), completed as (
            select e.symbol,x.open/e.entry_open-1.0 ret
            from entry_members e
            join crypto_b001_replication_15m x on x.run_id=%s and x.symbol=e.symbol and x.bucket_start=%s
        )
        select
            (select count(*) from members) intended_members,
            (select count(*) from entry_members) entry_members,
            count(*) completed_members,
            avg(ret) basket_return
        from completed
        """,
        (run_id,signal["bucket_start"],signal["symbol"],run_id,entry_ts,run_id,exit_ts),
    )
    intended = int(basket["intended_members"]) if basket else 0
    entered = int(basket["entry_members"]) if basket else 0
    completed = int(basket["completed_members"]) if basket else 0
    basket_return = None
    if basket and intended > 0 and intended == entered == completed and basket.get("basket_return") is not None:
        basket_return = float(basket["basket_return"])
    return {
        "entry_ts":entry_ts,"exit_ts":exit_ts,
        "token_entry":float(row["token_entry"]),"token_exit":float(row["token_exit"]),
        "btc_entry":float(row["btc_entry"]),"btc_exit":float(row["btc_exit"]),
        "token_gross":token_gross,"btc_return":btc_return,
        "basket_return":basket_return,"basket_members":intended,
        "basket_entry_members":entered,"basket_completed_members":completed,
    }


def _path_excursions(
    run_id: UUID,
    signal: dict[str, Any],
    outcome: dict[str, Any],
    structure: str,
    hedge_weight: float = 0.75,
) -> tuple[float | None,float | None]:
    rows = fetch_all(
        """
        select t.bucket_start,t.open token_open,b.open btc_open
        from crypto_b001_replication_15m t
        left join crypto_b001_replication_15m b
          on b.run_id=t.run_id and b.symbol='BTCUSDT' and b.bucket_start=t.bucket_start
        where t.run_id=%s and t.symbol=%s and t.bucket_start between %s and %s
        order by t.bucket_start
        """,
        (run_id,signal["symbol"],outcome["entry_ts"],outcome["exit_ts"]),
    )
    expected = int((outcome["exit_ts"]-outcome["entry_ts"]).total_seconds()//900)+1
    if len(rows) != expected:
        return None,None
    if structure == "B-001a" and any(row.get("btc_open") is None for row in rows):
        return None,None
    basket_path: dict[datetime,float] = {}
    if structure == "B-001c":
        intended = int(outcome.get("basket_members") or 0)
        if intended <= 0 or outcome.get("basket_return") is None:
            return None,None
        basket_rows = fetch_all(
            """
            with members as (
                select symbol
                from crypto_b001_replication_features
                where run_id=%s and bucket_start=%s and liquidity_eligible and symbol<>%s
            ), entry as (
                select m.symbol,b.open entry_open
                from members m join crypto_b001_replication_15m b
                  on b.run_id=%s and b.symbol=m.symbol and b.bucket_start=%s
            )
            select p.bucket_start,count(*) member_count,avg(p.open/e.entry_open-1.0) basket_return
            from entry e join crypto_b001_replication_15m p
              on p.run_id=%s and p.symbol=e.symbol and p.bucket_start between %s and %s
            group by p.bucket_start order by p.bucket_start
            """,
            (run_id,signal["bucket_start"],signal["symbol"],run_id,outcome["entry_ts"],run_id,outcome["entry_ts"],outcome["exit_ts"]),
        )
        if len(basket_rows) != expected or any(int(row["member_count"]) != intended for row in basket_rows):
            return None,None
        basket_path = {row["bucket_start"]:float(row["basket_return"]) for row in basket_rows}
    values: list[float] = []
    for row in rows:
        token = 1.0 - float(row["token_open"]) / outcome["token_entry"]
        if structure == "B-001b":
            value = token
        elif structure == "B-001a":
            btc = float(row["btc_open"]) / outcome["btc_entry"] - 1.0
            value = token + hedge_weight*btc
        else:
            value = token + hedge_weight*basket_path[row["bucket_start"]]
        values.append(value)
    return min(values),max(values)


def _export(run_id: UUID) -> dict[str, Any]:
    tables = {
        "signals":"select * from crypto_b001_replication_signals where run_id=%s order by bucket_start,symbol",
        "trades":"select * from crypto_b001_replication_trades where run_id=%s order by entry_ts,symbol,structure,position_mode,execution_subset",
        "metrics":"select * from crypto_b001_replication_metrics where run_id=%s order by structure,position_mode,execution_subset,cost_bp,block",
        "shortability":"select * from crypto_b001_replication_shortability where run_id=%s order by period_start,symbol",
        "archive_files":"select * from crypto_b001_replication_archive_files where run_id=%s order by period_start,symbol",
        "placebos":"select * from crypto_b001_replication_placebos where run_id=%s order by placebo_type,variant",
        "robustness":"select * from crypto_b001_replication_robustness where run_id=%s order by robustness_type,variant",
        "qa":"select * from crypto_b001_replication_qa where run_id=%s order by check_number",
    }
    run = fetch_one("select * from crypto_b001_replication_runs where id=%s",(run_id,))
    with tempfile.TemporaryDirectory(prefix="b001-export-") as tmp:
        root = Path(tmp)
        (root/"run.json").write_text(json.dumps(run,default=str,indent=2),encoding="utf-8")
        for name,sql in tables.items():
            rows = fetch_all(sql,(run_id,))
            path = root/f"{name}.csv"
            if rows:
                with path.open("w",newline="",encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle,fieldnames=list(rows[0].keys()))
                    writer.writeheader()
                    for row in rows:
                        writer.writerow({k:json.dumps(v,default=str) if isinstance(v,(dict,list)) else v for k,v in row.items()})
            else:
                path.write_text("",encoding="utf-8")
        zip_path = root/f"b001_replication_{run_id}.zip"
        with zipfile.ZipFile(zip_path,"w",compression=zipfile.ZIP_DEFLATED) as archive:
            for path in root.iterdir():
                if path != zip_path:
                    archive.write(path,arcname=path.name)
        object_path = f"b001/{run_id}/exports/{zip_path.name}"
        size,sha = SupabaseStorage().upload_file(zip_path,object_path,"application/zip")
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """insert into crypto_b001_replication_exports(run_id,export_type,storage_object_path,size_bytes,sha256)
            values (%s,'full_zip',%s,%s,%s) on conflict do nothing""",
            (run_id,object_path,size,sha),
        )
        conn.commit()
    return {"storage_object_path":object_path,"size_bytes":size,"sha256":sha}


def advance_b001_run(run_id: UUID) -> None:
    _refresh_run_stats(run_id)
    _ORIGINAL_ADVANCE(run_id)
    run = fetch_one("select status,stage from crypto_b001_replication_runs where id=%s",(run_id,))
    if not run or run["status"] in {"paused","cancelled","failed","completed"}:
        return
    if run["stage"] == "shortability":
        signal_count = fetch_one("select count(*) n from crypto_b001_replication_signals where run_id=%s",(run_id,))["n"]
        short_work = fetch_one("select count(*) n from crypto_b001_replication_work_items where run_id=%s and stage='shortability'",(run_id,))["n"]
        if int(signal_count or 0) == 0 and int(short_work or 0) == 0:
            with db_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    "insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload) values (%s,'analysis','full',%s) on conflict do nothing",
                    (run_id,Jsonb({})),
                )
                cur.execute("update crypto_b001_replication_runs set stage='replication_analysis',updated_at=now() where id=%s",(run_id,))
                conn.commit()


# Apply the hardened functions before exposing the worker facade.
replication._refresh_run_stats = _refresh_run_stats
replication._generate_signals = _generate_signals
replication.advance_b001_run = advance_b001_run
analysis._candidate_rows_for_variant = _candidate_rows_for_variant
analysis._signal_price_outcome = _signal_price_outcome
analysis._path_excursions = _path_excursions
analysis._export = _export

claim_b001_work = replication.claim_b001_work
process_b001_work = replication.process_b001_work
reclaim_stale_b001_work = replication.reclaim_stale_b001_work
create_b001_run = replication.create_b001_run
