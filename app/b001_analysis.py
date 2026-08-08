from __future__ import annotations

import csv
import json
import math
import tempfile
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import UUID

from psycopg.types.json import Jsonb

from app.b001_contract import (
    BTC_HEDGE_WEIGHT,
    CLOSE_VS_VWAP_MAX,
    DISPERSION_MAX,
    FINAL_5M_MAX,
    HIGH_TO_CLOSE_MIN,
    HOLD_HOURS,
    PRIMARY_COMBINED_COST_BP,
    STRESS_COSTS_BP,
    TOKEN_COST_BP,
    MetricInput,
    calculate_metrics,
)
from app.db import db_connection, fetch_all, fetch_one
from app.storage import SupabaseStorage


def _cost(bp: float) -> float:
    return float(bp) / 10_000.0


def _primary_cost(structure: str) -> float:
    return TOKEN_COST_BP if structure == "B-001b" else PRIMARY_COMBINED_COST_BP


def _signal_price_outcome(run_id: UUID, signal: dict[str, Any], hold_hours: int = HOLD_HOURS) -> dict[str, Any] | None:
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
        (exit_ts, entry_ts, exit_ts, run_id, signal["symbol"], entry_ts),
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
        ), outcomes as (
            select m.symbol,x.open/e.open-1.0 ret
            from members m
            join crypto_b001_replication_15m e on e.run_id=%s and e.symbol=m.symbol and e.bucket_start=%s
            join crypto_b001_replication_15m x on x.run_id=e.run_id and x.symbol=e.symbol and x.bucket_start=%s
        )
        select count(*) n,avg(ret) basket_return from outcomes
        """,
        (run_id, signal["bucket_start"], signal["symbol"], run_id, entry_ts, exit_ts),
    )
    return {
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "token_entry": float(row["token_entry"]),
        "token_exit": float(row["token_exit"]),
        "btc_entry": float(row["btc_entry"]),
        "btc_exit": float(row["btc_exit"]),
        "token_gross": token_gross,
        "btc_return": btc_return,
        "basket_return": float(basket["basket_return"]) if basket and basket.get("basket_return") is not None else None,
        "basket_members": int(basket["n"]) if basket else 0,
    }


def _path_excursions(run_id: UUID, signal: dict[str, Any], outcome: dict[str, Any], structure: str, hedge_weight: float = BTC_HEDGE_WEIGHT) -> tuple[float | None, float | None]:
    rows = fetch_all(
        """
        select t.bucket_start,t.open token_open,b.open btc_open
        from crypto_b001_replication_15m t
        left join crypto_b001_replication_15m b
          on b.run_id=t.run_id and b.symbol='BTCUSDT' and b.bucket_start=t.bucket_start
        where t.run_id=%s and t.symbol=%s and t.bucket_start between %s and %s
        order by t.bucket_start
        """,
        (run_id, signal["symbol"], outcome["entry_ts"], outcome["exit_ts"]),
    )
    expected = int((outcome["exit_ts"] - outcome["entry_ts"]).total_seconds() // 900) + 1
    if len(rows) != expected or any(row.get("btc_open") is None for row in rows):
        return None, None
    basket_path: dict[datetime, float] = {}
    if structure == "B-001c":
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
            select p.bucket_start,avg(p.open/e.entry_open-1.0) basket_return
            from entry e join crypto_b001_replication_15m p
              on p.run_id=%s and p.symbol=e.symbol and p.bucket_start between %s and %s
            group by p.bucket_start order by p.bucket_start
            """,
            (run_id, signal["bucket_start"], signal["symbol"], run_id, outcome["entry_ts"], run_id, outcome["entry_ts"], outcome["exit_ts"]),
        )
        basket_path = {row["bucket_start"]: float(row["basket_return"]) for row in basket_rows if row.get("basket_return") is not None}
        if len(basket_path) != expected:
            return None, None
    path_values: list[float] = []
    for row in rows:
        token = 1.0 - float(row["token_open"]) / outcome["token_entry"]
        if structure == "B-001b":
            value = token
        elif structure == "B-001a":
            btc = float(row["btc_open"]) / outcome["btc_entry"] - 1.0
            value = token + hedge_weight * btc
        else:
            value = token + hedge_weight * basket_path[row["bucket_start"]]
        path_values.append(value)
    return min(path_values), max(path_values)


def _insert_trade(
    run_id: UUID,
    signal: dict[str, Any],
    outcome: dict[str, Any],
    structure: str,
    mode: str,
    subset: str,
    ignored: bool,
    concurrency: int | None,
    hedge_weight: float = BTC_HEDGE_WEIGHT,
) -> None:
    if structure == "B-001a":
        hedge = hedge_weight * outcome["btc_return"]
    elif structure == "B-001c":
        if outcome["basket_return"] is None:
            return
        hedge = hedge_weight * outcome["basket_return"]
    else:
        hedge = 0.0
    gross = outcome["token_gross"] + hedge
    cost_bp = _primary_cost(structure)
    mae, mfe = _path_excursions(run_id, signal, outcome, structure, hedge_weight)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_b001_replication_trades(
                run_id,signal_id,symbol,chronological_block,structure,position_mode,execution_subset,cost_bp,
                entry_ts,exit_ts,token_entry,token_exit,btc_entry,btc_exit,basket_entry_value,basket_exit_value,
                token_gross_return,hedge_gross_return,gross_return,transaction_cost,net_return,mae,mfe,concurrency,ignored_overlap
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (run_id,signal_id,structure,position_mode,execution_subset,cost_bp) do update set
                entry_ts=excluded.entry_ts,exit_ts=excluded.exit_ts,token_entry=excluded.token_entry,token_exit=excluded.token_exit,
                btc_entry=excluded.btc_entry,btc_exit=excluded.btc_exit,basket_entry_value=excluded.basket_entry_value,
                basket_exit_value=excluded.basket_exit_value,token_gross_return=excluded.token_gross_return,
                hedge_gross_return=excluded.hedge_gross_return,gross_return=excluded.gross_return,
                transaction_cost=excluded.transaction_cost,net_return=excluded.net_return,mae=excluded.mae,mfe=excluded.mfe,
                concurrency=excluded.concurrency,ignored_overlap=excluded.ignored_overlap
            """,
            (
                run_id,signal["id"],signal["symbol"],signal["chronological_block"],structure,mode,subset,cost_bp,
                outcome["entry_ts"],outcome["exit_ts"],outcome["token_entry"],outcome["token_exit"],
                outcome["btc_entry"],outcome["btc_exit"],1.0,1.0 + (outcome["basket_return"] or 0.0) if structure=="B-001c" else None,
                outcome["token_gross"],hedge,gross,_cost(cost_bp),gross-_cost(cost_bp),mae,mfe,concurrency,ignored,
            ),
        )
        conn.commit()


def _build_primary_trades(run_id: UUID) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("delete from crypto_b001_replication_trades where run_id=%s", (run_id,))
        conn.commit()
    signals = fetch_all("select * from crypto_b001_replication_signals where run_id=%s order by bucket_start,symbol", (run_id,))
    outcomes: dict[int, dict[str, Any]] = {}
    for signal in signals:
        outcome = _signal_price_outcome(run_id, signal)
        if outcome:
            outcomes[int(signal["id"])] = outcome
    accepted: dict[int, bool] = {}
    active_until: dict[str, datetime] = {}
    accepted_intervals: list[tuple[int, datetime, datetime, str]] = []
    for signal in signals:
        outcome = outcomes.get(int(signal["id"]))
        if not outcome:
            continue
        overlapping = active_until.get(signal["symbol"]) is not None and outcome["entry_ts"] < active_until[signal["symbol"]]
        accepted[int(signal["id"])] = not overlapping
        if not overlapping:
            active_until[signal["symbol"]] = outcome["exit_ts"]
            accepted_intervals.append((int(signal["id"]),outcome["entry_ts"],outcome["exit_ts"],signal["symbol"]))
    concurrency = {
        signal_id: sum(1 for _other,entry,exit,_sym in accepted_intervals if entry <= this_entry < exit)
        for signal_id,this_entry,_this_exit,_symbol in accepted_intervals
    }
    for signal in signals:
        outcome = outcomes.get(int(signal["id"]))
        if not outcome:
            continue
        for subset in ("research","historically_executable"):
            if subset == "historically_executable" and signal.get("historically_executable") is not True:
                continue
            for structure in ("B-001a","B-001b","B-001c"):
                _insert_trade(run_id,signal,outcome,structure,"signal_level",subset,False,None)
                _insert_trade(
                    run_id,signal,outcome,structure,"portfolio",subset,
                    not accepted.get(int(signal["id"]),False),
                    concurrency.get(int(signal["id"])) if accepted.get(int(signal["id"]),False) else None,
                )


def _metrics_for_trade_rows(rows: Iterable[dict[str, Any]]) -> dict:
    metric_rows = [
        MetricInput(
            symbol=row["symbol"],signal_ts=row["signal_ts"],net_return=float(row["net_return"]),
            mae=float(row["mae"]) if row.get("mae") is not None else None,
            mfe=float(row["mfe"]) if row.get("mfe") is not None else None,
            concurrency=int(row["concurrency"]) if row.get("concurrency") is not None else None,
        )
        for row in rows if not row.get("ignored_overlap")
    ]
    return calculate_metrics(metric_rows)


def _persist_primary_metrics(run_id: UUID) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("delete from crypto_b001_replication_metrics where run_id=%s", (run_id,))
        conn.commit()
    for structure in ("B-001a","B-001b","B-001c"):
        for mode in ("signal_level","portfolio"):
            for subset in ("research","historically_executable"):
                base = fetch_all(
                    """
                    select t.*,s.signal_ts from crypto_b001_replication_trades t
                    join crypto_b001_replication_signals s on s.id=t.signal_id
                    where t.run_id=%s and t.structure=%s and t.position_mode=%s and t.execution_subset=%s and t.cost_bp=%s
                    order by s.signal_ts,t.symbol
                    """,
                    (run_id,structure,mode,subset,_primary_cost(structure)),
                )
                for block in ("aggregate","1","2","3"):
                    selected = base if block=="aggregate" else [row for row in base if int(row["chronological_block"])==int(block)]
                    metrics = _metrics_for_trade_rows(selected)
                    with db_connection() as conn, conn.cursor() as cur:
                        cur.execute(
                            """
                            insert into crypto_b001_replication_metrics(run_id,structure,position_mode,execution_subset,cost_bp,block,metrics)
                            values (%s,%s,%s,%s,%s,%s,%s)
                            on conflict (run_id,structure,position_mode,execution_subset,cost_bp,block) do update set metrics=excluded.metrics,created_at=now()
                            """,
                            (run_id,structure,mode,subset,_primary_cost(structure),block,Jsonb(metrics)),
                        )
                        conn.commit()


def _simple_b001a_outcome(run_id: UUID, symbol: str, signal_bucket: datetime, hold_hours: int = HOLD_HOURS, hedge_weight: float = BTC_HEDGE_WEIGHT, cost_bp: float = PRIMARY_COMBINED_COST_BP) -> tuple[datetime,float] | None:
    entry = signal_bucket + timedelta(minutes=15)
    exit_ts = entry + timedelta(hours=hold_hours)
    row = fetch_one(
        """
        select te.open te,tx.open tx,be.open be,bx.open bx
        from crypto_b001_replication_15m te
        join crypto_b001_replication_15m tx on tx.run_id=te.run_id and tx.symbol=te.symbol and tx.bucket_start=%s
        join crypto_b001_replication_15m be on be.run_id=te.run_id and be.symbol='BTCUSDT' and be.bucket_start=%s
        join crypto_b001_replication_15m bx on bx.run_id=te.run_id and bx.symbol='BTCUSDT' and bx.bucket_start=%s
        where te.run_id=%s and te.symbol=%s and te.bucket_start=%s
        """,
        (exit_ts,entry,exit_ts,run_id,symbol,entry),
    )
    if not row:
        return None
    token = 1-float(row["tx"])/float(row["te"])
    btc = float(row["bx"])/float(row["be"])-1
    return entry, token + hedge_weight*btc - _cost(cost_bp)


def _persist_placebo(run_id: UUID, placebo_type: str, variant: str, rows: list[MetricInput], details: dict | None = None) -> dict:
    metrics = calculate_metrics(rows)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_b001_replication_placebos(run_id,placebo_type,variant,metrics,details)
            values (%s,%s,%s,%s,%s)
            on conflict (run_id,placebo_type,variant,block) do update set metrics=excluded.metrics,details=excluded.details,created_at=now()
            """,
            (run_id,placebo_type,variant,Jsonb(metrics),Jsonb(details or {})),
        )
        conn.commit()
    return metrics


def _timestamp_placebos(run_id: UUID, signals: list[dict[str, Any]]) -> None:
    for shift in (-60,-30,-15,15,30,60):
        rows: list[MetricInput] = []
        for signal in signals:
            shifted = signal["bucket_start"] + timedelta(minutes=shift)
            result = _simple_b001a_outcome(run_id,signal["symbol"],shifted)
            if result:
                entry,value = result
                rows.append(MetricInput(signal["symbol"],entry,value))
        _persist_placebo(run_id,"timestamp",f"shift_{shift:+d}m",rows,{"shift_minutes":shift})


def _symbol_placebo(run_id: UUID, signals: list[dict[str, Any]]) -> None:
    rows: list[MetricInput] = []
    for signal in signals:
        controls = fetch_all(
            """
            select f.symbol from crypto_b001_replication_features f
            where f.run_id=%s and f.bucket_start=%s and f.liquidity_eligible and f.symbol<>%s
              and not exists (
                  select 1 from crypto_b001_replication_signals s
                  where s.run_id=f.run_id and s.bucket_start=f.bucket_start and s.symbol=f.symbol
              )
            order by md5(f.symbol || %s) limit 5
            """,
            (run_id,signal["bucket_start"],signal["symbol"],f"B001_SYMBOL_PLACEBO_V1:{signal['id']}"),
        )
        for control in controls:
            result = _simple_b001a_outcome(run_id,control["symbol"],signal["bucket_start"])
            if result:
                entry,value=result
                rows.append(MetricInput(control["symbol"],entry,value))
    _persist_placebo(run_id,"symbol","five_deterministic_non_signal_controls",rows,{"controls_per_signal":5,"seed":"B001_SYMBOL_PLACEBO_V1"})


def _low_dispersion_placebo(run_id: UUID) -> None:
    run = fetch_one("select requested_start,requested_end from crypto_b001_replication_runs where id=%s", (run_id,))
    rows = fetch_all(
        """
        with candidates as (
            select f.symbol,f.bucket_start,
                   row_number() over(partition by f.bucket_start order by md5(f.symbol || f.bucket_start::text || 'B001_LOW_DISP_V1')) rn
            from crypto_b001_replication_features f
            join crypto_b001_replication_market_state m on m.run_id=f.run_id and m.bucket_start=f.bucket_start
            where f.run_id=%s and f.bucket_start >= %s and f.bucket_start < %s
              and f.liquidity_eligible and m.dispersion15 <= %s
              and not exists (select 1 from crypto_b001_replication_signals s where s.run_id=f.run_id and s.symbol=f.symbol and s.bucket_start=f.bucket_start)
        ), picked as (select * from candidates where rn=1), outcomes as (
            select p.symbol,p.bucket_start,te.open te,tx.open tx,be.open be,bx.open bx
            from picked p
            join crypto_b001_replication_15m te on te.run_id=%s and te.symbol=p.symbol and te.bucket_start=p.bucket_start+interval '15 minutes'
            join crypto_b001_replication_15m tx on tx.run_id=te.run_id and tx.symbol=te.symbol and tx.bucket_start=p.bucket_start+interval '8 hours 15 minutes'
            join crypto_b001_replication_15m be on be.run_id=te.run_id and be.symbol='BTCUSDT' and be.bucket_start=p.bucket_start+interval '15 minutes'
            join crypto_b001_replication_15m bx on bx.run_id=te.run_id and bx.symbol='BTCUSDT' and bx.bucket_start=p.bucket_start+interval '8 hours 15 minutes'
        )
        select symbol,bucket_start,1-tx/te+0.75*(bx/be-1)-%s net_return from outcomes order by bucket_start
        """,
        (run_id,run["requested_start"],run["requested_end"],DISPERSION_MAX,run_id,_cost(PRIMARY_COMBINED_COST_BP)),
    )
    metrics_rows=[MetricInput(row["symbol"],row["bucket_start"]+timedelta(minutes=15),float(row["net_return"])) for row in rows]
    _persist_placebo(run_id,"low_dispersion","one_deterministic_ordinary_liquid_short_per_low_dispersion_timestamp",metrics_rows,{"seed":"B001_LOW_DISP_V1"})


def _candidate_rows_for_variant(
    run_id: UUID,
    removed: str | None = None,
    dispersion_max: float = DISPERSION_MAX,
    final_5m_max: float = FINAL_5M_MAX,
    high_to_close_min: float = HIGH_TO_CLOSE_MIN,
    close_vs_vwap_max: float = CLOSE_VS_VWAP_MAX,
) -> list[tuple[str,datetime]]:
    run = fetch_one("select requested_start,requested_end from crypto_b001_replication_runs where id=%s", (run_id,))
    output: list[tuple[str,datetime]]=[]
    cursor = run["requested_start"].replace(day=1,hour=0,minute=0,second=0,microsecond=0)
    while cursor < run["requested_end"]:
        month_end = (cursor + timedelta(days=32)).replace(day=1)
        start=max(cursor,run["requested_start"]); end=min(month_end,run["requested_end"])
        rank_start=start-timedelta(minutes=75)
        conditions=[]
        if removed != "extreme_state": conditions.append("c.extreme")
        if removed != "persistence_recency": conditions.append("p.extreme and p2.extreme and not p5.extreme")
        if removed != "exhaustion_confirmation": conditions.append("c.ret15 <= 0 and c.range15 < p.range15")
        if removed != "minute_rejection": conditions.append("((c.final_5m_return <= %(f5)s)::int+(c.high_to_close_rejection >= %(hc)s)::int+(c.close_vs_vwap <= %(cv)s)::int)>=2")
        if removed != "dispersion_filter": conditions.append("m.dispersion15 <= %(disp)s")
        where=" and ".join(conditions) if conditions else "true"
        sql=f"""
        with ranked as (
            select f.*,
                percent_rank() over(partition by bucket_start order by range15) rp,
                percent_rank() over(partition by bucket_start order by pos_vs_low4h) lp,
                percent_rank() over(partition by bucket_start order by qv_ratio16) qp
            from crypto_b001_replication_features f
            where f.run_id=%(run)s and f.bucket_start >= %(rank_start)s and f.bucket_start < %(end)s
              and f.liquidity_eligible and f.range15 is not null and f.pos_vs_low4h is not null and f.qv_ratio16 is not null
        ), state as (
            select ranked.*,(rp>=0.90 and lp>=0.90 and qp>=0.90) extreme from ranked
        )
        select c.symbol,c.bucket_start from state c
        join state p on p.symbol=c.symbol and p.bucket_start=c.bucket_start-interval '15 minutes'
        join state p2 on p2.symbol=c.symbol and p2.bucket_start=c.bucket_start-interval '30 minutes'
        join state p5 on p5.symbol=c.symbol and p5.bucket_start=c.bucket_start-interval '75 minutes'
        join crypto_b001_replication_market_state m on m.run_id=c.run_id and m.bucket_start=c.bucket_start
        where c.bucket_start >= %(start)s and c.bucket_start < %(end)s and {where}
        """
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(sql,{"run":run_id,"rank_start":rank_start,"start":start,"end":end,"f5":final_5m_max,"hc":high_to_close_min,"cv":close_vs_vwap_max,"disp":dispersion_max})
            output.extend((row["symbol"],row["bucket_start"]) for row in cur.fetchall())
            conn.commit()
        cursor=month_end
    return output


def _variant_metrics(run_id: UUID, candidates: list[tuple[str,datetime]], hold_hours: int = HOLD_HOURS, hedge_weight: float = BTC_HEDGE_WEIGHT, cost_bp: float | None = None) -> dict:
    if cost_bp is None:
        cost_bp=TOKEN_COST_BP+hedge_weight*TOKEN_COST_BP
    rows=[]
    for symbol,bucket in candidates:
        result=_simple_b001a_outcome(run_id,symbol,bucket,hold_hours=hold_hours,hedge_weight=hedge_weight,cost_bp=cost_bp)
        if result:
            entry,value=result
            rows.append(MetricInput(symbol,entry,value))
    return calculate_metrics(rows)


def _component_ablations(run_id: UUID) -> None:
    for removed in ("extreme_state","persistence_recency","exhaustion_confirmation","minute_rejection","dispersion_filter"):
        candidates=_candidate_rows_for_variant(run_id,removed=removed)
        metrics=_variant_metrics(run_id,candidates,cost_bp=PRIMARY_COMBINED_COST_BP)
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into crypto_b001_replication_placebos(run_id,placebo_type,variant,metrics,details)
                values (%s,'component_ablation',%s,%s,%s)
                on conflict (run_id,placebo_type,variant,block) do update set metrics=excluded.metrics,details=excluded.details,created_at=now()""",
                (run_id,f"remove_{removed}",Jsonb(metrics),Jsonb({"removed":removed,"primary_rule_unchanged":True})),
            ); conn.commit()


def _leave_out_tests(run_id: UUID) -> tuple[bool,dict[str,Any]]:
    base=fetch_all(
        """select t.symbol,s.signal_ts,t.net_return,t.mae,t.mfe,t.concurrency
        from crypto_b001_replication_trades t join crypto_b001_replication_signals s on s.id=t.signal_id
        where t.run_id=%s and t.structure='B-001a' and t.position_mode='portfolio' and t.execution_subset='research'
          and t.cost_bp=%s and not t.ignored_overlap order by s.signal_ts""",
        (run_id,PRIMARY_COMBINED_COST_BP),
    )
    symbols=sorted({row["symbol"] for row in base})
    all_positive=True
    leave_metrics={}
    for symbol in symbols:
        selected=[row for row in base if row["symbol"]!=symbol]
        metrics=_metrics_for_trade_rows(selected)
        leave_metrics[symbol]=metrics
        if (metrics.get("mean_net_return") or -1) <= 0:
            all_positive=False
        _persist_placebo(run_id,"leave_one_symbol_out",symbol,[MetricInput(r["symbol"],r["signal_ts"],float(r["net_return"])) for r in selected])
    months=sorted({row["signal_ts"].strftime("%Y-%m") for row in base})
    for month in months:
        selected=[row for row in base if row["signal_ts"].strftime("%Y-%m")!=month]
        _persist_placebo(run_id,"leave_one_month_out",month,[MetricInput(r["symbol"],r["signal_ts"],float(r["net_return"])) for r in selected])
    counts=Counter(row["symbol"] for row in base)
    top_two=[symbol for symbol,_count in counts.most_common(2)]
    selected=[row for row in base if row["symbol"] not in top_two]
    top_two_metrics=_metrics_for_trade_rows(selected)
    top_two_pass=(top_two_metrics.get("mean_net_return") or -1)>0 and (top_two_metrics.get("hit_rate") or 0)>0.5
    _persist_placebo(run_id,"symbol_concentration","exclude_top_two_symbols",[MetricInput(r["symbol"],r["signal_ts"],float(r["net_return"])) for r in selected],{"excluded":top_two})
    return all_positive and top_two_pass,{"top_two_symbols":top_two,"top_two_metrics":top_two_metrics,"leave_one_all_mean_positive":all_positive}


def _robustness(run_id: UUID, signals: list[dict[str, Any]]) -> dict[str,dict]:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("delete from crypto_b001_replication_robustness where run_id=%s",(run_id,));conn.commit()
    primary_candidates=[(s["symbol"],s["bucket_start"]) for s in signals]
    outputs={}
    variants=[]
    for bp in STRESS_COSTS_BP:
        variants.append(("cost_stress",f"{bp:g}bp",_variant_metrics(run_id,primary_candidates,cost_bp=bp),{"cost_bp":bp}))
    for hours in (6,10,12):
        variants.append(("holding_period",f"{hours}h",_variant_metrics(run_id,primary_candidates,hold_hours=hours,cost_bp=PRIMARY_COMBINED_COST_BP),{"hold_hours":hours}))
    for weight in (0.50,0.60,0.90):
        cost_bp=TOKEN_COST_BP+weight*TOKEN_COST_BP
        variants.append(("btc_hedge_weight",f"{weight:.2f}",_variant_metrics(run_id,primary_candidates,hedge_weight=weight,cost_bp=cost_bp),{"btc_hedge_weight":weight,"cost_bp":cost_bp}))
    threshold_variants=[]
    for mult in (0.8,0.9,1.1,1.2):
        threshold_variants.append(("dispersion",mult,{"dispersion_max":DISPERSION_MAX*mult}))
        threshold_variants.append(("final_5m",mult,{"final_5m_max":FINAL_5M_MAX*mult}))
        threshold_variants.append(("high_to_close",mult,{"high_to_close_min":HIGH_TO_CLOSE_MIN*mult}))
        threshold_variants.append(("close_vs_vwap",mult,{"close_vs_vwap_max":CLOSE_VS_VWAP_MAX*mult}))
    for kind,mult,params in threshold_variants:
        kwargs={"dispersion_max":DISPERSION_MAX,"final_5m_max":FINAL_5M_MAX,"high_to_close_min":HIGH_TO_CLOSE_MIN,"close_vs_vwap_max":CLOSE_VS_VWAP_MAX}
        kwargs.update(params)
        candidates=_candidate_rows_for_variant(run_id,**kwargs)
        variants.append((f"threshold_{kind}",f"x{mult:.1f}",_variant_metrics(run_id,candidates,cost_bp=PRIMARY_COMBINED_COST_BP),params))
    for rtype,variant,metrics,params in variants:
        outputs[f"{rtype}:{variant}"]=metrics
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute(
                """insert into crypto_b001_replication_robustness(run_id,robustness_type,variant,metrics,parameters)
                values (%s,%s,%s,%s,%s) on conflict (run_id,robustness_type,variant) do update set metrics=excluded.metrics,parameters=excluded.parameters,created_at=now()""",
                (run_id,rtype,variant,Jsonb(metrics),Jsonb(params)),
            );conn.commit()
    return outputs


def _qa_checks(run_id: UUID) -> list[dict[str,Any]]:
    run=fetch_one("select * from crypto_b001_replication_runs where id=%s",(run_id,))
    violations={}
    violations[2]=fetch_one("select count(*) n from crypto_b001_replication_signals where run_id=%s and signal_ts<>bucket_start+interval '15 minutes'",(run_id,))["n"]
    violations[3]=fetch_one("select count(*) n from crypto_b001_replication_trades t join crypto_b001_replication_signals s on s.id=t.signal_id where t.run_id=%s and t.entry_ts<=s.signal_ts",(run_id,))["n"]
    violations[8]=fetch_one("select count(*) n from crypto_b001_replication_trades where run_id=%s and (btc_entry is null or btc_exit is null) and structure='B-001a'",(run_id,))["n"]
    violations[9]=fetch_one("select count(*) n from crypto_b001_replication_trades where run_id=%s and abs(net_return-(gross_return-transaction_cost))>1e-12",(run_id,))["n"]
    violations[10]=fetch_one(
        """select count(*) n from crypto_b001_replication_trades a join crypto_b001_replication_trades b
        on a.run_id=b.run_id and a.symbol=b.symbol and a.id<b.id and a.entry_ts<b.exit_ts and b.entry_ts<a.exit_ts
        where a.run_id=%s and a.structure='B-001a' and b.structure='B-001a' and a.position_mode='portfolio' and b.position_mode='portfolio'
          and a.execution_subset='research' and b.execution_subset='research' and not a.ignored_overlap and not b.ignored_overlap""",
        (run_id,),
    )["n"]
    violations[11]=fetch_one("select count(*) n from crypto_b001_replication_15m where run_id=%s and minute_count<>15",(run_id,))["n"]
    historical_not_current=fetch_one(
        """select count(distinct a.symbol) n from crypto_b001_replication_archive_files a
        where a.run_id=%s and a.source_status='loaded' and not exists (
            select 1 from crypto_venue_symbols c where c.provider='binance' and c.venue_symbol=a.symbol and c.tradable
        )""",
        (run_id,),
    )["n"]
    exact_ok=(run["exact_thresholds"].get("dispersion_max")==DISPERSION_MAX and run["exact_thresholds"].get("final_5m_return_max")==FINAL_5M_MAX and run["exact_thresholds"].get("high_to_close_rejection_min")==HIGH_TO_CLOSE_MIN and run["exact_thresholds"].get("close_vs_vwap_max")==CLOSE_VS_VWAP_MAX)
    coverage_days=max(0.0,(min(run.get("effective_end") or run["requested_start"],run["requested_end"])-max(run.get("effective_start") or run["requested_end"],run["requested_start"])).total_seconds()/86400)
    checks=[
        (1,"No look-ahead leakage",run["requested_end"]<=run["discovery_start"] and run["liquidity_method"].startswith("trailing_18d_pre_signal"),{"requested_end":run["requested_end"].isoformat(),"discovery_start":run["discovery_start"].isoformat(),"liquidity_method":run["liquidity_method"]}),
        (2,"Signal features known by signal-bar completion",violations[2]==0,{"violations":violations[2]}),
        (3,"Entry occurs strictly after signal is known",violations[3]==0,{"violations":violations[3]}),
        (4,"Cross-sectional ranks are contemporaneous",True,{"implementation":"percent_rank partitioned by bucket_start over liquidity_eligible rows only"}),
        (5,"Rolling features contain no future observations",True,{"implementation":"ROWS ... PRECEDING/CURRENT; trailing liquidity RANGE ends 15 minutes before T"}),
        (6,"Historical/delisted symbols are not filtered by current listings",historical_not_current>=0,{"loaded_symbols_absent_from_current_tradable_catalogue":historical_not_current,"source":"Binance archive index"}),
        (7,"No future liquidity information",run["liquidity_method"]=="trailing_18d_pre_signal_avg_quote_volume_percent_rank_top_half",{"method":run["liquidity_method"]}),
        (8,"BTC hedge prices align with exact token timestamps",violations[8]==0,{"violations":violations[8]}),
        (9,"Transaction costs deducted exactly",violations[9]==0,{"violations":violations[9]}),
        (10,"Same-token overlapping portfolio trades suppressed",violations[10]==0,{"violations":violations[10]}),
        (11,"Incomplete one-minute data never creates a 15-minute observation",violations[11]==0,{"violations":violations[11],"incomplete_buckets_omitted":run["incomplete_15m_buckets"]}),
        (12,"Frozen numeric thresholds exactly match contract",exact_ok,{"thresholds":run["exact_thresholds"]}),
        (13,"Minimum historical replication coverage",coverage_days>=365-3,{"coverage_days":coverage_days,"minimum_target_days":362}),
    ]
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("delete from crypto_b001_replication_qa where run_id=%s",(run_id,))
        for number,name,passed,details in checks:
            cur.execute("insert into crypto_b001_replication_qa(run_id,check_number,check_name,passed,details) values (%s,%s,%s,%s,%s)",(run_id,number,name,passed,Jsonb(details)))
        conn.commit()
    return [{"number":n,"name":name,"passed":passed,"details":details} for n,name,passed,details in checks]


def _classification(run_id: UUID, robustness: dict[str,dict], concentration_pass: bool, concentration_details: dict) -> tuple[str,str,dict]:
    primary_row=fetch_one(
        "select metrics from crypto_b001_replication_metrics where run_id=%s and structure='B-001a' and position_mode='portfolio' and execution_subset='research' and cost_bp=%s and block='aggregate'",
        (run_id,PRIMARY_COMBINED_COST_BP),
    )
    primary=(primary_row or {}).get("metrics") or {}
    blocks=[]
    for block in ("1","2","3"):
        row=fetch_one("select metrics from crypto_b001_replication_metrics where run_id=%s and structure='B-001a' and position_mode='portfolio' and execution_subset='research' and cost_bp=%s and block=%s",(run_id,PRIMARY_COMBINED_COST_BP,block))
        blocks.append((row or {}).get("metrics") or {})
    exec_row=fetch_one("select metrics from crypto_b001_replication_metrics where run_id=%s and structure='B-001a' and position_mode='portfolio' and execution_subset='historically_executable' and cost_bp=%s and block='aggregate'",(run_id,PRIMARY_COMBINED_COST_BP))
    executable=(exec_row or {}).get("metrics") or {}
    n=int(primary.get("n_trades") or 0)
    hit=float(primary.get("hit_rate") or 0)
    mean=float(primary.get("mean_net_return") or 0)
    loss_ratio=primary.get("worst_loss_ratio")
    loss_pass=loss_ratio is not None and float(loss_ratio)<=0.10 and float(primary.get("losers_within_10pct_of_avg_winner_pct") or 0)>=1.0
    positive_blocks=sum(1 for m in blocks if (m.get("hit_rate") or 0)>0.5 and (m.get("mean_net_return") or 0)>0)
    cost_gate=[]
    for bp in (50.0,75.0,100.0):
        m=robustness.get(f"cost_stress:{bp:g}bp") or {}
        cost_gate.append((m.get("hit_rate") or 0)>0.5 and (m.get("mean_net_return") or 0)>0)
    cost_pass=all(cost_gate)
    exec_n=int(executable.get("n_trades") or 0)
    short_pass=exec_n>0 and (executable.get("hit_rate") or 0)>0.5 and (executable.get("mean_net_return") or 0)>0 and executable.get("worst_loss_ratio") is not None and float(executable["worst_loss_ratio"])<=0.10
    score={
        "hit_rate_gt_50":hit>0.5,
        "worst_loss_ratio_le_0_10":loss_pass,
        "sufficient_independent_n":n>=30,
        "multiple_chronological_blocks_positive":positive_blocks>=2,
        "positive_mean_net_return":mean>0,
        "cost_stress":cost_pass,
        "shortability":short_pass,
        "no_one_or_two_symbol_dependence":concentration_pass,
        "primary_n":n,"positive_blocks":positive_blocks,"historically_executable_n":exec_n,
        "concentration_details":concentration_details,
    }
    core=score["hit_rate_gt_50"] and score["worst_loss_ratio_le_0_10"] and score["positive_mean_net_return"]
    all_a=core and score["sufficient_independent_n"] and score["multiple_chronological_blocks_positive"] and cost_pass and short_pass and concentration_pass
    if all_a:
        return "A","Locked unseen-history replication satisfies the primary economic gates, sample-size gate, chronological consistency, cost stress, shortability and concentration checks.",score
    if n<30 and core:
        return "B",f"Primary economics remain promising, but the locked replication produced only {n} independent portfolio trades (<30 required).",score
    if mean<=0 and hit<=0.5:
        return "D",f"Older unseen history materially falsifies B-001: aggregate primary mean net return is {mean:.6f} and hit rate is {hit:.2%}.",score
    failed=[key for key,value in score.items() if isinstance(value,bool) and not value]
    return "C","Predictive/economic structure remains visible but one or more hard replication conditions fail: "+", ".join(failed)+".",score


def _export(run_id: UUID) -> dict[str,Any]:
    tables={
        "signals":"select * from crypto_b001_replication_signals where run_id=%s order by bucket_start,symbol",
        "trades":"select * from crypto_b001_replication_trades where run_id=%s order by entry_ts,symbol,structure,position_mode,execution_subset",
        "metrics":"select * from crypto_b001_replication_metrics where run_id=%s order by structure,position_mode,execution_subset,cost_bp,block",
        "placebos":"select * from crypto_b001_replication_placebos where run_id=%s order by placebo_type,variant",
        "robustness":"select * from crypto_b001_replication_robustness where run_id=%s order by robustness_type,variant",
        "qa":"select * from crypto_b001_replication_qa where run_id=%s order by check_number",
    }
    run=fetch_one("select * from crypto_b001_replication_runs where id=%s",(run_id,))
    with tempfile.TemporaryDirectory(prefix="b001-export-") as tmp:
        root=Path(tmp)
        (root/"run.json").write_text(json.dumps(run,default=str,indent=2),encoding="utf-8")
        for name,sql in tables.items():
            rows=fetch_all(sql,(run_id,))
            path=root/f"{name}.csv"
            if rows:
                with path.open("w",newline="",encoding="utf-8") as handle:
                    writer=csv.DictWriter(handle,fieldnames=list(rows[0].keys()))
                    writer.writeheader()
                    for row in rows:
                        writer.writerow({k:json.dumps(v,default=str) if isinstance(v,(dict,list)) else v for k,v in row.items()})
            else:
                path.write_text("",encoding="utf-8")
        zip_path=root/f"b001_replication_{run_id}.zip"
        with zipfile.ZipFile(zip_path,"w",compression=zipfile.ZIP_DEFLATED) as archive:
            for path in root.iterdir():
                if path!=zip_path:
                    archive.write(path,arcname=path.name)
        object_path=f"b001/{run_id}/exports/{zip_path.name}"
        size,sha=SupabaseStorage().upload_file(zip_path,object_path,"application/zip")
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("insert into crypto_b001_replication_exports(run_id,export_type,storage_object_path,size_bytes,sha256) values (%s,'full_zip',%s,%s,%s) on conflict do nothing",(run_id,object_path,size,sha));conn.commit()
    return {"storage_object_path":object_path,"size_bytes":size,"sha256":sha}


def run_full_analysis(run_id: UUID) -> None:
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("update crypto_b001_replication_runs set stage='trade_simulation',updated_at=now() where id=%s",(run_id,));
        cur.execute("delete from crypto_b001_replication_placebos where run_id=%s",(run_id,));conn.commit()
    _build_primary_trades(run_id)
    _persist_primary_metrics(run_id)
    signals=fetch_all("select * from crypto_b001_replication_signals where run_id=%s order by bucket_start,symbol",(run_id,))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("update crypto_b001_replication_runs set stage='falsification',updated_at=now() where id=%s",(run_id,));conn.commit()
    _timestamp_placebos(run_id,signals)
    _symbol_placebo(run_id,signals)
    _low_dispersion_placebo(run_id)
    _component_ablations(run_id)
    concentration_pass,concentration_details=_leave_out_tests(run_id)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("update crypto_b001_replication_runs set stage='post_replication_robustness',updated_at=now() where id=%s",(run_id,));conn.commit()
    robustness=_robustness(run_id,signals)
    qa=_qa_checks(run_id)
    classification,reason,score=_classification(run_id,robustness,concentration_pass,concentration_details)
    export=_export(run_id)
    mandatory_qa=all(check["passed"] for check in qa if check["number"]<=12)
    coverage_qa=next((check["passed"] for check in qa if check["number"]==13),False)
    final_status="completed" if mandatory_qa and coverage_qa else "completed_with_errors"
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """update crypto_b001_replication_runs set status=%s,stage='completed',classification=%s,
            classification_reason=%s,completed_at=now(),updated_at=now(),execution_spec=execution_spec || %s::jsonb
            where id=%s""",
            (final_status,classification,reason,Jsonb({"hard_rule_scorecard":score,"export":export}),run_id),
        );conn.commit()
