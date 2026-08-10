from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from app.cint001_contract import (
    CROSS_SECTIONAL_BUCKETS,
    ENTRY_OFFSET_MINUTES,
    EXPECTED_VALIDATION_MEMBERS,
    HOLD_MINUTES,
    RETURN_LOOKBACK_MINUTES,
    SELECT_BUCKET,
    VALIDATION_ACTIVE_UNIVERSE,
    VALIDATION_SIGNAL_END,
    VALIDATION_START,
)
from app.cint001_execution import (
    _complete,
    _fail,
    _process_month as _process_futures_month,
    _queue_analysis_if_ready,
    claim_execution_work,
    create_execution_run,
    reclaim_stale_execution_work,
)
from app.db import db_connection, fetch_one


def _month_bounds(month_value: str) -> tuple[datetime, datetime]:
    month = date.fromisoformat(month_value)
    start = datetime(month.year, month.month, 1, tzinfo=timezone.utc)
    end = (start + timedelta(days=32)).replace(day=1)
    return start, end


def _materialise_spot_month(item: dict[str, Any]) -> int:
    """Build neutral 15m bars directly from canonical Binance 1m history.

    One hour of lookback is included before every month so an exact 60-minute
    return can be formed at the first validation timestamp without using row-lag
    semantics across a data gap.
    """
    symbol = str(item["payload"]["spot_symbol"]).upper()
    start, end = _month_bounds(str(item["payload"]["month"]))
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("set local statement_timeout = '15min'")
        cur.execute(
            """
            insert into cint001_spot_15m(
                symbol,bucket_start,signal_ts,minute_count,open,high,low,close,
                volume,quote_volume,trade_count,taker_buy_quote_volume,source,updated_at
            )
            with instrument as (
                select id
                from instruments
                where provider='binance' and provider_symbol=%s
                limit 1
            ), aggregated as (
                select
                    date_bin(interval '15 minutes',b.ts,timestamptz '1970-01-01 00:00:00+00') bucket_start,
                    count(*)::int minute_count,
                    (array_agg(b.open order by b.ts))[1] open,
                    max(b.high) high,
                    min(b.low) low,
                    (array_agg(b.close order by b.ts desc))[1] close,
                    sum(coalesce(b.volume,0)) volume,
                    sum(coalesce(b.quote_volume,0)) quote_volume,
                    sum(coalesce(b.trade_count,0))::bigint trade_count,
                    sum(coalesce(b.taker_buy_quote_volume,0)) taker_buy_quote_volume
                from market_bars_1m_binance b
                join instrument i on i.id=b.instrument_id
                where b.provider='binance'
                  and b.ts >= %s - (%s * interval '1 minute')
                  and b.ts < %s
                group by 1
            )
            select %s,bucket_start,bucket_start+interval '15 minutes',minute_count,
                   open,high,low,close,volume,quote_volume,trade_count,
                   taker_buy_quote_volume,'market_bars_1m_binance',now()
            from aggregated
            on conflict(symbol,bucket_start) do update set
                signal_ts=excluded.signal_ts,minute_count=excluded.minute_count,
                open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,
                volume=excluded.volume,quote_volume=excluded.quote_volume,
                trade_count=excluded.trade_count,
                taker_buy_quote_volume=excluded.taker_buy_quote_volume,
                source=excluded.source,updated_at=now()
            """,
            (symbol, start, RETURN_LOOKBACK_MINUTES, end, symbol),
        )
        affected = max(0, cur.rowcount or 0)
        conn.commit()
        return affected


def _adjusted_futures_price_sql(alias: str, spot_symbol_expr: str, futures_symbol_expr: str) -> str:
    # 1000BONK/1000FLOKI/1000SHIB-style perpetuals have a contract unit 1000x
    # their spot token. Percentage returns are scale-invariant; basis is not.
    return (
        f"case when {futures_symbol_expr} like '1000%%USDT' "
        f"and {spot_symbol_expr} not like '1000%%USDT' then {alias}/1000.0 else {alias} end"
    )


def _process_analysis(item: dict[str, Any]) -> None:
    run_id = UUID(str(item["run_id"]))
    active = list(VALIDATION_ACTIVE_UNIVERSE)
    entry_offset = ENTRY_OFFSET_MINUTES
    exit_offset = ENTRY_OFFSET_MINUTES + HOLD_MINUTES
    adjusted_entry = _adjusted_futures_price_sql("fe.open", "m.symbol", "m.futures_symbol")

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("set local statement_timeout = '15min'")
        cur.execute("delete from cint001_execution_trades where run_id=%s", (run_id,))
        cur.execute(
            f"""
            insert into cint001_execution_trades(
                run_id,signal_bucket,signal_ts,entry_ts,exit_ts,phase,spot_symbol,futures_symbol,
                r1h,range15,q_r1h,q_range,selected_count,executable_count,panel_member_count,
                futures_short_return,funding_return,panel_long_return,gross_relative_return,
                spot_entry,futures_entry,futures_exit,entry_basis_bps,exit_basis_bps
            )
            with eligible as (
                select
                    c.symbol,c.bucket_start,c.signal_ts,c.open,c.high,c.low,c.close,
                    c.close/nullif(p.close,0)-1 r1h,
                    c.high/nullif(c.low,0)-1 range15
                from cint001_spot_15m c
                join cint001_spot_15m p
                  on p.symbol=c.symbol
                 and p.bucket_start=c.bucket_start-(%s*interval '1 minute')
                 and p.minute_count=15
                where c.symbol=any(%s)
                  and c.minute_count=15
                  and c.bucket_start >= %s
                  and c.bucket_start < %s
            ), complete_signal_times as (
                select bucket_start
                from eligible
                group by bucket_start
                having count(*)=%s
            ), ranked as (
                select e.*,
                       ntile(%s) over(partition by e.bucket_start order by e.r1h) q_r1h,
                       ntile(%s) over(partition by e.bucket_start order by e.range15) q_range
                from eligible e
                join complete_signal_times c using(bucket_start)
            ), selected as (
                select * from ranked where q_r1h=%s and q_range=%s
            ), counts as (
                select bucket_start,count(*)::int selected_count from selected group by 1
            ), panel as (
                select s.bucket_start,
                       avg(x.open/nullif(e.open,0)-1) panel_long_return,
                       count(*)::int panel_members
                from (select distinct bucket_start from selected) s
                join cint001_spot_15m e
                  on e.symbol=any(%s)
                 and e.bucket_start=s.bucket_start+(%s*interval '1 minute')
                 and e.minute_count=15
                join cint001_spot_15m x
                  on x.symbol=e.symbol
                 and x.bucket_start=s.bucket_start+(%s*interval '1 minute')
                 and x.minute_count=15
                group by 1
                having count(*)=%s
            ), mapped as (
                select s.*,c.selected_count,cm.futures_symbol,
                       s.bucket_start+(%s*interval '1 minute') entry_ts,
                       s.bucket_start+(%s*interval '1 minute') exit_ts
                from selected s
                join counts c using(bucket_start)
                left join cint001_contract_months cm
                  on cm.run_id=%s
                 and cm.spot_symbol=s.symbol
                 and cm.period_start=date_trunc('month',s.bucket_start+(%s*interval '1 minute'))::date
                 and cm.kline_available
                 and cm.funding_available
            ), outcomes as (
                select m.*,p.panel_long_return,p.panel_members,
                       fe.open futures_entry,fx.open futures_exit,se.open spot_entry,
                       1-fx.open/nullif(fe.open,0) futures_short_return,
                       coalesce((
                           select sum(f.funding_rate*coalesce(f.mark_price,fe.open)/nullif(fe.open,0))
                           from crypto_futures_funding_binance f
                           where f.venue_symbol=m.futures_symbol
                             and f.funding_ts>m.entry_ts and f.funding_ts<=m.exit_ts
                       ),0) funding_return
                from mapped m
                join panel p using(bucket_start)
                left join crypto_futures_15m_binance fe
                  on fe.venue_symbol=m.futures_symbol and fe.bucket_start=m.entry_ts
                left join crypto_futures_15m_binance fx
                  on fx.venue_symbol=m.futures_symbol and fx.bucket_start=m.exit_ts
                left join cint001_spot_15m se
                  on se.symbol=m.symbol and se.bucket_start=m.entry_ts and se.minute_count=15
            ), exec_counts as (
                select bucket_start,
                       count(*) filter(where futures_entry is not null and futures_exit is not null)::int executable_count
                from outcomes group by 1
            )
            select %s,o.bucket_start,o.signal_ts,o.entry_ts,o.exit_ts,
                   (extract(hour from o.signal_ts)::int*4+
                    floor(extract(minute from o.signal_ts)/15)::int)::smallint,
                   o.symbol,o.futures_symbol,o.r1h,o.range15,o.q_r1h,o.q_range,
                   o.selected_count,ec.executable_count,o.panel_members,
                   o.futures_short_return,o.funding_return,o.panel_long_return,
                   case when o.futures_short_return is not null
                        then o.futures_short_return+o.funding_return+o.panel_long_return end,
                   o.spot_entry,o.futures_entry,o.futures_exit,
                   case when o.futures_entry is not null and o.spot_entry is not null
                        then (({adjusted_entry})/o.spot_entry-1)*10000 end,
                   null
            from outcomes o join exec_counts ec using(bucket_start)
            """,
            (
                RETURN_LOOKBACK_MINUTES,
                active,
                VALIDATION_START,
                VALIDATION_SIGNAL_END,
                EXPECTED_VALIDATION_MEMBERS,
                CROSS_SECTIONAL_BUCKETS,
                CROSS_SECTIONAL_BUCKETS,
                SELECT_BUCKET,
                SELECT_BUCKET,
                active,
                entry_offset,
                exit_offset,
                EXPECTED_VALIDATION_MEMBERS,
                entry_offset,
                exit_offset,
                run_id,
                entry_offset,
                run_id,
            ),
        )
        conn.commit()

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            update cint001_execution_trades t
               set exit_basis_bps=(
                   case when t.futures_symbol like '1000%USDT' and t.spot_symbol not like '1000%USDT'
                        then t.futures_exit/1000.0 else t.futures_exit end
                   /nullif(s.open,0)-1
               )*10000
              from cint001_spot_15m s
             where t.run_id=%s and s.symbol=t.spot_symbol and s.bucket_start=t.exit_ts
               and s.minute_count=15 and t.futures_exit is not null
            """,
            (run_id,),
        )
        conn.commit()

    metrics = _execution_metrics(run_id)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into cint001_execution_results(run_id,result_scope,metrics)
            values (%s,'validation_60m_delay_24h_v2',%s)
            on conflict (run_id,result_scope)
            do update set metrics=excluded.metrics,created_at=now()
            """,
            (run_id, Jsonb(metrics)),
        )
        cur.execute(
            """
            update cint001_execution_runs
               set status='completed',stage='completed',completed_at=now(),updated_at=now(),
                   result_summary=%s
             where id=%s
            """,
            (Jsonb(metrics), run_id),
        )
        conn.commit()
    _complete(item["id"], int(metrics.get("selected_asset_observations") or 0), metrics)


def _execution_metrics(run_id: UUID) -> dict[str, Any]:
    coverage = fetch_one(
        """
        select count(*) selected_asset_observations,
               count(*) filter(where futures_short_return is not null) executable_asset_observations,
               count(distinct spot_symbol) selected_symbols,
               count(distinct spot_symbol) filter(where futures_short_return is not null) executable_symbols,
               count(distinct signal_bucket) signal_timestamps,
               count(distinct signal_bucket)
                   filter(where executable_count=selected_count and panel_member_count=%s) strict_timestamps
        from cint001_execution_trades where run_id=%s
        """,
        (EXPECTED_VALIDATION_MEMBERS, run_id),
    ) or {}
    phase = fetch_one(
        """
        with timestamp_returns as (
            select signal_bucket,phase,avg(gross_relative_return) gross_relative,
                   bool_and(executable_count=selected_count and panel_member_count=%s) strict
            from cint001_execution_trades
            where run_id=%s and gross_relative_return is not null
            group by signal_bucket,phase
        ), strict as (
            select * from timestamp_returns where strict
        ), phase_stats as (
            select phase,count(*) observations,avg(gross_relative) mean_ret,
                   percentile_cont(.5) within group(order by gross_relative) median_ret,
                   avg((gross_relative>0)::int) hit
            from strict group by phase
        )
        select count(*) phases,avg(observations) avg_phase_observations,avg(mean_ret) avg_phase_mean,
               percentile_cont(.5) within group(order by mean_ret) median_phase_mean,
               min(mean_ret) worst_phase,max(mean_ret) best_phase,
               avg((mean_ret>0)::int) positive_phase_fraction,
               avg((median_ret>0)::int) positive_median_fraction,avg(hit) avg_hit
        from phase_stats
        """,
        (EXPECTED_VALIDATION_MEMBERS, run_id),
    ) or {}
    daily = fetch_one(
        """
        with timestamp_returns as (
            select signal_bucket,avg(gross_relative_return) gross_relative,
                   bool_and(executable_count=selected_count and panel_member_count=%s) strict
            from cint001_execution_trades
            where run_id=%s and gross_relative_return is not null
            group by signal_bucket
        ), d as (
            select signal_bucket::date d,avg(gross_relative) ret
            from timestamp_returns where strict group by 1
        )
        select count(*) days,avg(ret) mean_daily,
               percentile_cont(.5) within group(order by ret) median_daily,
               stddev_samp(ret) sd_daily,avg((ret>0)::int) positive_day_fraction,
               min(ret) worst_day,max(ret) best_day
        from d
        """,
        (EXPECTED_VALIDATION_MEMBERS, run_id),
    ) or {}
    economics = fetch_one(
        """
        select avg(futures_short_return) avg_short_price_return,
               avg(funding_return) avg_short_funding_return,
               avg(panel_long_return) avg_panel_long_return,
               avg(gross_relative_return) avg_asset_level_gross_relative,
               percentile_cont(.5) within group(order by entry_basis_bps) median_entry_basis_bps,
               percentile_cont(.95) within group(order by abs(entry_basis_bps)) p95_abs_entry_basis_bps,
               percentile_cont(.5) within group(order by exit_basis_bps) median_exit_basis_bps
        from cint001_execution_trades
        where run_id=%s and gross_relative_return is not null
        """,
        (run_id,),
    ) or {}
    data_quality = fetch_one(
        """
        with signal_counts as (
            select bucket_start,count(*) members
            from cint001_spot_15m
            where symbol=any(%s) and minute_count=15
              and bucket_start >= %s and bucket_start < %s
            group by 1
        )
        select count(*) complete_signal_buckets,
               min(members) min_members,max(members) max_members,
               avg((members=%s)::int) exact_member_fraction
        from signal_counts
        """,
        (
            list(VALIDATION_ACTIVE_UNIVERSE),
            VALIDATION_START,
            VALIDATION_SIGNAL_END,
            EXPECTED_VALIDATION_MEMBERS,
        ),
    ) or {}
    selected = int(coverage.get("selected_asset_observations") or 0)
    executable = int(coverage.get("executable_asset_observations") or 0)
    signals = int(coverage.get("signal_timestamps") or 0)
    strict = int(coverage.get("strict_timestamps") or 0)
    metrics = {**coverage, **phase, **daily, **economics, **data_quality}
    metrics["asset_execution_coverage"] = executable / selected if selected else None
    metrics["strict_timestamp_coverage"] = strict / signals if signals else None
    sd = daily.get("sd_daily")
    n = int(daily.get("days") or 0)
    mean = daily.get("mean_daily")
    metrics["daily_naive_t"] = (
        float(mean) / (float(sd) / math.sqrt(n))
        if mean is not None and sd not in (None, 0) and n > 1
        else None
    )
    metrics["fees_spread_slippage_status"] = (
        "NOT_YET_INCLUDED: requires historical USD-M bookTicker and frozen fee tier; holdout remains sealed"
    )
    metrics["holdout_opened"] = False
    metrics["signal_source"] = "cint001_spot_15m materialised from market_bars_1m_binance"
    metrics["expected_validation_members"] = EXPECTED_VALIDATION_MEMBERS
    return metrics


def advance_execution_run(run_id: UUID) -> None:
    _queue_analysis_if_ready(run_id)


def process_execution_work(item: dict[str, Any]) -> None:
    try:
        if item["stage"] == "month":
            _materialise_spot_month(item)
            _process_futures_month(item)
        elif item["stage"] == "analysis":
            _process_analysis(item)
        else:
            raise ValueError(f"Unknown C-INT-001 execution stage: {item['stage']}")
        advance_execution_run(UUID(str(item["run_id"])))
    except Exception as exc:
        _fail(item, exc)
        raise
