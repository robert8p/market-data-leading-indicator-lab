from __future__ import annotations

"""Cheap, exact B-001 progress accounting for the long historical backfill.

Release-time hardening made progress counters exact to the requested test window,
but did so by repeatedly counting and sorting the entire growing 15-minute table.
With parallel monthly ingestion that became an O(N) query after every batch and
started timing out around the multi-million-row mark.

The archive ledger already stores per-month complete 15-minute counts.  The only
rows in those counts outside the requested test window are the fixed liquidity
lookback immediately before requested_start.  Subtract that small indexed slice,
and obtain first/last requested-window bars with two index LIMIT lookups.  This
keeps the same semantics without rescanning the full research table.
"""

from uuid import UUID

import app.b001_replication as replication
from app.b001_contract import LIQUIDITY_LOOKBACK_DAYS
from app.db import db_connection


def refresh_run_stats_fast(run_id: UUID) -> None:
    lookback_days = LIQUIDITY_LOOKBACK_DAYS + 1
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with run_window as (
                select requested_start,requested_end
                  from crypto_b001_replication_runs
                 where id=%s
            ), archives as (
                select count(*) planned,
                       count(*) filter(where source_status='loaded') completed,
                       count(*) filter(where source_status='missing') missing,
                       coalesce(sum(rows_in_replication_window)
                           filter(where source_status='loaded'),0)::bigint minute_rows,
                       coalesce(sum(complete_15m_count)
                           filter(where source_status='loaded'),0)::bigint complete15_with_lookback,
                       coalesce(sum(incomplete_15m_count)
                           filter(where source_status='loaded'),0)::bigint incomplete15,
                       count(distinct symbol)
                           filter(where source_status='loaded')::int symbols
                  from crypto_b001_replication_archive_files
                 where run_id=%s
            ), pretest as (
                select count(*)::bigint n
                  from crypto_b001_replication_15m b
                  cross join run_window w
                 where b.run_id=%s
                   and b.bucket_start >= w.requested_start-(%s * interval '1 day')
                   and b.bucket_start < w.requested_start
            ), bounds as (
                select
                    (
                        select b.bucket_start
                          from crypto_b001_replication_15m b
                          cross join run_window w
                         where b.run_id=%s
                           and b.bucket_start>=w.requested_start
                           and b.bucket_start<w.requested_end
                         order by b.bucket_start asc
                         limit 1
                    ) first_bucket,
                    (
                        select b.bucket_start
                          from crypto_b001_replication_15m b
                          cross join run_window w
                         where b.run_id=%s
                           and b.bucket_start>=w.requested_start
                           and b.bucket_start<w.requested_end
                         order by b.bucket_start desc
                         limit 1
                    ) last_bucket
            )
            update crypto_b001_replication_runs r set
                archive_files_planned=a.planned,
                archive_files_completed=a.completed,
                archive_files_missing=a.missing,
                one_minute_rows=a.minute_rows,
                complete_15m_rows=greatest(a.complete15_with_lookback-p.n,0),
                incomplete_15m_buckets=a.incomplete15,
                symbols_loaded=a.symbols,
                effective_start=b.first_bucket,
                effective_end=least(r.requested_end,b.last_bucket+interval '15 minutes'),
                completeness_pct=case when a.planned>0 then 100.0*a.completed/a.planned else 0 end,
                updated_at=now()
            from archives a cross join pretest p cross join bounds b
            where r.id=%s
            """,
            (
                run_id,
                run_id,
                run_id,
                lookback_days,
                run_id,
                run_id,
                run_id,
            ),
        )
        conn.commit()


def install() -> None:
    # app.b001_runtime's release facade stores the expensive helper in the
    # globals dict of its advance wrapper. Replace that binding directly, and
    # also replace the helper on the base replication module because the saved
    # original advance function resolves it there at runtime.
    advance = replication.advance_b001_run
    advance.__globals__["_refresh_run_stats"] = refresh_run_stats_fast
    replication._refresh_run_stats = refresh_run_stats_fast


install()
