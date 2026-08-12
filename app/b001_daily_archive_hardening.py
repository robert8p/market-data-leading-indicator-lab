from __future__ import annotations

"""Enable current-month B-001 holdouts without weakening the frozen signal.

Binance public data publishes completed daily archives before the monthly file
exists.  The original B-001 loader discovered only monthly archives, which
left a current-month blind spot.  This runtime patch keeps monthly archives as
primary and supplements only uncovered dates with daily 1m archives.

For shortability, a current-month USD-M perpetual is accepted only when either
the monthly archive exists or the *first day of the month* daily archive exists.
That daily fallback is deliberately conservative: tokens listed later in the
month remain excluded rather than being backfilled as if they were shortable
from month start.
"""

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
from psycopg.types.json import Jsonb

import app.b001_replication as replication
from app.b001_contract import LIQUIDITY_LOOKBACK_DAYS
from app.db import db_connection, fetch_all, fetch_one


_DAILY_FILE_RE = re.compile(r"-(\d{4})-(\d{2})-(\d{2})\.zip$")
_ORIGINAL_DISCOVER_SYMBOL = replication._discover_symbol


def _discover_symbol(item: dict[str, Any]) -> None:
    # Preserve the locked monthly archive behaviour first.
    _ORIGINAL_DISCOVER_SYMBOL(item)

    symbol = str(item["payload"]["symbol"]).upper()
    run = fetch_one("select * from crypto_b001_replication_runs where id=%s", (item["run_id"],))
    if not run:
        raise RuntimeError("Replication run disappeared")
    collection_start = run["requested_start"] - timedelta(days=LIQUIDITY_LOOKBACK_DAYS + 1)

    monthly = fetch_all(
        """
        select period_start,period_end
        from crypto_b001_replication_archive_files
        where run_id=%s and symbol=%s and period_end > period_start + 1
        """,
        (item["run_id"], symbol),
    )
    monthly_ranges = [(row["period_start"], row["period_end"]) for row in monthly]

    keys, _prefixes = replication._s3_list(f"data/spot/daily/klines/{symbol}/1m/")
    planned = 0
    with db_connection() as conn, conn.cursor() as cur:
        for key in keys:
            if not key.endswith(".zip") or key.endswith(".zip.CHECKSUM"):
                continue
            match = _DAILY_FILE_RE.search(key)
            if not match:
                continue
            period_start_dt = datetime(
                int(match.group(1)), int(match.group(2)), int(match.group(3)), tzinfo=timezone.utc
            )
            period_end_dt = period_start_dt + timedelta(days=1)
            if period_end_dt <= collection_start or period_start_dt >= run["requested_end"]:
                continue
            day = period_start_dt.date()
            if any(start <= day < end for start, end in monthly_ranges):
                continue

            source_url = f"{replication.DATA_BASE}/{key}"
            payload = {
                "symbol": symbol,
                "period_start": day.isoformat(),
                "period_end": period_end_dt.date().isoformat(),
                "source_url": source_url,
                "checksum_url": source_url + ".CHECKSUM",
                "archive_granularity": "daily",
            }
            cur.execute(
                """
                insert into crypto_b001_replication_archive_files(
                    run_id,symbol,period_start,period_end,source_url,checksum_url
                ) values (%s,%s,%s,%s,%s,%s)
                on conflict (run_id,symbol,market_type,interval,period_start) do nothing
                """,
                (
                    item["run_id"], symbol, day, period_end_dt.date(),
                    source_url, source_url + ".CHECKSUM",
                ),
            )
            inserted_archive = cur.rowcount
            cur.execute(
                """
                insert into crypto_b001_replication_work_items(run_id,stage,partition_key,payload)
                values (%s,'spot_month',%s,%s) on conflict do nothing
                """,
                (item["run_id"], f"{symbol}:{day:%Y-%m-%d}", Jsonb(payload)),
            )
            if inserted_archive:
                planned += 1
        cur.execute(
            """
            update crypto_b001_replication_work_items
               set row_count=row_count+%s,
                   progress=progress || %s::jsonb,
                   updated_at=now()
             where id=%s
            """,
            (
                planned,
                Jsonb({
                    "daily_archives_added": planned,
                    "daily_archive_policy": "daily files only for dates not covered by a monthly archive",
                }),
                item["id"],
            ),
        )
        conn.commit()


def _check_shortability(item: dict[str, Any]) -> None:
    symbol = str(item["payload"]["symbol"]).upper()
    period_start = date.fromisoformat(item["payload"]["period_start"])
    month_start = period_start.replace(day=1)
    candidates = [symbol]
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    if base and not base.startswith("1000"):
        candidates.append(f"1000{base}USDT")

    available = False
    evidence = None
    evidence_granularity = None
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        for futures_symbol in candidates:
            monthly_url = (
                f"{replication.DATA_BASE}/data/futures/um/monthly/klines/{futures_symbol}/1m/"
                f"{futures_symbol}-1m-{month_start:%Y-%m}.zip"
            )
            response = client.head(monthly_url)
            if response.status_code == 200:
                available = True
                evidence = monthly_url
                evidence_granularity = "monthly"
                break

            # Conservative current-month fallback: require a futures archive on
            # the first calendar day, so later listings are never backdated.
            daily_url = (
                f"{replication.DATA_BASE}/data/futures/um/daily/klines/{futures_symbol}/1m/"
                f"{futures_symbol}-1m-{month_start:%Y-%m-%d}.zip"
            )
            response = client.head(daily_url)
            if response.status_code == 200:
                available = True
                evidence = daily_url
                evidence_granularity = "daily_month_start"
                break

    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_b001_replication_shortability(
                run_id,symbol,period_start,spot_data_present,spot_trading_status,margin_enabled,margin_evidence_source,
                perpetual_available,perpetual_evidence_source,spread_bps,spread_evidence_source,metadata
            ) values (%s,%s,%s,true,'TRADING_OBSERVED',null,'historical_margin_metadata_unavailable',%s,%s,null,
                      '1m_kline_archive_has_no_spread',%s)
            on conflict (run_id,symbol,period_start) do update set
                spot_data_present=true,spot_trading_status='TRADING_OBSERVED',perpetual_available=excluded.perpetual_available,
                perpetual_evidence_source=excluded.perpetual_evidence_source,metadata=excluded.metadata,checked_at=now()
            """,
            (
                item["run_id"], symbol, month_start, available, evidence,
                Jsonb({"evidence_granularity": evidence_granularity, "conservative_month_start_rule": True}),
            ),
        )
        cur.execute(
            """
            update crypto_b001_replication_signals s set
                spot_trading_status='TRADING_OBSERVED',margin_enabled=null,perpetual_available=%s,spread_bps=null,
                historically_executable=%s,
                shortability_reason=%s
            where s.run_id=%s and s.symbol=%s and date_trunc('month',s.bucket_start)::date=%s
            """,
            (
                available, available,
                (
                    f"Binance USD-M perpetual archive present ({evidence_granularity})"
                    if available
                    else "No month-start direct/1000x USD-M perpetual archive found; conservatively not executable"
                ),
                item["run_id"], symbol, month_start,
            ),
        )
        conn.commit()
    replication._complete(
        item["id"], 1,
        {"perpetual_available": available, "evidence": evidence, "evidence_granularity": evidence_granularity},
    )


replication._discover_symbol = _discover_symbol
replication._check_shortability = _check_shortability
