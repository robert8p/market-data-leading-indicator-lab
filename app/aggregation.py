from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb

from app.db import db_connection


def aggregate_equity_microstructure(partition: dict[str, Any]) -> int:
    """Create reusable one-minute SIP trade/quote facts for an admitted equity window."""
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            with classified_trades as (
                select t.ts,t.price,t.size,
                       case
                           when t.aggressor_side in ('buy','sell') then t.aggressor_side
                           when q.ask_price is not null and q.ask_price > 0 and t.price >= q.ask_price then 'buy'
                           when q.bid_price is not null and q.bid_price > 0 and t.price <= q.bid_price then 'sell'
                           when q.ask_price is not null and q.bid_price is not null
                                and q.ask_price >= q.bid_price
                                and t.price > (q.ask_price+q.bid_price)/2.0 then 'buy'
                           when q.ask_price is not null and q.bid_price is not null
                                and q.ask_price >= q.bid_price
                                and t.price < (q.ask_price+q.bid_price)/2.0 then 'sell'
                           else null
                       end as classified_side
                  from market_trades t
                  left join lateral (
                       select mq.bid_price,mq.ask_price
                         from market_quotes_l1 mq
                        where mq.provider='alpaca'
                          and mq.instrument_id=t.instrument_id
                          and mq.ts <= t.ts
                          and mq.ts >= t.ts - interval '5 seconds'
                        order by mq.ts desc
                        limit 1
                  ) q on true
                 where t.provider='alpaca' and t.instrument_id=%s and t.ts >= %s and t.ts < %s
            ), trade_stats as (
                select date_trunc('minute', ts) as minute,
                       count(*) as trade_count,
                       count(*) filter (where classified_side='buy') as buy_trade_count,
                       count(*) filter (where classified_side='sell') as sell_trade_count,
                       count(*) filter (where classified_side is null) as unknown_trade_count,
                       coalesce(sum(size) filter (where classified_side='buy'),0) as buy_volume,
                       coalesce(sum(size) filter (where classified_side='sell'),0) as sell_volume,
                       coalesce(sum(size) filter (where classified_side is null),0) as unknown_volume,
                       coalesce(sum(size),0) as total_volume,
                       coalesce(sum(price*size),0) as total_notional,
                       case when sum(size)>0 then sum(price*size)/sum(size) end as vwap,
                       (array_agg(price order by ts asc))[1] as first_trade_price,
                       (array_agg(price order by ts desc))[1] as last_trade_price,
                       max(price) as high_trade_price,
                       min(price) as low_trade_price
                  from classified_trades
                 group by 1
            ), quote_stats as (
                select date_trunc('minute', ts) as minute,
                       count(*) as quote_count,
                       avg(bid_price) as avg_bid_price,
                       avg(ask_price) as avg_ask_price,
                       avg(bid_size) as avg_bid_size,
                       avg(ask_size) as avg_ask_size,
                       avg(ask_price-bid_price) filter (where ask_price>=bid_price) as avg_spread,
                       avg(
                           case when bid_price>0 and ask_price>=bid_price
                                then (ask_price-bid_price)/((ask_price+bid_price)/2.0)*10000 end
                       ) as avg_spread_bps,
                       min(
                           case when bid_price>0 and ask_price>=bid_price
                                then (ask_price-bid_price)/((ask_price+bid_price)/2.0)*10000 end
                       ) as min_spread_bps,
                       max(
                           case when bid_price>0 and ask_price>=bid_price
                                then (ask_price-bid_price)/((ask_price+bid_price)/2.0)*10000 end
                       ) as max_spread_bps,
                       (array_agg(bid_price order by ts desc))[1] as last_bid_price,
                       (array_agg(ask_price order by ts desc))[1] as last_ask_price,
                       (array_agg(bid_size order by ts desc))[1] as last_bid_size,
                       (array_agg(ask_size order by ts desc))[1] as last_ask_size
                  from market_quotes_l1
                 where provider='alpaca' and instrument_id=%s and ts >= %s and ts < %s
                 group by 1
            ), combined as (
                select coalesce(t.minute,q.minute) as minute,
                       t.trade_count,t.buy_trade_count,t.sell_trade_count,t.unknown_trade_count,
                       t.buy_volume,t.sell_volume,t.unknown_volume,t.total_volume,t.total_notional,
                       t.vwap,t.first_trade_price,t.last_trade_price,t.high_trade_price,t.low_trade_price,
                       q.quote_count,q.avg_bid_price,q.avg_ask_price,q.avg_bid_size,q.avg_ask_size,
                       q.avg_spread,q.avg_spread_bps,q.min_spread_bps,q.max_spread_bps,
                       q.last_bid_price,q.last_ask_price,q.last_bid_size,q.last_ask_size
                  from trade_stats t full outer join quote_stats q on q.minute=t.minute
            )
            insert into equity_microstructure_1m(
                provider,instrument_id,ts,trade_count,buy_trade_count,sell_trade_count,
                unknown_trade_count,buy_volume,sell_volume,unknown_volume,total_volume,total_notional,
                vwap,first_trade_price,last_trade_price,high_trade_price,low_trade_price,
                quote_count,avg_bid_price,avg_ask_price,avg_bid_size,avg_ask_size,avg_spread,
                avg_spread_bps,min_spread_bps,max_spread_bps,last_bid_price,last_ask_price,
                last_bid_size,last_ask_size,metadata
            )
            select 'alpaca',%s,minute,
                   coalesce(trade_count,0),coalesce(buy_trade_count,0),coalesce(sell_trade_count,0),
                   coalesce(unknown_trade_count,0),coalesce(buy_volume,0),coalesce(sell_volume,0),
                   coalesce(unknown_volume,0),coalesce(total_volume,0),coalesce(total_notional,0),
                   vwap,first_trade_price,last_trade_price,high_trade_price,low_trade_price,
                   coalesce(quote_count,0),avg_bid_price,avg_ask_price,avg_bid_size,avg_ask_size,
                   avg_spread,avg_spread_bps,min_spread_bps,max_spread_bps,last_bid_price,last_ask_price,
                   last_bid_size,last_ask_size,%s
              from combined
             where minute is not null
            on conflict(provider,instrument_id,ts) do update set
                trade_count=excluded.trade_count,buy_trade_count=excluded.buy_trade_count,
                sell_trade_count=excluded.sell_trade_count,unknown_trade_count=excluded.unknown_trade_count,
                buy_volume=excluded.buy_volume,sell_volume=excluded.sell_volume,
                unknown_volume=excluded.unknown_volume,total_volume=excluded.total_volume,
                total_notional=excluded.total_notional,vwap=excluded.vwap,
                first_trade_price=excluded.first_trade_price,last_trade_price=excluded.last_trade_price,
                high_trade_price=excluded.high_trade_price,low_trade_price=excluded.low_trade_price,
                quote_count=excluded.quote_count,avg_bid_price=excluded.avg_bid_price,
                avg_ask_price=excluded.avg_ask_price,avg_bid_size=excluded.avg_bid_size,
                avg_ask_size=excluded.avg_ask_size,avg_spread=excluded.avg_spread,
                avg_spread_bps=excluded.avg_spread_bps,min_spread_bps=excluded.min_spread_bps,
                max_spread_bps=excluded.max_spread_bps,last_bid_price=excluded.last_bid_price,
                last_ask_price=excluded.last_ask_price,last_bid_size=excluded.last_bid_size,
                last_ask_size=excluded.last_ask_size,metadata=excluded.metadata,updated_at=now()
            returning 1
            """,
            (
                partition["instrument_id"], partition["start_ts"], partition["end_ts"],
                partition["instrument_id"], partition["start_ts"], partition["end_ts"],
                partition["instrument_id"], Jsonb({"run_id": str(partition["run_id"]), "source": "sip_ticks", "trade_classification": "quote_test_5s_v1"}),
            ),
        )
        count = len(cur.fetchall())
        cur.execute(
            "update collection_partitions set row_count=%s,cursor=%s,heartbeat_at=now(),updated_at=now() where id=%s",
            (count, Jsonb({"finished": True}), partition["id"]),
        )
        conn.commit()
        return count
