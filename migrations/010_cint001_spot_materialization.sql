-- C-INT-001 neutral spot materialisation from canonical Binance 1m history.
-- Built solely from raw market_bars_1m_binance rows. This table is deliberately
-- independent of prior B-001 feature/signal tables and of the recent research
-- materialisation that does not cover the validation period.

create table if not exists cint001_spot_15m (
    symbol text not null,
    bucket_start timestamptz not null,
    signal_ts timestamptz not null,
    minute_count integer not null,
    open double precision not null,
    high double precision not null,
    low double precision not null,
    close double precision not null,
    volume double precision not null default 0,
    quote_volume double precision not null default 0,
    trade_count bigint not null default 0,
    taker_buy_quote_volume double precision not null default 0,
    source text not null default 'market_bars_1m_binance',
    inserted_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key(symbol,bucket_start),
    check(signal_ts = bucket_start + interval '15 minutes')
);
create index if not exists cint001_spot_15m_ts_symbol_idx
    on cint001_spot_15m(bucket_start,symbol);
create index if not exists cint001_spot_15m_ts_brin
    on cint001_spot_15m using brin(bucket_start);

alter table cint001_spot_15m enable row level security;
