-- v3.4.0: covering index for B-001 trade -> signal FK joins/cascades.
create index if not exists crypto_b001_trades_signal_id_idx
    on public.crypto_b001_replication_trades(signal_id);
