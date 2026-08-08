-- v3.4.0: requested_end is exclusive, so equality with discovery_start is non-overlapping.
alter table public.crypto_b001_replication_runs
    drop constraint if exists crypto_b001_replication_runs_check1;

alter table public.crypto_b001_replication_runs
    add constraint crypto_b001_replication_runs_before_discovery
    check (requested_end <= discovery_start);
