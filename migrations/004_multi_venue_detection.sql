-- v3.2.0: ranked multi-venue dynamic crypto candidate detection.

alter table crypto_capture_targets
    add column if not exists priority_score double precision not null default 0,
    add column if not exists last_observed_at timestamptz,
    add column if not exists confirmation_count integer not null default 1,
    add column if not exists trigger_type text;

create index if not exists crypto_capture_targets_rank_idx
    on crypto_capture_targets(expires_at, priority_score desc, last_observed_at desc);

create table if not exists crypto_dynamic_detections (
    id bigserial primary key,
    canonical_symbol text not null,
    detected_at timestamptz not null,
    score double precision not null,
    trigger_type text not null,
    admitted boolean not null,
    provider_count integer not null default 1,
    spot_provider_count integer not null default 0,
    derivatives_provider_count integer not null default 0,
    reason jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now()
);

create index if not exists crypto_dynamic_detections_symbol_ts_idx
    on crypto_dynamic_detections(canonical_symbol, detected_at desc);
create index if not exists crypto_dynamic_detections_admitted_ts_idx
    on crypto_dynamic_detections(admitted, detected_at desc);
create index if not exists crypto_dynamic_detections_ts_brin
    on crypto_dynamic_detections using brin(detected_at);
