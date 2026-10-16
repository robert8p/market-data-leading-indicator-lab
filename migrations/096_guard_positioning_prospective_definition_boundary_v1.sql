create or replace function research_hub.guard_positioning_prospective_definition_boundary_v1()
returns trigger
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
begin
    if tg_table_schema='public'
       and tg_table_name='crypto_derivatives_metrics'
       and coalesce(new.metadata->>'source','')='binance_public_prospective_positioning_v1'
       and new.ts<'2026-08-13 00:00:00+00'::timestamptz then
        raise exception 'Prospective positioning source cannot write pre-definition derivative timestamp %',new.ts;
    end if;
    if tg_table_schema='research_hub'
       and tg_table_name='binance_spot15m_positioning_v1'
       and new.source='binance_public_prospective_positioning_v1'
       and new.bucket_start<'2026-08-13 00:00:00+00'::timestamptz then
        raise exception 'Prospective positioning source cannot write pre-definition spot timestamp %',new.bucket_start;
    end if;
    return new;
end;
$$;

drop trigger if exists crypto_derivatives_prospective_boundary_guard_v1 on public.crypto_derivatives_metrics;
create trigger crypto_derivatives_prospective_boundary_guard_v1
before insert or update on public.crypto_derivatives_metrics
for each row execute function research_hub.guard_positioning_prospective_definition_boundary_v1();

drop trigger if exists spot15m_positioning_prospective_boundary_guard_v1 on research_hub.binance_spot15m_positioning_v1;
create trigger spot15m_positioning_prospective_boundary_guard_v1
before insert or update on research_hub.binance_spot15m_positioning_v1
for each row execute function research_hub.guard_positioning_prospective_definition_boundary_v1();

update research_hub.program_jobs
set metadata=metadata||jsonb_build_object(
        'database_definition_boundary_guard',true,
        'earliest_allowed_timestamp','2026-08-13T00:00:00Z'
    ),updated_at=now()
where job_key='SOURCE-BINANCE-POSITIONING-PROSPECTIVE-V1';
