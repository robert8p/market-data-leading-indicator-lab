create or replace function public.set_crypto_derivatives_partition_identity_v1()
returns trigger
language plpgsql
security invoker
set search_path=pg_catalog,public,pg_temp
as $$
declare
    v_instrument_id uuid;
    v_canonical text;
begin
    if new.provider='binance_futures' and new.data_type='crypto_derivatives' and new.instrument_id is null then
        v_canonical:=upper(coalesce(nullif(new.cursor->>'canonical_symbol',''),new.provider_symbol));
        select i.id into v_instrument_id
        from public.instruments i
        where i.provider='binance'
          and i.asset_class='crypto_spot'
          and i.preferred=true
          and upper(i.canonical_symbol)=v_canonical
        order by i.priority desc,i.provider_symbol
        limit 1;
        if v_instrument_id is null then
            raise exception 'No preferred Binance spot instrument identity for crypto derivatives base %',v_canonical;
        end if;
        new.instrument_id:=v_instrument_id;
    end if;
    return new;
end;
$$;

drop trigger if exists trg_crypto_derivatives_partition_identity_v1 on public.collection_partitions;
create trigger trg_crypto_derivatives_partition_identity_v1
before insert or update of provider,data_type,instrument_id,provider_symbol,cursor
on public.collection_partitions
for each row execute function public.set_crypto_derivatives_partition_identity_v1();

-- Repair any pristine pre-fix derivative partition without an identity.
update public.collection_partitions cp
set instrument_id=(
        select x.id
        from public.instruments x
        where x.provider='binance'
          and x.asset_class='crypto_spot'
          and x.preferred=true
          and upper(x.canonical_symbol)=upper(coalesce(nullif(cp.cursor->>'canonical_symbol',''),cp.provider_symbol))
        order by x.priority desc,x.provider_symbol
        limit 1
    ),
    updated_at=now()
where cp.provider='binance_futures'
  and cp.data_type='crypto_derivatives'
  and cp.instrument_id is null
  and cp.status='queued'
  and cp.attempts=0
  and cp.row_count=0
  and exists(
        select 1 from public.instruments x
        where x.provider='binance'
          and x.asset_class='crypto_spot'
          and x.preferred=true
          and upper(x.canonical_symbol)=upper(coalesce(nullif(cp.cursor->>'canonical_symbol',''),cp.provider_symbol))
    );

-- Reconstruct the intended derivative family for the active 30-day mining run.
with run as (
    select id,start_ts,end_ts
    from public.collection_runs
    where id='0f335c0d-1a11-473a-aa48-58111fac20f0'::uuid
      and status in ('queued','running','completed')
), crypto_rows as (
    select distinct on (canonical_symbol)
           id as instrument_id,canonical_symbol,provider_symbol,priority
    from public.instruments
    where provider='binance' and asset_class='crypto_spot' and preferred=true
    order by canonical_symbol,priority desc
    limit 300
)
insert into public.collection_partitions(
    run_id,provider,instrument_id,provider_symbol,data_type,start_ts,end_ts,status,priority,max_attempts,cursor
)
select r.id,'binance_futures',c.instrument_id,c.canonical_symbol,'crypto_derivatives',r.start_ts,r.end_ts,
       'queued',9500000,8,
       jsonb_build_object(
           'canonical_symbol',c.canonical_symbol,
           'partition_identity_fix','v1',
           'retention_sensitive_recovery',true,
           'priority_reason','Binance derivative history retention is time-limited; preserve available observations before non-expiring enrichment'
       )
from run r cross join crypto_rows c
on conflict do nothing;

update public.collection_partitions
set priority=9500000,
    cursor=coalesce(cursor,'{}'::jsonb)||jsonb_build_object(
        'retention_sensitive_recovery',true,
        'priority_reason','Binance derivative history retention is time-limited; preserve available observations before non-expiring enrichment'
    ),
    updated_at=now()
where run_id='0f335c0d-1a11-473a-aa48-58111fac20f0'::uuid
  and provider='binance_futures'
  and data_type='crypto_derivatives'
  and status in ('queued','retry_wait');

select public.refresh_collection_run_counts('0f335c0d-1a11-473a-aa48-58111fac20f0'::uuid);

insert into research_hub.research_findings(
    finding_key,finding_type,title,statement,status,evidence,source_run_keys,source_candidate_ids,reusable,propagation_targets
)
values(
    'FIND-CRYPTO-DERIV-PARTITION-IDENTITY-20260812','data_quality',
    'Crypto derivatives enrichment partitions were collapsed by a null instrument identity',
    'The enrichment planner intended one binance_futures crypto_derivatives partition per preferred Binance canonical base, but inserted them with instrument_id=NULL. The collection_partitions uniqueness key therefore collapsed the intended 300-symbol family to one partition. A database identity guard now maps each derivative partition to its preferred Binance spot instrument ID before uniqueness evaluation, the pristine existing partition was repaired, and missing partitions were seeded into the active 30-day run. The recovery family is temporarily prioritized because upstream derivative history is retention-limited. Recovered coverage must be measured explicitly and missing older observations must never be imputed.',
    'active',
    jsonb_build_object(
        'active_run_id','0f335c0d-1a11-473a-aa48-58111fac20f0',
        'intended_bases',300,
        'pre_fix_partitions',1,
        'pre_fix_existing_symbol','0G',
        'existing_partition_attempts',0,
        'existing_partition_rows',0,
        'identity_rule','preferred Binance spot instrument id',
        'recovery_priority',9500000,
        'coverage_must_be_measured_after_collection',true
    ),
    array[]::text[],array[]::text[],true,array['ARCH-PRO','METHOD','XAL','EXEC-SIGNAL']
)
on conflict(finding_key) do update set
    statement=excluded.statement,status=excluded.status,evidence=excluded.evidence,
    propagation_targets=excluded.propagation_targets,updated_at=now();