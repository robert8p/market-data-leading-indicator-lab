with source_run as (
    select id
    from public.crypto_b001_replication_runs
    where completeness_pct=100
      and complete_15m_rows>0
      and effective_start<='2025-10-01 00:00:00+00'
      and effective_end>='2026-06-01 00:00:00+00'
    order by complete_15m_rows desc,created_at desc
    limit 1
), days as (
    select d::date materialization_date
    from generate_series('2025-10-01'::date,'2026-05-31'::date,interval '1 day') g(d)
)
insert into research_hub.feature_materialization_checkpoints(
    feature_set_key,partition_key,last_source_ts,source_hash,row_count,code_version,status,last_error,metadata,updated_at
)
select
    'crypto.spot_futures15m.v1',
    to_char(days.materialization_date,'YYYY-MM-DD'),
    case when days.materialization_date='2025-10-01'::date then '2025-10-01 23:30:00+00'::timestamptz end,
    null,
    case when days.materialization_date='2025-10-01'::date then 2470 else null end,
    'crypto-spot-futures15m-typed-v1',
    case when days.materialization_date='2025-10-01'::date then 'completed' else 'queued' end,
    null,
    jsonb_build_object(
        'attempts',case when days.materialization_date='2025-10-01'::date then 1 else 0 end,
        'phase',case when days.materialization_date<'2026-04-01'::date then 'discovery' else 'validation' end,
        'spot_run_id',source_run.id,
        'outcome_set_key','crypto.binance_spot15m_returns.v1',
        'outcome_rows',case when days.materialization_date='2025-10-01'::date then 9880 else 0 end,
        'holdout_accessed',false,
        'holdout_materialized',false,
        'definition_version','crypto-spot-futures15m-typed-v1',
        'partition_start',days.materialization_date,
        'partition_end',days.materialization_date+1
    ),
    now()
from days cross join source_run
on conflict(feature_set_key,partition_key) do nothing;

create or replace function research_hub.process_next_crypto_spot_futures15m_partition_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
declare
    v_partition text;
    v_start timestamptz;
    v_end timestamptz;
    v_spot_run_id uuid;
    v_attempts integer;
    v_result jsonb;
    v_feature_rows bigint;
    v_outcome_rows bigint;
begin
    if not pg_try_advisory_xact_lock(hashtext('rh-crypto-spot-futures15m-queue-v1')::bigint) then
        return jsonb_build_object('status','busy','scope','queue');
    end if;

    select c.partition_key,
           c.partition_key::date::timestamptz,
           (c.partition_key::date+1)::timestamptz,
           nullif(c.metadata->>'spot_run_id','')::uuid,
           coalesce((c.metadata->>'attempts')::integer,0)
      into v_partition,v_start,v_end,v_spot_run_id,v_attempts
    from research_hub.feature_materialization_checkpoints c
    where c.feature_set_key='crypto.spot_futures15m.v1'
      and c.partition_key>='2025-10-01'
      and c.partition_key<='2026-05-31'
      and (
          c.status='queued'
          or (c.status='failed' and coalesce((c.metadata->>'attempts')::integer,0)<4)
      )
    order by c.partition_key
    limit 1
    for update skip locked;

    if v_partition is null then
        return jsonb_build_object('status','idle','holdout_materialized',false);
    end if;
    if v_spot_run_id is null then
        raise exception 'Checkpoint % has no spot_run_id binding',v_partition;
    end if;
    if v_start>='2026-06-01'::timestamptz or v_end>'2026-06-01'::timestamptz then
        raise exception 'Sealed holdout boundary violation attempted for partition %',v_partition;
    end if;

    update research_hub.feature_materialization_checkpoints
    set status='running',
        last_error=null,
        metadata=metadata||jsonb_build_object('attempts',v_attempts+1,'started_at',now()),
        updated_at=now()
    where feature_set_key='crypto.spot_futures15m.v1'
      and partition_key=v_partition;

    begin
        v_result:=research_hub.refresh_crypto_spot_futures15m_typed_v1(v_spot_run_id,v_start,v_end);
        if coalesce(v_result->>'status','')='busy' then
            update research_hub.feature_materialization_checkpoints
            set status='queued',
                last_error='Typed materializer busy; safe retry scheduled.',
                metadata=metadata||jsonb_build_object('attempts',v_attempts,'last_result',v_result),
                updated_at=now()
            where feature_set_key='crypto.spot_futures15m.v1'
              and partition_key=v_partition;
            return jsonb_build_object('status','busy','partition_key',v_partition,'holdout_materialized',false);
        end if;

        v_feature_rows:=coalesce((v_result->>'feature_rows_upserted')::bigint,0);
        v_outcome_rows:=coalesce((v_result->>'outcome_rows_upserted')::bigint,0);

        update research_hub.feature_materialization_checkpoints
        set status='completed',
            row_count=v_feature_rows,
            last_source_ts=v_end-interval '30 minutes',
            code_version='crypto-spot-futures15m-typed-v1',
            last_error=null,
            metadata=metadata||jsonb_build_object(
                'outcome_rows',v_outcome_rows,
                'last_result',v_result,
                'completed_at',now(),
                'holdout_accessed',false,
                'holdout_materialized',false
            ),
            updated_at=now()
        where feature_set_key='crypto.spot_futures15m.v1'
          and partition_key=v_partition;

        return jsonb_build_object(
            'status','completed_partition',
            'partition_key',v_partition,
            'feature_rows',v_feature_rows,
            'outcome_rows',v_outcome_rows,
            'holdout_materialized',false
        );
    exception when others then
        update research_hub.feature_materialization_checkpoints
        set status=case when v_attempts+1>=4 then 'failed' else 'queued' end,
            last_error=left(sqlerrm,4000),
            metadata=metadata||jsonb_build_object('last_failure_at',now()),
            updated_at=now()
        where feature_set_key='crypto.spot_futures15m.v1'
          and partition_key=v_partition;
        return jsonb_build_object(
            'status',case when v_attempts+1>=4 then 'failed' else 'retry_queued' end,
            'partition_key',v_partition,
            'error',sqlerrm,
            'holdout_materialized',false
        );
    end;
end;
$$;

revoke all on function research_hub.process_next_crypto_spot_futures15m_partition_v1()
    from public,anon,authenticated;

comment on function research_hub.process_next_crypto_spot_futures15m_partition_v1() is
'Claims one Oct-2025 through May-2026 typed spot/futures materialization day from the shared Research Hub checkpoint ledger. June 2026 is never queued or materialized, preserving the sealed holdout. Retries are idempotent and bounded.';

do $$
begin
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures15m_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures15m_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_crypto_spot_futures15m_v1',
        '1,11,31,41 * * * *',
        'set work_mem=''64MB''; set statement_timeout=''5min''; select research_hub.process_next_crypto_spot_futures15m_partition_v1();'
    );
end $$;