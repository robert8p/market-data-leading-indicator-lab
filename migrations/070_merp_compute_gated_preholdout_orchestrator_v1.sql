-- Compute-gated MERP pre-holdout robustness queue.
-- Heavy work runs only when no >30s B-001/XAL/MERP-heavy session is active.
-- Every invocation asserts the holdout is still sealed and unopened.
-- One cache symbol or one candidate is processed per clean compute slot.

create table if not exists research_hub.merp_preholdout_work_v1(
    work_key text primary key,
    work_type text not null,
    object_key text not null,
    priority integer not null,
    status text not null default 'queued',
    attempts integer not null default 0,
    max_attempts integer not null default 4,
    last_error text,
    result jsonb not null default '{}'::jsonb,
    started_at timestamptz,
    completed_at timestamptz,
    updated_at timestamptz not null default now(),
    unique(work_type,object_key)
);
revoke all on table research_hub.merp_preholdout_work_v1 from public,anon,authenticated;

insert into research_hub.merp_preholdout_work_v1(work_key,work_type,object_key,priority,status)
select 'cache:'||x,'cache_symbol',x,100,'queued'
from unnest(array['1000SATSUSDT','API3USDT','PHAUSDT','PSGUSDT','UMAUSDT']) x
on conflict(work_key) do nothing;

insert into research_hub.merp_preholdout_work_v1(work_key,work_type,object_key,priority,status)
select 'candidate:'||candidate_id,'candidate_robustness',candidate_id,50,'queued'
from research_hub.candidate_ledger
where run_id='56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid
  and status='STANDARD_ROBUSTNESS_REQUIRED'
on conflict(work_key) do nothing;

create or replace function research_hub.merp_preholdout_compute_pressure_v1()
returns jsonb
language sql
security invoker
stable
set search_path=pg_catalog,pg_temp
as $$
with busy as (
    select pid,now()-query_start elapsed,left(query,120) q
    from pg_stat_activity
    where pid<>pg_backend_pid()
      and state='active'
      and now()-query_start>interval '30 seconds'
      and (
          query ilike '%xal_lag_network%'
          or query ilike '%crypto_b001%'
          or query ilike '%b001_%replication%'
          or query ilike '%materialize_merp_preholdout%'
          or query ilike '%run_merp_candidate_robustness%'
      )
)
select jsonb_build_object(
    'busy',exists(select 1 from busy),
    'active_heavy_sessions',coalesce((
        select jsonb_agg(jsonb_build_object(
            'pid',pid,'elapsed_seconds',extract(epoch from elapsed),'query',q
        )) from busy
    ),'[]'::jsonb)
)
$$;
revoke all on function research_hub.merp_preholdout_compute_pressure_v1() from public,anon,authenticated;

create or replace function research_hub.advance_merp_preholdout_robustness_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_pressure jsonb;
    v_work research_hub.merp_preholdout_work_v1%rowtype;
    v_result jsonb;
    v_rows bigint;
    v_pending_cache bigint;
    v_done_candidates bigint;
    v_pass bigint;
    v_reject bigint;
begin
    if not pg_try_advisory_xact_lock(hashtext('MERP-CR-20260811-001-preholdout-robustness')::bigint) then
        return jsonb_build_object('status','busy_lock');
    end if;

    if exists(
        select 1 from research_hub.ai_experiment_registry
        where run_key='MERP-CR-20260811-001'
          and (not holdout_sealed or holdout_opened_at is not null or coalesce((latest_result->>'holdout_rows')::bigint,0)>0)
    ) or exists(
        select 1 from research_hub.experiment_runs
        where run_id='56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid
          and (not holdout_sealed or holdout_opened_at is not null or coalesce((latest_result->>'holdout_rows')::bigint,0)>0)
    ) then
        raise exception 'MERP holdout boundary violated: pre-holdout lane refuses to run';
    end if;

    v_pressure:=research_hub.merp_preholdout_compute_pressure_v1();
    if coalesce((v_pressure->>'busy')::boolean,false) then
        return jsonb_build_object('status','waiting_for_clean_compute_slot','pressure',v_pressure);
    end if;

    select count(*) into v_pending_cache
    from research_hub.merp_preholdout_work_v1
    where work_type='cache_symbol' and status<>'completed';

    if v_pending_cache>0 then
        select * into v_work
        from research_hub.merp_preholdout_work_v1
        where work_type='cache_symbol'
          and status in ('queued','retry_wait')
          and attempts<max_attempts
        order by priority desc,object_key
        limit 1 for update skip locked;

        if v_work.work_key is null then
            return jsonb_build_object('status','cache_blocked_or_terminal_failure');
        end if;

        update research_hub.merp_preholdout_work_v1
        set status='running',attempts=attempts+1,started_at=coalesce(started_at,now()),
            updated_at=now(),last_error=null
        where work_key=v_work.work_key;

        begin
            v_rows:=research_hub.materialize_merp_preholdout_cache_symbol_v1(v_work.object_key);
            update research_hub.merp_preholdout_work_v1
            set status='completed',result=jsonb_build_object('rows',v_rows,'holdout_accessed',false),
                completed_at=now(),updated_at=now()
            where work_key=v_work.work_key;
            update research_hub.program_jobs
            set current_state='preholdout_standard_robustness_cache_build',
                retry_state='compute-gated automatic typed-cache materialization',
                next_automatic_action='Continue one cache symbol per clean compute slot; holdout remains sealed.',
                updated_at=now()
            where job_key='MERP-CR-20260811-001';
            return jsonb_build_object('status','cache_symbol_completed','instrument',v_work.object_key,'rows',v_rows);
        exception when others then
            update research_hub.merp_preholdout_work_v1
            set status=case when attempts>=max_attempts then 'failed' else 'retry_wait' end,
                last_error=left(sqlerrm,4000),updated_at=now()
            where work_key=v_work.work_key;
            return jsonb_build_object('status','cache_symbol_failed','instrument',v_work.object_key,'error',sqlerrm);
        end;
    end if;

    select * into v_work
    from research_hub.merp_preholdout_work_v1
    where work_type='candidate_robustness'
      and status in ('queued','retry_wait')
      and attempts<max_attempts
    order by priority desc,object_key
    limit 1 for update skip locked;

    if v_work.work_key is not null then
        update research_hub.merp_preholdout_work_v1
        set status='running',attempts=attempts+1,started_at=coalesce(started_at,now()),
            updated_at=now(),last_error=null
        where work_key=v_work.work_key;
        begin
            v_result:=research_hub.run_merp_candidate_robustness_v1(v_work.object_key);
            update research_hub.merp_preholdout_work_v1
            set status='completed',result=v_result,completed_at=now(),updated_at=now()
            where work_key=v_work.work_key;
            update research_hub.program_jobs
            set current_state='preholdout_standard_robustness',
                retry_state='compute-gated automatic candidate battery',
                next_automatic_action='Continue identical frozen market_edge_standard_v1 battery one candidate per clean compute slot; do not open holdout.',
                updated_at=now()
            where job_key='MERP-CR-20260811-001';
            return jsonb_build_object('status','candidate_completed','candidate_id',v_work.object_key,'result',v_result);
        exception when others then
            update research_hub.merp_preholdout_work_v1
            set status=case when attempts>=max_attempts then 'failed' else 'retry_wait' end,
                last_error=left(sqlerrm,4000),updated_at=now()
            where work_key=v_work.work_key;
            return jsonb_build_object('status','candidate_failed','candidate_id',v_work.object_key,'error',sqlerrm);
        end;
    end if;

    select count(*) filter(where status='completed' and work_type='candidate_robustness')
    into v_done_candidates from research_hub.merp_preholdout_work_v1;
    select count(*) filter(where overall_preholdout_pass),count(*) filter(where not overall_preholdout_pass)
    into v_pass,v_reject
    from research_hub.merp_standard_robustness_v1
    where run_id='56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid;

    update research_hub.program_jobs
    set current_state=case
            when exists(select 1 from research_hub.merp_preholdout_work_v1 where status='failed')
                then 'preholdout_robustness_engineering_review'
            else 'preholdout_standard_complete_holdout_still_sealed' end,
        latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object(
            'robustness_candidates_completed',v_done_candidates,
            'preholdout_standard_pass',v_pass,
            'preholdout_standard_reject',v_reject,
            'holdout_opened',false
        ),
        retry_state='terminal pre-holdout robustness state',
        next_automatic_action=case when v_pass>0
            then 'Deduplicate passing candidate families and freeze execution-replication plan before any governed holdout-opening decision.'
            else 'No candidate may open the holdout; preserve robustness rejections and continue independent research families.' end,
        updated_at=now()
    where job_key='MERP-CR-20260811-001';

    return jsonb_build_object('status','terminal','pass',v_pass,'reject',v_reject,'holdout_opened',false);
end;
$$;
revoke all on function research_hub.advance_merp_preholdout_robustness_v1() from public,anon,authenticated;

DO $$
BEGIN
    if exists(select 1 from cron.job where jobname='research_hub_merp_preholdout_robustness_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_merp_preholdout_robustness_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_merp_preholdout_robustness_v1',
        '*/10 * * * *',
        'set statement_timeout=''20min''; set work_mem=''64MB''; select research_hub.advance_merp_preholdout_robustness_v1();'
    );
END $$;
