-- Freeze the successful 2026-08-12 10:18 UTC recovery snapshot and replace the
-- unsafe 7-day/500-row pagination with the validated 36-hour sub-window contract.
-- Existing recovered observations are retained: the destination PK makes the corrected
-- pass idempotent and fills gaps without imputing data that Binance no longer retains.

DO $$
DECLARE
    v_current_run uuid;
    v_duplicate_run uuid;
BEGIN
    select id into v_duplicate_run
    from public.collection_runs
    where name='Binance Derivatives Metrics Recovery 30D 2026-08-12T10:10Z'
    order by created_at desc
    limit 1;

    if v_duplicate_run is not null then
        update public.collection_runs
        set status='cancelled',
            error='Superseded by frozen 10:18 UTC recovery snapshot; no research use.',
            updated_at=now(),
            completed_at=coalesce(completed_at,now())
        where id=v_duplicate_run;
        update public.collection_partitions
        set status='cancelled',locked_by=null,locked_at=null,not_before=null,updated_at=now()
        where run_id=v_duplicate_run and status not in ('completed','completed_empty','cancelled');
    end if;

    select id into v_current_run
    from public.collection_runs
    where name='Binance Derivatives Metrics Recovery 30D 2026-08-12T10:18Z'
    order by created_at desc
    limit 1;

    if v_current_run is null then
        insert into public.collection_runs(
            name,status,stage,start_ts,end_ts,providers,config,
            planned_partitions,completed_partitions,failed_partitions,skipped_partitions,rows_written,
            enhancement_requested,started_at,updated_at
        ) values (
            'Binance Derivatives Metrics Recovery 30D 2026-08-12T10:18Z',
            'running','enrichment',
            '2026-07-13 10:18:00+00'::timestamptz,
            '2026-08-12 10:18:00+00'::timestamptz,
            array['binance']::text[],
            jsonb_build_object(
                'source','binance_rest_latest_30d',
                'purpose','retention_sensitive_binance_derivative_metrics_recovery',
                'created_by','chatgpt_pro_research_operating_layer',
                'observability_contract','binance-usdm-observability-v1',
                'recovery_version','36h-subwindow-v3-full-partition',
                'missing_history_must_not_be_imputed',true
            ),
            0,0,0,0,0,true,now(),now()
        ) returning id into v_current_run;
    else
        update public.collection_runs
        set status=case when status='cancelled' then 'running' else status end,
            stage='enrichment',
            config=coalesce(config,'{}'::jsonb)||jsonb_build_object(
                'recovery_version','36h-subwindow-v3-full-partition',
                'pagination_contract','36h segments; <=432 expected five-minute observations per endpoint request',
                'missing_history_must_not_be_imputed',true
            ),
            updated_at=now()
        where id=v_current_run;
    end if;

    insert into public.collection_partitions(
        run_id,provider,instrument_id,provider_symbol,data_type,start_ts,end_ts,
        status,priority,max_attempts,cursor
    )
    select
        v_current_run,'binance_futures',i.id,i.canonical_symbol,'crypto_derivatives',
        '2026-07-13 10:18:00+00'::timestamptz,
        '2026-08-12 10:18:00+00'::timestamptz,
        'queued',9600000,12,
        jsonb_build_object(
            'canonical_symbol',i.canonical_symbol,
            'retention_sensitive_recovery',true,
            'recovery_snapshot','current_30d_20260812T1018Z',
            'priority_reason','Current full 30-day derivative window takes precedence over legacy-window recovery',
            'observability_contract','binance-usdm-observability-v1',
            'recovery_window_version','36h-subwindow-v3-full-partition',
            'recovery_cursor_ts','2026-07-13 10:18:00+00'::timestamptz,
            'recovery_rows_written',0,
            'pagination_repair_required',true
        )
    from (
        select distinct on (canonical_symbol)
               id,canonical_symbol,priority
        from public.instruments
        where provider='binance'
          and asset_class='crypto_spot'
          and preferred=true
        order by canonical_symbol,priority desc,id
        limit 300
    ) i
    on conflict do nothing;

    -- Invalidate only checkpoints produced before the corrected pagination contract.
    -- Observations already in crypto_derivatives_metrics remain and will be upserted/fill-gapped.
    update public.collection_partitions
    set status='queued',attempts=0,max_attempts=12,row_count=0,priority=9600000,
        locked_by=null,locked_at=null,not_before=null,last_error=null,error_code=null,
        cursor=jsonb_strip_nulls(
            (coalesce(cursor,'{}'::jsonb)
             -'finished'-'recovery_cursor_ts'-'coverage_start'-'coverage_end'
             -'coverage_truncated_by_retention'-'recovery_rows_written')
            ||jsonb_build_object(
                'recovery_cursor_ts',start_ts,
                'recovery_rows_written',0,
                'recovery_window_version','36h-subwindow-v3-full-partition',
                'pagination_repair_required',true,
                'previous_completion_invalidated',true
            )
        ),
        updated_at=now()
    where run_id=v_current_run
      and provider='binance_futures'
      and data_type='crypto_derivatives'
      and coalesce(cursor->>'recovery_window_version','')<>'36h-subwindow-v3-full-partition';

    update public.collection_runs
    set planned_partitions=(
            select count(*) from public.collection_partitions
            where run_id=v_current_run and provider='binance_futures' and data_type='crypto_derivatives'
        ),
        updated_at=now()
    where id=v_current_run;
END $$;

create or replace function research_hub.claim_crypto_derivatives_recovery_partition_v1(p_worker_id text)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_row public.collection_partitions%rowtype;
    v_lease research_hub.recovery_lane_leases%rowtype;
begin
    select * into v_lease
    from research_hub.recovery_lane_leases
    where lane_key='binance_derivatives_recovery_v1'
    for update;
    if v_lease.next_allowed_at>now() then
        return jsonb_build_object('status','rate_limited','next_allowed_at',v_lease.next_allowed_at);
    end if;
    if exists(
        select 1 from public.collection_partitions
        where provider='binance_futures' and data_type='crypto_derivatives'
          and coalesce((cursor->>'retention_sensitive_recovery')::boolean,false)=true
          and status='running' and locked_by='supabase-edge:binance-deriv-v1'
    ) then
        return jsonb_build_object('status','busy');
    end if;

    with candidate as (
        select cp.id
        from public.collection_partitions cp
        join public.collection_runs cr on cr.id=cp.run_id
        where cp.provider='binance_futures'
          and cp.data_type='crypto_derivatives'
          and coalesce((cp.cursor->>'retention_sensitive_recovery')::boolean,false)=true
          and cp.status in ('queued','retry_wait')
          and (cp.not_before is null or cp.not_before<=now())
          and cp.attempts<cp.max_attempts
          and cr.status in ('queued','running')
        order by cp.end_ts desc,cp.priority desc,cp.provider_symbol,cp.id
        for update of cp skip locked
        limit 1
    )
    update public.collection_partitions cp
    set status='running',locked_by='supabase-edge:binance-deriv-v1',locked_at=now(),heartbeat_at=now(),
        attempts=attempts+1,updated_at=now(),last_error=null,error_code=null
    from candidate c where cp.id=c.id
    returning cp.* into v_row;

    if v_row.id is null then return jsonb_build_object('status','no_work'); end if;
    update research_hub.recovery_lane_leases
    set next_allowed_at=now()+interval '45 seconds',last_claimed_at=now(),claims=claims+1,updated_at=now()
    where lane_key='binance_derivatives_recovery_v1';

    return jsonb_build_object(
        'status','claimed','id',v_row.id,'run_id',v_row.run_id,'instrument_id',v_row.instrument_id,
        'canonical_symbol',coalesce(v_row.cursor->>'canonical_symbol',v_row.provider_symbol),
        'provider_symbol',v_row.provider_symbol,'start_ts',v_row.start_ts,'end_ts',v_row.end_ts,
        'attempts',v_row.attempts,'max_attempts',v_row.max_attempts,'row_count',v_row.row_count,
        'cursor',coalesce(v_row.cursor,'{}'::jsonb)
    );
end;
$$;

create or replace function research_hub.refresh_binance_deriv_recovery_program_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_legacy_run uuid := '0f335c0d-1a11-473a-aa48-58111fac20f0'::uuid;
    v_current_run uuid;
    v_total bigint := 0; v_completed bigint := 0; v_running bigint := 0; v_queued bigint := 0;
    v_failed bigint := 0; v_rows bigint := 0; v_pct double precision := 0;
    v_checkpoint timestamptz; v_current_start timestamptz; v_current_end timestamptz;
    v_legacy_start timestamptz; v_legacy_end timestamptz;
begin
    select cr.id into v_current_run
    from public.collection_runs cr
    where cr.name like 'Binance Derivatives Metrics Recovery 30D %'
      and cr.status<>'cancelled'
    order by cr.created_at desc
    limit 1;

    select count(*),
           count(*) filter(where cp.status in ('completed','completed_empty')),
           count(*) filter(where cp.status='running'),
           count(*) filter(where cp.status in ('queued','retry_wait')),
           count(*) filter(where cp.status='failed'),
           coalesce(sum(cp.row_count),0),
           max(cp.updated_at) filter(where cp.status in ('completed','completed_empty'))
    into v_total,v_completed,v_running,v_queued,v_failed,v_rows,v_checkpoint
    from public.collection_partitions cp
    where cp.provider='binance_futures'
      and cp.data_type='crypto_derivatives'
      and (cp.run_id=v_legacy_run or cp.run_id=v_current_run);

    if v_current_run is not null then
        select min(cp.start_ts),max(cp.end_ts) into v_current_start,v_current_end
        from public.collection_partitions cp
        where cp.run_id=v_current_run and cp.provider='binance_futures' and cp.data_type='crypto_derivatives';
    end if;
    select min(cp.start_ts),max(cp.end_ts) into v_legacy_start,v_legacy_end
    from public.collection_partitions cp
    where cp.run_id=v_legacy_run and cp.provider='binance_futures' and cp.data_type='crypto_derivatives';

    v_pct:=case when v_total>0 then 100.0*v_completed/v_total else 0 end;
    update research_hub.program_jobs
    set current_state=case
            when v_failed>0 then 'recovery_with_failed_partitions'
            when v_total>0 and v_completed=v_total then 'completed_quality_audit_required'
            when v_running>0 then 'running_retention_sensitive_recovery'
            else 'queued_retention_sensitive_recovery' end,
        latest_successful_checkpoint=coalesce(v_checkpoint,latest_successful_checkpoint),
        progress_current=v_completed,progress_total=v_total,completion_pct=v_pct,
        latest_result=jsonb_build_object(
            'total_partitions',v_total,'completed_partitions',v_completed,'running_partitions',v_running,
            'queued_partitions',v_queued,'failed_partitions',v_failed,'rows_written',v_rows,
            'legacy_run_id',v_legacy_run,'legacy_start',v_legacy_start,'legacy_end',v_legacy_end,
            'current_30d_run_id',v_current_run,'current_30d_start',v_current_start,'current_30d_end',v_current_end,
            'current_window_prioritized',true,'legacy_window_recovery_preserved',true,
            'superseded_duplicate_run_cancelled','23f5c2b3-9b4e-4c23-a1a1-4f48e9a2c8d0',
            'coverage_audit_required',true,
            'recovery_version','36h-subwindow-v3-full-partition',
            'progress_scope','retained legacy + latest current recovery runs only'),
        current_error=case when v_failed>0 then v_failed||' recovery partition(s) failed bounded retries' else null end,
        retry_state=case
            when v_failed>0 then 'engineering review after bounded retries'
            when v_completed=v_total and v_total>0 then 'collection complete; coverage audit required before research use'
            when v_running>0 then 'dedicated recovery lane active'
            else 'dedicated recovery lane queued' end,
        next_automatic_action=case
            when v_completed=v_total and v_total>0 then 'Run the preregistered coverage audit before allowing derivatives features into research.'
            else 'Continue the single hardened recovery lane from durable checkpoints; preserve missing history as missing rather than imputing it.' end,
        intervention_required=false,exact_intervention=null,
        metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object(
            'recovery_version','36h-subwindow-v3-full-partition',
            'pagination_defect','Seven-day requests could return the 500-row cap and leave systematic gaps; superseded by <=36h request segments.',
            'research_use_requires_quality_audit',true),
        updated_at=now()
    where job_key='SOURCE-BINANCE-DERIV-METRICS-RECOVERY-V1';

    return jsonb_build_object('total',v_total,'completed',v_completed,'running',v_running,'queued',v_queued,
        'failed',v_failed,'rows_written',v_rows,'completion_pct',v_pct,
        'legacy_run_id',v_legacy_run,'current_30d_run_id',v_current_run);
end;
$$;

create or replace function research_hub.invoke_binance_derivatives_recovery_edge_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,net,pg_temp
as $$
declare
    v_progress jsonb; v_request_id bigint; v_jobid bigint;
begin
    v_progress:=research_hub.refresh_binance_deriv_recovery_program_v1();
    if coalesce((v_progress->>'total')::bigint,0)>0
       and coalesce((v_progress->>'completed')::bigint,0)+coalesce((v_progress->>'failed')::bigint,0)>=coalesce((v_progress->>'total')::bigint,0) then
        select jobid into v_jobid from cron.job where jobname='research_hub_binance_deriv_recovery_edge_v1' limit 1;
        if v_jobid is not null then perform cron.unschedule(v_jobid); end if;
        return jsonb_build_object('status','terminal_unscheduled','progress',v_progress);
    end if;
    if exists(
        select 1 from public.collection_partitions
        where provider='binance_futures' and data_type='crypto_derivatives'
          and coalesce((cursor->>'retention_sensitive_recovery')::boolean,false)=true
          and status='running' and locked_by='supabase-edge:binance-deriv-v1'
    ) then
        return jsonb_build_object('status','busy','progress',v_progress);
    end if;
    select net.http_post(
        url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/binance-derivatives-recovery-v1',
        body:='{}'::jsonb,
        headers:=jsonb_build_object('content-type','application/json'),
        timeout_milliseconds:=120000
    ) into v_request_id;
    return jsonb_build_object('status','requested','request_id',v_request_id,'progress',v_progress);
end;
$$;

revoke all on function research_hub.claim_crypto_derivatives_recovery_partition_v1(text) from public,anon,authenticated;
revoke all on function research_hub.refresh_binance_deriv_recovery_program_v1() from public,anon,authenticated;
revoke all on function research_hub.invoke_binance_derivatives_recovery_edge_v1() from public,anon,authenticated;

DO $$
BEGIN
    if exists(select 1 from cron.job where jobname='research_hub_binance_deriv_recovery_edge_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_binance_deriv_recovery_edge_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_binance_deriv_recovery_edge_v1',
        '* * * * *',
        'select research_hub.invoke_binance_derivatives_recovery_edge_v1();'
    );
END $$;

select research_hub.refresh_binance_deriv_recovery_program_v1();
