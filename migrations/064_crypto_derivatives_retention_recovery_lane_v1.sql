create table if not exists research_hub.recovery_lane_leases(
    lane_key text primary key,
    next_allowed_at timestamptz not null default now(),
    last_claimed_at timestamptz,
    claims bigint not null default 0,
    updated_at timestamptz not null default now()
);
insert into research_hub.recovery_lane_leases(lane_key,next_allowed_at)
values('binance_derivatives_recovery_v1',now())
on conflict(lane_key) do nothing;

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
        where run_id='0f335c0d-1a11-473a-aa48-58111fac20f0'::uuid
          and provider='binance_futures' and data_type='crypto_derivatives'
          and status='running' and locked_by='supabase-edge:binance-deriv-v1'
    ) then
        return jsonb_build_object('status','busy');
    end if;

    with candidate as (
        select cp.id
        from public.collection_partitions cp
        join public.collection_runs cr on cr.id=cp.run_id
        where cp.run_id='0f335c0d-1a11-473a-aa48-58111fac20f0'::uuid
          and cp.provider='binance_futures'
          and cp.data_type='crypto_derivatives'
          and cp.status in ('queued','retry_wait')
          and (cp.not_before is null or cp.not_before<=now())
          and cp.attempts<cp.max_attempts
          and cr.status in ('queued','running')
        order by cp.priority desc,cp.provider_symbol,cp.id
        for update of cp skip locked
        limit 1
    )
    update public.collection_partitions cp
    set status='running',locked_by='supabase-edge:binance-deriv-v1',locked_at=now(),heartbeat_at=now(),
        attempts=attempts+1,updated_at=now(),last_error=null,error_code=null
    from candidate c
    where cp.id=c.id
    returning cp.* into v_row;

    if v_row.id is null then
        return jsonb_build_object('status','no_work');
    end if;
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

create or replace function research_hub.checkpoint_crypto_derivatives_recovery_partition_v1(
    p_partition_id uuid,p_worker_id text,p_next_cursor timestamptz,p_rows_written bigint,
    p_coverage_start timestamptz,p_coverage_end timestamptz,p_complete boolean,
    p_retention_floor timestamptz,p_error text default null
)
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_status text; v_run_id uuid; v_symbol text; v_attempts integer; v_max_attempts integer;
begin
    select run_id,provider_symbol,attempts,max_attempts
      into v_run_id,v_symbol,v_attempts,v_max_attempts
    from public.collection_partitions
    where id=p_partition_id and provider='binance_futures' and data_type='crypto_derivatives'
    for update;
    if v_run_id is null then raise exception 'Unknown crypto derivatives partition %',p_partition_id; end if;

    if p_error is not null then
        v_status:=case when v_attempts>=v_max_attempts then 'failed' else 'retry_wait' end;
        update public.collection_partitions
        set status=v_status,not_before=case when v_status='retry_wait' then now()+interval '60 seconds' else null end,
            locked_by=null,locked_at=null,heartbeat_at=now(),last_error=left(p_error,4000),
            error_code='derivatives_recovery_edge',updated_at=now(),
            cursor=coalesce(cursor,'{}'::jsonb)||jsonb_build_object(
                'recovery_lane','supabase_edge_v1','retention_floor',p_retention_floor,
                'coverage_start',p_coverage_start,'coverage_end',p_coverage_end,
                'recovery_rows_written',greatest(coalesce(p_rows_written,0),0))
        where id=p_partition_id;
    else
        v_status:=case when p_complete then 'completed' else 'queued' end;
        update public.collection_partitions
        set status=v_status,not_before=null,locked_by=null,locked_at=null,heartbeat_at=now(),last_error=null,error_code=null,
            updated_at=now(),row_count=greatest(coalesce(p_rows_written,0),0),
            cursor=coalesce(cursor,'{}'::jsonb)||jsonb_build_object(
                'recovery_lane','supabase_edge_v1','recovery_cursor_ts',p_next_cursor,'retention_floor',p_retention_floor,
                'coverage_start',p_coverage_start,'coverage_end',p_coverage_end,
                'coverage_truncated_by_retention',p_retention_floor is not null and p_retention_floor>(select start_ts from public.collection_partitions where id=p_partition_id),
                'recovery_rows_written',greatest(coalesce(p_rows_written,0),0),'finished',p_complete)
        where id=p_partition_id;
    end if;
    return jsonb_build_object('partition_id',p_partition_id,'symbol',v_symbol,'status',v_status,'rows_written',p_rows_written,'complete',p_complete);
end;
$$;

create or replace function public.claim_crypto_derivatives_recovery_edge_v1(p_worker_id text)
returns jsonb
language sql
security definer
set search_path=pg_catalog,research_hub,public,pg_temp
as $$ select research_hub.claim_crypto_derivatives_recovery_partition_v1(p_worker_id); $$;

create or replace function public.checkpoint_crypto_derivatives_recovery_edge_v1(
    p_partition_id uuid,p_worker_id text,p_next_cursor timestamptz,p_rows_written bigint,
    p_coverage_start timestamptz,p_coverage_end timestamptz,p_complete boolean,
    p_retention_floor timestamptz,p_error text default null
)
returns jsonb
language sql
security definer
set search_path=pg_catalog,research_hub,public,pg_temp
as $$
    select research_hub.checkpoint_crypto_derivatives_recovery_partition_v1(
        p_partition_id,p_worker_id,p_next_cursor,p_rows_written,p_coverage_start,p_coverage_end,p_complete,p_retention_floor,p_error);
$$;

revoke all on table research_hub.recovery_lane_leases from public,anon,authenticated;
revoke all on function research_hub.claim_crypto_derivatives_recovery_partition_v1(text) from public,anon,authenticated;
revoke all on function research_hub.checkpoint_crypto_derivatives_recovery_partition_v1(uuid,text,timestamptz,bigint,timestamptz,timestamptz,boolean,timestamptz,text) from public,anon,authenticated;
revoke all on function public.claim_crypto_derivatives_recovery_edge_v1(text) from public,anon,authenticated;
revoke all on function public.checkpoint_crypto_derivatives_recovery_edge_v1(uuid,text,timestamptz,bigint,timestamptz,timestamptz,boolean,timestamptz,text) from public,anon,authenticated;
grant execute on function public.claim_crypto_derivatives_recovery_edge_v1(text) to service_role;
grant execute on function public.checkpoint_crypto_derivatives_recovery_edge_v1(uuid,text,timestamptz,bigint,timestamptz,timestamptz,boolean,timestamptz,text) to service_role;

insert into research_hub.program_jobs(
    job_key,exact_name,purpose,store_key,source_schema,source_table,job_kind,current_state,
    progress_current,progress_total,completion_pct,latest_result,current_error,retry_state,next_automatic_action,
    intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata
)
values(
    'SOURCE-BINANCE-DERIV-METRICS-RECOVERY-V1','Recover retention-limited Binance derivatives metrics',
    'Recover the still-available portion of the intended 30-day Binance futures OI/positioning/taker-ratio enrichment after repairing the derivative partition identity collision. Preserve observed coverage exactly; never impute observations that have aged out of Binance history.',
    'market_data_primary','public','crypto_derivatives_metrics','source_recovery','queued_retention_sensitive_recovery',
    0,300,0,'{}'::jsonb,null,'bounded derivative-only recovery lane prepared',
    'Use the dedicated derivative-only recovery lane to process the 300 repaired partitions independently of the B-001-blocked collection loop. Measure actual coverage by metric/symbol after completion and only then expose OI/ratio/taker features to research.',
    false,null,false,false,
    jsonb_build_object('active_run_id','0f335c0d-1a11-473a-aa48-58111fac20f0','partition_count',300,'source_provider','binance_futures','data_type','crypto_derivatives','retention_sensitive',true,'missing_history_must_not_be_imputed',true,'observability_contract_job','METHOD-BINANCE-DERIV-OBSERVABILITY-V1','user_action_required',false)
)
on conflict(job_key) do update set purpose=excluded.purpose,progress_total=300,next_automatic_action=excluded.next_automatic_action,intervention_required=false,exact_intervention=null,metadata=research_hub.program_jobs.metadata||excluded.metadata,updated_at=now();

create or replace function research_hub.refresh_binance_deriv_recovery_program_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_total bigint; v_completed bigint; v_running bigint; v_queued bigint; v_failed bigint; v_rows bigint; v_pct double precision;
begin
    select count(*),count(*) filter(where status in ('completed','completed_empty')),count(*) filter(where status='running'),
           count(*) filter(where status in ('queued','retry_wait')),count(*) filter(where status='failed'),coalesce(sum(row_count),0)
      into v_total,v_completed,v_running,v_queued,v_failed,v_rows
    from public.collection_partitions
    where run_id='0f335c0d-1a11-473a-aa48-58111fac20f0'::uuid and provider='binance_futures' and data_type='crypto_derivatives';
    v_pct:=case when v_total>0 then 100.0*v_completed/v_total else 0 end;
    update research_hub.program_jobs
    set current_state=case when v_failed>0 then 'recovery_with_failed_partitions' when v_total>0 and v_completed=v_total then 'completed_quality_audit_required' when v_running>0 then 'running_retention_sensitive_recovery' else 'queued_retention_sensitive_recovery' end,
        progress_current=v_completed,progress_total=v_total,completion_pct=v_pct,
        latest_result=jsonb_build_object('total_partitions',v_total,'completed_partitions',v_completed,'running_partitions',v_running,'queued_partitions',v_queued,'failed_partitions',v_failed,'rows_written',v_rows,'coverage_audit_required',true),
        current_error=case when v_failed>0 then v_failed||' recovery partition(s) failed bounded retries' else null end,
        retry_state=case when v_failed>0 then 'engineering review after bounded retries' when v_completed=v_total and v_total>0 then 'collection complete; coverage audit required before research use' when v_running>0 then 'dedicated recovery lane active' else 'awaiting dedicated recovery invocation' end,
        next_automatic_action=case when v_completed=v_total and v_total>0 then 'Audit min/max timestamp and non-null coverage for OI, global/top ratios and taker ratio by symbol. Register only sufficiently covered point-in-time derived metrics for discovery.' else next_automatic_action end,
        intervention_required=false,exact_intervention=null,updated_at=now()
    where job_key='SOURCE-BINANCE-DERIV-METRICS-RECOVERY-V1';
    return jsonb_build_object('total',v_total,'completed',v_completed,'running',v_running,'queued',v_queued,'failed',v_failed,'rows_written',v_rows,'completion_pct',v_pct);
end;
$$;

revoke all on function research_hub.refresh_binance_deriv_recovery_program_v1() from public,anon,authenticated;