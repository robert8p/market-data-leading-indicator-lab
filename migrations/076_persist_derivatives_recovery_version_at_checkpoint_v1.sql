-- Persist the validated pagination/recovery definition at the durable checkpoint boundary.
-- The quality audit requires this marker; previously a successful Edge recovery could finish
-- without retaining it if the partition's initial cursor did not already contain the version.

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
        set status=v_status,
            not_before=case when v_status='retry_wait' then now()+interval '60 seconds' else null end,
            locked_by=null,locked_at=null,heartbeat_at=now(),last_error=left(p_error,4000),
            error_code='derivatives_recovery_edge',updated_at=now(),
            cursor=coalesce(cursor,'{}'::jsonb)||jsonb_build_object(
                'recovery_lane','supabase_edge_v1',
                'recovery_window_version','36h-subwindow-v3-full-partition',
                'retention_floor',p_retention_floor,
                'coverage_start',p_coverage_start,'coverage_end',p_coverage_end,
                'recovery_rows_written',greatest(coalesce(p_rows_written,0),0))
        where id=p_partition_id;
    else
        v_status:=case when p_complete then 'completed' else 'queued' end;
        update public.collection_partitions
        set status=v_status,not_before=null,locked_by=null,locked_at=null,heartbeat_at=now(),last_error=null,error_code=null,
            updated_at=now(),row_count=greatest(coalesce(p_rows_written,0),0),
            cursor=coalesce(cursor,'{}'::jsonb)||jsonb_build_object(
                'recovery_lane','supabase_edge_v1',
                'recovery_window_version','36h-subwindow-v3-full-partition',
                'recovery_cursor_ts',p_next_cursor,'retention_floor',p_retention_floor,
                'coverage_start',p_coverage_start,'coverage_end',p_coverage_end,
                'coverage_truncated_by_retention',p_retention_floor is not null and p_retention_floor>(select start_ts from public.collection_partitions where id=p_partition_id),
                'recovery_rows_written',greatest(coalesce(p_rows_written,0),0),'finished',p_complete)
        where id=p_partition_id;
    end if;
    return jsonb_build_object('partition_id',p_partition_id,'symbol',v_symbol,'status',v_status,'rows_written',p_rows_written,'complete',p_complete,'recovery_window_version','36h-subwindow-v3-full-partition');
end;
$$;

-- Conservative one-time provenance backfill. Never mark sparse generic-collector completions
-- as v3. Backfill only work currently owned by the retention lane, or completed partitions
-- with explicit Supabase Edge provenance and dense full-window recovery.
update public.collection_partitions cp
   set cursor=coalesce(cp.cursor,'{}'::jsonb)||jsonb_build_object(
           'recovery_window_version','36h-subwindow-v3-full-partition'
       ),
       updated_at=now()
 where cp.run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid
   and cp.provider='binance_futures'
   and cp.data_type='crypto_derivatives'
   and coalesce(cp.cursor->>'recovery_window_version','')<>'36h-subwindow-v3-full-partition'
   and coalesce((cp.cursor->>'retention_sensitive_recovery')::boolean,false)=true
   and (
       cp.status in ('queued','running','retry_wait')
       or (
           cp.status='completed'
           and cp.cursor->>'recovery_lane'='supabase_edge_v1'
           and cp.row_count>=5000
       )
   );
