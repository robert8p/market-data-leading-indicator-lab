with targets as (
    select cp.id
    from public.collection_partitions cp
    left join research_hub.binance_deriv_recovery_quality_v1 q
      on q.partition_id=cp.id
    where cp.run_id='1d57032e-20fa-4d23-b066-14cc659b13e2'::uuid
      and cp.provider='binance_futures'
      and cp.data_type='crypto_derivatives'
      and cp.status='completed'
      and q.partition_id is null
      and cp.row_count < 5000
), upd as (
    update public.collection_partitions cp
       set status='queued',
           row_count=0,
           priority=9700000,
           not_before=null,
           locked_by=null,
           locked_at=null,
           heartbeat_at=null,
           last_error=null,
           error_code=null,
           cursor=coalesce(cp.cursor,'{}'::jsonb) || jsonb_build_object(
               'finished',false,
               'retention_sensitive_recovery',true,
               'recovery_snapshot','current_30d_20260812T1018Z',
               'recovery_cursor_ts',greatest(cp.start_ts,now()-interval '30 days'),
               'recovery_rows_written',0,
               'pagination_repair_required',true,
               'previous_completion_invalidated',true,
               'recovery_repair_reason','shared_collector_sparse_completion_requeued_after_exclusive_lane_fix'
           ),
           updated_at=now()
      from targets t
     where cp.id=t.id
    returning cp.id
)
select count(*) as requeued_sparse_derivatives_partitions from upd;
