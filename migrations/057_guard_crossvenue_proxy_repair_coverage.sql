-- A source-repair checkpoint may advance only when at least 99% of the
-- synchronized rows in each batch reconstruct from the canonical Coinbase USD
-- raw bar. A materially incomplete batch keeps its cursor in place so late data
-- remain retryable; final partition completion also requires >=99% coverage.

create or replace function research_hub.process_next_coinbase_notional_proxy_batch_v1(p_limit integer default 2500)
returns jsonb
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare
  v_symbol text;
  v_cursor timestamptz;
  v_result jsonb;
  v_scanned bigint;
  v_updated bigint;
  v_max timestamptz;
  v_total_scanned bigint;
  v_total_updated bigint;
  v_fraction double precision;
begin
  if not pg_try_advisory_xact_lock(hashtext('rh-cv-coinbase-volume-proxy-v1')::bigint) then
    return jsonb_build_object('status','busy');
  end if;

  select partition_key,cursor_ts into v_symbol,v_cursor
  from research_hub.source_repair_checkpoints
  where repair_key='coinbase_usd_notional_proxy_v1'
    and (status in ('queued','running') or (status='failed' and failure_attempts<4))
  order by case status when 'running' then 0 when 'queued' then 1 else 2 end,
           coalesce(cursor_ts,'-infinity'::timestamptz),partition_key
  limit 1 for update skip locked;

  if v_symbol is null then return jsonb_build_object('status','idle'); end if;

  update research_hub.source_repair_checkpoints
     set status='running',last_error=null,updated_at=now()
   where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;

  begin
    v_result:=research_hub.backfill_coinbase_notional_proxy_batch_v1(v_symbol,v_cursor,p_limit);
    v_scanned:=coalesce((v_result->>'scanned')::bigint,0);
    v_updated:=coalesce((v_result->>'updated')::bigint,0);
    v_max:=nullif(v_result->>'max_ts','')::timestamptz;

    if v_scanned>0 then
      v_fraction:=v_updated::double precision/v_scanned::double precision;
      if v_fraction<0.99 then
        update research_hub.source_repair_checkpoints
           set status='failed',failure_attempts=failure_attempts+1,
               last_error='Coinbase notional proxy batch reconstruction below 99 percent; cursor not advanced',
               metadata=metadata||jsonb_build_object('quality_gate','batch_match_fraction_ge_0.99','failed_batch',v_result,'match_fraction',v_fraction),updated_at=now()
         where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
        return jsonb_build_object('status','failed_quality_gate','symbol',v_symbol,'scanned',v_scanned,'updated',v_updated,'match_fraction',v_fraction,'cursor_advanced',false,'outcome_accessed',false);
      end if;

      update research_hub.source_repair_checkpoints
         set status='running',cursor_ts=v_max,rows_scanned=rows_scanned+v_scanned,rows_updated=rows_updated+v_updated,
             metadata=metadata||jsonb_build_object('last_batch',v_result,'last_batch_match_fraction',v_fraction),updated_at=now()
       where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
      return jsonb_build_object('status','batch_completed','symbol',v_symbol,'match_fraction',v_fraction,'result',v_result);
    end if;

    select rows_scanned,rows_updated into v_total_scanned,v_total_updated
    from research_hub.source_repair_checkpoints
    where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
    v_fraction:=case when v_total_scanned>0 then v_total_updated::double precision/v_total_scanned::double precision else 0 end;

    if v_total_scanned<=0 or v_fraction<0.99 then
      update research_hub.source_repair_checkpoints
         set status='failed',failure_attempts=failure_attempts+1,
             last_error='Coinbase notional proxy final reconstruction below 99 percent',
             metadata=metadata||jsonb_build_object('quality_gate','final_match_fraction_ge_0.99','final_match_fraction',v_fraction),updated_at=now()
       where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
      return jsonb_build_object('status','failed_quality_gate','symbol',v_symbol,'total_scanned',v_total_scanned,'total_updated',v_total_updated,'match_fraction',v_fraction,'outcome_accessed',false);
    end if;

    update research_hub.source_repair_checkpoints
       set status='completed',metadata=metadata||jsonb_build_object('completed_at',now(),'last_batch',v_result,'final_match_fraction',v_fraction,'quality_gate_passed',true),updated_at=now()
     where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
    return jsonb_build_object('status','completed_partition','symbol',v_symbol,'match_fraction',v_fraction,'result',v_result);
  exception when others then
    update research_hub.source_repair_checkpoints
       set status='failed',failure_attempts=failure_attempts+1,last_error=sqlerrm,updated_at=now()
     where repair_key='coinbase_usd_notional_proxy_v1' and partition_key=v_symbol;
    return jsonb_build_object('status','failed','symbol',v_symbol,'error',sqlerrm);
  end;
end $$;

update research_hub.program_jobs
set metadata=metadata||jsonb_build_object('source_repair_min_match_fraction',0.99),updated_at=now()
where job_key='FEATURE-CROSSVENUE-LAG-V2';