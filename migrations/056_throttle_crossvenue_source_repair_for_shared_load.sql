-- The 2-minute repair schedule produced repeated pg_cron startup timeouts while
-- B-001 and MDM were active, with successful 2,500-row batches taking up to
-- several minutes. Reduce scheduler pressure without changing data semantics.

do $$ begin
  if exists(select 1 from cron.job where jobname='research_hub_coinbase_notional_proxy_v1') then
    perform cron.unschedule((select jobid from cron.job where jobname='research_hub_coinbase_notional_proxy_v1' limit 1));
  end if;
  perform cron.schedule('research_hub_coinbase_notional_proxy_v1','*/4 * * * *','select research_hub.process_next_coinbase_notional_proxy_batch_v1(2500);');
end $$;

update research_hub.program_jobs
set retry_state='automatic bounded source repair active; throttled to 4-minute cadence to protect B-001/MDM shared database workload',
    metadata=metadata||jsonb_build_object('source_repair_cron','*/4 * * * *','source_repair_batch_rows',2500,'shared_load_throttled',true),updated_at=now()
where job_key='FEATURE-CROSSVENUE-LAG-V2';