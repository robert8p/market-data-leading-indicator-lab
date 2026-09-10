-- Lightweight control-plane automation: resolve authoritative job dependencies
-- every five minutes so research work does not wait for a manual 'Proceed'.
do $$
declare j bigint;
begin
  select jobid into j from cron.job where jobname='research-hub-program-dependency-refresh';
  if j is not null then perform cron.unschedule(j); end if;
end $$;
select cron.schedule(
  'research-hub-program-dependency-refresh',
  '*/5 * * * *',
  $cron$select research_hub.refresh_program_job_dependencies();$cron$
);
