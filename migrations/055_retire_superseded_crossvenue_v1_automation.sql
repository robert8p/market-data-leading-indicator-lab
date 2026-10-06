-- Once v1.1 is superseded before screening, legacy schedulers must not recreate
-- its queued tasks. Leave the run auditable, permanently undispatchable and empty.

do $$ begin
  if exists(select 1 from cron.job where jobname='research_hub_crossvenue_materialize_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crossvenue_materialize_v1' limit 1)); end if;
  if exists(select 1 from cron.job where jobname='research_hub_crossvenue_advance_v1') then perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crossvenue_advance_v1' limit 1)); end if;
end $$;

create or replace function research_hub.advance_crypto_crossvenue_research_v1()
returns jsonb
language plpgsql
set search_path=research_hub,public,pg_temp
as $$
begin
  if exists(select 1 from research_hub.program_jobs where job_key='FEATURE-CROSSVENUE-LAG-V1' and current_state='superseded_pre_screen') then
    return jsonb_build_object('status','superseded_pre_screen','replacement_job_key','FEATURE-CROSSVENUE-LAG-V2','holdout_accessed',false);
  end if;
  raise exception 'Legacy v1 advance path retired; use FEATURE-CROSSVENUE-LAG-V2';
end $$;

create or replace function research_hub.materialize_next_crypto_crossvenue_symbol_v1()
returns jsonb
language plpgsql
set search_path=research_hub,public,pg_temp
as $$
begin
  return jsonb_build_object('status','superseded_pre_screen','replacement_feature_set_key','crypto.crossvenue.sync.v2','holdout_accessed',false);
end $$;

delete from research_hub.experiment_tasks
where run_id=(select run_id from research_hub.experiment_runs where run_key='RH-CV-SYNC-V1-20260812')
  and status in ('queued','failed');

update research_hub.program_jobs
set current_state='superseded_pre_screen',retry_state='terminal pre-screen supersession; legacy schedulers retired',updated_at=now()
where job_key='FEATURE-CROSSVENUE-LAG-V1';