-- Apply adaptive-reuse holdout and dispatch controls to all cross-venue versions.
-- Permanent holds (such as superseded v1.1) must never auto-release later.

create or replace function research_hub.guard_crypto_crossvenue_historical_run_v1()
returns trigger
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare v_version text;
begin
  if new.feature_set_key like 'crypto.crossvenue.sync.v%' or new.run_key like 'RH-CV-SYNC-V%' then
    if coalesce((new.config->>'adaptive_reuse')::boolean,true) then
      if new.holdout_start is not null or new.holdout_end is not null or new.holdout_sealed is true then
        raise exception 'Historical adaptive-reuse cross-venue runs may not be assigned an untouched holdout; use future post-definition replication under a new run key';
      end if;
      v_version:=case when new.feature_set_key='crypto.crossvenue.sync.v2' then 'crypto-crossvenue-sync-v2.0' else coalesce(new.config->>'feature_definition_version','crypto-crossvenue-sync-v1.1') end;
      new.code_version:=case when new.feature_set_key='crypto.crossvenue.sync.v2' then 'crypto_crossvenue_sync_v2.0' else coalesce(new.code_version,'crypto_crossvenue_sync_v1.1') end;
      new.config:=coalesce(new.config,'{}'::jsonb)||jsonb_build_object(
        'statistical_profile_key',case when new.feature_set_key='crypto.crossvenue.sync.v2' then 'crypto_crossvenue_v2' else coalesce(new.config->>'statistical_profile_key','crypto_crossvenue_v1') end,
        'feature_definition_version',v_version,
        'frozen_lag_family_minutes',jsonb_build_array(1,2,5,10,15,30),
        'placebo_controls',jsonb_build_array('time_shuffle','symbol_permutation','reverse_venue_role'),
        'adaptive_reuse',true,'promotion_requires_future_replication',true,
        'no_historical_holdout_assigned',true,'holdout_accessed',false
      );
      new.provenance:=coalesce(new.provenance,'{}'::jsonb)||jsonb_build_object('historical_window_role','discovery_validation_only','future_replication_required',true);
    end if;
  end if;
  return new;
end $$;

create or replace function research_hub.register_experiment_dispatch_control_v1()
returns trigger
language plpgsql
set search_path=research_hub,pg_temp
as $$
begin
  if new.feature_set_key like 'crypto.crossvenue.sync.v%' or new.run_key like 'RH-CV-SYNC-V%' then
    insert into research_hub.experiment_dispatch_controls(run_id,dispatch_enabled,dispatch_class,reason,required_job_keys,metadata)
    values(new.run_id,false,'shared_primary_db','Held while B-001 and MDM primary-database workloads are non-terminal.',array['B001-24M-REPLICATION','MDM-30D-COLLECTION'],jsonb_build_object('automatic_release',true,'user_action_required',false))
    on conflict(run_id) do update set
      required_job_keys=excluded.required_job_keys,
      dispatch_class=excluded.dispatch_class,
      metadata=research_hub.experiment_dispatch_controls.metadata||excluded.metadata,
      updated_at=now();
  end if;
  return new;
end $$;

create or replace function research_hub.refresh_experiment_dispatch_controls_v1()
returns bigint
language plpgsql
set search_path=research_hub,pg_temp
as $$
declare v_changed bigint:=0;
begin
  with status_eval as (
    select c.run_id,c.required_job_keys,c.metadata,
           cardinality(c.required_job_keys) required_n,
           count(j.job_key) matched_n,
           bool_and(j.current_state ~* '^(completed|finalized|rejected|ready|terminal)') filter(where j.job_key is not null) all_terminal
    from research_hub.experiment_dispatch_controls c
    left join lateral unnest(c.required_job_keys) req(job_key) on true
    left join research_hub.program_jobs j on j.job_key=req.job_key
    group by c.run_id,c.required_job_keys,c.metadata
  ), desired as (
    select run_id,
           case when coalesce((metadata->>'permanent_hold')::boolean,false) then false
                else (required_n=0 or (matched_n=required_n and coalesce(all_terminal,false))) end should_enable,
           coalesce((metadata->>'permanent_hold')::boolean,false) permanent_hold
    from status_eval
  ), u as (
    update research_hub.experiment_dispatch_controls c
    set dispatch_enabled=d.should_enable,
        reason=case when d.permanent_hold then coalesce(c.reason,'Permanently held by research-control policy.')
                    when d.should_enable then 'All required primary-database workloads are terminal/ready; dispatch released automatically.'
                    else 'Held until all required primary-database workloads are terminal/ready.' end,
        last_evaluated_at=now(),updated_at=now()
    from desired d
    where c.run_id=d.run_id and (c.dispatch_enabled is distinct from d.should_enable or c.last_evaluated_at is null)
    returning 1
  ) select count(*) into v_changed from u;
  return v_changed;
end $$;

select research_hub.refresh_experiment_dispatch_controls_v1();