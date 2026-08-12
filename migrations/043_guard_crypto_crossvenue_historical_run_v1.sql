-- This historical cross-venue window has already been exposed to exploratory
-- research. It may support discovery/validation, but it can never be relabelled
-- as untouched holdout evidence under this run key.
create or replace function research_hub.guard_crypto_crossvenue_historical_run_v1()
returns trigger language plpgsql security invoker set search_path=research_hub,pg_temp as $$
begin
  if new.run_key='RH-CV-SYNC-V1-20260812' then
    if new.holdout_start is not null or new.holdout_end is not null or new.holdout_sealed is true then
      raise exception 'Historical adaptive-reuse cross-venue run may not be assigned an untouched holdout; use future post-definition replication under a new run key';
    end if;
    new.code_version:='crypto_crossvenue_sync_v1.1';
    new.config:=coalesce(new.config,'{}'::jsonb)||jsonb_build_object(
      'statistical_profile_key','crypto_crossvenue_v1',
      'feature_definition_version','crypto-crossvenue-sync-v1.1',
      'frozen_lag_family_minutes',jsonb_build_array(1,2,5,10,15,30),
      'placebo_controls',jsonb_build_array('time_shuffle','symbol_permutation','reverse_venue_role'),
      'adaptive_reuse',true,'promotion_requires_future_replication',true,
      'no_historical_holdout_assigned',true,'holdout_accessed',false
    );
    new.provenance:=coalesce(new.provenance,'{}'::jsonb)||jsonb_build_object('historical_window_role','discovery_validation_only','future_replication_required',true);
  end if;
  return new;
end $$;

drop trigger if exists trg_guard_crypto_crossvenue_historical_run_v1 on research_hub.experiment_runs;
create trigger trg_guard_crypto_crossvenue_historical_run_v1 before insert or update on research_hub.experiment_runs for each row execute function research_hub.guard_crypto_crossvenue_historical_run_v1();