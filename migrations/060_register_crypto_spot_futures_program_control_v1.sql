insert into research_hub.program_jobs(
    job_key,exact_name,purpose,store_key,source_schema,source_table,job_kind,current_state,
    started_at,progress_current,progress_total,completion_pct,latest_result,current_error,retry_state,
    next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata
)
values(
    'FEATURE-CRYPTO-SPOT-FUTURES-V1',
    'Typed crypto spot/futures 15m feature materialization v1',
    'Materialize the point-in-time Binance spot/perpetual 15m state family covering basis, funding, mark-vs-spot divergence and contemporaneous spot path/flow/activity while preserving June 2026 as sealed holdout.',
    'market_data_primary','research_hub','crypto_spot_futures15m_features_v1','feature_materialization',
    'materializing_discovery_validation',now(),0,243,0,'{}'::jsonb,null,'automatic daily partition queue active',
    'Continue the bounded typed materialization queue. When all Oct-2025 through May-2026 discovery/validation partitions are complete with zero failures and the June holdout remains untouched, freeze one experiment family and build a typed screening adapter without opening holdout.',
    false,null,false,true,
    jsonb_build_object(
        'feature_set_key','crypto.spot_futures15m.v1',
        'outcome_set_key','crypto.binance_spot15m_returns.v1',
        'discovery_start','2025-10-01T00:00:00Z',
        'discovery_end','2026-04-01T00:00:00Z',
        'validation_start','2026-04-01T00:00:00Z',
        'validation_end','2026-06-01T00:00:00Z',
        'sealed_holdout_start','2026-06-01T00:00:00Z',
        'typed_point_in_time_adapter',true,
        'funding_observed_at_lte_decision',true,
        'contract_multiplier_normalized',true,
        'user_action_required',false,
        'priority',1
    )
)
on conflict(job_key) do update set
    exact_name=excluded.exact_name,purpose=excluded.purpose,store_key=excluded.store_key,
    source_schema=excluded.source_schema,source_table=excluded.source_table,job_kind=excluded.job_kind,
    next_automatic_action=excluded.next_automatic_action,intervention_required=false,exact_intervention=null,
    holdout_sensitive=true,metadata=research_hub.program_jobs.metadata||excluded.metadata,updated_at=now();

create or replace function research_hub.refresh_crypto_spot_futures_program_job_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,research_hub,pg_temp
as $$
declare
    v_status jsonb;
    v_discovery jsonb;
    v_validation jsonb;
    v_total bigint;
    v_completed bigint;
    v_failed bigint;
    v_running bigint;
    v_queued bigint;
    v_holdout_untouched boolean;
    v_state text;
    v_pct double precision;
begin
    v_status:=research_hub.crypto_spot_futures15m_materialization_status_v1();
    v_discovery:=coalesce(v_status->'discovery','{}'::jsonb);
    v_validation:=coalesce(v_status->'validation','{}'::jsonb);
    v_total:=coalesce((v_discovery->>'total_days')::bigint,0)+coalesce((v_validation->>'total_days')::bigint,0);
    v_completed:=coalesce((v_discovery->>'completed_days')::bigint,0)+coalesce((v_validation->>'completed_days')::bigint,0);
    v_failed:=coalesce((v_discovery->>'failed_days')::bigint,0)+coalesce((v_validation->>'failed_days')::bigint,0);
    v_running:=coalesce((v_discovery->>'running_days')::bigint,0)+coalesce((v_validation->>'running_days')::bigint,0);
    v_queued:=coalesce((v_discovery->>'queued_days')::bigint,0)+coalesce((v_validation->>'queued_days')::bigint,0);
    v_holdout_untouched:=coalesce((v_status->'sealed_holdout'->>'untouched')::boolean,false);
    v_pct:=case when v_total>0 then 100.0*v_completed::double precision/v_total::double precision else 0 end;
    v_state:=case
        when not v_holdout_untouched then 'blocked_holdout_contamination'
        when v_failed>0 then 'blocked_failed_partitions'
        when v_total>0 and v_completed=v_total then 'ready_for_experiment_freeze'
        else 'materializing_discovery_validation'
    end;

    update research_hub.program_jobs
    set current_state=v_state,
        progress_current=v_completed,
        progress_total=v_total,
        completion_pct=v_pct,
        latest_successful_checkpoint=case when v_completed>0 then now() else latest_successful_checkpoint end,
        latest_result=jsonb_build_object(
            'feature_set_key','crypto.spot_futures15m.v1',
            'outcome_set_key','crypto.binance_spot15m_returns.v1',
            'completed_days',v_completed,
            'total_days',v_total,
            'failed_days',v_failed,
            'running_days',v_running,
            'queued_days',v_queued,
            'discovery_ready',coalesce((v_status->>'discovery_ready')::boolean,false),
            'validation_ready',coalesce((v_status->>'validation_ready')::boolean,false),
            'sealed_holdout',v_status->'sealed_holdout'
        ),
        current_error=case
            when not v_holdout_untouched then 'Sealed June 2026 holdout is no longer untouched; stop promotion work and investigate.'
            when v_failed>0 then v_failed||' typed materialization partition(s) exhausted bounded retries.'
            else null end,
        retry_state=case
            when not v_holdout_untouched then 'hard holdout safety stop'
            when v_failed>0 then 'automatic queue exhausted for at least one partition; engineering review required, not Rob intervention'
            when v_completed=v_total and v_total>0 then 'materialization complete; experiment-freeze handoff ready'
            else 'automatic typed partition queue active' end,
        next_automatic_action=case
            when not v_holdout_untouched then 'Do not open or use the holdout. Investigate contamination source and preserve evidence.'
            when v_failed>0 then 'Inspect failed partition diagnostics, fix the engineering cause, then retry the same frozen partitions without changing research definitions.'
            when v_completed=v_total and v_total>0 then 'Freeze a single discovery/validation experiment for this typed family, add the screening adapter and predeclare multiplicity/placebo/dependence controls. Keep June 2026 sealed.'
            else 'Continue bounded typed materialization automatically; do not create a competing derivatives family.' end,
        intervention_required=false,
        exact_intervention=null,
        updated_at=now()
    where job_key='FEATURE-CRYPTO-SPOT-FUTURES-V1';

    return jsonb_build_object('state',v_state,'completed_days',v_completed,'total_days',v_total,'completion_pct',v_pct,'holdout_untouched',v_holdout_untouched,'failed_days',v_failed);
end;
$$;

revoke all on function research_hub.refresh_crypto_spot_futures_program_job_v1() from public,anon,authenticated;

select research_hub.refresh_crypto_spot_futures_program_job_v1();

do $$
begin
    if exists(select 1 from cron.job where jobname='research_hub_crypto_spot_futures_program_sync_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_crypto_spot_futures_program_sync_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_crypto_spot_futures_program_sync_v1',
        '6,16,26,36,46,56 * * * *',
        'select research_hub.refresh_crypto_spot_futures_program_job_v1();'
    );
end $$;

insert into research_hub.research_findings(
    finding_key,finding_type,title,statement,status,evidence,source_run_keys,source_candidate_ids,reusable,propagation_targets
)
values(
    'FIND-CRYPTO-TYPED-DERIVATIVES-20260812','architecture',
    'Typed spot/futures adapter resolves part of derivatives observability gate',
    'The typed crypto.spot_futures15m.v1 adapter now provides point-in-time-safe funding, basis, mark-vs-spot divergence and contemporaneous spot state for 26 Binance spot/perpetual overlaps. Funding observations are constrained to funding timestamp <= decision_ts and 1000-token contract multipliers are normalized. This resolves those fields for discovery; open-interest history, positioning ratios and taker-ratio bucket observability remain separately gated unless an equally explicit temporal contract is established.',
    'active',
    jsonb_build_object('feature_set_key','crypto.spot_futures15m.v1','outcome_set_key','crypto.binance_spot15m_returns.v1','sealed_holdout_start','2026-06-01T00:00:00Z','funding_observed_at_lte_decision',true,'contract_multiplier_normalized',true,'does_not_resolve',jsonb_build_array('open_interest_bucket_semantics','global_long_short_ratio_bucket_semantics','top_account_ratio_bucket_semantics','top_position_ratio_bucket_semantics','taker_buy_sell_ratio_bucket_semantics')),
    array[]::text[],array[]::text[],true,array['ARCH-PRO','METHOD','XAL','EXEC-SIGNAL']
)
on conflict(finding_key) do update set statement=excluded.statement,status=excluded.status,evidence=excluded.evidence,reusable=true,propagation_targets=excluded.propagation_targets,updated_at=now();