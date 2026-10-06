-- Freeze same-outcome candidate families before MERP robustness results exist.
-- Family key = source instrument + target instrument + direction + horizon.
-- At most one robustness-passing candidate per family may become a pre-holdout representative.
-- Representative ranking is predeclared: 3-day block-bootstrap LCB20 desc, PF100 desc, candidate_id asc.
-- This migration never opens the sealed holdout.

create table if not exists research_hub.merp_candidate_family_v1(
    candidate_id text primary key,
    family_key text not null,
    source_instrument text not null,
    target_instrument text not null,
    trade_direction integer not null,
    horizon_seconds integer not null,
    family_policy_version text not null default 'same-outcome-v1',
    representative boolean not null default false,
    family_status text not null default 'frozen_member',
    selection_rank integer,
    selection_evidence jsonb not null default '{}'::jsonb,
    frozen_at timestamptz not null default now(),
    selected_at timestamptz
);
revoke all on table research_hub.merp_candidate_family_v1 from public,anon,authenticated;

insert into research_hub.merp_candidate_family_v1(
    candidate_id,family_key,source_instrument,target_instrument,trade_direction,horizon_seconds
)
select
    candidate_id,
    concat_ws('|',
        frozen_definition->>'source_instrument',
        frozen_definition->>'target_instrument',
        frozen_definition->>'trade_direction',
        frozen_definition->>'horizon_seconds'
    ),
    frozen_definition->>'source_instrument',
    frozen_definition->>'target_instrument',
    (frozen_definition->>'trade_direction')::integer,
    (frozen_definition->>'horizon_seconds')::integer
from research_hub.candidate_ledger
where run_id='56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid
  and candidate_id in (
      select object_key from research_hub.merp_preholdout_work_v1
      where work_type='candidate_robustness'
  )
on conflict(candidate_id) do nothing;

create or replace function research_hub.select_merp_family_representatives_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_evaluated bigint;
    v_representatives bigint;
    v_families bigint;
begin
    if exists(
        select 1 from research_hub.ai_experiment_registry
        where run_key='MERP-CR-20260811-001'
          and (not holdout_sealed or holdout_opened_at is not null or coalesce((latest_result->>'holdout_rows')::bigint,0)>0)
    ) or exists(
        select 1 from research_hub.experiment_runs
        where run_id='56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid
          and (not holdout_sealed or holdout_opened_at is not null or coalesce((latest_result->>'holdout_rows')::bigint,0)>0)
    ) then
        raise exception 'MERP holdout boundary violated: family selection is pre-holdout only';
    end if;

    select count(*) into v_evaluated
    from research_hub.merp_standard_robustness_v1
    where run_id='56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid;
    if v_evaluated<>20 then
        return jsonb_build_object(
            'status','waiting_for_all_robustness_results',
            'evaluated',v_evaluated,
            'required',20,
            'holdout_opened',false
        );
    end if;

    update research_hub.merp_candidate_family_v1
    set representative=false,family_status='frozen_member',selection_rank=null,
        selection_evidence='{}'::jsonb,selected_at=null;

    with ranked as (
        select
            f.candidate_id,
            f.family_key,
            row_number() over(
                partition by f.family_key
                order by
                    (r.metrics->'dependence'->>'block3d_boot_lcb05_20')::double precision desc nulls last,
                    (r.metrics->'validation'->>'pf100')::double precision desc nulls last,
                    f.candidate_id
            ) rk,
            r.metrics
        from research_hub.merp_candidate_family_v1 f
        join research_hub.merp_standard_robustness_v1 r on r.candidate_id=f.candidate_id
        where r.overall_preholdout_pass
    ), upd as (
        update research_hub.merp_candidate_family_v1 f
        set representative=(ranked.rk=1),
            family_status=case when ranked.rk=1
                then 'family_representative_preholdout'
                else 'family_redundant_preholdout' end,
            selection_rank=ranked.rk,
            selection_evidence=jsonb_build_object(
                'ranking_policy','block3d_boot_lcb05_20 desc, pf100 desc, candidate_id asc',
                'block3d_boot_lcb05_20',ranked.metrics->'dependence'->'block3d_boot_lcb05_20',
                'pf100',ranked.metrics->'validation'->'pf100',
                'holdout_accessed',false
            ),
            selected_at=now()
        from ranked
        where f.candidate_id=ranked.candidate_id
        returning f.candidate_id,f.representative
    )
    select count(*) filter(where representative) into v_representatives from upd;

    update research_hub.candidate_ledger c
    set status=case when f.representative
            then 'FAMILY_REPRESENTATIVE_PREHOLDOUT'
            else 'FAMILY_REDUNDANT_PREHOLDOUT' end,
        confidence=case when f.representative
            then 'Passed frozen robustness and selected as the sole pre-holdout representative of its same-instrument/direction/horizon family.'
            else 'Passed frozen robustness but is redundant with a stronger predeclared same-outcome family representative; do not spend holdout separately.' end,
        next_test=case when f.representative
            then 'Freeze execution-replication plan and family-level holdout test definition. Do not open holdout automatically.'
            else 'No independent holdout test. Preserve as corroborating family evidence only.' end,
        updated_at=now()
    from research_hub.merp_candidate_family_v1 f
    join research_hub.merp_standard_robustness_v1 r on r.candidate_id=f.candidate_id
    where c.candidate_id=f.candidate_id and r.overall_preholdout_pass;

    select count(distinct family_key) into v_families
    from research_hub.merp_candidate_family_v1;

    update research_hub.program_jobs
    set current_state='preholdout_family_dedup_complete_holdout_still_sealed',
        latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object(
            'frozen_candidate_families',v_families,
            'family_representatives',coalesce(v_representatives,0),
            'holdout_opened',false,
            'family_policy','same instrument + target + direction + horizon; at most one representative per family'
        ),
        retry_state='family multiplicity controlled before holdout',
        next_automatic_action=case when coalesce(v_representatives,0)>0
            then 'Freeze execution-replication and one-shot family-level holdout definitions for representatives only. Do not open holdout automatically.'
            else 'No family representative survived; do not open holdout and continue independent research families.' end,
        updated_at=now()
    where job_key='MERP-CR-20260811-001';

    return jsonb_build_object(
        'status','family_selection_complete',
        'families',v_families,
        'representatives',coalesce(v_representatives,0),
        'holdout_opened',false
    );
end;
$$;
revoke all on function research_hub.select_merp_family_representatives_v1() from public,anon,authenticated;

create or replace function research_hub.select_merp_family_representatives_once_v1()
returns jsonb
language plpgsql
security invoker
set search_path=pg_catalog,public,research_hub,pg_temp
as $$
declare
    v_state text;
begin
    select current_state into v_state
    from research_hub.program_jobs
    where job_key='MERP-CR-20260811-001';
    if v_state='preholdout_family_dedup_complete_holdout_still_sealed' then
        return jsonb_build_object('status','already_complete','holdout_opened',false);
    end if;
    return research_hub.select_merp_family_representatives_v1();
end;
$$;
revoke all on function research_hub.select_merp_family_representatives_once_v1() from public,anon,authenticated;

DO $$
BEGIN
    if exists(select 1 from cron.job where jobname='research_hub_merp_family_dedup_v1') then
        perform cron.unschedule((select jobid from cron.job where jobname='research_hub_merp_family_dedup_v1' limit 1));
    end if;
    perform cron.schedule(
        'research_hub_merp_family_dedup_v1',
        '*/10 * * * *',
        'select research_hub.select_merp_family_representatives_once_v1();'
    );
END $$;
