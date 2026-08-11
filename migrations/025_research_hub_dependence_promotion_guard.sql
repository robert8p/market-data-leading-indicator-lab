-- Intraday observations and overlapping horizons are not IID. Screening-level
-- event tests may therefore materially understate uncertainty. Prevent any
-- candidate from being labelled holdout-ready unless a separate dependence-aware
-- robustness test has explicitly passed.

create or replace function research_hub.guard_candidate_dependence_review()
returns trigger
language plpgsql
security invoker
set search_path=research_hub,pg_temp
as $$
begin
  if new.status in ('FROZEN_VALIDATION_PASSED','PROMOTED','HOLDOUT_READY')
     and coalesce(new.metrics #>> '{dependence_robustness,passed}','false') <> 'true' then
    new.status := 'DEPENDENCE_REVIEW_REQUIRED';
    new.confidence := 'Screening only';
    new.next_test := 'Run dependence-aware robustness on frozen discovery/validation data (day-cluster/HAC or block bootstrap) before any sealed-holdout evaluation.';
    new.metrics := coalesce(new.metrics,'{}'::jsonb) || jsonb_build_object(
      'dependence_robustness',jsonb_build_object(
        'passed',false,
        'status','required',
        'reason','Intraday observations and overlapping horizons can materially understate uncertainty under IID event-level tests.'
      )
    );
  end if;
  return new;
end $$;

drop trigger if exists trg_candidate_dependence_review on research_hub.candidate_ledger;
create trigger trg_candidate_dependence_review
before insert or update of status,metrics on research_hub.candidate_ledger
for each row execute function research_hub.guard_candidate_dependence_review();

comment on function research_hub.guard_candidate_dependence_review() is
'Prevents screening/validation candidates from being labelled holdout-ready until dependence-aware robustness is explicitly recorded as passed.';
