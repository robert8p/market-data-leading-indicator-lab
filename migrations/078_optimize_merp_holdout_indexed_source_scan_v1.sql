-- Infrastructure-only repair after the first one-shot holdout invocation hit the
-- database statement timeout during source extraction. The transaction rolled back:
-- zero holdout statistics/results were committed and holdout_opened_at remained null.
-- No candidate, threshold, family, gate, direction, cost or holdout boundary changes.
-- Pre-holdout equivalence audit: 81,408 selected-universe source rows, zero mismatches;
-- signal_ts is exactly bucket_start + 15 minutes for every audited row.

update research_hub.program_jobs
set metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object(
      'holdout_infrastructure_attempts',1,
      'holdout_first_attempt_status','statement_timeout_during_source_scan_before_statistics_commit',
      'holdout_first_attempt_result_rows_committed',0,
      'holdout_first_attempt_methodology_changed',false,
      'holdout_retry_change','equivalent indexed bucket_start predicate; verified signal_ts= bucket_start+15m on 81408 preholdout rows with zero mismatches'
    ),updated_at=now()
where job_key='MERP-CR-20260811-001';

update research_hub.experiment_runs
set provenance=coalesce(provenance,'{}'::jsonb)||jsonb_build_object(
      'holdout_infrastructure_attempts',1,
      'first_holdout_attempt','statement_timeout_before_result_commit',
      'first_holdout_attempt_result_rows_committed',0,
      'retry_query_equivalence_verified_preholdout_rows',81408,
      'retry_query_equivalence_mismatches',0
    ),updated_at=now()
where run_id='56558e2a-60a1-4ca2-b3eb-fa59250cc390'::uuid;

do $$
declare vdef text;
begin
    select pg_get_functiondef(p.oid) into vdef
    from pg_proc p join pg_namespace n on n.oid=p.pronamespace
    where n.nspname='research_hub' and p.proname='run_merp_family_holdout_once_v1';
    if position('f.signal_ts>=v_hs and f.signal_ts<v_he' in vdef)=0 then
        raise exception 'Expected holdout source predicate not found; refusing rewrite';
    end if;
    vdef:=replace(
        vdef,
        'f.signal_ts>=v_hs and f.signal_ts<v_he',
        'f.bucket_start>=v_hs-interval ''15 minutes'' and f.bucket_start<v_he-interval ''15 minutes'''
    );
    execute vdef;
end $$;
