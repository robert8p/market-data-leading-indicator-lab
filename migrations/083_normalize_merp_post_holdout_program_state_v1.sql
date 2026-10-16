update research_hub.program_jobs
set latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object(
      'holdout_opened',true,
      'holdout_accessed',true,
      'holdout_opened_at','2026-08-12T22:13:04.488658Z',
      'holdout_sealed',false,
      'robustness_candidates_completed',20,
      'market_edge_standard_v1_robustness_pending',0,
      'preholdout_standard_pass',7,
      'preholdout_standard_reject',13,
      'frozen_candidate_families',6,
      'family_representatives',4,
      'family_representatives_tested',4,
      'family_representatives_passed',1,
      'sole_holdout_survivor','RH-1F6255D317EE',
      'sole_holdout_survivor_strategy_version','merp.psg.short4h.nonoverlap.v1'
    ),
    current_state='untouched_holdout_complete_execution_validation_required',
    retry_state='terminal one-shot holdout consumed; no retuning or repeat holdout use',
    next_automatic_action='Execution validation continues only for RH-1F6255D317EE under frozen non-overlap PSG 4h short implementation. No holdout reuse.',
    intervention_required=false,
    exact_intervention=null,
    updated_at=now()
where job_key='MERP-CR-20260811-001';
