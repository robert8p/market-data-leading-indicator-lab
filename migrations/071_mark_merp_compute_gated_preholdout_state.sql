update research_hub.program_jobs
set current_state='preholdout_standard_robustness_compute_gated',
    retry_state='automatic; frozen robustness queue waiting for clean shared-database compute slot',
    next_automatic_action='When no >30s B-001/XAL/MERP-heavy session is active, materialize one of the five typed pre-holdout cache symbols, then run one frozen survivor robustness battery. Holdout remains sealed and cannot be opened by this lane.',
    metadata=coalesce(metadata,'{}'::jsonb)||jsonb_build_object(
        'compute_gate_function','research_hub.merp_preholdout_compute_pressure_v1',
        'work_queue','research_hub.merp_preholdout_work_v1',
        'cache_symbols',5,
        'frozen_survivors',20,
        'holdout_opening_allowed',false
    ),
    updated_at=now()
where job_key='MERP-CR-20260811-001';
