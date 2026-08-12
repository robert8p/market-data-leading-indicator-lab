insert into research_hub.research_findings(
    finding_key,finding_type,title,statement,status,evidence,source_run_keys,
    source_candidate_ids,reusable,propagation_targets,updated_at
)
values
(
    'FIND-BINANCE-DERIV-RECOVERY-PAGINATION-20260812',
    'data_quality_architecture',
    'Binance derivatives recovery requires stable partition identity and sub-500-row time windows',
    'Binance futures enrichment originally collapsed ~300 intended symbol partitions because crypto_derivatives partitions had NULL instrument_id under a uniqueness key that ignored provider_symbol in that case. After identity repair, seven-day 5m REST requests still produced systematic holes because the OI/ratio endpoints cap responses at 500 rows. The validated recovery contract is now one stable instrument identity per canonical symbol plus <=36-hour request segments (<=432 expected 5m observations), idempotent PK upserts, explicit retention truncation, and a non-promotional continuity/completeness audit before research admission.',
    'active',
    jsonb_build_object(
        'current_recovery_run_id','1d57032e-20fa-4d23-b066-14cc659b13e2',
        'legacy_run_id','0f335c0d-1a11-473a-aa48-58111fac20f0',
        'recovery_version','36h-subwindow-v3-full-partition',
        'quality_table','research_hub.binance_deriv_recovery_quality_v1',
        'quality_gate','OI grid >=99.5%, minimum metric grid >=99%, max gap <=10m',
        'validated_examples',jsonb_build_array('BANK','1000CHEEMS'),
        'research_promotion',false
    ),
    array['SOURCE-BINANCE-DERIV-METRICS-RECOVERY-V1'],
    array[]::text[],
    true,
    array['all_research','crypto_derivatives','market_edge_research'],
    now()
),
(
    'FIND-MERP-FAMILY-MULTIPLICITY-20260812',
    'methodology',
    'MERP 20 pre-holdout survivors collapse to six same-outcome families',
    'The 20 frozen MERP cost-screen survivors are not 20 independent discoveries. Under the predeclared same-instrument + target + direction + horizon family rule they form six outcome families: 11 PSGUSDT 4h shorts, 4 PSGUSDT 1h shorts, 2 UMAUSDT 4h shorts, and one each for 1000SATSUSDT short 4h, API3USDT short 4h and PHAUSDT long 4h. At most one robustness-passing candidate per family may become a pre-holdout representative; family ranking is frozen as 3-day block-bootstrap LCB20 descending, then exact PF100 descending, then candidate_id. Redundant variants must not spend the sealed holdout separately.',
    'active',
    jsonb_build_object(
        'run_key','MERP-CR-20260811-001',
        'frozen_survivors',20,
        'families',6,
        'largest_family','PSGUSDT|PSGUSDT|-1|14400',
        'largest_family_candidates',11,
        'family_table','research_hub.merp_candidate_family_v1',
        'ranking_policy','block3d_boot_lcb05_20 desc, pf100 desc, candidate_id asc',
        'holdout_opened',false
    ),
    array['MERP-CR-20260811-001'],
    (select array_agg(candidate_id order by candidate_id) from research_hub.merp_candidate_family_v1),
    true,
    array['all_research','market_edge_research','merp'],
    now()
)
on conflict(finding_key) do update set
    finding_type=excluded.finding_type,
    title=excluded.title,
    statement=excluded.statement,
    status=excluded.status,
    evidence=excluded.evidence,
    source_run_keys=excluded.source_run_keys,
    source_candidate_ids=excluded.source_candidate_ids,
    reusable=excluded.reusable,
    propagation_targets=excluded.propagation_targets,
    updated_at=now();
