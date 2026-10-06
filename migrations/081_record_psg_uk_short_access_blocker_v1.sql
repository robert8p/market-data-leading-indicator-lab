update research_hub.merp_psg_execution_validation_v1
set short_access_status='NO_VERIFIED_COMPLIANT_UK_RETAIL_SHORT_ROUTE_FOUND',
    blockers=blockers||jsonb_build_array(
      'Kraken margin eligibility excludes United Kingdom residents as of 2026-05-06',
      'Trading 212 crypto CFDs are not offered by Trading 212 UK Ltd',
      'UK FCA retail crypto-derivatives prohibition remains in force',
      'Coinbase UK retail market-access material does not establish PSG short access',
      'Binance PSGUSDT spot metadata includes MARGIN permission, but actual PSG borrowability is account-specific/authenticated and UK-retail service eligibility is not publicly verified'
    ),updated_at=now()
where candidate_id='RH-1F6255D317EE';

update research_hub.program_jobs
set current_state='accumulating_prospective_microstructure_no_verified_uk_retail_short_route',
    latest_result=coalesce(latest_result,'{}'::jsonb)||jsonb_build_object(
      'short_access_public_research',jsonb_build_object(
        'status','no_verified_compliant_uk_retail_route_found',
        'kraken_uk_margin_eligible',false,
        'trading212_uk_crypto_cfd_available',false,
        'uk_retail_crypto_derivative_ban_active',true,
        'binance_psgusdt_margin_market_metadata',true,
        'binance_psg_borrowability_publicly_verifiable',false,
        'account_specific_check_required_if_platform_access_exists',true
      )
    ),
    retry_state='microstructure collection automatic; short-access verification blocked on account/jurisdiction-specific eligibility',
    next_automatic_action='Continue prospective PSGUSDT spread/depth capture. Preserve strategy as execution-qualified statistically but not deployable unless a compliant UK-retail PSG short/borrow route is verified.',
    intervention_required=false,exact_intervention=null,updated_at=now()
where job_key='EXEC-MERP-PSG-V1';

insert into research_hub.research_findings(finding_key,finding_type,title,statement,status,evidence,source_run_keys,source_candidate_ids,reusable,propagation_targets)
values(
 'MERP-PSG-UK-SHORT-ACCESS-20260812','execution_constraint','No verified compliant UK-retail PSG short route found',
 'Public execution research after the untouched holdout found no verified compliant UK-retail route to short PSG. Kraken excludes UK residents from margin trading; Trading 212 UK does not offer crypto CFDs; UK retail crypto derivatives remain prohibited. Binance PSGUSDT spot metadata advertises margin capability, but PSG borrowability and UK-retail account eligibility require authenticated/account-specific verification and therefore cannot be assumed.',
 'deployment_blocker',
 jsonb_build_object('candidate_id','RH-1F6255D317EE','kraken_uk_margin',false,'trading212_uk_crypto_cfd',false,'uk_retail_crypto_derivative_ban',true,'binance_spot_margin_metadata',true,'binance_public_borrowability_check','authentication_required','conclusion','no_verified_compliant_uk_retail_short_route_found'),
 array['MERP-CR-20260811-001'],array['RH-1F6255D317EE'],true,array['project_context','execution_validation','deployment_readiness']
)
on conflict(finding_key) do update set statement=excluded.statement,status=excluded.status,evidence=excluded.evidence,reusable=true,propagation_targets=excluded.propagation_targets,updated_at=now();
