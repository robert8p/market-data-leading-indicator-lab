create table if not exists research_hub.merp_psg_execution_validation_v1(
    candidate_id text primary key,
    strategy_version text not null,
    signal_definition jsonb not null,
    execution_definition jsonb not null,
    validation_metrics jsonb not null,
    holdout_metrics jsonb not null,
    risk_metrics jsonb not null,
    current_market_snapshot jsonb not null default '{}'::jsonb,
    short_access_status text not null,
    spread_depth_status text not null,
    deployment_status text not null,
    blockers jsonb not null default '[]'::jsonb,
    frozen_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
revoke all on table research_hub.merp_psg_execution_validation_v1 from public,anon,authenticated;

insert into research_hub.merp_psg_execution_validation_v1(
 candidate_id,strategy_version,signal_definition,execution_definition,validation_metrics,holdout_metrics,risk_metrics,current_market_snapshot,
 short_access_status,spread_depth_status,deployment_status,blockers
) values (
 'RH-1F6255D317EE','merp.psg.short4h.nonoverlap.v1',
 jsonb_build_object('instrument','PSGUSDT','feature','cr.log_trade_count','threshold',6.7428806357919,'trigger','completed prior 15m bar has ln(1+trade_count) >= 6.7428806357919','direction','short','horizon_seconds',14400,'untouched_holdout_period',jsonb_build_array('2026-03-01','2026-07-29'),'holdout_definition_hash','5c9bda627d2b7af6ac5bf696ec488765'),
 jsonb_build_object('entry','open of bar beginning at signal_ts','exit','open exactly 4 hours after signal_ts','position_concurrency',1,'pyramiding',false,'overlapping_signals','ignore while position open','signal_parameters_retuned',false,'sizing_policy','not optimized on holdout'),
 jsonb_build_object('trades',60,'trade_days',41,'mean20',0.00992092276000632,'pf20',2.04770278032262,'mean50',0.00692092276000632,'pf50',1.68524636090417,'mean100',0.00192092276000632,'pf100',1.16678559711994,'mean150',-0.00307907723999368,'pf150',0.770626141448904,'mean200',-0.00807907723999368,'pf200',0.490333216058354,'hit20',0.8,'worst20',-0.367644171779141),
 jsonb_build_object('trades',71,'trade_days',38,'mean20',0.0184327413820369,'pf20',3.1608129479449,'mean50',0.0154327413820369,'pf50',2.62931102864458,'mean100',0.0104327413820369,'pf100',1.9266303284546,'mean150',0.00543274138203693,'pf150',1.4112611901296,'mean200',0.000432741382036932,'pf200',1.02831865112423,'hit20',0.690140845070423,'worst20',-0.137531135531135),
 jsonb_build_object('validation_max_account_drawdown_at_10pct_notional',0.0367644171779142,'holdout_max_account_drawdown_at_10pct_notional',0.023574223677183,'validation_compounded_return_at_10pct_notional',0.0603035974738053,'holdout_compounded_return_at_10pct_notional',0.138975489665086,'validation_longest_losing_streak',2,'holdout_longest_losing_streak',3,'validation_worst_trade_notional_return',-0.367644171779141,'holdout_worst_trade_notional_return',-0.137531135531135,'historically_supported_cost_stress_bps',100,'note','10% account-notional metrics are fixed-size illustrations only; size was not optimized on holdout'),
 jsonb_build_object('snapshot_at','2026-08-12T22:20:19Z','bid',0.492,'ask',0.493,'quoted_spread_bps_approx',20.3045685279188,'bid_top_notional_usdt',1644.53952,'ask_top_notional_usdt',1986.55835,'depth_slippage_bps',jsonb_build_object('500',jsonb_build_object('sell',0,'buy',0),'1000',jsonb_build_object('sell',0,'buy',0),'2000',jsonb_build_object('sell',3.61845069631225,'buy',0.128965732020465),'5000',jsonb_build_object('sell',20.8870527271537,'buy',19.2694339708114)),'historical_quote_rows_available',0,'historical_trade_rows_available',0),
 'blocked_unverified_uk_retail_borrow','prospective_collection_required','NOT_DEPLOYABLE_EXECUTION_VALIDATION_REQUIRED',
 jsonb_build_array('No verified UK-retail PSG short/borrow route has been established','No historical PSG L1 quote/trade tape exists in the project','Borrow availability and borrow cost are not observable from public spot exchangeInfo','Prospective signal-time spread/depth distribution is not yet accumulated')
)
on conflict(candidate_id) do update set strategy_version=excluded.strategy_version,signal_definition=excluded.signal_definition,execution_definition=excluded.execution_definition,validation_metrics=excluded.validation_metrics,holdout_metrics=excluded.holdout_metrics,risk_metrics=excluded.risk_metrics,current_market_snapshot=excluded.current_market_snapshot,short_access_status=excluded.short_access_status,spread_depth_status=excluded.spread_depth_status,deployment_status=excluded.deployment_status,blockers=excluded.blockers,updated_at=now();

insert into research_hub.program_jobs(job_key,exact_name,purpose,store_key,source_schema,source_table,source_id,job_kind,current_state,started_at,progress_current,progress_total,completion_pct,latest_result,retry_state,next_automatic_action,intervention_required,exact_intervention,frozen_rule,holdout_sensitive,metadata)
values('EXEC-MERP-PSG-V1','PSG 4h short execution validation','Qualify the sole untouched-holdout MERP survivor for realistic execution without changing its signal definition.','market_data_primary','research_hub','merp_psg_execution_validation_v1','RH-1F6255D317EE','execution_validation','blocked_on_short_access_and_prospective_microstructure',now(),3,5,60,jsonb_build_object('candidate_id','RH-1F6255D317EE','holdout_passed',true,'nonoverlap_validation_complete',true,'cost_stress_complete',true,'live_depth_canary_complete',true,'short_access_verified',false,'historical_l1_available',false),'automatic where public data is sufficient; account-specific borrow eligibility remains unresolved','Accumulate 15-minute PSGUSDT spread/depth snapshots and verify a UK-accessible short/borrow route. Do not retune signal or reuse holdout.',false,null,true,false,jsonb_build_object('strategy_version','merp.psg.short4h.nonoverlap.v1','holdout_consumed',true,'no_retesting_holdout',true,'user_action_required_now',false))
on conflict(job_key) do update set current_state=excluded.current_state,latest_result=excluded.latest_result,retry_state=excluded.retry_state,next_automatic_action=excluded.next_automatic_action,intervention_required=false,exact_intervention=null,metadata=research_hub.program_jobs.metadata||excluded.metadata,updated_at=now();

insert into research_hub.research_findings(finding_key,finding_type,title,statement,status,evidence,source_run_keys,source_candidate_ids,reusable,propagation_targets)
values('MERP-PSG-4H-SHORT-HOLDOUT-20260812','validated_candidate','PSG high-trade-count 4h short survives untouched family-level holdout','In MERP-CR-20260811-001, only the PSGUSDT 4-hour short family survived the one-shot untouched holdout after full pre-holdout robustness and family deduplication. Frozen trigger: completed 15m PSGUSDT bar ln(1+trade_count) >= 6.7428806357919; short at next bar open; four-hour exit. The candidate is not deployable until real short/borrow access and prospective microstructure evidence are verified.','holdout_passed_execution_validation_required',jsonb_build_object('candidate_id','RH-1F6255D317EE','holdout_n',259,'holdout_mean20',0.0305755941665348,'holdout_mean100',0.0225755941665348,'holdout_pf20',3.56974300787162,'holdout_pf100',2.59441425564192,'utc_days',38,'folds20',4,'folds100',4,'nonoverlap_validation_trades',60,'nonoverlap_holdout_trades',71,'nonoverlap_validation_pf100',1.16678559711994,'nonoverlap_holdout_pf100',1.9266303284546,'deployment_blocker','verified short/borrow route and real spread/depth evidence'),array['MERP-CR-20260811-001'],array['RH-1F6255D317EE'],true,array['project_context','candidate_registry','execution_validation','future_market_edge_research'])
on conflict(finding_key) do update set statement=excluded.statement,status=excluded.status,evidence=excluded.evidence,source_run_keys=excluded.source_run_keys,source_candidate_ids=excluded.source_candidate_ids,reusable=true,propagation_targets=excluded.propagation_targets,updated_at=now();
