-- Frozen equity SIP microstructure sequence/interaction frontier v1.
-- Production was applied before any target screening. The prior 29 Jun-2 Jul
-- target period is excluded from this family; discovery starts 3 Jul 2026.

create or replace view research_hub.equity_microstructure_sequence_observations_v1
with (security_invoker=true)
as
with raw as (
    select m.*,i.provider_symbol,
           ('ALPACA:'||i.provider_symbol) instrument_key,
           coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0) last_depth,
           case when coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0)>0
                then (coalesce(m.last_bid_size,0)-coalesce(m.last_ask_size,0))
                     / nullif(coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0),0)::double precision end quote_imbalance,
           case when coalesce(m.buy_volume,0)+coalesce(m.sell_volume,0)>0
                then (coalesce(m.buy_volume,0)-coalesce(m.sell_volume,0))
                     / nullif(coalesce(m.buy_volume,0)+coalesce(m.sell_volume,0),0)::double precision end trade_imbalance,
           case when m.last_bid_price>0 and m.last_ask_price>=m.last_bid_price
                     and coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0)>0
                then ((((m.last_ask_price*coalesce(m.last_bid_size,0)
                         +m.last_bid_price*coalesce(m.last_ask_size,0))
                       / nullif(coalesce(m.last_bid_size,0)+coalesce(m.last_ask_size,0),0)::double precision)
                      / ((m.last_bid_price+m.last_ask_price)/2.0))-1.0)*10000.0 end microprice_dislocation_bps
    from public.equity_microstructure_1m m
    join public.instruments i on i.id=m.instrument_id
    where m.provider='alpaca'
), history as (
    select r.*,
           lag(r.ts,1) over w prev_ts,
           lag(r.ts,5) over w ts_lag5,
           lag(r.trade_imbalance,1) over w prev_trade_imbalance,
           lag(r.quote_imbalance,1) over w prev_quote_imbalance,
           avg(r.avg_spread_bps) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) spread_prev5,
           avg(r.last_depth) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) depth_prev5,
           avg(r.quote_count::double precision) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) quote_count_prev5,
           avg(r.trade_count::double precision) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) trade_count_prev5,
           count(*) over(partition by r.instrument_id order by r.ts rows between 5 preceding and 1 preceding) prev5_rows
    from raw r
    window w as (partition by r.instrument_id order by r.ts)
), ratios as (
    select h.*,
           case when h.prev5_rows=5 and h.ts_lag5=h.ts-interval '5 minutes'
                then ln(1+greatest(coalesce(h.avg_spread_bps,0),0))-ln(1+greatest(coalesce(h.spread_prev5,0),0)) end spread_log_ratio_prev5,
           case when h.prev5_rows=5 and h.ts_lag5=h.ts-interval '5 minutes'
                then ln(1+greatest(coalesce(h.last_depth,0),0))-ln(1+greatest(coalesce(h.depth_prev5,0),0)) end depth_log_ratio_prev5,
           case when h.prev5_rows=5 and h.ts_lag5=h.ts-interval '5 minutes'
                then ln(1+greatest(coalesce(h.quote_count,0),0))-ln(1+greatest(coalesce(h.quote_count_prev5,0),0)) end quote_rate_log_ratio_prev5,
           case when h.prev5_rows=5 and h.ts_lag5=h.ts-interval '5 minutes'
                then ln(1+greatest(coalesce(h.trade_count,0),0))-ln(1+greatest(coalesce(h.trade_count_prev5,0),0)) end trade_rate_log_ratio_prev5,
           case when h.prev_ts=h.ts-interval '1 minute' and h.trade_imbalance is not null and h.prev_trade_imbalance is not null
                then h.trade_imbalance-h.prev_trade_imbalance end trade_imbalance_delta_1m,
           case when h.prev_ts=h.ts-interval '1 minute' and h.quote_imbalance is not null and h.prev_quote_imbalance is not null
                then h.quote_imbalance-h.prev_quote_imbalance end quote_imbalance_delta_1m
    from history h
), states as (
    select r.*,
           case when r.spread_log_ratio_prev5 is not null and r.depth_log_ratio_prev5 is not null
                then r.spread_log_ratio_prev5-r.depth_log_ratio_prev5 end liquidity_withdrawal_score
    from ratios r
), final as (
    select s.*,lag(s.liquidity_withdrawal_score,1) over(partition by s.instrument_id order by s.ts) prev_withdrawal_score
    from states s
)
select f.instrument_id,f.provider_symbol,f.instrument_key,f.ts source_ts,
       f.ts+interval '1 minute' decision_ts,f.ts+interval '1 minute' observable_at,
       f.spread_log_ratio_prev5,f.depth_log_ratio_prev5,
       f.quote_rate_log_ratio_prev5,f.trade_rate_log_ratio_prev5,
       f.trade_imbalance_delta_1m,f.quote_imbalance_delta_1m,
       f.microprice_dislocation_bps,f.liquidity_withdrawal_score,
       case when f.trade_imbalance is not null and f.quote_imbalance is not null then f.trade_imbalance*f.quote_imbalance end flow_quote_agreement,
       case when f.trade_imbalance is not null and f.quote_imbalance is not null then abs(f.trade_imbalance-f.quote_imbalance) end pressure_disagreement,
       case when f.microprice_dislocation_bps is not null and f.trade_imbalance is not null then f.microprice_dislocation_bps*f.trade_imbalance end microprice_flow_alignment,
       case when f.prev_ts=f.ts-interval '1 minute' and f.prev_quote_imbalance is not null and f.trade_imbalance is not null then f.prev_quote_imbalance*f.trade_imbalance end prior_quote_to_trade,
       case when f.prev_ts=f.ts-interval '1 minute' and f.prev_trade_imbalance is not null and f.quote_imbalance is not null then f.prev_trade_imbalance*f.quote_imbalance end prior_trade_to_quote,
       case when f.prev_ts=f.ts-interval '1 minute' and f.prev_withdrawal_score is not null and f.trade_imbalance is not null then f.prev_withdrawal_score*f.trade_imbalance end prior_withdrawal_to_flow,
       case when f.liquidity_withdrawal_score is not null and f.trade_rate_log_ratio_prev5 is not null then f.liquidity_withdrawal_score+f.trade_rate_log_ratio_prev5 end burst_withdrawal,
       case when f.liquidity_withdrawal_score is not null and f.quote_rate_log_ratio_prev5 is not null then f.liquidity_withdrawal_score+f.quote_rate_log_ratio_prev5 end quote_burst_withdrawal
from final f;

insert into research_hub.feature_definitions(
    feature_key,dataset_key,feature_name,feature_family,value_type,source_expression,
    decision_time_rule,observable_at_rule,lookback_seconds,uses_future_data,enabled,metadata
)
values
('seq.spread_log_ratio_prev5','primary.equity_microstructure_1m','Spread change versus prior 5m','liquidity_transition','double precision','log spread ratio','source minute end','source minute end',360,false,true,'{"version":"v1"}'),
('seq.depth_log_ratio_prev5','primary.equity_microstructure_1m','Displayed depth change versus prior 5m','liquidity_transition','double precision','log depth ratio','source minute end','source minute end',360,false,true,'{"version":"v1"}'),
('seq.quote_rate_log_ratio_prev5','primary.equity_microstructure_1m','Quote-update burst versus prior 5m','activity_transition','double precision','log quote-rate ratio','source minute end','source minute end',360,false,true,'{"version":"v1"}'),
('seq.trade_rate_log_ratio_prev5','primary.equity_microstructure_1m','Trade-arrival burst versus prior 5m','activity_transition','double precision','log trade-rate ratio','source minute end','source minute end',360,false,true,'{"version":"v1"}'),
('seq.trade_imbalance_delta_1m','primary.equity_microstructure_1m','One-minute signed-flow acceleration','flow_transition','double precision','trade imbalance delta','source minute end','source minute end',120,false,true,'{"version":"v1"}'),
('seq.quote_imbalance_delta_1m','primary.equity_microstructure_1m','One-minute quote-pressure acceleration','book_transition','double precision','quote imbalance delta','source minute end','source minute end',120,false,true,'{"version":"v1"}'),
('seq.microprice_dislocation_bps','primary.equity_microstructure_1m','Closing microprice displacement from midpoint','microprice_pressure','double precision','size-weighted microprice displacement','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('seq.liquidity_withdrawal_score','primary.equity_microstructure_1m','Spread widening minus depth replenishment','liquidity_interaction','double precision','spread log ratio - depth log ratio','source minute end','source minute end',360,false,true,'{"version":"v1"}'),
('seq.flow_quote_agreement','primary.equity_microstructure_1m','Trade-flow and quote-pressure agreement','flow_book_interaction','double precision','trade imbalance * quote imbalance','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('seq.pressure_disagreement','primary.equity_microstructure_1m','Trade-flow versus quote-pressure disagreement','flow_book_interaction','double precision','abs trade imbalance - quote imbalance','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('seq.microprice_flow_alignment','primary.equity_microstructure_1m','Microprice and signed-flow alignment','flow_microprice_interaction','double precision','microprice displacement * trade imbalance','source minute end','source minute end',60,false,true,'{"version":"v1"}'),
('seq.prior_quote_to_trade','primary.equity_microstructure_1m','Prior quote pressure followed by current trade flow','sequence','double precision','lag quote imbalance * trade imbalance','source minute end','source minute end',120,false,true,'{"version":"v1"}'),
('seq.prior_trade_to_quote','primary.equity_microstructure_1m','Prior trade flow followed by current quote pressure','sequence','double precision','lag trade imbalance * quote imbalance','source minute end','source minute end',120,false,true,'{"version":"v1"}'),
('seq.prior_withdrawal_to_flow','primary.equity_microstructure_1m','Prior liquidity withdrawal followed by current flow','sequence','double precision','lag withdrawal score * trade imbalance','source minute end','source minute end',420,false,true,'{"version":"v1"}'),
('seq.burst_withdrawal','primary.equity_microstructure_1m','Trade burst plus liquidity withdrawal','activity_liquidity_interaction','double precision','withdrawal + trade burst','source minute end','source minute end',360,false,true,'{"version":"v1"}'),
('seq.quote_burst_withdrawal','primary.equity_microstructure_1m','Quote burst plus liquidity withdrawal','activity_liquidity_interaction','double precision','withdrawal + quote burst','source minute end','source minute end',360,false,true,'{"version":"v1"}')
on conflict(feature_key) do update set
    dataset_key=excluded.dataset_key,feature_name=excluded.feature_name,
    feature_family=excluded.feature_family,value_type=excluded.value_type,
    source_expression=excluded.source_expression,decision_time_rule=excluded.decision_time_rule,
    observable_at_rule=excluded.observable_at_rule,lookback_seconds=excluded.lookback_seconds,
    uses_future_data=false,enabled=true,metadata=excluded.metadata,updated_at=now();

insert into research_hub.feature_sets(
    feature_set_key,description,decision_grain,source_dataset_keys,feature_keys,
    materialization_schema,materialization_relation,point_in_time_verified,
    verification_notes,metadata
)
values(
    'equity.sip.microsequence.v1',
    'Frozen point-in-time equity SIP transition, interaction and one-step sequence features after broad quote-execution tails were rejected.',
    'instrument-minute',array['primary.equity_microstructure_1m'],
    array[
      'seq.spread_log_ratio_prev5','seq.depth_log_ratio_prev5','seq.quote_rate_log_ratio_prev5','seq.trade_rate_log_ratio_prev5',
      'seq.trade_imbalance_delta_1m','seq.quote_imbalance_delta_1m','seq.microprice_dislocation_bps','seq.liquidity_withdrawal_score',
      'seq.flow_quote_agreement','seq.pressure_disagreement','seq.microprice_flow_alignment','seq.prior_quote_to_trade',
      'seq.prior_trade_to_quote','seq.prior_withdrawal_to_flow','seq.burst_withdrawal','seq.quote_burst_withdrawal'
    ],
    'research_hub','feature_rows',true,
    'PIT only. Prior-5 features require exact five-minute continuity; one-step sequences require exact prior-minute continuity.',
    '{"adapter":"equity_microstructure_sequence_v1","family_frozen_before_first_target_screen":true,"cumulative_multiplicity_required":true}'::jsonb
)
on conflict(feature_set_key) do update set
    description=excluded.description,source_dataset_keys=excluded.source_dataset_keys,
    feature_keys=excluded.feature_keys,point_in_time_verified=true,
    verification_notes=excluded.verification_notes,metadata=excluded.metadata,updated_at=now();

-- The production implementation also installs a bounded parameterized materializer:
-- research_hub.refresh_equity_microstructure_sequence_v1(p_start,p_end).
-- It writes only research_hub.feature_rows, is idempotent on
-- (feature_set_key,instrument_key,decision_ts), and never reads target outcomes.

insert into research_hub.experiment_runs(
    run_key,name,status,feature_set_key,outcome_set_key,
    discovery_start,discovery_end,validation_start,validation_end,holdout_start,holdout_end,
    config,code_version,purpose,source_store_key,source_schema,source_table,dataset_keys,
    cost_model,execution_model,holdout_sealed,latest_result,provenance
)
values(
    'eq_micro_quote_exec_sequence_v1_20260703_20260728',
    'Equity SIP quoted-execution sequence/interaction discovery v1','planned',
    'equity.sip.microsequence.v1','equity.sip.quote_exec.v1',
    '2026-07-03 00:00+00','2026-07-16 00:00+00',
    '2026-07-16 00:00+00','2026-07-22 00:00+00',
    '2026-07-22 00:00+00','2026-07-29 00:00+00',
    jsonb_build_object(
      'tail_quantiles',jsonb_build_array(0.02,0.05,0.10,0.20),
      'extra_round_trip_cost_bps',10.0,
      'minimum_discovery_events',150,'minimum_validation_events',50,
      'fdr_q',0.05,'minimum_hit_rate',0.500001,'maximum_worst_loss_ratio',0.10,
      'minimum_dependence_clusters',8,'dependence_p_value',0.05,
      'multiplicity_parent_run_ids',jsonb_build_array(
        '045bbba1-769a-482e-9b3b-f8eb160cdd34','867611bb-ae43-4281-a6e9-03ad6123801e',
        '4d7dd6c5-b2a6-4dbf-8a2e-5a449cdb3b26','e2d55630-a696-40c9-bc71-59ee725d3258'
      ),
      'cumulative_prior_tests',5780,'family_frozen_before_first_screen',true,
      'fresh_date_discovery',true,'prior_used_period_excluded',true,'holdout_accessed',false
    ),
    'equity_microsequence_v1',
    'Search deeper liquidity/flow/book transitions and sequences on previously unused dates; preserve 22-28 July as one-way holdout.',
    'market_data_primary','research_hub','equity_microstructure_sequence_observations_v1',
    array['primary.equity_microstructure_1m'],
    '{"spread_crossing":"intrinsic in equity.sip.quote_exec.v1","extra_round_trip_cost_bps":10.0}'::jsonb,
    '{"long":"closing ask to future closing bid","short":"closing bid to future closing ask","same_instrument_only":true}'::jsonb,
    true,'{}'::jsonb,
    '{"adaptive_reuse":true,"cumulative_prior_searches":5780,"prior_feature_outcome_period_excluded":"2026-06-29 through 2026-07-02","holdout_preserved":true}'::jsonb
)
on conflict(run_key) do nothing;
