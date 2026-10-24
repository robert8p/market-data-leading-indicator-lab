-- Pure calculator only; root controller deploys after independent review. No source or research writes.
-- D01 frozen dictionary unchanged. V2 preserves source JSON numeric values through all aggregates.
-- Ratio-minus-one formulas use algebraically identical difference/denominator forms to avoid cancellation.
-- Old V1 function and outputs remain immutable; independent tolerances are unchanged.
create or replace function population_top3000_202608_202609_v1.d01_feature_grid_json_v2(
 p_bars jsonb,p_micro jsonb,p_regular_open timestamptz,p_regular_close timestamptz,
 p_context jsonb,p_assumed_publication_seconds integer default 1)
returns setof jsonb language plpgsql immutable parallel safe set search_path to pg_catalog set timezone to 'UTC'
as $f$
begin
 if jsonb_typeof(p_bars) is distinct from 'array' or jsonb_array_length(p_bars)>960
 or jsonb_typeof(p_micro) is distinct from 'array' or jsonb_array_length(p_micro)>960
 or p_regular_open is null or p_regular_close is null or p_regular_close<=p_regular_open
 or p_regular_close-p_regular_open>interval '16 hours'
 or date_trunc('minute',p_regular_open)<>p_regular_open or date_trunc('minute',p_regular_close)<>p_regular_close
 or p_assumed_publication_seconds is null or p_assumed_publication_seconds not between 0 and 60
 or coalesce(p_context->>'source_provider','')<>'alpaca' or coalesce(p_context->>'source_symbol','')=''
 then raise exception 'D01_INVALID_BOUNDED_INPUT';end if;
 if exists(select 1 from jsonb_array_elements(p_bars) r where jsonb_typeof(r)<>'object' or r->>'ts' is null or coalesce(r->>'source_id','')='' or coalesce(r->>'source_sha256','')!~'^[0-9a-f]{64}$' or coalesce(r->>'raw_record_sha256','')!~'^[0-9a-f]{64}$')
 or exists(select 1 from jsonb_array_elements(p_micro) r where jsonb_typeof(r)<>'object' or r->>'ts' is null or r->>'genuine_capture' is distinct from 'true' or coalesce(r->>'source_id','')='' or coalesce(r->>'source_sha256','')!~'^[0-9a-f]{64}$')
 then raise exception 'D01_SOURCE_PROVENANCE_OR_CAPTURE_REQUIRED';end if;
 if exists(select 1 from jsonb_array_elements(p_bars) r group by (r->>'ts')::timestamptz having count(*)>1)
 or exists(select 1 from jsonb_array_elements(p_micro) r group by (r->>'ts')::timestamptz having count(*)>1)
 then raise exception 'D01_DUPLICATE_MINUTE';end if;
 if exists(select 1 from (select (r->>'ts')::timestamptz ts from jsonb_array_elements(p_bars) r union all select (r->>'ts')::timestamptz from jsonb_array_elements(p_micro) r) t where ts<p_regular_open or ts>=p_regular_close or date_trunc('minute',ts)<>ts)
 then raise exception 'D01_OUTSIDE_EXACT_SESSION_GRID';end if;
 return query
with cal as (select (p_regular_open at time zone 'America/New_York')::date session_date,p_regular_open regular_open,p_regular_close regular_close), grid as (
 select cal.session_date,cal.regular_open,cal.regular_close,g as interval_start,
        row_number() over(order by g)::integer-1 as minute_index
 from cal cross join lateral
 generate_series(cal.regular_open,cal.regular_close-interval '1 minute',interval '1 minute') g
), id as (
 select p_context->>'source_symbol' provider_symbol,p_context->>'identity_mapping_quality' mapping_quality
), bars as (
 select * from jsonb_to_recordset(p_bars) as b(ts timestamptz,source_feed text,open numeric,high numeric,low numeric,close numeric,volume numeric,trade_count bigint,vwap numeric,source_id text,source_sha256 text,raw_record_sha256 text)
), micro as (
 select * from jsonb_to_recordset(p_micro) as m(ts timestamptz,trade_count bigint,quote_count bigint,avg_spread_bps numeric,last_bid_price numeric,last_ask_price numeric,last_bid_size numeric,last_ask_size numeric,last_trade_price numeric,buy_volume numeric,sell_volume numeric,source_id text,source_sha256 text,genuine_capture boolean)
), raw as (
 select g.*,b.source_feed,
  b.open as source_open,b.high as source_high,b.low as source_low,b.close as source_close,
  b.volume as source_volume,b.trade_count as source_trade_count,b.vwap as source_vwap,
  m.trade_count as micro_trades,m.quote_count as micro_quotes,m.avg_spread_bps as micro_spread,
  m.last_bid_price,m.last_ask_price,m.last_bid_size,m.last_ask_size,m.last_trade_price,
  m.buy_volume,m.sell_volume,
  (b.ts is not null) as bar_present,(m.ts is not null and m.genuine_capture) as micro_present,
 b.source_id,b.source_sha256,b.raw_record_sha256,m.source_id micro_source_id,m.source_sha256 micro_source_sha256
 from grid g
 left join bars b on b.ts=g.interval_start
 left join micro m on m.ts=g.interval_start and m.genuine_capture
), base as (
 select r.*,
  case when r.source_open>0 and r.source_high>0 and r.source_low>0 and r.source_close>0
   and (abs(r.source_open)<'Infinity'::numeric)
   and (abs(r.source_high)<'Infinity'::numeric)
   and (abs(r.source_low)<'Infinity'::numeric)
   and (abs(r.source_close)<'Infinity'::numeric)
   and r.source_high>=greatest(r.source_open,r.source_close,r.source_low)
   and r.source_low<=least(r.source_open,r.source_close,r.source_high)
   then true else false end as valid_bar,
  case when r.source_close>0
   and (abs(r.source_close)<'Infinity'::numeric)
   then r.source_close end as valid_close,
  case when r.source_volume>=0
   and (abs(r.source_volume)<'Infinity'::numeric)
   then r.source_volume end as valid_volume
 from raw r
), lagged as (
 select b.*,
  lag(b.valid_close,1) over w as close_lag1,
  lag(b.valid_close,5) over w as close_lag5,
  lag(b.valid_close,10) over w as close_lag10,
  lag(b.valid_close,20) over w as close_lag20,
  lag(b.valid_close,60) over w as close_lag60,
  lag(b.valid_close,120) over w as close_lag120,
  count(b.valid_close) over(order by b.interval_start rows between 1 preceding and current row) as close_n2,
  count(b.valid_close) over(order by b.interval_start rows between 5 preceding and current row) as close_n6,
  count(b.valid_close) over(order by b.interval_start rows between 10 preceding and current row) as close_n11,
  count(b.valid_close) over(order by b.interval_start rows between 20 preceding and current row) as close_n21,
  count(b.valid_close) over(order by b.interval_start rows between 60 preceding and current row) as close_n61,
  count(b.valid_close) over(order by b.interval_start rows between 120 preceding and current row) as close_n121,
  count(b.valid_close) over(order by b.interval_start rows between 4 preceding and current row) as close_n5,
  count(b.valid_close) over(order by b.interval_start rows between 19 preceding and current row) as close_n20,
  count(b.valid_volume) over(order by b.interval_start rows between 19 preceding and current row) as volume_n20,
  count(*) filter(where b.valid_bar) over(order by b.interval_start rows between 19 preceding and current row) as coherent_n20,
  count(*) filter(where b.valid_close is not null and b.valid_volume is not null) over(order by b.interval_start rows between 19 preceding and current row) as price_volume_n20,
  count(b.valid_close) over(order by b.interval_start rows between 59 preceding and current row) as close_n60,
  avg(b.valid_close) over(order by b.interval_start rows between 4 preceding and current row) as sma5,
  avg(b.valid_close) over(order by b.interval_start rows between 19 preceding and current row) as sma20,
  avg(b.valid_close) over(order by b.interval_start rows between 59 preceding and current row) as sma60,
  avg(b.valid_volume) over(order by b.interval_start rows between 19 preceding and current row) as vol_mean20,
  stddev_samp(b.valid_volume) over(order by b.interval_start rows between 19 preceding and current row) as vol_sd20,
  avg(case when b.valid_close is not null and b.valid_volume is not null then b.valid_close*b.valid_volume end)
   over(order by b.interval_start rows between 19 preceding and current row) as dv_mean20,
  max(case when b.valid_bar then b.source_high end)
   over(order by b.interval_start rows between 19 preceding and current row) as hi20,
  min(case when b.valid_bar then b.source_low end)
   over(order by b.interval_start rows between 19 preceding and current row) as lo20,
  sum(coalesce(b.source_vwap,b.valid_close)*b.valid_volume)
   filter(where b.valid_volume>0 and coalesce(b.source_vwap,b.valid_close)>0 and abs(coalesce(b.source_vwap,b.valid_close))<'Infinity'::numeric)
   over(order by b.interval_start rows between unbounded preceding and current row) as cum_pv,
  sum(b.valid_volume) filter(where b.valid_volume>0 and coalesce(b.source_vwap,b.valid_close)>0 and abs(coalesce(b.source_vwap,b.valid_close))<'Infinity'::numeric)
   over(order by b.interval_start rows between unbounded preceding and current row) as cum_vol,
  bool_or(b.source_vwap is null and b.valid_close>0 and b.valid_volume>0) over(order by b.interval_start rows between unbounded preceding and current row) as vwap_close_substituted,
  count(*) filter(where not b.bar_present or b.valid_volume is null or coalesce(b.source_vwap,b.valid_close) is null or not(coalesce(b.source_vwap,b.valid_close)>0 and abs(coalesce(b.source_vwap,b.valid_close))<'Infinity'::numeric)) over(order by b.interval_start rows between unbounded preceding and current row) as vwap_omitted_minutes,
  count(*) filter(where not b.bar_present or not b.valid_bar)
   over(order by b.interval_start rows between 19 preceding and current row) as missing20
 from base b window w as (order by b.interval_start)
), returns as (
 select l.*,
  case when l.close_n2=2 and l.valid_close>0 and l.close_lag1>0
   then ln(l.valid_close/l.close_lag1) end as logret1
 from lagged l
), rolled as (
 select r.*,
  count(r.logret1) over(order by r.interval_start rows between 4 preceding and current row) as lr_n5,
  count(r.logret1) over(order by r.interval_start rows between 19 preceding and current row) as lr_n20,
  count(r.logret1) over(order by r.interval_start rows between 59 preceding and current row) as lr_n60,
  stddev_samp(r.logret1) over(order by r.interval_start rows between 4 preceding and current row) as rv5,
  stddev_samp(r.logret1) over(order by r.interval_start rows between 19 preceding and current row) as rv20,
  stddev_samp(r.logret1) over(order by r.interval_start rows between 59 preceding and current row) as rv60
 from returns r
), result(session_date,provider,source_symbol,source_feed,provider_interval_start,provider_interval_end,availability_assumed_ts,minute_index,minutes_from_open,minutes_to_close,open,high,low,close,volume,trade_count,provider_vwap,bar_present,bar_valid,dollar_volume_close,dollar_volume_vwap,return_1m,return_5m,return_10m,return_20m,return_60m,return_120m,realized_vol_5m,realized_vol_20m,realized_vol_60m,candle_range_pct,candle_body_pct,upper_wick_pct,lower_wick_pct,range_20m_pct,volume_mean_20m,volume_rel_20m,volume_z_20m,dollar_volume_mean_20m,sma_close_5m,sma_close_20m,sma_close_60m,session_vwap_proxy,close_vs_provider_vwap_bps,close_vs_session_vwap_bps,close_vs_sma20_bps,sma5_vs_sma20_bps,missing_bars_20m,coverage_20m,microstructure_available,micro_trade_count,micro_quote_count,micro_avg_spread_bps,micro_last_spread_bps,micro_trade_imbalance,micro_quote_size_imbalance,microprice_dislocation_bps,quality_flags) as (
select x.session_date,'alpaca'::text as provider,(select id.provider_symbol from id) as source_symbol,x.source_feed,
 x.interval_start as provider_interval_start,x.interval_start+interval '60 seconds' as provider_interval_end,
 x.interval_start+interval '60 seconds'+make_interval(secs=>p_assumed_publication_seconds) as availability_assumed_ts,
 x.minute_index,x.minute_index,
 floor(extract(epoch from (x.regular_close-(x.interval_start+interval '60 seconds')))/60)::integer,
 x.source_open,x.source_high,x.source_low,x.source_close,x.source_volume,x.source_trade_count,x.source_vwap,
 x.bar_present,x.valid_bar,
 case when x.valid_close is not null and x.valid_volume is not null then x.valid_close*x.valid_volume end,
 case when x.source_vwap>0 and abs(x.source_vwap)<'Infinity'::numeric and x.valid_volume is not null then x.source_vwap*x.valid_volume end,
 case when x.close_n2=2 and x.close_lag1>0 then (x.valid_close-x.close_lag1)/x.close_lag1 end,
 case when x.close_n6=6 and x.close_lag5>0 then (x.valid_close-x.close_lag5)/x.close_lag5 end,
 case when x.close_n11=11 and x.close_lag10>0 then (x.valid_close-x.close_lag10)/x.close_lag10 end,
 case when x.close_n21=21 and x.close_lag20>0 then (x.valid_close-x.close_lag20)/x.close_lag20 end,
 case when x.close_n61=61 and x.close_lag60>0 then (x.valid_close-x.close_lag60)/x.close_lag60 end,
 case when x.close_n121=121 and x.close_lag120>0 then (x.valid_close-x.close_lag120)/x.close_lag120 end,
 case when x.lr_n5=5 then x.rv5 end,case when x.lr_n20=20 then x.rv20 end,
 case when x.lr_n60=60 then x.rv60 end,
 case when x.source_open>0 and x.valid_bar then (x.source_high-x.source_low)/x.source_open end,
 case when x.source_open>0 and x.valid_bar then (x.source_close-x.source_open)/x.source_open end,
 case when x.source_open>0 and x.valid_bar then (x.source_high-greatest(x.source_open,x.source_close))/x.source_open end,
 case when x.source_open>0 and x.valid_bar then (least(x.source_open,x.source_close)-x.source_low)/x.source_open end,
 case when x.coherent_n20=20 and x.lo20>0 then (x.hi20-x.lo20)/x.lo20 end,
 case when x.volume_n20=20 then x.vol_mean20 end,
 case when x.volume_n20=20 and x.vol_mean20>0 and x.valid_volume is not null then x.valid_volume/x.vol_mean20 end,
 case when x.volume_n20=20 and x.vol_sd20>0 and x.valid_volume is not null then (x.valid_volume-x.vol_mean20)/x.vol_sd20 end,
 case when x.price_volume_n20=20 then x.dv_mean20 end,
 case when x.close_n5=5 then x.sma5 end,case when x.close_n20=20 then x.sma20 end,
 case when x.close_n60=60 then x.sma60 end,
 case when x.cum_vol>0 then x.cum_pv/x.cum_vol end,
 case when x.valid_close>0 and x.source_vwap>0 and abs(x.source_vwap)<'Infinity'::numeric then (x.valid_close-x.source_vwap)/x.source_vwap*10000 end,
 case when x.valid_close>0 and x.cum_vol>0 and x.cum_pv>0 then (x.valid_close*x.cum_vol-x.cum_pv)/x.cum_pv*10000 end,
 case when x.valid_close>0 and x.close_n20=20 and x.sma20>0 then (x.valid_close-x.sma20)/x.sma20*10000 end,
 case when x.close_n20=20 and x.sma5>0 and x.sma20>0 then (x.sma5-x.sma20)/x.sma20*10000 end,
 x.missing20::integer,case when x.minute_index>=19 then (20-x.missing20)::numeric/20 end,
 x.micro_present,x.micro_trades,x.micro_quotes,x.micro_spread,
 case when x.last_bid_price>0 and x.last_ask_price>=x.last_bid_price and abs(x.last_bid_price)<'Infinity'::numeric and abs(x.last_ask_price)<'Infinity'::numeric and abs(x.last_bid_price+x.last_ask_price)<'Infinity'::numeric
  then (x.last_ask_price-x.last_bid_price)/((x.last_ask_price+x.last_bid_price)/2)*10000 end,
 case when x.buy_volume>=0 and x.sell_volume>=0 and abs(x.buy_volume)<'Infinity'::numeric and abs(x.sell_volume)<'Infinity'::numeric and x.buy_volume+x.sell_volume>0 and abs(x.buy_volume+x.sell_volume)<'Infinity'::numeric then (x.buy_volume-x.sell_volume)/(x.buy_volume+x.sell_volume) end,
 case when x.last_bid_size>=0 and x.last_ask_size>=0 and abs(x.last_bid_size)<'Infinity'::numeric and abs(x.last_ask_size)<'Infinity'::numeric and x.last_bid_size+x.last_ask_size>0 and abs(x.last_bid_size+x.last_ask_size)<'Infinity'::numeric then (x.last_bid_size-x.last_ask_size)/(x.last_bid_size+x.last_ask_size) end,
 case when x.last_trade_price>0 and abs(x.last_trade_price)<'Infinity'::numeric and x.last_bid_price>0 and x.last_ask_price>=x.last_bid_price and abs(x.last_bid_price)<'Infinity'::numeric and abs(x.last_ask_price)<'Infinity'::numeric and abs(x.last_bid_price+x.last_ask_price)<'Infinity'::numeric
  then (2*x.last_trade_price-x.last_bid_price-x.last_ask_price)/(x.last_bid_price+x.last_ask_price)*10000 end,
 jsonb_build_object(
  'source_row_present',x.bar_present,'ohlc_valid',x.valid_bar,'volume_valid',x.valid_volume is not null,
  'exact_clock_grid',true,'missing_bars_20m',x.missing20,'microstructure_available',x.micro_present,
  'identity_mapping_quality',(select id.mapping_quality from id),
  'corporate_action_adjustment_status',coalesce(p_context->>'corporate_action_adjustment_status','UNVERIFIED'),'outcome_independent',true,
  'session_vwap_close_substituted',x.vwap_close_substituted,'session_vwap_omitted_minutes',x.vwap_omitted_minutes,
  'availability_role','ARCHIVED_FINAL_BAR_PROXY_NOT_ORIGINAL_RECEIPT','source_id',x.source_id,'source_sha256',x.source_sha256,
  'raw_record_sha256',x.raw_record_sha256,'record_hash_role',p_context->>'record_hash_role','micro_source_id',x.micro_source_id,'micro_source_sha256',x.micro_source_sha256)
from rolled x
where p_assumed_publication_seconds between 0 and 60
order by x.interval_start
) select to_jsonb(result) from result order by provider_interval_start;
end;
$f$;
revoke all on function population_top3000_202608_202609_v1.d01_feature_grid_json_v2(jsonb,jsonb,timestamptz,timestamptz,jsonb,integer) from public,anon,authenticated;
