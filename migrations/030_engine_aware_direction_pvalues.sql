-- Statistical inference must match how trade direction enters the frozen search.
-- Explicit LONG/SHORT hypotheses can use one-sided positive-edge tests.
-- Engines that choose the profitable sign from discovery data pay for that
-- sign selection with a two-sided test unless both directions are separately
-- enumerated in the frozen multiplicity family.

create or replace function research_hub.direction_selected_pvalue_from_effect(
  p_effect_size double precision,p_n bigint
) returns double precision
language sql immutable strict set search_path=pg_catalog,pg_temp as $$
select case when p_n<=1 then null
 else least(1.0,greatest(0.0,erfc(abs(p_effect_size)*sqrt(p_n::double precision)/sqrt(2.0)))) end
$$;

create or replace function research_hub.enforce_positive_edge_test_pvalue()
returns trigger
language plpgsql security invoker set search_path=research_hub,pg_temp as $$
declare v_engine text; v_mode text;
begin
  if new.effect_size is null or new.n is null or new.n<=1 then return new; end if;
  v_engine:=coalesce(new.metadata->>'engine',(select r.config->>'engine' from research_hub.experiment_runs r where r.run_id=new.run_id));
  v_mode:=case
    when new.metadata ? 'position_side' or v_engine='research_hub_quote_exec_panel_v1' then 'explicit_direction_one_sided'
    when v_engine in ('research_hub_panel_v1','research_hub_chunked_feature_v1','research_hub_multiquantile_v2','research_hub_univariate_tail_v1') then 'direction_selected_two_sided'
    else 'explicit_direction_one_sided'
  end;
  if v_mode='direction_selected_two_sided' then
    new.p_value:=research_hub.direction_selected_pvalue_from_effect(new.effect_size,new.n);
  else
    new.p_value:=research_hub.positive_edge_pvalue_from_effect(new.effect_size,new.n);
  end if;
  new.metadata:=coalesce(new.metadata,'{}'::jsonb)||jsonb_build_object('p_value_mode',v_mode);
  return new;
end $$;

update research_hub.experiment_tests t
set p_value = case
      when t.metadata ? 'position_side' or coalesce(t.metadata->>'engine',r.config->>'engine')='research_hub_quote_exec_panel_v1'
        then research_hub.positive_edge_pvalue_from_effect(t.effect_size,t.n)
      when coalesce(t.metadata->>'engine',r.config->>'engine') in ('research_hub_panel_v1','research_hub_chunked_feature_v1','research_hub_multiquantile_v2','research_hub_univariate_tail_v1')
        then research_hub.direction_selected_pvalue_from_effect(t.effect_size,t.n)
      else research_hub.positive_edge_pvalue_from_effect(t.effect_size,t.n)
    end,
    metadata=coalesce(t.metadata,'{}'::jsonb)||jsonb_build_object('p_value_mode',case
      when t.metadata ? 'position_side' or coalesce(t.metadata->>'engine',r.config->>'engine')='research_hub_quote_exec_panel_v1' then 'explicit_direction_one_sided'
      when coalesce(t.metadata->>'engine',r.config->>'engine') in ('research_hub_panel_v1','research_hub_chunked_feature_v1','research_hub_multiquantile_v2','research_hub_univariate_tail_v1') then 'direction_selected_two_sided'
      else 'explicit_direction_one_sided' end)
from research_hub.experiment_runs r
where r.run_id=t.run_id and t.effect_size is not null and t.n>1;

with ranked as(
  select test_id,run_id,p_value,row_number() over(partition by run_id order by p_value,test_id) rn,count(*) over(partition by run_id) m
  from research_hub.experiment_tests where p_value is not null
),raw_q as(
  select test_id,run_id,rn,least(1.0,p_value*m::double precision/rn::double precision) raw_q from ranked
),adjusted as(
  select test_id,least(1.0,min(raw_q) over(partition by run_id order by rn desc rows between unbounded preceding and current row)) q_value from raw_q
)
update research_hub.experiment_tests t set q_value=a.q_value from adjusted a where t.test_id=a.test_id;

update research_hub.operating_policies
set policy=policy||jsonb_build_object('p_value_semantics','Explicit LONG/SHORT hypotheses may use one-sided positive-edge tests. Any engine that chooses direction from the same discovery data must use sign-selection-adjusted/two-sided inference or count both directions in the frozen multiplicity family.'),updated_at=now()
where policy_key='market_edge_core_v1';