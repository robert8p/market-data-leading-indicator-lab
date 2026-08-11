-- Tail-screen direction is learned from discovery data. A one-sided p-value
-- after choosing the profitable sign is anti-conservative; use a two-sided
-- normal approximation so the sign-selection degree of freedom is paid for.

create or replace function research_hub.positive_edge_pvalue_from_effect(
    p_effect_size double precision,
    p_n bigint
)
returns double precision
language sql
immutable
strict
set search_path=pg_catalog,pg_temp
as $$
select case
    when p_n<=1 then null
    else least(1.0,greatest(0.0,
        erfc(abs(p_effect_size)*sqrt(p_n::double precision)/sqrt(2.0))
    ))
end
$$;

create or replace function research_hub.positive_edge_pvalue(
    p_mean double precision,
    p_sd double precision,
    p_n bigint
)
returns double precision
language sql
immutable
strict
set search_path=pg_catalog,pg_temp
as $$
select case
    when p_sd<=0 or p_n<=1 then null
    else least(1.0,greatest(0.0,
        erfc(abs(p_mean/(p_sd/sqrt(p_n::double precision)))/sqrt(2.0))
    ))
end
$$;

comment on function research_hub.positive_edge_pvalue_from_effect(double precision,bigint)
is 'Direction-selection-adjusted two-sided large-sample normal p-value for a mean-net-return effect. Tail engines choose long/short direction in discovery, so the test pays for that sign selection.';

comment on function research_hub.positive_edge_pvalue(double precision,double precision,bigint)
is 'Direction-selection-adjusted two-sided large-sample normal p-value for H0 mean net return = 0.';

-- Canonical trigger: engines may omit p_value, but cannot silently bypass FDR.
create or replace function research_hub.enforce_positive_edge_test_pvalue()
returns trigger
language plpgsql
security invoker
set search_path=research_hub,pg_temp
as $$
begin
    if new.effect_size is not null and new.n is not null and new.n>1 then
        new.p_value:=research_hub.positive_edge_pvalue_from_effect(new.effect_size,new.n);
    end if;
    return new;
end
$$;

drop trigger if exists trg_experiment_tests_p_value on research_hub.experiment_tests;
drop function if exists research_hub.populate_experiment_test_p_value();
drop trigger if exists trg_research_hub_positive_edge_pvalue on research_hub.experiment_tests;
create trigger trg_research_hub_positive_edge_pvalue
before insert or update of effect_size,n,p_value on research_hub.experiment_tests
for each row execute function research_hub.enforce_positive_edge_test_pvalue();

-- Repair stored tests and recompute BH-FDR independently within each run.
update research_hub.experiment_tests
set p_value=research_hub.positive_edge_pvalue_from_effect(effect_size,n)
where effect_size is not null and n>1;

with ranked as(
    select test_id,run_id,p_value,
           row_number() over(partition by run_id order by p_value,test_id) rn,
           count(*) over(partition by run_id) m
    from research_hub.experiment_tests
    where p_value is not null
), raw_q as(
    select test_id,run_id,rn,
           least(1.0,p_value*m::double precision/rn::double precision) raw_q
    from ranked
), adjusted as(
    select test_id,
           least(1.0,min(raw_q) over(
               partition by run_id order by rn desc
               rows between unbounded preceding and current row
           )) q_value
    from raw_q
)
update research_hub.experiment_tests t
set q_value=a.q_value
from adjusted a
where t.test_id=a.test_id;
