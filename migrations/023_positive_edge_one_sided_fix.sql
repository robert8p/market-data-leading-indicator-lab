create or replace function research_hub.positive_edge_pvalue(
    p_mean double precision,
    p_sd double precision,
    p_n bigint
)
returns double precision
language sql
immutable strict
set search_path=pg_catalog,pg_temp
as $$
select case
    when p_sd<=0 or p_n<=1 then null
    else least(
        1.0,
        greatest(
            0.0,
            0.5*erfc((p_mean/(p_sd/sqrt(p_n::double precision)))/sqrt(2.0))
        )
    )
end
$$;

create or replace function research_hub.positive_edge_pvalue_from_effect(
    p_effect_size double precision,
    p_n bigint
)
returns double precision
language sql
immutable strict
set search_path=pg_catalog,pg_temp
as $$
select case
    when p_n<=1 then null
    else least(
        1.0,
        greatest(
            0.0,
            0.5*erfc((p_effect_size*sqrt(p_n::double precision))/sqrt(2.0))
        )
    )
end
$$;

comment on function research_hub.positive_edge_pvalue(double precision,double precision,bigint) is
'One-sided normal-approximation p-value for H1: mean net edge > 0. Negative effects therefore have p>0.5 rather than appearing significant.';

comment on function research_hub.positive_edge_pvalue_from_effect(double precision,bigint) is
'One-sided normal-approximation p-value for H1: effect > 0. Negative effects therefore have p>0.5 rather than appearing significant.';
