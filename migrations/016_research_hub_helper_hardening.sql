alter function research_hub.positive_edge_pvalue_from_effect(double precision,bigint) set search_path=pg_catalog,pg_temp;

-- Compatibility hardening when the transitional helper exists in an upgraded database.
do $$
begin
    if to_regprocedure('research_hub.positive_edge_pvalue(double precision,double precision,bigint)') is not null then
        execute 'alter function research_hub.positive_edge_pvalue(double precision,double precision,bigint) set search_path=pg_catalog,pg_temp';
    end if;
end $$;
