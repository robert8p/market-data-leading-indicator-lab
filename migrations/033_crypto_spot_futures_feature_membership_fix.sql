do $$
declare
    v_ddl text;
begin
    select pg_get_functiondef(
        'research_hub.run_crypto_spot_futures15m_feature_screen_v1(uuid,text)'::regprocedure
    ) into v_ddl;

    v_ddl:=replace(
        v_ddl,
        'if not (p_feature_key=any((select feature_keys from research_hub.feature_sets where feature_set_key=r.feature_set_key))) then',
        'if not exists(select 1 from research_hub.feature_sets fs cross join lateral unnest(fs.feature_keys) x(feature_key) where fs.feature_set_key=r.feature_set_key and x.feature_key=p_feature_key) then'
    );

    if position(
        'cross join lateral unnest(fs.feature_keys) x(feature_key)'
        in v_ddl
    )=0 then
        raise exception 'Could not apply typed crypto feature membership guard fix';
    end if;

    execute v_ddl;
end $$;

comment on function research_hub.run_crypto_spot_futures15m_feature_screen_v1(uuid,text) is
'Screens one allowed typed crypto spot/futures feature. Feature membership is validated by unnesting the feature-set key array; thresholds and direction are learned on discovery only, economics are event-level, and p-values are based on instrument-day cluster means.';