CREATE OR REPLACE FUNCTION market_governance.remediation_listed_prices_as_of_v2(p_key int,p_date date,p_decision timestamptz,p_archive boolean)
RETURNS SETOF market_governance.remediation_listed_price_release_v2 LANGUAGE sql STABLE SECURITY DEFINER SET search_path='pg_catalog' AS $$
 SELECT p FROM market_governance.remediation_listed_price_release_v2 p
 WHERE p.instrument_key=p_key AND p.session_date=p_date AND p_date BETWEEN '2025-09-01' AND '2026-07-31' AND p.bar_end<=p_decision
 AND (p_archive IS TRUE OR greatest(p.available_at,p.promoted_at)<=p_decision)
 AND EXISTS(SELECT 1 FROM market_governance.remediation_source_response_v1 s JOIN market_governance.remediation_source_batch_v1 b ON b.batch_id=s.batch_id WHERE s.source_id=p.source_id AND b.current_source_id=s.source_id AND b.status='COMPLETE')
 AND NOT EXISTS(SELECT 1 FROM market_governance.remediation_listed_identity_exception_v1 e WHERE e.observation_id=p.listing_observation_id AND e.active)
$$;
REVOKE ALL ON FUNCTION market_governance.remediation_listed_prices_as_of_v2(int,date,timestamptz,boolean) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION market_governance.remediation_listed_prices_as_of_v2(int,date,timestamptz,boolean) TO service_role;
CREATE OR REPLACE FUNCTION public.market_data_listed_prices_as_of_remediated_v2(p_instrument_key int,p_session_date date,p_decision_ts timestamptz,p_allow_archived_proxy boolean DEFAULT false)
RETURNS SETOF market_governance.remediation_listed_price_release_v2 LANGUAGE sql STABLE SECURITY INVOKER SET search_path='pg_catalog' AS $$
 SELECT * FROM market_governance.remediation_listed_prices_as_of_v2(p_instrument_key,p_session_date,p_decision_ts,p_allow_archived_proxy)
$$;
REVOKE ALL ON FUNCTION public.market_data_listed_prices_as_of_remediated_v2(int,date,timestamptz,boolean) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.market_data_listed_prices_as_of_remediated_v2(int,date,timestamptz,boolean) TO service_role;

