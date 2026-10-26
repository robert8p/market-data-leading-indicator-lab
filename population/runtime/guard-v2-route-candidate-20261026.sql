-- Candidate: distinct review and atomic recovery qualification required before production.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.source_guarded_stage_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $route$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;p record;k text;r jsonb;
BEGIN
tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
IF t.payload->>'execution_route' IS DISTINCT FROM 'SOURCE_GUARDED_STAGE_V2_20261026'
OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_adapter WHERE family_id=t.family_id AND stage=t.stage AND function_oid='population_top3000_202608_202609_v1.source_guarded_stage_v2(uuid,text)'::regprocedure)
THEN RAISE EXCEPTION 'GUARDED_V2_EXPLICIT_ROUTE_BINDING_REQUIRED';END IF;
FOR p IN SELECT * FROM (VALUES ('population_top3000_202608_202609_v1.alpaca_sip_resource_guard_v2()','7d6599c118451b4688779d30c0be3fa504e37e7db2707fc4873ec9574fd31cef'),('population_top3000_202608_202609_v1.massive_news_resource_guard_v2()','5f40e36f64168df192db64819d0149394e660ea3b2c286ad6235c7d519cbf280'),('population_top3000_202608_202609_v1.alpaca_sip_normalize_v2(uuid,text)','28286e838739edb049823a45abdfe646f00ef4a19a58a9d561581ebd4ea26139'),('population_top3000_202608_202609_v1.massive_news_derive_v2(uuid,text)','92c43c8010c00f8a42cc019e4f33f36778c904e8daf701bed2424b9139e51a88')) x(fn,h) LOOP
IF to_regprocedure(p.fn) IS NULL OR encode(sha256(convert_to(pg_get_functiondef(to_regprocedure(p.fn)),'UTF8')),'hex') IS DISTINCT FROM p.h THEN RAISE EXCEPTION 'GUARDED_V2_FUNCTION_CHANGED %',p.fn;END IF;
END LOOP;
IF t.family_id='N01' AND t.stage='DERIVE' THEN
PERFORM population_top3000_202608_202609_v1.massive_news_context_v1(tid);
r:=population_top3000_202608_202609_v1.massive_news_derive_v2(p_attempt,p_token);
-- This path rechecks a retained archive; total records are not newly acquired units.
r:=jsonb_set(r,'{commit_receipt,new_committed_units}','0'::jsonb);
ELSIF t.family_id='P01' AND t.stage='NORMALIZE' AND t.payload->>'kind'='ALPACA_SIP_GAP_BATCH_V1' THEN
r:=population_top3000_202608_202609_v1.alpaca_sip_normalize_v2(p_attempt,p_token);
ELSE RAISE EXCEPTION 'GUARDED_V2_UNSUPPORTED_FAMILY_STAGE';END IF;
RETURN r||jsonb_build_object('execution_route','SOURCE_GUARDED_STAGE_V2_20261026','legacy_source_bundle_preserved',true);
END $route$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.source_guarded_stage_v2(uuid,text) FROM PUBLIC,anon,authenticated;
