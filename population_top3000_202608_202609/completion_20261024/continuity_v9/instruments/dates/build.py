from pathlib import Path
b=Path('continuity_v9/instruments/dates')
s=Path('continuity_v7/ref01/instrument_v5/worker.sql').read_text()
s=s.replace('ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;\n asof', 'ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;\n plan population_top3000_202608_202609_v1.ref01_date_plan_v9%rowtype;\n asof')
a=s.index(' OR (t.input_versions->>\'transport_bundle_sha256\'')
z=s.index("\n OR t.input_versions->>'policy_sha256'",a)
s=s[:a]+s[z:]
needle=" SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ref01_date_request_v4 WHERE request_key=t.payload->>'request_key';"
s=s.replace(needle,needle+"""
 SELECT p.* INTO STRICT plan FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 p
 JOIN population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr USING(plan_id)
 WHERE pr.request_key=r.request_key AND pr.as_of=(r.manifest->>'as_of')::date
 AND pr.manifest_sha256=r.manifest_sha256 AND pr.input_versions_sha256=encode(sha256(convert_to(r.input_versions::text,'UTF8')),'hex');
 IF plan.enabled IS DISTINCT FROM true OR plan.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(plan.content::text,'UTF8')),'hex')
 OR plan.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4()
 OR plan.content->>'owner_id' IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
 OR plan.content->>'owner_fence' IS DISTINCT FROM '3' OR plan.content->>'stop_generation' IS DISTINCT FROM '1'
 THEN RAISE EXCEPTION 'REF01_CENSUS_PLAN_CODE_OR_AUTHORITY_CHANGED';END IF;
""")
s=s.replace("r.manifest->>'as_of' NOT IN('2026-08-01','2026-08-02','2026-08-04','2026-08-05')", "NOT (plan.content->'dates' ? (r.manifest->>'as_of'))")
s=s.replace("(r.manifest->>'max_pages')::integer IS DISTINCT FROM 14", "(r.manifest->>'max_pages')::integer NOT BETWEEN 1 AND 100000")
s=s.replace(">100000000 OR 16777216+", ">(plan.content->>'max_source_bytes')::bigint OR 16777216+")
s=s.replace("relname LIKE 'ref01_date_%' AND relkind='r')>134217728", "(relname LIKE 'ref01_date_%' OR relname='ref01_practical_dated_instrument_v3') AND relkind='r')>(plan.content->>'max_relation_bytes')::bigint")
s=s.replace("THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'request_key'", "THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'maximum_http_invocations',(plan.content->>'max_http_invocations')::bigint,'maximum_provider_calls',(plan.content->>'max_provider_calls')::bigint,'request_key'")
s=s.replace("OR used>=56 OR", "OR used>=(c->>'maximum_http_invocations')::bigint OR")
a=s.index(" IF v_next_url IS NULL AND c->>'as_of'='2026-08-04'")
z=s.index(" RETURN population_top3000_202608_202609_v1.pipeline_finish_v1",a)
s=s[:a]+s[z:]
# maintenance tick queues next date only after this immutable terminal commit, rather than before finish
(b/'worker.sql').write_text(s)
