CREATE TEMP TABLE basis_test_results(name text PRIMARY KEY,passed boolean,actual jsonb);
DO $tests$
DECLARE m jsonb;c jsonb;r jsonb;rv jsonb;o jsonb;mm jsonb;rr jsonb;cc jsonb;vv jsonb;testname text;fixture record;
BEGIN
WITH a AS MATERIALIZED(SELECT * FROM population_top3000_202608_202609_v1.working_accepted_companies_v1)
SELECT jsonb_build_object('view_sha256','f13272d86a26c1f9034e5fd4c6423484d589b68c04a7cd426416b1007641a6f3','company_count',count(*),'distinct_company_count',count(DISTINCT company_id),'null_company_count',0,'distinct_bindings',1,'cohort_sha256',min(cohort_sha256),'policy_sha256',min(policy_sha256),'membership_acceptance_sha256',min(membership_acceptance_sha256),'companies',jsonb_agg(jsonb_build_object('company_id',company_id) ORDER BY company_id)) INTO m FROM a;
SELECT jsonb_build_object('fixture_only',false,'full_period_verified',true,'window_start','2026-08-01','window_end','2026-09-30','evidence_semantics','QUALIFIED_DATED_INSTRUMENT_INTERVALS_WITH_EXPLICIT_LIFECYCLE_COVERAGE_V2','company_coverage',jsonb_agg(jsonb_build_object('company_id',x->>'company_id','status','QUALIFIED_INTERVALS','source_refs',jsonb_build_array('ROLLBACK_SYNTHETIC_TEST_ONLY'))),'instruments',jsonb_agg(jsonb_build_object('company_id',x->>'company_id','security_id',x->>'company_id','listing_episode_id','rollback-fixture','ticker',x->>'company_id','listing_start','2026-08-01T00:00:00Z','listing_end',null,'eligible_common_equity',true,'security_type','CS','links_evidence',jsonb_build_object('source_refs',jsonb_build_array('ROLLBACK_SYNTHETIC_TEST_ONLY'))))) INTO c FROM jsonb_array_elements(m->'companies')x;
r:=jsonb_build_object('content',c,'release_sha256',encode(sha256(convert_to(c::text,'UTF8')),'hex'),'cohort_sha256',m->>'cohort_sha256','policy_sha256',m->>'policy_sha256','acceptance_sha256',m->>'membership_acceptance_sha256');
rv:=jsonb_build_object('approved',true,'fixture_only',false,'full_period_instrument_evidence_verified',true,'instrument_manifest_sha256',r->>'release_sha256','owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','producer','synthetic-producer','reviewer','synthetic-reviewer');
o:=population_top3000_202608_202609_v1.practical_release_basis_v2();
INSERT INTO basis_test_results VALUES('live_accepted_membership_no_instrument_release',(o->>'membership_pass')::boolean AND NOT(o->>'release_basis_pass')::boolean,o);
o:=population_top3000_202608_202609_v1.practical_release_basis_evaluate_v2(m,r,rv,true);
INSERT INTO basis_test_results VALUES('synthetic_positive_full_basis',(o->>'release_basis_pass')::boolean AND NOT(o->>'family_accepted')::boolean,o);
FOR fixture IN SELECT * FROM (VALUES
 ('missing_membership',NULL::jsonb,r,rv,true),
 ('wrong_count',m||'{"company_count":2999}',r,rv,true),
 ('duplicate_companies',m||'{"distinct_company_count":2999}',r,rv,true),
 ('changed_view',m||'{"view_sha256":"changed"}',r,rv,true),
 ('missing_release',m,NULL::jsonb,rv,true),
 ('wrong_release_hash',m,r||'{"release_sha256":"changed"}',rv,true),
 ('wrong_cohort',m,r||'{"cohort_sha256":"changed"}',rv,true),
 ('missing_review',m,r,NULL::jsonb,true),
 ('self_review',m,r,rv||'{"reviewer":"synthetic-producer"}',true),
 ('review_not_approved',m,r,rv||'{"approved":false}',true),
 ('wrong_review_scope',m,r,rv||'{"scope_sha256":"changed"}',true),
 ('missing_native_commit',m,r,rv,false),
 ('null_native_commit',m,r,rv,NULL::boolean)
 )v(name,mm,rr,vv,nn) LOOP
 o:=population_top3000_202608_202609_v1.practical_release_basis_evaluate_v2(fixture.mm,fixture.rr,fixture.vv,fixture.nn);
 INSERT INTO basis_test_results VALUES(fixture.name,o->>'release_basis_pass'='false',o);
END LOOP;
FOR testname IN SELECT unnest(ARRAY['fixture_marked','wrong_window','missing_company','anchor_not_covered']) LOOP
 cc:=CASE testname WHEN 'fixture_marked' THEN c||'{"fixture_only":true}' WHEN 'wrong_window' THEN c||'{"window_end":"2026-09-29"}' WHEN 'missing_company' THEN jsonb_set(c,'{company_coverage}',(c->'company_coverage')-0) ELSE jsonb_set(c,'{instruments,0,listing_start}','"2026-08-04T00:00:00Z"') END;
 rr:=r||jsonb_build_object('content',cc,'release_sha256',encode(sha256(convert_to(cc::text,'UTF8')),'hex'));
 vv:=rv||jsonb_build_object('instrument_manifest_sha256',rr->>'release_sha256');
 o:=population_top3000_202608_202609_v1.practical_release_basis_evaluate_v2(m,rr,vv,true);
 INSERT INTO basis_test_results VALUES(testname,o->>'release_basis_pass'='false',o);
END LOOP;
IF EXISTS(SELECT 1 FROM basis_test_results WHERE passed IS DISTINCT FROM true) THEN RAISE EXCEPTION 'BASIS_TEST_FAILED';END IF;
END $tests$;
SELECT jsonb_build_object('tests',count(*),'passed',count(*) FILTER(WHERE passed),'cases',jsonb_agg(jsonb_build_object('name',name,'passed',passed,'blocking_conditions',actual->'blocking_conditions') ORDER BY name),'live_basis',(SELECT actual FROM basis_test_results WHERE name='live_accepted_membership_no_instrument_release'),'candidate_hashes',(SELECT jsonb_object_agg(proname,encode(sha256(convert_to(pg_get_functiondef(oid),'UTF8')),'hex')) FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname IN('practical_release_basis_v2','practical_release_basis_evaluate_v2'))) proof FROM basis_test_results;