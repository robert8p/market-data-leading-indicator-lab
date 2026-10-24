BEGIN;
SET LOCAL lock_timeout='5s'; SET LOCAL statement_timeout='90s';
DO $g$
DECLARE r record; h text;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_event e WHERE e.event_id IN(29634,29635) AND e.event='ENGINEERING_ASSIGNMENT_CLAIMED_V5' AND e.details->>'executor'='root:top3000-completion-20261024-root-5db48b32d103' AND (e.details->>'expires_at')::timestamptz>clock_timestamp() AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event d WHERE d.event IN('ENGINEERING_ASSIGNMENT_COMPLETED_V5','ENGINEERING_ASSIGNMENT_EFFECTS_RECONCILED_RELEASE_V5') AND d.details->>'claim_event_id'=e.event_id::text))<>2 THEN RAISE EXCEPTION 'EXACT_ROOT_CLAIMS_REQUIRED';END IF;
 FOR r IN SELECT key,value FROM jsonb_each_text($hash${"pipeline_tick_v1":"e697b7f01821471ceaabf5cb13870976937b69d6d82f220c553dc0794a8ec89b","ref01_date_admit_v4":"8e38052f114ca6ddfb43b360c02dbe191e412d7bae7621eabd2cf6462c070da4","ref01_date_bundle_v4":"cc5437f37289948c70d0ec44b094f06ff7b047832dc7dfea1d639952ac0774bb","ref01_date_commit_v4":"10373f73f6ef3a6350109c8c7c54ef72574a5422413152ede6e1e2310871cd46","ref01_date_acquire_v4":"d85e4c2ea0e13b4150219ecc2819338fd751e7e142e195846147e2d97056f4f6","ref01_date_context_v4":"0da8c55f043204516a064d507439b0b45b7158443f1fed27eaf072cf21d24739"}$hash$::jsonb) LOOP
 SELECT encode(sha256(convert_to(pg_get_functiondef(oid),'UTF8')),'hex') INTO STRICT h FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname=r.key;
 IF h IS DISTINCT FROM r.value THEN RAISE EXCEPTION 'CODE_PREIMAGE_CHANGED %',r.key;END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id) WHERE t.family_id='REF01' AND a.outcome='RUNNING') THEN RAISE EXCEPTION 'REF01_ACTIVE_OPERATION';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
END;$g$;


CREATE TEMP TABLE ref01_integration_metadata_v9(review_event_id bigint,admission_event_id bigint);
WITH e AS (INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_CONTINUATION_INDEPENDENT_REVIEW_V9',$j${"reviewer": "/root/instrument_review", "producer": "/root/instrument_dates", "integrator": "/root", "status": "APPROVED_FINITE_FULL_PERIOD_CONTINUATION_REPAIR", "approved": true, "code_review": "PASS", "actual_postgresql_rollback": "PASS", "test_groups": 11, "reviewed_plan": {"dates": 60, "retained_aug3_separate": true, "provider_http_cumulative_ceiling": 1200, "source_byte_ceiling": 300000000, "combined_relation_guard": 3000000000, "global_limits_unchanged": true, "new_root_admission_required": true, "inherited_assignment_144_calls_not_reused_as_new_authority": true}, "review_findings_fixed": ["SQL NULL safety preserves unknown dependency holds", "Context and continuation use identical conservative relation+2*raw+16MiB formula", "Source dispatch requires120seconds remaining admission", "Known waits exclude any previous HTTP or provider lease evidence", "Terminal progression checks retained original body and terminal cursor", "Error wrapper preserves existing blocked tasks and isolates only affected source lane; code integrity fault holds affected ready stages", "Bundle pins all10 processing/admission/continuation functions"], "actual_tests": ["60-date fixed population and old request/input hash contexts", "Expired finite source window requeue without resetting counters/cursor", "Null dependency stays held", "Stop and changed owner reject", "Changed code bundle produces executable repair", "Capacity boundary creates deduplicated clearing work", "Synthetic terminal source advances Aug6 beyond old whitelist idempotently", "Zero HTTP/provider calls in test", "90second source window rejected before HTTP", "Exact integrated tick isolates helper failure and executes disjoint DATABASE work"], "synthetic_fixture_note": "Terminal progression fixture modifies source/body hashes only within rolled-back transaction; it is not production source acceptance. Production Aug4 terminal and actual Aug5 output separately verified.", "projected": {"bundle": "f7af9ad2ab69f104c0c85dae02d7d2b65f3df07f6849ca2d8118e6828da6e35a", "manifest": {"adapters": "caccf5f74fc2e567f594b5b6ab7d17fb98083082cce5d630121ece4d53adc40a", "functions": "6c5613131161ead60b22cf22f3b5859bc3784b1a40b3134da35ad5ac59f2352c"}, "functions": {"pipeline_tick_v1": "c8df19b574c748d5494b0200b981cbd338864c6ea57e7e2240dce14aff5fc2d1", "ref01_date_admit_v4": "180e882e09a2183788fc63f1f8f5db9c34cf0ad463748848e0f1656f985516e7", "ref01_date_bundle_v4": "91705e61919dd4f874859ac99b93a6919e860c824838eb796804ac9e40aa98df", "ref01_date_commit_v4": "0e92ae64df07e1b605f0678d5e3c8931eeaf7055fd5d183dd975db606308fc1f", "ref01_date_acquire_v4": "02b8a28363dab46f274860d43078e15349667e552e73064a6bd8274318a10b07", "ref01_date_context_v4": "c0b3bdb12097583964cb9e3d11e6f7e0500d31ce0470d94815c40afdbe5ba319", "ref01_date_continue_v9": "b7283dc0fab2abcdeedd754dabcd2a4d55eb7eb113199576f15e45af2b4684b3", "ref01_date_continue_impl_v9": "ca43dbdd5a984456e6f71104c291989a27a48ab85f03a2952c1fccea88902791"}, "plan_sha256": "f6596e461a84315e6cadfc11ec8bf1ef52cd0d5b3011d4df0353d5976370c319", "manifest_sha256": "96a9d25997ed777cba133c800c633256c91c6f14802461b357383f546ae49d44"}, "file_sha256": {"schema.sql": "b39123ee248a0e4f3a6f2c336c7ae545bea5f5a2eb0fb02461a0ea4acc97da7c", "worker.sql": "0bf2c862bb0f2bdbe89c40072de032f1f1fb136c9e3bab0cb0740192b8a17d4b", "admit.sql": "11e9f9a3624876a7e152179e4eb7d63f2a42046342519581110aebed7f3c242e", "continue.sql": "4108564c28b9e52cd7c3594497e6a44bb3ef18c12ec9bd619fe6d024eb9a28d5", "wrapper.sql": "f4f088291074ca2ebb58b24f95b21d25bf7407dc07ad7fc74ee6283945a2c792", "bundle.sql": "4cb8b76af237b0657c7b215576c55e9c51d21fee086a38c1202fd2e7f36456ec", "seed.sql": "7e38013043e88da1f42934076bd56bf358708d7d2b1e02dbfe3df000eb031e83"}, "root_file_sha256": {"pipeline_tick_dates_v9.sql": "13c19fad4d0a6cca305e32a1a552fa8ebec59ac4fc61e7a2942a916392e7966c", "date_integration_header.sql": "7d65b9f092f046f83afccb16ac86715bf7e96036cc690d7090ab0637802b6d5c"}, "conditions": ["Exact current owner/fence/stop and claims29634/29635 must pass integration header", "Record new root operational admission with exact plan hash and independent review before use", "Requalify current combined recovery manifest", "Observe automatic Aug5-to-Aug6 production output and independently verify exact source/normalized hashes", "No family acceptance or full instrument history completion implied"], "production_writes_committed_by_reviewer": 0, "owner_id": "top3000-completion-20261024-root-5db48b32d103", "owner_fence": 3, "stop_generation": 1, "scope_sha256": "8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd", "reviewed_manifest_sha256": "96a9d25997ed777cba133c800c633256c91c6f14802461b357383f546ae49d44", "code_manifest": {"adapters": "caccf5f74fc2e567f594b5b6ab7d17fb98083082cce5d630121ece4d53adc40a", "functions": "6c5613131161ead60b22cf22f3b5859bc3784b1a40b3134da35ad5ac59f2352c"}}$j$::jsonb) RETURNING event_id) INSERT INTO ref01_integration_metadata_v9(review_event_id) SELECT event_id FROM e;
WITH e AS (INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) SELECT 'REF01_FULL_PERIOD_FINITE_ADMISSION_V9',$j${"actor": "root:top3000-completion-20261024-root-5db48b32d103", "authority_event_id": 9528, "owner_id": "top3000-completion-20261024-root-5db48b32d103", "owner_fence": 3, "stop_generation": 1, "scope_sha256": "8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd", "claim_event_ids": [29634, 29635], "assignment_event_id": 28605, "plan_id": "ref01-date-fullperiod-v9", "plan_sha256": "f6596e461a84315e6cadfc11ec8bf1ef52cd0d5b3011d4df0353d5976370c319", "max_provider_calls": 1200, "max_http_invocations": 1200, "max_source_bytes": 300000000, "max_combined_relation_bytes": 3000000000, "date_count": 60, "period_start": "2026-08-01", "period_end": "2026-09-30", "global_limits_unchanged": true, "counters_preserved": true, "original_deadline_preserved": true, "authority_basis": "Current user direction to complete instrument continuation and existing authority9528; new finite source admission supersedes only inherited144call allowance; original assignment history remains unchanged.", "family_accepted": false}$j$::jsonb || jsonb_build_object('independent_review_event_id',review_event_id,'admitted_at',clock_timestamp()) FROM ref01_integration_metadata_v9 RETURNING event_id) UPDATE ref01_integration_metadata_v9 SET admission_event_id=e.event_id FROM e;

CREATE TABLE population_top3000_202608_202609_v1.ref01_date_plan_v9(
 plan_id text PRIMARY KEY, content jsonb NOT NULL, content_sha256 text NOT NULL,
 code_bundle_sha256 text NOT NULL, enabled boolean NOT NULL DEFAULT false,
 last_blocker text, updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(content_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex')),
 CHECK(code_bundle_sha256 ~ '^[0-9a-f]{64}$'),
 CHECK(jsonb_typeof(content->'dates')='array' AND jsonb_array_length(content->'dates')=60),
 CHECK(content ?& ARRAY['max_provider_calls','max_http_invocations','max_source_bytes','max_relation_bytes','max_pages_per_date','initial_page_tranche','renewal_page_tranche','owner_id','owner_fence','stop_generation','dates','input_versions']),
 CHECK((content->>'max_provider_calls')::bigint BETWEEN 1 AND 1000000 AND (content->>'max_http_invocations')::bigint BETWEEN 1 AND 1000000),
 CHECK((content->>'max_source_bytes')::bigint BETWEEN 8388608 AND 100000000000 AND (content->>'max_relation_bytes')::bigint BETWEEN 16777216 AND 1000000000000),
 CHECK((content->>'max_pages_per_date')::int BETWEEN 1 AND 100000 AND (content->>'initial_page_tranche')::int BETWEEN 1 AND 100000 AND (content->>'renewal_page_tranche')::int BETWEEN 1 AND 100000)
);
CREATE UNIQUE INDEX ref01_date_one_enabled_plan_v9 ON population_top3000_202608_202609_v1.ref01_date_plan_v9(enabled) WHERE enabled;
CREATE TABLE population_top3000_202608_202609_v1.ref01_date_plan_request_v9(
 plan_id text NOT NULL REFERENCES population_top3000_202608_202609_v1.ref01_date_plan_v9,
 as_of date NOT NULL, request_key text NOT NULL UNIQUE REFERENCES population_top3000_202608_202609_v1.ref01_date_request_v4,
 manifest_sha256 text NOT NULL, input_versions_sha256 text NOT NULL,
 PRIMARY KEY(plan_id,as_of), CHECK(as_of BETWEEN DATE '2026-08-01' AND DATE '2026-09-30')
);
ALTER TABLE population_top3000_202608_202609_v1.ref01_date_plan_v9 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ref01_date_plan_request_v9 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ref01_date_plan_v9,population_top3000_202608_202609_v1.ref01_date_plan_request_v9 FROM PUBLIC,anon,authenticated;

;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_context_v4(p_task uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 r population_top3000_202608_202609_v1.ref01_date_request_v4%rowtype;
 prior population_top3000_202608_202609_v1.ref01_date_page_v4%rowtype;
 admission population_top3000_202608_202609_v1.ref01_date_admission_v4%rowtype;
 ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;
 plan population_top3000_202608_202609_v1.ref01_date_plan_v9%rowtype;
 asof date;first_url text;url text;k text;n integer;lim integer;page_only integer;used_calls integer;
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_practical_guard_v1();
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_practical_membership_v1 WHERE snapshot_sha256='2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec' AND acceptance_sha256='20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9' AND acceptance_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'REF01_MEMBERSHIP_CHANGED';END IF;
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 IF t.family_id IS DISTINCT FROM 'REF01' OR t.payload->>'kind' IS DISTINCT FROM 'REF01_CENSUS_SOURCE_V1'
 OR t.input_versions->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'

 OR t.input_versions->>'policy_sha256' IS DISTINCT FROM '3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb'
 OR t.input_versions->>'cohort_sha256' IS DISTINCT FROM '2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec'
 OR t.input_versions->>'membership_acceptance_sha256' IS DISTINCT FROM '20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9' THEN RAISE EXCEPTION 'REF01_CENSUS_SCOPE_OR_CODE_NOT_ADMITTED';END IF;
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ref01_date_request_v4 WHERE request_key=t.payload->>'request_key';
 SELECT p.* INTO STRICT plan FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 p
 JOIN population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr USING(plan_id)
 WHERE pr.request_key=r.request_key AND pr.as_of=(r.manifest->>'as_of')::date
 AND pr.manifest_sha256=r.manifest_sha256 AND pr.input_versions_sha256=encode(sha256(convert_to(r.input_versions::text,'UTF8')),'hex');
 IF (t.stage='ACQUIRE' AND plan.enabled IS DISTINCT FROM true) OR plan.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(plan.content::text,'UTF8')),'hex')
 OR plan.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4()
 OR plan.content->>'owner_id' IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103'
 OR plan.content->>'owner_fence' IS DISTINCT FROM '3' OR plan.content->>'stop_generation' IS DISTINCT FROM '1'
 THEN RAISE EXCEPTION 'REF01_CENSUS_PLAN_CODE_OR_AUTHORITY_CHANGED';END IF;

 IF r.input_versions IS DISTINCT FROM t.input_versions OR r.manifest_sha256 IS DISTINCT FROM t.payload->>'request_manifest_sha256'
 OR r.manifest_sha256 IS DISTINCT FROM encode(sha256(convert_to(r.manifest::text,'UTF8')),'hex')
 OR r.request_key IS DISTINCT FROM 'ref01-date-v4:'||r.manifest_sha256 THEN RAISE EXCEPTION 'REF01_CENSUS_MANIFEST_CHANGED';END IF;
 IF r.manifest->>'as_of' IS NULL OR NOT (plan.content->'dates' ? (r.manifest->>'as_of')) OR r.manifest->>'as_of'!~'^2026-(08|09)-[0-9]{2}$'
 OR r.manifest->'active' IS DISTINCT FROM 'true'::jsonb OR r.manifest->>'population' IS DISTINCT FROM 'ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER'
 OR coalesce(r.manifest->>'max_pages','')!~'^[0-9]{1,6}$' OR (r.manifest->>'max_pages')::integer NOT BETWEEN 1 AND 100000
 THEN RAISE EXCEPTION 'REF01_CENSUS_EXACT_QUERY_REQUIRED';END IF;
 asof:=(r.manifest->>'as_of')::date;
 IF asof<DATE '2026-08-01' OR asof>DATE '2026-09-30' THEN RAISE EXCEPTION 'REF01_CENSUS_ASOF_OUT_OF_SCOPE';END IF;
 first_url:='https://api.massive.com/v3/reference/tickers?date='||asof::text||'&sort=ticker&limit=1000&order=asc&active=true&locale=us&market=stocks';
 IF r.manifest->>'first_url' IS DISTINCT FROM first_url OR r.manifest->>'max_response_bytes' IS DISTINCT FROM '8388608'
 OR r.manifest->>'max_runtime_ms' IS DISTINCT FROM '45000' THEN RAISE EXCEPTION 'REF01_CENSUS_QUERY_BOUNDS_CHANGED';END IF;
 k:='ref01-date-archive-v4:'||encode(sha256(convert_to(r.request_key||E'\n'||r.input_versions::text,'UTF8')),'hex');
 IF t.payload ? 'source_page_number' THEN
  IF coalesce(t.payload->>'source_page_number','')!~'^[0-9]{1,6}$' OR t.stage='ACQUIRE' THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_TASK_INVALID';END IF;
  page_only:=(t.payload->>'source_page_number')::integer;
  IF page_only NOT BETWEEN 1 AND (r.manifest->>'max_pages')::integer OR t.partition_key IS DISTINCT FROM r.request_key||':page:'||page_only::text THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_TASK_INVALID';END IF;
 ELSE
  IF t.partition_key IS DISTINCT FROM r.request_key THEN RAISE EXCEPTION 'REF01_CENSUS_STREAM_TASK_INVALID';END IF;
 END IF;
 IF t.stage<>'ACQUIRE' THEN RETURN r.manifest||jsonb_build_object('state','READY','request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256,'archive_key',k,'family_id',t.family_id,'partition_key',t.partition_key,'source_page_number',page_only);END IF;
 SELECT * INTO STRICT admission FROM population_top3000_202608_202609_v1.ref01_date_admission_v4 WHERE request_key=r.request_key;
 SELECT * INTO STRICT ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 IF admission.owner_id IS DISTINCT FROM ctl.owner_id OR admission.owner_fence IS DISTINCT FROM ctl.owner_fence OR admission.stop_generation IS DISTINCT FROM ctl.stop_generation THEN RAISE EXCEPTION 'REF01_CENSUS_TRANCHE_OWNER_CHANGED';END IF;
 SELECT count(*) INTO used_calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a ON a.attempt_id=l.attempt_id JOIN population_top3000_202608_202609_v1.pipeline_task pt ON pt.task_id=a.task_id WHERE pt.family_id='REF01' AND pt.payload->>'request_key'=r.request_key AND pt.stage='ACQUIRE' AND l.provider='Massive' AND l.attempt_id IS DISTINCT FROM t.attempt_id;
 IF coalesce(t.checkpoint->>'next_page','1')!~'^[0-9]{1,6}$' THEN RAISE EXCEPTION 'REF01_CENSUS_CHECKPOINT_INVALID';END IF;
 n:=coalesce(t.checkpoint->>'next_page','1')::integer;lim:=(r.manifest->>'max_pages')::integer;
 IF n NOT BETWEEN 1 AND 100001 THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_BOUND';END IF;
 IF n=1 THEN url:=first_url;ELSE
  SELECT * INTO prior FROM population_top3000_202608_202609_v1.ref01_date_page_v4 WHERE archive_key=k AND page_number=n-1;
  IF NOT FOUND OR prior.next_url IS NULL OR t.checkpoint->>'last_source_id' IS DISTINCT FROM prior.source_id
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=prior.source_id AND d.source_sha256=prior.source_sha256 AND d.source_sha256=t.checkpoint->>'last_source_sha256' AND d.source_sha256=encode(sha256(convert_to(d.content_text,'UTF8')),'hex') AND d.content_bytes=octet_length(d.content_text))
  THEN RAISE EXCEPTION 'REF01_CENSUS_CONTINUATION_PROOF_MISSING';END IF;
  url:=prior.next_url;
  IF url!~'^https://api[.]massive[.]com/v3/reference/tickers[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(url)>16384 THEN RAISE EXCEPTION 'REF01_CENSUS_CURSOR_SCOPE_INVALID';END IF;
 END IF;
 RETURN r.manifest||jsonb_build_object('state',CASE WHEN (SELECT coalesce(sum(wire_bytes),0)+8388608 FROM population_top3000_202608_202609_v1.ref01_date_page_v4)>(plan.content->>'max_source_bytes')::bigint OR 16777216+(SELECT coalesce(sum(wire_bytes),0)*2 FROM population_top3000_202608_202609_v1.ref01_date_page_v4)+(SELECT coalesce(sum(pg_total_relation_size(oid)),0) FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND (relname LIKE 'ref01_date_%' OR relname='ref01_practical_dated_instrument_v3') AND relkind='r')>(plan.content->>'max_relation_bytes')::bigint OR n>lim OR n>admission.admitted_page_ceiling OR used_calls>=admission.max_provider_calls OR admission.expires_at<=clock_timestamp() THEN 'PAGE_LIMIT_REVIEW' ELSE 'READY' END,'source_admission_expires_at',admission.expires_at,'maximum_http_invocations',(plan.content->>'max_http_invocations')::bigint,'maximum_provider_calls',(plan.content->>'max_provider_calls')::bigint,'request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256,'archive_key',k,'page_number',n,'family_id',t.family_id,'partition_key',t.partition_key,'request_url',url,'provider_calls_used',used_calls,'max_provider_calls',admission.max_provider_calls,'admitted_page_ceiling',admission.admitted_page_ceiling);
END; $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_acquire_v4(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;c jsonb;ctl population_top3000_202608_202609_v1.acquisition_control%rowtype;used int;rid bigint;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);c:=population_top3000_202608_202609_v1.ref01_date_context_v4(tid);
 SELECT * INTO STRICT ctl FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT count(*) INTO used FROM population_top3000_202608_202609_v1.pipeline_attempt a JOIN population_top3000_202608_202609_v1.pipeline_task t USING(task_id) WHERE t.payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND a.request_id IS NOT NULL;
 IF c->>'state'<>'READY' OR (c->>'source_admission_expires_at')::timestamptz<=clock_timestamp()+interval '120seconds' OR used>=(c->>'maximum_http_invocations')::bigint OR ctl.http_invocations>=ctl.max_http_invocations THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','REF01_DATE_FINITE_ADMISSION_REVIEW_V4','resumption_condition',jsonb_build_object('request_key',c->>'request_key','next_page',c->'page_number','used_http',used,'engineering_assignment_event_id',(SELECT input_versions->'assignment_event_id' FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid)));END IF;
 IF ctl.next_provider_at>clock_timestamp() OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','DEPENDENCY_PENDING','recheck_at',greatest(ctl.next_provider_at,clock_timestamp()+interval '5 seconds'),'resumption_condition',jsonb_build_object('event','PROVIDER_SLOT_AVAILABLE'));END IF;
 SELECT net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-instrument-dates-v4',headers:='{"Content-Type":"application/json"}',body:=jsonb_build_object('attempt_id',p_attempt,'token',p_token),timeout_milliseconds:=55000) INTO rid;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET http_invocations=http_invocations+1 WHERE singleton;
 UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET request_id=rid WHERE attempt_id=p_attempt;
 RETURN jsonb_build_object('state','ASYNC','request_id',rid,'worker','population-top3000-instrument-dates-v4');
END $function$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_commit_v4(p_attempt uuid, p_token text, p_page jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;body jsonb;txt text;h text;wire text;
 sid text;k text;n integer;cnt integer;bytes bigint;stored_bytes bigint;v_next_url text;at_time timestamptz;fresh integer;receipt jsonb;source_evidence jsonb;
BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF t.stage<>'ACQUIRE' THEN RAISE EXCEPTION 'REF01_CENSUS_STAGE_MISMATCH';END IF;
 c:=population_top3000_202608_202609_v1.ref01_date_context_v4(tid);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF c->>'state'<>'READY' OR jsonb_typeof(p_page) IS DISTINCT FROM 'object' OR p_page->>'archive_key' IS DISTINCT FROM c->>'archive_key'
 OR p_page->>'page_number' IS DISTINCT FROM c->>'page_number' OR p_page->>'request_url' IS DISTINCT FROM c->>'request_url'
 OR p_page->>'request_manifest_sha256' IS DISTINCT FROM c->>'request_manifest_sha256' OR p_page->>'http_status' IS DISTINCT FROM '200' THEN RAISE EXCEPTION 'REF01_CENSUS_RECEIPT_SCOPE_MISMATCH';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider='Massive' AND released_at IS NULL AND reserved_bytes=(c->>'max_response_bytes')::bigint) THEN RAISE EXCEPTION 'REF01_CENSUS_RESERVATION_REQUIRED';END IF;
 txt:=p_page->>'content_text';stored_bytes:=octet_length(txt);bytes:=(p_page->>'wire_bytes')::bigint;wire:=p_page->>'wire_sha256';
 IF txt IS NULL OR stored_bytes>(c->>'max_response_bytes')::bigint OR bytes IS NULL OR bytes<1 OR bytes>(c->>'max_response_bytes')::bigint OR wire IS NULL OR wire!~'^[0-9a-f]{64}$'
 OR coalesce((p_page->>'elapsed_ms')::bigint,-1) NOT BETWEEN 0 AND 45000 THEN RAISE EXCEPTION 'REF01_CENSUS_SOURCE_BOUND_INVALID';END IF;
 h:=encode(sha256(convert_to(txt,'UTF8')),'hex');
 IF wire IS DISTINCT FROM h OR bytes IS DISTINCT FROM stored_bytes THEN RAISE EXCEPTION 'REF01_EXACT_ORIGINAL_REQUIRED';END IF;
 IF h IS DISTINCT FROM p_page->>'source_sha256' THEN RAISE EXCEPTION 'REF01_CENSUS_STORED_HASH_MISMATCH';END IF;
 body:=txt::jsonb;
 IF body->>'status' IS DISTINCT FROM 'OK' OR jsonb_typeof(body->'results') IS DISTINCT FROM 'array' THEN RAISE EXCEPTION 'REF01_CENSUS_INVALID_SOURCE_ENVELOPE';END IF;
 cnt:=jsonb_array_length(body->'results');
 IF cnt>1000 OR cnt IS DISTINCT FROM (p_page->>'result_count')::integer OR EXISTS(SELECT 1 FROM jsonb_array_elements(body->'results') v WHERE jsonb_typeof(v) IS DISTINCT FROM 'object') THEN RAISE EXCEPTION 'REF01_CENSUS_SOURCE_RECORD_SHAPE';END IF;
 v_next_url:=p_page->>'next_url';
 IF v_next_url IS DISTINCT FROM body->>'next_url' OR (v_next_url IS NOT NULL AND (v_next_url!~'^https://api[.]massive[.]com/v3/reference/tickers[?]cursor=[A-Za-z0-9_.~%+\-]+$' OR length(v_next_url)>16384 OR v_next_url=c->>'request_url')) THEN RAISE EXCEPTION 'REF01_CENSUS_CURSOR_MISMATCH';END IF;
 IF txt~* '"(apiKey|api_key|access_token|authorization)"[[:space:]]*:' OR txt~* '[?&](apiKey|api_key|access_token)=' THEN RAISE EXCEPTION 'REF01_CENSUS_UNREDACTED_CREDENTIAL_FIELD';END IF;
 k:=c->>'archive_key';n:=(c->>'page_number')::integer;sid:=k||':page:'||n::text;at_time:=(p_page->>'retrieved_at')::timestamptz;
 IF at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '30 seconds' OR at_time>clock_timestamp()+interval '30 seconds' THEN RAISE EXCEPTION 'REF01_CENSUS_RETRIEVAL_CLOCK_INVALID';END IF;
 IF v_next_url IS NOT NULL AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_page_v4 WHERE archive_key=k AND request_url=v_next_url) THEN RAISE EXCEPTION 'REF01_CENSUS_CURSOR_CYCLE';END IF;
 source_evidence:=jsonb_build_object('wire_sha256',wire,'wire_bytes',bytes,'stored_content_bytes',stored_bytes,'hash_representation','EXACT_UTF8_ORIGINAL_RESPONSE','wire_bytes_retained',true,'provider_request_id',p_page->>'provider_request_id','scope','Full historical active stock ticker census; not complete inactive instrument history','request_manifest_sha256',c->>'request_manifest_sha256','as_of',c->>'as_of','active',true,'source_completeness_certified',false,'no_trade_inferred',false,'original_historical_receipt',false);
 INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(sid,h,c->>'request_url','Massive',at_time,NULL,NULL,txt,stored_bytes,
 source_evidence,
 'EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') ON CONFLICT(source_id) DO NOTHING;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=sid AND d.source_sha256=h AND d.source_uri=c->>'request_url' AND d.content_text=txt AND d.content_bytes=stored_bytes AND d.source_provider='Massive' AND d.retrieved_at=at_time AND d.source_published_at IS NULL AND d.historical_received_at IS NULL AND d.evidence=source_evidence AND d.license_policy='EXISTING_MASSIVE_SUBSCRIPTION_API_RESPONSE_INTERNAL_USE') THEN RAISE EXCEPTION 'REF01_CENSUS_SOURCE_REVISION_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_page_v4
 (archive_key,family_id,partition_key,page_number,acquire_task_id,attempt_id,source_id,source_sha256,request_url,next_url,wire_sha256,wire_bytes,result_count,retrieved_at)
 VALUES(k,t.family_id,t.partition_key,n,tid,p_attempt,sid,h,c->>'request_url',v_next_url,wire,bytes,cnt,at_time)
 ON CONFLICT(archive_key,page_number) DO NOTHING;
 GET DIAGNOSTICS fresh=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_page_v4 q WHERE q.archive_key=k AND q.page_number=n AND q.source_id=sid AND q.source_sha256=h AND q.wire_sha256=wire AND q.wire_bytes=bytes AND q.request_url=c->>'request_url' AND q.next_url IS NOT DISTINCT FROM v_next_url AND q.result_count=cnt AND q.family_id=t.family_id AND q.partition_key=t.partition_key AND q.acquire_task_id=tid AND q.attempt_id=p_attempt AND q.retrieved_at=at_time) THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_REVISION_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,stored_bytes,0,0);
 PERFORM population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','NORMALIZE',t.partition_key||':page:'||n::text,
 t.payload||jsonb_build_object('source_page_number',n),t.input_versions,NULL);
 receipt:=jsonb_build_object('state',CASE WHEN v_next_url IS NULL THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('next_page',n+1,'last_source_id',sid,'last_source_sha256',h,'next_url',v_next_url,'source_records_seen',coalesce((t.checkpoint->>'source_records_seen')::bigint,0)+cnt),
 'not_before',(SELECT next_provider_at FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton),
 'commit_receipt',jsonb_build_object('commit_key','massive-page:'||sid||':'||h,'output_sha256',h,'new_committed_units',fresh,'immutable_output_ids',jsonb_build_array(sid),'input_versions',t.input_versions,'raw_source_records',cnt,'terminal_page',v_next_url IS NULL,'cohort_certified',false,'reference_lifecycle_qualified',false,'original_receipt_qualified',false));
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,receipt);
END; $function$;

;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_admit_v4(p_owner text, p_fence bigint, p_stop bigint, p_request text, p_page_ceiling integer, p_call_ceiling integer, p_expires timestamp with time zone, p_reason text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old population_top3000_202608_202609_v1.ref01_date_admission_v4%rowtype;r jsonb; page_limit int;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS NULL OR p_fence IS NULL OR p_stop IS NULL OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true THEN RAISE EXCEPTION 'REF01_CENSUS_CURRENT_CONTROLLER_ADMISSION_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT least((prq.manifest->>'max_pages')::integer,(pl.content->>'max_pages_per_date')::integer) INTO STRICT page_limit
 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 pl
 JOIN population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr USING(plan_id)
 JOIN population_top3000_202608_202609_v1.ref01_date_request_v4 prq USING(request_key)
 WHERE pr.request_key=p_request AND pl.enabled
 AND pl.content_sha256=encode(sha256(convert_to(pl.content::text,'UTF8')),'hex')
 AND pl.code_bundle_sha256=population_top3000_202608_202609_v1.ref01_date_bundle_v4();
 SELECT * INTO old FROM population_top3000_202608_202609_v1.ref01_date_admission_v4 WHERE request_key=p_request FOR UPDATE;
 IF p_page_ceiling IS NULL OR p_call_ceiling IS NULL OR p_page_ceiling NOT BETWEEN greatest(1,coalesce(old.admitted_page_ceiling,0)) AND least(100000,page_limit)
 OR p_call_ceiling NOT BETWEEN greatest(1,coalesce(old.max_provider_calls,0)) AND least(100000,page_limit)
 OR p_expires IS NULL OR p_expires<=clock_timestamp() OR p_expires>least(a.admitted_until,p.deadline,clock_timestamp()+interval '30 minutes')
 OR p_reason IS NULL OR length(p_reason) NOT BETWEEN 10 AND 1000
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_request_v4 x WHERE x.request_key=p_request AND x.manifest_sha256=encode(sha256(convert_to(x.manifest::text,'UTF8')),'hex') AND (x.manifest->>'max_pages')::integer>=p_page_ceiling)
 THEN RAISE EXCEPTION 'REF01_CENSUS_FINITE_TRANCHE_INVALID';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_admission_v4(request_key,owner_id,owner_fence,stop_generation,admitted_page_ceiling,max_provider_calls,expires_at)
 VALUES(p_request,p_owner,p_fence,p_stop,p_page_ceiling,p_call_ceiling,p_expires)
 ON CONFLICT(request_key) DO UPDATE SET owner_id=excluded.owner_id,owner_fence=excluded.owner_fence,stop_generation=excluded.stop_generation,admitted_page_ceiling=excluded.admitted_page_ceiling,max_provider_calls=excluded.max_provider_calls,expires_at=excluded.expires_at,updated_at=clock_timestamp();
 r:=jsonb_build_object('request_key',p_request,'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'prior',to_jsonb(old),'admitted_page_ceiling',p_page_ceiling,'max_provider_calls',p_call_ceiling,'expires_at',p_expires,'reason',p_reason,'source_denominator_changed',false,'cumulative_counters_preserved',true);
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)VALUES('REF01_CENSUS_TRANCHE_ADMITTED_V1',r);
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN r;
END;$function$


;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_impl_v9(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb; pl population_top3000_202608_202609_v1.ref01_date_plan_v9%rowtype;
 ac population_top3000_202608_202609_v1.acquisition_control%rowtype;
 pc population_top3000_202608_202609_v1.pipeline_control%rowtype;
 r population_top3000_202608_202609_v1.ref01_date_request_v4%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 adm population_top3000_202608_202609_v1.ref01_date_admission_v4%rowtype;
 dt date;k text;h text;iv jsonb;m jsonb;n int;pages int;calls bigint;http bigint;raw bigint;rel bigint;allrel bigint;
 cap int;callcap int;until_at timestamptz;reason text;tid uuid;ctx jsonb;clear_manifest jsonb;clear_hash text;
BEGIN
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 SELECT * INTO pl FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 WHERE enabled FOR UPDATE;
 IF NOT FOUND THEN RETURN '{"state":"NO_ENABLED_REF01_DATE_PLAN"}';END IF;
 IF pl.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(pl.content::text,'UTF8')),'hex')
 OR pl.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4()
 OR pl.content->>'owner_id' IS DISTINCT FROM p_owner OR pl.content->>'owner_fence' IS DISTINCT FROM p_fence::text
 OR pl.content->>'stop_generation' IS DISTINCT FROM p_stop::text THEN RAISE EXCEPTION 'REF01_DATE_PLAN_AUTHORITY_OR_CODE_CHANGED';END IF;
 SELECT * INTO STRICT ac FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT pc FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 -- Date order comes from the pinned complete population, not from an implementation ceiling.
 SELECT x::date INTO dt FROM jsonb_array_elements_text(pl.content->'dates') x
 WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr
 JOIN population_top3000_202608_202609_v1.pipeline_task q ON q.payload->>'request_key'=pr.request_key AND q.family_id='REF01' AND q.stage='ACQUIRE'
 WHERE pr.plan_id=pl.plan_id AND pr.as_of=x::date AND q.status='COMPLETE'
 AND EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_page_v4 pg
 JOIN population_top3000_202608_202609_v1.source_document ds USING(source_id)
 WHERE pg.acquire_task_id=q.task_id AND pg.partition_key=pr.request_key AND pg.next_url IS NULL
 AND ds.source_sha256=pg.source_sha256 AND ds.source_sha256=encode(sha256(convert_to(ds.content_text,'UTF8')),'hex')
 AND ds.content_bytes=octet_length(ds.content_text) AND ds.content_text::jsonb->>'next_url' IS NULL
 AND jsonb_array_length(ds.content_text::jsonb->'results')=pg.result_count)) ORDER BY x::date LIMIT 1;
 IF dt IS NULL THEN RETURN jsonb_build_object('state','ALL_PLANNED_DATE_SOURCES_TERMINAL','plan_id',pl.plan_id,'family_accepted',false,'full_instrument_history_accepted',false);END IF;
 SELECT rq.* INTO r FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9 pr
 JOIN population_top3000_202608_202609_v1.ref01_date_request_v4 rq USING(request_key) WHERE pr.plan_id=pl.plan_id AND pr.as_of=dt;
 IF FOUND THEN
  SELECT * INTO t FROM population_top3000_202608_202609_v1.pipeline_task WHERE family_id='REF01' AND stage='ACQUIRE' AND payload->>'request_key'=r.request_key;
  IF FOUND AND t.status='LEASED' THEN RETURN jsonb_build_object('state','SOURCE_ATTEMPT_ACTIVE','task_id',t.task_id);END IF;
  -- Neither expired leases nor failed HTTP requests are interpreted as harmless waits.
  IF t.task_id IS NOT NULL AND (t.cumulative_failures>0 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE l.attempt_id=t.attempt_id AND l.released_at IS NULL)
   OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.task_id=t.task_id AND a.outcome='RUNNING')
   OR (t.status='BLOCKED' AND t.blocker_code NOT IN('REF01_DATE_FINITE_ADMISSION_REVIEW_V4','REF01_CENSUS_PAGE_CAP_REVIEW','REF01_DATE_PLAN_RESOURCE_REVIEW_V9')
      AND (t.blocker_code='DEPENDENCY_PENDING' AND t.resumption_condition->>'event'='PROVIDER_SLOT_AVAILABLE'
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.attempt_id=t.attempt_id AND a.request_id IS NOT NULL)
 AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease l WHERE l.attempt_id=t.attempt_id)) IS NOT TRUE))
  THEN RETURN jsonb_build_object('state','PRESERVED_EVIDENCE_OR_UNKNOWN_OUTCOME_HOLD','task_id',t.task_id,'blocker',t.blocker_code);END IF;
  SELECT * INTO adm FROM population_top3000_202608_202609_v1.ref01_date_admission_v4 WHERE request_key=r.request_key;
  n:=coalesce((t.checkpoint->>'next_page')::int,1);
  IF adm.expires_at>clock_timestamp()+interval '5minutes' AND n<=adm.admitted_page_ceiling AND (t.status='READY' OR (t.status='BLOCKED' AND t.blocker_code='DEPENDENCY_PENDING' AND t.resumption_condition->>'event'='PROVIDER_SLOT_AVAILABLE' AND NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_attempt a WHERE a.attempt_id=t.attempt_id AND a.request_id IS NOT NULL))) THEN
   RETURN jsonb_build_object('state','EXISTING_ADMITTED_SOURCE_READY','task_id',t.task_id,'as_of',dt);
  END IF;
 ELSE n:=1;END IF;
 SELECT count(*) INTO http FROM population_top3000_202608_202609_v1.pipeline_attempt a JOIN population_top3000_202608_202609_v1.pipeline_task q USING(task_id)
 WHERE q.family_id='REF01' AND q.payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND a.request_id IS NOT NULL;
 SELECT count(*) INTO calls FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt a USING(attempt_id)
 JOIN population_top3000_202608_202609_v1.pipeline_task q USING(task_id) WHERE q.family_id='REF01' AND q.payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND l.provider='Massive';
 SELECT coalesce(sum(wire_bytes),0),count(*) INTO raw,pages FROM population_top3000_202608_202609_v1.ref01_date_page_v4;
 SELECT coalesce(sum(pg_total_relation_size(oid)),0) INTO rel FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind='r'
 AND (relname LIKE 'ref01_date_%' OR relname='ref01_practical_dated_instrument_v3');
 SELECT coalesce(sum(pg_total_relation_size(oid)),0) INTO allrel FROM pg_class WHERE relnamespace='population_top3000_202608_202609_v1'::regnamespace AND relkind IN('r','m');
 -- Enough headroom for one exact maximum response and conservative in-database expansion.
 IF http>=(pl.content->>'max_http_invocations')::bigint OR calls>=(pl.content->>'max_provider_calls')::bigint THEN reason:='PLAN_CALL_REVIEW';
 ELSIF raw+8388608>(pl.content->>'max_source_bytes')::bigint OR rel+2*raw+16777216>(pl.content->>'max_relation_bytes')::bigint THEN reason:='PLAN_STORAGE_REVIEW';
 ELSIF ac.http_invocations>=ac.max_http_invocations OR ac.provider_requests>=ac.max_provider_requests OR ac.stored_bytes+ac.reserved_bytes+8388608>ac.max_source_bytes THEN reason:='GLOBAL_SOURCE_CAPACITY_REVIEW';
 ELSIF allrel+16777216>(pc.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint THEN reason:='GLOBAL_RELATION_CAPACITY_REVIEW';
 ELSIF least(ac.admitted_until,pc.deadline)<=clock_timestamp()+interval '2minutes' THEN reason:='GLOBAL_TIME_ADMISSION_REVIEW';
 ELSIF n>(pl.content->>'max_pages_per_date')::int THEN reason:='PLAN_DATE_PAGE_REVIEW';
 ELSIF r.request_key IS NOT NULL AND n>(r.manifest->>'max_pages')::int THEN reason:='LEGACY_REQUEST_VERSION_SUCCESSOR_REQUIRED';END IF;
 IF reason IS NOT NULL THEN
  IF pl.last_blocker IS DISTINCT FROM reason THEN
   INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_DATE_PLAN_REVIEW_REQUIRED_V9',ctl||jsonb_build_object('plan_id',pl.plan_id,'reason',reason,'as_of',dt,'task_id',t.task_id,'http_calls',http,'provider_calls',calls,'raw_bytes',raw,'relation_bytes',rel,'mission_relation_bytes',allrel,'required_action','Current controller executes measured finite resource admission or separately versioned request; preserve counters, sources, cursor and acceptance holds','assignment_event_id',pl.content->'assignment_event_id'));
   UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET last_blocker=reason,updated_at=clock_timestamp() WHERE plan_id=pl.plan_id;
  END IF;

  clear_manifest:=jsonb_build_array(jsonb_build_object('plan_id',pl.plan_id,'plan_sha256',pl.content_sha256,'reason',reason,'request_key',r.request_key,
   'global_source_ceiling',ac.max_source_bytes,'global_relation_ceiling',pc.budget_evidence#>'{resource_limits,max_relation_bytes}',
   'global_provider_ceiling',ac.max_provider_requests,'global_http_ceiling',ac.max_http_invocations,'original_deadline',pc.deadline));
  clear_hash:=encode(sha256(convert_to(clear_manifest::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
  VALUES('ref01-date-plan:'||clear_hash,'OPERATIONAL_REVIEW',CASE WHEN reason LIKE 'GLOBAL_%' THEN 'RESOURCE' ELSE 'REF01' END,'REF01_DATE_PLAN_'||reason,clear_manifest,clear_hash,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
   jsonb_build_object('source_bytes',raw,'relation_bytes',rel,'mission_relation_bytes',allrel,'provider_calls',calls,'http_calls',http,
   'execution_recipe','Inspect pinned instrument plan, exact retained cursor and actual measured source/relation/call/time headroom. Execute independently reviewed finite operational admission or separately versioned request/plan generation. Preserve all previous request manifests, counters, failures and original deadline. Reinvoke ref01_date_continue_v9 and verify next source plus normalization commits. Do not reset clearing attempts or claim family acceptance.')) ON CONFLICT DO NOTHING;
  RETURN jsonb_build_object('state','REVIEW_REQUIRED','reason',reason,'as_of',dt,'clearing_id','ref01-date-plan:'||clear_hash);
 END IF;
 IF r.request_key IS NULL THEN
  iv:=pl.content->'input_versions'||jsonb_build_object('transport_bundle_sha256',pl.code_bundle_sha256,'date_plan_id',pl.plan_id,'date_plan_sha256',pl.content_sha256);
  m:=jsonb_build_object('as_of',dt::text,'active',true,'population','ALL_PROVIDER_HISTORICAL_REFERENCE_NO_COHORT_FILTER','max_pages',100000,
  'first_url','https://api.massive.com/v3/reference/tickers?date='||dt::text||'&sort=ticker&limit=1000&order=asc&active=true&locale=us&market=stocks','max_response_bytes',8388608,'max_runtime_ms',45000,'input_versions',iv,
  'maximum_source_bytes',(pl.content->>'max_source_bytes')::bigint,'maximum_http_invocations',(pl.content->>'max_http_invocations')::bigint,'maximum_provider_calls',(pl.content->>'max_provider_calls')::bigint,'maximum_additional_relation_bytes',(pl.content->>'max_relation_bytes')::bigint);
  h:=encode(sha256(convert_to(m::text,'UTF8')),'hex');k:='ref01-date-v4:'||h;
  INSERT INTO population_top3000_202608_202609_v1.ref01_date_request_v4(request_key,manifest,manifest_sha256,input_versions) VALUES(k,m,h,iv);
  INSERT INTO population_top3000_202608_202609_v1.ref01_date_plan_request_v9 VALUES(pl.plan_id,dt,k,h,encode(sha256(convert_to(iv::text,'UTF8')),'hex'));
  SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ref01_date_request_v4 WHERE request_key=k;
 END IF;
 cap:=least((r.manifest->>'max_pages')::int,(pl.content->>'max_pages_per_date')::int,
 greatest(coalesce(adm.admitted_page_ceiling,0),(pl.content->>'initial_page_tranche')::int,n+(pl.content->>'renewal_page_tranche')::int-1));
 callcap:=cap;
 until_at:=least(ac.admitted_until,pc.deadline,clock_timestamp()+interval '29minutes');
 PERFORM population_top3000_202608_202609_v1.ref01_date_admit_v4(p_owner,p_fence,p_stop,r.request_key,cap,callcap,until_at,
 'Reviewed versioned finite plan '||pl.plan_id||'; retained cursor and cumulative accounting checked; dates advance sequentially; source and family acceptance remain separate.');
 IF t.task_id IS NULL THEN
  tid:=population_top3000_202608_202609_v1.pipeline_enqueue_v1('REF01','ACQUIRE',r.request_key,jsonb_build_object('kind','REF01_CENSUS_SOURCE_V1','request_key',r.request_key,'request_manifest_sha256',r.manifest_sha256),r.input_versions);
 ELSE
  tid:=t.task_id;
  ctx:=population_top3000_202608_202609_v1.ref01_date_context_v4(tid);
  IF ctx->>'state'='READY' AND t.status='BLOCKED' AND t.blocker_code IN('REF01_DATE_FINITE_ADMISSION_REVIEW_V4','REF01_CENSUS_PAGE_CAP_REVIEW','REF01_DATE_PLAN_RESOURCE_REVIEW_V9') THEN
   UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,resumption_condition=NULL WHERE task_id=tid;
  END IF;
 END IF;
 UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET last_blocker=NULL,updated_at=clock_timestamp() WHERE plan_id=pl.plan_id;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('REF01_DATE_PLAN_ADVANCED_V9',ctl||jsonb_build_object('plan_id',pl.plan_id,'as_of',dt,'task_id',tid,'request_key',r.request_key,'next_page',n,'page_ceiling',cap,'expires_at',until_at,'cumulative_provider_calls',calls,'cumulative_http_calls',http,'source_bytes',raw,'relation_bytes',rel,'all_counters_preserved',true,'family_accepted',false));
 RETURN jsonb_build_object('state','ADMITTED_AND_QUEUED','task_id',tid,'as_of',dt,'page_ceiling',cap);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_impl_v9(text,bigint,bigint) FROM PUBLIC,anon,authenticated;

;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_v9(p_owner text,p_fence bigint,p_stop bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog AS $f$
DECLARE ctl jsonb; result jsonb; err text; errstate text; manifest jsonb;h text;integrity_fault boolean;
BEGIN
 -- Owner, user stop, scope and global admission rejection are outside fault isolation.
 ctl:=population_top3000_202608_202609_v1.continuation_guard_v6(p_owner,p_fence,p_stop);
 BEGIN
  result:=population_top3000_202608_202609_v1.ref01_date_continue_impl_v9(p_owner,p_fence,p_stop);
  RETURN result;
 EXCEPTION WHEN OTHERS THEN
  GET STACKED DIAGNOSTICS err=MESSAGE_TEXT,errstate=RETURNED_SQLSTATE;
  -- The failed implementation subtransaction has no committed effects. Hold only its source lane.
  manifest:=jsonb_build_array(jsonb_build_object('plan_ids',(SELECT jsonb_agg(plan_id ORDER BY plan_id) FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 WHERE enabled),
   'error_state',errstate,'error',left(err,500),'installed_bundle',population_top3000_202608_202609_v1.ref01_date_bundle_v4()));
  h:=encode(sha256(convert_to(manifest::text,'UTF8')),'hex');
  SELECT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 p WHERE p.enabled AND
   (p.code_bundle_sha256 IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_date_bundle_v4()
    OR p.content_sha256 IS DISTINCT FROM encode(sha256(convert_to(p.content::text,'UTF8')),'hex')
    OR p.content->>'owner_id' IS DISTINCT FROM p_owner OR p.content->>'owner_fence' IS DISTINCT FROM p_fence::text OR p.content->>'stop_generation' IS DISTINCT FROM p_stop::text)) INTO integrity_fault;
  UPDATE population_top3000_202608_202609_v1.ref01_date_plan_v9 SET enabled=false,last_blocker='IMPLEMENTATION_REPAIR',updated_at=clock_timestamp() WHERE enabled;
  UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='BLOCKED',blocker_code=CASE WHEN integrity_fault THEN 'CODE_INTEGRITY' ELSE 'REF01_DATE_PLAN_IMPLEMENTATION_HOLD_V9' END,blocker_until=NULL,
   resumption_condition=jsonb_build_object('clearing_id','ref01-date-code:'||h,'required_action','Repair and independently verify exact dated-source plan; preserve source hashes, attempts and counters')
   WHERE family_id='REF01' AND payload->>'kind'='REF01_CENSUS_SOURCE_V1' AND (stage='ACQUIRE' OR integrity_fault) AND status='READY';
  INSERT INTO population_top3000_202608_202609_v1.continuation_clear_queue_v6(clearing_id,kind,family_id,blocker_code,input_manifest,input_sha256,status,owner_id,owner_fence,stop_generation,scope_sha256,not_after,last_receipt)
  VALUES('ref01-date-code:'||h,'IMPLEMENTATION_REPAIR','REF01','REF01_DATE_PLAN_IMPLEMENTATION_HOLD_V9',manifest,h,'READY',p_owner,p_fence,p_stop,ctl->>'scope_sha256',(ctl->>'not_after')::timestamptz,
   jsonb_build_object('effects_known',true,'failed_handler_transaction_rolled_back',true,'execution_recipe','Reconcile exact dated source plan code/hash or malformed-state failure. Implement narrowly reviewed correction; prove rollback tests, retain source originals and counts, then re-enable exact plan and only named implementation holds. Independent mission lanes continue.')) ON CONFLICT DO NOTHING;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details)
   SELECT 'REF01_DATE_PLAN_FAILURE_ISOLATED_V9',ctl||jsonb_build_object('clearing_id','ref01-date-code:'||h,'error',left(err,500),'error_state',errstate,'failed_subtransaction_rolled_back',true,'family_accepted',false)
   WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='REF01_DATE_PLAN_FAILURE_ISOLATED_V9' AND details->>'clearing_id'='ref01-date-code:'||h);
  RETURN jsonb_build_object('state','REF01_PLAN_HELD_REPAIR_DISPATCHED','clearing_id','ref01-date-code:'||h);
 END;
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_date_continue_v9(text,bigint,bigint) FROM PUBLIC,anon,authenticated;

;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_date_bundle_v4()
 RETURNS text
 LANGUAGE sql
 STABLE
 SET search_path TO 'pg_catalog'
AS $function$ SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(oid),E'\n' ORDER BY proname),'UTF8')),'hex') FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname IN('ref01_date_context_v4','ref01_date_claim_v4','ref01_date_commit_v4','ref01_date_failure_v4','ref01_date_acquire_v4','ref01_date_process_v4','ref01_date_admit_v4','ref01_date_continue_v9','ref01_date_continue_impl_v9','ref01_date_bundle_v4'); $function$


;

DO $f$
DECLARE p jsonb;dates jsonb;iv jsonb;
BEGIN
 PERFORM population_top3000_202608_202609_v1.continuation_guard_v6('top3000-completion-20261024-root-5db48b32d103',3,1);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9) THEN RAISE EXCEPTION 'REF01_DATE_PLAN_ALREADY_EXISTS';END IF;
 SELECT jsonb_agg(d::date::text ORDER BY d) INTO dates FROM generate_series(DATE '2026-08-01',DATE '2026-09-30',INTERVAL '1day') d WHERE d::date<>DATE '2026-08-03';
 iv:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','policy_sha256','3e49d661fcdb8533e5ce88a85689b41e9dde71eeae2dc8578c9e31f9ae1738bb','cohort_sha256','2dc0f766befed6de6b052221c3c87482d507ba1bad931cbb98654eb3af899bec','membership_acceptance_sha256','20b9dac759857e9b510ccaeca33e4a94d8c748d588fb0cc27740cc66c389b5f9','assignment_event_id',28605,'engineering_successor_event_id',28605);
 p:=jsonb_build_object('revision','REF01_FULL_PERIOD_DATED_SOURCE_CONTINUATION_V9','owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'authority_event_id',9528,'assignment_event_id',28605,'root_claim_event_id',29634,'source_admission_basis','New independently reviewed fullperiod finite plan under current user authority9528 and rootclaim29634;1200 cumulativecalls supersede inherited144-call assignment allowance only after current root records this plan admission.',
 'dates',dates,'retained_aug3_anchor','27c81878e9ea5ce6eb05ead025f5c9aaae11108af50ba59ecd65b3402c877c86','input_versions',iv,
 'max_provider_calls',1200,'max_http_invocations',1200,'max_source_bytes',300000000,'max_relation_bytes',3000000000,
 'initial_page_tranche',14,'renewal_page_tranche',10,'max_pages_per_date',24,'expected_date_count',60,
 'measurement',jsonb_build_object('retained_pages',40,'retained_rows',38096,'retained_wire_bytes',11268945,'date_relation_bytes',60907520,'dated_relation_bytes',21127168,'measurement_utc','2026-10-24T15:24:00Z'),
 'planning_basis','40 retained pages measured; 840 pages expected across 60 date censuses, <=1200 finite cumulative requests. 300MB source and 3GB combined relation plan ceiling (relation+2*raw+16MiB); 40-page measured~82MB relations scales to~1.72GB across840pages plus~473MB source allowance and16MiB margin, about2.21GB forecast;3GB includes~36percent margin; global limits remain binding and trigger deduplicated executable clearing work. Every date includes all active provider stocks; no inactive history or family acceptance inferred.');
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_plan_v9(plan_id,content,content_sha256,code_bundle_sha256,enabled)
 VALUES('ref01-date-fullperiod-v9',p,encode(sha256(convert_to(p::text,'UTF8')),'hex'),population_top3000_202608_202609_v1.ref01_date_bundle_v4(),true);
 INSERT INTO population_top3000_202608_202609_v1.ref01_date_plan_request_v9(plan_id,as_of,request_key,manifest_sha256,input_versions_sha256)
 SELECT 'ref01-date-fullperiod-v9',(manifest->>'as_of')::date,request_key,manifest_sha256,encode(sha256(convert_to(input_versions::text,'UTF8')),'hex')
 FROM population_top3000_202608_202609_v1.ref01_date_request_v4
 WHERE (manifest->>'as_of')::date IN(DATE '2026-08-01',DATE '2026-08-02',DATE '2026-08-04',DATE '2026-08-05');
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_date_plan_request_v9)<>4 THEN RAISE EXCEPTION 'EXACT_LEGACY_REQUEST_POPULATION_REQUIRED';END IF;
END;$f$;

;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_tick_v1()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare c population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 t population_top3000_202608_202609_v1.pipeline_task%rowtype;b population_top3000_202608_202609_v1.pipeline_adapter%rowtype;
 x record;a uuid;tok text;fn text;receipt jsonb;result jsonb;err text;errstate text;h text;wake timestamptz;ready_count bigint;hold_state text;
begin
 if not pg_try_advisory_xact_lock(2026083000261022) then return '{"state":"BUSY"}';end if;
 select * into c from population_top3000_202608_202609_v1.acquisition_control where singleton for update;
 select * into p from population_top3000_202608_202609_v1.pipeline_control where singleton for update;
 -- Never alter an unrelated scheduler job if the mission binding has changed.
 if not exists(select 1 from cron.job where jobid=596 and database=current_database()
  and btrim(command)='select population_top3000_202608_202609_v1.pipeline_tick_v1();') then
  return '{"state":"SCHEDULER_BINDING_MISMATCH"}';
 end if;
 if p.singleton is null or c.singleton is null then hold_state:='CONTROL_MISSING';
 elsif c.owner_id is distinct from p.expected_owner_id or c.owner_fence is distinct from p.expected_owner_fence or c.stop_generation is distinct from p.expected_stop_generation then hold_state:='OWNER_OR_STOP_CHANGED';
 elsif c.scope_sha256 is distinct from '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' or p.scope_sha256 is distinct from c.scope_sha256 then hold_state:='SCOPE_MISMATCH';
 elsif c.stopped is distinct from false or c.enabled is distinct from true or c.hold_reason is not null or p.enabled is distinct from true then hold_state:='HELD';
 end if;
 if hold_state is not null then
  -- Disarm this bound mission job without changing either owner's state, holds or fences.
  if exists(select 1 from cron.job where jobid=596 and active) then
   perform cron.alter_job(596,active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('HOLD_DISARMED',
    jsonb_build_object('reason',hold_state,'owner_id',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'expected_owner_id',p.expected_owner_id,'expected_owner_fence',p.expected_owner_fence,'expected_stop_generation',p.expected_stop_generation));
  end if;
  return jsonb_build_object('state',hold_state);
 end if;
 if not p.shared_scheduler_verified then
  insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('SCHEDULER_PROBE',jsonb_build_object('backend_pid',pg_backend_pid(),'application_name',current_setting('application_name'),'database_user',current_user,'owner_id',c.owner_id,'owner_fence',c.owner_fence,'stop_generation',c.stop_generation,'no_task_executed',true));
  perform cron.alter_job(596,active=>false);
  return '{"state":"SCHEDULER_PROBE_RECORDED_AWAITING_EVIDENCE_REVIEW"}';
 end if;
 if population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) is distinct from true then
  if exists(select 1 from cron.job where jobid=596 and active) then
   perform cron.alter_job(596,active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('HOLD_DISARMED',jsonb_build_object('reason','BUDGET_HOLD','owner_fence',c.owner_fence));
  end if;
  return '{"state":"BUDGET_HOLD"}';
 end if;
 if c.admitted_until is null or c.admitted_until<=clock_timestamp() or p.deadline is null or p.deadline<=clock_timestamp() or p.tick_count>=p.max_ticks then
  perform cron.alter_job(596,active=>false);
  insert into population_top3000_202608_202609_v1.pipeline_event(event,details) values('ADMISSION_EXPIRED_OR_TICK_LIMIT',jsonb_build_object('admitted_until',c.admitted_until,'deadline',p.deadline,'ticks',p.tick_count));
  return '{"state":"ADMISSION_EXPIRED_OR_TICK_LIMIT"}';end if;
 -- Bounded blocker clearing remains distinct from data acceptance; no user/access/unknown-effect hold is cleared.
 perform population_top3000_202608_202609_v1.continuation_dispatch_v6(c.owner_id,c.owner_fence,c.stop_generation);
 -- Advance the reviewed finite instrument date plan; exact source errors stay scoped.
 perform population_top3000_202608_202609_v1.ref01_date_continue_v9(c.owner_id,c.owner_fence,c.stop_generation);
 if p.next_wakeup_at>clock_timestamp() then return jsonb_build_object('state','SCHEDULED_WAIT','not_before',p.next_wakeup_at);end if;
 update population_top3000_202608_202609_v1.pipeline_control set tick_count=tick_count+1 where singleton;
 -- Expired source effects remain blocked. Uninvoiced provider reservations remain commitments, never inferred actual charges.
 for x in select q.*,exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id and pl.released_at is null) as uncertain_provider_effect from population_top3000_202608_202609_v1.pipeline_task q where q.status='LEASED' and q.lease_until<=clock_timestamp() for update of q loop
  update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=greatest(0,reserved_bytes-coalesce((select sum(reserved_bytes) from population_top3000_202608_202609_v1.pipeline_provider_lease where attempt_id=x.attempt_id and released_at is null),0)) where singleton;
  if x.uncertain_provider_effect then
  if encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(uuid)'::regprocedure),'UTF8')),'hex') is distinct from 'ace964d10158e91bbdcfa658dd7583961a9e8557993266ed1d2cfcb1ac041eeb' then raise exception 'EXPIRY_ACCOUNTING_HELPER_CHANGED';end if;
  perform population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(x.attempt_id);
  end if;
  update population_top3000_202608_202609_v1.pipeline_attempt set outcome=case when x.uncertain_provider_effect then 'BLOCKED' else 'LEASE_EXPIRED_RECONCILED' end,ended_at=clock_timestamp() where attempt_id=x.attempt_id and outcome='RUNNING';
  update population_top3000_202608_202609_v1.pipeline_task set status=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then 'READY' else 'BLOCKED' end,
   cumulative_failures=cumulative_failures+1,same_failure_count=case when last_failure_class='LEASE_EXPIRED' then same_failure_count+1 else 1 end,last_failure_class=case when x.uncertain_provider_effect then 'UNCERTAIN_EFFECT' else 'LEASE_EXPIRED' end,last_failure_fingerprint=case when x.uncertain_provider_effect then 'LEASE_EXPIRED_UNCERTAIN_PROVIDER' else 'LEASE_EXPIRED' end,
   blocker_code=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then null else case when x.uncertain_provider_effect then 'UNCERTAIN_EFFECT' else 'LEASE_EXPIRED' end end,
   resumption_condition=case when not x.uncertain_provider_effect and cumulative_failures+1<max_failures and (last_failure_class is distinct from 'LEASE_EXPIRED' or same_failure_count<1) then null else jsonb_build_object('required_action',case when x.uncertain_provider_effect then 'Reconcile immutable output and actual billing receipts before any retry; expired provider reservation remains an uninvoiced commitment and no retry is admitted.' else 'Inspect retained source/commit hashes and worker response, apply changed approach before retry' end) end,
   lease_until=null,not_before=greatest(clock_timestamp()+interval '30 seconds',c.next_provider_at) where task_id=x.task_id;
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)values(x.task_id,x.attempt_id,'EXPIRED_RECONCILED',jsonb_build_object('checkpoint',x.checkpoint,'retained_commits',(select count(*) from population_top3000_202608_202609_v1.pipeline_commit where task_id=x.task_id)));
 end loop;
 -- A failed external attempt remains blocked; only its expired reservation is released.
 -- pipeline_fail_v1 preserves the original lease deadline when effects are uncertain.
 for x in select q.* from population_top3000_202608_202609_v1.pipeline_task q
  where q.status='BLOCKED' and q.blocker_code='UNCERTAIN_EFFECT'
   and q.lease_until is not null and q.lease_until<=clock_timestamp()
   and exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id and pl.released_at is null)
  for update of q loop
  update population_top3000_202608_202609_v1.acquisition_control set reserved_bytes=reserved_bytes-coalesce((select sum(pl.reserved_bytes) from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=x.attempt_id and pl.released_at is null),0) where singleton;
  if encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(uuid)'::regprocedure),'UTF8')),'hex') is distinct from 'ace964d10158e91bbdcfa658dd7583961a9e8557993266ed1d2cfcb1ac041eeb' then raise exception 'EXPIRY_ACCOUNTING_HELPER_CHANGED';end if;
  perform population_top3000_202608_202609_v1.pipeline_provider_expiry_accounting_v2(x.attempt_id);
  update population_top3000_202608_202609_v1.pipeline_task set lease_until=null,
   resumption_condition=coalesce(resumption_condition,'{}'::jsonb)||jsonb_build_object('required_action','Reconcile immutable output and actual billing receipts before any retry. Operational lease ended after its original deadline; the uninvoiced monetary reservation remains retained and the task remains blocked.','operational_lease_released_at',clock_timestamp(),'billing_basis','UNINVOICED_RESERVATION_NOT_ACTUAL_CHARGE') where task_id=x.task_id;
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details) values(x.task_id,x.attempt_id,'UNCERTAIN_RESERVATION_RECONCILED',jsonb_build_object('original_deadline',x.lease_until,'task_remains_blocked',true,'retry_admitted',false));
 end loop;
 -- Exact known no-HTTP provider waits are scheduling waits, not evidence retries.
 -- Old exhausted waits recover only at a free provider slot; all counters remain intact.
 for x in update population_top3000_202608_202609_v1.pipeline_task q
  set status='READY',blocker_until=null,blocker_code=null,resumption_condition=null
  where q.family_id='REF01' and q.stage='ACQUIRE' and q.payload->>'kind'='REF01_CENSUS_SOURCE_V1'
   and q.status='BLOCKED' and q.blocker_code='DEPENDENCY_PENDING'
   and q.resumption_condition->>'event'='PROVIDER_SLOT_AVAILABLE'
   and q.blocker_until<=clock_timestamp() and c.next_provider_at<=clock_timestamp()
   and q.owner_fence=c.owner_fence and q.stop_generation=c.stop_generation
   and q.executions<q.max_executions and q.cumulative_failures<q.max_failures
   and q.last_failure_class is distinct from 'UNCERTAIN_EFFECT'
   and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)
   and exists(select 1 from population_top3000_202608_202609_v1.pipeline_attempt a where a.attempt_id=q.attempt_id and a.outcome='BLOCKED' and a.request_id is null)
   and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_attempt a where a.task_id=q.task_id and a.outcome='RUNNING')
   and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease pl where pl.attempt_id=q.attempt_id)
  returning q.task_id,q.attempt_id,q.recheck_count,q.executions,q.checkpoint loop
  insert into population_top3000_202608_202609_v1.pipeline_event(event,task_id,attempt_id,details)
  values('REF01_PROVIDER_WAIT_RESUMED_V9',x.task_id,x.attempt_id,jsonb_build_object('rechecks_preserved',x.recheck_count,'executions_preserved',x.executions,'checkpoint_preserved',x.checkpoint,'basis','KNOWN_NO_HTTP_WAIT_CURRENT_FREE_PROVIDER_SLOT','family_accepted',false));
 end loop;
 -- Only explicitly timed temporary conditions are rechecked. Auth, budget, code and user holds never auto-release.
 update population_top3000_202608_202609_v1.pipeline_task q set status='READY',recheck_count=recheck_count+1,blocker_until=null,blocker_code=null,resumption_condition=null
 where q.status='BLOCKED' and q.blocker_until<=clock_timestamp() and q.recheck_count<q.max_rechecks
 and q.blocker_code in('DEPENDENCY_PENDING','RETAINED_IMPORT_PENDING','RETAINED_SEC_SOURCE_IMPORT_INCOMPLETE','ADAPTER_NOT_INSTALLED')
 and (q.family_id='REF01' and q.stage='ACQUIRE' and q.payload->>'kind'='REF01_CENSUS_SOURCE_V1' and q.resumption_condition->>'event'='PROVIDER_SLOT_AVAILABLE') IS NOT TRUE
 and exists(select 1 from population_top3000_202608_202609_v1.pipeline_adapter adapter_rows where adapter_rows.family_id=q.family_id and adapter_rows.stage=q.stage and adapter_rows.enabled);
 -- Executions are bounded independently of failure count. Do not leave exhausted work READY.
 for x in update population_top3000_202608_202609_v1.pipeline_task q
  set status='BLOCKED',blocker_code='EXECUTION_LIMIT',blocker_until=null,
   resumption_condition=jsonb_build_object('required_action','Review retained checkpoint, immutable outputs and measured batch size; admit an explicitly bounded changed approach without resetting counters.','executions',q.executions,'max_executions',q.max_executions)
  where q.status='READY' and q.executions>=q.max_executions
  returning q.task_id,q.attempt_id,q.executions,q.max_executions loop
  insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details) values(x.task_id,x.attempt_id,'EXECUTION_LIMIT_BLOCKED',jsonb_build_object('executions',x.executions,'max_executions',x.max_executions));
 end loop;
 if (select count(*) from population_top3000_202608_202609_v1.pipeline_task where status='LEASED' and lease_until>clock_timestamp())>=p.max_active_tasks then return '{"state":"CONCURRENCY_LIMIT"}';end if;
 -- Least-recently-dispatched ready family first: independent bounded lanes cannot starve behind fresh cases.
 -- Aggregate retained attempts once per tick; preserve all attempt/cost/execution history and existing locks.
 -- At most four REF01 non-validation turns before a ready validation turn.
 -- History comes only from durable attempts; no task dates, counts, holds or validators are rewritten.
 with ref01_recent as materialized (
  select z.stage from population_top3000_202608_202609_v1.pipeline_attempt v
  join population_top3000_202608_202609_v1.pipeline_task z on z.task_id=v.task_id
  where z.family_id='REF01' order by v.started_at desc,v.attempt_id desc limit 4
 ), ref01_turn as materialized (
  select count(*)=4 and bool_and(stage<>'VALIDATE') as validation_due from ref01_recent
 ), family_dispatch as materialized (
  select z.family_id,max(v.started_at) as last_dispatched_at
  from population_top3000_202608_202609_v1.pipeline_attempt v
  join population_top3000_202608_202609_v1.pipeline_task z on z.task_id=v.task_id
  group by z.family_id
 )
 select q.* into t from population_top3000_202608_202609_v1.pipeline_task q
 left join population_top3000_202608_202609_v1.pipeline_adapter adapter_rows on adapter_rows.family_id=q.family_id and adapter_rows.stage=q.stage
 left join family_dispatch fair on fair.family_id=q.family_id
 cross join ref01_turn
 where q.status='READY' and q.not_before<=clock_timestamp() and q.executions<q.max_executions
 and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_dependency d join population_top3000_202608_202609_v1.pipeline_task z on z.task_id=d.prerequisite_task_id where d.task_id=q.task_id and z.status<>'COMPLETE')
 and ((adapter_rows.resource_class is distinct from 'PROVIDER' and (q.family_id='REF01' and q.stage='ACQUIRE' and q.payload->>'kind'='REF01_CENSUS_SOURCE_V1') IS NOT TRUE) or (c.next_provider_at<=clock_timestamp() and not exists(select 1 from population_top3000_202608_202609_v1.pipeline_provider_lease where released_at is null)))
 order by (adapter_rows.enabled is true) desc, coalesce(fair.last_dispatched_at,'-infinity'::timestamptz),
 case when q.family_id='REF01' then
  case when ref01_turn.validation_due then case when q.stage='VALIDATE' then 0 else 1 end
   else case when q.stage='VALIDATE' then 1 else 0 end end
  else 0 end,
 case when q.family_id='REF01' and q.stage='REPAIR' then 0 else 1 end,
 case when q.family_id in('P01','P02') and q.payload->>'kind'='RETAINED_ALPACA_AUGUST_SESSION' then 20 else 0 end, q.executions,q.created_at,q.task_id for update of q skip locked limit 1;
 if not found then
  select min(q.not_before) into wake from population_top3000_202608_202609_v1.pipeline_task q where status='READY' and not_before>clock_timestamp();
  if exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') then wake:=least(coalesce(wake,'infinity'),clock_timestamp()+interval '5 seconds');
  elsif exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='READY') then wake:=least(coalesce(wake,'infinity'),greatest(c.next_provider_at,clock_timestamp()+interval '30 seconds'));
  else wake:=least(coalesce(wake,'infinity'),clock_timestamp()+interval '5 minutes');end if;
  update population_top3000_202608_202609_v1.pipeline_control set next_wakeup_at=wake,idle_rechecks=idle_rechecks+1,wake_reason='WAIT_FOR_READY_TASK_OR_EVIDENCE' where singleton;
  if not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status in('READY','LEASED')) and p.idle_rechecks>=p.max_idle_rechecks and not exists(select 1 from population_top3000_202608_202609_v1.continuation_clear_queue_v6 where status in('READY','NEEDS_REVIEW','CLAIMED','WAITING_OUTPUT') and not_after>clock_timestamp()) then
   update population_top3000_202608_202609_v1.pipeline_control set enabled=false,wake_reason='BOUNDED_WAIT_EXHAUSTED_REQUIRES_NEW_EVIDENCE' where singleton;
   perform cron.alter_job(596,schedule=>'*/5 * * * *',active=>false);
   insert into population_top3000_202608_202609_v1.pipeline_event(event,details)values('BOUNDED_WAIT_DISARMED',jsonb_build_object('resumption','New authorised evidence/adapters admitted by controller; preserve attempts and counters'));
  elsif not exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='LEASED') then
   if exists(select 1 from population_top3000_202608_202609_v1.pipeline_task where status='READY') then perform cron.alter_job(596,schedule=>'2 seconds');
   else perform cron.alter_job(596,schedule=>'17 seconds');end if;end if;
  return jsonb_build_object('state','BOUNDED_WAIT','not_before',wake);
 end if;
 select * into b from population_top3000_202608_202609_v1.pipeline_adapter where family_id=t.family_id and stage=t.stage;
 if not found or not b.enabled then
  update population_top3000_202608_202609_v1.pipeline_task set status='BLOCKED',blocker_code='ADAPTER_NOT_INSTALLED',resumption_condition=jsonb_build_object('family',t.family_id,'stage',t.stage,'required_action','Install reviewed scoped adapter'),blocker_until=clock_timestamp()+interval '5 minutes' where task_id=t.task_id;
  return jsonb_build_object('state','ADAPTER_NOT_INSTALLED','task_id',t.task_id);
 end if;
 h:=encode(sha256(convert_to(pg_get_functiondef(b.function_oid),'UTF8')),'hex');
 if h is distinct from b.function_sha256 then
  update population_top3000_202608_202609_v1.pipeline_task set status='BLOCKED',blocker_code='CODE_INTEGRITY',resumption_condition=jsonb_build_object('required_action','Reconcile changed adapter definition and tests; bind reviewed revision') where task_id=t.task_id;return '{"state":"CODE_INTEGRITY_HOLD"}';
 end if;
 a:=gen_random_uuid();tok:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
 update population_top3000_202608_202609_v1.pipeline_task set status='LEASED',attempt_id=a,lease_until=least(clock_timestamp()+interval '90 seconds',c.admitted_until),owner_fence=c.owner_fence,stop_generation=c.stop_generation,executions=executions+1,started_at=coalesce(started_at,clock_timestamp()) where task_id=t.task_id;
 insert into population_top3000_202608_202609_v1.pipeline_attempt(attempt_id,task_id,execution_number,token_sha256,owner_fence,stop_generation,adapter_sha256) values(a,t.task_id,t.executions+1,encode(sha256(convert_to(tok,'UTF8')),'hex'),c.owner_fence,c.stop_generation,b.function_sha256);
 update population_top3000_202608_202609_v1.pipeline_control set next_wakeup_at=clock_timestamp(),idle_rechecks=0,wake_reason=null where singleton;
 perform cron.alter_job(596,schedule=>'2 seconds');
 update population_top3000_202608_202609_v1.acquisition_control set next_stage='PIPELINE_EXECUTION',next_action=t.family_id||'/'||t.stage||'/'||t.partition_key where singleton;
 select format('%I.%I',n.nspname,r.proname) into fn from pg_proc r join pg_namespace n on n.oid=r.pronamespace where r.oid=b.function_oid and r.pronargs=2 and r.proargtypes[0]='uuid'::regtype and r.proargtypes[1]='text'::regtype;
 begin
  if fn is null then raise exception 'INVALID_ADAPTER_SIGNATURE';end if;
  execute format('select %s($1,$2)',fn) into receipt using a,tok;
  if receipt->>'state'='ASYNC' then result:=receipt;
  elsif (select outcome from population_top3000_202608_202609_v1.pipeline_attempt where attempt_id=a)<>'RUNNING' then result:=receipt;
  else result:=population_top3000_202608_202609_v1.pipeline_finish_v1(a,tok,receipt);end if;
 exception
 when query_canceled then
  -- The adapter/finish subtransaction has rolled back. Preserve the outer attempt and failure receipt.
  -- Non-DATABASE cancellation retains its existing external-effect handling.
  if b.resource_class is distinct from 'DATABASE' then raise;end if;
  result:=population_top3000_202608_202609_v1.pipeline_record_database_cancel_v1(a,tok);
 when others then
  get stacked diagnostics err=MESSAGE_TEXT,errstate=RETURNED_SQLSTATE;
  result:=population_top3000_202608_202609_v1.pipeline_fail_v1(a,tok,case when errstate in('55P03','40P01') then 'LOCK' else 'PARSER' end,errstate||':'||left(err,150),jsonb_build_object('sqlstate',errstate,'message',left(err,500)),60);
 end;
 insert into population_top3000_202608_202609_v1.pipeline_event(task_id,attempt_id,event,details)values(t.task_id,a,'DISPATCHED',jsonb_build_object('family',t.family_id,'stage',t.stage,'partition',t.partition_key,'resource',b.resource_class,'result',result));
 return jsonb_build_object('state','DISPATCHED','task_id',t.task_id,'attempt_id',a,'result',result);
end $function$


;
DO $verify$
DECLARE r record;h text;
BEGIN
 FOR r IN SELECT key,value FROM jsonb_each_text($j${"pipeline_tick_v1": "c8df19b574c748d5494b0200b981cbd338864c6ea57e7e2240dce14aff5fc2d1", "ref01_date_admit_v4": "180e882e09a2183788fc63f1f8f5db9c34cf0ad463748848e0f1656f985516e7", "ref01_date_bundle_v4": "91705e61919dd4f874859ac99b93a6919e860c824838eb796804ac9e40aa98df", "ref01_date_commit_v4": "0e92ae64df07e1b605f0678d5e3c8931eeaf7055fd5d183dd975db606308fc1f", "ref01_date_acquire_v4": "02b8a28363dab46f274860d43078e15349667e552e73064a6bd8274318a10b07", "ref01_date_context_v4": "c0b3bdb12097583964cb9e3d11e6f7e0500d31ce0470d94815c40afdbe5ba319", "ref01_date_continue_v9": "b7283dc0fab2abcdeedd754dabcd2a4d55eb7eb113199576f15e45af2b4684b3", "ref01_date_continue_impl_v9": "ca43dbdd5a984456e6f71104c291989a27a48ab85f03a2952c1fccea88902791"}$j$::jsonb) LOOP
  SELECT encode(sha256(convert_to(pg_get_functiondef(oid),'UTF8')),'hex') INTO STRICT h FROM pg_proc WHERE pronamespace='population_top3000_202608_202609_v1'::regnamespace AND proname=r.key;
  IF h IS DISTINCT FROM r.value THEN RAISE EXCEPTION 'POSTINSTALL_HASH_MISMATCH %',r.key;END IF;
 END LOOP;
 IF population_top3000_202608_202609_v1.ref01_date_bundle_v4() IS DISTINCT FROM 'f7af9ad2ab69f104c0c85dae02d7d2b65f3df07f6849ca2d8118e6828da6e35a' THEN RAISE EXCEPTION 'POSTINSTALL_BUNDLE_MISMATCH';END IF;
 IF (SELECT content_sha256 FROM population_top3000_202608_202609_v1.ref01_date_plan_v9 WHERE plan_id='ref01-date-fullperiod-v9') IS DISTINCT FROM 'f6596e461a84315e6cadfc11ec8bf1ef52cd0d5b3011d4df0353d5976370c319' THEN RAISE EXCEPTION 'POSTINSTALL_PLAN_MISMATCH';END IF;
 IF encode(sha256(convert_to(population_top3000_202608_202609_v1.population_recovery_manifest_v1()::text,'UTF8')),'hex') IS DISTINCT FROM '96a9d25997ed777cba133c800c633256c91c6f14802461b357383f546ae49d44' THEN RAISE EXCEPTION 'POSTINSTALL_RECOVERY_MANIFEST_MISMATCH';END IF;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) SELECT 'REF01_FULL_PERIOD_CONTINUATION_DEPLOYED_V9',jsonb_build_object('owner_id','top3000-completion-20261024-root-5db48b32d103','owner_fence',3,'stop_generation',1,'scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','code_commit','__CODE_COMMIT__','claim_event_ids',jsonb_build_array(29634,29635),'independent_review_event_id',review_event_id,'admission_event_id',admission_event_id,'plan_sha256','f6596e461a84315e6cadfc11ec8bf1ef52cd0d5b3011d4df0353d5976370c319','bundle_sha256','f7af9ad2ab69f104c0c85dae02d7d2b65f3df07f6849ca2d8118e6828da6e35a','recovery_manifest_sha256','96a9d25997ed777cba133c800c633256c91c6f14802461b357383f546ae49d44','family_accepted',false,'actual_installation_hashes_verified',true) FROM ref01_integration_metadata_v9;
END;$verify$;
COMMIT;
