BEGIN;SET LOCAL statement_timeout='15s';SET LOCAL lock_timeout='500ms';SELECT pg_advisory_xact_lock(2026083000261022);DO $guard$ BEGIN IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND owner_fence=2 AND stop_generation=0 AND stopped=false AND enabled=true AND hold_reason IS NULL AND admitted_until>clock_timestamp() AND scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd') OR EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED') OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) OR to_regclass('population_top3000_202608_202609_v1.ref01_resolution_run_v2') IS NOT NULL THEN RAISE EXCEPTION 'REF01_INITIAL_SCHEMA_REQUIRES_OWNER_PAUSE_DRAIN_AND_NO_EXISTING_RUN';END IF;END $guard$;
-- LOCAL DRAFT. Root alone installs, runs the actual rollback, binds and admits.
-- Retained data only. No HTTP, provider dispatch, source overwrite, cohort freeze or publication.
-- All existing REF01 source rows, old review groups, old attempts and old handlers are preserved.

CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_resolution_run_v2 (
 run_key text PRIMARY KEY,
 purpose text NOT NULL CHECK(purpose IN('FULL_CENSUS','ROLLBACK_FIXTURE')),
 scope_sha256 text NOT NULL CHECK(scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'),
 rank_date date NOT NULL CHECK(rank_date=DATE '2026-08-03'),
 rank_instant timestamptz NOT NULL CHECK(rank_instant=TIMESTAMPTZ '2026-08-03 20:00:00+00'),
 candidate_manifest_sha256 text NOT NULL CHECK(candidate_manifest_sha256~'^[0-9a-f]{64}$'),
 group_manifest_sha256 text NOT NULL CHECK(group_manifest_sha256~'^[0-9a-f]{64}$'),
 source_manifest_sha256 text NOT NULL CHECK(source_manifest_sha256~'^[0-9a-f]{64}$'),
 fact_manifest_sha256 text NOT NULL CHECK(fact_manifest_sha256~'^[0-9a-f]{64}$'),
 policy jsonb NOT NULL CHECK(jsonb_typeof(policy)='object'),
 policy_sha256 text NOT NULL CHECK(policy_sha256=encode(sha256(convert_to(policy::text,'UTF8')),'hex')),
 input_versions jsonb NOT NULL CHECK(jsonb_typeof(input_versions)='object'),
 expected_inputs integer NOT NULL CHECK(expected_inputs BETWEEN 1 AND 13044),
 expected_overviews integer NOT NULL CHECK(expected_overviews BETWEEN 1 AND 5684),
 expected_census_pages integer NOT NULL CHECK(expected_census_pages=14),
 expected_sources integer NOT NULL CHECK(expected_sources BETWEEN 15 AND 5698),
 expected_groups integer NOT NULL CHECK(expected_groups=5553),
 owner_fence bigint NOT NULL CHECK(owner_fence=2),
 stop_generation bigint NOT NULL CHECK(stop_generation=0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(purpose='ROLLBACK_FIXTURE' OR (expected_inputs=13044 AND expected_overviews=5684 AND expected_sources=5698))
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_resolution_source_v2 (
 run_key text NOT NULL REFERENCES population_top3000_202608_202609_v1.ref01_resolution_run_v2(run_key),
 source_ordinal integer NOT NULL CHECK(source_ordinal>0),
 source_job_id text NOT NULL,
 source_kind text NOT NULL CHECK(source_kind IN('CENSUS','OVERVIEW')),
 source_sha256 text NOT NULL CHECK(source_sha256~'^[0-9a-f]{64}$'),
 source_bytes bigint NOT NULL CHECK(source_bytes BETWEEN 1 AND 8388608),
 snapshot_date date NOT NULL CHECK(snapshot_date=DATE '2026-08-03'),
 page_number integer, ticker text, expected_records integer NOT NULL CHECK(expected_records BETWEEN 1 AND 1000),
 request_params jsonb NOT NULL,
 source_evidence_sha256 text NOT NULL CHECK(source_evidence_sha256~'^[0-9a-f]{64}$'),
 received_at timestamptz NOT NULL,
 PRIMARY KEY(run_key,source_job_id), UNIQUE(run_key,source_ordinal)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_resolution_input_v2 (
 run_key text NOT NULL REFERENCES population_top3000_202608_202609_v1.ref01_resolution_run_v2(run_key),
 input_ordinal integer NOT NULL CHECK(input_ordinal>0), ticker text NOT NULL,
 census_job_id text NOT NULL, census_ordinal integer NOT NULL CHECK(census_ordinal>0),
 overview_job_id text, candidate_record jsonb NOT NULL,
 candidate_record_sha256 text NOT NULL CHECK(candidate_record_sha256=encode(sha256(convert_to(candidate_record::text,'UTF8')),'hex')),
 PRIMARY KEY(run_key,ticker), UNIQUE(run_key,input_ordinal), UNIQUE(run_key,census_job_id,census_ordinal)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_resolution_fact_v2 (
 run_key text NOT NULL REFERENCES population_top3000_202608_202609_v1.ref01_resolution_run_v2(run_key),
 evidence_sha256 text NOT NULL, source_id text NOT NULL, source_sha256 text NOT NULL,
 tickers text[] NOT NULL, component text NOT NULL, record_json jsonb NOT NULL,
 PRIMARY KEY(run_key,evidence_sha256),
 CHECK(evidence_sha256=encode(sha256(convert_to(record_json::text,'UTF8')),'hex'))
);
CREATE INDEX IF NOT EXISTS ref01_resolution_fact_tickers_v2 ON population_top3000_202608_202609_v1.ref01_resolution_fact_v2 USING gin(tickers);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_resolution_source_proof_v2 (
 run_key text NOT NULL, source_job_id text NOT NULL, proof jsonb NOT NULL,
 proof_sha256 text NOT NULL CHECK(proof_sha256=encode(sha256(convert_to(proof::text,'UTF8')),'hex')),
 first_attempt_id uuid NOT NULL, verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(run_key,source_job_id),
 FOREIGN KEY(run_key,source_job_id) REFERENCES population_top3000_202608_202609_v1.ref01_resolution_source_v2(run_key,source_job_id)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_resolution_record_proof_v2 (
 run_key text NOT NULL, source_job_id text NOT NULL, source_ordinal integer NOT NULL,
 ticker text NOT NULL, record_sha256 text NOT NULL CHECK(record_sha256~'^[0-9a-f]{64}$'),
 PRIMARY KEY(run_key,source_job_id,source_ordinal), UNIQUE(run_key,source_job_id,ticker),
 FOREIGN KEY(run_key,source_job_id) REFERENCES population_top3000_202608_202609_v1.ref01_resolution_source_v2(run_key,source_job_id)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_security_decision_v2 (
 run_key text NOT NULL, ticker text NOT NULL, input_ordinal integer NOT NULL,
 case_key text NOT NULL,
 eligibility_state text NOT NULL CHECK(eligibility_state IN('SOURCE_SUPPORTED_COMMON','SOURCE_SUPPORTED_EXCLUSION','REVIEW_REQUIRED')),
 company_identity_state text NOT NULL CHECK(company_identity_state IN('SOURCE_SUPPORTED_ISSUER','REVIEW_REQUIRED')),
 security_identity_state text NOT NULL CHECK(security_identity_state IN('DATED_NATIVE_IDENTIFIERS','DATED_NATIVE_TICKER_ONLY','REVIEW_REQUIRED')),
 projection jsonb NOT NULL,
 output_sha256 text NOT NULL CHECK(output_sha256=encode(sha256(convert_to(projection::text,'UTF8')),'hex')),
 PRIMARY KEY(run_key,ticker), UNIQUE(run_key,input_ordinal),
 FOREIGN KEY(run_key,ticker) REFERENCES population_top3000_202608_202609_v1.ref01_resolution_input_v2(run_key,ticker)
);
CREATE INDEX IF NOT EXISTS ref01_security_case_v2 ON population_top3000_202608_202609_v1.ref01_security_decision_v2(run_key,case_key,ticker);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_company_work_v2 (
 run_key text NOT NULL, case_key text NOT NULL, case_ordinal integer NOT NULL,
 tickers text[] NOT NULL, security_manifest_sha256 text NOT NULL CHECK(security_manifest_sha256~'^[0-9a-f]{64}$'),
 PRIMARY KEY(run_key,case_key), UNIQUE(run_key,case_ordinal),
 FOREIGN KEY(run_key) REFERENCES population_top3000_202608_202609_v1.ref01_resolution_run_v2(run_key)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_company_decision_v2 (
 run_key text NOT NULL, decision_revision text NOT NULL, case_key text NOT NULL,
 company_key text,
 decision_state text NOT NULL CHECK(decision_state IN('SOURCE_QUALIFIED_DATED_VENDOR','TARGETED_EVIDENCE_REQUIRED','SOURCE_SUPPORTED_EXCLUSION')),
 source_company_cap_usd numeric,
 verified_rank_cap_usd numeric,
 content jsonb NOT NULL,
 output_sha256 text NOT NULL CHECK(output_sha256=encode(sha256(convert_to(content::text,'UTF8')),'hex')),
 first_attempt_id uuid NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(run_key,decision_revision,case_key),
 FOREIGN KEY(run_key,case_key) REFERENCES population_top3000_202608_202609_v1.ref01_company_work_v2(run_key,case_key),
 CHECK(source_company_cap_usd IS NULL OR source_company_cap_usd>0),
 CHECK(verified_rank_cap_usd IS NULL), -- Initial lane does not independently certify the ranking cap.
 CHECK((content->'original_historical_receipt_proven') IS NOT DISTINCT FROM 'false'::jsonb),
 CHECK((content->'company_membership_frozen') IS NOT DISTINCT FROM 'false'::jsonb)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_evidence_requirement_v2 (
 run_key text NOT NULL, decision_revision text NOT NULL, case_key text NOT NULL, requirement_code text NOT NULL,
 priority integer NOT NULL CHECK(priority BETWEEN 1 AND 100),
 requirement jsonb NOT NULL,
 requirement_sha256 text NOT NULL CHECK(requirement_sha256=encode(sha256(convert_to(requirement::text,'UTF8')),'hex')),
 PRIMARY KEY(run_key,decision_revision,case_key,requirement_code),
 FOREIGN KEY(run_key,decision_revision,case_key) REFERENCES population_top3000_202608_202609_v1.ref01_company_decision_v2(run_key,decision_revision,case_key)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_resolution_batch_v2 (
 run_key text NOT NULL, stage text NOT NULL CHECK(stage IN('ACQUIRE','NORMALIZE','DERIVE')),
 batch_number integer NOT NULL CHECK(batch_number>0),
 first_ordinal integer NOT NULL, last_ordinal integer NOT NULL, unit_count integer NOT NULL CHECK(unit_count>0),
 output_sha256 text NOT NULL CHECK(output_sha256~'^[0-9a-f]{64}$'),
 input_versions jsonb NOT NULL, first_attempt_id uuid NOT NULL,
 PRIMARY KEY(run_key,stage,batch_number)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_resolution_stage_v2 (
 run_key text NOT NULL, stage text NOT NULL CHECK(stage IN('ACQUIRE','NORMALIZE','DERIVE')),
 units integer NOT NULL CHECK(units>0), batch_count integer NOT NULL CHECK(batch_count>0),
 output_sha256 text NOT NULL CHECK(output_sha256~'^[0-9a-f]{64}$'),
 input_versions jsonb NOT NULL,
 PRIMARY KEY(run_key,stage)
);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ref01_sustained_legacy_adapter_v2 (
 stage text PRIMARY KEY CHECK(stage IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE')),
 function_oid oid NOT NULL, function_sha256 text NOT NULL CHECK(function_sha256~'^[0-9a-f]{64}$'),
 registration_evidence jsonb NOT NULL
);

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2()
RETURNS void LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_limit bigint;v_bytes bigint;v_ref01_bytes bigint;
BEGIN
 SELECT (p.budget_evidence#>>'{resource_limits,max_relation_bytes}')::bigint INTO v_limit
 FROM population_top3000_202608_202609_v1.pipeline_control p WHERE p.singleton;
 IF v_limit IS NULL OR v_limit<=0 THEN RAISE EXCEPTION 'REF01_CURRENT_RELATION_ADMISSION_REQUIRED';END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint INTO v_bytes
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m');
 IF v_bytes>v_limit THEN RAISE EXCEPTION 'REF01_RELATION_BOUND_EXCEEDED actual=% admitted=%',v_bytes,v_limit;END IF;
 SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint INTO v_ref01_bytes
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind IN('r','m') AND c.relname=ANY(ARRAY[
  'ref01_resolution_run_v2','ref01_resolution_source_v2','ref01_resolution_input_v2','ref01_resolution_fact_v2','ref01_resolution_source_proof_v2','ref01_resolution_record_proof_v2','ref01_security_decision_v2','ref01_company_work_v2','ref01_company_decision_v2','ref01_evidence_requirement_v2','ref01_resolution_batch_v2','ref01_resolution_stage_v2','ref01_sustained_legacy_adapter_v2']);
 IF v_ref01_bytes>536870912 THEN RAISE EXCEPTION 'REF01_NEW_RELATION_BOUND_EXCEEDED actual=% admitted=536870912',v_ref01_bytes;END IF;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_number_v2(p_value jsonb)
RETURNS numeric LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path='pg_catalog' AS $f$
 SELECT CASE WHEN jsonb_typeof(p_value)='number' THEN (p_value#>>'{}')::numeric ELSE NULL END
$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_cursor_v2(p_url text)
RETURNS text LANGUAGE plpgsql IMMUTABLE SET search_path='pg_catalog' AS $f$
DECLARE v_cursor text;
BEGIN
 IF nullif(p_url,'') IS NULL THEN RETURN NULL;END IF;
 IF p_url !~ '^https://api[.](massive[.]com|polygon[.]io)/v3/reference/tickers[?]' THEN RAISE EXCEPTION 'REF01_CENSUS_NEXT_ORIGIN_INVALID';END IF;
 v_cursor:=substring(p_url from '[?&]cursor=([^&]+)');
 v_cursor:=regexp_replace(regexp_replace(regexp_replace(v_cursor,'%3[dD]','=','g'),'%2[bB]','+','g'),'%2[fF]','/','g');
 IF v_cursor IS NULL OR v_cursor !~ '^[A-Za-z0-9_+/-]+={0,2}$' THEN RAISE EXCEPTION 'REF01_CENSUS_CURSOR_INVALID';END IF;
 RETURN v_cursor;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_context_v2(p_task uuid,p_stage text)
RETURNS population_top3000_202608_202609_v1.ref01_resolution_run_v2 LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_run population_top3000_202608_202609_v1.ref01_resolution_run_v2%rowtype;
BEGIN
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=p_task;
 SELECT * INTO v_run FROM population_top3000_202608_202609_v1.ref01_resolution_run_v2 r WHERE r.run_key=v_task.payload->>'run_key';
 IF NOT FOUND OR v_task.family_id IS DISTINCT FROM 'REF01' OR v_task.stage IS DISTINCT FROM p_stage
 OR v_task.partition_key IS DISTINCT FROM v_run.run_key
 OR v_task.payload IS DISTINCT FROM jsonb_build_object('kind','REF01_SUSTAINED_RECONCILIATION_V2','run_key',v_run.run_key)
 OR v_task.input_versions IS DISTINCT FROM v_run.input_versions
 OR (v_run.purpose='ROLLBACK_FIXTURE' AND current_setting('ref01.rollback_fixture',true) IS DISTINCT FROM v_run.run_key)
 OR v_run.policy_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_run.policy::text,'UTF8')),'hex')
 OR v_run.input_versions->>'resolution_bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ref01_resolution_bundle_sha256_v2()
 THEN RAISE EXCEPTION 'REF01_RUN_STAGE_INPUT_OR_BUNDLE_MISMATCH';END IF;
 IF p_stage IN('ACQUIRE','NORMALIZE','DERIVE') THEN
  IF v_task.checkpoint='{}'::jsonb THEN NULL;
  ELSIF jsonb_typeof(v_task.checkpoint->'last_ordinal') IS DISTINCT FROM 'number' OR jsonb_typeof(v_task.checkpoint->'batch_number') IS DISTINCT FROM 'number'
   OR v_task.checkpoint IS DISTINCT FROM jsonb_build_object('last_ordinal',(v_task.checkpoint->>'last_ordinal')::integer,'batch_number',(v_task.checkpoint->>'batch_number')::integer)
   OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_batch_v2 b WHERE b.run_key=v_run.run_key AND b.stage=p_stage
    AND b.batch_number=(v_task.checkpoint->>'batch_number')::integer AND b.last_ordinal=(v_task.checkpoint->>'last_ordinal')::integer AND b.input_versions=v_task.input_versions)
   THEN RAISE EXCEPTION 'REF01_CHECKPOINT_WITHOUT_COMMITTED_PREDECESSOR';END IF;
 END IF;
 RETURN v_run;
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_batch_receipt_v2(
 p_attempt uuid,p_task uuid,p_run text,p_stage text,p_batch integer,p_lo integer,p_hi integer,p_units integer,p_hash text,p_fresh bigint,p_terminal boolean)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_total integer;v_batches integer;v_manifest text;v_old record;
BEGIN
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=p_task;
 INSERT INTO population_top3000_202608_202609_v1.ref01_resolution_batch_v2(run_key,stage,batch_number,first_ordinal,last_ordinal,unit_count,output_sha256,input_versions,first_attempt_id)
 VALUES(p_run,p_stage,p_batch,p_lo,p_hi,p_units,p_hash,v_task.input_versions,p_attempt) ON CONFLICT DO NOTHING;
 SELECT * INTO v_old FROM population_top3000_202608_202609_v1.ref01_resolution_batch_v2 b WHERE b.run_key=p_run AND b.stage=p_stage AND b.batch_number=p_batch;
 IF v_old.first_ordinal IS DISTINCT FROM p_lo OR v_old.last_ordinal IS DISTINCT FROM p_hi OR v_old.unit_count IS DISTINCT FROM p_units
 OR v_old.output_sha256 IS DISTINCT FROM p_hash OR v_old.input_versions IS DISTINCT FROM v_task.input_versions THEN RAISE EXCEPTION 'REF01_BATCH_REPLAY_CONFLICT';END IF;
 IF p_terminal THEN
  SELECT sum(b.unit_count)::integer,count(*)::integer,encode(sha256(convert_to(string_agg(b.batch_number||':'||b.first_ordinal||':'||b.last_ordinal||':'||b.unit_count||':'||b.output_sha256,E'\n' ORDER BY b.batch_number),'UTF8')),'hex')
  INTO v_total,v_batches,v_manifest FROM population_top3000_202608_202609_v1.ref01_resolution_batch_v2 b WHERE b.run_key=p_run AND b.stage=p_stage;
  IF v_total IS DISTINCT FROM p_hi OR v_batches IS DISTINCT FROM p_batch OR EXISTS(
   SELECT 1 FROM(SELECT b.first_ordinal,lag(b.last_ordinal,1,0) OVER(ORDER BY b.batch_number) prior_last FROM population_top3000_202608_202609_v1.ref01_resolution_batch_v2 b WHERE b.run_key=p_run AND b.stage=p_stage) q WHERE q.first_ordinal<>q.prior_last+1)
  THEN RAISE EXCEPTION 'REF01_STAGE_BATCH_COVERAGE_MISMATCH';END IF;
  INSERT INTO population_top3000_202608_202609_v1.ref01_resolution_stage_v2(run_key,stage,units,batch_count,output_sha256,input_versions)
  VALUES(p_run,p_stage,v_total,v_batches,v_manifest,v_task.input_versions) ON CONFLICT DO NOTHING;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_stage_v2 s WHERE s.run_key=p_run AND s.stage=p_stage AND s.units=v_total AND s.batch_count=v_batches AND s.output_sha256=v_manifest AND s.input_versions=v_task.input_versions) THEN RAISE EXCEPTION 'REF01_STAGE_REPLAY_CONFLICT';END IF;
 END IF;
 RETURN jsonb_build_object('state',CASE WHEN p_terminal THEN 'COMPLETE' ELSE 'CONTINUE' END,
 'checkpoint',jsonb_build_object('last_ordinal',p_hi,'batch_number',p_batch),
 'commit_receipt',jsonb_build_object('commit_key','ref01-resolution-v2:'||p_run||':'||p_stage||':'||p_batch,
 'output_sha256',p_hash,'new_committed_units',p_fresh,'immutable_output_ids',jsonb_build_array(p_run||':'||p_stage||':'||p_batch),
 'input_versions',v_task.input_versions,'first_ordinal',p_lo,'last_ordinal',p_hi,'batch_units',p_units,
 'no_provider_request',true,'source_processing_only',true,'company_membership_frozen',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_acquire_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_tid uuid;v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_r population_top3000_202608_202609_v1.ref01_resolution_run_v2%rowtype;
 v_s record;v_a population_top3000_202608_202609_v1.acquisition_raw%rowtype;v_j population_top3000_202608_202609_v1.acquisition_job%rowtype;
 v_doc jsonb;v_records jsonb;v_proof jsonb;v_cursor text;v_previous_cursor text;v_count integer;v_bad bigint;v_rec_hash text;
 v_last integer;v_hi integer;v_n integer:=0;v_batch integer;v_fresh bigint:=0;v_inserted bigint;v_output text;v_sum_bytes bigint:=0;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 v_r:=population_top3000_202608_202609_v1.ref01_resolution_context_v2(v_tid,'ACQUIRE');
 PERFORM population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2();
 v_last:=coalesce((v_t.checkpoint->>'last_ordinal')::integer,0);v_batch:=coalesce((v_t.checkpoint->>'batch_number')::integer,0)+1;
 IF v_last<0 OR v_last>=v_r.expected_sources OR v_batch<1 THEN RAISE EXCEPTION 'REF01_SOURCE_CHECKPOINT_INVALID';END IF;
 FOR v_s IN SELECT q.* FROM(
  SELECT s.*,sum(s.source_bytes) OVER(ORDER BY s.source_ordinal) bounded_bytes FROM population_top3000_202608_202609_v1.ref01_resolution_source_v2 s
  WHERE s.run_key=v_r.run_key AND s.source_ordinal>v_last ORDER BY s.source_ordinal LIMIT 50
 ) q WHERE q.bounded_bytes<=8388608 ORDER BY q.source_ordinal LOOP
  IF v_s.source_ordinal<>v_last+v_n+1 THEN RAISE EXCEPTION 'REF01_SOURCE_INPUT_GAP';END IF;
  SELECT * INTO v_a FROM population_top3000_202608_202609_v1.acquisition_raw a WHERE a.job_id=v_s.source_job_id;
  SELECT * INTO v_j FROM population_top3000_202608_202609_v1.acquisition_job j WHERE j.job_id=v_s.source_job_id;
  IF v_a.raw_text IS NULL OR v_a.sha256 IS DISTINCT FROM v_s.source_sha256 OR v_a.source_bytes IS DISTINCT FROM v_s.source_bytes
  OR octet_length(v_a.raw_text)::bigint IS DISTINCT FROM v_s.source_bytes
  OR encode(sha256(convert_to(v_a.raw_text,'UTF8')),'hex') IS DISTINCT FROM v_s.source_sha256
  OR encode(sha256(convert_to(v_a.evidence::text,'UTF8')),'hex') IS DISTINCT FROM v_s.source_evidence_sha256
  OR v_a.received_at IS DISTINCT FROM v_s.received_at OR v_j.kind IS DISTINCT FROM v_s.source_kind
  OR v_j.status IS DISTINCT FROM 'COMPLETE' OR v_j.snapshot_date IS DISTINCT FROM v_s.snapshot_date OR v_j.params IS DISTINCT FROM v_s.request_params
  OR v_j.ticker IS DISTINCT FROM v_s.ticker OR v_j.page_number IS DISTINCT FROM v_s.page_number
  OR v_a.evidence->>'provider' IS DISTINCT FROM 'Massive' OR v_a.evidence->>'http_status' IS DISTINCT FROM '200'
  OR v_a.evidence->'request_params' IS DISTINCT FROM v_s.request_params THEN RAISE EXCEPTION 'REF01_SOURCE_VERSION_OR_DATE_MISMATCH job=%',v_s.source_job_id;END IF;
  v_doc:=v_a.raw_text::jsonb;
  IF v_doc->>'status' IS DISTINCT FROM 'OK' THEN RAISE EXCEPTION 'REF01_SOURCE_STATUS_INVALID';END IF;
  IF v_s.source_kind='CENSUS' THEN
   IF jsonb_typeof(v_doc->'results') IS DISTINCT FROM 'array' OR v_a.evidence->>'route' IS DISTINCT FROM '/v3/reference/tickers' THEN RAISE EXCEPTION 'REF01_CENSUS_ENVELOPE_INVALID';END IF;
   v_records:=v_doc->'results';v_cursor:=population_top3000_202608_202609_v1.ref01_resolution_cursor_v2(v_doc->>'next_url');
   IF v_s.page_number=1 THEN
    IF v_s.request_params IS DISTINCT FROM '{"date":"2026-08-03","sort":"ticker","limit":"1000","order":"asc","active":"true","locale":"us","market":"stocks"}'::jsonb THEN RAISE EXCEPTION 'REF01_CENSUS_FIRST_QUERY_CHANGED';END IF;
   ELSE
    SELECT p.proof->>'next_cursor' INTO v_previous_cursor FROM population_top3000_202608_202609_v1.ref01_resolution_source_v2 s JOIN population_top3000_202608_202609_v1.ref01_resolution_source_proof_v2 p USING(run_key,source_job_id)
    WHERE s.run_key=v_r.run_key AND s.source_kind='CENSUS' AND s.page_number=v_s.page_number-1;
    IF v_previous_cursor IS NULL OR v_s.request_params IS DISTINCT FROM jsonb_build_object('cursor',v_previous_cursor) THEN RAISE EXCEPTION 'REF01_CENSUS_PAGE_CHAIN_MISMATCH';END IF;
   END IF;
   IF (v_s.page_number<v_r.expected_census_pages AND v_cursor IS NULL) OR (v_s.page_number=v_r.expected_census_pages AND v_cursor IS NOT NULL) THEN RAISE EXCEPTION 'REF01_CENSUS_TERMINAL_MISMATCH';END IF;
  ELSE
   IF jsonb_typeof(v_doc->'results') IS DISTINCT FROM 'object' OR v_doc->'results'->>'ticker' IS DISTINCT FROM v_s.ticker
   OR v_s.request_params IS DISTINCT FROM '{"date":"2026-08-03"}'::jsonb
   OR v_a.evidence->>'route' IS DISTINCT FROM ('/v3/reference/tickers/'||v_s.ticker) THEN RAISE EXCEPTION 'REF01_OVERVIEW_ENVELOPE_OR_DATE_INVALID';END IF;
   v_records:=jsonb_build_array(v_doc->'results');v_cursor:=NULL;
  END IF;
  v_count:=jsonb_array_length(v_records);
  IF v_count<>v_s.expected_records OR EXISTS(SELECT 1 FROM jsonb_array_elements(v_records) x WHERE jsonb_typeof(x)<>'object' OR nullif(x->>'ticker','') IS NULL) THEN RAISE EXCEPTION 'REF01_SOURCE_RECORD_SHAPE_COUNT_INVALID';END IF;
  IF v_s.source_kind='CENSUS' THEN
   WITH expected AS MATERIALIZED(SELECT x.ordinality::integer source_ordinal,x.value raw_record FROM jsonb_array_elements(v_records) WITH ORDINALITY x)
   SELECT count(*) FILTER(WHERE e.source_ordinal IS NULL OR r.ordinal IS NULL OR e.raw_record IS DISTINCT FROM r.raw_record OR e.raw_record->>'ticker' IS DISTINCT FROM r.ticker OR e.raw_record->>'type' IS DISTINCT FROM r.security_type)
   INTO v_bad FROM expected e FULL JOIN(SELECT * FROM population_top3000_202608_202609_v1.reference_row q WHERE q.page_job_id=v_s.source_job_id) r ON r.ordinal=e.source_ordinal;
  ELSE
   SELECT count(*) INTO v_bad FROM population_top3000_202608_202609_v1.ticker_overview o WHERE o.source_job_id=v_s.source_job_id AND o.ticker=v_s.ticker AND o.snapshot_date=v_s.snapshot_date AND o.raw_record=v_doc->'results';
   v_bad:=CASE WHEN v_bad=1 THEN 0 ELSE 1 END;
  END IF;
  IF v_bad<>0 THEN RAISE EXCEPTION 'REF01_SOURCE_NORMALIZED_RECORD_MISMATCH job=%',v_s.source_job_id;END IF;
  INSERT INTO population_top3000_202608_202609_v1.ref01_resolution_record_proof_v2(run_key,source_job_id,source_ordinal,ticker,record_sha256)
  SELECT v_r.run_key,v_s.source_job_id,x.ordinality::integer,x.value->>'ticker',encode(sha256(convert_to(x.value::text,'UTF8')),'hex') FROM jsonb_array_elements(v_records) WITH ORDINALITY x ON CONFLICT DO NOTHING;
  WITH expected AS MATERIALIZED(SELECT x.ordinality::integer source_ordinal,x.value->>'ticker' ticker,encode(sha256(convert_to(x.value::text,'UTF8')),'hex') h FROM jsonb_array_elements(v_records) WITH ORDINALITY x)
  SELECT count(*) FILTER(WHERE e.source_ordinal IS NULL OR p.source_ordinal IS NULL OR e.ticker IS DISTINCT FROM p.ticker OR e.h IS DISTINCT FROM p.record_sha256),
   encode(sha256(convert_to(string_agg(e.source_ordinal||':'||e.ticker||':'||e.h,E'\n' ORDER BY e.source_ordinal),'UTF8')),'hex')
  INTO v_bad,v_rec_hash FROM expected e FULL JOIN(SELECT * FROM population_top3000_202608_202609_v1.ref01_resolution_record_proof_v2 q WHERE q.run_key=v_r.run_key AND q.source_job_id=v_s.source_job_id) p USING(source_ordinal);
  IF v_bad<>0 THEN RAISE EXCEPTION 'REF01_SOURCE_RECORD_PROOF_REPLAY_CONFLICT';END IF;
  v_proof:=jsonb_build_object('source_job_id',v_s.source_job_id,'source_kind',v_s.source_kind,'source_sha256',v_s.source_sha256,'source_bytes',v_s.source_bytes,'snapshot_date',v_s.snapshot_date,
   'request_params',v_s.request_params,'source_evidence_sha256',v_s.source_evidence_sha256,'received_at',v_s.received_at,'record_count',v_count,'record_manifest_sha256',v_rec_hash,'next_cursor',v_cursor,
   'stored_source_bytes_rehashed',true,'normalized_records_exact',true,'wire_representation',v_a.evidence->>'redaction','original_historical_receipt_proven',false);
  INSERT INTO population_top3000_202608_202609_v1.ref01_resolution_source_proof_v2(run_key,source_job_id,proof,proof_sha256,first_attempt_id)
  VALUES(v_r.run_key,v_s.source_job_id,v_proof,encode(sha256(convert_to(v_proof::text,'UTF8')),'hex'),p_attempt) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_inserted=ROW_COUNT;v_fresh:=v_fresh+v_inserted;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_source_proof_v2 p WHERE p.run_key=v_r.run_key AND p.source_job_id=v_s.source_job_id AND p.proof=v_proof AND p.proof_sha256=encode(sha256(convert_to(v_proof::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'REF01_SOURCE_PROOF_REPLAY_CONFLICT';END IF;
  v_n:=v_n+1;v_hi:=v_s.source_ordinal;v_sum_bytes:=v_sum_bytes+v_s.source_bytes;
 END LOOP;
 IF v_n=0 THEN RAISE EXCEPTION 'REF01_NO_BOUNDED_SOURCE_PROGRESS';END IF;
 SELECT encode(sha256(convert_to(string_agg(s.source_ordinal||':'||p.proof_sha256,E'\n' ORDER BY s.source_ordinal),'UTF8')),'hex') INTO v_output
 FROM population_top3000_202608_202609_v1.ref01_resolution_source_v2 s JOIN population_top3000_202608_202609_v1.ref01_resolution_source_proof_v2 p USING(run_key,source_job_id)
 WHERE s.run_key=v_r.run_key AND s.source_ordinal BETWEEN v_last+1 AND v_hi;
 PERFORM population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2();
 RETURN population_top3000_202608_202609_v1.ref01_resolution_batch_receipt_v2(p_attempt,v_tid,v_r.run_key,'ACQUIRE',v_batch,v_last+1,v_hi,v_n,v_output,v_fresh,v_hi=v_r.expected_sources);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_security_projection_v2(p_run text,p_ticker text)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path='pg_catalog' AS $f$
DECLARE v_run population_top3000_202608_202609_v1.ref01_resolution_run_v2%rowtype;
 v_input population_top3000_202608_202609_v1.ref01_resolution_input_v2%rowtype;
 v_current jsonb;v_census jsonb;v_overview jsonb;v_cp text;v_op text;v_type text;v_cik text;v_figi text;v_class text;v_exchange text;
 v_field text;v_flags text[]:=ARRAY[]::text[];v_eligibility text;v_issuer_state text;v_security_state text;v_case text;
 v_reviews jsonb;v_review jsonb;v_facts jsonb;v_refs jsonb;v_issuer_review jsonb;v_kind_review jsonb;v_price jsonb;
 v_cap numeric;v_equivalent numeric;v_class_shares numeric;v_price_rows integer;v_close numeric;v_price_agrees boolean;
 v_listing boolean;v_name_conflict boolean;v_same_fields boolean:=true;v_policy_type text;
BEGIN
 SELECT * INTO v_run FROM population_top3000_202608_202609_v1.ref01_resolution_run_v2 r WHERE r.run_key=p_run;
 SELECT * INTO v_input FROM population_top3000_202608_202609_v1.ref01_resolution_input_v2 i WHERE i.run_key=p_run AND i.ticker=p_ticker;
 IF NOT FOUND THEN RAISE EXCEPTION 'REF01_SECURITY_FROZEN_INPUT_REQUIRED';END IF;
 SELECT to_jsonb(c)-'created_at' INTO v_current FROM population_top3000_202608_202609_v1.company_candidate_v1 c WHERE c.ticker=p_ticker;
 IF v_current IS DISTINCT FROM v_input.candidate_record OR v_input.candidate_record_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_current::text,'UTF8')),'hex') THEN RAISE EXCEPTION 'REF01_CANDIDATE_SNAPSHOT_CHANGED ticker=%',p_ticker;END IF;
 SELECT r.raw_record INTO v_census FROM population_top3000_202608_202609_v1.reference_row r WHERE r.page_job_id=v_input.census_job_id AND r.ordinal=v_input.census_ordinal AND r.ticker=p_ticker;
 SELECT p.record_sha256 INTO v_cp FROM population_top3000_202608_202609_v1.ref01_resolution_record_proof_v2 p WHERE p.run_key=p_run AND p.source_job_id=v_input.census_job_id AND p.source_ordinal=v_input.census_ordinal AND p.ticker=p_ticker;
 IF v_census IS NULL OR v_cp IS NULL OR v_cp IS DISTINCT FROM encode(sha256(convert_to(v_census::text,'UTF8')),'hex')
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_source_proof_v2 p WHERE p.run_key=p_run AND p.source_job_id=v_input.census_job_id AND p.proof_sha256=encode(sha256(convert_to(p.proof::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'REF01_CENSUS_RECORD_NOT_VERIFIED';END IF;
 IF v_input.overview_job_id IS NOT NULL THEN
  SELECT o.raw_record INTO v_overview FROM population_top3000_202608_202609_v1.ticker_overview o WHERE o.source_job_id=v_input.overview_job_id AND o.ticker=p_ticker AND o.snapshot_date=v_run.rank_date;
  SELECT p.record_sha256 INTO v_op FROM population_top3000_202608_202609_v1.ref01_resolution_record_proof_v2 p WHERE p.run_key=p_run AND p.source_job_id=v_input.overview_job_id AND p.source_ordinal=1 AND p.ticker=p_ticker;
  IF v_overview IS NULL OR v_op IS NULL OR v_op IS DISTINCT FROM encode(sha256(convert_to(v_overview::text,'UTF8')),'hex')
  OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_source_proof_v2 p WHERE p.run_key=p_run AND p.source_job_id=v_input.overview_job_id AND p.proof_sha256=encode(sha256(convert_to(p.proof::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'REF01_OVERVIEW_RECORD_NOT_VERIFIED';END IF;
 END IF;
 SELECT coalesce(jsonb_agg(jsonb_build_object('evidence_sha256',f.evidence_sha256,'source_id',f.source_id,'source_sha256',f.source_sha256,'component',f.component,'record_json',f.record_json) ORDER BY f.evidence_sha256),'[]'::jsonb)
 INTO v_facts FROM population_top3000_202608_202609_v1.ref01_resolution_fact_v2 f WHERE f.run_key=p_run AND p_ticker=ANY(f.tickers);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_fact_v2 f
 LEFT JOIN population_top3000_202608_202609_v1.company_component_source_evidence_v1 e ON e.evidence_sha256=f.evidence_sha256
 WHERE f.run_key=p_run AND p_ticker=ANY(f.tickers) AND (e.evidence_sha256 IS NULL OR e.record_json IS DISTINCT FROM f.record_json OR e.source_id IS DISTINCT FROM f.source_id OR e.source_sha256 IS DISTINCT FROM f.source_sha256)) THEN RAISE EXCEPTION 'REF01_COMPONENT_FACT_CHANGED';END IF;
 SELECT coalesce(jsonb_agg(x.value ORDER BY x.value->>'component'),'[]'::jsonb) INTO v_reviews
 FROM jsonb_array_elements(v_run.policy->'component_reviews') x WHERE x.value->>'ticker'=p_ticker;
 FOR v_review IN SELECT value FROM jsonb_array_elements(v_reviews) LOOP
  v_refs:=v_review->'evidence_sha256';
  IF jsonb_typeof(v_refs) IS DISTINCT FROM 'array' OR jsonb_array_length(v_refs)=0 OR EXISTS(
   SELECT 1 FROM jsonb_array_elements_text(v_refs) e WHERE NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_fact_v2 f WHERE f.run_key=p_run AND f.evidence_sha256=e.value AND p_ticker=ANY(f.tickers)))
  THEN RAISE EXCEPTION 'REF01_REVIEW_WITHOUT_CASE_FACT';END IF;
  IF v_review->>'component'='SECURITY_KIND' THEN
   IF v_kind_review IS NOT NULL OR v_review->>'decision' NOT IN('COMMON','DEBT','PREFERRED','FUND') THEN RAISE EXCEPTION 'REF01_SECURITY_COMPONENT_REVIEW_INVALID';END IF;
   v_kind_review:=v_review;
  ELSIF v_review->>'component'='COMPANY_IDENTITY' THEN
   IF v_issuer_review IS NOT NULL OR nullif(v_review->>'company_key','') IS NULL OR v_review->>'decision' IS DISTINCT FROM 'SOURCE_SUPPORTED_ISSUER' THEN RAISE EXCEPTION 'REF01_ISSUER_COMPONENT_REVIEW_INVALID';END IF;
   v_issuer_review:=v_review;
  ELSE RAISE EXCEPTION 'REF01_UNRECOGNIZED_COMPONENT_REVIEW';END IF;
 END LOOP;
 FOREACH v_field IN ARRAY ARRAY['ticker','cik','type','primary_exchange','locale','market','currency_name','composite_figi','share_class_figi'] LOOP
  IF nullif(v_census->>v_field,'') IS NOT NULL AND nullif(v_overview->>v_field,'') IS NOT NULL AND v_census->>v_field IS DISTINCT FROM v_overview->>v_field THEN
   v_flags:=array_append(v_flags,'CENSUS_OVERVIEW_'||upper(v_field)||'_CONFLICT');v_same_fields:=false;
  END IF;
 END LOOP;
 v_type:=coalesce(nullif(v_overview->>'type',''),nullif(v_census->>'type',''));
 v_cik:=coalesce(nullif(v_overview->>'cik',''),nullif(v_census->>'cik',''));
 v_figi:=coalesce(nullif(v_overview->>'composite_figi',''),nullif(v_census->>'composite_figi',''));
 v_class:=coalesce(nullif(v_overview->>'share_class_figi',''),nullif(v_census->>'share_class_figi',''));
 v_exchange:=coalesce(nullif(v_overview->>'primary_exchange',''),nullif(v_census->>'primary_exchange',''));
 v_listing:=v_census->'active'='true'::jsonb AND v_census->>'locale'='us' AND v_census->>'market'='stocks'
  AND (v_run.policy->'eligible_exchanges') ? v_exchange;
 IF v_listing IS DISTINCT FROM true THEN v_flags:=array_append(v_flags,'HISTORICAL_EXCHANGE_ELIGIBILITY_REQUIRED');END IF;
 v_name_conflict:=coalesce(v_current->>'classification_conflict','false')::boolean;
 IF v_name_conflict AND v_kind_review IS NULL THEN v_flags:=array_append(v_flags,'SOURCE_NAME_TYPE_CONTRADICTION');END IF;
 IF v_kind_review IS NOT NULL THEN
  v_eligibility:=CASE WHEN v_kind_review->>'decision'='COMMON' AND v_listing IS TRUE THEN 'SOURCE_SUPPORTED_COMMON'
   WHEN v_kind_review->>'decision' IN('DEBT','PREFERRED','FUND') THEN 'SOURCE_SUPPORTED_EXCLUSION' ELSE 'REVIEW_REQUIRED' END;
 ELSIF v_listing IS TRUE AND v_same_fields AND NOT v_name_conflict AND (v_run.policy->'eligible_common_types') ? v_type THEN v_eligibility:='SOURCE_SUPPORTED_COMMON';
 ELSIF v_same_fields AND NOT v_name_conflict AND (v_run.policy->'unambiguous_excluded_types') ? v_type THEN v_eligibility:='SOURCE_SUPPORTED_EXCLUSION';
 ELSE v_eligibility:='REVIEW_REQUIRED';END IF;
 IF v_eligibility='REVIEW_REQUIRED' THEN v_flags:=array_append(v_flags,'SECURITY_CLASSIFICATION_REQUIRED');END IF;
 v_case:=CASE WHEN v_issuer_review IS NOT NULL THEN v_issuer_review->>'company_key' WHEN v_cik~'^[0-9]{10}$' THEN 'CIK:'||v_cik ELSE 'UNRESOLVED:'||coalesce(v_class,v_figi,p_ticker||':'||v_input.candidate_record_sha256) END;
 v_issuer_state:=CASE WHEN v_issuer_review IS NOT NULL THEN 'SOURCE_SUPPORTED_ISSUER' WHEN v_same_fields AND v_cik~'^[0-9]{10}$' THEN 'SOURCE_SUPPORTED_ISSUER' ELSE 'REVIEW_REQUIRED' END;
 IF v_issuer_state='REVIEW_REQUIRED' THEN v_flags:=array_append(v_flags,'CANONICAL_ISSUER_RELATIONSHIP_REQUIRED');END IF;
 v_security_state:=CASE WHEN NOT v_same_fields OR v_listing IS DISTINCT FROM true THEN 'REVIEW_REQUIRED' WHEN v_figi IS NOT NULL AND v_class IS NOT NULL THEN 'DATED_NATIVE_IDENTIFIERS' ELSE 'DATED_NATIVE_TICKER_ONLY' END;
 IF v_security_state='DATED_NATIVE_TICKER_ONLY' THEN v_flags:=array_append(v_flags,'ADDITIONAL_SECURITY_IDENTIFIER_REVIEW');END IF;
 v_cap:=population_top3000_202608_202609_v1.ref01_resolution_number_v2(v_overview->'market_cap');
 v_equivalent:=population_top3000_202608_202609_v1.ref01_resolution_number_v2(v_overview->'weighted_shares_outstanding');
 v_class_shares:=population_top3000_202608_202609_v1.ref01_resolution_number_v2(v_overview->'share_class_shares_outstanding');
 SELECT count(*)::integer,CASE WHEN count(*)=1 THEN min(b.provider_close) END,
  coalesce(jsonb_agg(jsonb_build_object('source_day_id',s.source_day_id,'source_id',s.source_id,'source_sha256',s.source_sha256,'source_row',b.source_row,'source_ticker',b.source_ticker,
   'provider_close',b.provider_close,'provider_ny_date',b.provider_ny_date,'missing_mask',b.missing_mask,'invalid_mask',b.invalid_mask,'structure_mask',b.structure_mask,'raw_record_sha256',encode(b.raw_record_sha256,'hex')) ORDER BY b.source_row),'[]'::jsonb)
 INTO v_price_rows,v_close,v_price
 FROM population_top3000_202608_202609_v1.d02_daily_source_v2 s JOIN population_top3000_202608_202609_v1.d02_daily_bar_v2 b ON b.source_day_id=s.source_day_id
 WHERE s.session_date=v_run.rank_date AND s.source_sha256=v_run.policy->>'rank_price_source_sha256' AND s.normalized_at IS NOT NULL AND s.source_verified_at IS NOT NULL
 AND b.source_ticker=p_ticker;
 v_price_agrees:=v_price_rows=1 AND v_close>0 AND v_cap>0 AND v_equivalent>0
 AND abs(v_cap-v_equivalent*v_close)<=greatest(0.005,abs(v_cap)*0.000000000001)
 AND v_price->0->>'provider_ny_date'='2026-08-03'
 AND (((v_price->0->>'missing_mask')::integer|(v_price->0->>'invalid_mask')::integer)&35)=0
 AND (v_price->0->>'structure_mask')::integer=0;
 IF v_eligibility='SOURCE_SUPPORTED_COMMON' THEN
  IF v_cap IS NULL OR v_cap<=0 THEN v_flags:=array_append(v_flags,'MISSING_DATED_COMPANY_CAP');END IF;
  IF v_equivalent IS NULL OR v_equivalent<=0 THEN v_flags:=array_append(v_flags,'MISSING_COMPANY_EQUIVALENT_SHARES');END IF;
  IF coalesce(v_overview->>'currency_name',v_census->>'currency_name') IS DISTINCT FROM 'usd' THEN v_flags:=array_append(v_flags,'USD_VALUATION_OR_FX_REQUIRED');END IF;
  IF v_price_rows<>1 OR v_close IS NULL OR v_close<=0 THEN v_flags:=array_append(v_flags,'RANK_CLOSE_UNAVAILABLE_OR_AMBIGUOUS');
  ELSIF v_price_agrees IS DISTINCT FROM true THEN v_flags:=array_append(v_flags,'RANK_PRICE_OR_UNIT_CONFLICT');END IF;
  IF v_class_shares IS DISTINCT FROM v_equivalent THEN v_flags:=array_append(v_flags,'CLASS_EQUIVALENT_SHARE_DIFFERENCE_REVIEW');END IF;
 END IF;
 RETURN jsonb_build_object('ticker',p_ticker,'input_ordinal',v_input.input_ordinal,'rank_date',v_run.rank_date,'case_key',v_case,
 'eligibility_state',v_eligibility,'company_identity_state',v_issuer_state,'security_identity_state',v_security_state,
 'native_type',v_type,'native_cik',v_cik,'composite_figi',v_figi,'share_class_figi',v_class,'primary_exchange',v_exchange,
 'native_name',coalesce(v_overview->>'name',v_census->>'name'),'historical_active',v_census->'active','market',v_census->>'market','locale',v_census->>'locale',
 'currency_name',coalesce(v_overview->>'currency_name',v_census->>'currency_name'),'list_date_source_text',v_overview->>'list_date',
 'vendor_company_cap_usd',v_cap,'vendor_equivalent_shares',v_equivalent,'vendor_class_shares',v_class_shares,
 'vendor_field_json',jsonb_build_object('market_cap',v_overview->'market_cap','weighted_shares_outstanding',v_overview->'weighted_shares_outstanding','share_class_shares_outstanding',v_overview->'share_class_shares_outstanding'),
 'rank_price_rows',v_price,'rank_price_arithmetic_consistent',v_price_agrees IS TRUE,'price_comparison_tolerance',jsonb_build_object('absolute_usd',0.005,'relative',0.000000000001),
 'diagnostic_codes',to_jsonb(v_flags),'component_source_facts',v_facts,'reviewed_components',v_reviews,
 'census_job_id',v_input.census_job_id,'census_ordinal',v_input.census_ordinal,'census_record_sha256',v_cp,
 'overview_job_id',v_input.overview_job_id,'overview_record_sha256',v_op,'candidate_record_sha256',v_input.candidate_record_sha256,
 'prior_group_key',v_current->>'group_key','original_historical_receipt_proven',false,'actual_rank_date_shares_proven',false,
 'whole_period_listing_interval_proven',false,'source_publication_time_independently_verified',false,'company_membership_frozen',false);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_normalize_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_tid uuid;v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_r population_top3000_202608_202609_v1.ref01_resolution_run_v2%rowtype;
 v_i record;v_projection jsonb;v_hash text;v_last integer;v_hi integer;v_batch integer;v_n integer:=0;v_fresh bigint:=0;v_inserted bigint;v_output text;v_case_count integer;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 v_r:=population_top3000_202608_202609_v1.ref01_resolution_context_v2(v_tid,'NORMALIZE');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_stage_v2 s WHERE s.run_key=v_r.run_key AND s.stage='ACQUIRE' AND s.units=v_r.expected_sources AND s.input_versions=v_t.input_versions) THEN RAISE EXCEPTION 'REF01_COMPLETE_SOURCE_STAGE_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2();
 v_last:=coalesce((v_t.checkpoint->>'last_ordinal')::integer,0);v_batch:=coalesce((v_t.checkpoint->>'batch_number')::integer,0)+1;
 IF v_last<0 OR v_last>=v_r.expected_inputs THEN RAISE EXCEPTION 'REF01_SECURITY_CHECKPOINT_INVALID';END IF;
 FOR v_i IN SELECT i.* FROM population_top3000_202608_202609_v1.ref01_resolution_input_v2 i WHERE i.run_key=v_r.run_key AND i.input_ordinal>v_last ORDER BY i.input_ordinal LIMIT 200 LOOP
  IF v_i.input_ordinal<>v_last+v_n+1 THEN RAISE EXCEPTION 'REF01_SECURITY_INPUT_GAP';END IF;
  v_projection:=population_top3000_202608_202609_v1.ref01_resolution_security_projection_v2(v_r.run_key,v_i.ticker);
  v_hash:=encode(sha256(convert_to(v_projection::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.ref01_security_decision_v2(run_key,ticker,input_ordinal,case_key,eligibility_state,company_identity_state,security_identity_state,projection,output_sha256)
  VALUES(v_r.run_key,v_i.ticker,v_i.input_ordinal,v_projection->>'case_key',v_projection->>'eligibility_state',v_projection->>'company_identity_state',v_projection->>'security_identity_state',v_projection,v_hash) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_inserted=ROW_COUNT;v_fresh:=v_fresh+v_inserted;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s WHERE s.run_key=v_r.run_key AND s.ticker=v_i.ticker AND s.input_ordinal=v_i.input_ordinal AND s.case_key=v_projection->>'case_key'
   AND s.eligibility_state=v_projection->>'eligibility_state' AND s.company_identity_state=v_projection->>'company_identity_state' AND s.security_identity_state=v_projection->>'security_identity_state'
   AND s.projection=v_projection AND s.output_sha256=v_hash) THEN RAISE EXCEPTION 'REF01_SECURITY_REPLAY_CONFLICT';END IF;
  v_n:=v_n+1;v_hi:=v_i.input_ordinal;
 END LOOP;
 IF v_n=0 THEN RAISE EXCEPTION 'REF01_NO_SECURITY_PROGRESS';END IF;
 SELECT encode(sha256(convert_to(string_agg(s.input_ordinal||':'||s.ticker||':'||s.output_sha256,E'\n' ORDER BY s.input_ordinal),'UTF8')),'hex') INTO v_output
 FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s WHERE s.run_key=v_r.run_key AND s.input_ordinal BETWEEN v_last+1 AND v_hi;
 IF v_hi=v_r.expected_inputs THEN
  WITH grouped AS MATERIALIZED(
   SELECT s.case_key,array_agg(s.ticker ORDER BY s.ticker) tickers,
    encode(sha256(convert_to(string_agg(s.ticker||':'||s.output_sha256,E'\n' ORDER BY s.ticker),'UTF8')),'hex') h
   FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s
   WHERE s.run_key=v_r.run_key AND s.eligibility_state<>'SOURCE_SUPPORTED_EXCLUSION' GROUP BY s.case_key)
  INSERT INTO population_top3000_202608_202609_v1.ref01_company_work_v2(run_key,case_key,case_ordinal,tickers,security_manifest_sha256)
  SELECT v_r.run_key,g.case_key,row_number() OVER(ORDER BY g.case_key)::integer,g.tickers,g.h FROM grouped g ON CONFLICT DO NOTHING;
  WITH expected AS MATERIALIZED(
   SELECT s.case_key,array_agg(s.ticker ORDER BY s.ticker) tickers,encode(sha256(convert_to(string_agg(s.ticker||':'||s.output_sha256,E'\n' ORDER BY s.ticker),'UTF8')),'hex') h
   FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s WHERE s.run_key=v_r.run_key AND s.eligibility_state<>'SOURCE_SUPPORTED_EXCLUSION' GROUP BY s.case_key),
  numbered AS(SELECT e.*,row_number() OVER(ORDER BY e.case_key)::integer ord FROM expected e)
  SELECT count(*) FILTER(WHERE e.case_key IS NULL OR w.case_key IS NULL OR e.tickers IS DISTINCT FROM w.tickers OR e.h IS DISTINCT FROM w.security_manifest_sha256 OR e.ord IS DISTINCT FROM w.case_ordinal)::integer INTO v_case_count
  FROM numbered e FULL JOIN(SELECT * FROM population_top3000_202608_202609_v1.ref01_company_work_v2 q WHERE q.run_key=v_r.run_key) w USING(case_key);
  IF v_case_count<>0 THEN RAISE EXCEPTION 'REF01_COMPANY_WORKSET_REPLAY_CONFLICT';END IF;
 END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2();
 RETURN population_top3000_202608_202609_v1.ref01_resolution_batch_receipt_v2(p_attempt,v_tid,v_r.run_key,'NORMALIZE',v_batch,v_last+1,v_hi,v_n,v_output,v_fresh,v_hi=v_r.expected_inputs);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_company_projection_v2(p_run text,p_case text)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path='pg_catalog' AS $f$
DECLARE v_r population_top3000_202608_202609_v1.ref01_resolution_run_v2%rowtype;
 v_work population_top3000_202608_202609_v1.ref01_company_work_v2%rowtype;
 v_lines jsonb;v_line jsonb;v_codes text[]:=ARRAY[]::text[];v_diagnostics text[]:=ARRAY[]::text[];v_code text;
 v_count integer;v_eligible integer;v_issuer_bad integer;v_adr integer;v_cap numeric;v_state text;v_company text;v_input_hash text;v_contract jsonb;
BEGIN
 SELECT * INTO v_r FROM population_top3000_202608_202609_v1.ref01_resolution_run_v2 r WHERE r.run_key=p_run;
 SELECT * INTO v_work FROM population_top3000_202608_202609_v1.ref01_company_work_v2 w WHERE w.run_key=p_run AND w.case_key=p_case;
 IF NOT FOUND THEN RAISE EXCEPTION 'REF01_COMPANY_WORKSET_REQUIRED';END IF;
 SELECT count(*)::integer,count(*) FILTER(WHERE s.eligibility_state='SOURCE_SUPPORTED_COMMON')::integer,
  count(*) FILTER(WHERE s.company_identity_state<>'SOURCE_SUPPORTED_ISSUER')::integer,
  count(*) FILTER(WHERE s.projection->>'native_type'='ADRC')::integer,
  jsonb_agg(s.projection ORDER BY s.ticker),encode(sha256(convert_to(string_agg(s.ticker||':'||s.output_sha256,E'\n' ORDER BY s.ticker),'UTF8')),'hex')
 INTO v_count,v_eligible,v_issuer_bad,v_adr,v_lines,v_input_hash
 FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s
 WHERE s.run_key=p_run AND s.case_key=p_case AND s.eligibility_state<>'SOURCE_SUPPORTED_EXCLUSION';
 IF v_count<>cardinality(v_work.tickers) OR v_input_hash IS DISTINCT FROM v_work.security_manifest_sha256 OR EXISTS(
  SELECT 1 FROM population_top3000_202608_202609_v1.ref01_security_decision_v2 s WHERE s.run_key=p_run AND s.case_key=p_case
   AND s.output_sha256 IS DISTINCT FROM encode(sha256(convert_to(s.projection::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'REF01_COMPANY_INPUT_CHANGED';END IF;
 IF v_eligible<>v_count THEN v_codes:=array_append(v_codes,'SECURITY_CLASSIFICATION_REQUIRED');END IF;
 IF v_issuer_bad<>0 OR p_case LIKE 'UNRESOLVED:%' THEN v_codes:=array_append(v_codes,'CANONICAL_ISSUER_RELATIONSHIP_REQUIRED');END IF;
 IF v_adr>0 THEN v_codes:=array_append(v_codes,'ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION');END IF;
 IF v_count>1 THEN v_codes:=array_append(v_codes,'MULTICLASS_OR_SHARED_CIK_RECONCILIATION');END IF;
 FOR v_line IN SELECT value FROM jsonb_array_elements(v_lines) LOOP
  FOR v_code IN SELECT value FROM jsonb_array_elements_text(v_line->'diagnostic_codes') LOOP
   IF v_code IN('ADDITIONAL_SECURITY_IDENTIFIER_REVIEW','CLASS_EQUIVALENT_SHARE_DIFFERENCE_REVIEW') THEN v_diagnostics:=array_append(v_diagnostics,v_code);
   ELSE v_codes:=array_append(v_codes,v_code);END IF;
  END LOOP;
 END LOOP;
 v_contract:=v_r.policy->'dated_vendor_contract';
 IF v_contract->>'state' IS DISTINCT FROM 'SOURCE_QUALIFIED_RECONSTRUCTION'
 OR v_contract->>'company_cap_semantics' IS DISTINCT FROM 'WHOLE_COMPANY_EQUIVALENT_SHARES_TIMES_MOST_RECENT_CLOSE'
 OR v_contract->>'historical_security_date' IS DISTINCT FROM '2026-08-03'
 OR v_contract->>'sec_fields_may_follow_later_report_period' IS DISTINCT FROM 'true'
 OR v_contract->>'strict_historical_receipt_inferred' IS DISTINCT FROM 'false'
 OR jsonb_typeof(v_contract->'source_refs') IS DISTINCT FROM 'array' OR jsonb_array_length(v_contract->'source_refs')=0
 THEN v_codes:=array_append(v_codes,'DATED_VENDOR_METHODOLOGY_EVIDENCE_REQUIRED');END IF;
 SELECT coalesce(array_agg(DISTINCT c ORDER BY c),ARRAY[]::text[]) INTO v_codes FROM unnest(v_codes) c;
 SELECT coalesce(array_agg(DISTINCT c ORDER BY c),ARRAY[]::text[]) INTO v_diagnostics FROM unnest(v_diagnostics) c;
 -- A single observed US line does not assert that no foreign/nonlisted class exists.
 -- The documented whole-company equivalent-share field is retained once; never MAX or SUM across lines.
 IF v_count=1 THEN v_cap:=population_top3000_202608_202609_v1.ref01_resolution_number_v2(v_lines->0->'vendor_company_cap_usd');END IF;
 IF v_cap IS NOT NULL AND v_cap<=0 THEN v_cap:=NULL;END IF;
 v_state:=CASE WHEN cardinality(v_codes)=0 AND v_cap IS NOT NULL THEN 'SOURCE_QUALIFIED_DATED_VENDOR' ELSE 'TARGETED_EVIDENCE_REQUIRED' END;
 v_company:=CASE WHEN v_issuer_bad=0 AND p_case NOT LIKE 'UNRESOLVED:%' THEN p_case ELSE NULL END;
 RETURN jsonb_build_object('run_key',p_run,'decision_revision',v_r.policy_sha256,'case_key',p_case,'case_ordinal',v_work.case_ordinal,
 'company_key',v_company,'decision_state',v_state,'member_tickers',to_jsonb(v_work.tickers),'observed_nonexcluded_lines',v_count,
 'source_company_cap_usd',v_cap,'verified_rank_cap_usd',NULL,'valuation_method',CASE WHEN v_state='SOURCE_QUALIFIED_DATED_VENDOR' THEN 'DATED_VENDOR_WHOLE_COMPANY_EQUIVALENT_SHARE_RECONSTRUCTION' ELSE 'UNRESOLVED_NO_ARBITRARY_CAP_AGGREGATION' END,
 'rank_date','2026-08-03','rank_instant','2026-08-03T20:00:00Z','currency','USD','security_manifest_sha256',v_input_hash,
 'line_evidence',v_lines,'dated_vendor_contract',v_contract,'targeted_requirement_codes',to_jsonb(v_codes),'risk_diagnostic_codes',to_jsonb(v_diagnostics),
 'unresolved_global_gates',jsonb_build_array('INDEPENDENT_PROVIDER_RISK_SAMPLE','ACTUAL_RANK_DATE_SHARE_BASIS_AND_CUTOFF_SENSITIVITY','FULL_COMPANY_CENSUS_AND_CLASS_COVERAGE','TARGET_WINDOW_SECURITY_INTERVALS'),
 'actual_rank_date_shares_proven',false,'independent_provider_accuracy_verified',false,'original_historical_receipt_proven',false,
 'source_publication_time_independently_verified',false,'company_membership_frozen',false,
 'source_date_caution','Historical date applies to security details; SEC-derived fields may reflect a report period filed later. The dated vendor reconstruction is not a strict receipt or individually certified rank-date share count.',
 'no_company_cap_max_or_repeated_cap_sum',true,'automatic_provider_request',false);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_derive_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_tid uuid;v_t population_top3000_202608_202609_v1.pipeline_task%rowtype;
 v_r population_top3000_202608_202609_v1.ref01_resolution_run_v2%rowtype;v_work record;v_content jsonb;v_hash text;v_requirement jsonb;v_code text;
 v_last integer;v_hi integer;v_batch integer;v_total integer;v_n integer:=0;v_fresh bigint:=0;v_inserted bigint;v_output text;v_priority integer;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_t FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 v_r:=population_top3000_202608_202609_v1.ref01_resolution_context_v2(v_tid,'DERIVE');
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_stage_v2 s WHERE s.run_key=v_r.run_key AND s.stage='NORMALIZE' AND s.units=v_r.expected_inputs AND s.input_versions=v_t.input_versions) THEN RAISE EXCEPTION 'REF01_COMPLETE_SECURITY_STAGE_REQUIRED';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2();
 SELECT count(*)::integer INTO v_total FROM population_top3000_202608_202609_v1.ref01_company_work_v2 w WHERE w.run_key=v_r.run_key;
 v_last:=coalesce((v_t.checkpoint->>'last_ordinal')::integer,0);v_batch:=coalesce((v_t.checkpoint->>'batch_number')::integer,0)+1;
 IF v_total NOT BETWEEN 1 AND 13044 OR v_last<0 OR v_last>=v_total THEN RAISE EXCEPTION 'REF01_COMPANY_CHECKPOINT_INVALID';END IF;
 FOR v_work IN SELECT w.* FROM population_top3000_202608_202609_v1.ref01_company_work_v2 w WHERE w.run_key=v_r.run_key AND w.case_ordinal>v_last ORDER BY w.case_ordinal LIMIT 100 LOOP
  IF v_work.case_ordinal<>v_last+v_n+1 THEN RAISE EXCEPTION 'REF01_COMPANY_INPUT_GAP';END IF;
  v_content:=population_top3000_202608_202609_v1.ref01_resolution_company_projection_v2(v_r.run_key,v_work.case_key);v_hash:=encode(sha256(convert_to(v_content::text,'UTF8')),'hex');
  INSERT INTO population_top3000_202608_202609_v1.ref01_company_decision_v2(run_key,decision_revision,case_key,company_key,decision_state,source_company_cap_usd,verified_rank_cap_usd,content,output_sha256,first_attempt_id)
  VALUES(v_r.run_key,v_r.policy_sha256,v_work.case_key,v_content->>'company_key',v_content->>'decision_state',population_top3000_202608_202609_v1.ref01_resolution_number_v2(v_content->'source_company_cap_usd'),NULL,v_content,v_hash,p_attempt) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_inserted=ROW_COUNT;v_fresh:=v_fresh+v_inserted;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_company_decision_v2 d WHERE d.run_key=v_r.run_key AND d.decision_revision=v_r.policy_sha256 AND d.case_key=v_work.case_key
   AND d.company_key IS NOT DISTINCT FROM v_content->>'company_key' AND d.decision_state=v_content->>'decision_state'
   AND d.source_company_cap_usd IS NOT DISTINCT FROM population_top3000_202608_202609_v1.ref01_resolution_number_v2(v_content->'source_company_cap_usd')
   AND d.verified_rank_cap_usd IS NULL AND d.content=v_content AND d.output_sha256=v_hash) THEN RAISE EXCEPTION 'REF01_COMPANY_DECISION_REPLAY_CONFLICT';END IF;
  FOR v_code IN SELECT value FROM jsonb_array_elements_text(v_content->'targeted_requirement_codes') LOOP
   v_priority:=CASE WHEN v_code IN('MISSING_DATED_COMPANY_CAP','CANONICAL_ISSUER_RELATIONSHIP_REQUIRED','MULTICLASS_OR_SHARED_CIK_RECONCILIATION','ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION','SOURCE_NAME_TYPE_CONTRADICTION') THEN 95 ELSE 80 END;
   v_requirement:=jsonb_build_object('case_key',v_work.case_key,'tickers',to_jsonb(v_work.tickers),'code',v_code,'decision_sha256',v_hash,
    'precise_question',CASE v_code
     WHEN 'MISSING_DATED_COMPANY_CAP' THEN 'Obtain a supported 2026-08-03 whole-company common-equity cap or an evidenced bound. Missing is not zero and may cross the cutoff.'
     WHEN 'CANONICAL_ISSUER_RELATIONSHIP_REQUIRED' THEN 'Resolve this exact issuer/security relationship using dated stable identifiers and contrary facts; do not merge by ticker/name alone.'
     WHEN 'MULTICLASS_OR_SHARED_CIK_RECONCILIATION' THEN 'Resolve whether the lines belong to one economic company, their distinct economic classes and any non-US/nonlisted components; use a justified company field or economic-class valuation, never a maximum or repeated sum.'
     WHEN 'ADR_GLOBAL_CAP_AND_RATIO_RECONCILIATION' THEN 'Verify ADR ratio, ordinary/global common-equity coverage, class identity and valuation units; prevent ADR/underlying double counting.'
     WHEN 'RANK_PRICE_OR_UNIT_CONFLICT' THEN 'Reconcile the vendor cap/equivalent-share/3-Aug close disagreement and effective source state before ranking.'
     WHEN 'SECURITY_CLASSIFICATION_REQUIRED' THEN 'Resolve this exact historical security as eligible common-equity/ADR or an evidenced excluded type; retain common REITs even if the vendor labelled FUND.'
     WHEN 'DATED_VENDOR_METHODOLOGY_EVIDENCE_REQUIRED' THEN 'Retain and review the dated provider methodology once; record its report-period limitations and independent risk-check policy.'
     ELSE 'Resolve the named source-field contradiction or missing component using the smallest precise dated evidence request.' END,
    'next_action_kind',CASE WHEN v_code='MISSING_DATED_COMPANY_CAP' AND cardinality(v_work.tickers)=1 THEN 'EXACT_DATED_OVERVIEW_OR_TARGETED_RECONSTRUCTION' WHEN v_code='DATED_VENDOR_METHODOLOGY_EVIDENCE_REQUIRED' THEN 'SHARED_METHODOLOGY_REVIEW' ELSE 'TARGETED_CASE_RECONCILIATION' END,
    'blanket_sec_submissions_disallowed',true,'automatic_provider_request',false,'unbounded_missing_cap_can_cross_cutoff',true);
   INSERT INTO population_top3000_202608_202609_v1.ref01_evidence_requirement_v2(run_key,decision_revision,case_key,requirement_code,priority,requirement,requirement_sha256)
   VALUES(v_r.run_key,v_r.policy_sha256,v_work.case_key,v_code,v_priority,v_requirement,encode(sha256(convert_to(v_requirement::text,'UTF8')),'hex')) ON CONFLICT DO NOTHING;
   IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_evidence_requirement_v2 q WHERE q.run_key=v_r.run_key AND q.decision_revision=v_r.policy_sha256 AND q.case_key=v_work.case_key AND q.requirement_code=v_code AND q.priority=v_priority AND q.requirement=v_requirement AND q.requirement_sha256=encode(sha256(convert_to(v_requirement::text,'UTF8')),'hex')) THEN RAISE EXCEPTION 'REF01_REQUIREMENT_REPLAY_CONFLICT';END IF;
  END LOOP;
  IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_evidence_requirement_v2 q WHERE q.run_key=v_r.run_key AND q.decision_revision=v_r.policy_sha256 AND q.case_key=v_work.case_key)<>jsonb_array_length(v_content->'targeted_requirement_codes') THEN RAISE EXCEPTION 'REF01_REQUIREMENT_SET_MISMATCH';END IF;
  v_n:=v_n+1;v_hi:=v_work.case_ordinal;
 END LOOP;
 IF v_n=0 THEN RAISE EXCEPTION 'REF01_NO_COMPANY_PROGRESS';END IF;
 SELECT encode(sha256(convert_to(string_agg(w.case_ordinal||':'||d.case_key||':'||d.output_sha256,E'\n' ORDER BY w.case_ordinal),'UTF8')),'hex') INTO v_output
 FROM population_top3000_202608_202609_v1.ref01_company_work_v2 w JOIN population_top3000_202608_202609_v1.ref01_company_decision_v2 d USING(run_key,case_key)
 WHERE w.run_key=v_r.run_key AND d.decision_revision=v_r.policy_sha256 AND w.case_ordinal BETWEEN v_last+1 AND v_hi;
 PERFORM population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2();
 RETURN population_top3000_202608_202609_v1.ref01_resolution_batch_receipt_v2(p_attempt,v_tid,v_r.run_key,'DERIVE',v_batch,v_last+1,v_hi,v_n,v_output,v_fresh,v_hi=v_total);
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_validate_bridge_v2(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_tid uuid;v_run population_top3000_202608_202609_v1.ref01_resolution_run_v2%rowtype;v_cases integer;v_done integer;v_qualified integer;v_targeted integer;v_requirements integer;v_stages jsonb;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 v_run:=population_top3000_202608_202609_v1.ref01_resolution_context_v2(v_tid,'VALIDATE');
 SELECT count(*)::integer INTO v_cases FROM population_top3000_202608_202609_v1.ref01_company_work_v2 w WHERE w.run_key=v_run.run_key;
 SELECT count(*)::integer,count(*) FILTER(WHERE d.decision_state='SOURCE_QUALIFIED_DATED_VENDOR')::integer,count(*) FILTER(WHERE d.decision_state='TARGETED_EVIDENCE_REQUIRED')::integer
 INTO v_done,v_qualified,v_targeted FROM population_top3000_202608_202609_v1.ref01_company_decision_v2 d WHERE d.run_key=v_run.run_key AND d.decision_revision=v_run.policy_sha256;
 SELECT count(*)::integer INTO v_requirements FROM population_top3000_202608_202609_v1.ref01_evidence_requirement_v2 q WHERE q.run_key=v_run.run_key AND q.decision_revision=v_run.policy_sha256;
 SELECT jsonb_object_agg(s.stage,jsonb_build_object('units',s.units,'batch_count',s.batch_count,'output_sha256',s.output_sha256)) INTO v_stages
 FROM population_top3000_202608_202609_v1.ref01_resolution_stage_v2 s WHERE s.run_key=v_run.run_key AND s.input_versions=v_run.input_versions;
 IF v_cases<>v_done OR v_cases<1 OR v_stages#>>'{ACQUIRE,units}' IS DISTINCT FROM v_run.expected_sources::text
 OR v_stages#>>'{NORMALIZE,units}' IS DISTINCT FROM v_run.expected_inputs::text OR v_stages#>>'{DERIVE,units}' IS DISTINCT FROM v_cases::text THEN RAISE EXCEPTION 'REF01_ALL_RECONCILIATION_PRODUCER_STAGES_REQUIRED';END IF;
 RETURN jsonb_build_object('state','BLOCKED','blocker_code','REF01_INDEPENDENT_COMPANY_CUTOFF_AND_LISTING_RECONCILIATION_REQUIRED',
  'resumption_condition',jsonb_build_object('run_key',v_run.run_key,'decision_revision',v_run.policy_sha256,
  'retained_source_processing_complete',true,'source_jobs',v_run.expected_sources,'historical_census_lines',v_run.expected_inputs,
  'company_case_units',v_cases,'source_qualified_vendor_reconstructions',v_qualified,'targeted_cases',v_targeted,'precise_requirement_rows',v_requirements,
  'producer_stage_manifests',v_stages,'quality_state','RECONCILIATION_INPUTS_READY_NOT_COHORT_CERTIFIED',
  'next_automatic_work_input','ref01_evidence_requirement_v2 plus the shared independent risk/cutoff policy',
  'required_action','Continue targeted evidence and independent risk/cutoff/listing-life reconciliation using these committed case decisions; bind a reviewed actual validator when its evidence is ready. Do not restart the retained source census or dispatch blanket SEC submissions.',
  'provider_accuracy_verified',false,'actual_rank_date_shares_verified',false,'complete_company_population_verified',false,
  'original_historical_receipt_verified',false,'database_publication_allowed',false,'mission_complete',false,'automatic_retry',false));
END $f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_route_v2(p_attempt uuid,p_token text,p_stage text)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_tid uuid;v_task population_top3000_202608_202609_v1.pipeline_task%rowtype;v_old population_top3000_202608_202609_v1.ref01_sustained_legacy_adapter_v2%rowtype;v_schema text;v_name text;v_result jsonb;
BEGIN
 v_tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 SELECT * INTO v_task FROM population_top3000_202608_202609_v1.pipeline_task t WHERE t.task_id=v_tid;
 IF v_task.family_id IS DISTINCT FROM 'REF01' OR v_task.stage IS DISTINCT FROM p_stage OR p_stage NOT IN('ACQUIRE','NORMALIZE','DERIVE','VALIDATE') THEN RAISE EXCEPTION 'REF01_ROUTER_STAGE_MISMATCH';END IF;
 IF v_task.payload->>'kind'='REF01_SUSTAINED_RECONCILIATION_V2' THEN
  RETURN CASE p_stage
   WHEN 'ACQUIRE' THEN population_top3000_202608_202609_v1.ref01_resolution_acquire_v2(p_attempt,p_token)
   WHEN 'NORMALIZE' THEN population_top3000_202608_202609_v1.ref01_resolution_normalize_v2(p_attempt,p_token)
   WHEN 'DERIVE' THEN population_top3000_202608_202609_v1.ref01_resolution_derive_v2(p_attempt,p_token)
   WHEN 'VALIDATE' THEN population_top3000_202608_202609_v1.ref01_resolution_validate_bridge_v2(p_attempt,p_token) END;
 END IF;
 SELECT * INTO v_old FROM population_top3000_202608_202609_v1.ref01_sustained_legacy_adapter_v2 a WHERE a.stage=p_stage;
 IF NOT FOUND OR v_old.function_sha256 IS DISTINCT FROM encode(sha256(convert_to(pg_get_functiondef(v_old.function_oid),'UTF8')),'hex') THEN RAISE EXCEPTION 'REF01_PRESERVED_LEGACY_FUNCTION_CHANGED';END IF;
 SELECT n.nspname,p.proname INTO v_schema,v_name FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE p.oid=v_old.function_oid AND p.proargtypes='2950 25'::oidvector;
 IF v_schema IS DISTINCT FROM 'population_top3000_202608_202609_v1' OR v_name LIKE 'ref01_resolution_%' THEN RAISE EXCEPTION 'REF01_LEGACY_DELEGATION_INVALID';END IF;
 EXECUTE format('SELECT %I.%I($1,$2)',v_schema,v_name) INTO v_result USING p_attempt,p_token;
 RETURN v_result;
END $f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_route_acquire_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path='pg_catalog' AS $f$SELECT population_top3000_202608_202609_v1.ref01_resolution_route_v2(p_attempt,p_token,'ACQUIRE')$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_route_normalize_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path='pg_catalog' AS $f$SELECT population_top3000_202608_202609_v1.ref01_resolution_route_v2(p_attempt,p_token,'NORMALIZE')$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_route_derive_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path='pg_catalog' AS $f$SELECT population_top3000_202608_202609_v1.ref01_resolution_route_v2(p_attempt,p_token,'DERIVE')$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_route_validate_v2(p_attempt uuid,p_token text) RETURNS jsonb LANGUAGE sql SET search_path='pg_catalog' AS $f$SELECT population_top3000_202608_202609_v1.ref01_resolution_route_v2(p_attempt,p_token,'VALIDATE')$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_bundle_sha256_v2()
RETURNS text LANGUAGE sql STABLE SET search_path='pg_catalog' AS $f$
 SELECT encode(sha256(convert_to(string_agg(p.proname||':'||pg_get_function_identity_arguments(p.oid)||':'||encode(sha256(convert_to(pg_get_functiondef(p.oid),'UTF8')),'hex'),E'\n' ORDER BY p.proname,pg_get_function_identity_arguments(p.oid)),'UTF8')),'hex')
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
 WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname=ANY(ARRAY[
  'ref01_resolution_resource_guard_v2','ref01_resolution_number_v2','ref01_resolution_cursor_v2','ref01_resolution_context_v2',
  'ref01_resolution_batch_receipt_v2','ref01_resolution_acquire_v2','ref01_resolution_security_projection_v2','ref01_resolution_normalize_v2',
  'ref01_resolution_company_projection_v2','ref01_resolution_derive_v2','ref01_resolution_validate_bridge_v2','ref01_resolution_route_v2',
  'ref01_resolution_route_acquire_v2','ref01_resolution_route_normalize_v2','ref01_resolution_route_derive_v2','ref01_resolution_route_validate_v2','ref01_resolution_seed_v2'])
$f$;

-- Root-only immutable input snapshot. A fixture may use a disclosed subset of real candidate lines;
-- it still verifies all 14 exact census pages and only the actual overviews needed for those lines.
-- Live admission below permits only FULL_CENSUS with the original exact 13,044/5,684 manifests.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_resolution_seed_v2(p_run text,p_policy jsonb,p_fixture_tickers text[] DEFAULT NULL)
RETURNS jsonb LANGUAGE plpgsql SET search_path='pg_catalog' AS $f$
DECLARE v_candidates integer;v_overviews integer;v_pages integer;v_sources integer;v_groups integer;v_candidate_hash text;v_group_hash text;v_source_hash text;v_fact_hash text;v_policy_hash text;
 v_versions jsonb;v_refs jsonb;v_ref jsonb;v_doc population_top3000_202608_202609_v1.source_document%rowtype;v_fact record;v_source_snapshot jsonb;v_old record;v_purpose text;
BEGIN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control c JOIN population_top3000_202608_202609_v1.pipeline_control p ON p.singleton=c.singleton
  WHERE c.singleton AND c.owner_id='top3000-recovery-20261023-root-4b1314bcb5c2' AND c.owner_fence=2 AND c.stop_generation=0 AND c.enabled AND NOT c.stopped AND c.hold_reason IS NULL AND c.admitted_until>clock_timestamp()
  AND c.scope_sha256='8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
  AND p.expected_owner_id=c.owner_id AND p.expected_owner_fence=c.owner_fence AND p.expected_stop_generation=c.stop_generation AND p.scope_sha256=c.scope_sha256 AND p.shared_scheduler_verified AND p.deadline>clock_timestamp()) THEN RAISE EXCEPTION 'REF01_CURRENT_OWNER_ADMISSION_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM cron.job WHERE jobid=596 AND active) OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE status='LEASED')
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE released_at IS NULL) THEN RAISE EXCEPTION 'REF01_SEED_REQUIRES_PAUSED_DRAINED_QUEUE';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2();
 v_purpose:=CASE WHEN p_fixture_tickers IS NULL THEN 'FULL_CENSUS' ELSE 'ROLLBACK_FIXTURE' END;
 IF v_purpose='ROLLBACK_FIXTURE' AND (current_setting('ref01.rollback_fixture',true) IS DISTINCT FROM p_run OR cardinality(p_fixture_tickers) NOT BETWEEN 5 AND 50) THEN RAISE EXCEPTION 'REF01_EXPLICIT_ROLLBACK_FIXTURE_REQUIRED';END IF;
 IF jsonb_typeof(p_policy) IS DISTINCT FROM 'object' OR p_policy->>'policy_revision' IS DISTINCT FROM 'REF01_DATED_SOURCE_RECONCILIATION_V2'
 OR p_policy->>'scope_sha256' IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR p_policy->>'rank_date' IS DISTINCT FROM '2026-08-03' OR p_policy->>'rank_price_source_sha256' IS DISTINCT FROM '787163eb9f6ffb408898c8a133c6ca63c822577187793d25c94b436f639bb97b'
 OR jsonb_typeof(p_policy->'eligible_exchanges') IS DISTINCT FROM 'array' OR jsonb_array_length(p_policy->'eligible_exchanges')=0
 OR p_policy->'eligible_common_types' IS DISTINCT FROM '["CS","ADRC"]'::jsonb
 OR p_policy->'unambiguous_excluded_types' IS DISTINCT FROM '["ETF","ETN","ETS","ETV","PFD","RIGHT","UNIT","WARRANT"]'::jsonb
 OR jsonb_typeof(p_policy->'component_reviews') IS DISTINCT FROM 'array'
 OR p_policy->>'strict_historical_receipt_inferred' IS DISTINCT FROM 'false' THEN RAISE EXCEPTION 'REF01_REVIEWED_POLICY_REQUIRED';END IF;
 v_refs:=p_policy#>'{dated_vendor_contract,source_refs}';
 IF jsonb_typeof(v_refs) IS DISTINCT FROM 'array' OR jsonb_array_length(v_refs)<>2 THEN RAISE EXCEPTION 'REF01_TWO_RETAINED_METHOD_DOCUMENTS_REQUIRED';END IF;
 FOR v_ref IN SELECT value FROM jsonb_array_elements(v_refs) LOOP
  SELECT * INTO v_doc FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=v_ref->>'source_id';
  IF NOT FOUND OR v_doc.source_sha256 IS DISTINCT FROM v_ref->>'source_sha256' OR v_doc.content_text IS NULL OR v_doc.content_bytes NOT BETWEEN 1 AND 8388608
  OR v_doc.content_bytes IS DISTINCT FROM octet_length(v_doc.content_text)::bigint OR v_doc.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_doc.content_text,'UTF8')),'hex') THEN RAISE EXCEPTION 'REF01_METHODOLOGY_DOCUMENT_INTEGRITY';END IF;
 END LOOP;
 -- These are existing source facts. Retaining them does not silently accept the entire case.
 FOR v_fact IN SELECT DISTINCT e.source_id,e.source_sha256 FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1 e LOOP
  SELECT * INTO v_doc FROM population_top3000_202608_202609_v1.source_document d WHERE d.source_id=v_fact.source_id;
  IF NOT FOUND OR v_doc.source_sha256 IS DISTINCT FROM v_fact.source_sha256 OR v_doc.content_text IS NULL OR v_doc.content_bytes NOT BETWEEN 1 AND 8388608
  OR v_doc.content_bytes IS DISTINCT FROM octet_length(v_doc.content_text)::bigint OR v_doc.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(v_doc.content_text,'UTF8')),'hex') THEN RAISE EXCEPTION 'REF01_COMPONENT_SOURCE_INTEGRITY';END IF;
 END LOOP;
 SELECT count(*)::integer,encode(sha256(convert_to(string_agg(c.ticker||':'||c.input_sha256,E'\n' ORDER BY c.ticker),'UTF8')),'hex') INTO v_candidates,v_candidate_hash
 FROM population_top3000_202608_202609_v1.company_candidate_v1 c WHERE p_fixture_tickers IS NULL OR c.ticker=ANY(p_fixture_tickers);
 SELECT count(*)::integer,encode(sha256(convert_to(string_agg(g.group_key||':'||g.input_sha256,E'\n' ORDER BY g.group_key),'UTF8')),'hex') INTO v_groups,v_group_hash
 FROM population_top3000_202608_202609_v1.company_group_v1 g;
 IF v_groups<>5553 OR v_group_hash IS DISTINCT FROM '03045ee6945dc9aee6eed06819c06d662f37b3feff128566f5dab0688b7443b3' THEN RAISE EXCEPTION 'REF01_ORIGINAL_GROUP_MANIFEST_CHANGED';END IF;
 IF v_purpose='FULL_CENSUS' AND (v_candidates<>13044 OR v_candidate_hash IS DISTINCT FROM 'd5f7b305ffe150ce99fd86449c9187b3e3f783bfdcc63382de7b9ea5d7be37f5') THEN RAISE EXCEPTION 'REF01_ORIGINAL_CANDIDATE_MANIFEST_CHANGED';END IF;
 IF p_fixture_tickers IS NOT NULL AND v_candidates<>cardinality(p_fixture_tickers) THEN RAISE EXCEPTION 'REF01_FIXTURE_CANDIDATE_MISSING_OR_DUPLICATED';END IF;
 WITH wanted AS MATERIALIZED(
  SELECT DISTINCT c.overview_job_id job_id FROM population_top3000_202608_202609_v1.company_candidate_v1 c WHERE c.overview_job_id IS NOT NULL AND (p_fixture_tickers IS NULL OR c.ticker=ANY(p_fixture_tickers))
  UNION SELECT DISTINCT c.census_job_id FROM population_top3000_202608_202609_v1.company_candidate_v1 c
 ), sources AS MATERIALIZED(
  SELECT j.job_id,j.kind,j.snapshot_date,j.page_number,j.ticker,j.params,a.sha256,a.source_bytes,a.received_at,encode(sha256(convert_to(a.evidence::text,'UTF8')),'hex') evidence_sha256,
   CASE WHEN j.kind='CENSUS' THEN (SELECT count(*)::integer FROM population_top3000_202608_202609_v1.reference_row r WHERE r.page_job_id=j.job_id) ELSE 1 END expected_records
  FROM wanted w JOIN population_top3000_202608_202609_v1.acquisition_job j USING(job_id) JOIN population_top3000_202608_202609_v1.acquisition_raw a USING(job_id)
  WHERE j.status='COMPLETE' AND j.snapshot_date=DATE '2026-08-03' AND j.kind IN('CENSUS','OVERVIEW'))
 SELECT count(*)::integer,count(*) FILTER(WHERE kind='OVERVIEW')::integer,count(*) FILTER(WHERE kind='CENSUS')::integer,
  encode(sha256(convert_to(string_agg(job_id||':'||sha256||':'||source_bytes||':'||params::text||':'||evidence_sha256,E'\n' ORDER BY kind,coalesce(page_number,2147483647),job_id),'UTF8')),'hex'),
  jsonb_agg(to_jsonb(s) ORDER BY kind,coalesce(page_number,2147483647),job_id)
 INTO v_sources,v_overviews,v_pages,v_source_hash,v_source_snapshot FROM sources s;
 IF v_pages<>14 OR v_sources<>v_pages+v_overviews
 OR (SELECT sum((x.value->>'source_bytes')::bigint) FROM jsonb_array_elements(v_source_snapshot) x)>52428800
 OR (v_purpose='FULL_CENSUS' AND (v_sources<>5698 OR v_overviews<>5684)) THEN RAISE EXCEPTION 'REF01_COMPLETE_RETAINED_SOURCE_MANIFEST_REQUIRED';END IF;
 SELECT encode(sha256(convert_to(coalesce(string_agg(e.evidence_sha256||':'||e.source_id||':'||e.source_sha256,E'\n' ORDER BY e.evidence_sha256),''),'UTF8')),'hex') INTO v_fact_hash
 FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1 e;
 v_policy_hash:=encode(sha256(convert_to(p_policy::text,'UTF8')),'hex');
 v_versions:=jsonb_build_object('scope_sha256','8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd','resolution_revision','REF01_SUSTAINED_RECONCILIATION_V2',
  'resolution_bundle_sha256',population_top3000_202608_202609_v1.ref01_resolution_bundle_sha256_v2(),'candidate_manifest_sha256',v_candidate_hash,'group_manifest_sha256',v_group_hash,
  'source_manifest_sha256',v_source_hash,'fact_manifest_sha256',v_fact_hash,'policy_sha256',v_policy_hash,'purpose',v_purpose);
 INSERT INTO population_top3000_202608_202609_v1.ref01_resolution_run_v2(run_key,purpose,scope_sha256,rank_date,rank_instant,candidate_manifest_sha256,group_manifest_sha256,source_manifest_sha256,fact_manifest_sha256,policy,policy_sha256,input_versions,expected_inputs,expected_overviews,expected_census_pages,expected_sources,expected_groups,owner_fence,stop_generation)
 VALUES(p_run,v_purpose,v_versions->>'scope_sha256',DATE '2026-08-03',TIMESTAMPTZ '2026-08-03 20:00:00Z',v_candidate_hash,v_group_hash,v_source_hash,v_fact_hash,p_policy,v_policy_hash,v_versions,v_candidates,v_overviews,v_pages,v_sources,v_groups,2,0) ON CONFLICT DO NOTHING;
 SELECT * INTO v_old FROM population_top3000_202608_202609_v1.ref01_resolution_run_v2 r WHERE r.run_key=p_run;
 IF v_old.input_versions IS DISTINCT FROM v_versions OR v_old.policy IS DISTINCT FROM p_policy OR v_old.expected_inputs IS DISTINCT FROM v_candidates OR v_old.expected_sources IS DISTINCT FROM v_sources OR v_old.purpose IS DISTINCT FROM v_purpose THEN RAISE EXCEPTION 'REF01_RUN_SNAPSHOT_REPLAY_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ref01_resolution_source_v2(run_key,source_ordinal,source_job_id,source_kind,source_sha256,source_bytes,snapshot_date,page_number,ticker,expected_records,request_params,source_evidence_sha256,received_at)
 SELECT p_run,x.ordinality::integer,x.value->>'job_id',x.value->>'kind',x.value->>'sha256',(x.value->>'source_bytes')::bigint,(x.value->>'snapshot_date')::date,
  (x.value->>'page_number')::integer,x.value->>'ticker',(x.value->>'expected_records')::integer,x.value->'params',x.value->>'evidence_sha256',(x.value->>'received_at')::timestamptz
 FROM jsonb_array_elements(v_source_snapshot) WITH ORDINALITY x ON CONFLICT DO NOTHING;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v_source_snapshot) WITH ORDINALITY x LEFT JOIN population_top3000_202608_202609_v1.ref01_resolution_source_v2 s ON s.run_key=p_run AND s.source_job_id=x.value->>'job_id'
  WHERE s.source_job_id IS NULL OR s.source_ordinal IS DISTINCT FROM x.ordinality::integer OR s.source_kind IS DISTINCT FROM x.value->>'kind' OR s.source_sha256 IS DISTINCT FROM x.value->>'sha256'
  OR s.source_bytes IS DISTINCT FROM (x.value->>'source_bytes')::bigint OR s.snapshot_date IS DISTINCT FROM (x.value->>'snapshot_date')::date OR s.page_number IS DISTINCT FROM (x.value->>'page_number')::integer
  OR s.ticker IS DISTINCT FROM x.value->>'ticker' OR s.expected_records IS DISTINCT FROM (x.value->>'expected_records')::integer OR s.request_params IS DISTINCT FROM x.value->'params'
  OR s.source_evidence_sha256 IS DISTINCT FROM x.value->>'evidence_sha256' OR s.received_at IS DISTINCT FROM (x.value->>'received_at')::timestamptz)
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_resolution_source_v2 s WHERE s.run_key=p_run)<>v_sources THEN RAISE EXCEPTION 'REF01_SOURCE_SNAPSHOT_REPLAY_CONFLICT';END IF;
 WITH numbered AS MATERIALIZED(SELECT c.*,row_number() OVER(ORDER BY c.ticker)::integer ord,to_jsonb(c)-'created_at' rec FROM population_top3000_202608_202609_v1.company_candidate_v1 c WHERE p_fixture_tickers IS NULL OR c.ticker=ANY(p_fixture_tickers))
 INSERT INTO population_top3000_202608_202609_v1.ref01_resolution_input_v2(run_key,input_ordinal,ticker,census_job_id,census_ordinal,overview_job_id,candidate_record,candidate_record_sha256)
 SELECT p_run,n.ord,n.ticker,n.census_job_id,n.census_ordinal,n.overview_job_id,n.rec,encode(sha256(convert_to(n.rec::text,'UTF8')),'hex') FROM numbered n ON CONFLICT DO NOTHING;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_resolution_input_v2 i WHERE i.run_key=p_run)<>v_candidates OR EXISTS(
  SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_input_v2 i LEFT JOIN population_top3000_202608_202609_v1.company_candidate_v1 c ON c.ticker=i.ticker
  WHERE i.run_key=p_run AND (i.candidate_record IS DISTINCT FROM to_jsonb(c)-'created_at' OR i.census_job_id IS DISTINCT FROM c.census_job_id OR i.census_ordinal IS DISTINCT FROM c.census_ordinal OR i.overview_job_id IS DISTINCT FROM c.overview_job_id)) THEN RAISE EXCEPTION 'REF01_CANDIDATE_SNAPSHOT_REPLAY_CONFLICT';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ref01_resolution_fact_v2(run_key,evidence_sha256,source_id,source_sha256,tickers,component,record_json)
 SELECT p_run,e.evidence_sha256,e.source_id,e.source_sha256,e.tickers,e.component,e.record_json FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1 e ON CONFLICT DO NOTHING;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ref01_resolution_fact_v2 f WHERE f.run_key=p_run)<>(SELECT count(*) FROM population_top3000_202608_202609_v1.company_component_source_evidence_v1)
 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ref01_resolution_fact_v2 f LEFT JOIN population_top3000_202608_202609_v1.company_component_source_evidence_v1 e USING(evidence_sha256)
 WHERE f.run_key=p_run AND (f.record_json IS DISTINCT FROM e.record_json OR f.source_id IS DISTINCT FROM e.source_id OR f.source_sha256 IS DISTINCT FROM e.source_sha256 OR f.tickers IS DISTINCT FROM e.tickers OR f.component IS DISTINCT FROM e.component)) THEN RAISE EXCEPTION 'REF01_FACT_SNAPSHOT_REPLAY_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_resolution_resource_guard_v2();
 RETURN jsonb_build_object('run_key',p_run,'purpose',v_purpose,'input_versions',v_versions,'census_lines',v_candidates,'overview_sources',v_overviews,'census_sources',v_pages,'source_jobs',v_sources,'no_provider_request',true,'source_processing_only',true);
END $f$;

DO $permissions$
DECLARE v_rel record;v_fn record;
BEGIN
 FOR v_rel IN SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND c.relkind='r' AND c.relname=ANY(ARRAY[
  'ref01_resolution_run_v2','ref01_resolution_source_v2','ref01_resolution_input_v2','ref01_resolution_fact_v2','ref01_resolution_source_proof_v2','ref01_resolution_record_proof_v2','ref01_security_decision_v2','ref01_company_work_v2','ref01_company_decision_v2','ref01_evidence_requirement_v2','ref01_resolution_batch_v2','ref01_resolution_stage_v2','ref01_sustained_legacy_adapter_v2']) LOOP
  EXECUTE format('ALTER TABLE population_top3000_202608_202609_v1.%I ENABLE ROW LEVEL SECURITY',v_rel.relname);
  EXECUTE format('REVOKE ALL ON population_top3000_202608_202609_v1.%I FROM PUBLIC, anon, authenticated',v_rel.relname);
 END LOOP;
 FOR v_fn IN SELECT p.oid::regprocedure signature FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname LIKE 'ref01_resolution_%_v2' LOOP
  EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC, anon, authenticated',v_fn.signature);
  EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO service_role',v_fn.signature);
 END LOOP;
END $permissions$;

COMMIT;
