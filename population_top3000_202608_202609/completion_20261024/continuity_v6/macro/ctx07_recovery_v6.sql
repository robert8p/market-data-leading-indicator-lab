-- Scoped CTX07 source recovery. New rows only; all historical values and receipts preserved.
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx07_recovery_request_v6(
 request_key text PRIMARY KEY,source_id text NOT NULL,source_url text NOT NULL,expected_raw_sha256 text,
 provider text NOT NULL CHECK(provider IN('BLS','BEA','Census')),input_versions jsonb NOT NULL,
 not_after timestamptz NOT NULL,max_bytes bigint NOT NULL CHECK(max_bytes BETWEEN 1 AND 8388608),created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx07_recovery_body_v6(
 request_key text PRIMARY KEY REFERENCES population_top3000_202608_202609_v1.ctx07_recovery_request_v6,body bytea NOT NULL,raw_sha256 text NOT NULL,
 retrieved_at timestamptz NOT NULL,content_type text NOT NULL,first_attempt_id uuid,
 CHECK(octet_length(body) BETWEEN 1 AND 8388608),CHECK(raw_sha256=encode(sha256(body),'hex')));
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6(
 request_key text NOT NULL REFERENCES population_top3000_202608_202609_v1.ctx07_recovery_request_v6,observation_id text NOT NULL,
 evidence jsonb NOT NULL,evidence_sha256 text NOT NULL,first_attempt_id uuid NOT NULL,
 PRIMARY KEY(request_key,observation_id),CHECK(evidence_sha256=encode(sha256(convert_to(evidence::text,'UTF8')),'hex')));
ALTER TABLE population_top3000_202608_202609_v1.ctx07_recovery_request_v6 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ctx07_recovery_body_v6 ENABLE ROW LEVEL SECURITY;
ALTER TABLE population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON population_top3000_202608_202609_v1.ctx07_recovery_request_v6,population_top3000_202608_202609_v1.ctx07_recovery_body_v6,population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 FROM PUBLIC,anon,authenticated;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_context_v6(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;r population_top3000_202608_202609_v1.ctx07_recovery_request_v6%rowtype;a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;
BEGIN
 SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=p_task;
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.admitted_until IS NULL OR p.deadline IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline<=clock_timestamp() OR p.tick_count>=p.max_ticks
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd'
 OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 THEN RAISE EXCEPTION 'CTX07_V6_CURRENT_AUTHORITY_REQUIRED';END IF;
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ctx07_recovery_request_v6 WHERE request_key=t.partition_key;
 IF t.family_id IS DISTINCT FROM 'CTX07' OR t.payload->>'kind' IS DISTINCT FROM 'CTX07_SOURCE_RECOVERY_V6'
 OR t.payload->>'request_key' IS DISTINCT FROM r.request_key OR t.input_versions IS DISTINCT FROM r.input_versions
 OR r.input_versions->>'scope_sha256' IS DISTINCT FROM a.scope_sha256
 OR r.input_versions->>'bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ctx07_recovery_bundle_v6()
 OR r.input_versions->>'observations_sha256' IS DISTINCT FROM (SELECT encode(sha256(convert_to(string_agg(to_jsonb(o)::text,E'\n' ORDER BY observation_id),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.macro_observations o)
 OR r.input_versions->>'dictionary_sha256' IS DISTINCT FROM (SELECT encode(sha256(convert_to(string_agg(payload_sha256,E'\n' ORDER BY item_key),'UTF8')),'hex') FROM population_top3000_202608_202609_v1.catalog_dictionary WHERE family_id='CTX07' AND scope_id='corrected_company_scope_8e6ca1178f03822c')
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.macro_observations)<>79
 OR (SELECT count(*) FROM population_top3000_202608_202609_v1.catalog_dictionary WHERE family_id='CTX07' AND scope_id='corrected_company_scope_8e6ca1178f03822c')<>21
 OR r.not_after<=clock_timestamp() OR r.not_after>a.admitted_until OR r.not_after>p.deadline
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.macro_source_registry g WHERE g.source_id=r.source_id AND g.source_url=r.source_url AND g.raw_sha256 IS NOT DISTINCT FROM r.expected_raw_sha256)
 OR r.source_url !~ '^https://(www[.]bls[.]gov|www[.]bea[.]gov|www2?[.]census[.]gov)/'
 THEN RAISE EXCEPTION 'CTX07_V6_SCOPE_SOURCE_OR_CODE_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_recovery_request_v6)>28 OR ((SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task x USING(task_id) WHERE x.payload->>'kind'='CTX07_SOURCE_RECOVERY_V6')+(SELECT coalesce(sum((details->>'source_http_requests')::bigint),0) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_DIRECT_SOURCE_IMPORT'))>64
 OR (SELECT coalesce(sum(octet_length(body)),0) FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6)>67108864 THEN RAISE EXCEPTION 'CTX07_V6_FINITE_ADMISSION_REVIEW_REQUIRED';END IF;
 RETURN to_jsonb(r)||jsonb_build_object('state','READY','max_runtime_ms',45000);
END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_claim_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;c jsonb;r jsonb;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);c:=population_top3000_202608_202609_v1.ctx07_recovery_context_v6(tid);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'CTX07_V6_STAGE';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6 WHERE request_key=c->>'request_key') THEN RAISE EXCEPTION 'CTX07_V6_BODY_ALREADY_RETAINED_NO_REFETCH';END IF;
 IF ((SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task x USING(task_id) WHERE x.payload->>'kind'='CTX07_SOURCE_RECOVERY_V6')+(SELECT coalesce(sum((details->>'source_http_requests')::bigint),0) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_DIRECT_SOURCE_IMPORT'))>=64 THEN RAISE EXCEPTION 'CTX07_V6_REQUEST_CAP';END IF;
 IF (SELECT coalesce(sum(octet_length(body)),0) FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6)+(c->>'max_bytes')::bigint>67108864 THEN RAISE EXCEPTION 'CTX07_V6_BYTE_ADMISSION';END IF;
 r:=population_top3000_202608_202609_v1.pipeline_provider_reserve_v1(p_attempt,p_token,c->>'provider',(c->>'max_bytes')::bigint,0);
 IF r->>'state'<>'RESERVED' THEN RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code',CASE WHEN r->>'state'='WAIT' THEN 'DEPENDENCY_PENDING' ELSE 'PROVIDER_BUDGET_OR_RESOURCE_HOLD' END,'recheck_at',r->>'not_before','resumption_condition',r));END IF;
 RETURN c||jsonb_build_object('state','RESERVED');END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_commit_v6(p_attempt uuid,p_token text,p_result jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;c jsonb;b bytea;h text;at_time timestamptz;n integer;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);c:=population_top3000_202608_202609_v1.ctx07_recovery_context_v6(tid);
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid AND stage='ACQUIRE') THEN RAISE EXCEPTION 'CTX07_V6_STAGE';END IF;
 IF p_result->>'request_key' IS DISTINCT FROM c->>'request_key' OR p_result->>'request_url' IS DISTINCT FROM c->>'source_url'
 OR p_result->>'http_status' IS DISTINCT FROM '200' OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_provider_lease WHERE attempt_id=p_attempt AND provider=c->>'provider' AND released_at IS NULL) THEN RAISE EXCEPTION 'CTX07_V6_EXACT_RECEIPT';END IF;
 b:=decode(p_result->>'body_base64','base64');h:=encode(sha256(b),'hex');at_time:=(p_result->>'retrieved_at')::timestamptz;
 IF b IS NULL OR octet_length(b) NOT BETWEEN 1 AND (c->>'max_bytes')::bigint OR h IS DISTINCT FROM p_result->>'raw_sha256'
 OR at_time IS NULL OR at_time<(SELECT started_at FROM population_top3000_202608_202609_v1.pipeline_attempt WHERE attempt_id=p_attempt)-interval '5 seconds' OR at_time>clock_timestamp()+interval '30seconds'
 OR (SELECT coalesce(sum(octet_length(body)),0) FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6)+octet_length(b)>67108864 THEN RAISE EXCEPTION 'CTX07_V6_BODY_OR_TIME_INVALID';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_body_v6 VALUES(c->>'request_key',b,h,at_time,coalesce(p_result->>'content_type','application/octet-stream'),p_attempt) ON CONFLICT DO NOTHING;GET DIAGNOSTICS n=ROW_COUNT;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6 WHERE request_key=c->>'request_key' AND body=b AND raw_sha256=h AND retrieved_at=at_time) THEN RAISE EXCEPTION 'CTX07_V6_IMMUTABLE_CONFLICT';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,CASE WHEN n=1 THEN octet_length(b) ELSE 0 END,0,0);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','ctx07-recovery-v6:ACQUIRE:'||(c->>'request_key'),'output_sha256',h,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(c->>'request_key'),'input_versions',c->'input_versions','exact_prior_hash_match',h=c->>'expected_raw_sha256','historical_receipt_qualified',false)));
END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_failure_v6(p_attempt uuid,p_token text,p_failure jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;c jsonb;st integer;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);c:=population_top3000_202608_202609_v1.ctx07_recovery_context_v6(tid);st:=(p_failure->>'http_status')::integer;
 IF p_failure->>'fetch_started'='true' AND st IS NULL THEN RETURN population_top3000_202608_202609_v1.pipeline_fail_v1(p_attempt,p_token,'UNCERTAIN_EFFECT','CTX07_V6_UNKNOWN_HTTP',jsonb_build_object('details',p_failure,'automatic_retry',false),60);END IF;
 PERFORM population_top3000_202608_202609_v1.pipeline_provider_release_v1(p_attempt,p_token,0,0,CASE WHEN st=429 THEN 60 ELSE 0 END);
 RETURN population_top3000_202608_202609_v1.pipeline_finish_v1(p_attempt,p_token,jsonb_build_object('state','BLOCKED','blocker_code',CASE WHEN st IN(401,403) THEN 'CTX07_V6_ENFORCED_ACCESS_DENIAL' WHEN st=429 THEN 'CTX07_V6_RATE_LIMIT' WHEN st=404 THEN 'CTX07_V6_SOURCE_ABSENT' ELSE 'CTX07_V6_SOURCE_RECOVERY_REVIEW' END,'resumption_condition',p_failure));END;$f$;

CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_import_v6(p_request_key text,p_body_base64 text,p_receipt jsonb)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE r population_top3000_202608_202609_v1.ctx07_recovery_request_v6%rowtype;t uuid;b bytea;h text;n integer;key text:='ctx07-v6-direct-empsit-202607-20261024T090408Z';BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT r FROM population_top3000_202608_202609_v1.ctx07_recovery_request_v6 WHERE request_key=p_request_key;
 SELECT task_id INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE partition_key=p_request_key AND family_id='CTX07' AND stage='ACQUIRE';
 PERFORM population_top3000_202608_202609_v1.ctx07_recovery_context_v6(t);
 IF r.source_id IS DISTINCT FROM 'empsit_202607' OR p_receipt->>'url' IS DISTINCT FROM r.source_url OR p_receipt->>'http_status' IS DISTINCT FROM '200' OR p_receipt->>'sha256' IS DISTINCT FROM '4602e50c53ccfc789d52ef73191171d9918141516b7734524e70f2b8326024a5' OR p_receipt->>'retrieved_at' IS DISTINCT FROM '2026-10-24T09:04:08.612178+00:00' OR p_receipt->>'historical_received_at' IS NOT NULL THEN RAISE EXCEPTION 'CTX07_IMPORT_EXACT_DIRECT_RECEIPT';END IF;
 b:=decode(p_body_base64,'base64');h:=encode(sha256(b),'hex');
 IF h IS DISTINCT FROM r.expected_raw_sha256 OR h IS DISTINCT FROM p_receipt->>'sha256' OR octet_length(b)<>1066217 OR octet_length(b)>r.max_bytes THEN RAISE EXCEPTION 'CTX07_IMPORT_BYTES';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_DIRECT_SOURCE_IMPORT' AND details->>'request_key'=p_request_key) THEN
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6 WHERE request_key=p_request_key AND body=b AND raw_sha256=h) THEN RAISE EXCEPTION 'CTX07_IMPORT_REPLAY_CONFLICT';END IF;
 RETURN jsonb_build_object('state','ALREADY_RETAINED','bytes',octet_length(b),'new_provider_requests',0);END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=t AND (status<>'BLOCKED' OR blocker_code IS DISTINCT FROM 'CTX07_V6_RETAINED_BODY_IMPORT_PENDING' OR executions>0)) THEN RAISE EXCEPTION 'CTX07_IMPORT_TASK_NOT_WAITING';END IF;
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task x USING(task_id) WHERE x.payload->>'kind'='CTX07_SOURCE_RECOVERY_V6')+(SELECT coalesce(sum((details->>'source_http_requests')::bigint),0) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_DIRECT_SOURCE_IMPORT')>=64 THEN RAISE EXCEPTION 'CTX07_V6_REQUEST_CAP';END IF;
 IF (SELECT coalesce(sum(octet_length(body)),0) FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6)+octet_length(b)>67108864 OR EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND (stored_bytes+reserved_bytes+octet_length(b)>max_source_bytes OR provider_requests>=max_provider_requests)) THEN RAISE EXCEPTION 'CTX07_IMPORT_CURRENT_CAP';END IF;
 INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_body_v6 VALUES(p_request_key,b,h,(p_receipt->>'retrieved_at')::timestamptz,'text/html',NULL);
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET provider_requests=provider_requests+1,stored_bytes=stored_bytes+octet_length(b) WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CTX07_V6_DIRECT_SOURCE_IMPORT',jsonb_build_object('request_key',p_request_key,'logical_request_key',key,'receipt',p_receipt,'source_sha256',h,'stored_bytes',octet_length(b),'new_provider_requests',1,'edge_http_invocations_added',0,'source_http_requests',1,'actual_billing_unknown',true,'historical_receipt_qualified',false));
 UPDATE population_top3000_202608_202609_v1.pipeline_task SET status='READY',blocker_code=NULL,blocker_until=NULL,not_before=clock_timestamp(),resumption_condition=coalesce(resumption_condition,'{}'::jsonb)||jsonb_build_object('exact_import_retained',true,'import_receipt_key',key) WHERE task_id=t AND status='BLOCKED' AND blocker_code='CTX07_V6_RETAINED_BODY_IMPORT_PENDING';
 PERFORM population_top3000_202608_202609_v1.pipeline_wake_v1('top3000-completion-20261024-root-5db48b32d103',3,'CTX07 retained exact original body imported; successor route runnable');
 RETURN jsonb_build_object('state','RETAINED','bytes',octet_length(b),'new_provider_requests',1);END;$f$;

-- Independent lexical parser: no retained actual or excerpt is used to choose the source value.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_empsit_extract_v6(p_html text,p_factor text,p_role text,p_reference timestamptz,p_release timestamptz)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SET search_path TO pg_catalog AS $f$
DECLARE txt text;pat text;lex text;header text;period text;units text;BEGIN
 IF p_role<>'headline' OR p_factor NOT IN('average_hourly_earnings_as_released','labor_force_participation_as_released','unemployment_rate_as_released') THEN RETURN '{"status":"EXTRACTION_RULE_REQUIRED"}';END IF;
 txt:=regexp_replace(regexp_replace(p_html,'<[^>]*>',' ','g'),'[[:space:]]+',' ','g');
 header:='8:30 a.m. (ET) '||to_char(p_release AT TIME ZONE 'America/New_York','FMDay, FMMonth FMDD, YYYY');
 period:='THE EMPLOYMENT SITUATION - '||upper(to_char(p_reference AT TIME ZONE 'UTC','FMMonth YYYY'));
 IF position(header in txt)=0 OR position(period in txt)=0 THEN RETURN '{"status":"PUBLICATION_OR_REFERENCE_PERIOD_UNVERIFIED"}';END IF;
 IF p_factor='average_hourly_earnings_as_released' THEN pat:='In '||to_char(p_reference AT TIME ZONE 'UTC','FMMonth')||', average hourly earnings for all employees on private nonfarm payrolls, at [$]([0-9]+[.][0-9]+)';units:='USD/hour';
 ELSIF p_factor='labor_force_participation_as_released' THEN pat:='Both the labor force participation rate, at ([0-9]+[.][0-9]+) percent';units:='percent';
 ELSE pat:='Both the unemployment rate, at ([0-9]+[.][0-9]+) percent';units:='percent';END IF;
 lex:=substring(txt from pat);
 IF lex IS NULL THEN RETURN '{"status":"NUMERICAL_PATTERN_UNVERIFIED"}';END IF;
 RETURN jsonb_build_object('status','EXTRACTED','lexeme',lex,'value',lex::numeric,'units',units,'source_text_sha256',encode(sha256(convert_to(txt,'UTF8')),'hex'),'source_location_start',strpos(txt,substring(txt from '('||pat||')')),'publication_header',header,'reference_period_header',period,'release_vintage',p_release,'historical_receipt_qualified',false);
END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_process_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;b population_top3000_202608_202609_v1.ctx07_recovery_body_v6%rowtype;o record;x jsonb;e jsonb;h text;txt text;n integer:=0;inserted integer;count_units integer;out_hash text;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;c:=population_top3000_202608_202609_v1.ctx07_recovery_context_v6(tid);
 SELECT * INTO STRICT b FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6 WHERE request_key=c->>'request_key';
 IF b.raw_sha256 IS DISTINCT FROM encode(sha256(b.body),'hex') THEN RAISE EXCEPTION 'CTX07_V6_BODY_TAMPER';END IF;
 IF t.stage='VALIDATE' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX07_V6_INDEPENDENT_NUMERICAL_TEMPORAL_REVIEW','resumption_condition',jsonb_build_object('request_key',c->>'request_key','evidence_rows',(SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 WHERE request_key=c->>'request_key'),'required_action_kind','INDEPENDENT_VERIFICATION','original_receipt_qualified',false,'full_vintage_denominator_accepted',false));END IF;
 IF b.content_type LIKE '%html%' OR b.content_type LIKE 'text/%' THEN txt:=convert_from(b.body,'UTF8');END IF;
 FOR o IN SELECT * FROM population_top3000_202608_202609_v1.macro_observations WHERE source_id=c->>'source_id' ORDER BY observation_id LOOP
  x:=CASE WHEN txt IS NOT NULL THEN population_top3000_202608_202609_v1.ctx07_empsit_extract_v6(txt,o.factor_name,o.record_role,o.reference_period,o.released_at) ELSE '{"status":"BINARY_EXTRACTION_RULE_REQUIRED"}'::jsonb END;
  e:=jsonb_build_object('observation_id',o.observation_id,'source_id',o.source_id,'source_sha256',b.raw_sha256,'prior_source_sha256',o.source_content_sha256,'source_retrieved_at',b.retrieved_at,'exact_prior_hash_match',b.raw_sha256=o.source_content_sha256,'factor_name',o.factor_name,'record_role',o.record_role,'reference_period',o.reference_period,'retained_numeric',o.actual,'retained_units',o.units,'extraction',x,
  'status',CASE WHEN b.raw_sha256 IS DISTINCT FROM o.source_content_sha256 THEN 'SOURCE_VERSION_DIFFERS' WHEN x->>'status'<>'EXTRACTED' THEN x->>'status' WHEN o.actual IS NULL THEN 'RETAINED_VALUE_MISSING' WHEN (x->>'value')::numeric IS DISTINCT FROM o.actual OR x->>'units' IS DISTINCT FROM o.units THEN 'NUMERICAL_OR_UNIT_MISMATCH' ELSE 'NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED' END,
  'publication_supported_available_at',CASE WHEN x->>'status'='EXTRACTED' THEN o.released_at ELSE NULL END,'historical_received_at',NULL,'strict_original_receipt_qualified',false,'full_family_accepted',false);
  h:=encode(sha256(convert_to(e::text,'UTF8')),'hex');
  IF t.stage='NORMALIZE' THEN INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 VALUES(c->>'request_key',o.observation_id,e,h,p_attempt) ON CONFLICT DO NOTHING;GET DIAGNOSTICS inserted=ROW_COUNT;n:=n+inserted;END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 z WHERE z.request_key=c->>'request_key' AND z.observation_id=o.observation_id AND z.evidence=e AND z.evidence_sha256=h) THEN RAISE EXCEPTION 'CTX07_V6_EVIDENCE_REPLAY_CONFLICT';END IF;
 END LOOP;
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(evidence_sha256,E'\n' ORDER BY observation_id),''),'UTF8')),'hex') INTO count_units,out_hash FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 WHERE request_key=c->>'request_key';
 IF count_units<>(SELECT count(*) FROM population_top3000_202608_202609_v1.macro_observations WHERE source_id=c->>'source_id') THEN RAISE EXCEPTION 'CTX07_V6_DENOMINATOR_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','ctx07-recovery-v6:'||t.stage||':'||(c->>'request_key'),'output_sha256',out_hash,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(c->>'request_key'),'input_versions',t.input_versions,'evidence_units',count_units,'full_family_accepted',false,'strict_original_receipt_qualified',false));END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_asof_v6(p_factor text,p_at timestamptz)
RETURNS SETOF jsonb LANGUAGE sql STABLE SET search_path TO pg_catalog AS $f$
 SELECT evidence FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 WHERE evidence->>'factor_name'=p_factor AND evidence->>'status'='NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED' AND (evidence->>'publication_supported_available_at')::timestamptz<=p_at ORDER BY (evidence->>'publication_supported_available_at')::timestamptz DESC,(evidence->>'reference_period')::timestamptz DESC LIMIT 1;
$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_acquire_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;c jsonb;rid bigint;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k IS DISTINCT FROM 'CTX07_SOURCE_RECOVERY_V6' THEN RETURN population_top3000_202608_202609_v1.ctx07_source_verification_acquire_v5(p_attempt,p_token);END IF;
 c:=population_top3000_202608_202609_v1.ctx07_recovery_context_v6(tid);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6 WHERE request_key=c->>'request_key') THEN RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','ctx07-recovery-v6:ACQUIRE:'||(c->>'request_key'),'output_sha256',(SELECT raw_sha256 FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6 WHERE request_key=c->>'request_key'),'new_committed_units',0,'immutable_output_ids',jsonb_build_array(c->>'request_key'),'input_versions',c->'input_versions'));END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_WORKER_REVIEWED_DEPLOYED' AND details->>'bundle_sha256'=population_top3000_202608_202609_v1.ctx07_recovery_bundle_v6()) THEN RAISE EXCEPTION 'CTX07_V6_DEPLOYMENT_REQUIRED';END IF;
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton AND http_invocations>=max_http_invocations) THEN RETURN '{"state":"BLOCKED","blocker_code":"HTTP_INVOCATION_CAP"}';END IF;
 SELECT net.http_post(url:='https://oxzabweahkoimtevbbny.supabase.co/functions/v1/population-top3000-macro-source-v6',headers:='{"Content-Type":"application/json"}',body:=jsonb_build_object('attempt_id',p_attempt,'token',p_token),timeout_milliseconds:=55000) INTO rid;
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET http_invocations=http_invocations+1 WHERE singleton;UPDATE population_top3000_202608_202609_v1.pipeline_attempt SET request_id=rid WHERE attempt_id=p_attempt;
 RETURN jsonb_build_object('state','ASYNC','request_id',rid,'worker','population-top3000-macro-source-v6');
END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_normalize_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;c jsonb;rid bigint;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k IS DISTINCT FROM 'CTX07_SOURCE_RECOVERY_V6' THEN RETURN population_top3000_202608_202609_v1.ctx07_source_verification_normalize_v5(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx07_recovery_process_v6(p_attempt,p_token);
END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_derive_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;c jsonb;rid bigint;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k IS DISTINCT FROM 'CTX07_SOURCE_RECOVERY_V6' THEN RETURN population_top3000_202608_202609_v1.ctx07_source_verification_derive_v5(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx07_recovery_process_v6(p_attempt,p_token);
END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_validate_v6(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;k text;c jsonb;rid bigint;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT payload->>'kind' INTO k FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;
 IF k IS DISTINCT FROM 'CTX07_SOURCE_RECOVERY_V6' THEN RETURN population_top3000_202608_202609_v1.ctx07_source_verification_validate_v5(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx07_recovery_process_v6(p_attempt,p_token);
END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_bundle_v6()
RETURNS text LANGUAGE sql STABLE SET search_path TO pg_catalog AS $f$
 SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(p.oid),E'\n' ORDER BY p.proname),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname IN('ctx07_recovery_context_v6','ctx07_recovery_import_v6','ctx07_recovery_claim_v6','ctx07_recovery_commit_v6','ctx07_recovery_failure_v6','ctx07_empsit_extract_v6','ctx07_recovery_process_v6','ctx07_recovery_asof_v6','ctx07_recovery_acquire_v6','ctx07_recovery_normalize_v6','ctx07_recovery_derive_v6','ctx07_recovery_validate_v6');$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_context_v6(uuid) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_claim_v6(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_commit_v6(uuid,text,jsonb) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_failure_v6(uuid,text,jsonb) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_import_v6(text,text,jsonb) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_empsit_extract_v6(text,text,text,timestamptz,timestamptz) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_process_v6(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_asof_v6(text,timestamptz) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_acquire_v6(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_normalize_v6(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_derive_v6(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_validate_v6(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_bundle_v6() FROM PUBLIC,anon,authenticated;
