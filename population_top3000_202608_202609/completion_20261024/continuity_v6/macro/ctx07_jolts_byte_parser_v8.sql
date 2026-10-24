-- Four exact retained JOLTS sources only. No HTTP, charset inference or body rewriting.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_scope_v8()
RETURNS TABLE(source_id text,raw_sha256 text,normalize_task_id uuid,expected_units integer) LANGUAGE sql IMMUTABLE SET search_path TO pg_catalog AS $f$
 VALUES ('existing_warmup_d7606a9701f41c82','79ad299ec645796dbc2a961a6d37a743aac80975476921aaff8bf73535b86483','43f8e4b4-6d6a-4ec1-b599-b8a74885d006'::uuid,1),('jolts_202606','e5e4aac65e338bad17ffc7ba80740575eb3cde770dfbc16a0dc929b922e5d0a8','c01d683c-9bb5-4f8f-946f-767d416f989c'::uuid,2),('jolts_202607','78ce44264dc7e95fde56200f82233f2b053413c5ee1fa7b34221f9d99e7f2d74','4bb7d101-1437-435d-972f-bed174251243'::uuid,2),('jolts_202608','24d558352a9b8e14755c7201956297a6579b4018fc968bf7de2ca557c8abcb47','13de0647-819f-4cf6-af70-9a7928a819e4'::uuid,2);$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_context_v8(p_task uuid)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE t population_top3000_202608_202609_v1.pipeline_task%rowtype;r population_top3000_202608_202609_v1.ctx07_recovery_request_v6%rowtype;a population_top3000_202608_202609_v1.acquisition_control%rowtype;p population_top3000_202608_202609_v1.pipeline_control%rowtype;ad jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 PERFORM 1 FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;PERFORM 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
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

 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.macro_source_registry g WHERE g.source_id=r.source_id AND g.source_url=r.source_url AND g.raw_sha256 IS NOT DISTINCT FROM r.expected_raw_sha256)
 OR r.source_url !~ '^https://(www[.]bls[.]gov|www[.]bea[.]gov|www2?[.]census[.]gov)/'
 THEN RAISE EXCEPTION 'CTX07_V6_SCOPE_SOURCE_OR_CODE_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 IF (SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_recovery_request_v6)>28 OR ((SELECT count(*) FROM population_top3000_202608_202609_v1.pipeline_provider_lease l JOIN population_top3000_202608_202609_v1.pipeline_attempt z USING(attempt_id) JOIN population_top3000_202608_202609_v1.pipeline_task x USING(task_id) WHERE x.payload->>'kind'='CTX07_SOURCE_RECOVERY_V6')+(SELECT coalesce(sum((details->>'source_http_requests')::bigint),0) FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V6_DIRECT_SOURCE_IMPORT'))>64
 OR (SELECT coalesce(sum(octet_length(body)),0) FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6)>67108864 THEN RAISE EXCEPTION 'CTX07_V6_FINITE_ADMISSION_REVIEW_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_jolts_scope_v8() z JOIN population_top3000_202608_202609_v1.ctx07_recovery_body_v6 b ON b.request_key=r.request_key WHERE z.source_id=r.source_id AND z.raw_sha256=r.expected_raw_sha256 AND b.raw_sha256=z.raw_sha256 AND b.raw_sha256=encode(sha256(b.body),'hex') AND b.content_type='text/html') THEN RAISE EXCEPTION 'CTX07_V8_EXACT_JOLTS_BODY_REQUIRED';END IF;
 SELECT details INTO ad FROM population_top3000_202608_202609_v1.pipeline_event WHERE event='CTX07_V8_JOLTS_PARSER_ADMISSION' ORDER BY event_id DESC LIMIT 1;
 IF ad->>'owner_id' IS DISTINCT FROM a.owner_id OR ad->>'owner_fence' IS DISTINCT FROM '3' OR ad->>'stop_generation' IS DISTINCT FROM '1' OR ad->>'scope_sha256' IS DISTINCT FROM a.scope_sha256 OR ad->>'bundle_sha256' IS DISTINCT FROM population_top3000_202608_202609_v1.ctx07_jolts_bundle_v8() OR ad->>'assignment_id' IS DISTINCT FROM 'ctx07-four-retained-jolts-byte-parser-v8' OR ad->>'source_http_requests' IS DISTINCT FROM '0' OR ad->>'not_after' IS NULL OR (ad->>'not_after')::timestamptz<=clock_timestamp() OR (ad->>'not_after')::timestamptz>least(a.admitted_until,p.deadline) THEN RAISE EXCEPTION 'CTX07_V8_PARSER_ADMISSION_REQUIRED';END IF;
 RETURN to_jsonb(r)||jsonb_build_object('state','READY','max_runtime_ms',45000,'parser_admission',ad);
END;$f$;
-- LATIN1 here is a reversible byte-to-codepoint projection, NOT an asserted document encoding.
-- Only exact ASCII spans and the pinned heading byte 0x96 are interpreted. Bytes never rewritten.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(p_body bytea,p_factor text,p_role text,p_reference timestamptz,p_release timestamptz)
RETURNS jsonb LANGUAGE plpgsql IMMUTABLE SET search_path TO pg_catalog AS $f$
DECLARE txt text;pub text;heading text;pat text;span text;lex text;cnt integer;mon text;headline_ref timestamptz;ws text:='[ \t\r\n]+';BEGIN
 IF p_factor IS DISTINCT FROM 'job_openings_millions_as_released' OR p_role NOT IN('headline','revision','existing_warmup') THEN RETURN '{"status":"EXTRACTION_RULE_REQUIRED"}';END IF;
 IF p_body IS NULL OR octet_length(p_body)>8388608 OR position(decode('00','hex') in p_body)>0 THEN RETURN '{"status":"UNSUPPORTED_BYTE_BODY"}';END IF;
 txt:=convert_from(p_body,'LATIN1');
 IF convert_to(txt,'LATIN1') IS DISTINCT FROM p_body THEN RAISE EXCEPTION 'CTX07_V8_PROJECTION_NOT_LOSSLESS';END IF;
 IF p_release AT TIME ZONE 'America/New_York' <> date_trunc('day',p_release AT TIME ZONE 'America/New_York')+interval '10hours' OR p_reference AT TIME ZONE 'America/New_York'<>date_trunc('month',p_reference AT TIME ZONE 'America/New_York') THEN RETURN '{"status":"PUBLICATION_OR_REFERENCE_PERIOD_UNVERIFIED"}';END IF;
 pub:='For release 10:00 a[.]m[.] [(]ET[)] '||to_char(p_release AT TIME ZONE 'America/New_York','FMDay, FMMonth FMDD, YYYY');pub:=replace(pub,' ',ws);
 SELECT count(*) INTO cnt FROM regexp_matches(txt,pub,'g');IF cnt<>1 THEN RETURN '{"status":"PUBLICATION_OR_REFERENCE_PERIOD_UNVERIFIED"}';END IF;
 headline_ref:=CASE WHEN p_role='revision' THEN p_reference+interval '1month' ELSE p_reference END;
 heading:=replace('JOB OPENINGS AND LABOR TURNOVER '||chr(150)||' '||upper(to_char(headline_ref AT TIME ZONE 'America/New_York','FMMonth YYYY')),' ',ws);
 SELECT count(*) INTO cnt FROM regexp_matches(txt,heading,'g');IF cnt<>1 THEN RETURN '{"status":"PUBLICATION_OR_REFERENCE_PERIOD_UNVERIFIED"}';END IF;
 mon:=to_char(p_reference AT TIME ZONE 'America/New_York','FMMonth');
 IF p_role='revision' THEN pat:=mon||' '||to_char(p_reference AT TIME ZONE 'America/New_York','YYYY')||' Revisions The number of job openings for '||mon||' was revised (?:up|down) by [0-9,]+ to ([0-9]+[.][0-9]) million,';
 ELSE pat:='The number of job openings was (?:unchanged|little changed) at ([0-9]+[.][0-9]) million in '||mon||', the U[.]S[.] Bureau of Labor Statistics reported today[.]';END IF;
 pat:='('||replace(pat,' ',ws)||')';SELECT count(*),min(m[1]),min(m[2]) INTO cnt,span,lex FROM regexp_matches(txt,pat,'g')m;
 IF cnt<>1 OR span IS NULL OR convert_to(span,'LATIN1') IS DISTINCT FROM convert_to(span,'UTF8') THEN RETURN jsonb_build_object('status','NUMERICAL_PATTERN_UNVERIFIED','matching_spans',cnt);END IF;
 RETURN jsonb_build_object('status','EXTRACTED','parser_version','ctx07-jolts-byte-ascii-v8','lexeme',lex,'value',lex::numeric,'units','millions of jobs','projection','LOSSLESS_BYTE_PROJECTION_ASCII_ANCHORS_ONLY','document_charset_qualified',false,'matched_byte_start',strpos(txt,span),'matched_byte_start_basis','ONE_BASED_RAW_BYTE_OFFSET','matched_byte_length',length(span),'matched_bytes_sha256',encode(sha256(convert_to(span,'LATIN1')),'hex'),'publication_header_pattern',pub,'reference_period_header_pattern',heading,'release_vintage',p_release,'historical_receipt_qualified',false);
END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_process_v8(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;c jsonb;b population_top3000_202608_202609_v1.ctx07_recovery_body_v6%rowtype;o record;x jsonb;e jsonb;h text;txt text;n integer:=0;inserted integer;count_units integer;out_hash text;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);SELECT * INTO STRICT t FROM population_top3000_202608_202609_v1.pipeline_task WHERE task_id=tid;c:=population_top3000_202608_202609_v1.ctx07_jolts_context_v8(tid);
 SELECT * INTO STRICT b FROM population_top3000_202608_202609_v1.ctx07_recovery_body_v6 WHERE request_key=c->>'request_key';
 IF b.raw_sha256 IS DISTINCT FROM encode(sha256(b.body),'hex') THEN RAISE EXCEPTION 'CTX07_V6_BODY_TAMPER';END IF;
 IF t.stage='VALIDATE' THEN RETURN jsonb_build_object('state','BLOCKED','blocker_code','CTX07_V6_INDEPENDENT_NUMERICAL_TEMPORAL_REVIEW','resumption_condition',jsonb_build_object('request_key',c->>'request_key','evidence_rows',(SELECT count(*) FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 WHERE request_key=c->>'request_key'),'required_action_kind','INDEPENDENT_VERIFICATION','original_receipt_qualified',false,'full_vintage_denominator_accepted',false));END IF;
 IF t.stage NOT IN('NORMALIZE','DERIVE') THEN RAISE EXCEPTION 'CTX07_V8_STAGE';END IF;
 FOR o IN SELECT * FROM population_top3000_202608_202609_v1.macro_observations WHERE source_id=c->>'source_id' ORDER BY observation_id LOOP
  x:=population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(b.body,o.factor_name,o.record_role,o.reference_period,o.released_at);
  e:=jsonb_build_object('parser_version','ctx07-jolts-byte-ascii-v8','original_content_type',b.content_type,'document_charset_qualified',false,'observation_id',o.observation_id,'source_id',o.source_id,'source_sha256',b.raw_sha256,'prior_source_sha256',o.source_content_sha256,'source_retrieved_at',b.retrieved_at,'exact_prior_hash_match',b.raw_sha256=o.source_content_sha256,'factor_name',o.factor_name,'record_role',o.record_role,'reference_period',o.reference_period,'retained_numeric',o.actual,'retained_units',o.units,'extraction',x,
  'status',CASE WHEN b.raw_sha256 IS DISTINCT FROM o.source_content_sha256 THEN 'SOURCE_VERSION_DIFFERS' WHEN x->>'status'<>'EXTRACTED' THEN x->>'status' WHEN o.actual IS NULL THEN 'RETAINED_VALUE_MISSING' WHEN (x->>'value')::numeric IS DISTINCT FROM o.actual OR x->>'units' IS DISTINCT FROM o.units THEN 'NUMERICAL_OR_UNIT_MISMATCH' ELSE 'NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED' END,
  'publication_supported_available_at',CASE WHEN x->>'status'='EXTRACTED' THEN o.released_at ELSE NULL END,'historical_received_at',NULL,'strict_original_receipt_qualified',false,'full_family_accepted',false);
  h:=encode(sha256(convert_to(e::text,'UTF8')),'hex');
  IF t.stage='NORMALIZE' THEN INSERT INTO population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 VALUES(c->>'request_key',o.observation_id,e,h,p_attempt) ON CONFLICT DO NOTHING;GET DIAGNOSTICS inserted=ROW_COUNT;n:=n+inserted;END IF;
  IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 z WHERE z.request_key=c->>'request_key' AND z.observation_id=o.observation_id AND z.evidence=e AND z.evidence_sha256=h) THEN RAISE EXCEPTION 'CTX07_V6_EVIDENCE_REPLAY_CONFLICT';END IF;
 END LOOP;
 SELECT count(*),encode(sha256(convert_to(coalesce(string_agg(evidence_sha256,E'\n' ORDER BY observation_id),''),'UTF8')),'hex') INTO count_units,out_hash FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 WHERE request_key=c->>'request_key';
 IF count_units<>(SELECT count(*) FROM population_top3000_202608_202609_v1.macro_observations WHERE source_id=c->>'source_id') THEN RAISE EXCEPTION 'CTX07_V6_DENOMINATOR_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','ctx07-jolts-v8:'||t.stage||':'||(c->>'request_key'),'output_sha256',out_hash,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(c->>'request_key'),'input_versions',t.input_versions,'evidence_units',count_units,'full_family_accepted',false,'strict_original_receipt_qualified',false));END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_normalize_v8(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.ctx07_recovery_request_v6 r ON r.request_key=t.partition_key JOIN population_top3000_202608_202609_v1.ctx07_jolts_scope_v8() s USING(source_id) WHERE t.task_id=tid AND t.payload->>'kind'='CTX07_SOURCE_RECOVERY_V6') THEN RETURN population_top3000_202608_202609_v1.ctx07_jolts_process_v8(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx07_recovery_normalize_v6(p_attempt,p_token);END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_derive_v8(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.ctx07_recovery_request_v6 r ON r.request_key=t.partition_key JOIN population_top3000_202608_202609_v1.ctx07_jolts_scope_v8() s USING(source_id) WHERE t.task_id=tid AND t.payload->>'kind'='CTX07_SOURCE_RECOVERY_V6') THEN RETURN population_top3000_202608_202609_v1.ctx07_jolts_process_v8(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx07_recovery_derive_v6(p_attempt,p_token);END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_validate_v8(p_attempt uuid,p_token text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO pg_catalog AS $f$
DECLARE tid uuid;BEGIN
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 IF EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_task t JOIN population_top3000_202608_202609_v1.ctx07_recovery_request_v6 r ON r.request_key=t.partition_key JOIN population_top3000_202608_202609_v1.ctx07_jolts_scope_v8() s USING(source_id) WHERE t.task_id=tid AND t.payload->>'kind'='CTX07_SOURCE_RECOVERY_V6') THEN RETURN population_top3000_202608_202609_v1.ctx07_jolts_process_v8(p_attempt,p_token);END IF;
 RETURN population_top3000_202608_202609_v1.ctx07_recovery_validate_v6(p_attempt,p_token);END;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_asof_v8(p_reference timestamptz,p_at timestamptz)
RETURNS SETOF jsonb LANGUAGE sql STABLE SET search_path TO pg_catalog AS $f$
 SELECT evidence FROM population_top3000_202608_202609_v1.ctx07_recovery_evidence_v6 WHERE evidence->>'parser_version'='ctx07-jolts-byte-ascii-v8' AND evidence->>'status'='NUMERICAL_PUBLICATION_RECONSTRUCTION_VERIFIED' AND (evidence->>'reference_period')::timestamptz=p_reference AND (evidence->>'publication_supported_available_at')::timestamptz<=p_at ORDER BY (evidence->>'publication_supported_available_at')::timestamptz DESC LIMIT 1;$f$;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_bundle_v8()
RETURNS text LANGUAGE sql STABLE SET search_path TO pg_catalog AS $f$
 SELECT encode(sha256(convert_to(string_agg(pg_get_functiondef(p.oid),E'\n' ORDER BY p.proname),'UTF8')),'hex') FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='population_top3000_202608_202609_v1' AND p.proname IN('ctx07_jolts_scope_v8','ctx07_jolts_context_v8','ctx07_jolts_extract_v8','ctx07_jolts_process_v8','ctx07_recovery_normalize_v8','ctx07_recovery_derive_v8','ctx07_recovery_validate_v8','ctx07_jolts_asof_v8');$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_scope_v8() FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_context_v8(uuid) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_extract_v8(bytea,text,text,timestamptz,timestamptz) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_process_v8(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_normalize_v8(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_derive_v8(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_recovery_validate_v8(uuid,text) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_asof_v8(timestamptz,timestamptz) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ctx07_jolts_bundle_v8() FROM PUBLIC,anon,authenticated;
