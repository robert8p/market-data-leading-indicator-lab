-- Root-only exact retained corporate-action primary-document chunk staging; no network or new data scope.
BEGIN;
SET LOCAL lock_timeout='15s'; SET LOCAL statement_timeout='30s';
SELECT population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
CREATE TABLE IF NOT EXISTS population_top3000_202608_202609_v1.action_primary_chunk_v1(
 source_id text NOT NULL,part_number integer NOT NULL CHECK(part_number BETWEEN 0 AND 15),
 full_source_sha256 text NOT NULL CHECK(full_source_sha256 ~ '^[0-9a-f]{64}$'),
 content_text text NOT NULL,content_bytes integer NOT NULL CHECK(content_bytes BETWEEN 1 AND 262144),
 content_sha256 text NOT NULL CHECK(content_sha256 ~ '^[0-9a-f]{64}$'),
 owner_id text NOT NULL,owner_fence bigint NOT NULL,stop_generation bigint NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(source_id,part_number),
 CHECK(content_bytes=octet_length(content_text)),
 CHECK(content_sha256=encode(sha256(convert_to(content_text,'UTF8')),'hex'))
);
ALTER TABLE population_top3000_202608_202609_v1.action_primary_chunk_v1 ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE population_top3000_202608_202609_v1.action_primary_chunk_v1 FROM PUBLIC,anon,authenticated;
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.action_primary_chunk_import_v1(
 p_owner text,p_fence bigint,p_stop bigint,p_source_id text,p_part integer,p_body text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE catalog constant jsonb:=$catalog$[{"source_id":"action-primary-v1:b7fa03798cc1ce1ad12eec6716e331deb9f567b979e229f5a826723b5cd8c1e5","content_bytes":299838,"source_sha256":"b7fa03798cc1ce1ad12eec6716e331deb9f567b979e229f5a826723b5cd8c1e5","source_uri":"https://ir.tenaris.com/news-releases/news-release-details/tenaris-announces-2026-second-quarter-results","source_provider":"TENARIS_IR","chunks":[{"part_number":0,"bytes":250000,"sha256":"1de1493c4b55e3cd8de6230f7ef727b740b3e754485d1eb12c44caa2efbbd149"},{"part_number":1,"bytes":49838,"sha256":"155606165d2f070e0b0d7156194b8461e0fb4ff96cfdd89f247c0d2c7f40838a"}]},{"source_id":"action-primary-v1:78201a2b3c211c8030b752ca5ea9e6f43ccb5da941f7e98f77bc2c71d033631b","content_bytes":58466,"source_sha256":"78201a2b3c211c8030b752ca5ea9e6f43ccb5da941f7e98f77bc2c71d033631b","source_uri":"https://www.nasdaqtrader.com/TraderNews.aspx?id=eca2026-542","source_provider":"NASDAQ","chunks":[{"part_number":0,"bytes":58466,"sha256":"78201a2b3c211c8030b752ca5ea9e6f43ccb5da941f7e98f77bc2c71d033631b"}]},{"source_id":"action-primary-v1:87cf4f3029d1bf16a88dfa4225b5342ae4f0c099af6da4b1f0f9a7abf29c671e","content_bytes":26363,"source_sha256":"87cf4f3029d1bf16a88dfa4225b5342ae4f0c099af6da4b1f0f9a7abf29c671e","source_uri":"https://www.sec.gov/Archives/edgar/data/1896425/000149315226036118/form6-k.htm","source_provider":"SEC","chunks":[{"part_number":0,"bytes":26363,"sha256":"87cf4f3029d1bf16a88dfa4225b5342ae4f0c099af6da4b1f0f9a7abf29c671e"}]},{"source_id":"action-primary-v1:bd2e263472c9c2cb39890662a670e381741c2b796a1109b904a7efeb38fc0de6","content_bytes":12640,"source_sha256":"bd2e263472c9c2cb39890662a670e381741c2b796a1109b904a7efeb38fc0de6","source_uri":"https://www.sec.gov/Archives/edgar/data/1896425/000149315226036118/ex99-1.htm","source_provider":"SEC","chunks":[{"part_number":0,"bytes":12640,"sha256":"bd2e263472c9c2cb39890662a670e381741c2b796a1109b904a7efeb38fc0de6"}]}]$catalog$::jsonb;
 spec jsonb;part jsonb;c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 old population_top3000_202608_202609_v1.action_primary_chunk_v1%rowtype;
 doc population_top3000_202608_202609_v1.source_document%rowtype;
 b bigint;h text;whole text;result jsonb;new_staging_bytes bigint:=0;n integer;
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 SELECT * INTO STRICT c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF c.owner_id IS DISTINCT FROM p_owner OR c.owner_fence IS DISTINCT FROM p_fence OR c.stop_generation IS DISTINCT FROM p_stop
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND enabled)
 THEN RAISE EXCEPTION 'ACTION_CHUNK_CURRENT_OWNER_REQUIRED';END IF;
 SELECT value INTO spec FROM jsonb_array_elements(catalog) WHERE value->>'source_id'=p_source_id;
 IF spec IS NULL THEN RAISE EXCEPTION 'ACTION_CHUNK_PINNED_SOURCE_REQUIRED';END IF;
 SELECT value INTO part FROM jsonb_array_elements(spec->'chunks') WHERE (value->>'part_number')::integer=p_part;
 b:=octet_length(p_body);h:=encode(sha256(convert_to(p_body,'UTF8')),'hex');
 IF part IS NULL OR b IS NULL OR b NOT BETWEEN 1 AND 262144 OR b IS DISTINCT FROM (part->>'bytes')::bigint OR h IS DISTINCT FROM part->>'sha256'
 THEN RAISE EXCEPTION 'ACTION_CHUNK_EXACT_PIN_REQUIRED';END IF;
 SELECT * INTO doc FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p_source_id;
 IF FOUND THEN
  IF doc.source_sha256 IS DISTINCT FROM spec->>'source_sha256' OR doc.content_bytes IS DISTINCT FROM (spec->>'content_bytes')::bigint
   OR encode(sha256(convert_to(doc.content_text,'UTF8')),'hex') IS DISTINCT FROM spec->>'source_sha256'
   OR doc.content_bytes IS DISTINCT FROM octet_length(doc.content_text) OR doc.source_provider IS DISTINCT FROM spec->>'source_provider'
   OR doc.source_uri IS DISTINCT FROM spec->>'source_uri'
   OR doc.historical_received_at IS NOT NULL OR doc.source_published_at IS NOT NULL
  THEN RAISE EXCEPTION 'ACTION_CHUNK_FINAL_SOURCE_CONFLICT';END IF;
  PERFORM population_top3000_202608_202609_v1.action_primary_source_import_v1(p_owner,p_fence,p_stop,p_source_id,doc.content_text);
  RETURN jsonb_build_object('state','SOURCE_REUSED','source_id',p_source_id,'new_staging_bytes',0,'new_source_bytes',0);
 END IF;
 SELECT * INTO old FROM population_top3000_202608_202609_v1.action_primary_chunk_v1 WHERE source_id=p_source_id AND part_number=p_part;
 IF FOUND THEN
  IF old.full_source_sha256 IS DISTINCT FROM spec->>'source_sha256' OR old.content_bytes IS DISTINCT FROM b OR old.content_sha256 IS DISTINCT FROM h OR old.content_text IS DISTINCT FROM p_body
   OR old.owner_id IS DISTINCT FROM p_owner OR old.owner_fence IS DISTINCT FROM p_fence OR old.stop_generation IS DISTINCT FROM p_stop
  THEN RAISE EXCEPTION 'ACTION_CHUNK_IMMUTABLE_REPLAY_CONFLICT';END IF;
 ELSE
  IF c.stored_bytes+c.reserved_bytes+b>c.max_source_bytes THEN RAISE EXCEPTION 'ACTION_CHUNK_FINITE_STORAGE_BOUND';END IF;
  INSERT INTO population_top3000_202608_202609_v1.action_primary_chunk_v1(source_id,part_number,full_source_sha256,content_text,content_bytes,content_sha256,owner_id,owner_fence,stop_generation)
  VALUES(p_source_id,p_part,spec->>'source_sha256',p_body,b,h,p_owner,p_fence,p_stop);
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+b,updated_at=clock_timestamp() WHERE singleton;
  new_staging_bytes:=b;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ACTION_PRIMARY_EXACT_CHUNK_RETAINED_V1',jsonb_build_object(
   'source_id',p_source_id,'part_number',p_part,'part_sha256',h,'new_staging_bytes',b,'full_source_sha256',spec->>'source_sha256',
   'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'scope_sha256',c.scope_sha256,
   'provider_requests',0,'http_invocations',0,'budget_basis','IMMUTABLE_STAGING_BYTES_CHARGED_ONCE_SEPARATELY_FROM_FINAL_SOURCE_BYTES','source_complete',false));
 END IF;
 SELECT count(*),string_agg(content_text,'' ORDER BY part_number) INTO n,whole
 FROM population_top3000_202608_202609_v1.action_primary_chunk_v1 WHERE source_id=p_source_id;
 IF n=jsonb_array_length(spec->'chunks') THEN
  IF octet_length(whole) IS DISTINCT FROM (spec->>'content_bytes')::bigint OR encode(sha256(convert_to(whole,'UTF8')),'hex') IS DISTINCT FROM spec->>'source_sha256'
  THEN RAISE EXCEPTION 'ACTION_CHUNK_FINAL_EXACT_HASH_FAILED';END IF;
  result:=population_top3000_202608_202609_v1.action_primary_source_import_v1(p_owner,p_fence,p_stop,p_source_id,whole);
 ELSE result:=jsonb_build_object('state','CHUNK_RETAINED','source_id',p_source_id,'retained_parts',n,'expected_parts',jsonb_array_length(spec->'chunks'));END IF;
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 RETURN result||jsonb_build_object('new_staging_bytes',new_staging_bytes,'staging_and_source_budget_separate',true,'new_provider_requests',0,'new_http_invocations',0);
END;
$function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.action_primary_chunk_import_v1(text,bigint,bigint,text,integer,text) FROM PUBLIC,anon,authenticated;
COMMIT;
