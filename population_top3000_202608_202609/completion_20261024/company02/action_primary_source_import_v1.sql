-- Root-only import of four exact already-acquired public primary corporate-action documents. No external requests.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.action_primary_source_import_v1(
 p_owner text,p_fence bigint,p_stop bigint,p_source_id text,p_body text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $function$
DECLARE catalog constant jsonb:=$catalog$[{"source_id":"action-primary-v1:b7fa03798cc1ce1ad12eec6716e331deb9f567b979e229f5a826723b5cd8c1e5","source_uri":"https://ir.tenaris.com/news-releases/news-release-details/tenaris-announces-2026-second-quarter-results","source_provider":"TENARIS_IR","content_path":"/workspace/scratch/5db48b32d103/company02/N02_TS_Q2_20260805.htm","content_bytes":299838,"source_sha256":"b7fa03798cc1ce1ad12eec6716e331deb9f567b979e229f5a826723b5cd8c1e5","retrieved_at":"2026-10-24T04:25:28.370717+00:00","current_receipt":{"request_key":"company02:n02-ts-payment:79e73028ea83d23dc1e739bdbc2b8c0dea52af4bcb8144f07fce3b7dbb0aef2a","url":"https://ir.tenaris.com/news-releases/news-release-details/tenaris-announces-2026-second-quarter-results","http_status":200,"bytes":299838,"sha256":"b7fa03798cc1ce1ad12eec6716e331deb9f567b979e229f5a826723b5cd8c1e5","acquired_at":"2026-10-24T04:25:28.370717+00:00","elapsed_seconds":6.905748029003007,"headers":{"content-type":"text/html; charset=UTF-8","last-modified":"Wed, 14 Oct 2026 22:30:12 GMT","date":"Sat, 24 Oct 2026 04:25:28 GMT"},"logical_provider_calls":1,"client_http_invocations_observed":1,"physical_upstream_http_count":null,"historical_received_at":null,"original_receipt_proven":false,"source_publication_at":null,"source_body":"/workspace/scratch/5db48b32d103/company02/N02_TS_Q2_20260805.htm"}},{"source_id":"action-primary-v1:78201a2b3c211c8030b752ca5ea9e6f43ccb5da941f7e98f77bc2c71d033631b","source_uri":"https://www.nasdaqtrader.com/TraderNews.aspx?id=eca2026-542","source_provider":"NASDAQ","content_path":"/workspace/scratch/5db48b32d103/options05/private_nasdaq_eca2026_542.html","content_bytes":58466,"source_sha256":"78201a2b3c211c8030b752ca5ea9e6f43ccb5da941f7e98f77bc2c71d033631b","retrieved_at":"2026-10-24T05:06:40.961504+00:00","current_receipt":{"source_key":"nasdaq_eca2026_542","request_url":"https://www.nasdaqtrader.com/TraderNews.aspx?id=eca2026-542","request_started_at":"2026-10-24T05:06:36.164988+00:00","response_received_at":"2026-10-24T05:06:40.961504+00:00","http_status":200,"body_bytes":58466,"body_sha256":"78201a2b3c211c8030b752ca5ea9e6f43ccb5da941f7e98f77bc2c71d033631b","elapsed_seconds":4.796569808997447,"private_artifact_path":"/workspace/scratch/5db48b32d103/options05/private_nasdaq_eca2026_542.html","original_historical_receipt_proven":false,"source_publication_timestamp_proven":false,"http_headers":{"content-type":"text/html; charset=utf-8","date":"Sat, 24 Oct 2026 05:06:40 GMT"}}},{"source_id":"action-primary-v1:87cf4f3029d1bf16a88dfa4225b5342ae4f0c099af6da4b1f0f9a7abf29c671e","source_uri":"https://www.sec.gov/Archives/edgar/data/1896425/000149315226036118/form6-k.htm","source_provider":"SEC","content_path":"/workspace/scratch/5db48b32d103/options05/private_sec_lbgj_6k_20260805.html","content_bytes":26363,"source_sha256":"87cf4f3029d1bf16a88dfa4225b5342ae4f0c099af6da4b1f0f9a7abf29c671e","retrieved_at":"2026-10-24T05:06:46.487617+00:00","current_receipt":{"source_key":"sec_lbgj_6k_20260805","request_url":"https://www.sec.gov/Archives/edgar/data/1896425/000149315226036118/form6-k.htm","request_started_at":"2026-10-24T05:06:40.961596+00:00","response_received_at":"2026-10-24T05:06:46.487617+00:00","http_status":200,"body_bytes":26363,"body_sha256":"87cf4f3029d1bf16a88dfa4225b5342ae4f0c099af6da4b1f0f9a7abf29c671e","elapsed_seconds":5.526063395001984,"private_artifact_path":"/workspace/scratch/5db48b32d103/options05/private_sec_lbgj_6k_20260805.html","original_historical_receipt_proven":false,"source_publication_timestamp_proven":false,"http_headers":{"content-type":"text/html","last-modified":"Wed, 05 Aug 2026 13:05:33 GMT","date":"Sat, 24 Oct 2026 05:06:46 GMT"}}},{"source_id":"action-primary-v1:bd2e263472c9c2cb39890662a670e381741c2b796a1109b904a7efeb38fc0de6","source_uri":"https://www.sec.gov/Archives/edgar/data/1896425/000149315226036118/ex99-1.htm","source_provider":"SEC","content_path":"/workspace/scratch/5db48b32d103/options05/private_sec_lbgj_press_20260729.html","content_bytes":12640,"source_sha256":"bd2e263472c9c2cb39890662a670e381741c2b796a1109b904a7efeb38fc0de6","retrieved_at":"2026-10-24T05:06:51.847502+00:00","current_receipt":{"source_key":"sec_lbgj_press_20260729","request_url":"https://www.sec.gov/Archives/edgar/data/1896425/000149315226036118/ex99-1.htm","request_started_at":"2026-10-24T05:06:46.487689+00:00","response_received_at":"2026-10-24T05:06:51.847502+00:00","http_status":200,"body_bytes":12640,"body_sha256":"bd2e263472c9c2cb39890662a670e381741c2b796a1109b904a7efeb38fc0de6","elapsed_seconds":5.359845302002213,"private_artifact_path":"/workspace/scratch/5db48b32d103/options05/private_sec_lbgj_press_20260729.html","original_historical_receipt_proven":false,"source_publication_timestamp_proven":false,"http_headers":{"content-type":"text/html","last-modified":"Wed, 05 Aug 2026 13:05:33 GMT","date":"Sat, 24 Oct 2026 05:06:51 GMT"}}}]$catalog$::jsonb;
 spec jsonb;c population_top3000_202608_202609_v1.acquisition_control%rowtype;
 old population_top3000_202608_202609_v1.source_document%rowtype;b bigint;h text;
BEGIN
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 SELECT * INTO STRICT c FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 IF c.owner_id IS DISTINCT FROM p_owner OR c.owner_fence IS DISTINCT FROM p_fence OR c.stop_generation IS DISTINCT FROM p_stop
 OR NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton AND enabled)
 THEN RAISE EXCEPTION 'ACTION_SOURCE_CURRENT_CONTROLLER_REQUIRED';END IF;
 SELECT value INTO spec FROM jsonb_array_elements(catalog) WHERE value->>'source_id'=p_source_id;
 IF spec IS NULL THEN RAISE EXCEPTION 'ACTION_SOURCE_PINNED_CATALOG_REQUIRED';END IF;
 b:=octet_length(p_body);h:=encode(sha256(convert_to(p_body,'UTF8')),'hex');
 IF b IS NULL OR b NOT BETWEEN 1 AND 524288 OR b IS DISTINCT FROM (spec->>'content_bytes')::bigint OR h IS DISTINCT FROM spec->>'source_sha256'
 THEN RAISE EXCEPTION 'ACTION_SOURCE_EXACT_BYTES_REQUIRED';END IF;
 SELECT * INTO old FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p_source_id;
 IF FOUND THEN
  IF old.source_sha256 IS DISTINCT FROM h OR old.content_bytes IS DISTINCT FROM b OR old.content_text IS DISTINCT FROM p_body
   OR old.source_uri IS DISTINCT FROM spec->>'source_uri' OR old.source_provider IS DISTINCT FROM spec->>'source_provider'
   OR old.historical_received_at IS NOT NULL OR old.source_published_at IS NOT NULL
   OR old.retrieved_at IS DISTINCT FROM (spec->>'retrieved_at')::timestamptz
   OR old.license_policy IS DISTINCT FROM 'PUBLIC_PRIMARY_SOURCE_ATTRIBUTION_PRIVATE_MISSION_USE'
   OR old.evidence->'current_receipt' IS DISTINCT FROM spec->'current_receipt'
   OR old.evidence->>'representation' IS DISTINCT FROM 'EXACT_CURRENT_HTTP_RESPONSE_BODY_UTF8'
   OR old.evidence->'strict_original_receipt_qualified' IS DISTINCT FROM 'false'::jsonb
   OR old.evidence->'source_publication_instant_inferred' IS DISTINCT FROM 'false'::jsonb
   OR old.evidence->'source_complete' IS DISTINCT FROM 'false'::jsonb
   OR old.evidence->'private_mission_use_only' IS DISTINCT FROM 'true'::jsonb
   OR old.evidence->'public_republication_license_inferred' IS DISTINCT FROM 'false'::jsonb
  THEN RAISE EXCEPTION 'ACTION_SOURCE_IMMUTABLE_REPLAY_CONFLICT';END IF;
  RETURN jsonb_build_object('state','SOURCE_REUSED','source_id',p_source_id,'new_source_bytes',0);
 END IF;
 IF c.stored_bytes+c.reserved_bytes+b>c.max_source_bytes THEN RAISE EXCEPTION 'ACTION_SOURCE_FINITE_STORAGE_BOUND';END IF;
 INSERT INTO population_top3000_202608_202609_v1.source_document
  (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
 VALUES(p_source_id,h,spec->>'source_uri',spec->>'source_provider',(spec->>'retrieved_at')::timestamptz,NULL,NULL,p_body,b,
  jsonb_build_object('representation','EXACT_CURRENT_HTTP_RESPONSE_BODY_UTF8','current_receipt',spec->'current_receipt',
   'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'scope_sha256',c.scope_sha256,'current_database_transfer_at',clock_timestamp(),
   'source_complete',false,'strict_original_receipt_qualified',false,'source_publication_instant_inferred',false,
   'new_provider_requests_during_import',0,'new_http_invocations_during_import',0,'acquisition_accounting','SEPARATE_EXACTLY_ONCE_RETAINED_REQUEST_LEDGER',
   'private_mission_use_only',true,'public_republication_license_inferred',false),'PUBLIC_PRIMARY_SOURCE_ATTRIBUTION_PRIVATE_MISSION_USE');
 UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+b,last_useful_progress_at=clock_timestamp(),updated_at=clock_timestamp() WHERE singleton;
 INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('ACTION_PRIMARY_SOURCE_RETAINED_V1',jsonb_build_object(
  'source_id',p_source_id,'source_sha256',h,'new_source_bytes',b,'provider_requests',0,'http_invocations',0,
  'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,'scope_sha256',c.scope_sha256,'original_historical_receipt_inferred',false));
 PERFORM population_top3000_202608_202609_v1.ref01_adjudication_controller_v3(false);
 RETURN jsonb_build_object('state','SOURCE_COMMITTED','source_id',p_source_id,'sha256',h,'new_source_bytes',b,'new_provider_requests',0,'new_http_invocations',0);
END;
$function$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.action_primary_source_import_v1(text,bigint,bigint,text,text) FROM PUBLIC,anon,authenticated;
