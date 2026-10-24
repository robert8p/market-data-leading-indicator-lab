-- Exact six retained SEC archive members only; no provider calls or population acceptance.
CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.ref01_retained_archive_member_import_v6(
 p_owner text,p_fence bigint,p_stop bigint,p_source_id text,p_part integer,p_body text)
RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'pg_catalog' AS $f$
DECLARE catalog constant jsonb:=$catalog$[{"source_id":"ref01-sec-ncen:2025q4:FUND_REPORTED_INFO.tsv","source_uri":"https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2025q4_ncen.zip","source_sha256":"d6c23cbb23b1480cec5403cf25f4fdc373286f84824e41a80978eb158b9e0847","content_bytes":756677,"archive_sha256":"edbd91ac464203625dda4f00ec07083d06ebbf5f04194673b13a3362ee1c1476","archive_bytes":7571694,"archive_member":"FUND_REPORTED_INFO.tsv","anchor_source_id":"ref01-sec-ncen:2025q4:REGISTRANT.tsv","anchor_sha256":"21443790b21b039f957a20ce0956aa1641bc1d459afc2b07a908f26f8ad39d46","anchor_bytes":112807,"parts":[{"part_number":0,"bytes":250000,"sha256":"b287213a07342038dc0d82f3a8a95f4742bff70865716bb7084fe8d78b0f0964"},{"part_number":1,"bytes":250000,"sha256":"d7bac1d78dd26a8533c0a07d7dc7a9f4c2fbfc47f495d57cbbb7ac63344bf1e0"},{"part_number":2,"bytes":250000,"sha256":"d8445e3d3b26f91a9c32b90b73e4fd2107f62704a103e0959e0abdccbcd5cd4a"},{"part_number":3,"bytes":6677,"sha256":"fe94b11a5e9ecabebb55b514a6faf26fe1a4d6c4a68f7acd3885488f47a4fbc8"}]},{"source_id":"ref01-sec-ncen:2025q4:SECURITY_RELATED_ITEM.tsv","source_uri":"https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2025q4_ncen.zip","source_sha256":"937e7dcf0a3dff42a284e3af1bbdf5a3daae66e445748b020afc06384fe09deb","content_bytes":19449,"archive_sha256":"edbd91ac464203625dda4f00ec07083d06ebbf5f04194673b13a3362ee1c1476","archive_bytes":7571694,"archive_member":"SECURITY_RELATED_ITEM.tsv","anchor_source_id":"ref01-sec-ncen:2025q4:REGISTRANT.tsv","anchor_sha256":"21443790b21b039f957a20ce0956aa1641bc1d459afc2b07a908f26f8ad39d46","anchor_bytes":112807,"parts":[{"part_number":0,"bytes":19449,"sha256":"937e7dcf0a3dff42a284e3af1bbdf5a3daae66e445748b020afc06384fe09deb"}]},{"source_id":"ref01-sec-ncen:2026q2:FUND_REPORTED_INFO.tsv","source_uri":"https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2026q2_ncen.zip","source_sha256":"d57b791aca3109f87be0ab6ddc4ce5386ca00c8fd40b919339c6652d5a9f7067","content_bytes":710985,"archive_sha256":"97899fef613a151faf42b6b01573bde3d22db81af7f896598c43a62cec0c7614","archive_bytes":8404110,"archive_member":"FUND_REPORTED_INFO.tsv","anchor_source_id":"ref01-sec-ncen:2026q2:REGISTRANT.tsv","anchor_sha256":"a738b558f882fb84d766bca2c13b353dea176033b832fe2ed25d341220ed4383","anchor_bytes":112560,"parts":[{"part_number":0,"bytes":250000,"sha256":"16c81d8d3480fd1650f6f9ffc7ab099bb3e18ee6f7af277209769c20e090b89a"},{"part_number":1,"bytes":250000,"sha256":"ba5208778240df217a29f0073025e7135f7a4fcbbbf5a7a07f2d809e37c1c178"},{"part_number":2,"bytes":210985,"sha256":"aa3b33f609a9bf4e457c07053af6af33a753eda2b18e0fdfe9762fb883e75bfc"}]},{"source_id":"ref01-sec-ncen:2026q2:SECURITY_RELATED_ITEM.tsv","source_uri":"https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2026q2_ncen.zip","source_sha256":"5eda5281821901fd7972cdd4076d15a992bca60e7b07dc9cd1026c2169ddf861","content_bytes":36457,"archive_sha256":"97899fef613a151faf42b6b01573bde3d22db81af7f896598c43a62cec0c7614","archive_bytes":8404110,"archive_member":"SECURITY_RELATED_ITEM.tsv","anchor_source_id":"ref01-sec-ncen:2026q2:REGISTRANT.tsv","anchor_sha256":"a738b558f882fb84d766bca2c13b353dea176033b832fe2ed25d341220ed4383","anchor_bytes":112560,"parts":[{"part_number":0,"bytes":36457,"sha256":"5eda5281821901fd7972cdd4076d15a992bca60e7b07dc9cd1026c2169ddf861"}]},{"source_id":"ref01-sec-ncen:2026q3:FUND_REPORTED_INFO.tsv","source_uri":"https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2026q3_ncen.zip","source_sha256":"37c204c965acab7ea5fef5637517fb21ddbb892a6ed61999454aff935942c571","content_bytes":713006,"archive_sha256":"4761f8614d42dc52892417d3ad5f809309a39573b1f44530aa549c36f2f6593d","archive_bytes":5871882,"archive_member":"FUND_REPORTED_INFO.tsv","anchor_source_id":"ref01-sec-ncen:2026q3:REGISTRANT.tsv","anchor_sha256":"8548eaeacbdc3357ac00b43049c715480df8b96e846eed851f3748667439b3e8","anchor_bytes":98030,"parts":[{"part_number":0,"bytes":250000,"sha256":"80c04464c327b1c04e716f12c23504595c31aeab4825b29857ebddafe6a5ed60"},{"part_number":1,"bytes":250000,"sha256":"d4521918e023c407f69ca0e2c6ecdd99ff03e1a951f5de70381c6dfed366484e"},{"part_number":2,"bytes":213006,"sha256":"41d7d7d49a6266eba59efbb0e84408d4db0c2a87e055fd8a295d6399d805de28"}]},{"source_id":"ref01-sec-ncen:2026q3:SECURITY_RELATED_ITEM.tsv","source_uri":"https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2026q3_ncen.zip","source_sha256":"61594a6890cdd262466f261c0a0f8ff9f8cefa22de52f3c592c23450435b08a1","content_bytes":13652,"archive_sha256":"4761f8614d42dc52892417d3ad5f809309a39573b1f44530aa549c36f2f6593d","archive_bytes":5871882,"archive_member":"SECURITY_RELATED_ITEM.tsv","anchor_source_id":"ref01-sec-ncen:2026q3:REGISTRANT.tsv","anchor_sha256":"8548eaeacbdc3357ac00b43049c715480df8b96e846eed851f3748667439b3e8","anchor_bytes":98030,"parts":[{"part_number":0,"bytes":13652,"sha256":"61594a6890cdd262466f261c0a0f8ff9f8cefa22de52f3c592c23450435b08a1"}]}]$catalog$::jsonb;
 a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old population_top3000_202608_202609_v1.ref01_ncen_member_chunk_v1%rowtype;
 doc population_top3000_202608_202609_v1.source_document%rowtype;
 spec jsonb;part jsonb;n integer;whole text;b integer;h text;fresh bigint:=0;expected integer;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF p_owner IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR p_fence IS DISTINCT FROM 3 OR p_stop IS DISTINCT FROM 1
 OR a.owner_id IS DISTINCT FROM p_owner OR a.owner_fence IS DISTINCT FROM p_fence OR a.stop_generation IS DISTINCT FROM p_stop
 OR p.expected_owner_id IS DISTINCT FROM p_owner OR p.expected_owner_fence IS DISTINCT FROM p_fence OR p.expected_stop_generation IS DISTINCT FROM p_stop
 OR a.enabled IS DISTINCT FROM true OR a.stopped IS DISTINCT FROM false OR a.hold_reason IS NOT NULL OR p.enabled IS DISTINCT FROM true
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.budget_evidence#>>'{monetary_policy,authority_event_id}' IS DISTINCT FROM '9528'
 THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_CURRENT_AUTHORITY_REQUIRED';END IF;
 IF encode(sha256(convert_to(pg_get_functiondef('population_top3000_202608_202609_v1.d02_daily_resource_guard_v2()'::regprocedure),'UTF8')),'hex') IS DISTINCT FROM '08cfdd49924160857830640a037d822ac1ed5046ea798caef1e4b3b49c8aa589' THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_RESOURCE_GUARD_PREIMAGE_CHANGED';END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 SELECT value INTO spec FROM jsonb_array_elements(catalog) WHERE value->>'source_id'=p_source_id;
 IF spec IS NULL THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_PINNED_SOURCE_REQUIRED';END IF;
 SELECT value INTO part FROM jsonb_array_elements(spec->'parts') WHERE (value->>'part_number')::integer=p_part;
 b:=octet_length(p_body);h:=encode(sha256(convert_to(p_body,'UTF8')),'hex');expected:=jsonb_array_length(spec->'parts');
 IF part IS NULL OR p_body IS NULL OR b IS DISTINCT FROM (part->>'bytes')::integer OR h IS DISTINCT FROM part->>'sha256'
 THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_PINNED_PART_REQUIRED';END IF;
 IF NOT EXISTS(SELECT 1 FROM population_top3000_202608_202609_v1.source_document s
  WHERE s.source_id=spec->>'anchor_source_id' AND s.source_provider='SEC' AND s.source_uri=spec->>'source_uri'
  AND s.source_sha256=spec->>'anchor_sha256' AND s.content_bytes=(spec->>'anchor_bytes')::bigint
  AND s.content_bytes=octet_length(s.content_text) AND s.source_sha256=encode(sha256(convert_to(s.content_text,'UTF8')),'hex')
  AND s.evidence->>'archive_sha256'=spec->>'archive_sha256')
 THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_EXISTING_ARCHIVE_ANCHOR_REQUIRED';END IF;
 SELECT * INTO doc FROM population_top3000_202608_202609_v1.source_document WHERE source_id=p_source_id;
 IF FOUND THEN
  IF doc.source_sha256 IS DISTINCT FROM spec->>'source_sha256' OR doc.content_bytes IS DISTINCT FROM (spec->>'content_bytes')::bigint
   OR doc.content_bytes IS DISTINCT FROM octet_length(doc.content_text) OR doc.source_sha256 IS DISTINCT FROM encode(sha256(convert_to(doc.content_text,'UTF8')),'hex')
   OR doc.source_provider IS DISTINCT FROM 'SEC' OR doc.source_uri IS DISTINCT FROM spec->>'source_uri'
   OR doc.evidence->>'archive_sha256' IS DISTINCT FROM spec->>'archive_sha256'
   OR doc.evidence->>'archive_member' IS DISTINCT FROM spec->>'archive_member'
   OR doc.source_published_at IS NOT NULL OR doc.historical_received_at IS NOT NULL
  THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_EXISTING_SOURCE_CONFLICT';END IF;
  RETURN jsonb_build_object('state','SOURCE_REUSED','source_id',p_source_id,'new_stored_bytes',0);
 END IF;
 SELECT * INTO old FROM population_top3000_202608_202609_v1.ref01_ncen_member_chunk_v1 WHERE source_id=p_source_id AND part_number=p_part;
 IF FOUND THEN
  IF old.full_source_sha256 IS DISTINCT FROM spec->>'source_sha256' OR old.content_text IS DISTINCT FROM p_body
   OR old.content_sha256 IS DISTINCT FROM h OR old.content_bytes IS DISTINCT FROM b
   OR old.owner_id IS DISTINCT FROM p_owner OR old.owner_fence IS DISTINCT FROM p_fence OR old.stop_generation IS DISTINCT FROM p_stop
  THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_CHUNK_REPLAY_CONFLICT';END IF;
 ELSE
  IF a.stored_bytes+a.reserved_bytes+b>a.max_source_bytes THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_STORAGE_BOUND';END IF;
  INSERT INTO population_top3000_202608_202609_v1.ref01_ncen_member_chunk_v1(source_id,part_number,full_source_sha256,content_text,content_bytes,content_sha256,owner_id,owner_fence,stop_generation)
  VALUES(p_source_id,p_part,spec->>'source_sha256',p_body,b,h,p_owner,p_fence,p_stop);fresh:=b;
 END IF;
 SELECT count(*),string_agg(content_text,'' ORDER BY part_number) INTO n,whole
 FROM population_top3000_202608_202609_v1.ref01_ncen_member_chunk_v1 WHERE source_id=p_source_id;
 IF n>expected THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_PART_COUNT_CONFLICT';END IF;
 IF n=expected THEN
  IF octet_length(whole) IS DISTINCT FROM (spec->>'content_bytes')::integer OR encode(sha256(convert_to(whole,'UTF8')),'hex') IS DISTINCT FROM spec->>'source_sha256'
  THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_ASSEMBLED_BODY_CONFLICT';END IF;
  fresh:=fresh+octet_length(whole);
  IF a.stored_bytes+a.reserved_bytes+fresh>a.max_source_bytes THEN RAISE EXCEPTION 'ARCHIVE_MEMBER_STORAGE_BOUND';END IF;
  INSERT INTO population_top3000_202608_202609_v1.source_document(source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_bytes,evidence,license_policy)
  VALUES(p_source_id,spec->>'source_sha256',spec->>'source_uri','SEC',clock_timestamp(),NULL,NULL,whole,octet_length(whole),jsonb_build_object(
   'representation','EXACT_UNCOMPRESSED_ZIP_MEMBER','archive_sha256',spec->>'archive_sha256','archive_bytes',(spec->>'archive_bytes')::bigint,
   'archive_member',spec->>'archive_member','retained_archive_anchor_source_id',spec->>'anchor_source_id',
   'new_provider_requests',0,'original_historical_receipt_proven',false,'source_complete',false,'strict_original_receipt_qualified',false,
   'known_archive_limitation','SEC archive excludes schema3.1 submissions; absence is not classification evidence.'),'PUBLIC_SEC_DATA_WITH_SOURCE_ATTRIBUTION');
 END IF;
 IF fresh>0 THEN
  UPDATE population_top3000_202608_202609_v1.acquisition_control SET stored_bytes=stored_bytes+fresh,updated_at=clock_timestamp() WHERE singleton;
  INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('CONTINUITY_RETAINED_ARCHIVE_MEMBER_V6',jsonb_build_object(
   'source_id',p_source_id,'part_number',p_part,'retained_parts',n,'expected_parts',expected,'source_sha256',spec->>'source_sha256',
   'new_stored_bytes',fresh,'provider_requests',0,'http_invocations',0,'owner_id',p_owner,'owner_fence',p_fence,'stop_generation',p_stop,
   'scope_sha256',a.scope_sha256,'complete_source',n=expected,'full_population_accepted',false));
 END IF;
 PERFORM population_top3000_202608_202609_v1.d02_daily_resource_guard_v2();
 RETURN jsonb_build_object('state',CASE WHEN n=expected THEN 'SOURCE_RETAINED' ELSE 'PART_RETAINED' END,'source_id',p_source_id,'retained_parts',n,'expected_parts',expected,'new_stored_bytes',fresh,'full_population_accepted',false);
END;$f$;
REVOKE ALL ON FUNCTION population_top3000_202608_202609_v1.ref01_retained_archive_member_import_v6(text,bigint,bigint,text,integer,text) FROM PUBLIC,anon,authenticated;
