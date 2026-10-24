-- Root-controller-only authoritative component records. No family validation/pointer/hold changes.
BEGIN;
SET LOCAL lock_timeout='5s';SET LOCAL statement_timeout='20s';
DO $accept$
DECLARE a population_top3000_202608_202609_v1.acquisition_control%rowtype;
 p population_top3000_202608_202609_v1.pipeline_control%rowtype;
 old_controls jsonb;n bigint;h text;bad bigint;item jsonb;prior jsonb;
 source_pins jsonb:='[{"id":"ctx02-connector-sample:contracts_aapl:045cab853e491756c8236a4e866adc510849ad11e74c76e943dc62db8fa0a9dc","hash":"045cab853e491756c8236a4e866adc510849ad11e74c76e943dc62db8fa0a9dc","bytes":11884},{"id":"ctx02-connector-sample:contracts_adjusted:b1c1c9a018b8a68d6bb7e0f93620f6d921f6206c0e212362753b3f1e0ee1f605","hash":"b1c1c9a018b8a68d6bb7e0f93620f6d921f6206c0e212362753b3f1e0ee1f605","bytes":1314},{"id":"ctx02-connector-sample:contracts_new:381804c58c61036c59831355ad8a678522f198f68f690b7b0f48c72900960606","hash":"381804c58c61036c59831355ad8a678522f198f68f690b7b0f48c72900960606","bytes":1741},{"id":"ctx02-connector-sample:bars_expired:e6b0b9d7108d808bbd9c55c789b9f935f62814b82b1b705a8679379b3607c4f4","hash":"e6b0b9d7108d808bbd9c55c789b9f935f62814b82b1b705a8679379b3607c4f4","bytes":178},{"id":"ctx02-connector-sample:bars_introduced:ef019c0f6c85c3cb96d58257f3825d9704eb4031591c29fe05c26ff91635f6b0","hash":"ef019c0f6c85c3cb96d58257f3825d9704eb4031591c29fe05c26ff91635f6b0","bytes":520}]'::jsonb;
BEGIN
 PERFORM pg_advisory_xact_lock(2026083000261022);
 SELECT * INTO STRICT a FROM population_top3000_202608_202609_v1.acquisition_control WHERE singleton FOR UPDATE;
 SELECT * INTO STRICT p FROM population_top3000_202608_202609_v1.pipeline_control WHERE singleton FOR UPDATE;
 IF a.owner_id IS DISTINCT FROM 'top3000-completion-20261024-root-5db48b32d103' OR a.owner_fence IS DISTINCT FROM 3 OR a.stop_generation IS DISTINCT FROM 1
 OR a.stopped IS DISTINCT FROM false OR a.enabled IS DISTINCT FROM true OR p.enabled IS DISTINCT FROM true OR a.hold_reason IS NOT NULL
 OR a.scope_sha256 IS DISTINCT FROM '8e6ca1178f03822c34bb1b339818b34dcfdbf2a8642ef875e6e1af34dbf1bedd' OR p.scope_sha256 IS DISTINCT FROM a.scope_sha256
 OR p.expected_owner_id IS DISTINCT FROM a.owner_id OR p.expected_owner_fence IS DISTINCT FROM a.owner_fence OR p.expected_stop_generation IS DISTINCT FROM a.stop_generation
 OR a.admitted_until IS NULL OR a.admitted_until<=clock_timestamp() OR p.deadline IS NULL OR p.deadline<=clock_timestamp()
 OR p.shared_scheduler_verified IS DISTINCT FROM true OR p.tick_count>=p.max_ticks
 OR population_top3000_202608_202609_v1.pipeline_cost_admission_v2(p,0) IS DISTINCT FROM true
 THEN RAISE EXCEPTION 'COMPONENT_ACCEPTANCE_CURRENT_CONTROLLER_REQUIRED';END IF;
 old_controls:=jsonb_build_object('acquisition',to_jsonb(a),'pipeline',to_jsonb(p));
 -- Small SHARE locks pin the independently reviewed data throughout this evidence commit.
 LOCK TABLE population_top3000_202608_202609_v1.ctx02_normalized_sample_v1,
 population_top3000_202608_202609_v1.ctx02_sample_evidence_v1,
 population_top3000_202608_202609_v1.catalog_d06_preservation IN SHARE MODE;
 SELECT count(*) INTO bad FROM jsonb_array_elements(source_pins) z
 LEFT JOIN population_top3000_202608_202609_v1.source_document d ON d.source_id=z->>'id'
 WHERE d.source_sha256 IS DISTINCT FROM z->>'hash' OR d.content_bytes IS DISTINCT FROM (z->>'bytes')::bigint
 OR encode(extensions.digest(d.content_text,'sha256'),'hex') IS DISTINCT FROM z->>'hash'
 OR octet_length(d.content_text) IS DISTINCT FROM (z->>'bytes')::bigint
 OR d.source_published_at IS NOT NULL OR d.historical_received_at IS NOT NULL;
 IF bad<>0 THEN RAISE EXCEPTION 'CTX02_EXACT_FIVE_SOURCE_BYTES_REQUIRED';END IF;
 SELECT count(*),count(*) FILTER(WHERE record_sha256 IS DISTINCT FROM encode(extensions.digest(normalized::text,'sha256'),'hex')),
 encode(extensions.digest(string_agg(source_id||':'||source_row::text||':'||source_kind||':'||option_ticker||':'||observation_date::text||':'||coalesce(underlying_ticker,'')||':'||record_sha256,E'\n' ORDER BY source_id COLLATE "C",source_row),'sha256'),'hex')
 INTO n,bad,h FROM population_top3000_202608_202609_v1.ctx02_normalized_sample_v1
 WHERE source_id IN(SELECT z->>'id' FROM jsonb_array_elements(source_pins)z);
 IF n<>203 OR bad<>0 OR h IS DISTINCT FROM '2071e5b22387940e7e206ae8ec96abfd3fe801236eb553f9d08c0edbe202198c'
 THEN RAISE EXCEPTION 'CTX02_INDEPENDENTLY_VERIFIED_TYPED_OUTPUT_CHANGED';END IF;
 SELECT count(*),count(*) FILTER(WHERE evidence_sha256 IS DISTINCT FROM encode(extensions.digest(evidence::text,'sha256'),'hex')),
 encode(extensions.digest(string_agg(source_id||':'||evidence_sha256,E'\n' ORDER BY source_id COLLATE "C"),'sha256'),'hex')
 INTO n,bad,h FROM population_top3000_202608_202609_v1.ctx02_sample_evidence_v1
 WHERE source_id IN(SELECT z->>'id' FROM jsonb_array_elements(source_pins)z);
 IF n<>5 OR bad<>0 OR h IS DISTINCT FROM '061e2dc293d0b76c93986c8d923022ed887dfab8230fd5ff54e61d02e1b3ba83'
 THEN RAISE EXCEPTION 'CTX02_REVIEWED_DERIVED_EVIDENCE_CHANGED';END IF;
 SELECT count(*),count(*) FILTER(WHERE to_regclass(relation_name) IS NULL OR metadata_sha256 IS DISTINCT FROM encode(extensions.digest(metadata_text,'sha256'),'hex')),
 encode(extensions.digest(string_agg(to_jsonb(d)::text,E'\n' ORDER BY relation_name),'sha256'),'hex') INTO n,bad,h
 FROM population_top3000_202608_202609_v1.catalog_d06_preservation d WHERE scope_id='corrected_company_scope_8e6ca1178f03822c';
 IF n<>1065 OR bad<>0 OR h IS DISTINCT FROM 'e8434fbedae898ea45c2ce1d31439e1945614f778b333ab3f41a63a751547721'
 THEN RAISE EXCEPTION 'D06_REVIEWED_METADATA_CATALOG_CHANGED';END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(jsonb_build_array(
 jsonb_build_object('component_key','CTX02_SAMPLE_TYPED_PROJECTION_203_20261024','family_id','CTX02','accepted_component_units',203,
 'component_unit','EXACT_RETAINED_CONNECTOR_CSV_TO_TYPED_PROJECTION','independent_receipt_path','acceptance07/ctx02_live_independent_verification.json',
 'independent_receipt_sha256','3a88c2200531bb8f08e012760d8816a155926f986a03acfddcc868a84acfd077',
 'projection_with_outer_identity_manifest_sha256','2071e5b22387940e7e206ae8ec96abfd3fe801236eb553f9d08c0edbe202198c',
 'evidence_manifest_sha256','061e2dc293d0b76c93986c8d923022ed887dfab8230fd5ff54e61d02e1b3ba83','source_pins',source_pins,
 'reference_records',190,'daily_bar_records',13,'adjusted_deliverable_records',8,'source_market_accuracy_accepted',false,'population_census_complete',false,'missing_bar_is_no_trade',false),
 jsonb_build_object('component_key','D06_PRESERVATION_METADATA_1065_20261024','family_id','D06_PRESERVATION_ONLY','accepted_component_units',1065,
 'component_unit','EXISTING_CATALOG_RELATION_AND_RETAINED_METADATA_HASH','independent_receipt_path','acceptance07/d06_preservation_consumer_review_20261024.md',
 'independent_receipt_sha256','52e7f9a74a088f200bc6cc3c55dea6f53e183a621297e0bb47d3efdd2def2a89',
 'catalog_sha256','e8434fbedae898ea45c2ce1d31439e1945614f778b333ab3f41a63a751547721','source_data_content_unchanged_proven',false,
 'original_study_definitions_verified',false,'date_coverage_state','UNKNOWN','prior_validation_state','NOT_INSPECTED_NO_STATUS_PROMOTION',
 'administrative_consumer_read_only',true,'application_role_consumer_access_proven',false,'new_study_executed',false,'belongs_to_24_required_family_denominator',false))) LOOP
 item:=item||jsonb_build_object('result','ACCEPTED_COMPONENT_ONLY','authority_event_id',9528,'accepted_by_controller',a.owner_id,'owner_fence',a.owner_fence,
 'stop_generation',a.stop_generation,'scope_sha256',a.scope_sha256,'independent_reviewer','top3000-completion-20261024-acceptance07',
 'full_family_accepted',false,'company_population_accepted',false,'publication_supported_reconstruction_qualified',false,'strict_original_receipt_qualified',false,
 'serving_pointer_promoted',false,'required_denominator_changed',false,'existing_hold_released',false,'actual_new_provider_requests',0);
 SELECT count(*),max(details::text)::jsonb INTO n,prior FROM population_top3000_202608_202609_v1.pipeline_event
 WHERE event='AUTHORITATIVE_COMPONENT_ACCEPTANCE_RECORDED_V1' AND details->>'component_key'=item->>'component_key';
 IF n=0 THEN INSERT INTO population_top3000_202608_202609_v1.pipeline_event(event,details) VALUES('AUTHORITATIVE_COMPONENT_ACCEPTANCE_RECORDED_V1',item);
 ELSIF n<>1 OR prior IS DISTINCT FROM item THEN RAISE EXCEPTION 'COMPONENT_ACCEPTANCE_IMMUTABLE_REPLAY_CONFLICT';END IF;
 END LOOP;
 IF old_controls IS DISTINCT FROM jsonb_build_object('acquisition',(SELECT to_jsonb(x) FROM population_top3000_202608_202609_v1.acquisition_control x WHERE singleton),'pipeline',(SELECT to_jsonb(x) FROM population_top3000_202608_202609_v1.pipeline_control x WHERE singleton))
 THEN RAISE EXCEPTION 'COMPONENT_ACCEPTANCE_MUST_PRESERVE_ALL_CONTROLS_AND_COUNTERS';END IF;
END; $accept$;
COMMIT;
SELECT event_id,event,details FROM population_top3000_202608_202609_v1.pipeline_event
WHERE event='AUTHORITATIVE_COMPONENT_ACCEPTANCE_RECORDED_V1' AND details->>'component_key' IN('CTX02_SAMPLE_TYPED_PROJECTION_203_20261024','D06_PRESERVATION_METADATA_1065_20261024') ORDER BY event_id;
