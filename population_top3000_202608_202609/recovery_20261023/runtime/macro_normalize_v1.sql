CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.macro_normalize_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;before_n integer;after_n integer;expected_n integer;h text;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 if t.partition_key not in('headline','revision','warmup') then raise exception 'MACRO_PARTITION_NOT_ADMITTED';end if;
 select count(*) into before_n from population_top3000_202608_202609_v1.source_observation where family_id='CTX07' and partition_key=t.partition_key;
INSERT INTO population_top3000_202608_202609_v1.source_document
 (source_id,source_sha256,source_uri,source_provider,retrieved_at,source_published_at,historical_received_at,content_text,content_external_uri,content_bytes,evidence,license_policy)
SELECT 'CTX07:'||r.source_id,coalesce(r.raw_sha256,r.payload_sha256),r.source_url,r.source_kind,r.received_at,
 (SELECT min(o.released_at) FROM population_top3000_202608_202609_v1.macro_observations o WHERE o.source_id=r.source_id),NULL,
 CASE WHEN r.raw_sha256 IS NULL THEN r.payload_text ELSE NULL END,
 CASE WHEN r.raw_sha256 IS NOT NULL THEN r.source_url ELSE NULL END,
 NULL,jsonb_build_object('typed_source_id',r.source_id,'exact_metadata_sha256',r.payload_sha256,'retained_hash_only_reason','Original government source is retained in separately hashed evidence bundle; raw publisher URL is not asserted immutable. ISM article bodies are not retained.','metadata',r.evidence),
 CASE WHEN r.source_kind LIKE '%ISM%' OR r.source_url LIKE '%prnewswire.com%' THEN 'PUBLIC_METADATA_ONLY_USAGE_RIGHTS_QUALIFICATION_PENDING' ELSE 'OFFICIAL_GOVERNMENT_SOURCE_EVIDENCE' END
FROM population_top3000_202608_202609_v1.macro_source_registry r
ON CONFLICT DO NOTHING;

WITH selected AS (
 SELECT o.*,CASE o.record_role WHEN 'existing_warmup' THEN 'warmup' ELSE o.record_role END AS partition_key,
 CASE WHEN o.record_role='existing_warmup' THEN 'SUPPORT' ELSE 'TARGET' END AS time_role,
 COALESCE((o.payload->>'prior_print_as_known')::numeric,p.actual) AS earlier_headline_print
 FROM population_top3000_202608_202609_v1.macro_observations o
 LEFT JOIN LATERAL (
  SELECT q.actual FROM population_top3000_202608_202609_v1.macro_observations q
  WHERE q.factor_name=o.factor_name AND q.released_at<o.released_at
   AND q.record_role IN ('headline','existing_warmup')
  ORDER BY q.released_at DESC LIMIT 1
 ) p ON true
 WHERE o.record_role IN ('headline','revision','existing_warmup')
  AND (CASE o.record_role WHEN 'existing_warmup' THEN 'warmup' ELSE o.record_role END)=t.partition_key
  AND o.factor_name NOT IN ('ism_manufacturing_pmi_as_released','ism_services_pmi_as_released')
), shaped AS (
 SELECT s.*,jsonb_build_object('factor_name',factor_name,'actual',actual,'units',units,'reference_period',reference_period,'released_at',released_at,'available_at',available_at,'prior_print_as_known',earlier_headline_print,'consensus',NULL,'surprise',NULL) AS values_json,
 jsonb_build_object('actual','VALUE','units','VALUE','reference_period','VALUE','released_at','VALUE','available_at','VALUE','prior_print_as_known',CASE WHEN earlier_headline_print IS NULL THEN 'UNAVAILABLE' ELSE 'VALUE' END) AS field_state,
 jsonb_build_object('typed_observation_id',observation_id,'exact_payload_sha256',payload_sha256,'source_content_sha256',source_content_sha256,'record_role',record_role,'qualification',qualification,'publication_time_basis','OFFICIAL_PUBLICATION_HEADER_RECONSTRUCTION','historical_receipt',NULL,'prior_is_not_revised_same_release',true) AS lineage
 FROM selected s
)
INSERT INTO population_top3000_202608_202609_v1.source_observation
 (family_id,partition_key,unit_key,time_role,event_at,effective_date,source_published_at,historical_received_at,available_at,availability_policy,vintage_key,source_id,source_record_key,normalization_sha256,values_json,field_state,lineage,record_sha256)
SELECT 'CTX07',partition_key,source_key,time_role,released_at,(reference_period AT TIME ZONE 'America/New_York')::date,released_at,NULL,available_at,
 'PUBLICATION_SUPPORTED_RECONSTRUCTION',released_at::text,'CTX07:'||source_id,source_key,payload_sha256,values_json,field_state,lineage,
 encode(sha256(convert_to(jsonb_build_object('values',values_json,'states',field_state,'lineage',lineage)::text,'UTF8')),'hex')
FROM shaped
ON CONFLICT DO NOTHING;

INSERT INTO population_top3000_202608_202609_v1.expected_observation
 (family_id,partition_key,unit_key,time_role,required_fields,expectation_source_id,expectation_record_key,applicability_evidence)
SELECT 'CTX07',CASE record_role WHEN 'existing_warmup' THEN 'warmup' WHEN 'ism_metadata_candidate' THEN 'headline' ELSE record_role END,
 source_key,CASE WHEN record_role='existing_warmup' THEN 'SUPPORT' ELSE 'TARGET' END,
 ARRAY['actual','reference_period','released_at','available_at','units','prior_print_as_known'],
 'CTX07:'||source_id,source_key,
 jsonb_build_object('factor_name',factor_name,'record_role',record_role,'exact21_series_scope',true,
 'publication_header_verified',true,'rights_status',CASE WHEN factor_name LIKE 'ism_%' THEN 'PENDING_USAGE_RIGHTS_QUALIFICATION' ELSE 'OFFICIAL_GOVERNMENT_SOURCE' END,
 'denominator_status',CASE WHEN record_role='revision' THEN 'PROVISIONAL' ELSE 'ENUMERATED_SOURCE_RELEASES' END,
 'revision_source_index_complete',CASE WHEN record_role='revision' THEN false ELSE NULL END,
 'required_revision_source_index_check',CASE WHEN record_role='revision' THEN 'UNAVAILABLE' ELSE NULL END,
 'revision_scope_note',CASE WHEN record_role='revision' THEN '15 explicit relevant prior-period revisions enumerated. Census September28 annual table evidence retained separately. Exhaustive relevant revision-index coverage not yet certified.' ELSE NULL END)
FROM population_top3000_202608_202609_v1.macro_observations
WHERE (CASE record_role WHEN 'existing_warmup' THEN 'warmup' WHEN 'ism_metadata_candidate' THEN 'headline' ELSE record_role END)=t.partition_key
ON CONFLICT DO NOTHING;

 select count(*),encode(sha256(convert_to(string_agg(unit_key||':'||record_sha256||E'\n','' order by unit_key),'UTF8')),'hex') into after_n,h
 from population_top3000_202608_202609_v1.source_observation where family_id='CTX07' and partition_key=t.partition_key;
 select count(*) into expected_n from population_top3000_202608_202609_v1.expected_observation where family_id='CTX07' and partition_key=t.partition_key;
 if after_n<>(case t.partition_key when 'headline' then 39 when 'revision' then 15 else 19 end) or expected_n<>(case t.partition_key when 'headline' then 43 when 'revision' then 15 else 21 end) then raise exception 'MACRO_NORMALIZED_COUNTS_MISMATCH';end if;
 insert into population_top3000_202608_202609_v1.pipeline_partition(family_id,partition_key,time_role,denominator_state,denominator_evidence,producer_revision,immutable_input_versions,publication_eligible)
 values('CTX07',t.partition_key,case when t.partition_key='warmup' then 'SUPPORT' else 'TARGET' end,'PROVISIONAL',
 jsonb_build_object('source_expected_units',expected_n,'source_observed_units',after_n,'basis','Enumeration of retained original release headers and prior source-backed support; exhaustive release/revision index not independently certified','ism_rights_unresolved',t.partition_key<>'revision','strict_historical_receipts',0),
 'macro-bridge-v2',t.input_versions,false) on conflict(family_id,partition_key)do nothing;
 return jsonb_build_object('state','COMPLETE','commit_receipt',jsonb_build_object('commit_key','macro-normalized:'||t.partition_key||':'||h,'input_versions',t.input_versions,
 'immutable_output_ids',jsonb_build_array('CTX07:'||t.partition_key),'output_sha256',h,'new_committed_units',after_n-before_n,'expected_units',expected_n,'received_units',after_n,'original_receipt_qualified',0));
end $function$
