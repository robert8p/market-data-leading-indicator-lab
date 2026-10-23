CREATE OR REPLACE FUNCTION population_top3000_202608_202609_v1.pipeline_repair_v1(p_attempt uuid, p_token text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog'
AS $function$
declare tid uuid;t population_top3000_202608_202609_v1.pipeline_task%rowtype;v population_top3000_202608_202609_v1.pipeline_validation%rowtype;
 m jsonb;item record;n bigint:=0;out_hash text;
begin
 tid:=population_top3000_202608_202609_v1.pipeline_assert_attempt_v1(p_attempt,p_token);
 select * into t from population_top3000_202608_202609_v1.pipeline_task where task_id=tid;
 select x.* into v from population_top3000_202608_202609_v1.pipeline_partition p join population_top3000_202608_202609_v1.pipeline_validation x on x.validation_id=p.last_validation_id where p.family_id=t.family_id and p.partition_key=t.partition_key;
 if not found then return jsonb_build_object('state','BLOCKED','blocker_code','VALIDATION_REQUIRED','resumption_condition',jsonb_build_object('event','PARTITION_VALIDATION_COMMITTED'));end if;
 m:=population_top3000_202608_202609_v1.pipeline_measure_partition_v1(t.family_id,t.partition_key);
 if m->>'state'<>'MEASURED' or m->>'expected_manifest_sha256' is distinct from v.expected_manifest_sha256 or m->>'observed_manifest_sha256' is distinct from v.observed_manifest_sha256 then
 return jsonb_build_object('state','BLOCKED','blocker_code','INPUT_CHANGED_REVERIFY_NEW_REVISION','resumption_condition',jsonb_build_object('event','NEW_INPUT_REVISION_VALIDATION_ENQUEUED'));end if;
 for item in select * from (values
 ('MISSING_SOURCE_UNITS',v.missing_units,'Acquire missing expected source keys through the bound family acquisition adapter.'),
 ('MISSING_REQUIRED_FIELDS',v.missing_required_fields,'Repair exact absent required fields from source spans; never fill fabricated values.'),
 ('UNAVAILABLE_HISTORY',v.unavailable_fields,'Provide a genuine historical source/version or documented publisher missingness.'),
 ('CONFLICTING_FIELDS',v.conflicting_fields,'Resolve source conflict with evidence and preserve both source versions.'),
 ('QUARANTINED_FIELDS',v.quarantined_fields,'Apply diagnosed source-backed correction and independently reverify.'),
 ('UNEXPECTED_UNITS',v.unexpected_units,'Reconcile source/index membership; never silently drop source rows.'),
 ('STRUCTURAL_FAILURES',v.structural_failures,'Rebuild only mismatched normalized rows from retained immutable raw evidence.'),
 ('MEMBERSHIP_FAILURES',v.membership_failures,'Resolve evidenced dated company/security mapping.'),
 ('TEMPORAL_FAILURES',v.temporal_failures,'Correct actual source time semantics; never synthesize receipt timestamps.'),
 ('REQUIRED_INDEPENDENT_CHECKS',cardinality(v.required_checks_missing)::bigint,'Complete missing independent source, outlier and consumer checks bound to these manifests.'),
 ('DENOMINATOR_UNRESOLVED',case when v.result='DENOMINATOR_UNRESOLVED' then 1 else 0 end::bigint,'Freeze independently supported source expectations and required company universe.')
 ) x(code,units,resume) where units>0 loop
 insert into population_top3000_202608_202609_v1.pipeline_exception(family_id,partition_key,validation_id,code,affected_units,evidence,resumption_condition)
 values(t.family_id,t.partition_key,v.validation_id,item.code,item.units,jsonb_build_object('validation_id',v.validation_id,'expected_manifest_sha256',v.expected_manifest_sha256,'observed_manifest_sha256',v.observed_manifest_sha256,'required_checks_missing',v.required_checks_missing),jsonb_build_object('event','REVIEWED_REPAIR_SOURCE_OR_CODE_AVAILABLE','action',item.resume)) on conflict(validation_id,code) do nothing;
 get diagnostics n=row_count;
 end loop;
 select encode(sha256(convert_to(coalesce(jsonb_agg(jsonb_build_object('code',code,'units',affected_units) order by code),'[]')::text,'UTF8')),'hex'),count(*) into out_hash,n from population_top3000_202608_202609_v1.pipeline_exception where validation_id=v.validation_id;
 return jsonb_build_object('state','COMPLETE','repair_outcome','NO_SAFE_AUTOMATIC_REPAIR','repair_applied',false,'unresolved_exceptions',n,'next_stage','PUBLISH','publication_classification','PROVISIONAL_SOURCE_EVIDENCE',
 'commit_receipt',jsonb_build_object('commit_key','repair-diagnosis:'||v.validation_id::text,'output_sha256',out_hash,'new_committed_units',n,'immutable_output_ids',jsonb_build_array(v.validation_id),'input_versions',t.input_versions));
end $function$
